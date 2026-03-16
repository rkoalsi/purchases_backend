"""
sync_amazon_sku_mapping.py  —  mismatch checker (read-only, no DB writes)

Compares live Amazon SP-API listings against the master Google Sheet and
writes a single Excel report flagging every discrepancy:
  • SKU Code mismatch  (SP-API seller-sku ≠ Sheet "SKU Code (Final)")
  • Name mismatch      (SP-API item-name ≠ Sheet "Amazon Final Title")
  • Missing in Sheet   (ASIN exists in SP-API but has no row in the Sheet)
  • Not in SP-API      (ASIN exists in Sheet but has no active SP-API listing)

Output: scripts/output/amazon_name_comparison.xlsx   (one sheet)

Usage:
    python scripts/sync_amazon_sku_mapping.py
"""

import csv
import gzip
import io
import logging
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

SP_REFRESH_TOKEN = os.getenv("SP_REFRESH_TOKEN")
SP_CLIENT_ID     = os.getenv("SP_CLIENT_ID")
SP_CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET")
SP_LOGIN_URL     = os.getenv("SP_LOGIN_URL", "https://api.amazon.com/auth/o2/token")
SP_API_BASE_URL  = os.getenv("SP_API_BASE_URL", "https://sellingpartnerapi-eu.amazon.com")
MARKETPLACE_ID   = os.getenv("SP_MARKETPLACE_ID", "A21TJRUUN4KGV")

SHEET_ID      = "1tn_Lj3KR0zXY8B-8ZUkSznZgE4YzyjtAkcpdHzBCgt4"
SHEET_CSV_URL = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/export?format=csv"

COL_ASIN       = "Amazon ASIN"
COL_SKU_CODE   = "SKU Code (Final)"
COL_SHEET_NAME = "Amazon Final Title"

REPORT_POLL_INTERVAL_S = 15
REPORT_TIMEOUT_S       = 600

OUTPUT_DIR      = Path(__file__).parent / "output"
COMPARISON_XLSX = OUTPUT_DIR / "amazon_name_comparison.xlsx"


# ---------------------------------------------------------------------------
# SP-API auth
# ---------------------------------------------------------------------------

_access_token: Optional[str] = None
_token_expires_at: Optional[datetime] = None


def get_sp_token() -> str:
    global _access_token, _token_expires_at
    if _access_token and _token_expires_at and datetime.now() < _token_expires_at - timedelta(minutes=5):
        return _access_token
    logger.info("Fetching SP-API access token...")
    resp = requests.post(
        SP_LOGIN_URL,
        data={
            "grant_type":    "refresh_token",
            "refresh_token": SP_REFRESH_TOKEN,
            "client_id":     SP_CLIENT_ID,
            "client_secret": SP_CLIENT_SECRET,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    _access_token = data["access_token"]
    _token_expires_at = datetime.now() + timedelta(seconds=data.get("expires_in", 3600))
    return _access_token


def sp_get(endpoint: str, params: dict = None) -> dict:
    resp = requests.get(
        f"{SP_API_BASE_URL}{endpoint}",
        headers={"x-amz-access-token": get_sp_token(), "Content-Type": "application/json"},
        params=params,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def sp_post(endpoint: str, body: dict) -> dict:
    resp = requests.post(
        f"{SP_API_BASE_URL}{endpoint}",
        headers={"x-amz-access-token": get_sp_token(), "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# SP-API: fetch all listings via GET_MERCHANT_LISTINGS_ALL_DATA report
# ---------------------------------------------------------------------------

@dataclass
class SpListing:
    asin: str
    seller_sku: str
    item_name: str
    status: str = ""


def _request_listings_report() -> str:
    logger.info("Requesting GET_MERCHANT_LISTINGS_ALL_DATA report...")
    data = sp_post("/reports/2021-06-30/reports", {
        "reportType":     "GET_MERCHANT_LISTINGS_ALL_DATA",
        "marketplaceIds": [MARKETPLACE_ID],
    })
    report_id = data.get("reportId")
    if not report_id:
        raise RuntimeError(f"No reportId in response: {data}")
    logger.info(f"Report requested — reportId={report_id}")
    return report_id


def _poll_until_done(report_id: str) -> str:
    elapsed = 0
    while elapsed < REPORT_TIMEOUT_S:
        status_data = sp_get(f"/reports/2021-06-30/reports/{report_id}")
        processing_status = status_data.get("processingStatus", "")
        logger.info(f"  Report status: {processing_status} ({elapsed}s)")
        if processing_status == "DONE":
            doc_id = status_data.get("reportDocumentId")
            if not doc_id:
                raise RuntimeError("Report DONE but no reportDocumentId")
            return doc_id
        elif processing_status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"Report ended with status {processing_status}: {status_data}")
        time.sleep(REPORT_POLL_INTERVAL_S)
        elapsed += REPORT_POLL_INTERVAL_S
    raise TimeoutError(f"Report did not complete within {REPORT_TIMEOUT_S}s")


def _download_report(report_document_id: str) -> str:
    doc_info = sp_get(f"/reports/2021-06-30/documents/{report_document_id}")
    download_url = doc_info.get("url")
    if not download_url:
        raise RuntimeError(f"No download URL in document info: {doc_info}")
    logger.info("Downloading report...")
    resp = requests.get(download_url, timeout=120)
    resp.raise_for_status()

    raw = resp.content
    if doc_info.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)

    for enc in ("utf-8", "utf-8-sig", "latin-1", "cp1252"):
        try:
            return raw.decode(enc)
        except UnicodeDecodeError:
            continue
    return raw.decode("latin-1")


def fetch_all_sp_listings() -> List[SpListing]:
    report_id = _request_listings_report()
    doc_id    = _poll_until_done(report_id)
    tsv_text  = _download_report(doc_id)

    reader = csv.DictReader(io.StringIO(tsv_text), delimiter="\t")
    listings: List[SpListing] = []
    for row in reader:
        asin = (row.get("asin1") or "").strip()
        if not asin:
            continue
        listings.append(SpListing(
            asin=asin,
            seller_sku=(row.get("seller-sku") or "").strip(),
            item_name=(row.get("item-name") or "").strip(),
            status=(row.get("status") or "").strip(),
        ))
    logger.info(f"Parsed {len(listings)} listings from SP-API.")
    return listings


# ---------------------------------------------------------------------------
# Google Sheet
# ---------------------------------------------------------------------------

@dataclass
class SheetRow:
    asin: str
    sku_code: str
    sheet_name: str


def fetch_google_sheet() -> List[SheetRow]:
    logger.info("Fetching Google Sheet...")
    resp = requests.get(SHEET_CSV_URL, timeout=30)
    resp.raise_for_status()

    lines = resp.text.splitlines()
    if len(lines) < 2:
        raise RuntimeError("Google Sheet has fewer than 2 rows")

    # Row 1 is a top-level header; row 2 is the actual column header
    reader = csv.DictReader(io.StringIO("\n".join(lines[1:])))
    rows: List[SheetRow] = []
    for row in reader:
        asin      = (row.get(COL_ASIN) or "").strip()
        sku_code  = (row.get(COL_SKU_CODE) or "").strip()
        sheet_name = (row.get(COL_SHEET_NAME) or "").strip()
        if not asin:
            continue
        rows.append(SheetRow(asin=asin, sku_code=sku_code, sheet_name=sheet_name))

    logger.info(f"Loaded {len(rows)} rows from Google Sheet.")
    return rows


# ---------------------------------------------------------------------------
# Compare
# ---------------------------------------------------------------------------

def build_comparison_rows(
    sp_listings: List[SpListing],
    sheet_rows: List[SheetRow],
) -> List[dict]:
    """
    Returns one dict per discrepancy row:
      ASIN | SP-API Seller SKU | Sheet SKU Code | SP-API Name | Sheet Name | Mismatched Fields
    """
    sheet_by_asin: Dict[str, SheetRow] = {r.asin: r for r in sheet_rows}
    sp_by_asin:    Dict[str, SpListing] = {l.asin: l for l in sp_listings}

    rows = []

    # Check every SP-API listing
    for listing in sp_listings:
        sheet = sheet_by_asin.get(listing.asin)
        if not sheet:
            rows.append({
                "ASIN":              listing.asin,
                "SP-API Seller SKU": listing.seller_sku,
                "Sheet SKU Code":    "",
                "SP-API Name":       listing.item_name,
                "Sheet Name":        "",
                "Mismatched Fields": "Missing in Sheet",
            })
            continue

        mismatches = []
        if sheet.sku_code and listing.seller_sku.lower() != sheet.sku_code.lower():
            mismatches.append("SKU Code")
        if sheet.sheet_name and listing.item_name.lower().strip() != sheet.sheet_name.lower().strip():
            mismatches.append("Name")

        if mismatches:
            rows.append({
                "ASIN":              listing.asin,
                "SP-API Seller SKU": listing.seller_sku,
                "Sheet SKU Code":    sheet.sku_code,
                "SP-API Name":       listing.item_name,
                "Sheet Name":        sheet.sheet_name,
                "Mismatched Fields": ", ".join(mismatches),
            })

    # Sheet rows with no active SP-API listing
    for sheet in sheet_rows:
        if sheet.asin not in sp_by_asin:
            rows.append({
                "ASIN":              sheet.asin,
                "SP-API Seller SKU": "",
                "Sheet SKU Code":    sheet.sku_code,
                "SP-API Name":       "",
                "Sheet Name":        sheet.sheet_name,
                "Mismatched Fields": "Not in SP-API",
            })

    return rows


# ---------------------------------------------------------------------------
# Excel export
# ---------------------------------------------------------------------------

def save_comparison_xlsx(rows: List[dict]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows, columns=[
        "ASIN", "SP-API Seller SKU", "Sheet SKU Code",
        "SP-API Name", "Sheet Name", "Mismatched Fields",
    ])
    df.to_excel(COMPARISON_XLSX, index=False, sheet_name="Mismatches")
    logger.info(f"Comparison report saved → {COMPARISON_XLSX}  ({len(df)} rows)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    missing_vars = [k for k, v in {
        "SP_REFRESH_TOKEN": SP_REFRESH_TOKEN,
        "SP_CLIENT_ID":     SP_CLIENT_ID,
        "SP_CLIENT_SECRET": SP_CLIENT_SECRET,
    }.items() if not v]
    if missing_vars:
        logger.error(f"Missing env vars: {', '.join(missing_vars)}")
        sys.exit(1)

    sp_listings = fetch_all_sp_listings()
    sheet_rows  = fetch_google_sheet()
    rows        = build_comparison_rows(sp_listings, sheet_rows)

    # Summary
    counts: Dict[str, int] = {}
    for r in rows:
        key = r["Mismatched Fields"]
        counts[key] = counts.get(key, 0) + 1

    print("\n" + "=" * 60)
    print("MISMATCH SUMMARY")
    print("=" * 60)
    if not counts:
        print("  No discrepancies found — everything matches!")
    else:
        for label, n in sorted(counts.items()):
            print(f"  {label:<30} {n}")
    print(f"\n  Total discrepancies: {sum(counts.values())}")
    print("=" * 60 + "\n")

    if rows:
        save_comparison_xlsx(rows)
    else:
        logger.info("No discrepancies — skipping Excel export.")


if __name__ == "__main__":
    main()
