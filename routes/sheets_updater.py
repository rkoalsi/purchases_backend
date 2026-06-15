"""
routes/sheets_updater.py

Endpoints to update specific columns in Google Sheets workbooks from live data.

Jobs
----
POST /sheets/update-all              (recommended — single Zoho fetch, parallel writes)
POST /sheets/update-master-stock
POST /sheets/update-vendor-sheets
GET  /sheets/config
PUT  /sheets/config/{job_id}
"""

from __future__ import annotations

import logging
import os
import asyncio
from datetime import datetime, timedelta

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, Form
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..database import get_database

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Google Sheet IDs ──────────────────────────────────────────────────────────
MASTER_SHEET_ID         = "1bQEqJxPR_m1Y-K7Bjh74kio2ZTEWENFLoFTl1POabMY"
MASTER_SHEET_NAME       = "Master"
AMAZON_COMBO_SHEET_NAME = "AmazonComboProducts"
HEADER_ROW_INDEX        = 1   # 0-based; row 0 is group headers, row 1 = column headers (sheet row 2)
DATA_START_ROW          = 2   # 0-based index of first data row (sheet row 3)

# Column CS is column 97 (1-indexed): CA=79 … CS=79+18=97
STOCK_COL_INDEX = 97

VENDOR_SHEET_ID = "1EDPhQe3OwQacohdbVs194jQZU2L5MEJtA4jOswn8oW8"

VENDOR_TABS = [
    "FOFOS Master_Dec 25",
    "Truelove Master_USD",
    "Truelove Master_RMB_Dec 25",
    "Zippy Paws Master_Jan 26",
    "Petfest Master_Jan 26",
    "Joyser Master_Jan 26",
]

VENDOR_HEADER_ROW_INDEX = 5

COL_SALES      = "Total sale of Last 3 months"
COL_DAYS       = "Total No. of days in stock last 3 months"
COL_ZOHO_STOCK = "Zoho Stock (Available for Sale)"
COL_AVG_SALES  = "Average Sales per day"

SKU_COL_CANDIDATES = [
    "bb code", "bb_code",
    "sku code (final)", "sku code", "sku", "cf_sku_code",
]

_ZOHO_TOKEN_URL    = "https://accounts.zoho.com/oauth/v2/token"
_PUPSCRIBE_LOCATION_ID = "3220178000000403010"
_INV_SUMMARY_URL = (
    "https://inventory.zoho.com/api/v1/reports/inventorysummary"
    "?page={page}&per_page=200&filter_by=TransactionDate.CustomDate"
    "&show_actual_stock=false&to_date={to_date}"
    "&organization_id={org_id}&location_id={location_id}"
)
_ZOHO_ORG_ID = os.getenv("ZOHO_ORG_ID", "776755316")

CONFIG_COLLECTION = "sheets_updater_config"

# ── Request models ────────────────────────────────────────────────────────────

class MasterStockRequest(BaseModel):
    sheet_id: str | None = None


class VendorSheetsRequest(BaseModel):
    sheet_id: str | None = None


class CombinedSheetsRequest(BaseModel):
    master_sheet_id: str | None = None
    vendor_sheet_id: str | None = None


class ConfigUpdateRequest(BaseModel):
    sheet_url: str


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_sheet_id(value: str | None, default: str) -> str:
    if not value:
        return default
    value = value.strip()
    import re
    m = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", value)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]{20,}", value):
        return value
    logger.warning("_parse_sheet_id: could not parse '%s', using default", value)
    return default


def _get_inventory_token() -> str:
    import requests as _req
    r = _req.post(
        _ZOHO_TOKEN_URL,
        params={
            "refresh_token": os.getenv("INVENTORY_REFRESH_TOKEN", os.getenv("BOOKS_REFRESH_TOKEN", "")),
            "client_id":     os.getenv("CLIENT_ID", ""),
            "client_secret": os.getenv("CLIENT_SECRET", ""),
            "grant_type":    "refresh_token",
        },
        timeout=30,
    )
    r.raise_for_status()
    token = r.json().get("access_token")
    if not token:
        raise RuntimeError(f"Zoho token response had no access_token: {r.text[:300]}")
    return token


def _fetch_live_zoho_stock(db) -> tuple[dict[str, int], str]:
    """
    Fetch Pupscribe Enterprises Private Limited stock via the inventorysummary API.
    quantity_available_for_sale from inventorysummary is location-specific (unlike the
    items list API which always returns global totals regardless of location_id).
    Returns ({cf_sku_code: qty}, date_str).
    """
    import requests as _req

    today = datetime.now().strftime("%Y-%m-%d")
    token = _get_inventory_token()
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    # Step 1: fetch inventorysummary for Pupscribe WH → {item_id: qty}
    item_id_to_qty: dict[str, int] = {}
    page = 1
    while True:
        resp = _req.get(
            _INV_SUMMARY_URL.format(
                page=page, to_date=today,
                org_id=_ZOHO_ORG_ID, location_id=_PUPSCRIBE_LOCATION_ID,
            ),
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()
        if not data.get("inventory"):
            break
        for item in data["inventory"][0].get("item_details", []):
            iid = str(item.get("item_id") or "")
            if iid:
                item_id_to_qty[iid] = int(item.get("quantity_available_for_sale") or 0)
        logger.info("inventorysummary: page %d fetched (%d items so far)", page, len(item_id_to_qty))
        if not data.get("page_context", {}).get("has_more_page"):
            break
        page += 1

    # Step 2: map item_id -> cf_sku_code via products collection
    sku_stock: dict[str, int] = {}
    item_ids = list(item_id_to_qty.keys())
    for prod in db["products"].find(
        {"item_id": {"$in": item_ids}},
        {"item_id": 1, "cf_sku_code": 1},
    ):
        iid = str(prod.get("item_id") or "")
        sku = prod.get("cf_sku_code") or ""
        if iid and sku:
            sku_stock[sku] = item_id_to_qty.get(iid, 0)

    logger.info("Live Zoho stock (Pupscribe WH): %d SKUs for %s", len(sku_stock), today)
    return sku_stock, today


def _open_sheet(sheet_id: str, writable: bool = True):
    import gspread
    from google.oauth2.service_account import Credentials

    backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    creds_path = os.path.join(backend_dir, "creds.json")
    scope = (
        ["https://www.googleapis.com/auth/spreadsheets"]
        if writable
        else ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    )
    credentials = Credentials.from_service_account_file(creds_path, scopes=scope)
    gc = gspread.authorize(credentials)
    sh = gc.open_by_key(sheet_id)
    return gc, sh


def _find_col_idx(header: list[str], candidates: list[str]) -> int:
    header_lower = [h.lower() for h in header]
    for cand in candidates:
        c = cand.lower()
        for i, h in enumerate(header_lower):
            if h == c:
                return i
    for cand in candidates:
        c = cand.lower()
        for i, h in enumerate(header_lower):
            if c in h:
                return i
    return -1


def _last_3_months_range() -> tuple[str, str]:
    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    end_dt = first_of_this_month - timedelta(days=1)

    start_month = end_dt.month - 2
    start_year  = end_dt.year
    if start_month <= 0:
        start_month += 12
        start_year  -= 1

    start_dt = end_dt.replace(year=start_year, month=start_month, day=1)
    s, e = start_dt.strftime("%Y-%m-%d"), end_dt.strftime("%Y-%m-%d")
    logger.info("Last 3 complete calendar months: %s → %s", s, e)
    return s, e


# ── Core update logic (shared between individual and combined endpoints) ───────

async def _run_master_stock_core(
    db,
    sheet_id: str,
    sku_stock_map: dict[str, int],
    stock_date: str,
) -> dict:
    """Open master sheet, fetch product statuses, write stock + status columns."""
    import gspread

    try:
        _, sh = _open_sheet(sheet_id)
        ws = sh.worksheet(MASTER_SHEET_NAME)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not open master sheet: {exc}")

    try:
        all_rows = await asyncio.to_thread(ws.get_all_values)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read master sheet: {exc}")

    if len(all_rows) <= HEADER_ROW_INDEX:
        raise HTTPException(status_code=400, detail=f"Master sheet has fewer than {HEADER_ROW_INDEX + 1} rows")

    header = [h.strip() for h in all_rows[HEADER_ROW_INDEX]]

    def _col_idx(name: str) -> int:
        name_lower = name.lower()
        for i, h in enumerate(header):
            if h.lower() == name_lower:
                return i
        return -1

    sku_col_0 = _col_idx("SKU Code (Final)")
    if sku_col_0 == -1:
        sku_col_0 = 3
        logger.warning("'SKU Code (Final)' not found; defaulting to column D")

    status_col_0 = _col_idx("Purchase Status")
    if status_col_0 == -1:
        status_col_0 = _col_idx("Status")
    if status_col_0 == -1:
        status_col_0 = _col_idx("Remark")
    if status_col_0 == -1:
        status_col_0 = 0
        logger.warning("Status column not found; defaulting to column A")

    data_rows = all_rows[DATA_START_ROW:]
    skus = [
        (row[sku_col_0].strip() if sku_col_0 < len(row) else "")
        for row in data_rows
    ]
    unique_skus = [s for s in set(skus) if s]
    if not unique_skus:
        raise HTTPException(status_code=400, detail="No SKU codes found in master sheet")

    products_col = db.get_collection("products")

    def _fetch_products():
        return list(
            products_col.find(
                {"cf_sku_code": {"$in": unique_skus}},
                {"cf_sku_code": 1, "purchase_status": 1, "_id": 0},
            )
        )

    product_docs = await asyncio.to_thread(_fetch_products)
    product_by_sku: dict[str, dict] = {
        doc["cf_sku_code"]: doc for doc in product_docs if doc.get("cf_sku_code")
    }

    def _row_num(zero_based_data_idx: int) -> int:
        return DATA_START_ROW + 1 + zero_based_data_idx

    stock_updates: list[dict] = []
    status_updates: list[dict] = []
    updated = skipped_no_sku = skipped_no_product = skipped_no_stock = 0
    skipped_no_product_skus: list[str] = []
    skipped_no_stock_skus: list[str] = []

    for idx, row in enumerate(data_rows):
        sku = row[sku_col_0].strip() if sku_col_0 < len(row) else ""
        if not sku:
            skipped_no_sku += 1
            continue
        prod = product_by_sku.get(sku)
        if prod is None:
            skipped_no_product += 1
            skipped_no_product_skus.append(sku)
            continue

        sheet_row = _row_num(idx)
        stock_val = sku_stock_map.get(sku, "")
        stock_updates.append({
            "range": gspread.utils.rowcol_to_a1(sheet_row, STOCK_COL_INDEX),
            "values": [[stock_val]],
        })
        if status_col_0 != -1:
            status_updates.append({
                "range": gspread.utils.rowcol_to_a1(sheet_row, status_col_0 + 1),
                "values": [[prod.get("purchase_status", "")]],
            })
        updated += 1
        if sku not in sku_stock_map:
            skipped_no_stock += 1
            skipped_no_stock_skus.append(sku)

    if stock_updates:
        try:
            await asyncio.to_thread(
                ws.batch_update,
                stock_updates + status_updates,
                value_input_option="USER_ENTERED",
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"Master sheet write failed: {exc}")

    return {
        "success": True,
        "message": f"Updated {updated} rows in '{MASTER_SHEET_NAME}' sheet",
        "sheet_id": sheet_id,
        "updated": updated,
        "skipped_no_sku": skipped_no_sku,
        "skipped_no_product": skipped_no_product,
        "skipped_no_product_skus": skipped_no_product_skus,
        "skipped_no_stock": skipped_no_stock,
        "skipped_no_stock_skus": skipped_no_stock_skus,
        "stock_skus_resolved": len(sku_stock_map),
        "stock_date": stock_date,
        "products_matched": len(product_by_sku),
    }


async def _run_amazon_combo_status_core(db, sheet_id: str) -> dict:
    """Write purchase_status from products collection into the 'Amazon ComboProducts' sheet Status column."""
    import gspread

    try:
        _, sh = _open_sheet(sheet_id)
        ws = sh.worksheet(AMAZON_COMBO_SHEET_NAME)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not open '{AMAZON_COMBO_SHEET_NAME}' sheet: {exc}")

    try:
        all_rows = await asyncio.to_thread(ws.get_all_values)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not read '{AMAZON_COMBO_SHEET_NAME}' sheet: {exc}")

    if len(all_rows) <= HEADER_ROW_INDEX:
        raise HTTPException(status_code=400, detail=f"'{AMAZON_COMBO_SHEET_NAME}' sheet has fewer than {HEADER_ROW_INDEX + 1} rows")

    header = [h.strip() for h in all_rows[HEADER_ROW_INDEX]]

    def _col_idx(name: str) -> int:
        name_lower = name.lower()
        for i, h in enumerate(header):
            if h.lower() == name_lower:
                return i
        return -1

    sku_col_0    = _col_idx("SKU Code")
    status_col_0 = _col_idx("Status")

    if sku_col_0 == -1:
        raise HTTPException(status_code=400, detail=f"'SKU Code' column not found in '{AMAZON_COMBO_SHEET_NAME}'")
    if status_col_0 == -1:
        raise HTTPException(status_code=400, detail=f"'Status' column not found in '{AMAZON_COMBO_SHEET_NAME}'")

    data_rows = all_rows[DATA_START_ROW:]
    unique_skus = list({
        row[sku_col_0].strip()
        for row in data_rows
        if sku_col_0 < len(row) and row[sku_col_0].strip()
    })

    if not unique_skus:
        return {
            "success": True,
            "message": f"No SKU codes found in '{AMAZON_COMBO_SHEET_NAME}'",
            "updated": 0,
            "skipped_no_sku": len(data_rows),
            "skipped_no_product": 0,
        }

    def _fetch_products():
        return list(
            db.get_collection("products").find(
                {"cf_sku_code": {"$in": unique_skus}},
                {"cf_sku_code": 1, "purchase_status": 1, "_id": 0},
            )
        )

    product_docs = await asyncio.to_thread(_fetch_products)
    product_by_sku: dict[str, dict] = {
        doc["cf_sku_code"]: doc for doc in product_docs if doc.get("cf_sku_code")
    }

    status_updates: list[dict] = []
    updated = skipped_no_sku = skipped_no_product = 0

    for idx, row in enumerate(data_rows):
        sku = row[sku_col_0].strip() if sku_col_0 < len(row) else ""
        if not sku:
            skipped_no_sku += 1
            continue
        prod = product_by_sku.get(sku)
        if prod is None:
            skipped_no_product += 1
            continue
        sheet_row = DATA_START_ROW + 1 + idx  # 1-based for gspread
        status_updates.append({
            "range": gspread.utils.rowcol_to_a1(sheet_row, status_col_0 + 1),
            "values": [[prod.get("purchase_status", "")]],
        })
        updated += 1

    if status_updates:
        try:
            await asyncio.to_thread(
                ws.batch_update, status_updates, value_input_option="USER_ENTERED"
            )
        except Exception as exc:
            raise HTTPException(status_code=500, detail=f"'{AMAZON_COMBO_SHEET_NAME}' write failed: {exc}")

    return {
        "success": True,
        "message": f"Updated {updated} rows in '{AMAZON_COMBO_SHEET_NAME}'",
        "sheet_id": sheet_id,
        "updated": updated,
        "skipped_no_sku": skipped_no_sku,
        "skipped_no_product": skipped_no_product,
        "products_matched": len(product_by_sku),
    }


async def _run_vendor_sheets_core(
    db,
    vendor_sheet_id: str,
    sku_data: dict[str, dict],
    sku_stock_map: dict[str, int],
    stock_date: str,
    start_date: str,
    end_date: str,
) -> dict:
    """Open vendor workbook, iterate tabs, write sales / days / stock / avg formula."""
    import gspread

    try:
        _, sh = _open_sheet(vendor_sheet_id)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Could not open vendor workbook: {exc}")

    tab_results: list[dict] = []

    for tab_name in VENDOR_TABS:
        try:
            ws = sh.worksheet(tab_name)
        except Exception as exc:
            logger.warning("Tab '%s' not found: %s", tab_name, exc)
            tab_results.append({
                "tab": tab_name, "success": False,
                "error": f"Sheet tab not found: {exc}", "updated": 0,
            })
            continue

        try:
            all_rows = await asyncio.to_thread(ws.get_all_values)
        except Exception as exc:
            tab_results.append({
                "tab": tab_name, "success": False,
                "error": f"Could not read rows: {exc}", "updated": 0,
            })
            continue

        if len(all_rows) <= VENDOR_HEADER_ROW_INDEX:
            tab_results.append({
                "tab": tab_name, "success": False,
                "error": f"Sheet has fewer than {VENDOR_HEADER_ROW_INDEX + 1} rows",
                "updated": 0,
            })
            continue

        header_idx = VENDOR_HEADER_ROW_INDEX
        header = [h.strip() for h in all_rows[header_idx]]
        data_start = header_idx + 1

        sku_col_0     = _find_col_idx(header, SKU_COL_CANDIDATES)
        sales_col_0   = _find_col_idx(header, [COL_SALES, "total sale", "sales last 3"])
        days_col_0    = _find_col_idx(header, [COL_DAYS, "days in stock last 3", "days in stock"])
        avg_sales_col_0 = _find_col_idx(header, [COL_AVG_SALES, "average sales per day", "avg sales"])

        stock_col_0 = _find_col_idx(header, [COL_ZOHO_STOCK, "zoho stock", "available for sale"])
        zoho_col_created = False
        if stock_col_0 == -1:
            if avg_sales_col_0 != -1:
                stock_col_0 = avg_sales_col_0 + 1
                zoho_col_created = True
            elif days_col_0 != -1:
                stock_col_0 = days_col_0 + 2
                zoho_col_created = True

        missing = []
        if sku_col_0 == -1:
            missing.append("SKU/BB Code column")
        if sales_col_0 == -1:
            missing.append(f"'{COL_SALES}'")
        if days_col_0 == -1:
            missing.append(f"'{COL_DAYS}'")

        if missing:
            tab_results.append({
                "tab": tab_name, "success": False,
                "error": f"Missing columns: {', '.join(missing)}",
                "header_sample": header[:20], "updated": 0,
            })
            continue

        last_data_row = data_start + len(all_rows[data_start:])

        def _col_range(col_0: int) -> str:
            first = gspread.utils.rowcol_to_a1(data_start + 1, col_0 + 1)
            last  = gspread.utils.rowcol_to_a1(last_data_row,  col_0 + 1)
            return f"{first}:{last}"

        number_fmt  = {"numberFormat": {"type": "NUMBER"}}
        decimal_fmt = {"numberFormat": {"type": "NUMBER", "pattern": "0.00"}}
        fmt_ranges = [
            {"range": _col_range(sales_col_0),  "format": number_fmt},
            {"range": _col_range(days_col_0),   "format": number_fmt},
        ]
        if stock_col_0 != -1:
            fmt_ranges.append({"range": _col_range(stock_col_0), "format": number_fmt})
        if avg_sales_col_0 != -1:
            fmt_ranges.append({"range": _col_range(avg_sales_col_0), "format": decimal_fmt})

        try:
            await asyncio.to_thread(ws.batch_format, fmt_ranges)
        except Exception as fmt_exc:
            logger.warning("Tab '%s': format clear failed (non-fatal): %s", tab_name, fmt_exc)

        cell_updates: list[dict] = []
        if zoho_col_created and stock_col_0 != -1:
            cell_updates.append({
                "range": gspread.utils.rowcol_to_a1(header_idx + 1, stock_col_0 + 1),
                "values": [[COL_ZOHO_STOCK]],
            })

        data_rows_tab = all_rows[data_start:]
        updated = skipped_no_sku = skipped_no_data = 0
        skipped_no_data_skus: list[str] = []

        for row_idx, row in enumerate(data_rows_tab):
            sku = row[sku_col_0].strip() if sku_col_0 < len(row) else ""
            if not sku:
                skipped_no_sku += 1
                continue

            data = sku_data.get(sku)
            if data is None:
                # SKU had no activity in the report window — write zeros so stale
                # values from a previous update period are replaced with correct data.
                data = {"total_sales": 0, "total_days_in_stock": 0}

            sheet_row = data_start + 1 + row_idx

            if sales_col_0 != -1:
                cell_updates.append({
                    "range": gspread.utils.rowcol_to_a1(sheet_row, sales_col_0 + 1),
                    "values": [[data["total_sales"]]],
                })
            if days_col_0 != -1:
                cell_updates.append({
                    "range": gspread.utils.rowcol_to_a1(sheet_row, days_col_0 + 1),
                    "values": [[data["total_days_in_stock"]]],
                })
            if stock_col_0 != -1:
                cell_updates.append({
                    "range": gspread.utils.rowcol_to_a1(sheet_row, stock_col_0 + 1),
                    "values": [[sku_stock_map.get(sku, "")]],
                })
            if avg_sales_col_0 != -1 and sales_col_0 != -1 and days_col_0 != -1:
                sales_ref = gspread.utils.rowcol_to_a1(sheet_row, sales_col_0 + 1)
                days_ref  = gspread.utils.rowcol_to_a1(sheet_row, days_col_0 + 1)
                cell_updates.append({
                    "range": gspread.utils.rowcol_to_a1(sheet_row, avg_sales_col_0 + 1),
                    "values": [[f"=IFERROR({sales_ref}/{days_ref},0)"]],
                })
            updated += 1

        if not cell_updates:
            tab_results.append({
                "tab": tab_name, "success": True,
                "message": "No rows to update", "updated": 0,
                "skipped_no_sku": skipped_no_sku, "skipped_no_data": skipped_no_data,
                "skipped_no_data_skus": skipped_no_data_skus,
            })
            continue

        try:
            await asyncio.to_thread(
                ws.batch_update, cell_updates, value_input_option="USER_ENTERED",
            )
            tab_results.append({
                "tab": tab_name, "success": True,
                "message": f"Updated {updated} rows"
                + (" (Zoho Stock column header created)" if zoho_col_created else ""),
                "updated": updated,
                "zoho_col_created": zoho_col_created,
                "skipped_no_sku": skipped_no_sku,
                "skipped_no_data": skipped_no_data,
                "skipped_no_data_skus": skipped_no_data_skus,
            })
            logger.info("Tab '%s': %d rows updated", tab_name, updated)
        except Exception as exc:
            logger.exception("Failed to write tab '%s'", tab_name)
            tab_results.append({
                "tab": tab_name, "success": False,
                "error": f"Sheet write failed: {exc}", "updated": 0,
            })

    total_updated = sum(t.get("updated", 0) for t in tab_results)
    tabs_ok = sum(1 for t in tab_results if t.get("success"))

    return {
        "success": True,
        "sheet_id": vendor_sheet_id,
        "date_range": {
            "start_date": start_date,
            "end_date":   end_date,
            "note": "Last 3 complete calendar months",
        },
        "zoho_stock_date": stock_date,
        "message": (
            f"Vendor sheets update complete: {tabs_ok}/{len(VENDOR_TABS)} tabs OK, "
            f"{total_updated} total rows updated"
        ),
        "sku_data_entries": len(sku_data),
        "tabs": tab_results,
    }


# ── Config endpoints ──────────────────────────────────────────────────────────

@router.get("/config")
async def get_config(db=Depends(get_database)):
    def _fetch():
        return list(db[CONFIG_COLLECTION].find({}, {"_id": 0, "job_id": 1, "sheet_url": 1}))

    docs = await asyncio.to_thread(_fetch)
    return {d["job_id"]: d["sheet_url"] for d in docs if d.get("job_id") and d.get("sheet_url")}


@router.put("/config/{job_id}")
async def update_config(job_id: str, body: ConfigUpdateRequest, db=Depends(get_database)):
    url = body.sheet_url.strip()

    def _upsert():
        if url:
            db[CONFIG_COLLECTION].update_one(
                {"job_id": job_id},
                {"$set": {"job_id": job_id, "sheet_url": url}},
                upsert=True,
            )
        else:
            db[CONFIG_COLLECTION].delete_one({"job_id": job_id})

    await asyncio.to_thread(_upsert)
    return {"job_id": job_id, "sheet_url": url}


# ── Combined endpoint (recommended) ──────────────────────────────────────────

@router.post("/update-all")
async def update_all_sheets(
    body: CombinedSheetsRequest = CombinedSheetsRequest(),
    db=Depends(get_database),
):
    """
    Fetch Zoho stock then write Master stock + Amazon ComboProducts status in parallel.
    """
    master_sheet_id = _parse_sheet_id(body.master_sheet_id, MASTER_SHEET_ID)

    # ── Phase 1: fetch Zoho stock ─────────────────────────────────────────────
    logger.info("update-all: fetching Zoho stock")
    try:
        sku_stock_map, stock_date = await asyncio.to_thread(_fetch_live_zoho_stock, db)
    except Exception as exc:
        logger.exception("update-all: phase 1 failed")
        raise HTTPException(status_code=500, detail=f"Data fetch failed: {exc}")

    logger.info("update-all: Zoho stock=%d SKUs  stock_date=%s", len(sku_stock_map), stock_date)

    # ── Phase 2: write all sheets in parallel ────────────────────────────────
    logger.info("update-all: writing all sheets in parallel")
    try:
        master_result, combo_result = await asyncio.gather(
            _run_master_stock_core(db, master_sheet_id, sku_stock_map, stock_date),
            _run_amazon_combo_status_core(db, master_sheet_id),
        )
    except Exception as exc:
        logger.exception("update-all: phase 2 failed")
        raise HTTPException(status_code=500, detail=f"Sheet write failed: {exc}")

    master_updated = master_result.get("updated", 0)
    combo_updated = combo_result.get("updated", 0)

    return JSONResponse({
        "success": True,
        "message": (
            f"All sheets updated — "
            f"{master_updated} master rows, "
            f"{combo_updated} Amazon combo rows"
        ),
        "stock_date": stock_date,
        "master": master_result,
        "amazon_combo": combo_result,
    })


# ── Individual endpoints ──────────────────────────────────────────────────────

@router.post("/update-master-stock")
async def update_master_stock(
    body: MasterStockRequest = MasterStockRequest(),
    db=Depends(get_database),
):
    sheet_id = _parse_sheet_id(body.sheet_id, MASTER_SHEET_ID)

    try:
        sku_stock_map, stock_date = await asyncio.to_thread(_fetch_live_zoho_stock, db)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Live Zoho stock fetch failed: {exc}")

    result = await _run_master_stock_core(db, sheet_id, sku_stock_map, stock_date)
    return JSONResponse(result)


@router.post("/update-vendor-sheets")
async def update_vendor_sheets(
    body: VendorSheetsRequest = VendorSheetsRequest(),
    db=Depends(get_database),
):
    from .master import _generate_master_report_data

    vendor_sheet_id = _parse_sheet_id(body.sheet_id, VENDOR_SHEET_ID)
    start_date, end_date = _last_3_months_range()

    try:
        (sku_stock_map, stock_date), report = await asyncio.gather(
            asyncio.to_thread(_fetch_live_zoho_stock, db),
            _generate_master_report_data(
                start_date=start_date, end_date=end_date, db=db,
            ),
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Data fetch failed: {exc}")

    combined_data: list[dict] = report.get("combined_data", [])
    sku_data: dict[str, dict] = {}
    for item in combined_data:
        sku = item.get("sku_code", "")
        if not sku:
            continue
        metrics = item.get("combined_metrics", {})
        sku_data[sku] = {
            "total_sales":         round(metrics.get("total_sales", 0) or 0, 2),
            "total_days_in_stock": int(metrics.get("total_days_in_stock", 0) or 0),
        }

    result = await _run_vendor_sheets_core(
        db, vendor_sheet_id, sku_data, sku_stock_map, stock_date, start_date, end_date,
    )
    return JSONResponse(result)


@router.post("/update-amazon-combo-status")
async def update_amazon_combo_status(db=Depends(get_database)):
    result = await _run_amazon_combo_status_core(db, MASTER_SHEET_ID)
    return JSONResponse(result)


# ── Product images (Master / Discontinued Items sheet ↔ S3) ──────────────────

_PUBLIC_S3_BUCKET  = os.getenv("PUBLIC_S3_BUCKET", "order-form")
_PUBLIC_S3_REGION  = os.getenv("PUBLIC_S3_REGION", "ap-south-1")
_PUBLIC_S3_URL     = os.getenv("PUBLIC_S3_URL", "https://assets.pupscribe.in")
_IMAGE_SHEET_NAMES = ["Master", "Discontinued Items"]
_NUM_IMAGE_SLOTS   = 12


def _public_s3():
    import boto3
    return boto3.client(
        "s3",
        region_name=_PUBLIC_S3_REGION,
        aws_access_key_id=os.getenv("PUBLIC_S3_ACCESS_KEY", ""),
        aws_secret_access_key=os.getenv("PUBLIC_S3_SECRET_KEY", ""),
    )


class ProductImagesUpdateRequest(BaseModel):
    sheet: str = "Master"
    images: dict[str, str]  # "1" → url … "12" → url


def _read_product_images_sync(sku_code: str) -> dict | None:
    _, sh = _open_sheet(MASTER_SHEET_ID, writable=False)

    for sheet_name in _IMAGE_SHEET_NAMES:
        try:
            ws = sh.worksheet(sheet_name)
        except Exception:
            continue

        all_rows = ws.get_all_values()
        if len(all_rows) <= HEADER_ROW_INDEX:
            continue

        header       = [h.strip() for h in all_rows[HEADER_ROW_INDEX]]
        header_lower = [h.lower() for h in header]

        sku_col = next(
            (i for i, h in enumerate(header_lower) if h == "sku code (final)"), 3
        )

        img_col: dict[str, int] = {}
        for slot in range(1, _NUM_IMAGE_SLOTS + 1):
            idx = next(
                (i for i, h in enumerate(header_lower) if h == f"image url {slot}"), -1
            )
            if idx != -1:
                img_col[str(slot)] = idx

        data_rows = all_rows[DATA_START_ROW:]
        for row_idx, row in enumerate(data_rows):
            cell_sku = row[sku_col].strip() if sku_col < len(row) else ""
            if cell_sku != sku_code:
                continue

            images: dict[str, str] = {}
            for slot in range(1, _NUM_IMAGE_SLOTS + 1):
                s = str(slot)
                col = img_col.get(s, -1)
                images[s] = row[col].strip() if col != -1 and col < len(row) else ""

            return {
                "sheet": sheet_name,
                "sku_code": sku_code,
                "images": images,
                "row_number": DATA_START_ROW + 1 + row_idx,
            }

    return None


def _write_product_images_sync(sku_code: str, sheet_name: str, images: dict[str, str]) -> int:
    import gspread

    _, sh = _open_sheet(MASTER_SHEET_ID, writable=True)
    ws = sh.worksheet(sheet_name)
    all_rows = ws.get_all_values()

    if len(all_rows) <= HEADER_ROW_INDEX:
        raise HTTPException(status_code=400, detail="Sheet header row missing")

    header       = [h.strip() for h in all_rows[HEADER_ROW_INDEX]]
    header_lower = [h.lower() for h in header]
    sku_col      = next(
        (i for i, h in enumerate(header_lower) if h == "sku code (final)"), 3
    )

    img_col: dict[str, int] = {}
    for slot in range(1, _NUM_IMAGE_SLOTS + 1):
        idx = next(
            (i for i, h in enumerate(header_lower) if h == f"image url {slot}"), -1
        )
        if idx != -1:
            img_col[str(slot)] = idx

    data_rows = all_rows[DATA_START_ROW:]
    row_number: int | None = None
    for row_idx, row in enumerate(data_rows):
        if (row[sku_col].strip() if sku_col < len(row) else "") == sku_code:
            row_number = DATA_START_ROW + 1 + row_idx
            break

    if row_number is None:
        raise HTTPException(
            status_code=404, detail=f"SKU '{sku_code}' not found in sheet '{sheet_name}'"
        )

    updates = [
        {
            "range": gspread.utils.rowcol_to_a1(row_number, img_col[s] + 1),
            "values": [[url]],
        }
        for s, url in images.items()
        if s in img_col
    ]

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")

    return len(updates)


@router.get("/product-images/{sku_code}")
async def get_product_images(sku_code: str):
    result = await asyncio.to_thread(_read_product_images_sync, sku_code)
    if result is None:
        raise HTTPException(
            status_code=404, detail=f"SKU '{sku_code}' not found in Master or Discontinued Items sheet"
        )
    return result


@router.post("/product-images/upload")
async def upload_product_image(
    file: UploadFile = File(...),
    item_id: str = Form(...),
):
    import time as _time
    from botocore.exceptions import BotoCoreError, NoCredentialsError

    if not (file.content_type or "").startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image")

    file.file.seek(0, 2)
    size = file.file.tell()
    file.file.seek(0)
    if size > 10 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File exceeds 10 MB")

    ext = os.path.splitext(file.filename or "image.jpg")[1] or ".jpg"
    ts  = int(_time.time() * 1000)
    key = f"product_images/{item_id}_{ts}{ext}"

    try:
        _public_s3().upload_fileobj(
            file.file,
            _PUBLIC_S3_BUCKET,
            key,
            ExtraArgs={"ACL": "public-read", "ContentType": file.content_type},
        )
    except NoCredentialsError:
        raise HTTPException(status_code=500, detail="S3 credentials not configured")
    except BotoCoreError as exc:
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {exc}")
    finally:
        file.file.close()

    return {"url": f"{_PUBLIC_S3_URL}/{key}"}


@router.put("/product-images/{sku_code}")
async def update_product_images(sku_code: str, body: ProductImagesUpdateRequest):
    updated = await asyncio.to_thread(
        _write_product_images_sync, sku_code, body.sheet, body.images
    )
    return {"success": True, "sku_code": sku_code, "sheet": body.sheet, "cells_updated": updated}
