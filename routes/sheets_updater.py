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

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..database import get_database

logger = logging.getLogger(__name__)

router = APIRouter()

# ── Google Sheet IDs ──────────────────────────────────────────────────────────
MASTER_SHEET_ID    = "1bQEqJxPR_m1Y-K7Bjh74kio2ZTEWENFLoFTl1POabMY"
MASTER_SHEET_NAME  = "Master"
HEADER_ROW_INDEX   = 1   # 0-based; row 0 is empty, row 1 = header (sheet row 2)
DATA_START_ROW     = 2   # 0-based index of first data row (sheet row 3)

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
_PUPSCRIBE_WAREHOUSE_ID = "3220178000000403010"
_WAREHOUSE_URL     = (
    "https://inventory.zoho.com/api/v1/reports/warehouse"
    "?page={page}&per_page=2000&sort_column=item_name&sort_order=A"
    "&response_option=1&filter_by=TransactionDate.CustomDate"
    "&show_actual_stock=false&to_date={date1}&organization_id={org_id}"
)
_TOTAL_WAREHOUSE_URL = (
    "https://inventory.zoho.com/api/v1/reports/warehouse"
    "?page=1&per_page=2000&sort_column=item_name&sort_order=A"
    "&response_option=2&filter_by=TransactionDate.CustomDate"
    "&show_actual_stock=false&to_date={date1}&organization_id={org_id}"
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
    Fetch today's Zoho Inventory warehouse stock via the live API.
    Reads quantity_available_for_sale from the Pupscribe Enterprises Private Limited (Warehouse)
    sub-entry in each item's warehouse_stock array.
    Returns ({cf_sku_code: qty}, date_str).
    """
    import requests as _req

    date1 = datetime.now().strftime("%Y-%m-%d")
    token = _get_inventory_token()
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}

    total_resp = _req.get(
        _TOTAL_WAREHOUSE_URL.format(date1=date1, org_id=_ZOHO_ORG_ID),
        headers=headers,
        timeout=60,
    )
    total_resp.raise_for_status()
    total_pages = total_resp.json().get("page_context", {}).get("total_pages", 1)
    logger.info("Live Zoho stock: %d pages to fetch for %s", total_pages, date1)

    def _fetch_page(page: int) -> list[dict]:
        resp = _req.get(
            _WAREHOUSE_URL.format(page=page, date1=date1, org_id=_ZOHO_ORG_ID),
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json().get("warehouse_stock_info", [])

    from concurrent.futures import ThreadPoolExecutor, as_completed
    stock_by_item_id: dict[str, int] = {}
    logger.info("Live Zoho stock: fetching %d pages in parallel for %s", total_pages, date1)
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_page, page): page for page in range(1, total_pages + 1)}
        for future in as_completed(futures):
            for item in future.result():
                if not isinstance(item, dict):
                    continue
                item_id = item.get("group_name") or ""
                wh_entries = item.get("warehouse_stock", [])
                if not item_id and wh_entries:
                    item_id = wh_entries[0].get("item_id", "")
                if not item_id:
                    continue
                pupscribe_entry = next(
                    (w for w in wh_entries if str(w.get("warehouse_id", "")) == _PUPSCRIBE_WAREHOUSE_ID),
                    None,
                )
                if pupscribe_entry is None:
                    continue
                qty = int(pupscribe_entry.get("quantity_available_for_sale", 0) or 0)
                stock_by_item_id[str(item_id)] = qty

    logger.info("Live Zoho stock: %d item_ids fetched", len(stock_by_item_id))

    if not stock_by_item_id:
        return {}, date1

    products_col = db["products"]
    item_id_to_sku: dict[str, str] = {}
    for doc in products_col.find(
        {"item_id": {"$in": list(stock_by_item_id.keys())}},
        {"item_id": 1, "cf_sku_code": 1, "_id": 0},
    ):
        iid = str(doc.get("item_id", ""))
        sku = doc.get("cf_sku_code", "")
        if iid and sku:
            item_id_to_sku[iid] = sku

    sku_stock: dict[str, int] = {}
    for iid, qty in stock_by_item_id.items():
        sku = item_id_to_sku.get(iid)
        if sku:
            sku_stock[sku] = sku_stock.get(sku, 0) + qty

    logger.info("Live Zoho stock: %d SKUs resolved from %d item_ids", len(sku_stock), len(stock_by_item_id))
    return sku_stock, date1


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
                skipped_no_data += 1
                skipped_no_data_skus.append(sku)
                continue

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
    Fetch Zoho stock ONCE and run the master report ONCE (both in parallel),
    then write the master workbook and all vendor tabs in parallel.
    Fastest option when both updates are needed.
    """
    from .master import _generate_master_report_data

    master_sheet_id = _parse_sheet_id(body.master_sheet_id, MASTER_SHEET_ID)
    vendor_sheet_id = _parse_sheet_id(body.vendor_sheet_id, VENDOR_SHEET_ID)
    start_date, end_date = _last_3_months_range()

    # ── Phase 1: fetch shared data in parallel ────────────────────────────────
    logger.info("update-all: fetching Zoho stock + master report in parallel")
    try:
        (sku_stock_map, stock_date), report = await asyncio.gather(
            asyncio.to_thread(_fetch_live_zoho_stock, db),
            _generate_master_report_data(
                start_date=start_date, end_date=end_date, include_zoho=True, db=db,
            ),
        )
    except Exception as exc:
        logger.exception("update-all: phase 1 failed")
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

    logger.info(
        "update-all: Zoho stock=%d SKUs  master report=%d SKUs  stock_date=%s",
        len(sku_stock_map), len(sku_data), stock_date,
    )

    # ── Phase 2: write both workbooks in parallel ─────────────────────────────
    logger.info("update-all: writing both workbooks in parallel")
    try:
        master_result, vendor_result = await asyncio.gather(
            _run_master_stock_core(db, master_sheet_id, sku_stock_map, stock_date),
            _run_vendor_sheets_core(db, vendor_sheet_id, sku_data, sku_stock_map, stock_date, start_date, end_date),
        )
    except Exception as exc:
        logger.exception("update-all: phase 2 failed")
        raise HTTPException(status_code=500, detail=f"Sheet write failed: {exc}")

    master_updated = master_result.get("updated", 0)
    vendor_updated = sum(t.get("updated", 0) for t in vendor_result.get("tabs", []))
    tabs_ok = sum(1 for t in vendor_result.get("tabs", []) if t.get("success"))

    return JSONResponse({
        "success": True,
        "message": (
            f"Both workbooks updated — "
            f"{master_updated} master rows, "
            f"{vendor_updated} vendor rows ({tabs_ok}/{len(VENDOR_TABS)} tabs)"
        ),
        "stock_date": stock_date,
        "master": master_result,
        "vendor": vendor_result,
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
                start_date=start_date, end_date=end_date, include_zoho=True, db=db,
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
