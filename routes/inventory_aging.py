import os
import json
import asyncio
import logging
import requests
import io
from datetime import datetime

from fastapi import APIRouter, Depends, Query, HTTPException
from fastapi.responses import StreamingResponse
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

from ..database import get_database

logger = logging.getLogger(__name__)

router = APIRouter()

ORGANIZATION_ID = os.getenv("ORGANIZATION_ID", "776755316")
BOOKS_URL = os.getenv("BOOKS_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
BOOKS_REFRESH_TOKEN = os.getenv("BOOKS_REFRESH_TOKEN")
ZOHO_BOOKS_BASE = "https://books.zoho.com/api/v3"


def _get_zoho_token() -> str:
    url = BOOKS_URL.format(
        clientId=CLIENT_ID,
        clientSecret=CLIENT_SECRET,
        grantType="refresh_token",
        books_refresh_token=BOOKS_REFRESH_TOKEN,
    )
    r = requests.post(url, timeout=30)
    r.raise_for_status()
    return r.json()["access_token"]


def _fetch_all_aging_items(token: str, to_date: str) -> list:
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    select_columns = json.dumps([
        {"field": "item_name", "group": "item"},
        {"field": "intervals", "group": "report"},
    ])
    base_params = {
        "organization_id": ORGANIZATION_ID,
        "per_page": 500,
        "interval_type": "days",
        "number_of_columns": 4,
        "interval_range": 90,
        "select_columns": select_columns,
        "to_date": to_date,
        "sort_column": "item_id",
        "sort_order": "A",
        "response_option": 1,
    }

    all_items = []
    page = 1
    while True:
        params = {**base_params, "page": page}
        resp = requests.get(
            f"{ZOHO_BOOKS_BASE}/reports/inventoryagingsummary",
            headers=headers,
            params=params,
            timeout=60,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("code", 0) != 0:
            raise Exception(f"Zoho API error: {data.get('message')}")

        items = data.get("inventory_aging_summary", [])
        all_items.extend(items)
        logger.info(f"Fetched page {page}, {len(items)} items (total so far: {len(all_items)})")

        page_ctx = data.get("page_context", {})
        if not page_ctx.get("has_more_page", False):
            break
        page += 1

    return all_items


def _extract_interval(intervals: list, label: str) -> tuple[float, float]:
    """Return (stock_on_hand, asset_value) for the given interval label."""
    qty = 0.0
    val = 0.0
    for entry in intervals:
        iv = entry.get("interval", "").strip()
        if iv == label:
            qty = entry.get("stock_on_hand", 0.0) or 0.0
        elif iv == f"Asset Value ({label})":
            val = entry.get("asset_value", 0.0) or 0.0
    return qty, val


def _apply_header_style(ws, row: int, col_count: int, fill_hex: str):
    fill = PatternFill("solid", fgColor=fill_hex)
    bold_font = Font(bold=True, color="FFFFFF")
    center = Alignment(horizontal="center", vertical="center")
    thin = Side(style="thin", color="CCCCCC")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for col in range(1, col_count + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = fill
        cell.font = bold_font
        cell.alignment = center
        cell.border = border


def _apply_row_style(ws, row: int, col_count: int, alt: bool):
    fill = PatternFill("solid", fgColor="F5F5F5" if alt else "FFFFFF")
    thin = Side(style="thin", color="DDDDDD")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)
    for col in range(1, col_count + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = fill
        cell.border = border
        cell.alignment = Alignment(vertical="center")


def _auto_width(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                max_len = max(max_len, len(str(cell.value or "")))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max_len + 4, 40)


def _build_item_sheet(ws, title: str, interval_label: str, rows: list, fill_hex: str):
    headers = [
        "Item ID", "Item Name", "Brand",
        interval_label, f"Asset Value ({interval_label})",
        "MRP", "Total MRP Value", "Collection Value",
    ]
    ws.append(headers)
    _apply_header_style(ws, 1, len(headers), fill_hex)
    ws.row_dimensions[1].height = 20

    for i, row in enumerate(rows, start=2):
        qty = row["qty"]
        asset_val = row["asset_value"]
        mrp = row["mrp"]

        ws.append([
            row["item_id"],
            row["item_name"],
            row["brand"],
            qty,
            asset_val,
            mrp if mrp else "",
            f"=D{i}*F{i}" if mrp else "",
            f"=G{i}/2" if mrp else "",
        ])
        _apply_row_style(ws, i, len(headers), i % 2 == 0)

    _auto_width(ws)


def _build_brand_sheet(ws, brand_totals: dict, stock_date: str):
    # Stock date label placed in column F (away from the table)
    ws.cell(row=1, column=6, value="Zoho Stock Date")
    ws.cell(row=1, column=6).font = Font(bold=True)
    ws.cell(row=2, column=6, value=stock_date)
    ws.column_dimensions["F"].width = 20

    headers = ["Brand", "Zoho Stock", "Total MRP Value", "Collection Value"]
    ws.append(headers)
    header_row = ws.max_row
    _apply_header_style(ws, header_row, len(headers), "1F5C99")
    ws.row_dimensions[header_row].height = 20
    start_data_row = header_row + 1

    for brand, data in sorted(brand_totals.items()):
        stock = round(data["stock"], 2)
        tmrp = round(data["total_mrp"], 2)
        ws.append([brand, stock, tmrp, None])
        row_num = ws.max_row
        ws.cell(row=row_num, column=4).value = f"=C{row_num}/2"
        _apply_row_style(ws, row_num, len(headers), row_num % 2 == 0)

    total_row = ws.max_row + 1
    end_data_row = total_row - 1
    ws.cell(row=total_row, column=1, value="TOTAL")
    ws.cell(row=total_row, column=2).value = f"=SUM(B{start_data_row}:B{end_data_row})"
    ws.cell(row=total_row, column=3).value = f"=SUM(C{start_data_row}:C{end_data_row})"
    ws.cell(row=total_row, column=4).value = f"=SUM(D{start_data_row}:D{end_data_row})"
    bold = Font(bold=True)
    for col in range(1, 5):
        ws.cell(row=total_row, column=col).font = bold

    _auto_width(ws)


@router.get("/download")
async def download_inventory_aging(
    to_date: str = Query(..., description="Report end date in YYYY-MM-DD format"),
    db=Depends(get_database),
):
    # Validate date
    try:
        datetime.strptime(to_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="to_date must be YYYY-MM-DD")

    # Fetch Zoho data
    token = await asyncio.to_thread(_get_zoho_token)
    raw_items = await asyncio.to_thread(_fetch_all_aging_items, token, to_date)

    if not raw_items:
        raise HTTPException(status_code=404, detail="No inventory aging data returned from Zoho")

    # Batch-load products (for slow/dead rows) and all brands in parallel
    all_item_ids = [item["item_id"] for item in raw_items]
    products_col = db["products"]
    brands_col = db["brands"]
    stock_col = db["zoho_warehouse_stock"]

    def _load_products():
        return list(products_col.find(
            {"item_id": {"$in": all_item_ids}},
            {"item_id": 1, "brand": 1, "rate": 1},
        ))

    def _load_brands():
        return list(brands_col.find({}, {"name": 1}))

    def _find_stock_date():
        rec = stock_col.find_one(
            {},
            sort=[("date", -1)],
            projection={"date": 1},
        )
        return rec["date"] if rec else None

    product_docs, brand_docs, stock_date = await asyncio.gather(
        asyncio.to_thread(_load_products),
        asyncio.to_thread(_load_brands),
        asyncio.to_thread(_find_stock_date),
    )

    product_map = {p["item_id"]: p for p in product_docs}

    # Build brand_totals from zoho_warehouse_stock on the nearest date <= to_date
    brand_names = [b["name"] for b in brand_docs if b.get("name")]
    brand_totals: dict[str, dict] = {
        name: {"stock": 0.0, "total_mrp": 0.0, "collection_value": 0.0}
        for name in brand_names
    }
    stock_date_str = stock_date.strftime("%Y-%m-%d") if stock_date else to_date

    if stock_date:
        def _load_stock_records():
            return list(stock_col.find(
                {"date": stock_date},
                {"zoho_item_id": 1, "warehouses": 1},
            ))

        def _load_all_products_for_brands():
            return list(products_col.find(
                {},
                {"item_id": 1, "brand": 1, "rate": 1},
            ))

        stock_records, all_product_docs = await asyncio.gather(
            asyncio.to_thread(_load_stock_records),
            asyncio.to_thread(_load_all_products_for_brands),
        )

        all_product_map = {p["item_id"]: p for p in all_product_docs}

        for rec in stock_records:
            zoho_id = rec.get("zoho_item_id")
            warehouses = rec.get("warehouses") or {}
            total_stock = sum(v for v in warehouses.values() if isinstance(v, (int, float)))
            if total_stock <= 0:
                continue
            prod = all_product_map.get(zoho_id, {})
            brand = (prod.get("brand") or "").strip()
            mrp = prod.get("rate") or 0.0
            if brand not in brand_totals:
                continue
            total_mrp_val = total_stock * mrp
            brand_totals[brand]["stock"] += total_stock
            brand_totals[brand]["total_mrp"] += total_mrp_val
            brand_totals[brand]["collection_value"] += total_mrp_val / 2

    # Build slow/dead rows from Zoho aging API data
    slow_rows = []
    dead_rows = []

    for item in raw_items:
        item_id = item["item_id"]
        item_name = item["item_name"]
        intervals = item.get("intervals", [])
        prod = product_map.get(item_id, {})
        brand = (prod.get("brand") or "Unknown").strip() or "Unknown"
        mrp = prod.get("rate") or 0.0

        slow_qty, slow_val = _extract_interval(intervals, "181 - 270 days")
        dead_qty, dead_val = _extract_interval(intervals, "> 270 days")

        base = {"item_id": item_id, "item_name": item_name, "brand": brand, "mrp": mrp}

        if slow_qty > 0:
            slow_rows.append({**base, "qty": slow_qty, "asset_value": slow_val})

        if dead_qty > 0:
            dead_rows.append({**base, "qty": dead_qty, "asset_value": dead_val})

    # Sort by brand then item_name
    slow_rows.sort(key=lambda r: (r["brand"], r["item_name"]))
    dead_rows.sort(key=lambda r: (r["brand"], r["item_name"]))

    # Build XLSX
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    ws_slow = wb.create_sheet("Slow Movers")
    _build_item_sheet(ws_slow, "Slow Movers", "181 - 270 days", slow_rows, "C0392B")

    ws_dead = wb.create_sheet("Deadstock")
    _build_item_sheet(ws_dead, "Deadstock", "> 270 days", dead_rows, "7D3C98")

    ws_brand = wb.create_sheet("Brand wise collection value")
    _build_brand_sheet(ws_brand, brand_totals, stock_date_str)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    filename = f"Inventory_Aging_{to_date}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
