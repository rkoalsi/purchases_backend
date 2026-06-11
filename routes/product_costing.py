"""
routes/product_costing.py

Generate product costing Excel workbooks.

POST /product-costing/generate
    Accept brand tab configs + exchange rates → download .xlsx
    One tab per brand; columns mirror the vendor costing sheets plus
    optional live Zoho stock + 3-month sales columns appended at end.
"""

from __future__ import annotations

import asyncio
import io
import logging
import os
from datetime import datetime

import openpyxl
from openpyxl.styles import Alignment, Font, PatternFill
from fastapi import APIRouter, Depends, Form, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ..database import get_database

router = APIRouter()
logger = logging.getLogger(__name__)

# ── Column positions (1-indexed, openpyxl convention) ─────────────────────────
# These match the existing vendor costing sheet layout exactly.
COL_SR         = 1   # A
COL_STATUS     = 2   # B
COL_CODE       = 3   # C  — cf_item_code (manufacturer code)
COL_BB         = 4   # D  — cf_sku_code
COL_NAME       = 5   # E
COL_CBM        = 6   # F  — CBM per unit
COL_TOTAL_CBM  = 7   # G  — formula =F*J
COL_DUTY_PCT   = 8   # H  — Custom Duty % (user fills)
COL_HSN        = 9   # I
COL_CARTON     = 10  # J  — (user fills)
COL_QTY        = 11  # K  — formula =L
COL_CASEPACK   = 12  # L
COL_PER_PC_1   = 13  # M  — per pc foreign currency (purchase_rate)
COL_PER_PC_2   = 14  # N  — formula =M  (mirror column, matching original)
COL_FX_TOTAL   = 15  # O  — currency total = N*K
COL_CP_ACTUAL  = 16  # P  — CP Actual = Q/K
COL_COST_ACT   = 17  # Q  — Total Cost Actual = (N*$O$1)*K
COL_CP_ASS     = 18  # R  — CP Ass Val = S/K
COL_ASS_VAL    = 19  # S  — Assessed Value = Q+Q*3%
COL_FREIGHT_I  = 20  # T  — Item Freight (empty)
COL_INS_I      = 21  # U  — Item Insurance (empty)
COL_CUS_DUTY   = 22  # V  — Cus duty = S*H
COL_SURCHARGE  = 23  # W  — Surcharge = V*10%
COL_DUTY_PC    = 24  # X  — Cus Duty/PC = V/K
COL_SURCH_PC   = 25  # Y  — Surcharge/PC = W/K
COL_EMPTY_Z    = 26  # Z
COL_SHIPPING   = 27  # AA — Shipping fee = F*150*$Q$1*J
COL_PACKAGING  = 28  # AB — Packaging (user fills)
COL_ZIPTIES    = 29  # AC — Zip ties (user fills)
COL_LABELS     = 30  # AD — Labels (user fills)
# AE=31, AF=32, AG=33 intentionally blank (match original layout)
COL_GST_PCT    = 34  # AH — GST rate = 0.18
COL_LANDED     = 35  # AI — Total Landed Cost/PC = (Q+V+W+AA)/K
COL_GST_AMT    = 36  # AJ — GST Amount = (R+X+Y)*AH

# Appended live-data columns (when include_live_data=True)
COL_MRP        = 37  # AK
COL_SALES      = 38  # AL — Total Sales (3 months)
COL_DAYS       = 39  # AM — Days in Stock
COL_STOCK      = 40  # AN — Zoho Stock
COL_AVG_SALES  = 41  # AO — Avg Sales/Day = IFERROR(AL/AM, 0)
COL_DAYS_COVER = 42  # AP — Days Cover = IFERROR(AN/AO, 0)

# Exchange rate metadata positions in Row 1 (absolute refs in formulas)
META_ROW         = 1   # Row 1 — exchange rate metadata
META_BANK_LABEL  = 14  # N
META_BANK_VAL    = 15  # O  ← $O$1 in formulas
META_CUST_LABEL  = 16  # P
META_CUST_VAL    = 17  # Q  ← $Q$1 in formulas
META_FRET_LABEL  = 18  # R
META_FRET_VAL    = 19  # S

HEADER_ROW = 6
DATA_START = 7


# ── Request models ─────────────────────────────────────────────────────────────

class ExchangeRates(BaseModel):
    bank:    float = 96.0
    customs: float = 92.0
    freight: float = 92.0


class BrandTabConfig(BaseModel):
    label:           str           # Excel tab name
    brand_names:     list[str]     # products.brand values to include
    currency_label:  str  = "USD"  # shown in column headers
    currency_filter: str | None = None  # optional products.currency filter
    exchange_rates:  ExchangeRates = ExchangeRates()
    include_inactive: bool = False


class ProductCostingRequest(BaseModel):
    tabs:              list[BrandTabConfig]
    include_live_data: bool = True


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _fetch_products(db, tab: BrandTabConfig) -> list[dict]:
    query: dict = {"brand": {"$in": tab.brand_names}}
    if not tab.include_inactive:
        query["status"] = "active"
    if tab.currency_filter:
        query["currency"] = tab.currency_filter
    fields = {
        "_id": 0,
        "cf_sku_code": 1, "cf_item_code": 1,
        "item_name": 1, "brand": 1,
        "purchase_rate": 1, "rate": 1,
        "hsn_or_sac": 1, "cbm": 1, "case_pack": 1,
        "status": 1, "purchase_status": 1,
        "costing_carton": 1, "custom_duty": 1, "purchase_price": 1,
        "item_tax_preferences": 1,
    }
    return list(db.products.find(query, fields).sort([("item_name", 1)]))


# ── Excel builder ──────────────────────────────────────────────────────────────

_HEADER_FILL  = PatternFill("solid", fgColor="1F3864")
_HEADER_FONT  = Font(bold=True, color="FFFFFF", size=9)
_META_FONT    = Font(bold=True, size=9)
_DATA_FONT    = Font(size=9)


def _col(c: int) -> str:
    """Return A1 column letter for 1-indexed column number."""
    from openpyxl.utils import get_column_letter
    return get_column_letter(c)


def _set(ws, row: int, col: int, value, bold: bool = False, pct: bool = False):
    cell = ws.cell(row, col)
    cell.value = value
    cell.font = Font(bold=bold, size=9) if bold else _DATA_FONT
    if pct:
        cell.number_format = "0%"


def _gst_decimal(product: dict) -> float | None:
    """Return intra-state GST as a decimal (e.g. 0.18 for 18%), or None."""
    # Direct gst_pct field (used for uploaded templates where GST % is a plain number)
    if product.get("gst_pct") is not None:
        return float(product["gst_pct"]) / 100
    for pref in product.get("item_tax_preferences") or []:
        if pref.get("tax_specification") == "intra":
            pct = pref.get("tax_percentage")
            if pct is not None:
                return float(pct) / 100
    return None


def _build_sheet(
    wb,
    tab: BrandTabConfig,
    products: list[dict],
    sku_stock_map: dict | None,
    sku_sales_data: dict | None,
    stock_date: str,
    sales_label: str,
):
    ws = wb.create_sheet(title=tab.label[:31])
    rates = tab.exchange_rates
    cur   = tab.currency_label

    # ── Row 1: exchange rate metadata ─────────────────────────────────────────
    _set(ws, META_ROW, META_BANK_LABEL, "Conversion rate By Bank",   bold=True)
    _set(ws, META_ROW, META_BANK_VAL,   rates.bank)
    _set(ws, META_ROW, META_CUST_LABEL, "Conversion rate Customs",   bold=True)
    _set(ws, META_ROW, META_CUST_VAL,   rates.customs)
    _set(ws, META_ROW, META_FRET_LABEL, "Conversion rate(Freight)",  bold=True)
    _set(ws, META_ROW, META_FRET_VAL,   rates.freight)

    # ── Row 2: date + currency label ──────────────────────────────────────────
    _set(ws, 2, 1, "Date",  bold=True)
    _set(ws, 2, 3, datetime.now().strftime("%d-%m-%Y"))
    _set(ws, 2, 5, cur)

    # ── Row 3: brand title ────────────────────────────────────────────────────
    _set(ws, 3, 1, f"{tab.label} Latest Price", bold=True)

    # ── Row 6: column headers ─────────────────────────────────────────────────
    include_live = sku_stock_map is not None
    headers = _build_headers(cur, include_live, stock_date, sales_label)
    for c, h in enumerate(headers, 1):
        cell = ws.cell(HEADER_ROW, c)
        cell.value   = h
        cell.font    = _HEADER_FONT
        cell.fill    = _HEADER_FILL
        cell.alignment = Alignment(wrap_text=True, vertical="center")

    ws.row_dimensions[HEADER_ROW].height = 40

    # ── Data rows ─────────────────────────────────────────────────────────────
    for i, product in enumerate(products, 1):
        r = DATA_START + i - 1
        _write_row(ws, r, i, product, include_live, sku_stock_map, sku_sales_data)

    # ── Column widths ─────────────────────────────────────────────────────────
    ws.column_dimensions[_col(COL_NAME)].width   = 45
    ws.column_dimensions[_col(COL_BB)].width     = 16
    ws.column_dimensions[_col(COL_CODE)].width   = 12
    ws.column_dimensions[_col(COL_HSN)].width    = 12
    ws.column_dimensions[_col(COL_STATUS)].width = 22

    ws.freeze_panes = f"A{DATA_START}"


def _build_headers(cur: str, include_live: bool, stock_date: str, sales_label: str) -> list[str]:
    headers = [
        "Sr", "Status", "Code", "BB code", "PRODUCT NAME",
        "CBM as per PL", "Total CBM", "Custom Duty", "HSN", "Carton",
        "Quantity", "Casepack",
        f"per pc {cur}", f"per pc {cur}", f"{cur} TOTAL",
        "CP (Actual)", "Total Cost (Actual)", "CP (Ass. Val.)", "Ass. Val.",
        "Item Freight", "Item Insurance",
        "Cus duty", "Surcharge", "Cus Duty / PC", "Surcharge / PC",
        "",  # Z
        "Shipping fee, stamp, trans to ware, Agency, Handling, exami & Gst",
        "Packaging & designing", "zip ties", "labels",
        "", "", "",   # AE, AF, AG — blank (match original layout)
        "GST %", "Total Landed Cost / PC", "GST Amount",
    ]
    if include_live:
        headers += [
            "MRP",
            f"Total Sales ({sales_label})",
            "Days in Stock",
            f"Zoho Stock ({stock_date})",
            "Avg Sales / Day",
            "Days Cover",
        ]
    return headers


def _write_row(
    ws,
    r: int,
    sr: int,
    product: dict,
    include_live: bool,
    sku_stock_map: dict | None,
    sku_sales_data: dict | None,
):
    sku        = product.get("cf_sku_code", "")
    cbm        = product.get("cbm")
    case_pack  = product.get("case_pack")
    pr         = product.get("purchase_price")
    carton     = product.get("costing_carton", 1)
    duty_pct   = product.get("custom_duty")  # stored as percentage, e.g. 10.0 = 10%

    _set(ws, r, COL_SR,     sr)
    _set(ws, r, COL_STATUS, product.get("purchase_status", ""))
    _set(ws, r, COL_CODE,   product.get("cf_item_code", ""))
    _set(ws, r, COL_BB,     sku)
    _set(ws, r, COL_NAME,   product.get("item_name", ""))

    if cbm:
        _set(ws, r, COL_CBM,       round(cbm, 2))
        ws.cell(r, COL_TOTAL_CBM).value  = f"=F{r}*J{r}"
        ws.cell(r, COL_SHIPPING).value   = f"=F{r}*150*$Q$1*J{r}"

    _set(ws, r, COL_HSN, product.get("hsn_or_sac", ""))
    _set(ws, r, COL_CARTON, carton)
    if duty_pct is not None:
        _set(ws, r, COL_DUTY_PCT, duty_pct / 100, pct=True)

    if case_pack:
        _set(ws, r, COL_CASEPACK, int(case_pack))
        ws.cell(r, COL_QTY).value = f"=L{r}"

    if pr and pr > 0:
        _set(ws, r, COL_PER_PC_1, pr)
    ws.cell(r, COL_PER_PC_2).value  = f"=M{r}"
    ws.cell(r, COL_FX_TOTAL).value  = f"=N{r}*K{r}"

    # Cost formulas
    ws.cell(r, COL_COST_ACT).value  = f"=(N{r}*$O$1)*K{r}"
    ws.cell(r, COL_CP_ACTUAL).value = f"=IFERROR(Q{r}/K{r},0)"
    ws.cell(r, COL_ASS_VAL).value   = f"=Q{r}+Q{r}*3%"
    ws.cell(r, COL_CP_ASS).value    = f"=IFERROR(S{r}/K{r},0)"
    ws.cell(r, COL_CUS_DUTY).value  = f"=S{r}*H{r}"
    ws.cell(r, COL_SURCHARGE).value = f"=V{r}*10%"
    ws.cell(r, COL_DUTY_PC).value   = f"=IFERROR(V{r}/K{r},0)"
    ws.cell(r, COL_SURCH_PC).value  = f"=IFERROR(W{r}/K{r},0)"
    gst = _gst_decimal(product)
    _set(ws, r, COL_GST_PCT, gst if gst is not None else 0.18, pct=True)
    ws.cell(r, COL_LANDED).value    = f"=IFERROR((Q{r}+V{r}+W{r}+AA{r})/K{r},0)"
    ws.cell(r, COL_GST_AMT).value   = f"=(R{r}+X{r}+Y{r})*AH{r}"

    if include_live:
        _set(ws, r, COL_MRP, product.get("rate") or "")

        if sku_sales_data and sku:
            sd = sku_sales_data.get(sku, {})
            total_sales = sd.get("total_sales", "")
            days_in_stk = sd.get("total_days_in_stock", "")
            if total_sales != "":
                _set(ws, r, COL_SALES, total_sales)
            if days_in_stk != "":
                _set(ws, r, COL_DAYS, days_in_stk)

        if sku_stock_map and sku is not None:
            stock = sku_stock_map.get(sku)
            if stock is not None:
                _set(ws, r, COL_STOCK, stock)

        ws.cell(r, COL_AVG_SALES).value  = f"=IFERROR(AL{r}/AM{r},0)"
        ws.cell(r, COL_DAYS_COVER).value = f"=IFERROR(AN{r}/AO{r},0)"


def _build_workbook(
    tabs: list[BrandTabConfig],
    products_by_tab: list[list[dict]],
    sku_stock_map: dict | None,
    sku_sales_data: dict | None,
    stock_date: str,
    sales_label: str,
) -> bytes:
    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for tab, products in zip(tabs, products_by_tab):
        _build_sheet(wb, tab, products, sku_stock_map, sku_sales_data, stock_date, sales_label)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


# ── Endpoint ───────────────────────────────────────────────────────────────────

@router.post("/generate")
async def generate_product_costing(
    body: ProductCostingRequest,
    db=Depends(get_database),
):
    if not body.tabs:
        raise HTTPException(status_code=400, detail="Select at least one brand tab.")

    # Fetch products for all tabs in parallel
    products_by_tab: list[list[dict]] = await asyncio.to_thread(
        lambda: [_fetch_products(db, tab) for tab in body.tabs]
    )

    sku_stock_map: dict | None = None
    sku_sales_data: dict | None = None
    stock_date = datetime.now().strftime("%d %b %Y")
    sales_label = ""

    if body.include_live_data:
        from .sheets_updater import _fetch_live_zoho_stock, _last_3_months_range
        from ..services.master_report import _generate_master_report_data

        start_date, end_date = _last_3_months_range()
        sales_label = f"{start_date} to {end_date}"

        try:
            (sku_stock_map, stock_date), report = await asyncio.gather(
                asyncio.to_thread(_fetch_live_zoho_stock, db),
                _generate_master_report_data(
                    start_date=start_date, end_date=end_date,
                    include_zoho=True, db=db,
                ),
            )
            sku_sales_data = {}
            for item in report.get("combined_data", []):
                sku = item.get("sku_code", "")
                if sku:
                    m = item.get("combined_metrics", {})
                    sku_sales_data[sku] = {
                        "total_sales":        round(m.get("total_sales", 0) or 0, 2),
                        "total_days_in_stock": int(m.get("total_days_in_stock", 0) or 0),
                    }
        except Exception as exc:
            logger.warning("Live data fetch failed (continuing without): %s", exc)
            sku_stock_map  = None
            sku_sales_data = None

    excel_bytes = await asyncio.to_thread(
        _build_workbook,
        body.tabs, products_by_tab,
        sku_stock_map, sku_sales_data,
        stock_date, sales_label,
    )

    filename = f"product_costing_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ── Template download ──────────────────────────────────────────────────────────

_TEMPLATE_COLUMNS = [
    ("Product Name",             True),
    ("Unit Price (USD)",         True),
    ("Units per Carton",         True),
    ("CBM per Carton",           True),
    ("Custom Duty %",            True),
    ("HSN Code",                 True),
    ("GST %",                    True),
    ("Vendor SKU / Article No.", False),
    ("Unit Price (RMB)",         False),
    ("Carton Weight (kg)",       False),
    ("Notes",                    False),
]

_TEMPLATE_WIDTHS = [40, 18, 18, 16, 15, 14, 10, 24, 16, 18, 20]

_REQ_FILL = PatternFill("solid", fgColor="1F3864")
_OPT_FILL = PatternFill("solid", fgColor="4472C4")
_HDR_FONT = Font(bold=True, color="FFFFFF", size=10)


def _build_template_workbook() -> bytes:
    from openpyxl.utils import get_column_letter

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Price List"

    for col, ((name, required), width) in enumerate(zip(_TEMPLATE_COLUMNS, _TEMPLATE_WIDTHS), 1):
        cell = ws.cell(1, col, name)
        cell.font      = _HDR_FONT
        cell.fill      = _REQ_FILL if required else _OPT_FILL
        cell.alignment = Alignment(wrap_text=True, vertical="center", horizontal="center")
        ws.column_dimensions[get_column_letter(col)].width = width

    ws.row_dimensions[1].height = 30

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.read()


@router.get("/template")
async def download_template():
    excel_bytes = await asyncio.to_thread(_build_template_workbook)
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="product_costing_template.xlsx"'},
    )


# ── Template upload → costing sheet ───────────────────────────────────────────

# Map header text → product dict key
_COL_MAP = {
    "Product Name":             "item_name",
    "Unit Price (USD)":         "purchase_price",
    "Units per Carton":         "case_pack",
    "CBM per Carton":           "cbm",
    "Custom Duty %":            "custom_duty",
    "HSN Code":                 "hsn_or_sac",
    "GST %":                    "gst_pct",
    "Vendor SKU / Article No.": "cf_item_code",
    "Unit Price (RMB)":         "rmb_price",
    "Carton Weight (kg)":       "ctn_weight",
    "Notes":                    "notes",
}


def _parse_template(file_bytes: bytes) -> list[dict]:
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), read_only=True, data_only=True)
    ws = wb.active

    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    # Detect header row — first row where at least one cell matches a known column name
    header_row_idx = None
    col_indices: dict[str, int] = {}
    for ri, row in enumerate(rows):
        mapping = {}
        for ci, cell in enumerate(row):
            if cell and str(cell).strip() in _COL_MAP:
                mapping[str(cell).strip()] = ci
        if mapping:
            header_row_idx = ri
            col_indices = mapping
            break

    if header_row_idx is None:
        raise HTTPException(
            status_code=400,
            detail="Could not find header row. Make sure the file uses the standard template."
        )

    if "Product Name" not in col_indices or "Unit Price (USD)" not in col_indices:
        raise HTTPException(
            status_code=400,
            detail="Template must have at least 'Product Name' and 'Unit Price (USD)' columns."
        )

    products = []
    for row in rows[header_row_idx + 1:]:
        if not any(row):
            continue

        def _get(col_name: str):
            idx = col_indices.get(col_name)
            if idx is None or idx >= len(row):
                return None
            v = row[idx]
            return v if v not in ("", None) else None

        name = _get("Product Name")
        price_usd = _get("Unit Price (USD)")

        if not name or not isinstance(price_usd, (int, float)) or float(price_usd) <= 0:
            continue

        case_pack  = _get("Units per Carton")
        cbm        = _get("CBM per Carton")
        duty       = _get("Custom Duty %")
        hsn        = _get("HSN Code")
        gst        = _get("GST %")
        vendor_sku = _get("Vendor SKU / Article No.")

        product: dict = {
            "item_name":     str(name).strip(),
            "purchase_price": float(price_usd),
            "cf_sku_code":   "",
            "cf_item_code":  str(vendor_sku) if vendor_sku is not None else "",
            "purchase_status": "",
        }
        if case_pack is not None:
            try:
                product["case_pack"] = int(float(case_pack))
            except (ValueError, TypeError):
                pass
        if cbm is not None:
            try:
                product["cbm"] = float(cbm)
            except (ValueError, TypeError):
                pass
        if duty is not None:
            try:
                product["custom_duty"] = float(duty)
            except (ValueError, TypeError):
                pass
        if hsn is not None:
            product["hsn_or_sac"] = str(hsn)
        if gst is not None:
            try:
                product["gst_pct"] = float(gst)
            except (ValueError, TypeError):
                pass

        products.append(product)

    if not products:
        raise HTTPException(status_code=400, detail="No valid product rows found in the uploaded file.")

    return products


@router.post("/upload")
async def upload_price_list(
    file:          UploadFile = File(...),
    tab_label:     str        = Form("Uploaded"),
    currency_label: str       = Form("USD"),
    bank_rate:     float      = Form(96.0),
    customs_rate:  float      = Form(92.0),
    freight_rate:  float      = Form(92.0),
):
    content = await file.read()

    products = await asyncio.to_thread(_parse_template, content)

    tab = BrandTabConfig(
        label=tab_label,
        brand_names=[],
        currency_label=currency_label,
        exchange_rates=ExchangeRates(bank=bank_rate, customs=customs_rate, freight=freight_rate),
    )

    excel_bytes = await asyncio.to_thread(
        _build_workbook,
        [tab], [products],
        None, None,
        datetime.now().strftime("%d %b %Y"), "",
    )

    safe = tab_label.replace(" ", "_").replace("/", "-")
    filename = f"product_costing_{safe}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
