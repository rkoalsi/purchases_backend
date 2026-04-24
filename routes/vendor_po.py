from fastapi import APIRouter, HTTPException, status, Depends, UploadFile, File, Form
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime, timedelta
from pymongo import UpdateOne
from ..database import get_database, serialize_mongo_document
import asyncio
import io
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.utils import get_column_letter
from typing import Optional
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

PO_COLLECTION = "vendor_purchase_orders"
MARGINS_COLLECTION = "vendor_margins"
PRODUCTS_COLLECTION = "products"
SKU_MAPPING_COLLECTION = "amazon_sku_mapping"
ZOHO_STOCK_COLLECTION = "zoho_warehouse_stock"
INVENTORY_COLLECTION = "amazon_vendor_inventory"
SALES_COLLECTION = "amazon_vendor_sales"

COVERAGE_DAYS = 35


# ─── helpers ──────────────────────────────────────────────────────────────────

def _extract_gst(item_tax_preferences: list) -> float:
    if not item_tax_preferences:
        return 0.0
    for pref in item_tax_preferences:
        if pref.get("tax_specification") == "intra":
            return float(pref.get("tax_percentage", 0))
    return float(item_tax_preferences[0].get("tax_percentage", 0))


def _parse_po_excel(file_bytes: bytes) -> tuple[str, str, list[dict]]:
    """Parse VC PO Excel. Returns (po_number, vendor, items)."""
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 2:
        raise ValueError("PO file has no data rows")

    # header row: PO, Vendor, Ship to location, ASIN, External Id, External Id Type,
    #             Model Number, Title, Availability, Window Type, Window start,
    #             Window end, Expected date, Quantity Requested, Expected Quantity,
    #             Unit Cost, currency
    po_number = str(rows[1][0]).strip() if rows[1][0] else ""
    vendor = str(rows[1][1]).strip() if rows[1][1] else ""

    items = []
    for row in rows[1:]:
        if not row[0]:
            continue
        items.append({
            "asin": str(row[3]).strip() if row[3] else "",
            "model_number": str(row[6]).strip() if row[6] else "",
            "title": str(row[7]).strip() if row[7] else "",
            "ship_to_location": str(row[2]).strip() if row[2] else "",
            "requested_qty": int(row[13]) if row[13] is not None else 0,
            "supply_qty": int(row[14]) if row[14] is not None else None,
            "accepted_qty": None,
            "received_qty": None,
            "etrade_unit_cost": float(row[15]) if row[15] is not None else 0.0,
        })
    return po_number, vendor, items


def _enrich_items(items: list[dict], po_number: str, db) -> tuple[list[dict], str | None]:
    """Enrich PO items with product/stock/sales data. Returns (enriched_items, inventory_date_str)."""
    asins = [it["asin"] for it in items if it["asin"]]
    model_numbers = [it["model_number"] for it in items if it["model_number"]]

    # --- batch load products by cf_sku_code (model number) ---
    products_by_model: dict[str, dict] = {}
    for p in db[PRODUCTS_COLLECTION].find(
        {"cf_sku_code": {"$in": model_numbers}},
        {"cf_sku_code": 1, "item_id": 1, "rate": 1, "item_tax_preferences": 1,
         "hsn_or_sac": 1, "purchase_status": 1}
    ):
        products_by_model[p["cf_sku_code"]] = p

    # --- batch load sku mapping for ASIN → sku_code fallback ---
    sku_map_by_asin: dict[str, str] = {}
    for m in db[SKU_MAPPING_COLLECTION].find(
        {"item_id": {"$in": asins}},
        {"item_id": 1, "sku_code": 1}
    ):
        sku_map_by_asin[m["item_id"]] = m["sku_code"]

    # fill in any missing model → product via ASIN → sku_code mapping
    extra_skus = [
        sku_map_by_asin[asin]
        for asin in asins
        if asin in sku_map_by_asin and sku_map_by_asin[asin] not in products_by_model
    ]
    if extra_skus:
        for p in db[PRODUCTS_COLLECTION].find(
            {"cf_sku_code": {"$in": extra_skus}},
            {"cf_sku_code": 1, "item_id": 1, "rate": 1, "item_tax_preferences": 1,
             "hsn_or_sac": 1, "purchase_status": 1}
        ):
            products_by_model[p["cf_sku_code"]] = p

    # build asin → product lookup
    products_by_asin: dict[str, dict] = {}
    for asin in asins:
        sku = sku_map_by_asin.get(asin, "")
        if sku and sku in products_by_model:
            products_by_asin[asin] = products_by_model[sku]

    # --- batch load zoho stock (latest date per zoho_item_id) ---
    zoho_item_ids = list({
        p["item_id"] for p in products_by_model.values() if p.get("item_id")
    })
    zoho_latest: dict[str, int] = {}
    if zoho_item_ids:
        pipeline = [
            {"$match": {"zoho_item_id": {"$in": zoho_item_ids}}},
            {"$sort": {"date": -1}},
            {"$group": {
                "_id": "$zoho_item_id",
                "warehouses": {"$first": "$warehouses"}
            }}
        ]
        for doc in db[ZOHO_STOCK_COLLECTION].aggregate(pipeline):
            warehouses = doc.get("warehouses", {})
            if isinstance(warehouses, dict):
                total = sum(v for v in warehouses.values() if isinstance(v, (int, float)))
            else:
                total = 0
            zoho_latest[doc["_id"]] = int(total)

    # --- batch load vendor margins ---
    margins_by_asin: dict[str, float] = {}
    for m in db[MARGINS_COLLECTION].find({"asin": {"$in": asins}}, {"asin": 1, "margin": 1}):
        margins_by_asin[m["asin"]] = float(m["margin"])

    # --- current stock: latest inventory date ---
    latest_inv = db[INVENTORY_COLLECTION].find_one(
        {"asin": {"$in": asins}},
        {"date": 1},
        sort=[("date", -1)]
    )
    inventory_date = latest_inv["date"] if latest_inv else None
    inventory_date_str = inventory_date.strftime("%Y-%m-%d") if inventory_date else None

    current_stock_by_asin: dict[str, int] = {}
    if inventory_date:
        for doc in db[INVENTORY_COLLECTION].find(
            {"asin": {"$in": asins}, "date": inventory_date},
            {"asin": 1, "sellableOnHandInventoryUnits": 1}
        ):
            current_stock_by_asin[doc["asin"]] = int(doc.get("sellableOnHandInventoryUnits") or 0)

    # --- open PO quantities (same ASIN, other POs, status != completed) ---
    open_po_pipeline = [
        {"$match": {"po_number": {"$ne": po_number}, "po_status": {"$nin": ["completed", "closed"]}}},
        {"$unwind": "$items"},
        {"$match": {"items.asin": {"$in": asins}}},
        {"$group": {"_id": "$items.asin", "total": {"$sum": "$items.requested_qty"}}}
    ]
    open_po_by_asin: dict[str, int] = {}
    for doc in db[PO_COLLECTION].aggregate(open_po_pipeline):
        open_po_by_asin[doc["_id"]] = int(doc["total"])

    # --- last 30 days sales ---
    thirty_days_ago = datetime.now() - timedelta(days=30)
    sales_pipeline = [
        {"$match": {"asin": {"$in": asins}, "date": {"$gte": thirty_days_ago}}},
        {"$group": {"_id": "$asin", "total_units": {"$sum": "$orderedUnits"}}}
    ]
    sales_by_asin: dict[str, int] = {}
    for doc in db[SALES_COLLECTION].aggregate(sales_pipeline):
        sales_by_asin[doc["_id"]] = int(doc["total_units"])

    # --- enrich each item ---
    enriched = []
    for item in items:
        asin = item["asin"]
        model = item["model_number"]

        product = products_by_asin.get(asin) or products_by_model.get(model) or {}
        zoho_item_id = product.get("item_id", "")

        mrp = float(product.get("rate") or 0)
        gst = _extract_gst(product.get("item_tax_preferences") or [])
        mrp_wo_gst = round(mrp / (1 + gst / 100), 2) if (mrp and gst) else mrp
        margin = margins_by_asin.get(asin, None)
        cost_price_wo_tax = round(mrp_wo_gst - mrp_wo_gst * margin, 2) if margin is not None else None
        etrade = item["etrade_unit_cost"]
        diff = round(etrade - cost_price_wo_tax, 2) if cost_price_wo_tax is not None else None
        supply_qty = item.get("supply_qty") or item["requested_qty"]
        total_cost = round(cost_price_wo_tax * supply_qty, 2) if cost_price_wo_tax is not None else None
        total_cost_gst = round(total_cost * (1 + gst / 100), 2) if (total_cost is not None and gst) else total_cost

        zoho_stock = zoho_latest.get(zoho_item_id, 0) if zoho_item_id else 0
        current_stock = current_stock_by_asin.get(asin, 0)
        open_po = open_po_by_asin.get(asin, 0)
        total_qty = current_stock + open_po

        last_30_sales = sales_by_asin.get(asin, 0)
        ads = round(last_30_sales / 30, 2)
        target_stock = round(ads * COVERAGE_DAYS, 0)
        max_allowed_qty = round(target_stock - total_qty, 0)

        if total_qty == 0:
            final_supply_qty = item["requested_qty"]
        else:
            final_supply_qty = int(round(max(0, min(float(total_qty), float(max_allowed_qty))), 0))

        enriched.append({
            **item,
            "zoho_mrp": mrp,
            "gst": gst,
            "mrp_wo_gst": mrp_wo_gst,
            "margin": margin,
            "cost_price_wo_tax": cost_price_wo_tax,
            "total_cost": total_cost,
            "total_cost_gst": total_cost_gst,
            "hsn": str(product.get("hsn_or_sac") or ""),
            "diff": diff,
            "zoho_stock": zoho_stock,
            "purchase_status": product.get("purchase_status") or "",
            "current_stock": current_stock,
            "open_po": open_po,
            "total_qty": total_qty,
            "last_30_sales": last_30_sales,
            "ads": ads,
            "coverage_days": COVERAGE_DAYS,
            "target_stock": int(target_stock),
            "max_allowed_qty": int(max_allowed_qty),
            "final_supply_qty": final_supply_qty,
        })

    return enriched, inventory_date_str


# ─── endpoints ────────────────────────────────────────────────────────────────

@router.post("/upload")
async def upload_vendor_po(
    file: UploadFile = File(...),
    po_date: str = Form(...),
    db=Depends(get_database),
):
    """Upload a Vendor Central PO Excel file and store it."""
    try:
        datetime.strptime(po_date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="po_date must be YYYY-MM-DD")

    file_bytes = await file.read()

    def _process():
        po_number, vendor, items = _parse_po_excel(file_bytes)
        if not po_number:
            raise ValueError("Could not extract PO number from file")

        existing = db[PO_COLLECTION].find_one({"po_number": po_number})
        if existing:
            raise ValueError(f"PO {po_number} already exists")

        enriched_items, inventory_date = _enrich_items(items, po_number, db)

        doc = {
            "po_number": po_number,
            "vendor": vendor,
            "po_date": po_date,
            "po_status": "pending",
            "inventory_date": inventory_date,
            "item_count": len(items),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "items": items,  # raw items stored
        }
        db[PO_COLLECTION].insert_one(doc)
        return po_number, enriched_items, inventory_date

    try:
        po_number, enriched_items, inventory_date = await asyncio.to_thread(_process)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return JSONResponse(status_code=201, content={
        "po_number": po_number,
        "inventory_date": inventory_date,
        "items": serialize_mongo_document(enriched_items),
    })


@router.get("/")
async def list_vendor_pos(db=Depends(get_database)):
    """List all purchase orders."""
    def _fetch():
        return list(db[PO_COLLECTION].find(
            {},
            {"po_number": 1, "vendor": 1, "po_date": 1, "po_status": 1,
             "item_count": 1, "created_at": 1, "_id": 0}
        ).sort("created_at", -1))

    pos = await asyncio.to_thread(_fetch)
    return serialize_mongo_document(pos)


@router.get("/{po_number}/report")
async def get_po_report(po_number: str, db=Depends(get_database)):
    """Generate enriched report for a PO."""
    def _fetch():
        doc = db[PO_COLLECTION].find_one({"po_number": po_number})
        if not doc:
            return None, None, None, None
        items = doc.get("items", [])
        enriched, inventory_date = _enrich_items(items, po_number, db)
        return doc, items, enriched, inventory_date

    doc, _, enriched, inventory_date = await asyncio.to_thread(_fetch)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"PO {po_number} not found")

    return {
        "po_number": doc["po_number"],
        "vendor": doc.get("vendor"),
        "po_date": doc["po_date"],
        "po_status": doc["po_status"],
        "inventory_date": inventory_date,
        "items": serialize_mongo_document(enriched),
    }


@router.patch("/{po_number}/status")
async def update_po_status(po_number: str, po_status: str, db=Depends(get_database)):
    valid = {"pending", "processing", "packed", "closed", "completed"}
    if po_status not in valid:
        raise HTTPException(status_code=400, detail=f"status must be one of {valid}")

    def _update():
        result = db[PO_COLLECTION].update_one(
            {"po_number": po_number},
            {"$set": {"po_status": po_status, "updated_at": datetime.now()}}
        )
        return result.matched_count

    matched = await asyncio.to_thread(_update)
    if not matched:
        raise HTTPException(status_code=404, detail=f"PO {po_number} not found")
    return {"po_number": po_number, "po_status": po_status}


@router.get("/{po_number}/download")
async def download_po_report(po_number: str, db=Depends(get_database)):
    """Download enriched PO report as Excel."""
    def _build():
        doc = db[PO_COLLECTION].find_one({"po_number": po_number})
        if not doc:
            return None, None
        enriched, inventory_date = _enrich_items(doc.get("items", []), po_number, db)
        return doc, enriched, inventory_date

    result = await asyncio.to_thread(_build)
    doc, enriched, inventory_date = result
    if doc is None:
        raise HTTPException(status_code=404, detail=f"PO {po_number} not found")

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "PO Report"

    yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
    header_font = Font(bold=True)
    center_align = Alignment(horizontal="center", vertical="center", wrap_text=True)
    thin_border = Border(
        left=Side(style="thin"), right=Side(style="thin"),
        top=Side(style="thin"), bottom=Side(style="thin")
    )

    inv_date_label = inventory_date or "Latest"
    headers = [
        ("PO Date", False), ("PO", False), ("PO Status", False), ("Ship to location", False),
        ("ASIN", True), ("Model Number", True), ("Title", True), ("Requested Qty", True),
        ("Supply Qty", False), ("Accepted Qty", False), ("Supply - Accepted", False),
        ("Received QTY", False), ("Mismatch QTY", False),
        ("Zoho MRP", True), ("GST", True), ("MRP w/o GST", True), ("Margin (%)", True),
        ("Cost Price w/o Tax", True), ("Total Cost", True), ("Total Cost with GST", True),
        ("HSN", True), ("Etrade Unit Cost", True), ("Diff", True),
        ("Zoho Stock", True), ("Status", True),
        (f"Current Stock ({inv_date_label})", True),
        ("Open PO", True), ("Total Qty", True), ("Last 30 Days Sales", True),
        ("ADS", True), ("Coverage Days", True), ("Target Stock", True),
        ("Max Allowed Qty", True), ("Final Supply Qty", True),
    ]

    for col_idx, (label, highlight) in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=label)
        cell.font = header_font
        cell.alignment = center_align
        cell.border = thin_border
        if highlight:
            cell.fill = yellow_fill

    pct_format = "0.00%"
    num_format = "#,##0.00"
    int_format = "#,##0"

    po_date_str = doc["po_date"]
    for row_idx, item in enumerate(enriched, 2):
        r = row_idx
        supply = item.get("supply_qty") or item["requested_qty"]
        accepted = item.get("accepted_qty")
        received = item.get("received_qty")

        # Static values — sourced from PO / DB
        static = {
            1:  po_date_str,                        # A  PO Date
            2:  po_number,                          # B  PO
            3:  doc["po_status"],                   # C  PO Status
            4:  item["ship_to_location"],           # D  Ship to location
            5:  item["asin"],                       # E  ASIN
            6:  item["model_number"],               # F  Model Number
            7:  item["title"],                      # G  Title
            8:  item["requested_qty"],              # H  Requested Qty
            9:  supply,                             # I  Supply Qty
            10: accepted if accepted is not None else "",   # J  Accepted Qty
            12: received if received is not None else "",   # L  Received QTY
            14: item["zoho_mrp"],                   # N  Zoho MRP
            15: item["gst"] / 100,                  # O  GST (as decimal for formula use)
            17: item["margin"] if item["margin"] is not None else "",  # Q  Margin
            21: item["hsn"],                        # U  HSN
            22: item["etrade_unit_cost"],            # V  Etrade Unit Cost
            24: item["zoho_stock"],                 # X  Zoho Stock
            25: item["purchase_status"],            # Y  Status
            26: item["current_stock"],              # Z  Current Stock
            27: item["open_po"],                    # AA Open PO
            29: item["last_30_sales"],              # AC Last 30 Days Sales
            31: item["coverage_days"],              # AE Coverage Days
        }

        # Formula values — reference other cells
        formulas = {
            11: f"=IF(J{r}=\"\",\"\",I{r}-J{r})",                          # K  Supply - Accepted
            13: f"=IF(L{r}=\"\",\"\",J{r}-L{r})",                          # M  Mismatch QTY
            16: f"=ROUND(N{r}/(1+O{r}),2)",                                 # P  MRP w/o GST
            18: f"=IF(Q{r}=\"\",\"\",ROUND(P{r}*(1-Q{r}),2))",             # R  Cost Price w/o Tax
            19: f"=IF(R{r}=\"\",\"\",ROUND(R{r}*I{r},2))",                 # S  Total Cost
            20: f"=IF(S{r}=\"\",\"\",ROUND(S{r}*(1+O{r}),2))",             # T  Total Cost w/ GST
            23: f"=IF(R{r}=\"\",\"\",V{r}-R{r})",                          # W  Diff
            28: f"=Z{r}+AA{r}",                                             # AB Total Qty
            30: f"=ROUND(AC{r}/30,2)",                                      # AD ADS
            32: f"=ROUND(AD{r}*AE{r},0)",                                   # AF Target Stock
            33: f"=AF{r}-AB{r}",                                            # AG Max Allowed Qty
            34: f"=IF(AB{r}=0,H{r},ROUND(MAX(0,MIN(AB{r},AG{r})),0))",     # AH Final Supply Qty
        }

        for col_idx in range(1, 35):
            if col_idx in formulas:
                cell = ws.cell(row=r, column=col_idx, value=formulas[col_idx])
            else:
                cell = ws.cell(row=r, column=col_idx, value=static.get(col_idx, ""))
            cell.border = thin_border
            cell.alignment = Alignment(vertical="center")

            # Number formats
            if col_idx in (14, 16, 18, 19, 20, 22):   # currency cols
                cell.number_format = num_format
            elif col_idx == 15:                         # GST as %
                cell.number_format = pct_format
            elif col_idx == 17:                         # Margin as %
                cell.number_format = pct_format
            elif col_idx == 23:                         # Diff — colour via conditional format not available, plain num
                cell.number_format = num_format
            elif col_idx in (8, 9, 10, 12, 24, 26, 27, 28, 29, 31, 32, 33, 34):
                cell.number_format = int_format

    for col_idx in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = 16
    ws.column_dimensions["G"].width = 40
    ws.row_dimensions[1].height = 40

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    filename = f"PO_Report_{po_number}_{po_date_str}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ─── margins ──────────────────────────────────────────────────────────────────

@router.get("/margins")
async def get_margins(db=Depends(get_database)):
    def _fetch():
        return list(db[MARGINS_COLLECTION].find({}, {"_id": 0}))
    margins = await asyncio.to_thread(_fetch)
    return serialize_mongo_document(margins)


@router.put("/margins/{asin}")
async def upsert_margin(asin: str, margin: float, db=Depends(get_database)):
    if not (0 <= margin <= 1):
        raise HTTPException(status_code=400, detail="margin must be between 0 and 1 (e.g. 0.35 for 35%)")

    def _upsert():
        db[MARGINS_COLLECTION].update_one(
            {"asin": asin},
            {"$set": {"asin": asin, "margin": margin, "updated_at": datetime.now()}},
            upsert=True
        )

    await asyncio.to_thread(_upsert)
    return {"asin": asin, "margin": margin}


@router.get("/margins/bulk")
async def get_margins_for_asins(asins: str, db=Depends(get_database)):
    asin_list = [a.strip() for a in asins.split(",") if a.strip()]

    def _fetch():
        return list(db[MARGINS_COLLECTION].find({"asin": {"$in": asin_list}}, {"_id": 0}))

    margins = await asyncio.to_thread(_fetch)
    return {m["asin"]: m["margin"] for m in margins}
