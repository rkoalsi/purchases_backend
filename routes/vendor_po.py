from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime, timedelta
from ..database import get_database, serialize_mongo_document
import asyncio
import io
import zipfile
from decimal import Decimal, ROUND_HALF_UP
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
            "supply_qty": None,     # will be set to final_supply_qty after enrichment
            "accepted_qty": 0,
            "received_qty": None,
            "etrade_unit_cost": float(row[15]) if row[15] is not None else 0.0,
        })
    return po_number, vendor, items


def _enrich_items(
    items: list[dict],
    po_number: str,
    db,
    use_stored_stock: bool = False,
    po_date_str: str | None = None,
) -> tuple[list[dict], str | None, str | None]:
    """Enrich PO items with product/stock/sales data. Returns (enriched_items, inventory_date_str, zoho_stock_date_str).

    use_stored_stock=True: zoho_stock/current_stock/open_po/last_30_sales are read from the stored
    item fields (frozen at upload time). Only margin/product-derived fields are recomputed.
    use_stored_stock=False: all data is fetched live.
      - If po_date_str provided: zoho_stock and current_stock are fetched from T-2 (po_date - 2 days).
      - supply_qty is set to final_supply_qty.
    """
    asins = [it["asin"] for it in items if it["asin"]]
    model_numbers = [it["model_number"] for it in items if it["model_number"]]

    # detect old-format items that predate stock snapshotting — fall back to live fetch
    has_stored_stock = items and "zoho_stock" in items[0]
    effective_use_stored = use_stored_stock and has_stored_stock

    # zoho_cutoff = PO date (Zoho data is available same-day)
    # stock_cutoff = T-2 from PO date (Amazon vendor inventory has a 2-day lag)
    zoho_cutoff: datetime | None = None
    stock_cutoff: datetime | None = None
    if po_date_str and not effective_use_stored:
        try:
            po_date_dt = datetime.strptime(po_date_str, "%Y-%m-%d")
            zoho_cutoff = po_date_dt
            stock_cutoff = po_date_dt - timedelta(days=2)
        except ValueError:
            pass

    # --- batch load products by cf_sku_code (always fresh — MRP/GST/HSN can change) ---
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

    products_by_asin: dict[str, dict] = {}
    for asin in asins:
        sku = sku_map_by_asin.get(asin, "")
        if sku and sku in products_by_model:
            products_by_asin[asin] = products_by_model[sku]

    # --- batch load vendor margins (always fresh — editable after upload) ---
    margins_by_asin: dict[str, float] = {}
    cost_prices_by_asin: dict[str, float] = {}
    etrade_asp_by_asin: dict[str, float] = {}
    for m in db[MARGINS_COLLECTION].find({"asin": {"$in": asins}}, {"asin": 1, "margin": 1, "cost_price_wo_tax": 1, "etrade_asp": 1}):
        if m.get("margin") is not None:
            margins_by_asin[m["asin"]] = float(m["margin"])
        if m.get("cost_price_wo_tax") is not None:
            cost_prices_by_asin[m["asin"]] = float(m["cost_price_wo_tax"])
        if m.get("etrade_asp") is not None:
            etrade_asp_by_asin[m["asin"]] = float(m["etrade_asp"])

    # --- live stock/sales — only fetched at upload time (or for old-format items) ---
    zoho_latest: dict[str, int] = {}
    current_stock_by_asin: dict[str, int] = {}
    open_po_by_asin: dict[str, int] = {}
    sales_by_asin: dict[str, int] = {}
    inventory_date_str: str | None = None
    zoho_stock_date_str: str | None = None

    if not effective_use_stored:
        zoho_item_ids = list({
            p["item_id"] for p in products_by_model.values() if p.get("item_id")
        })
        if zoho_item_ids:
            zoho_match: dict = {"zoho_item_id": {"$in": zoho_item_ids}}
            if zoho_cutoff:
                zoho_match["date"] = {"$lte": zoho_cutoff}
            for doc in db[ZOHO_STOCK_COLLECTION].aggregate([
                {"$match": zoho_match},
                {"$sort": {"date": -1}},
                {"$group": {"_id": "$zoho_item_id", "warehouses": {"$first": "$warehouses"}, "date": {"$first": "$date"}}}
            ]):
                wh = doc.get("warehouses", {})
                zoho_latest[doc["_id"]] = int(sum(v for v in wh.values() if isinstance(v, (int, float)))) if isinstance(wh, dict) else 0
                if doc.get("date"):
                    d = doc["date"].strftime("%Y-%m-%d") if hasattr(doc["date"], "strftime") else str(doc["date"])[:10]
                    if zoho_stock_date_str is None or d > zoho_stock_date_str:
                        zoho_stock_date_str = d

        inv_match: dict = {"asin": {"$in": asins}}
        if stock_cutoff:
            # use $lt next-day midnight so records stored at any time on T-2 are included
            inv_match["date"] = {"$lt": stock_cutoff + timedelta(days=1)}
        latest_inv = db[INVENTORY_COLLECTION].find_one(
            inv_match, {"date": 1}, sort=[("date", -1)]
        )
        inventory_date = latest_inv["date"] if latest_inv else None
        inventory_date_str = inventory_date.strftime("%Y-%m-%d") if inventory_date else None
        if inventory_date:
            for doc in db[INVENTORY_COLLECTION].find(
                {"asin": {"$in": asins}, "date": inventory_date},
                {"asin": 1, "sellableOnHandInventoryUnits": 1}
            ):
                current_stock_by_asin[doc["asin"]] = int(doc.get("sellableOnHandInventoryUnits") or 0)

        # open PO: processing → supply_qty; packed/closed/intransit → accepted_qty
        for doc in db[PO_COLLECTION].aggregate([
            {"$match": {"po_number": {"$ne": po_number}, "po_status": {"$in": ["processing", "packed", "closed", "intransit"]}}},
            {"$unwind": "$items"},
            {"$match": {"items.asin": {"$in": asins}}},
            {"$group": {
                "_id": "$items.asin",
                "total": {"$sum": {"$cond": {
                    "if": {"$eq": ["$po_status", "processing"]},
                    "then": {"$ifNull": ["$items.supply_qty", "$items.requested_qty"]},
                    "else": {"$ifNull": ["$items.accepted_qty", 0]}
                }}}
            }}
        ]):
            open_po_by_asin[doc["_id"]] = int(doc["total"])

        # Sales: strictly 30 days ending at T-2 (stock_cutoff). Inclusive window: [T-2-29, T-2] = 30 days.
        if stock_cutoff:
            sales_end = stock_cutoff
        else:
            sales_end = datetime.now()
        sales_start = sales_end - timedelta(days=29)
        for doc in db[SALES_COLLECTION].aggregate([
            {"$match": {"asin": {"$in": asins}, "date": {"$gte": sales_start, "$lte": sales_end}}},
            {"$group": {"_id": "$asin", "total_units": {"$sum": "$orderedUnits"}}}
        ]):
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
        etrade_asp = etrade_asp_by_asin.get(asin)
        # MRP w/o GST is based on eTrade ASP if available, else falls back to Zoho MRP
        asp_base = etrade_asp if etrade_asp is not None else mrp
        mrp_wo_gst = round(asp_base / (1 + gst / 100), 2) if (asp_base and gst) else asp_base
        margin = margins_by_asin.get(asin, None)
        # Use directly stored cost_price_wo_tax if available, else compute from margin
        if asin in cost_prices_by_asin:
            cost_price_wo_tax = cost_prices_by_asin[asin]
        elif margin is not None:
            cost_price_wo_tax = round(mrp_wo_gst - mrp_wo_gst * margin, 2)
        else:
            cost_price_wo_tax = None
        etrade = item.get("etrade_unit_cost", 0)
        diff = round(etrade - cost_price_wo_tax, 2) if cost_price_wo_tax is not None else None

        accepted_qty = item.get("accepted_qty") or 0
        received_qty = item.get("received_qty")

        if effective_use_stored:
            zoho_stock = item.get("zoho_stock", 0)
            current_stock = item.get("current_stock", 0)
            open_po = item.get("open_po", 0)
            last_30_sales = item.get("last_30_sales", 0)
            sv = item.get("supply_qty")
            supply_qty = sv if sv is not None else item["requested_qty"]
        else:
            zoho_stock = zoho_latest.get(zoho_item_id, 0) if zoho_item_id else 0
            current_stock = current_stock_by_asin.get(asin, 0)
            open_po = open_po_by_asin.get(asin, 0)
            last_30_sales = sales_by_asin.get(asin, 0)
            # compute final_supply_qty and use it as supply_qty
            # Use round-half-up (math.floor(x+0.5)) to match Excel's ROUND() behaviour.
            # Python's built-in round() uses banker's rounding (round-half-to-even) which
            # diverges from Excel on .5 boundaries (e.g. 10.5 → Python=10, Excel=11).
            total_qty_tmp = current_stock + open_po
            ads_tmp = round(last_30_sales / 30, 2)
            target_tmp = int((Decimal(str(ads_tmp)) * Decimal(COVERAGE_DAYS)).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
            max_allowed_tmp = target_tmp - total_qty_tmp
            if total_qty_tmp == 0:
                supply_qty = item["requested_qty"]
            else:
                supply_qty = max(0, min(int(item["requested_qty"]), int(max_allowed_tmp)))

        total_cost = round(cost_price_wo_tax * supply_qty, 2) if cost_price_wo_tax is not None else None
        total_cost_gst = round(total_cost * (1 + gst / 100), 2) if (total_cost is not None and gst) else total_cost

        total_qty = current_stock + open_po
        ads = round(last_30_sales / 30, 2)
        target_stock = int((Decimal(str(ads)) * Decimal(COVERAGE_DAYS)).quantize(Decimal('1'), rounding=ROUND_HALF_UP))
        max_allowed_qty = target_stock - total_qty

        enriched.append({
            **item,
            "supply_qty": supply_qty,
            "accepted_qty": accepted_qty,
            "received_qty": received_qty,
            "zoho_mrp": mrp,
            "etrade_asp": etrade_asp_by_asin.get(asin),
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
            "final_supply_qty": supply_qty,   # final = supply (they are always equal)
        })

    return enriched, inventory_date_str, zoho_stock_date_str


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
            current_status = existing.get("po_status", "pending")
            if current_status not in {"pending", "processing", "packed", "closed"}:
                raise ValueError(
                    f"PO {po_number} already exists with status '{current_status}' which cannot be overwritten"
                )
            # Re-enrich with fresh data, then preserve accepted_qty/received_qty per item
            enriched_items, inventory_date, zoho_stock_date = _enrich_items(
                items, po_number, db, po_date_str=po_date
            )
            existing_by_asin = {it["asin"]: it for it in existing.get("items", [])}
            for item in enriched_items:
                prev = existing_by_asin.get(item["asin"], {})
                item["accepted_qty"] = prev.get("accepted_qty", item.get("accepted_qty", 0))
                item["received_qty"] = prev.get("received_qty", item.get("received_qty"))
            db[PO_COLLECTION].update_one(
                {"po_number": po_number},
                {"$set": {
                    "vendor": vendor,
                    "po_date": po_date,
                    "po_status": current_status,
                    "inventory_date": inventory_date,
                    "zoho_stock_date": zoho_stock_date,
                    "item_count": len(items),
                    "updated_at": datetime.now(),
                    "items": enriched_items,
                }}
            )
            return po_number, enriched_items, inventory_date, zoho_stock_date

        enriched_items, inventory_date, zoho_stock_date = _enrich_items(items, po_number, db, po_date_str=po_date)

        doc = {
            "po_number": po_number,
            "vendor": vendor,
            "po_date": po_date,
            "po_status": "pending",
            "inventory_date": inventory_date,
            "zoho_stock_date": zoho_stock_date,
            "item_count": len(items),
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
            "items": enriched_items,  # enriched items stored (stock/sales frozen at upload time)
        }
        db[PO_COLLECTION].insert_one(doc)
        return po_number, enriched_items, inventory_date, zoho_stock_date

    try:
        po_number, enriched_items, inventory_date, zoho_stock_date = await asyncio.to_thread(_process)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return JSONResponse(status_code=201, content={
        "po_number": po_number,
        "inventory_date": inventory_date,
        "zoho_stock_date": zoho_stock_date,
        "items": serialize_mongo_document(enriched_items),
    })


@router.get("/")
async def list_vendor_pos(db=Depends(get_database)):
    """List all purchase orders with total requested/accepted/received qty sums."""
    def _fetch():
        pipeline = [
            {"$addFields": {
                "total_requested_qty": {"$sum": "$items.requested_qty"},
                "total_accepted_qty": {"$sum": "$items.accepted_qty"},
                "total_received_qty": {"$ifNull": ["$received_qty", {"$sum": "$items.received_qty"}]},
            }},
            {"$project": {
                "po_number": 1, "vendor": 1, "po_date": 1, "po_status": 1,
                "item_count": 1, "created_at": 1,
                "total_requested_qty": 1, "total_accepted_qty": 1, "total_received_qty": 1,
                "_id": 0,
            }},
            {"$sort": {"po_date": -1}},
        ]
        return list(db[PO_COLLECTION].aggregate(pipeline))

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
        po_status = doc.get("po_status", "pending")
        # frozen statuses + processing: serve stored snapshot; pending: re-fetch live with T-2 cutoff
        use_stored = po_status in FREEZE_ON_STATUS
        enriched, inv_date, zoho_date = _enrich_items(items, po_number, db, use_stored_stock=use_stored, po_date_str=doc.get("po_date"))
        return doc, enriched, inv_date, zoho_date

    doc, enriched, live_inv_date, live_zoho_date = await asyncio.to_thread(_fetch)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"PO {po_number} not found")

    po_status = doc["po_status"]
    # For new-format frozen POs, live_inv_date is None (stored snapshot used).
    # For old-format frozen POs (no zoho_stock in items), live_inv_date is freshly computed
    # with T-2 cutoff — prefer it over the stale stored value.
    inv_date = live_inv_date if live_inv_date is not None else doc.get("inventory_date")
    zoho_date = live_zoho_date if live_zoho_date is not None else doc.get("zoho_stock_date")

    return {
        "po_number": doc["po_number"],
        "vendor": doc.get("vendor"),
        "po_date": doc["po_date"],
        "po_status": po_status,
        "inventory_date": inv_date,
        "zoho_stock_date": zoho_date,
        "po_update_date": doc.get("po_update_date"),
        "items": serialize_mongo_document(enriched),
    }


VALID_STATUSES = {"pending", "processing", "packed", "closed", "intransit", "delivered", "completed"}
FROZEN_STATUSES = {"packed", "closed", "intransit", "delivered", "completed"}
# Processing also triggers a freeze — stock/sales are locked when PO moves to processing
FREEZE_ON_STATUS = FROZEN_STATUSES | {"processing"}


@router.patch("/{po_number}/status")
async def update_po_status(po_number: str, po_status: str, db=Depends(get_database)):
    if po_status not in VALID_STATUSES:
        raise HTTPException(status_code=400, detail=f"status must be one of {VALID_STATUSES}")

    def _update():
        doc = db[PO_COLLECTION].find_one({"po_number": po_number})
        if not doc:
            return None

        update_fields: dict = {"po_status": po_status, "updated_at": datetime.now()}

        # Transitioning into processing or a frozen status: re-enrich with live T-2 data and freeze permanently
        if po_status in FREEZE_ON_STATUS and doc.get("po_status") not in FREEZE_ON_STATUS:
            enriched, inv_date, zoho_date = _enrich_items(
                doc.get("items", []), po_number, db,
                use_stored_stock=False, po_date_str=doc.get("po_date")
            )
            update_fields["items"] = enriched
            update_fields["inventory_date"] = inv_date
            update_fields["zoho_stock_date"] = zoho_date
            if po_status == "processing":
                update_fields["po_update_date"] = datetime.now().strftime("%Y-%m-%d")

        db[PO_COLLECTION].update_one({"po_number": po_number}, {"$set": update_fields})
        return po_status

    result = await asyncio.to_thread(_update)
    if result is None:
        raise HTTPException(status_code=404, detail=f"PO {po_number} not found")
    return {"po_number": po_number, "po_status": po_status}


@router.patch("/{po_number}/items/{asin}/accepted_qty")
async def update_item_accepted_qty(po_number: str, asin: str, accepted_qty: int, db=Depends(get_database)):
    """Update the accepted quantity for a specific item in a PO."""
    if accepted_qty < 0:
        raise HTTPException(status_code=400, detail="accepted_qty must be >= 0")

    def _update():
        result = db[PO_COLLECTION].update_one(
            {"po_number": po_number, "items.asin": asin},
            {"$set": {"items.$.accepted_qty": accepted_qty, "updated_at": datetime.now()}}
        )
        return result.matched_count

    matched = await asyncio.to_thread(_update)
    if not matched:
        raise HTTPException(status_code=404, detail=f"PO {po_number} / ASIN {asin} not found")
    return {"po_number": po_number, "asin": asin, "accepted_qty": accepted_qty}


@router.patch("/{po_number}/items/{asin}/received_qty")
async def update_item_received_qty(po_number: str, asin: str, received_qty: int, db=Depends(get_database)):
    """Update the received quantity for a specific item in a PO."""
    if received_qty < 0:
        raise HTTPException(status_code=400, detail="received_qty must be >= 0")

    def _update():
        result = db[PO_COLLECTION].update_one(
            {"po_number": po_number, "items.asin": asin},
            {"$set": {"items.$.received_qty": received_qty, "updated_at": datetime.now()}}
        )
        return result.matched_count

    matched = await asyncio.to_thread(_update)
    if not matched:
        raise HTTPException(status_code=404, detail=f"PO {po_number} / ASIN {asin} not found")
    return {"po_number": po_number, "asin": asin, "received_qty": received_qty}


@router.patch("/{po_number}/received_qty")
async def update_po_received_qty(po_number: str, received_qty: int, db=Depends(get_database)):
    """Set total received quantity at the PO level (overrides per-item sum in list view)."""
    if received_qty < 0:
        raise HTTPException(status_code=400, detail="received_qty must be >= 0")

    def _update():
        result = db[PO_COLLECTION].update_one(
            {"po_number": po_number},
            {"$set": {"received_qty": received_qty, "updated_at": datetime.now()}}
        )
        return result.matched_count

    matched = await asyncio.to_thread(_update)
    if not matched:
        raise HTTPException(status_code=404, detail=f"PO {po_number} not found")
    return {"po_number": po_number, "received_qty": received_qty}


@router.patch("/{po_number}/items/{asin}/etrade_unit_cost")
async def update_item_etrade_unit_cost(po_number: str, asin: str, etrade_unit_cost: float, db=Depends(get_database)):
    """Update the eTrade unit cost for a specific item and recompute diff."""
    if etrade_unit_cost < 0:
        raise HTTPException(status_code=400, detail="etrade_unit_cost must be >= 0")

    def _update():
        doc = db[PO_COLLECTION].find_one(
            {"po_number": po_number, "items.asin": asin},
            {"items.$": 1}
        )
        if not doc or not doc.get("items"):
            return False, None
        cost = doc["items"][0].get("cost_price_wo_tax")
        new_diff = round(etrade_unit_cost - cost, 2) if cost is not None else None
        db[PO_COLLECTION].update_one(
            {"po_number": po_number, "items.asin": asin},
            {"$set": {
                "items.$.etrade_unit_cost": etrade_unit_cost,
                "items.$.diff": new_diff,
                "updated_at": datetime.now(),
            }}
        )
        return True, new_diff

    found, new_diff = await asyncio.to_thread(_update)
    if not found:
        raise HTTPException(status_code=404, detail=f"PO {po_number} / ASIN {asin} not found")
    return {"po_number": po_number, "asin": asin, "etrade_unit_cost": etrade_unit_cost, "diff": new_diff}


def _build_po_excel(doc: dict, enriched: list) -> bytes:
    """Build PO report Excel workbook and return bytes."""
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

    inv_date_label = doc.get("inventory_date") or "Latest"
    headers = [
        ("PO Date", False), ("PO", False), ("PO Status", False), ("Ship to location", False),
        ("ASIN", True), ("Model Number", True), ("Title", True), ("Requested Qty", True),
        ("Supply Qty", False), ("Accepted Qty", False), ("Supply - Accepted", False),
        ("Received QTY", False), ("Mismatch QTY", False),
        ("Zoho MRP", True), ("eTrade ASP", True), ("GST", True), ("MRP w/o GST", True), ("Margin (%)", True),
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

    po_number = doc["po_number"]
    po_date_str = doc["po_date"]
    for row_idx, item in enumerate(enriched, 2):
        r = row_idx
        accepted = item.get("accepted_qty")
        received = item.get("received_qty")

        # Static values — sourced from PO / DB
        # Col 15 = eTrade ASP inserted after Zoho MRP; all cols from old-15 shift +1
        # Col 9 (I = Supply Qty) is a formula =AI{r} so it stays in sync with Final Supply Qty
        static = {
            1:  po_date_str,                        # A  PO Date
            2:  po_number,                          # B  PO
            3:  doc["po_status"],                   # C  PO Status
            4:  item["ship_to_location"],           # D  Ship to location
            5:  item["asin"],                       # E  ASIN
            6:  item["model_number"],               # F  Model Number
            7:  item["title"],                      # G  Title
            8:  item["requested_qty"],              # H  Requested Qty
            10: accepted if accepted is not None else "",   # J  Accepted Qty
            12: received if received is not None else "",   # L  Received QTY
            14: item["zoho_mrp"],                   # N  Zoho MRP
            15: item.get("etrade_asp") if item.get("etrade_asp") is not None else "",  # O  eTrade ASP
            16: item["gst"] / 100,                  # P  GST (as decimal for formula use)
            18: item["margin"] if item["margin"] is not None else "",  # R  Margin
            22: item["hsn"],                        # V  HSN
            23: item["etrade_unit_cost"],            # W  Etrade Unit Cost
            25: item["zoho_stock"],                 # Y  Zoho Stock
            26: item["purchase_status"],            # Z  Status
            27: item["current_stock"],              # AA Current Stock
            28: item["open_po"],                    # AB Open PO
            30: item["last_30_sales"],              # AD Last 30 Days Sales
            32: item["coverage_days"],              # AF Coverage Days
        }

        # Formula values — reference other cells
        # P=GST, Q=MRP w/o GST, R=Margin, S=Cost Price, T=Total Cost, U=Total Cost GST
        # V=HSN, W=Etrade Unit Cost, X=Diff, Y=Zoho Stock, Z=Status
        # AA=Current Stock, AB=Open PO, AC=Total Qty, AD=Sales, AE=ADS, AF=Coverage
        # AG=Target Stock, AH=Max Allowed, AI=Final Supply
        formulas = {
            9:  f"=AI{r}",                                                  # I  Supply Qty = Final Supply Qty
            11: f"=IF(J{r}=\"\",\"\",I{r}-J{r})",                          # K  Supply - Accepted
            13: f"=IF(L{r}=\"\",\"\",J{r}-L{r})",                          # M  Mismatch QTY
            17: f"=IF(O{r}=\"\",ROUND(N{r}/(1+P{r}),2),ROUND(O{r}/(1+P{r}),2))",  # Q  MRP w/o GST (eTrade ASP if set, else Zoho MRP)
            19: f"=IF(R{r}=\"\",\"\",ROUND(Q{r}*(1-R{r}),2))",             # S  Cost Price w/o Tax
            20: f"=IF(S{r}=\"\",\"\",ROUND(S{r}*I{r},2))",                 # T  Total Cost
            21: f"=IF(T{r}=\"\",\"\",ROUND(T{r}*(1+P{r}),2))",             # U  Total Cost w/ GST
            24: f"=IF(S{r}=\"\",\"\",W{r}-S{r})",                          # X  Diff
            29: f"=AA{r}+AB{r}",                                            # AC Total Qty
            31: f"=ROUND(AD{r}/30,2)",                                      # AE ADS
            33: f"=ROUND(AE{r}*AF{r},0)",                                   # AG Target Stock
            34: f"=AG{r}-AC{r}",                                            # AH Max Allowed Qty
            35: f"=IF(AC{r}=0,H{r},ROUND(MAX(0,MIN(H{r},AH{r})),0))",      # AI Final Supply Qty
        }

        for col_idx in range(1, 36):
            if col_idx in formulas:
                cell = ws.cell(row=r, column=col_idx, value=formulas[col_idx])
            else:
                cell = ws.cell(row=r, column=col_idx, value=static.get(col_idx, ""))
            cell.border = thin_border
            cell.alignment = Alignment(vertical="center")

            # Number formats (eTrade ASP inserted at col 15; all old cols >=15 shifted +1)
            if col_idx in (14, 15, 17, 19, 20, 21, 23):  # currency: Zoho MRP, eTrade ASP, MRP w/o GST, Cost Price, Total Cost, Total Cost GST, Etrade Unit Cost
                cell.number_format = num_format
            elif col_idx == 16:                           # GST as %
                cell.number_format = pct_format
            elif col_idx == 18:                           # Margin as %
                cell.number_format = pct_format
            elif col_idx == 24:                           # Diff
                cell.number_format = num_format
            elif col_idx in (8, 10, 12, 25, 27, 28, 29, 30, 32, 33, 34, 35):
                cell.number_format = int_format

    for col_idx in range(1, len(headers) + 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = 16
    ws.column_dimensions["G"].width = 40  # Title column
    ws.row_dimensions[1].height = 40

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


@router.get("/{po_number}/download")
async def download_po_report(po_number: str, db=Depends(get_database)):
    """Download enriched PO report as Excel."""
    def _build():
        doc = db[PO_COLLECTION].find_one({"po_number": po_number})
        if not doc:
            return None, None
        po_status = doc.get("po_status", "pending")
        use_stored = po_status in FREEZE_ON_STATUS
        enriched, _, __ = _enrich_items(doc.get("items", []), po_number, db, use_stored_stock=use_stored, po_date_str=doc.get("po_date"))
        return doc, enriched

    doc, enriched = await asyncio.to_thread(_build)
    if doc is None:
        raise HTTPException(status_code=404, detail=f"PO {po_number} not found")

    excel_bytes = _build_po_excel(doc, enriched)
    po_date_str = doc.get("po_date", "unknown")
    filename = f"PO_Report_{po_number}_{po_date_str}.xlsx"
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/bulk_download")
async def bulk_download_po_reports(po_numbers: list[str], db=Depends(get_database)):
    """Download multiple PO reports as a single zip file."""
    if not po_numbers:
        raise HTTPException(status_code=400, detail="No PO numbers provided")

    def _build_all():
        results = []
        for pn in po_numbers:
            doc = db[PO_COLLECTION].find_one({"po_number": pn})
            if not doc:
                continue
            po_status = doc.get("po_status", "pending")
            use_stored = po_status in FREEZE_ON_STATUS
            enriched, _, __ = _enrich_items(doc.get("items", []), pn, db, use_stored_stock=use_stored, po_date_str=doc.get("po_date"))
            results.append((pn, doc.get("po_date", "unknown"), doc, enriched))
        return results

    results = await asyncio.to_thread(_build_all)
    if not results:
        raise HTTPException(status_code=404, detail="No matching POs found")

    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for pn, po_date_str, doc, enriched in results:
            excel_bytes = _build_po_excel(doc, enriched)
            zf.writestr(f"PO_Report_{pn}_{po_date_str}.xlsx", excel_bytes)
    zip_buf.seek(0)

    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": 'attachment; filename="PO_Reports.zip"'},
    )


# ─── margins ──────────────────────────────────────────────────────────────────

@router.get("/margins")
async def get_margins(db=Depends(get_database)):
    def _fetch():
        return list(db[MARGINS_COLLECTION].find({}, {"_id": 0}))
    margins = await asyncio.to_thread(_fetch)
    return serialize_mongo_document(margins)


@router.put("/margins/{asin}")
async def upsert_margin(
    asin: str,
    margin: Optional[float] = None,
    cost_price_wo_tax: Optional[float] = None,
    etrade_asp: Optional[float] = None,
    db=Depends(get_database),
):
    if margin is None and cost_price_wo_tax is None and etrade_asp is None:
        raise HTTPException(status_code=400, detail="At least one of margin, cost_price_wo_tax, or etrade_asp must be provided")
    if margin is not None and not (0 <= margin <= 1):
        raise HTTPException(status_code=400, detail="margin must be between 0 and 1 (e.g. 0.35 for 35%)")
    if cost_price_wo_tax is not None and cost_price_wo_tax < 0:
        raise HTTPException(status_code=400, detail="cost_price_wo_tax must be >= 0")
    if etrade_asp is not None and etrade_asp < 0:
        raise HTTPException(status_code=400, detail="etrade_asp must be >= 0")

    fields: dict = {"asin": asin, "updated_at": datetime.now()}
    if margin is not None:
        fields["margin"] = margin
    if cost_price_wo_tax is not None:
        fields["cost_price_wo_tax"] = cost_price_wo_tax
    if etrade_asp is not None:
        fields["etrade_asp"] = etrade_asp

    def _upsert():
        db[MARGINS_COLLECTION].update_one(
            {"asin": asin},
            {"$set": fields},
            upsert=True
        )

    await asyncio.to_thread(_upsert)
    return {"asin": asin, "margin": margin, "cost_price_wo_tax": cost_price_wo_tax, "etrade_asp": etrade_asp}


@router.post("/bulk_update")
async def bulk_update_vendor_pos(
    file: UploadFile = File(...),
    db=Depends(get_database),
):
    """Bulk-update accepted_qty, received_qty, and po_status for existing POs.

    Expected Excel columns (header row required):
      A: PO Number | B: ASIN | C: Accepted Qty | D: Received Qty | E: PO Status

    Only POs with status pending/processing/packed/closed will be updated.
    PO Status in column E is optional per row; the first non-empty value wins for each PO.
    """
    file_bytes = await file.read()

    def _process():
        wb = openpyxl.load_workbook(io.BytesIO(file_bytes))
        ws = wb.active
        rows = list(ws.iter_rows(values_only=True))
        if len(rows) < 2:
            raise ValueError("File has no data rows")

        # Group rows by PO number
        updates: dict = {}
        for row in rows[1:]:
            if not row[0]:
                continue
            po_num = str(row[0]).strip()
            asin = str(row[1]).strip() if row[1] else ""
            accepted_qty = int(row[2]) if row[2] is not None else None
            received_qty = int(row[3]) if row[3] is not None else None
            po_status_raw = str(row[4]).strip().lower() if len(row) > 4 and row[4] else None

            if po_num not in updates:
                updates[po_num] = {"po_status": po_status_raw, "items": {}}
            elif po_status_raw and updates[po_num]["po_status"] is None:
                updates[po_num]["po_status"] = po_status_raw

            if asin:
                updates[po_num]["items"][asin] = {
                    "accepted_qty": accepted_qty,
                    "received_qty": received_qty,
                }

        updatable_statuses = {"pending", "processing", "packed", "closed"}
        results = []

        for po_num, update_data in updates.items():
            doc = db[PO_COLLECTION].find_one({"po_number": po_num})
            if not doc:
                results.append({"po_number": po_num, "status": "not_found"})
                continue

            current_status = doc.get("po_status", "pending")
            if current_status not in updatable_statuses:
                results.append({
                    "po_number": po_num,
                    "status": "skipped",
                    "reason": f"status '{current_status}' is not updatable",
                })
                continue

            update_fields: dict = {"updated_at": datetime.now()}

            new_status = update_data.get("po_status")
            if new_status and new_status in VALID_STATUSES:
                update_fields["po_status"] = new_status

            items = doc.get("items", [])
            item_changes = 0
            for item in items:
                asin = item["asin"]
                if asin in update_data["items"]:
                    item_update = update_data["items"][asin]
                    if item_update["accepted_qty"] is not None:
                        item["accepted_qty"] = item_update["accepted_qty"]
                        item_changes += 1
                    if item_update["received_qty"] is not None:
                        item["received_qty"] = item_update["received_qty"]
                        item_changes += 1

            if item_changes:
                update_fields["items"] = items

            if len(update_fields) > 1:  # more than just updated_at
                db[PO_COLLECTION].update_one({"po_number": po_num}, {"$set": update_fields})
                results.append({"po_number": po_num, "status": "updated", "items_changed": item_changes})
            else:
                results.append({"po_number": po_num, "status": "no_changes"})

        return results

    try:
        results = await asyncio.to_thread(_process)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return JSONResponse(status_code=200, content={"results": results})


@router.get("/margins/bulk")
async def get_margins_for_asins(asins: str, db=Depends(get_database)):
    asin_list = [a.strip() for a in asins.split(",") if a.strip()]

    def _fetch():
        return list(db[MARGINS_COLLECTION].find({"asin": {"$in": asin_list}}, {"_id": 0}))

    margins = await asyncio.to_thread(_fetch)
    return {m["asin"]: m["margin"] for m in margins}
