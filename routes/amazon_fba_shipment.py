from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta, date
from ..database import get_database
import asyncio
import io
import openpyxl
from typing import Optional, Any
from pydantic import BaseModel
import logging

from .amazon import compute_drr_for_asins_sync

logger = logging.getLogger(__name__)
router = APIRouter()

PLANNING_OVERRIDES_COLLECTION = "amazon_fba_shipment_planning_overrides"
PROCESSING_COLLECTION = "amazon_fba_shipment_processing"
SUMMARY_COLLECTION = "amazon_fba_shipment_summary"

DEFAULT_LEAD_TIME = 10
DEFAULT_COVERAGE_DAYS = 30

MONTH_NAMES = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec",
}

# ─── Pydantic models ──────────────────────────────────────────────────────────

class PlanningOverride(BaseModel):
    asin: str
    open_shipment_qty_override: Optional[int] = None
    lead_time: Optional[int] = None
    coverage_days: Optional[int] = None
    final_units_override: Optional[int] = None
    fnsku: Optional[str] = None
    platform: Optional[str] = None

class SummaryUpdate(BaseModel):
    reason_for_short_supply: Optional[str] = None
    appointment_initiated_date: Optional[str] = None
    appointment_date: Optional[str] = None
    dispatched_date: Optional[str] = None
    status: Optional[str] = None


# ─── Sync helpers (run in thread) ────────────────────────────────────────────

def _fetch_planning_data(db) -> list[dict]:
    """Compute full FBA shipment planning rows from MongoDB."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    # 1. All ASINs from amazon_sku_mapping
    sku_map_docs = list(db["amazon_sku_mapping"].find(
        {}, {"item_id": 1, "sku_code": 1, "fnsku": 1, "_id": 0}
    ))
    if not sku_map_docs:
        return []

    asins = list({d["item_id"] for d in sku_map_docs if d.get("item_id")})
    asin_to_sku: dict[str, str] = {d["item_id"]: d["sku_code"] for d in sku_map_docs if d.get("item_id") and d.get("sku_code")}
    asin_to_fnsku_default: dict[str, str] = {d["item_id"]: d.get("fnsku", "") for d in sku_map_docs if d.get("item_id")}

    # 2. Product details by SKU
    skus = list(asin_to_sku.values())
    product_by_sku: dict[str, dict] = {}
    for p in db["products"].find(
        {"cf_sku_code": {"$in": skus}},
        {"cf_sku_code": 1, "name": 1, "rate": 1, "item_tax_preferences": 1, "status": 1, "_id": 0},
    ):
        product_by_sku[p["cf_sku_code"]] = p

    # 3. Current FBA inventory from amazon_ledger (exclude VKSX, SELLABLE only)
    ledger_latest = db["amazon_ledger"].find_one(
        {"asin": {"$in": asins}, "location": {"$ne": "VKSX"}, "disposition": "SELLABLE"},
        {"date": 1}, sort=[("date", -1)],
    )
    fba_inv_by_asin: dict[str, int] = {}
    fba_inv_date = None
    if ledger_latest:
        fba_inv_date = ledger_latest["date"]
        for doc in db["amazon_ledger"].aggregate([
            {"$match": {
                "asin": {"$in": asins},
                "date": fba_inv_date,
                "location": {"$ne": "VKSX"},
                "disposition": "SELLABLE",
            }},
            {"$group": {"_id": "$asin", "total": {"$sum": "$ending_warehouse_balance"}}},
        ]):
            fba_inv_by_asin[doc["_id"]] = int(doc["total"] or 0)

    # 4. DRR (last 30 days) — reuse existing function
    drr_map: dict[str, Any] = compute_drr_for_asins_sync(db, today, asins)

    # 5. Open shipment qty from VC POs
    #    processing → requested_qty; packed/intransit → accepted_qty
    open_fba_shipment: dict[str, int] = {}
    for doc in db["vendor_purchase_orders"].aggregate([
        {"$match": {"po_status": {"$in": ["processing", "packed", "intransit"]}}},
        {"$unwind": "$items"},
        {"$match": {"items.asin": {"$in": asins}}},
        {"$group": {
            "_id": "$items.asin",
            "total": {"$sum": {"$cond": {
                "if": {"$eq": ["$po_status", "processing"]},
                "then": {"$ifNull": ["$items.requested_qty", 0]},
                "else": {"$ifNull": ["$items.accepted_qty", 0]},
            }}},
        }},
    ]):
        open_fba_shipment[doc["_id"]] = int(doc["total"] or 0)

    # 6. Latest Zoho stock by item_id
    zoho_item_ids = list({p["item_id"] for p in product_by_sku.values() if p.get("item_id")})
    zoho_latest: dict[str, int] = {}
    if zoho_item_ids:
        for doc in db["zoho_warehouse_stock"].aggregate([
            {"$match": {"zoho_item_id": {"$in": zoho_item_ids}}},
            {"$sort": {"date": -1}},
            {"$group": {"_id": "$zoho_item_id", "warehouses": {"$first": "$warehouses"}}},
        ]):
            wh = doc.get("warehouses", {})
            zoho_latest[doc["_id"]] = int(sum(v for v in wh.values() if isinstance(v, (int, float)))) if isinstance(wh, dict) else 0

    # item_id by sku
    item_id_by_sku: dict[str, str] = {}
    for p in db["products"].find({"cf_sku_code": {"$in": skus}}, {"cf_sku_code": 1, "item_id": 1, "_id": 0}):
        item_id_by_sku[p["cf_sku_code"]] = p.get("item_id", "")

    # 7. Current inventory with Etrade (amazon_vendor_inventory)
    etrade_latest_doc = db["amazon_vendor_inventory"].find_one(
        {"asin": {"$in": asins}}, {"date": 1}, sort=[("date", -1)]
    )
    etrade_inv_by_asin: dict[str, int] = {}
    if etrade_latest_doc:
        etrade_date = etrade_latest_doc["date"]
        for doc in db["amazon_vendor_inventory"].find(
            {"asin": {"$in": asins}, "date": etrade_date},
            {"asin": 1, "sellableOnHandInventoryUnits": 1},
        ):
            etrade_inv_by_asin[doc["asin"]] = int(doc.get("sellableOnHandInventoryUnits") or 0)

    # 8. Open PO (etrade): processing → supply_qty, packed/intransit → accepted_qty
    open_etrade_po: dict[str, int] = {}
    for doc in db["vendor_purchase_orders"].aggregate([
        {"$match": {"po_status": {"$in": ["processing", "packed", "intransit"]}}},
        {"$unwind": "$items"},
        {"$match": {"items.asin": {"$in": asins}}},
        {"$group": {
            "_id": "$items.asin",
            "total": {"$sum": {"$cond": {
                "if": {"$eq": ["$po_status", "processing"]},
                "then": {"$ifNull": ["$items.supply_qty", {"$ifNull": ["$items.requested_qty", 0]}]},
                "else": {"$ifNull": ["$items.accepted_qty", 0]},
            }}},
        }},
    ]):
        open_etrade_po[doc["_id"]] = int(doc["total"] or 0)

    # 9. Monthly sales — last 4 calendar months from amazon_vendor_sales + amazon_ledger FBA sales
    #    Build month labels for the 4 most recent months
    months: list[tuple[int, int]] = []
    ref = today.replace(day=1)
    for _ in range(4):
        ref = (ref - timedelta(days=1)).replace(day=1)
        months.append((ref.year, ref.month))
    months.reverse()  # oldest first

    def month_start_end(y: int, m: int):
        s = datetime(y, m, 1)
        if m == 12:
            e = datetime(y + 1, 1, 1)
        else:
            e = datetime(y, m + 1, 1)
        return s, e

    monthly_sales: dict[str, dict] = {a: {} for a in asins}  # asin → {label: qty}
    for (y, m) in months:
        label = f"{MONTH_NAMES[m]} {y}"
        ms, me = month_start_end(y, m)
        # VC
        for doc in db["amazon_vendor_sales"].aggregate([
            {"$match": {"asin": {"$in": asins}, "date": {"$gte": ms, "$lt": me}}},
            {"$group": {"_id": "$asin", "total": {"$sum": "$orderedUnits"}}},
        ]):
            monthly_sales[doc["_id"]][label] = int(doc["total"] or 0)
        # FBA (from amazon_ledger using salesUnits field if exists, else skip)
        for doc in db["amazon_ledger"].aggregate([
            {"$match": {"asin": {"$in": asins}, "date": {"$gte": ms, "$lt": me}}},
            {"$group": {"_id": "$asin", "total": {"$sum": {"$ifNull": ["$customer_shipments", 0]}}}},
        ]):
            existing = monthly_sales[doc["_id"]].get(label, 0)
            fba_sales = int(doc["total"] or 0)
            monthly_sales[doc["_id"]][label] = existing + fba_sales

    month_labels = [f"{MONTH_NAMES[m]} {y}" for (y, m) in months]

    # 10. Load overrides
    overrides: dict[str, dict] = {}
    for doc in db[PLANNING_OVERRIDES_COLLECTION].find({"asin": {"$in": asins}}):
        overrides[doc["asin"]] = doc

    # 11. Assemble rows
    rows = []
    for asin in asins:
        sku = asin_to_sku.get(asin, "")
        prod = product_by_sku.get(sku, {})
        override = overrides.get(asin, {})

        # MRP: rate from product
        mrp = prod.get("rate") or 0
        # SP: use rate (no separate SP field in products — can be overridden)
        sp = prod.get("rate") or 0

        current_fba_inv = fba_inv_by_asin.get(asin, 0)

        # Open Shipment Qty: DB value or override
        open_shipment_qty = override.get("open_shipment_qty_override") if "open_shipment_qty_override" in override and override["open_shipment_qty_override"] is not None else open_fba_shipment.get(asin, 0)

        total_inventory = current_fba_inv + open_shipment_qty

        drr_info = drr_map.get(asin, {})
        drr_val = drr_info.get("drr") if isinstance(drr_info, dict) else None
        try:
            drr = float(drr_val) if drr_val is not None and str(drr_val) != "No data" else 0.0
        except (TypeError, ValueError):
            drr = 0.0

        net_total_days = round(total_inventory / drr, 1) if drr > 0 else 0

        lead_time = override.get("lead_time") if override.get("lead_time") is not None else DEFAULT_LEAD_TIME
        coverage_days = override.get("coverage_days") if override.get("coverage_days") is not None else DEFAULT_COVERAGE_DAYS
        total_target_days = lead_time + coverage_days
        target_stock = round(drr * total_target_days, 1)

        # Final units to send formula
        if "final_units_override" in override and override["final_units_override"] is not None:
            final_units = override["final_units_override"]
        else:
            if net_total_days < lead_time:
                final_units = round(drr * total_target_days)
            elif net_total_days > total_target_days:
                final_units = 0
            else:
                final_units = round((total_target_days - net_total_days) * drr)

        # Zoho stock
        item_id = item_id_by_sku.get(sku, "")
        zoho_stock = zoho_latest.get(item_id, 0)

        etrade_inv = etrade_inv_by_asin.get(asin, 0)
        open_po = open_etrade_po.get(asin, 0)
        total_qty = etrade_inv + open_po

        sales_by_month = monthly_sales.get(asin, {})

        row = {
            "asin": asin,
            "sku_code": sku,
            "fnsku": override.get("fnsku") or asin_to_fnsku_default.get(asin, ""),
            "item_name": prod.get("name", ""),
            "mrp": mrp,
            "sp": sp,
            "current_fba_inventory": current_fba_inv,
            "open_shipment_qty": open_shipment_qty,
            "open_shipment_qty_auto": open_fba_shipment.get(asin, 0),
            "total_inventory": total_inventory,
            "drr": round(drr, 2),
            "net_total_days": net_total_days,
            "lead_time": lead_time,
            "coverage_days": coverage_days,
            "total_target_days": total_target_days,
            "target_stock": target_stock,
            "final_units_to_send": max(0, final_units),
            "zoho_stock": zoho_stock,
            "status": prod.get("status", ""),
            "platform": override.get("platform", ""),
            "current_inventory_etrade": etrade_inv,
            "open_po": open_po,
            "total_qty": total_qty,
            "monthly_sales": sales_by_month,
            "month_labels": month_labels,
            # Override flags for frontend display
            "open_shipment_overridden": "open_shipment_qty_override" in override and override["open_shipment_qty_override"] is not None,
            "final_units_overridden": "final_units_override" in override and override["final_units_override"] is not None,
        }
        rows.append(row)

    return rows


def _fetch_processing_data(db) -> list[dict]:
    docs = list(db[PROCESSING_COLLECTION].find({}, {"_id": 0}))
    return docs


def _fetch_summary_data(db) -> list[dict]:
    # Aggregate processing data by (shipment_id, location)
    pipeline = [
        {"$group": {
            "_id": {"shipment_id": "$shipment_id", "location": "$location"},
            "shipment_date": {"$first": "$date"},
            "requested_qty": {"$sum": "$requested_qty"},
            "dispatched_qty": {"$sum": "$packed_qty"},
        }},
        {"$project": {
            "_id": 0,
            "shipment_id": "$_id.shipment_id",
            "location": "$_id.location",
            "shipment_date": 1,
            "requested_qty": 1,
            "dispatched_qty": 1,
            "short_supply_qty": {"$subtract": ["$requested_qty", "$dispatched_qty"]},
            "short_supply_pct": {
                "$cond": {
                    "if": {"$gt": ["$dispatched_qty", 0]},
                    "then": {"$divide": [{"$subtract": ["$requested_qty", "$dispatched_qty"]}, "$dispatched_qty"]},
                    "else": 0,
                }
            },
        }},
        {"$sort": {"shipment_date": -1, "shipment_id": 1}},
    ]
    aggregated = list(db[PROCESSING_COLLECTION].aggregate(pipeline))

    # Load stored editable fields
    stored_edits: dict[str, dict] = {}
    for doc in db[SUMMARY_COLLECTION].find({}, {"_id": 0}):
        key = f"{doc.get('shipment_id')}|||{doc.get('location', '')}"
        stored_edits[key] = doc

    rows = []
    for row in aggregated:
        key = f"{row['shipment_id']}|||{row.get('location', '')}"
        edit = stored_edits.get(key, {})
        rows.append({
            **row,
            "reason_for_short_supply": edit.get("reason_for_short_supply", ""),
            "appointment_initiated_date": edit.get("appointment_initiated_date", ""),
            "appointment_date": edit.get("appointment_date", ""),
            "dispatched_date": edit.get("dispatched_date", ""),
            "status": edit.get("status", "Pending"),
        })
    return rows


# ─── Page 1: Planning ─────────────────────────────────────────────────────────

@router.get("/amazon-fba-shipment/planning")
async def get_fba_planning(database=Depends(get_database)):
    try:
        rows = await asyncio.to_thread(_fetch_planning_data, database)
        return {"rows": rows}
    except Exception as e:
        logger.error(f"Error fetching FBA planning data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/amazon-fba-shipment/planning/{asin}")
async def update_planning_override(asin: str, payload: PlanningOverride, database=Depends(get_database)):
    try:
        update_doc = {}
        if payload.open_shipment_qty_override is not None:
            update_doc["open_shipment_qty_override"] = payload.open_shipment_qty_override
        if payload.lead_time is not None:
            update_doc["lead_time"] = payload.lead_time
        if payload.coverage_days is not None:
            update_doc["coverage_days"] = payload.coverage_days
        if payload.final_units_override is not None:
            update_doc["final_units_override"] = payload.final_units_override
        if payload.fnsku is not None:
            update_doc["fnsku"] = payload.fnsku
        if payload.platform is not None:
            update_doc["platform"] = payload.platform

        if update_doc:
            def _upsert(db):
                db[PLANNING_OVERRIDES_COLLECTION].update_one(
                    {"asin": asin},
                    {"$set": {**update_doc, "updated_at": datetime.utcnow()}},
                    upsert=True,
                )
            await asyncio.to_thread(_upsert, database)

        return {"success": True}
    except Exception as e:
        logger.error(f"Error updating planning override for {asin}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/amazon-fba-shipment/planning/{asin}/override")
async def clear_planning_override(asin: str, field: str, database=Depends(get_database)):
    """Clear a specific override field so the auto-computed value is used."""
    allowed = {"open_shipment_qty_override", "lead_time", "coverage_days", "final_units_override"}
    if field not in allowed:
        raise HTTPException(status_code=400, detail=f"Field {field} is not clearable")

    def _clear(db):
        db[PLANNING_OVERRIDES_COLLECTION].update_one(
            {"asin": asin},
            {"$unset": {field: ""}, "$set": {"updated_at": datetime.utcnow()}},
        )
    await asyncio.to_thread(_clear, database)
    return {"success": True}


@router.post("/amazon-fba-shipment/planning/upload")
async def upload_planning_overrides(file: UploadFile = File(...), database=Depends(get_database)):
    """
    Upload an Excel sheet to update planning override fields.
    Expected columns: ASIN, SKU Code, Open Shipment Qty, Lead Time, Coverage Days, Final Units to Send, FNSKU, Platform
    """
    try:
        contents = await file.read()
        wb = openpyxl.load_workbook(io.BytesIO(contents), data_only=True)
        ws = wb.active

        rows = list(ws.iter_rows(values_only=True))
        if not rows:
            raise HTTPException(status_code=400, detail="Empty file")

        # Find header row
        header_row = None
        header_idx = 0
        for i, row in enumerate(rows):
            row_lower = [str(c).strip().lower() if c is not None else "" for c in row]
            if "asin" in row_lower:
                header_row = row_lower
                header_idx = i
                break

        if header_row is None:
            raise HTTPException(status_code=400, detail="Could not find header row with 'ASIN' column")

        def col(name):
            candidates = [name.lower(), name.lower().replace(" ", "_"), name.lower().replace("_", " ")]
            for c in candidates:
                if c in header_row:
                    return header_row.index(c)
            return None

        idx_asin = col("asin")
        idx_sku = col("sku code") or col("sku_code")
        idx_open_qty = col("open shipment qty") or col("open_shipment_qty")
        idx_lead = col("lead time") or col("lead_time")
        idx_coverage = col("coverage days") or col("coverage_days")
        idx_final = col("final units to send") or col("final_units_to_send") or col("final units")
        idx_fnsku = col("fnsku")
        idx_platform = col("platform")

        if idx_asin is None:
            raise HTTPException(status_code=400, detail="Missing 'ASIN' column")

        updated = 0
        errors = []

        def _int_or_none(val):
            try:
                return int(float(val)) if val is not None and str(val).strip() not in ("", "None", "N/A") else None
            except (TypeError, ValueError):
                return None

        def _str_or_none(val):
            s = str(val).strip() if val is not None else ""
            return s if s and s.lower() not in ("none", "n/a", "") else None

        def _process_rows(db):
            nonlocal updated
            for row in rows[header_idx + 1:]:
                if not any(row):
                    continue
                asin_val = _str_or_none(row[idx_asin]) if idx_asin is not None else None
                if not asin_val:
                    continue

                update_doc: dict = {"updated_at": datetime.utcnow()}
                if idx_open_qty is not None:
                    v = _int_or_none(row[idx_open_qty])
                    if v is not None:
                        update_doc["open_shipment_qty_override"] = v
                if idx_lead is not None:
                    v = _int_or_none(row[idx_lead])
                    if v is not None:
                        update_doc["lead_time"] = v
                if idx_coverage is not None:
                    v = _int_or_none(row[idx_coverage])
                    if v is not None:
                        update_doc["coverage_days"] = v
                if idx_final is not None:
                    v = _int_or_none(row[idx_final])
                    if v is not None:
                        update_doc["final_units_override"] = v
                if idx_fnsku is not None:
                    v = _str_or_none(row[idx_fnsku])
                    if v:
                        update_doc["fnsku"] = v
                if idx_platform is not None:
                    v = _str_or_none(row[idx_platform])
                    if v:
                        update_doc["platform"] = v

                if len(update_doc) > 1:  # more than just updated_at
                    db[PLANNING_OVERRIDES_COLLECTION].update_one(
                        {"asin": asin_val},
                        {"$set": update_doc},
                        upsert=True,
                    )
                    updated += 1

        await asyncio.to_thread(_process_rows, database)
        return {"success": True, "updated": updated, "errors": errors}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading planning overrides: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Page 2: Processing ───────────────────────────────────────────────────────

@router.get("/amazon-fba-shipment/processing")
async def get_fba_processing(database=Depends(get_database)):
    try:
        rows = await asyncio.to_thread(_fetch_processing_data, database)
        return {"rows": rows}
    except Exception as e:
        logger.error(f"Error fetching FBA processing data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/amazon-fba-shipment/processing/upload")
async def upload_fba_processing(file: UploadFile = File(...), database=Depends(get_database)):
    """
    Upload FBA shipment processing sheet.
    Expected columns: Shipment ID, Date, Location, SKU Code, ASIN, FNSKU, Item Name, MRP, SP,
                      Requested Qty, Packed Qty, Cost Price, HSN Code, GST
    """
    try:
        contents = await file.read()
        wb = openpyxl.load_workbook(io.BytesIO(contents), data_only=True)
        ws = wb.active

        all_rows = list(ws.iter_rows(values_only=True))
        if not all_rows:
            raise HTTPException(status_code=400, detail="Empty file")

        # Find header
        header_row = None
        header_idx = 0
        for i, row in enumerate(all_rows):
            row_clean = [str(c).strip().lower() if c else "" for c in row]
            if "shipment id" in row_clean or "shipment_id" in row_clean:
                header_row = row_clean
                header_idx = i
                break

        if header_row is None:
            raise HTTPException(status_code=400, detail="Could not find header row with 'Shipment ID' column")

        def _find_col(*names):
            for name in names:
                try:
                    return header_row.index(name.lower())
                except ValueError:
                    pass
            return None

        idx_shipment_id = _find_col("shipment id", "shipment_id")
        idx_date = _find_col("date")
        idx_location = _find_col("location")
        idx_sku = _find_col("sku code", "sku_code")
        idx_asin = _find_col("asin")
        idx_fnsku = _find_col("fnsku")
        idx_item_name = _find_col("item name", "item_name")
        idx_mrp = _find_col("mrp")
        idx_sp = _find_col("sp")
        idx_requested_qty = _find_col("requested qty", "requested_qty")
        idx_packed_qty = _find_col("packed qty", "packed_qty")
        idx_cost_price = _find_col("cost price", "cost_price")
        idx_hsn = _find_col("hsn code", "hsn_code", "hsn")
        idx_gst = _find_col("gst")

        if idx_shipment_id is None:
            raise HTTPException(status_code=400, detail="Missing 'Shipment ID' column")

        def _safe_num(val):
            try:
                return float(val) if val is not None and str(val).strip() not in ("", "None") else None
            except (TypeError, ValueError):
                return None

        def _safe_int(val):
            try:
                return int(float(val)) if val is not None and str(val).strip() not in ("", "None") else 0
            except (TypeError, ValueError):
                return 0

        def _safe_str(val, idx):
            if idx is None or val is None:
                return ""
            return str(val).strip()

        def _parse_date(val):
            if val is None:
                return None
            if isinstance(val, datetime):
                return val
            if isinstance(val, date):
                return datetime(val.year, val.month, val.day)
            try:
                return datetime.strptime(str(val).strip(), "%Y-%m-%d")
            except ValueError:
                try:
                    return datetime.strptime(str(val).strip(), "%d/%m/%Y")
                except ValueError:
                    return None

        records = []
        for raw in all_rows[header_idx + 1:]:
            if not any(raw):
                continue
            shipment_id = _safe_str(raw[idx_shipment_id], idx_shipment_id)
            if not shipment_id:
                continue

            record = {
                "shipment_id": shipment_id,
                "date": _parse_date(raw[idx_date] if idx_date is not None else None),
                "location": _safe_str(raw[idx_location] if idx_location is not None else None, idx_location),
                "sku_code": _safe_str(raw[idx_sku] if idx_sku is not None else None, idx_sku),
                "asin": _safe_str(raw[idx_asin] if idx_asin is not None else None, idx_asin),
                "fnsku": _safe_str(raw[idx_fnsku] if idx_fnsku is not None else None, idx_fnsku),
                "item_name": _safe_str(raw[idx_item_name] if idx_item_name is not None else None, idx_item_name),
                "mrp": _safe_num(raw[idx_mrp] if idx_mrp is not None else None),
                "sp": _safe_num(raw[idx_sp] if idx_sp is not None else None),
                "requested_qty": _safe_int(raw[idx_requested_qty] if idx_requested_qty is not None else None),
                "packed_qty": _safe_int(raw[idx_packed_qty] if idx_packed_qty is not None else None),
                "cost_price": _safe_num(raw[idx_cost_price] if idx_cost_price is not None else None),
                "hsn_code": _safe_str(raw[idx_hsn] if idx_hsn is not None else None, idx_hsn),
                "gst": _safe_num(raw[idx_gst] if idx_gst is not None else None),
                "uploaded_at": datetime.utcnow(),
            }
            records.append(record)

        if not records:
            raise HTTPException(status_code=400, detail="No data rows found in file")

        def _store(db):
            # Upsert by (shipment_id, asin/sku_code) — replace existing rows for same shipment
            for rec in records:
                db[PROCESSING_COLLECTION].update_one(
                    {"shipment_id": rec["shipment_id"], "sku_code": rec["sku_code"], "asin": rec["asin"]},
                    {"$set": rec},
                    upsert=True,
                )

        await asyncio.to_thread(_store, database)
        return {"success": True, "rows_uploaded": len(records)}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading FBA processing data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/amazon-fba-shipment/processing")
async def clear_fba_processing(database=Depends(get_database)):
    """Clear all processing records."""
    def _clear(db):
        return db[PROCESSING_COLLECTION].delete_many({}).deleted_count
    deleted = await asyncio.to_thread(_clear, database)
    return {"success": True, "deleted": deleted}


# ─── Page 3: Summary ──────────────────────────────────────────────────────────

@router.get("/amazon-fba-shipment/summary")
async def get_fba_summary(database=Depends(get_database)):
    try:
        rows = await asyncio.to_thread(_fetch_summary_data, database)
        return {"rows": rows}
    except Exception as e:
        logger.error(f"Error fetching FBA summary data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/amazon-fba-shipment/summary/{shipment_id}")
async def update_fba_summary(shipment_id: str, location: str, payload: SummaryUpdate, database=Depends(get_database)):
    try:
        update_doc: dict = {"shipment_id": shipment_id, "location": location, "updated_at": datetime.utcnow()}
        if payload.reason_for_short_supply is not None:
            update_doc["reason_for_short_supply"] = payload.reason_for_short_supply
        if payload.appointment_initiated_date is not None:
            update_doc["appointment_initiated_date"] = payload.appointment_initiated_date
        if payload.appointment_date is not None:
            update_doc["appointment_date"] = payload.appointment_date
        if payload.dispatched_date is not None:
            update_doc["dispatched_date"] = payload.dispatched_date
        if payload.status is not None:
            update_doc["status"] = payload.status

        def _upsert(db):
            db[SUMMARY_COLLECTION].update_one(
                {"shipment_id": shipment_id, "location": location},
                {"$set": update_doc},
                upsert=True,
            )
        await asyncio.to_thread(_upsert, database)
        return {"success": True}
    except Exception as e:
        logger.error(f"Error updating FBA summary: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
