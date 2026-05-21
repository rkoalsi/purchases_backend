from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime, timedelta, date
from ..database import get_database
import asyncio
import io
import openpyxl
from openpyxl.styles import PatternFill, Font, Alignment
from openpyxl.utils import get_column_letter
from typing import Optional, Any
from pydantic import BaseModel
import logging

from .amazon import compute_drr_for_asins_sync, generate_report_by_date_range

logger = logging.getLogger(__name__)
router = APIRouter()

PLANNING_OVERRIDES_COLLECTION = "amazon_fba_shipment_planning_overrides"
PROCESSING_COLLECTION = "amazon_fba_shipment_processing"
SUMMARY_COLLECTION = "amazon_fba_shipment_summary"
SKU_MAPPING_COLLECTION = "amazon_sku_mapping"

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
    mrp_override: Optional[float] = None
    sp_override: Optional[float] = None
    status_override: Optional[str] = None

class SummaryUpdate(BaseModel):
    reason_for_short_supply: Optional[str] = None
    appointment_initiated_date: Optional[str] = None
    appointment_date: Optional[str] = None
    dispatched_date: Optional[str] = None
    status: Optional[str] = None


# ─── DRR (same methodology as vendor_po and vc_under_ordering) ───────────────

async def _compute_drr_async(db, today: datetime, asins: list[str]) -> dict:
    """Compute DRR using the PSR 'all' report, matching vendor_po and vc_under_ordering."""
    if not asins:
        return {}
    drr_start = today - timedelta(days=89)
    report = await generate_report_by_date_range(
        drr_start.strftime("%Y-%m-%d"), today.strftime("%Y-%m-%d"), db, report_type="all"
    )
    asin_set = set(asins)
    result = {
        item["asin"]: {"drr": item.get("drr", 0), "drr_flag": item.get("drr_flag", "")}
        for item in report
        if item.get("asin") in asin_set
    }
    missing = [a for a in asins if a not in result]
    if missing:
        end_dt = today.replace(hour=23, minute=59, second=59, microsecond=999999)
        fallback = await asyncio.to_thread(compute_drr_for_asins_sync, db, end_dt, missing)
        result.update(fallback)
    return result


# ─── Sync helpers (run in thread) ────────────────────────────────────────────

def _get_asins(db) -> list[str]:
    docs = list(db[SKU_MAPPING_COLLECTION].find({}, {"item_id": 1, "_id": 0}))
    return list({d["item_id"] for d in docs if d.get("item_id")})


def _fetch_planning_data(db, drr_map: dict) -> tuple:
    """Compute full FBA shipment planning rows from MongoDB."""
    today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    fba_inv_date_str = ""
    zoho_date_str = ""
    etrade_date_str = ""

    # 1. All ASINs from amazon_sku_mapping
    sku_map_docs = list(db[SKU_MAPPING_COLLECTION].find(
        {}, {"item_id": 1, "sku_code": 1, "fnsku": 1, "_id": 0}
    ))
    if not sku_map_docs:
        return []

    asins = list({d["item_id"] for d in sku_map_docs if d.get("item_id")})
    asin_to_sku: dict[str, str] = {d["item_id"]: d["sku_code"] for d in sku_map_docs if d.get("item_id") and d.get("sku_code")}
    asin_to_fnsku_default: dict[str, str] = {d["item_id"]: d.get("fnsku", "") for d in sku_map_docs if d.get("item_id")}

    # Platform derived from vendor_margins flags
    etrade_po_asins: set[str] = set()
    etrade_df_asins: set[str] = set()
    for d in db["vendor_margins"].find(
        {"asin": {"$in": asins}, "$or": [{"etrade_po": True}, {"etrade_df": True}]},
        {"asin": 1, "etrade_po": 1, "etrade_df": 1, "_id": 0},
    ):
        a = d.get("asin")
        if not a:
            continue
        if d.get("etrade_po"):
            etrade_po_asins.add(a)
        elif d.get("etrade_df"):
            etrade_df_asins.add(a)

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
    if ledger_latest:
        fba_inv_date = ledger_latest["date"]
        fba_inv_date_str = fba_inv_date.strftime("%-d %b %Y") if isinstance(fba_inv_date, datetime) else str(fba_inv_date)
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

    # 4. Open shipment qty from FBA processing sheet (uploaded shipments)
    open_fba_shipment: dict[str, int] = {}
    for doc in db[PROCESSING_COLLECTION].aggregate([
        {"$match": {"asin": {"$in": asins}}},
        {"$group": {"_id": "$asin", "total": {"$sum": {"$ifNull": ["$requested_qty", 0]}}}},
    ]):
        open_fba_shipment[doc["_id"]] = int(doc["total"] or 0)

    # 5. Latest Zoho stock by item_id
    zoho_item_ids = list({p.get("item_id", "") for p in product_by_sku.values() if p.get("item_id")})
    zoho_latest: dict[str, int] = {}
    if zoho_item_ids:
        latest_zoho_dt = None
        for doc in db["zoho_warehouse_stock"].aggregate([
            {"$match": {"zoho_item_id": {"$in": zoho_item_ids}}},
            {"$sort": {"date": -1}},
            {"$group": {"_id": "$zoho_item_id", "warehouses": {"$first": "$warehouses"}, "date": {"$first": "$date"}}},
        ]):
            wh = doc.get("warehouses", {})
            zoho_latest[doc["_id"]] = int(sum(v for v in wh.values() if isinstance(v, (int, float)))) if isinstance(wh, dict) else 0
            d = doc.get("date")
            if d and (latest_zoho_dt is None or d > latest_zoho_dt):
                latest_zoho_dt = d
        if latest_zoho_dt:
            zoho_date_str = latest_zoho_dt.strftime("%-d %b %Y") if isinstance(latest_zoho_dt, datetime) else str(latest_zoho_dt)

    item_id_by_sku: dict[str, str] = {}
    for p in db["products"].find({"cf_sku_code": {"$in": skus}}, {"cf_sku_code": 1, "item_id": 1, "_id": 0}):
        item_id_by_sku[p["cf_sku_code"]] = p.get("item_id", "")

    # 6. Current inventory with Etrade (amazon_vendor_inventory)
    etrade_latest_doc = db["amazon_vendor_inventory"].find_one(
        {"asin": {"$in": asins}}, {"date": 1}, sort=[("date", -1)]
    )
    etrade_inv_by_asin: dict[str, int] = {}
    if etrade_latest_doc:
        etrade_date = etrade_latest_doc["date"]
        etrade_date_str = etrade_date.strftime("%-d %b %Y") if isinstance(etrade_date, datetime) else str(etrade_date)
        for doc in db["amazon_vendor_inventory"].find(
            {"asin": {"$in": asins}, "date": etrade_date},
            {"asin": 1, "sellableOnHandInventoryUnits": 1},
        ):
            etrade_inv_by_asin[doc["asin"]] = int(doc.get("sellableOnHandInventoryUnits") or 0)

    # 7. Open PO (etrade) — same logic as vc_under_ordering.py:
    #    processing → supply_qty_override → final_supply_fo → supply_qty (if >0) → requested_qty
    #    packed/closed/intransit → accepted_qty
    open_etrade_po: dict[str, int] = {}
    for doc in db["vendor_purchase_orders"].aggregate([
        {"$match": {"po_status": {"$in": ["processing", "packed", "closed", "intransit"]}}},
        {"$unwind": "$items"},
        {"$match": {"items.asin": {"$in": asins}}},
        {"$group": {
            "_id": "$items.asin",
            "total": {"$sum": {
                "$cond": {
                    "if": {"$eq": ["$po_status", "processing"]},
                    "then": {
                        "$cond": {
                            "if": {"$ne": [{"$ifNull": ["$items.supply_qty_override", None]}, None]},
                            "then": {"$ifNull": ["$items.supply_qty_override", 0]},
                            "else": {
                                "$cond": {
                                    "if": {"$ne": [{"$ifNull": ["$items.final_supply_fo", None]}, None]},
                                    "then": {"$ifNull": ["$items.final_supply_fo", 0]},
                                    "else": {
                                        "$cond": {
                                            "if": {"$gt": [{"$ifNull": ["$items.supply_qty", 0]}, 0]},
                                            "then": {"$ifNull": ["$items.supply_qty", 0]},
                                            "else": {"$ifNull": ["$items.requested_qty", 0]},
                                        }
                                    },
                                }
                            },
                        }
                    },
                    "else": {"$ifNull": ["$items.accepted_qty", 0]},
                }
            }},
        }},
    ]):
        open_etrade_po[doc["_id"]] = int(doc["total"] or 0)

    # 8. Monthly sales — last 4 calendar months
    months: list[tuple[int, int]] = []
    ref = today.replace(day=1)
    for _ in range(4):
        ref = (ref - timedelta(days=1)).replace(day=1)
        months.append((ref.year, ref.month))
    months.reverse()  # oldest first

    def month_start_end(y: int, m: int):
        s = datetime(y, m, 1)
        e = datetime(y + 1, 1, 1) if m == 12 else datetime(y, m + 1, 1)
        return s, e

    monthly_sales: dict[str, dict] = {a: {} for a in asins}
    for (y, m) in months:
        label = f"{MONTH_NAMES[m]} {y}"
        ms, me = month_start_end(y, m)
        for doc in db["amazon_vendor_sales"].aggregate([
            {"$match": {"asin": {"$in": asins}, "date": {"$gte": ms, "$lt": me}}},
            {"$group": {"_id": "$asin", "total": {"$sum": "$orderedUnits"}}},
        ]):
            monthly_sales[doc["_id"]][label] = int(doc["total"] or 0)
        for doc in db["amazon_ledger"].aggregate([
            {"$match": {"asin": {"$in": asins}, "date": {"$gte": ms, "$lt": me}}},
            {"$group": {"_id": "$asin", "total": {"$sum": {"$ifNull": ["$customer_shipments", 0]}}}},
        ]):
            existing = monthly_sales[doc["_id"]].get(label, 0)
            monthly_sales[doc["_id"]][label] = existing + int(doc["total"] or 0)

    month_labels = [f"{MONTH_NAMES[m]} {y}" for (y, m) in months]

    # 9. Load overrides
    overrides: dict[str, dict] = {}
    for doc in db[PLANNING_OVERRIDES_COLLECTION].find({"asin": {"$in": asins}}):
        overrides[doc["asin"]] = doc

    # 10. Assemble rows
    rows = []
    for asin in asins:
        sku = asin_to_sku.get(asin, "")
        prod = product_by_sku.get(sku, {})
        override = overrides.get(asin, {})

        # MRP/SP: use override if present, else fall back to product rate
        mrp = override["mrp_override"] if override.get("mrp_override") is not None else (prod.get("rate") or 0)
        sp = override["sp_override"] if override.get("sp_override") is not None else (prod.get("rate") or 0)

        # Status: use override if present
        status = override["status_override"] if override.get("status_override") is not None else prod.get("status", "")

        current_fba_inv = fba_inv_by_asin.get(asin, 0)

        open_shipment_qty = (
            override["open_shipment_qty_override"]
            if "open_shipment_qty_override" in override and override["open_shipment_qty_override"] is not None
            else open_fba_shipment.get(asin, 0)
        )

        total_inventory = current_fba_inv + open_shipment_qty

        drr_info = drr_map.get(asin, {})
        drr_val = drr_info.get("drr") if isinstance(drr_info, dict) else None
        try:
            drr = float(drr_val) if drr_val is not None and str(drr_val) != "No data" else 0.0
        except (TypeError, ValueError):
            drr = 0.0

        net_total_days = round(total_inventory / drr, 2) if drr > 0 else 0

        lead_time = override.get("lead_time") if override.get("lead_time") is not None else DEFAULT_LEAD_TIME
        coverage_days = override.get("coverage_days") if override.get("coverage_days") is not None else DEFAULT_COVERAGE_DAYS
        total_target_days = lead_time + coverage_days
        target_stock = round(drr * total_target_days, 0) if drr > 0 else 0

        if "final_units_override" in override and override["final_units_override"] is not None:
            final_units = override["final_units_override"]
        else:
            if drr == 0:
                final_units = 0
            elif net_total_days < lead_time:
                final_units = round(drr * total_target_days)
            elif net_total_days > total_target_days:
                final_units = 0
            else:
                final_units = max(0, round((total_target_days - net_total_days) * drr))

        item_id = item_id_by_sku.get(sku, "")
        zoho_stock = zoho_latest.get(item_id, 0)

        etrade_inv = etrade_inv_by_asin.get(asin, 0)
        open_po = open_etrade_po.get(asin, 0)
        total_qty = etrade_inv + open_po

        sales_by_month = monthly_sales.get(asin, {})

        rows.append({
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
            "status": status,
            "platform": override.get("platform") or (
                "Etrade - PO & DF" if asin in etrade_po_asins
                else "Etrade - DF only" if asin in etrade_df_asins
                else "FBA or SC"
            ),
            "current_inventory_etrade": etrade_inv,
            "open_po": open_po,
            "total_qty": total_qty,
            "monthly_sales": sales_by_month,
            "month_labels": month_labels,
            "open_shipment_overridden": "open_shipment_qty_override" in override and override["open_shipment_qty_override"] is not None,
            "final_units_overridden": "final_units_override" in override and override["final_units_override"] is not None,
        })

    return rows, fba_inv_date_str, zoho_date_str, etrade_date_str


def _fetch_processing_data(db) -> list[dict]:
    docs = list(db[PROCESSING_COLLECTION].find({}, {"_id": 0}))
    return docs


def _fetch_summary_data(db) -> list[dict]:
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


def _generate_planning_excel(
    rows: list[dict],
    fba_inv_date: str = "",
    zoho_date: str = "",
    etrade_date: str = "",
    drr_period: str = "",
) -> bytes:
    """Generate Excel matching the FBA shipment format with formulas for computed columns."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "FBA Shipment Planning"

    if not rows:
        buf = io.BytesIO()
        wb.save(buf)
        return buf.getvalue()

    month_labels = rows[0].get("month_labels", [])

    # Column layout (FNSKU removed):
    # A(1) ASIN  B(2) SKU  C(3) Item Name  D(4) MRP  E(5) SP
    # F(6) FBA Inv  G(7) Open Shipment  H(8)=F+G Total Inv
    # I(9) DRR  J(10)=H/I Net Days  K(11) Lead  L(12) Coverage
    # M(13)=K+L Target Days  N(14)=I*M Target Stock  O(15) Final Units
    # P(16) Zoho  Q(17) Status  R(18) Platform
    # S(19) Etrade Inv  T(20) Open PO  U(21)=S+T Total Qty
    # V+(22+) Monthly Sales

    fba_inv_label = f"Current FBA\nInventory\n({fba_inv_date})" if fba_inv_date else "Current FBA\nInventory"
    drr_label = f"DRR\n({drr_period})" if drr_period else "DRR"
    zoho_label = f"Zoho Stock\n({zoho_date})" if zoho_date else "Zoho Stock"
    etrade_label = f"Current Inventory\nwith Etrade\n({etrade_date})" if etrade_date else "Current Inventory\nwith Etrade"

    headers = [
        "ASIN",
        "SKU Code",
        "Item Name",
        "MRP",
        "SP",
        fba_inv_label,
        "Open Shipment\nQty",
        "Total Inventory\n(FBA + Open Shipment)",
        drr_label,
        "Net Total Days\n(=Total Inv ÷ DRR)",
        "Lead Time",
        "Coverage Days",
        "Total Target Days\n(=Lead + Coverage)",
        "Target Stock\n(=DRR × Total Target Days)",
        "Final Units\n(under-ordering formula)",
        zoho_label,
        "Status",
        "Platform",
        etrade_label,
        "Open PO",
        "Total Qty\n(Etrade + Open PO)",
    ] + [f"{label}\nUnits Sold" for label in month_labels]

    header_fill = PatternFill("solid", fgColor="4472C4")
    header_font = Font(bold=True, color="FFFFFF")
    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left_wrap = Alignment(horizontal="left", vertical="top", wrap_text=True)

    for col_idx, header in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col_idx, value=header)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = center

    ws.row_dimensions[1].height = 55

    for row_idx, row in enumerate(rows, 2):
        r = row_idx
        drr_val = row.get("drr") or None

        ws.cell(row=r, column=1, value=row.get("asin", ""))
        ws.cell(row=r, column=2, value=row.get("sku_code", ""))
        c = ws.cell(row=r, column=3, value=row.get("item_name", "") or None)
        c.alignment = left_wrap
        ws.cell(row=r, column=4, value=row.get("mrp") or None)
        ws.cell(row=r, column=5, value=row.get("sp") or None)
        ws.cell(row=r, column=6, value=row.get("current_fba_inventory") or None)
        ws.cell(row=r, column=7, value=row.get("open_shipment_qty") or None)
        ws.cell(row=r, column=8, value=f"=F{r}+G{r}")                                                                    # H: Total Inv
        ws.cell(row=r, column=9, value=drr_val)                                                                           # I: DRR
        ws.cell(row=r, column=10, value=f'=IF(I{r}=0,"",ROUND(H{r}/I{r},2))')                                           # J: Net Days
        ws.cell(row=r, column=11, value=row.get("lead_time", DEFAULT_LEAD_TIME))
        ws.cell(row=r, column=12, value=row.get("coverage_days", DEFAULT_COVERAGE_DAYS))
        ws.cell(row=r, column=13, value=f"=K{r}+L{r}")                                                                   # M: Total Target Days
        ws.cell(row=r, column=14, value=f"=IF(I{r}=0,0,ROUND(I{r}*M{r},0))")                                            # N: Target Stock
        ws.cell(row=r, column=15, value=f'=IF(I{r}=0,"",IF(J{r}<K{r},ROUND(I{r}*M{r},0),IF(J{r}>M{r},0,MAX(0,ROUND((M{r}-J{r})*I{r},0)))))') # O: Final Units
        ws.cell(row=r, column=16, value=row.get("zoho_stock") or None)
        ws.cell(row=r, column=17, value=row.get("status") or None)
        ws.cell(row=r, column=18, value=row.get("platform") or None)
        ws.cell(row=r, column=19, value=row.get("current_inventory_etrade") or None)
        ws.cell(row=r, column=20, value=row.get("open_po") or None)
        ws.cell(row=r, column=21, value=f"=S{r}+T{r}")                                                                   # U: Total Qty

        monthly_sales = row.get("monthly_sales", {})
        for offset, label in enumerate(month_labels):
            val = monthly_sales.get(label)
            ws.cell(row=r, column=22 + offset, value=val if val else None)

    col_widths = [14, 18, 50, 8, 8, 12, 12, 14, 10, 12, 9, 10, 14, 14, 14, 10, 12, 12, 15, 9, 12]
    col_widths += [14] * len(month_labels)
    for col_idx, width in enumerate(col_widths, 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    ws.freeze_panes = "D2"

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


# ─── Page 1: Planning ─────────────────────────────────────────────────────────

@router.get("/planning")
async def get_fba_planning(database=Depends(get_database)):
    try:
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        drr_start = today - timedelta(days=89)
        drr_period = f"{drr_start.strftime('%-d %b %Y')} – {today.strftime('%-d %b %Y')}"
        asins = await asyncio.to_thread(_get_asins, database)
        drr_map = await _compute_drr_async(database, today, asins)
        rows, fba_inv_date, zoho_date, etrade_date = await asyncio.to_thread(_fetch_planning_data, database, drr_map)
        return {"rows": rows, "fba_inv_date": fba_inv_date, "zoho_date": zoho_date, "etrade_date": etrade_date, "drr_period": drr_period}
    except Exception as e:
        logger.error(f"Error fetching FBA planning data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/planning/download")
async def download_fba_planning(database=Depends(get_database)):
    try:
        today = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
        drr_start = today - timedelta(days=89)
        drr_period = f"{drr_start.strftime('%-d %b %Y')} – {today.strftime('%-d %b %Y')}"
        asins = await asyncio.to_thread(_get_asins, database)
        drr_map = await _compute_drr_async(database, today, asins)
        rows, fba_inv_date, zoho_date, etrade_date = await asyncio.to_thread(_fetch_planning_data, database, drr_map)
        excel_bytes = _generate_planning_excel(rows, fba_inv_date, zoho_date, etrade_date, drr_period)
        filename = f"fba_shipment_planning_{today.strftime('%Y%m%d')}.xlsx"
        return StreamingResponse(
            io.BytesIO(excel_bytes),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
        )
    except Exception as e:
        logger.error(f"Error generating FBA planning Excel: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/planning/{asin}")
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
        if payload.mrp_override is not None:
            update_doc["mrp_override"] = payload.mrp_override
        if payload.sp_override is not None:
            update_doc["sp_override"] = payload.sp_override
        if payload.status_override is not None:
            update_doc["status_override"] = payload.status_override

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


@router.delete("/planning/{asin}/override")
async def clear_planning_override(asin: str, field: str, database=Depends(get_database)):
    """Clear a specific override field so the auto-computed value is used."""
    allowed = {"open_shipment_qty_override", "lead_time", "coverage_days", "final_units_override", "mrp_override", "sp_override", "status_override"}
    if field not in allowed:
        raise HTTPException(status_code=400, detail=f"Field {field} is not clearable")

    def _clear(db):
        db[PLANNING_OVERRIDES_COLLECTION].update_one(
            {"asin": asin},
            {"$unset": {field: ""}, "$set": {"updated_at": datetime.utcnow()}},
        )
    await asyncio.to_thread(_clear, database)
    return {"success": True}


@router.post("/planning/upload")
async def upload_planning_overrides(file: UploadFile = File(...), database=Depends(get_database)):
    """
    Upload the FBA shipment planning Excel to populate amazon_sku_mapping and overrides.
    Expected columns match the reference format:
      ASIN, SKU Code, FNSKU, Item Name, MRP, SP, Lead Time, Coverage Days, Status, Platform,
      Open Shipment Qty (optional), Final Units (optional)
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

        def _col(*names):
            for name in names:
                n = name.lower().strip()
                if n in header_row:
                    return header_row.index(n)
                # try partial match for multi-line headers
                for i, h in enumerate(header_row):
                    if n in h:
                        return i
            return None

        idx_asin = _col("asin")
        idx_sku = _col("sku code", "sku_code")
        idx_fnsku = _col("fnsku")
        idx_item_name = _col("item name", "item_name")
        idx_mrp = _col("mrp")
        idx_sp = _col("sp")
        idx_lead = _col("lead time", "lead_time")
        idx_coverage = _col("coverage days", "coverage_days")
        idx_status = _col("status")
        idx_platform = _col("platform")
        idx_open_qty = _col("open shipment qty", "open_shipment_qty")
        idx_final = _col("final units", "final_units_to_send")

        if idx_asin is None:
            raise HTTPException(status_code=400, detail="Missing 'ASIN' column")

        def _int_or_none(val):
            try:
                return int(float(val)) if val is not None and str(val).strip() not in ("", "None", "N/A") else None
            except (TypeError, ValueError):
                return None

        def _float_or_none(val):
            try:
                return float(val) if val is not None and str(val).strip() not in ("", "None", "N/A") else None
            except (TypeError, ValueError):
                return None

        def _str_or_none(val):
            s = str(val).strip() if val is not None else ""
            return s if s and s.lower() not in ("none", "n/a", "") else None

        updated_sku_mapping = 0
        updated_overrides = 0

        def _process_rows(db):
            nonlocal updated_sku_mapping, updated_overrides
            for row in rows[header_idx + 1:]:
                if not any(row):
                    continue
                asin_val = _str_or_none(row[idx_asin]) if idx_asin is not None else None
                if not asin_val:
                    continue

                sku_val = _str_or_none(row[idx_sku]) if idx_sku is not None else None
                fnsku_val = _str_or_none(row[idx_fnsku]) if idx_fnsku is not None else None

                # Upsert amazon_sku_mapping
                if sku_val or fnsku_val:
                    sku_map_update: dict = {"updated_at": datetime.utcnow()}
                    if sku_val:
                        sku_map_update["sku_code"] = sku_val
                    if fnsku_val:
                        sku_map_update["fnsku"] = fnsku_val
                    db[SKU_MAPPING_COLLECTION].update_one(
                        {"item_id": asin_val},
                        {"$set": {**sku_map_update, "item_id": asin_val}},
                        upsert=True,
                    )
                    updated_sku_mapping += 1

                # Build planning overrides doc
                override_update: dict = {"updated_at": datetime.utcnow()}

                if fnsku_val:
                    override_update["fnsku"] = fnsku_val

                mrp_val = _float_or_none(row[idx_mrp]) if idx_mrp is not None else None
                if mrp_val is not None:
                    override_update["mrp_override"] = mrp_val

                sp_val = _float_or_none(row[idx_sp]) if idx_sp is not None else None
                if sp_val is not None:
                    override_update["sp_override"] = sp_val

                lead_val = _int_or_none(row[idx_lead]) if idx_lead is not None else None
                if lead_val is not None:
                    override_update["lead_time"] = lead_val

                coverage_val = _int_or_none(row[idx_coverage]) if idx_coverage is not None else None
                if coverage_val is not None:
                    override_update["coverage_days"] = coverage_val

                status_val = _str_or_none(row[idx_status]) if idx_status is not None else None
                if status_val is not None:
                    override_update["status_override"] = status_val

                platform_val = _str_or_none(row[idx_platform]) if idx_platform is not None else None
                if platform_val is not None:
                    override_update["platform"] = platform_val

                open_qty_val = _int_or_none(row[idx_open_qty]) if idx_open_qty is not None else None
                if open_qty_val is not None:
                    override_update["open_shipment_qty_override"] = open_qty_val

                final_val = _int_or_none(row[idx_final]) if idx_final is not None else None
                if final_val is not None:
                    override_update["final_units_override"] = final_val

                if len(override_update) > 1:  # more than just updated_at
                    db[PLANNING_OVERRIDES_COLLECTION].update_one(
                        {"asin": asin_val},
                        {"$set": override_update},
                        upsert=True,
                    )
                    updated_overrides += 1

        await asyncio.to_thread(_process_rows, database)
        return {
            "success": True,
            "sku_mapping_updated": updated_sku_mapping,
            "overrides_updated": updated_overrides,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error uploading planning overrides: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ─── Page 2: Processing ───────────────────────────────────────────────────────

@router.get("/processing")
async def get_fba_processing(database=Depends(get_database)):
    try:
        rows = await asyncio.to_thread(_fetch_processing_data, database)
        return {"rows": rows}
    except Exception as e:
        logger.error(f"Error fetching FBA processing data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/processing/upload")
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


@router.delete("/processing")
async def clear_fba_processing(database=Depends(get_database)):
    """Clear all processing records."""
    def _clear(db):
        return db[PROCESSING_COLLECTION].delete_many({}).deleted_count
    deleted = await asyncio.to_thread(_clear, database)
    return {"success": True, "deleted": deleted}


# ─── Page 3: Summary ──────────────────────────────────────────────────────────

@router.get("/summary")
async def get_fba_summary(database=Depends(get_database)):
    try:
        rows = await asyncio.to_thread(_fetch_summary_data, database)
        return {"rows": rows}
    except Exception as e:
        logger.error(f"Error fetching FBA summary data: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/summary/{shipment_id}")
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
