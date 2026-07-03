from fastapi import APIRouter, HTTPException, Depends, Query, Body, UploadFile, File
from fastapi.responses import StreamingResponse
from datetime import datetime
from typing import Optional
import io
import logging
import os
import requests
import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment
from openpyxl.utils import get_column_letter

from ..database import get_database, serialize_mongo_document

logger = logging.getLogger(__name__)
router = APIRouter()

PRODUCTS_COLLECTION = "products"
PURCHASE_ORDERS_COLLECTION = "purchase_orders"
BILLS_COLLECTION = "bills"
BRAND_ORDERS_COLLECTION = "brand_orders"
INVENTORY_ADJUSTMENTS_COLLECTION = "inventory_adjustments"

ZOHO_BOOKS_BASE = "https://books.zoho.com/api/v3"
ZOHO_INVENTORY_BASE = os.getenv("ZOHO_INVENTORY_BASE", "https://www.zohoapis.com/inventory/v1")
ORGANIZATION_ID = os.getenv("ORGANIZATION_ID", "776755316")
BOOKS_URL = os.getenv("BOOKS_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
BOOKS_REFRESH_TOKEN = os.getenv("BOOKS_REFRESH_TOKEN")
INVENTORY_REFRESH_TOKEN = os.getenv("INVENTORY_REFRESH_TOKEN", BOOKS_REFRESH_TOKEN)
_ZOHO_TOKEN_URL = "https://accounts.zoho.com/oauth/v2/token"

DEFAULT_ACCOUNT_ID = "3220178000000034001"  # Inventory Asset
DEFAULT_WAREHOUSE_ID = "3220178000000403010"  # Pupscribe Enterprises Private Limited

# Line-item-level custom field definitions (distinct from item-master custom fields).
# Confirmed from a live bill (SH26E0558) where a fully-populated line item carried these.
BILL_LINE_ITEM_CF_MFR_CODE = "3220178000000075182"  # api_name cf_item_code, label "Manufacturer Code"
BILL_LINE_ITEM_CF_SKU_CODE = "3220178000000075204"  # api_name cf_sku_code, label "SKU Code"


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


def _get_inventory_token() -> str:
    r = requests.post(
        _ZOHO_TOKEN_URL,
        params={
            "refresh_token": INVENTORY_REFRESH_TOKEN,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "refresh_token",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["access_token"]


_INVENTORY_TRANSIT_ACCOUNT_NAME = "Inventory Transit (Loss/Gain)"
_inventory_transit_account_id_cache: Optional[str] = None


def _get_inventory_transit_account_id() -> Optional[str]:
    """Resolve the 'Inventory Transit (Loss/Gain)' chart-of-accounts id (used on adjustment line
    items), looked up by name and cached for the process lifetime — avoids hardcoding an id that
    could silently point at the wrong GL account if it's ever recreated."""
    global _inventory_transit_account_id_cache
    if _inventory_transit_account_id_cache:
        return _inventory_transit_account_id_cache
    try:
        token = _get_zoho_token()
        r = requests.get(
            f"{ZOHO_BOOKS_BASE}/chartofaccounts",
            headers={"Authorization": f"Zoho-oauthtoken {token}"},
            params={"organization_id": ORGANIZATION_ID, "account_name": _INVENTORY_TRANSIT_ACCOUNT_NAME},
            timeout=30,
        )
        r.raise_for_status()
        accounts = r.json().get("chartofaccounts", [])
        for acc in accounts:
            if acc.get("account_name") == _INVENTORY_TRANSIT_ACCOUNT_NAME:
                _inventory_transit_account_id_cache = acc.get("account_id")
                return _inventory_transit_account_id_cache
    except Exception as e:
        logger.warning("Could not resolve '%s' account id: %s", _INVENTORY_TRANSIT_ACCOUNT_NAME, e)
    return None


def _load_products_by_item_id(db, item_ids: list[str]) -> dict[str, dict]:
    item_ids = [i for i in set(item_ids) if i]
    if not item_ids:
        return {}
    return {
        p["item_id"]: p
        for p in db.get_collection(PRODUCTS_COLLECTION).find(
            {"item_id": {"$in": item_ids}},
            {"item_id": 1, "cf_item_code": 1, "cf_sku_code": 1, "hsn_or_sac": 1},
        )
    }


def _line_item_custom_fields(prod: dict) -> list[dict]:
    return [
        {"customfield_id": BILL_LINE_ITEM_CF_MFR_CODE, "value": (prod or {}).get("cf_item_code", "") or ""},
        {"customfield_id": BILL_LINE_ITEM_CF_SKU_CODE, "value": (prod or {}).get("cf_sku_code", "") or ""},
    ]


def _needs_fix(bill: dict) -> bool:
    return any(not li.get("item_custom_fields") for li in bill.get("line_items", []))


# Zoho's bill-update API only accepts a small subset of the fields it returns on GET —
# echoing the full GET response back (e.g. purchaseorder_details, item_type_formatted,
# rate_formatted, ...) trips its own field-length/type validation on unrelated read-only
# fields. Keep only what's needed to preserve the line item and set the custom fields.
_LINE_ITEM_WRITABLE_FIELDS = (
    "line_item_id",
    "item_id",
    "name",
    "description",
    "quantity",
    "rate",
    "unit",
    "account_id",
    "hsn_or_sac",
    "tax_id",
    "item_custom_fields",
)


def _writable_line_item(li: dict) -> dict:
    return {k: li[k] for k in _LINE_ITEM_WRITABLE_FIELDS if k in li}


def _upsert_bill(db, bill: dict):
    if not bill or not bill.get("bill_id"):
        return
    db.get_collection(BILLS_COLLECTION).update_one(
        {"bill_id": bill["bill_id"]},
        {"$set": bill},
        upsert=True,
    )


def _upsert_inventory_adjustment(db, adjustment: dict):
    if not adjustment or not adjustment.get("inventory_adjustment_id"):
        return
    db.get_collection(INVENTORY_ADJUSTMENTS_COLLECTION).update_one(
        {"inventory_adjustment_id": adjustment["inventory_adjustment_id"]},
        {"$set": adjustment},
        upsert=True,
    )


def _link_adjustment_to_bill(db, bill_id: str, adjustment_id: str):
    db.get_collection(BILLS_COLLECTION).update_one(
        {"bill_id": bill_id},
        {"$addToSet": {"linked_inventory_adjustment_ids": adjustment_id}},
        upsert=True,
    )


def _linked_adjustments_for_bill(db, bill_id: str) -> list[dict]:
    bill_doc = db.get_collection(BILLS_COLLECTION).find_one({"bill_id": bill_id}, {"linked_inventory_adjustment_ids": 1})
    ids = (bill_doc or {}).get("linked_inventory_adjustment_ids") or []
    if not ids:
        return []
    docs = list(
        db.get_collection(INVENTORY_ADJUSTMENTS_COLLECTION).find(
            {"inventory_adjustment_id": {"$in": ids}},
            {
                "inventory_adjustment_id": 1,
                "reference_number": 1,
                "date": 1,
                "reason": 1,
                "status": 1,
                "adjustment_type": 1,
            },
        )
    )
    return serialize_mongo_document(docs)


def _vendor_brand_map(db) -> dict[str, list[str]]:
    """{vendor contact_id: [brand names]} for every vendor assigned to at least one brand."""
    result: dict[str, set[str]] = {}
    for b in db.get_collection("brands").find({}, {"name": 1, "vendor_ids": 1, "vendor_id": 1}):
        vids = b.get("vendor_ids") or ([b["vendor_id"]] if b.get("vendor_id") else [])
        name = b.get("name")
        if not name:
            continue
        for vid in vids:
            if not vid:
                continue
            result.setdefault(vid, set()).add(name)
    return {vid: sorted(names) for vid, names in result.items()}


@router.get("/list")
def list_bills(
    search: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    per_page: int = Query(25, ge=1, le=200),
    db=Depends(get_database),
):
    def _fetch():
        token = _get_zoho_token()
        headers = {"Authorization": f"Zoho-oauthtoken {token}"}
        params = {
            "organization_id": ORGANIZATION_ID,
            "page": page,
            "per_page": per_page,
            "sort_column": "date",
            "sort_order": "D",
        }
        if search:
            params["search_text"] = search
        r = requests.get(f"{ZOHO_BOOKS_BASE}/bills", headers=headers, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            raise ValueError(f"Zoho error: {data.get('message', 'Unknown error')}")
        return data.get("bills", []), data.get("page_context", {})

    try:
        bills, page_context = _fetch()
    except Exception as e:
        logger.error("Failed to list bills: %s", e)
        raise HTTPException(status_code=500, detail=str(e))

    return {"bills": bills, "page_context": page_context}


@router.get("/po-groups")
def po_groups(search: Optional[str] = Query(None), po: Optional[str] = Query(None), db=Depends(get_database)):
    """Vendors assigned to at least one brand, with their PO/order numbers and any linked bill(s)."""
    vendor_brands = _vendor_brand_map(db)
    vendor_ids = list(vendor_brands.keys())
    if not vendor_ids:
        return {"vendors": []}

    and_clauses: list[dict] = [{"vendor_id": {"$in": vendor_ids}}]
    if search:
        and_clauses.append(
            {
                "$or": [
                    {"purchaseorder_number": {"$regex": search, "$options": "i"}},
                    {"vendor_name": {"$regex": search, "$options": "i"}},
                ]
            }
        )
    query = {"$and": and_clauses} if len(and_clauses) > 1 else and_clauses[0]

    projection = {
        "purchaseorder_number": 1,
        "purchaseorder_id": 1,
        "vendor_id": 1,
        "vendor_name": 1,
        "date": 1,
        "total": 1,
        "currency_code": 1,
        "status": 1,
    }
    pos = list(db.get_collection(PURCHASE_ORDERS_COLLECTION).find(query, projection))

    # Deep-link guarantee: make sure the requested PO is present even if it fell outside the search filter.
    if po and not any(p.get("purchaseorder_number") == po for p in pos):
        extra = db.get_collection(PURCHASE_ORDERS_COLLECTION).find_one({"purchaseorder_number": po}, projection)
        if extra:
            pos.append(extra)

    # Join brand_orders (by purchaseorder_number) for the order's own name/number and created_at —
    # created_at drives sorting, matching BrandOrders.tsx's ordersByVendor/vendorList sort.
    po_numbers = [p["purchaseorder_number"] for p in pos if p.get("purchaseorder_number")]
    order_by_po: dict[str, dict] = {}
    if po_numbers:
        for o in db.get_collection(BRAND_ORDERS_COLLECTION).find(
            {"purchaseorder_number": {"$in": po_numbers}},
            {"purchaseorder_number": 1, "name": 1, "created_at": 1},
        ):
            pon = o.get("purchaseorder_number")
            if pon and pon not in order_by_po:
                order_by_po[pon] = o

    po_ids = [p["purchaseorder_id"] for p in pos if p.get("purchaseorder_id")]
    bills_by_po: dict[str, list[dict]] = {}
    if po_ids:
        for b in db.get_collection(BILLS_COLLECTION).find(
            {"purchaseorder_ids": {"$in": po_ids}},
            {
                "bill_id": 1,
                "bill_number": 1,
                "status": 1,
                "total": 1,
                "total_formatted": 1,
                "currency_code": 1,
                "currency_symbol": 1,
                "purchaseorder_ids": 1,
                "line_items": 1,
            },
        ):
            entry = {
                "bill_id": b.get("bill_id"),
                "bill_number": b.get("bill_number"),
                "status": b.get("status"),
                "total": b.get("total"),
                "total_formatted": b.get("total_formatted"),
                "currency_code": b.get("currency_code"),
                "currency_symbol": b.get("currency_symbol"),
                "needs_fix": _needs_fix(b),
            }
            for pid in b.get("purchaseorder_ids") or []:
                bills_by_po.setdefault(pid, []).append(entry)

    groups: dict[str, dict] = {}
    for p in pos:
        vid = p.get("vendor_id") or "__unknown__"
        group = groups.setdefault(
            vid,
            {
                "vendor_id": vid,
                "vendor_name": p.get("vendor_name") or "Unknown Vendor",
                "currency_code": p.get("currency_code"),
                "brands": vendor_brands.get(vid, []),
                "purchase_orders": [],
            },
        )
        order_info = order_by_po.get(p.get("purchaseorder_number")) or {}
        real_ts = order_info.get("created_at") or ""  # only counts if backed by an actual brand_orders doc
        sort_ts = real_ts or p.get("date") or ""  # PO date is just a display-order fallback within the vendor
        group["purchase_orders"].append(
            {
                "purchaseorder_number": p.get("purchaseorder_number"),
                "purchaseorder_id": p.get("purchaseorder_id"),
                "order_name": order_info.get("name"),
                "date": p.get("date"),
                "total": p.get("total"),
                "currency_code": p.get("currency_code"),
                "status": p.get("status"),
                "bills": bills_by_po.get(p.get("purchaseorder_id"), []),
                "_sort_ts": sort_ts,
                "_real_ts": real_ts,
            }
        )

    # Mirror BrandOrders.tsx's ordersByVendor/vendorList sort: most-recent order created_at first
    # (falling back to PO date within a vendor's own list when there's no matching brand_orders doc),
    # tie-break by vendor name asc. Vendor-level ranking uses ONLY real brand_orders created_at —
    # exactly like vendorList's aLatest/bLatest — so vendors whose POs never went through the Brand
    # Orders flow don't get bumped up the list by an unrelated PO date.
    for group in groups.values():
        group["purchase_orders"].sort(key=lambda e: str(e.get("_sort_ts") or ""), reverse=True)

    vendors = sorted(groups.values(), key=lambda g: g["vendor_name"] or "")
    vendors.sort(
        key=lambda g: max((str(po.get("_real_ts") or "") for po in g["purchase_orders"]), default=""),
        reverse=True,
    )
    for v in vendors:
        for e in v["purchase_orders"]:
            e.pop("_sort_ts", None)
            e.pop("_real_ts", None)

    return {"vendors": serialize_mongo_document(vendors)}


@router.get("/po-options")
def po_options(
    search: Optional[str] = Query(None),
    vendor_id: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=100),
    db=Depends(get_database),
):
    query: dict = {}
    if search:
        query["purchaseorder_number"] = {"$regex": search, "$options": "i"}
    if vendor_id:
        query["vendor_id"] = vendor_id

    docs = list(
        db.get_collection(PURCHASE_ORDERS_COLLECTION)
        .find(
            query,
            {
                "purchaseorder_number": 1,
                "purchaseorder_id": 1,
                "vendor_id": 1,
                "vendor_name": 1,
                "date": 1,
                "total": 1,
            },
        )
        .sort([("date", -1), ("_id", -1)])
        .limit(limit)
    )
    return {"options": serialize_mongo_document(docs)}


@router.get("/{bill_id}")
def get_bill(bill_id: str, db=Depends(get_database)):
    def _fetch():
        token = _get_zoho_token()
        headers = {"Authorization": f"Zoho-oauthtoken {token}"}
        r = requests.get(
            f"{ZOHO_BOOKS_BASE}/bills/{bill_id}",
            headers=headers,
            params={"organization_id": ORGANIZATION_ID},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            raise ValueError(f"Zoho error: {data.get('message', 'Unknown error')}")
        return data["bill"]

    try:
        bill = _fetch()
    except Exception as e:
        logger.error("Failed to fetch bill %s: %s", bill_id, e)
        raise HTTPException(status_code=500, detail=str(e))

    return {"bill": bill, "needs_fix": _needs_fix(bill)}


@router.post("/{bill_id}/fix-custom-fields")
def fix_custom_fields(bill_id: str, db=Depends(get_database)):
    def _do_fix():
        token = _get_zoho_token()
        headers = {"Authorization": f"Zoho-oauthtoken {token}"}

        r = requests.get(
            f"{ZOHO_BOOKS_BASE}/bills/{bill_id}",
            headers=headers,
            params={"organization_id": ORGANIZATION_ID},
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            raise ValueError(f"Zoho error: {data.get('message', 'Unknown error')}")
        bill = data["bill"]
        line_items = bill.get("line_items", [])

        empty_item_ids = [li["item_id"] for li in line_items if not li.get("item_custom_fields") and li.get("item_id")]
        if not empty_item_ids:
            return bill, 0

        prod_by_id = _load_products_by_item_id(db, empty_item_ids)

        fixed_count = 0
        for li in line_items:
            if li.get("item_custom_fields"):
                continue
            prod = prod_by_id.get(li.get("item_id"), {})
            li["item_custom_fields"] = _line_item_custom_fields(prod)
            if not li.get("hsn_or_sac"):
                li["hsn_or_sac"] = prod.get("hsn_or_sac", "")
            fixed_count += 1

        if fixed_count == 0:
            return bill, 0

        put_line_items = [_writable_line_item(li) for li in line_items]

        put_r = requests.put(
            f"{ZOHO_BOOKS_BASE}/bills/{bill_id}",
            headers=headers,
            json={"line_items": put_line_items},
            params={"organization_id": ORGANIZATION_ID},
            timeout=30,
        )
        logger.info("Zoho bill update response %s: %s", put_r.status_code, put_r.text)
        put_r.raise_for_status()
        put_data = put_r.json()
        if put_data.get("code") != 0:
            raise ValueError(f"Zoho error: {put_data.get('message', 'Unknown error')}")

        return put_data["bill"], fixed_count

    try:
        bill, fixed_count = _do_fix()
    except Exception as e:
        logger.error("Failed to fix custom fields for bill %s: %s", bill_id, e)
        raise HTTPException(status_code=500, detail=str(e))

    _upsert_bill(db, bill)
    return {"bill": bill, "fixed_count": fixed_count, "needs_fix": _needs_fix(bill)}


@router.post("/create")
def create_bill(
    purchaseorder_number: str = Body(..., embed=True),
    bill_number: str = Body(..., embed=True),
    date: str = Body(..., embed=True),
    db=Depends(get_database),
):
    po = db.get_collection(PURCHASE_ORDERS_COLLECTION).find_one(
        {"purchaseorder_number": purchaseorder_number},
        {"purchaseorder_id": 1, "vendor_id": 1},
    )
    if not po or not po.get("purchaseorder_id"):
        raise HTTPException(status_code=404, detail=f"Purchase order {purchaseorder_number} not found")

    brand_order = db.get_collection(BRAND_ORDERS_COLLECTION).find_one(
        {"purchaseorder_number": purchaseorder_number}, {"name": 1}
    )
    order_number = (brand_order or {}).get("name") or purchaseorder_number

    def _do_create():
        token = _get_zoho_token()
        headers = {"Authorization": f"Zoho-oauthtoken {token}"}

        po_r = requests.get(
            f"{ZOHO_BOOKS_BASE}/purchaseorders/{po['purchaseorder_id']}",
            headers=headers,
            params={"organization_id": ORGANIZATION_ID},
            timeout=30,
        )
        po_r.raise_for_status()
        po_data = po_r.json()
        if po_data.get("code") != 0:
            raise ValueError(f"Zoho error: {po_data.get('message', 'Unknown error')}")
        po_line_items = po_data["purchaseorder"].get("line_items", [])
        if not po_line_items:
            raise ValueError("Purchase order has no line items")

        prod_by_id = _load_products_by_item_id(db, [li.get("item_id") for li in po_line_items])

        line_items = []
        for pli in po_line_items:
            prod = prod_by_id.get(pli.get("item_id"), {})
            line_items.append(
                {
                    "item_id": pli["item_id"],
                    "quantity": pli.get("quantity_ordered") or pli.get("quantity") or 0,
                    "rate": pli.get("rate", 0),
                    "account_id": DEFAULT_ACCOUNT_ID,
                    "hsn_or_sac": pli.get("hsn_or_sac") or prod.get("hsn_or_sac", ""),
                    "item_custom_fields": _line_item_custom_fields(prod),
                }
            )

        payload = {
            "vendor_id": po.get("vendor_id"),
            "purchaseorder_ids": [po["purchaseorder_id"]],
            "bill_number": bill_number,
            "reference_number": order_number,
            "date": date,
            "line_items": line_items,
        }

        logger.info("Zoho bill create payload: %s", payload)
        r = requests.post(
            f"{ZOHO_BOOKS_BASE}/bills",
            headers=headers,
            json=payload,
            params={
                "organization_id": ORGANIZATION_ID,
                "ignore_auto_number_generation": "true",
            },
            timeout=30,
        )
        logger.info("Zoho bill create response %s: %s", r.status_code, r.text)
        r.raise_for_status()
        data = r.json()
        if data.get("code") != 0:
            raise ValueError(f"Zoho error: {data.get('message', 'Unknown error')}")
        return data["bill"]

    try:
        bill = _do_create()
    except Exception as e:
        logger.error("Failed to create bill for PO %s: %s", purchaseorder_number, e)
        raise HTTPException(status_code=500, detail=str(e))

    _upsert_bill(db, bill)
    return {"bill": bill}


# ─── Inventory Adjustments (warehouse short/extra count vs. a bill) ───────────

_IA_SHEETS = [("Short Received", "Short Qty"), ("Extra Received", "Extra Qty")]
_IA_HEADER_FILL = "FFC000"


def _style_ia_header(ws, col_count: int):
    fill = PatternFill("solid", fgColor=_IA_HEADER_FILL)
    font = Font(bold=True, size=11)
    for col in range(1, col_count + 1):
        cell = ws.cell(row=1, column=col)
        cell.fill = fill
        cell.font = font
        cell.alignment = Alignment(horizontal="center", vertical="center")


def _auto_col_width(ws):
    for col in ws.columns:
        max_len = max((len(str(c.value or "")) for c in col), default=0)
        ws.column_dimensions[get_column_letter(col[0].column)].width = min(max(max_len + 2, 10), 50)


def _fetch_bill(bill_id: str) -> dict:
    token = _get_zoho_token()
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    r = requests.get(
        f"{ZOHO_BOOKS_BASE}/bills/{bill_id}",
        headers=headers,
        params={"organization_id": ORGANIZATION_ID},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if data.get("code") != 0:
        raise ValueError(f"Zoho error: {data.get('message', 'Unknown error')}")
    return data["bill"]


@router.get("/{bill_id}/inventory-adjustment/template")
def inventory_adjustment_template(bill_id: str):
    try:
        bill = _fetch_bill(bill_id)
    except Exception as e:
        logger.error("Failed to fetch bill %s for IA template: %s", bill_id, e)
        raise HTTPException(status_code=500, detail=str(e))

    line_items = bill.get("line_items", [])

    wb = openpyxl.Workbook()
    wb.remove(wb.active)
    for sheet_name, qty_label in _IA_SHEETS:
        ws = wb.create_sheet(title=sheet_name)
        headers = ["Sr No", "SKU Code", "Product Name", qty_label]
        ws.append(headers)
        _style_ia_header(ws, len(headers))
        for idx, li in enumerate(line_items, start=1):
            ws.append([idx, li.get("sku", ""), li.get("name", ""), None])
        ws.freeze_panes = "A2"
        _auto_col_width(ws)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    filename = f"inventory_adjustment_{bill.get('bill_number', bill_id)}.xlsx"
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _parse_ia_workbook(wb, bill_line_items: list[dict]) -> dict:
    sku_to_item = {li.get("sku"): li for li in bill_line_items if li.get("sku")}

    def _parse_sheet(sheet_name: str):
        rows = []
        unresolved = []
        if sheet_name not in wb.sheetnames:
            return rows, unresolved
        ws = wb[sheet_name]
        for row in ws.iter_rows(min_row=2, values_only=True):
            if not row or len(row) < 4:
                continue
            _, sku, name, qty = row[0], row[1], row[2], row[3]
            sku = str(sku).strip() if sku is not None else ""
            if not sku or qty in (None, ""):
                continue
            try:
                qty = float(qty)
            except (TypeError, ValueError):
                continue
            if qty == 0:
                continue
            li = sku_to_item.get(sku)
            entry = {"sku": sku, "name": name or (li or {}).get("name", ""), "qty": abs(qty)}
            if li:
                entry["item_id"] = li.get("item_id")
                rows.append(entry)
            else:
                unresolved.append(entry)
        return rows, unresolved

    short_rows, short_unresolved = _parse_sheet("Short Received")
    extra_rows, extra_unresolved = _parse_sheet("Extra Received")
    return {
        "short_received": short_rows,
        "extra_received": extra_rows,
        "unresolved": short_unresolved + extra_unresolved,
    }


@router.post("/{bill_id}/inventory-adjustment/preview")
async def inventory_adjustment_preview(bill_id: str, file: UploadFile = File(...)):
    try:
        bill = _fetch_bill(bill_id)
    except Exception as e:
        logger.error("Failed to fetch bill %s for IA preview: %s", bill_id, e)
        raise HTTPException(status_code=500, detail=str(e))

    try:
        data = await file.read()
        wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
        result = _parse_ia_workbook(wb, bill.get("line_items", []))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse workbook: {e}")

    result["bill_number"] = bill.get("bill_number")
    return result


@router.post("/{bill_id}/inventory-adjustment/confirm")
async def inventory_adjustment_confirm(bill_id: str, file: UploadFile = File(...), db=Depends(get_database)):
    try:
        bill = _fetch_bill(bill_id)
    except Exception as e:
        logger.error("Failed to fetch bill %s for IA confirm: %s", bill_id, e)
        raise HTTPException(status_code=500, detail=str(e))

    try:
        data = await file.read()
        wb = openpyxl.load_workbook(io.BytesIO(data), data_only=True)
        parsed = _parse_ia_workbook(wb, bill.get("line_items", []))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to parse workbook: {e}")

    # Net short (-) vs extra (+) per item_id, in case the same SKU appears on both sheets.
    net_by_item: dict[str, float] = {}
    name_by_item: dict[str, str] = {}
    for row in parsed["short_received"]:
        net_by_item[row["item_id"]] = net_by_item.get(row["item_id"], 0) - row["qty"]
        name_by_item[row["item_id"]] = row["name"]
    for row in parsed["extra_received"]:
        net_by_item[row["item_id"]] = net_by_item.get(row["item_id"], 0) + row["qty"]
        name_by_item[row["item_id"]] = row["name"]

    adjustment_account_id = _get_inventory_transit_account_id()

    line_items = [
        {
            "item_id": item_id,
            "name": name_by_item.get(item_id, ""),
            "quantity_adjusted": qty,
            "location_id": DEFAULT_WAREHOUSE_ID,
            **({"adjustment_account_id": adjustment_account_id} if adjustment_account_id else {}),
        }
        for item_id, qty in net_by_item.items()
        if qty != 0
    ]
    if not line_items:
        raise HTTPException(status_code=400, detail="No non-zero short/extra quantities found in the uploaded sheet.")

    # Resolve the PO number + order number/name backing this bill, e.g. bill_number "SH26E0558",
    # po_number "PO-PETZOO066", order_name "66" → ref# "SH26E0558 ( PO-PETZOO066 )" and description
    # "Shortage & Excess Received in Order 66" — mirrors the manually-created adjustment format.
    po_number = None
    po_ids = bill.get("purchaseorder_ids") or []
    if po_ids:
        po_doc = db.get_collection(PURCHASE_ORDERS_COLLECTION).find_one(
            {"purchaseorder_id": po_ids[0]}, {"purchaseorder_number": 1}
        )
        po_number = (po_doc or {}).get("purchaseorder_number")

    order_name = None
    if po_number:
        brand_order = db.get_collection(BRAND_ORDERS_COLLECTION).find_one({"purchaseorder_number": po_number}, {"name": 1})
        order_name = (brand_order or {}).get("name")

    bill_number = bill.get("bill_number")
    reference_number = f"{bill_number} ( {po_number} )" if po_number else bill_number
    description = f"Shortage & Excess Received in Order {order_name or po_number or bill_number}"

    def _do_create():
        token = _get_inventory_token()
        headers = {"Authorization": f"Zoho-oauthtoken {token}"}
        payload = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "reason": "Shipment Inward adjustment",
            "description": description,
            "adjustment_type": "quantity",
            "reference_number": reference_number,
            "location_id": DEFAULT_WAREHOUSE_ID,
            "line_items": line_items,
        }
        logger.info("Zoho inventory adjustment payload: %s", payload)
        r = requests.post(
            f"{ZOHO_INVENTORY_BASE}/inventoryadjustments",
            headers=headers,
            json=payload,
            params={"organization_id": ORGANIZATION_ID},
            timeout=30,
        )
        logger.info("Zoho inventory adjustment response %s: %s", r.status_code, r.text)
        r.raise_for_status()
        resp_data = r.json()
        if resp_data.get("code") != 0:
            raise ValueError(f"Zoho error: {resp_data.get('message', 'Unknown error')}")
        return resp_data["inventory_adjustment"]

    try:
        adjustment = _do_create()
    except Exception as e:
        logger.error("Failed to create inventory adjustment for bill %s: %s", bill_id, e)
        raise HTTPException(status_code=500, detail=str(e))

    _upsert_inventory_adjustment(db, adjustment)
    if adjustment.get("inventory_adjustment_id"):
        _link_adjustment_to_bill(db, bill_id, adjustment["inventory_adjustment_id"])

    return {"inventory_adjustment": adjustment}


# ─── Linking existing inventory adjustments to a bill ─────────────────────────


@router.get("/inventory-adjustments/search")
def search_inventory_adjustments(search: Optional[str] = Query(None), limit: int = Query(20, ge=1, le=100), db=Depends(get_database)):
    query: dict = {}
    if search:
        query["$or"] = [
            {"reference_number": {"$regex": search, "$options": "i"}},
            {"reason": {"$regex": search, "$options": "i"}},
            {"inventory_adjustment_id": {"$regex": search, "$options": "i"}},
        ]
    docs = list(
        db.get_collection(INVENTORY_ADJUSTMENTS_COLLECTION)
        .find(query, {"inventory_adjustment_id": 1, "reference_number": 1, "date": 1, "reason": 1, "status": 1})
        .sort([("date", -1), ("_id", -1)])
        .limit(limit)
    )
    return {"options": serialize_mongo_document(docs)}


@router.get("/{bill_id}/inventory-adjustments")
def get_linked_inventory_adjustments(bill_id: str, db=Depends(get_database)):
    return {"inventory_adjustments": _linked_adjustments_for_bill(db, bill_id)}


@router.post("/{bill_id}/inventory-adjustment/link")
def link_inventory_adjustment(bill_id: str, inventory_adjustment_id: str = Body(..., embed=True), db=Depends(get_database)):
    if not db.get_collection(INVENTORY_ADJUSTMENTS_COLLECTION).find_one({"inventory_adjustment_id": inventory_adjustment_id}):
        raise HTTPException(status_code=404, detail="Inventory adjustment not found")
    _link_adjustment_to_bill(db, bill_id, inventory_adjustment_id)
    return {"inventory_adjustments": _linked_adjustments_for_bill(db, bill_id)}


@router.delete("/{bill_id}/inventory-adjustment/link/{inventory_adjustment_id}")
def unlink_inventory_adjustment(bill_id: str, inventory_adjustment_id: str, db=Depends(get_database)):
    db.get_collection(BILLS_COLLECTION).update_one(
        {"bill_id": bill_id},
        {"$pull": {"linked_inventory_adjustment_ids": inventory_adjustment_id}},
    )
    return {"inventory_adjustments": _linked_adjustments_for_bill(db, bill_id)}
