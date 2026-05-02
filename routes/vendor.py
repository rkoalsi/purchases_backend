from fastapi import APIRouter, HTTPException, Depends, Query, Body, UploadFile, File
from fastapi.responses import StreamingResponse, JSONResponse
import io
import os
import re
import asyncio
import logging
import requests
import pandas as pd
from functools import lru_cache
from openpyxl import Workbook, load_workbook
import openpyxl
from openpyxl.utils.dataframe import dataframe_to_rows
from datetime import datetime
from typing import List, Dict, Optional
from pydantic import BaseModel
from ..database import get_database, serialize_mongo_document

router = APIRouter()
logger = logging.getLogger(__name__)

ORGANIZATION_ID = os.getenv("ORGANIZATION_ID", "776755316")
BOOKS_URL = os.getenv("BOOKS_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
BOOKS_REFRESH_TOKEN = os.getenv("BOOKS_REFRESH_TOKEN")
ZOHO_BOOKS_BASE = "https://books.zoho.com/api/v3"

PRODUCTS_COLLECTION = "products"
PURCHASE_ORDERS_COLLECTION = "purchase_orders"
DRAFT_PURCHASE_ORDERS_COLLECTION = "draft_purchase_orders"


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


def _parse_draft_order_excel(file_bytes: bytes) -> list[dict]:
    """Parse draft order Excel. Columns: Manufacturer Code, BBCode, Item Name, Qty, Unit Price, ..."""
    wb = openpyxl.load_workbook(io.BytesIO(file_bytes), data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if len(rows) < 2:
        raise ValueError("Order file has no data rows")

    items = []
    for row in rows[1:]:
        if not row[0] and not row[1]:
            continue
        qty = row[3]
        if not isinstance(qty, (int, float)) or qty <= 0:
            continue
        items.append(
            {
                "manufacturer_code": str(row[0]).strip() if row[0] else "",
                "bb_code": str(row[1]).strip() if row[1] else "",
                "item_name": str(row[2]).strip() if row[2] else "",
                "qty": int(qty),
                "unit_price": float(row[4]) if row[4] is not None else 0.0,
            }
        )
    return items


class CreateDraftPORequest(BaseModel):
    contact_id: str
    date: str
    items: List[dict]
    notes: Optional[str] = ""
    reference_number: Optional[str] = ""
    purchaseorder_number: Optional[str] = ""
    description: Optional[str] = ""
    draft_id: Optional[str] = None


class SaveDraftOrderRequest(BaseModel):
    description: Optional[str] = ""
    items: List[dict]
    detected_vendor: Optional[dict] = None
    date: str
    notes: Optional[str] = ""
    reference_number: Optional[str] = ""
    purchaseorder_number: Optional[str] = ""


@router.get("/brands")
def get_available_brands(db=Depends(get_database)):
    """Get list of available brands with vendor contact_name"""
    try:
        brands_collection = db.get_collection("brands")

        pipeline = [
            {
                "$lookup": {
                    "from": "vendors",
                    "localField": "vendor_id",
                    "foreignField": "vendor_id",  # adjust if your vendors use different field
                    "as": "vendor_data",
                }
            },
            {"$unwind": {"path": "$vendor_data", "preserveNullAndEmptyArrays": True}},
            {
                "$addFields": {
                    "vendor_name": "$vendor_data.contact_name",
                    "vendor_currency_code": "$vendor_data.currency_code",
                    "vendor_currency_symbol": "$vendor_data.currency_symbol",
                    "vendor_billing_address": "$vendor_data.billing_address",
                    "vendor_shipping_address": "$vendor_data.shipping_address",
                }
            },
            {"$project": {"vendor_data": 0}},  # remove joined object from response
            {"$sort": {"name": 1}},
        ]

        brands = list(brands_collection.aggregate(pipeline))

        return {"brands": serialize_mongo_document(brands)}

    except Exception as e:
        logger.error(f"Error getting brands: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving brands")


@router.get("")
def get_vendors(
    page: int = Query(1, ge=1),
    page_size: int = Query(10, ge=1, le=100),
    search: str | None = Query(None, description="Search by contact_name"),
    db=Depends(get_database),
):
    """Get paginated list of vendors with optional search on contact_name"""
    try:
        vendors_collection = db.get_collection("vendors")

        # Build filter
        query = {}
        if search:
            query["contact_name"] = {
                "$regex": re.escape(search),
                "$options": "i",  # case-insensitive
            }

        skip = (page - 1) * page_size

        vendors_cursor = vendors_collection.find(query).skip(skip).limit(page_size)
        vendors = list(vendors_cursor)

        total_count = vendors_collection.count_documents(query)

        return {
            "page": page,
            "page_size": page_size,
            "total": total_count,
            "total_pages": (total_count + page_size - 1) // page_size,
            "search": search,
            "vendors": serialize_mongo_document(vendors),
        }

    except Exception as e:
        logger.error(f"Error getting vendors: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving vendors")


@router.put("/brands/vendor")
def update_brand_vendor(
    name: str = Body(..., embed=True),
    vendor_id: str = Body(..., embed=True),
    db=Depends(get_database),
):
    """Update vendor_id in brands collection by brand name"""
    try:
        brands_collection = db.get_collection("brands")

        # Case-insensitive search on name
        query = {"name": {"$regex": f"^{re.escape(name)}$", "$options": "i"}}

        update = {"$set": {"vendor_id": vendor_id}}

        result = brands_collection.update_many(query, update)

        if result.matched_count == 0:
            raise HTTPException(
                status_code=404, detail="No brand found with given name"
            )

        return {
            "message": "Vendor ID updated successfully",
            "matched_count": result.matched_count,
            "modified_count": result.modified_count,
        }

    except Exception as e:
        logger.error(f"Error updating brand vendor_id: {e}")
        raise HTTPException(status_code=500, detail="Error updating brand")


@router.post("/draft_orders/validate")
async def validate_draft_order(
    file: UploadFile = File(...),
    db=Depends(get_database),
):
    """Parse draft order Excel and check all items exist in the product database."""
    file_bytes = await file.read()

    def _process():
        items = _parse_draft_order_excel(file_bytes)
        if not items:
            raise ValueError("No valid data rows found in file")

        bb_codes = [it["bb_code"] for it in items if it["bb_code"]]
        mfr_codes = [it["manufacturer_code"] for it in items if it["manufacturer_code"]]

        products_col = db.get_collection(PRODUCTS_COLLECTION)

        PRODUCT_PROJECTION = {
            "cf_sku_code": 1, "cf_item_code": 1, "item_id": 1,
            "name": 1, "item_name": 1, "brand": 1,
            "hsn_or_sac": 1, "purchase_account_id": 1,
        }

        products_by_bb: dict[str, dict] = {}
        for p in products_col.find({"cf_sku_code": {"$in": bb_codes}}, PRODUCT_PROJECTION):
            products_by_bb[p["cf_sku_code"]] = p

        remaining_mfr = [
            it["manufacturer_code"]
            for it in items
            if it["manufacturer_code"]
            and (not it["bb_code"] or it["bb_code"] not in products_by_bb)
        ]
        products_by_mfr: dict[str, dict] = {}
        if remaining_mfr:
            for p in products_col.find({"cf_item_code": {"$in": remaining_mfr}}, PRODUCT_PROJECTION):
                products_by_mfr[p.get("cf_item_code", "")] = p

        validated = []
        missing = []
        first_brand: str | None = None
        for it in items:
            product = None
            if it["bb_code"] and it["bb_code"] in products_by_bb:
                product = products_by_bb[it["bb_code"]]
            elif it["manufacturer_code"] and it["manufacturer_code"] in products_by_mfr:
                product = products_by_mfr[it["manufacturer_code"]]

            if product:
                brand = product.get("brand") or ""
                if brand and first_brand is None:
                    first_brand = brand
                validated.append(
                    {
                        **it,
                        "item_id": product.get("item_id", ""),
                        "product_name": product.get("name") or product.get("item_name", ""),
                        "hsn_or_sac": product.get("hsn_or_sac", ""),
                        "purchase_account_id": product.get("purchase_account_id", ""),
                    }
                )
            else:
                missing.append(
                    {
                        "manufacturer_code": it["manufacturer_code"],
                        "bb_code": it["bb_code"],
                        "item_name": it["item_name"],
                    }
                )

        # Detect vendor from the first product's brand
        detected_vendor = None
        if first_brand:
            brand_doc = db.get_collection("brands").find_one(
                {"name": {"$regex": f"^{re.escape(first_brand)}$", "$options": "i"}},
                {"vendor_id": 1},
            )
            if brand_doc and brand_doc.get("vendor_id"):
                vendor_doc = db.get_collection("vendors").find_one(
                    {"contact_id": brand_doc["vendor_id"]},
                    {"vendor_id": 1, "contact_id": 1, "contact_name": 1, "currency_code": 1},
                )
                if vendor_doc:
                    detected_vendor = {
                        "contact_id": vendor_doc.get("contact_id", ""),
                        "contact_name": vendor_doc.get("contact_name", ""),
                        "currency_code": vendor_doc.get("currency_code", "USD"),
                    }

        return validated, missing, detected_vendor

    try:
        validated, missing, detected_vendor = await asyncio.to_thread(_process)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    if missing:
        return JSONResponse(
            status_code=200,
            content={
                "valid": False,
                "missing_items": missing,
                "found_count": len(validated),
                "missing_count": len(missing),
            },
        )

    return JSONResponse(
        status_code=200,
        content={
            "valid": True,
            "items": serialize_mongo_document(validated),
            "total_items": len(validated),
            "detected_vendor": detected_vendor,
        },
    )


@router.get("/draft_orders")
def list_draft_orders(db=Depends(get_database)):
    """List all saved draft purchase orders (excluding items for brevity)."""
    col = db.get_collection(DRAFT_PURCHASE_ORDERS_COLLECTION)
    docs = list(col.find({}, {"items": 1, "description": 1, "detected_vendor": 1, "date": 1, "item_count": 1, "po_created": 1, "po_number": 1, "po_status": 1, "notes": 1, "reference_number": 1, "purchaseorder_number": 1, "created_at": 1}).sort("created_at", -1).limit(100))
    return serialize_mongo_document(docs)


@router.post("/draft_orders/save")
async def save_draft_order(body: SaveDraftOrderRequest, db=Depends(get_database)):
    """Persist a validated draft order to the draft_purchase_orders collection."""
    def _insert():
        col = db.get_collection(DRAFT_PURCHASE_ORDERS_COLLECTION)
        doc = {
            "description": body.description or "",
            "items": body.items,
            "detected_vendor": body.detected_vendor,
            "date": body.date,
            "item_count": len(body.items),
            "po_created": False,
            "notes": body.notes or "",
            "reference_number": body.reference_number or "",
            "purchaseorder_number": body.purchaseorder_number or "",
            "created_at": datetime.now(),
            "updated_at": datetime.now(),
        }
        result = col.insert_one(doc)
        doc["_id"] = result.inserted_id
        return doc

    doc = await asyncio.to_thread(_insert)
    return JSONResponse(status_code=201, content=serialize_mongo_document(doc))


@router.delete("/draft_orders/{draft_id}")
async def delete_draft_order(draft_id: str, db=Depends(get_database)):
    """Delete a saved draft order by its MongoDB _id."""
    from bson import ObjectId
    try:
        oid = ObjectId(draft_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid draft_id")

    def _delete():
        col = db.get_collection(DRAFT_PURCHASE_ORDERS_COLLECTION)
        return col.delete_one({"_id": oid})

    result = await asyncio.to_thread(_delete)
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Draft order not found")
    return {"deleted": True}


@router.post("/draft_orders/create_po")
async def create_draft_order_po(
    body: CreateDraftPORequest,
    db=Depends(get_database),
):
    """Create a Zoho Books purchase order from a validated draft order and save it to the purchase_orders collection."""
    try:
        datetime.strptime(body.date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    if not body.contact_id:
        raise HTTPException(status_code=400, detail="contact_id must not be empty")
    if not body.items:
        raise HTTPException(status_code=400, detail="items must not be empty")

    def _create():
        token = _get_zoho_token()
        headers = {"Authorization": f"Zoho-oauthtoken {token}"}

        vendor_doc = db.get_collection("vendors").find_one(
            {"contact_id": body.contact_id},
            {
                "currency_id": 1, "gst_treatment": 1,
                "billing_address": 1, "shipping_address": 1,
                "destination_of_supply": 1,
            },
        )
        vd = vendor_doc or {}

        DEFAULT_ACCOUNT_ID = "3220178000000034001"
        DEFAULT_WAREHOUSE_ID = "3220178000000403010"

        line_items = [
            {
                "item_id": it["item_id"],
                "name": it.get("product_name", ""),
                "quantity": it["qty"],
                "rate": it["unit_price"],
                "account_id": it.get("purchase_account_id") or DEFAULT_ACCOUNT_ID,
                "hsn_or_sac": it.get("hsn_or_sac", ""),
                "warehouse_id": DEFAULT_WAREHOUSE_ID,
                "unit": "pcs",
                "tags": [],
                "item_custom_fields": [],
            }
            for it in body.items
            if it.get("item_id")
        ]

        billing_addr = vd.get("billing_address") or {}
        shipping_addr = vd.get("shipping_address") or {}

        payload: dict = {
            "vendor_id": body.contact_id,
            "date": body.date,
            "location_id": "3220178000143298047",
            "delivery_org_address_id": DEFAULT_WAREHOUSE_ID,
            "template_id": "3220178000000017007",
            "destination_of_supply": vd.get("destination_of_supply") or "MH",
            "gst_treatment": vd.get("gst_treatment", "overseas"),
            "gst_no": "",
            "is_inclusive_tax": False,
            "discount": 0,
            "discount_type": "entity_level",
            "is_discount_before_tax": True,
            "payment_terms": 0,
            "payment_terms_label": "Due on Receipt",
            "contact_persons": [],
            "custom_fields": [],
            "documents": [],
            "line_items": line_items,
        }
        if vd.get("currency_id"):
            payload["currency_id"] = vd["currency_id"]
        if billing_addr.get("address_id"):
            payload["billing_address_id"] = billing_addr["address_id"]
        if shipping_addr.get("address_id"):
            payload["shipping_address_id"] = shipping_addr["address_id"]
        if body.notes:
            payload["notes"] = body.notes
        if body.reference_number:
            payload["reference_number"] = body.reference_number
        if body.purchaseorder_number:
            payload["purchaseorder_number"] = body.purchaseorder_number

        logger.info("Zoho PO payload: %s", payload)
        r = requests.post(
            f"{ZOHO_BOOKS_BASE}/purchaseorders",
            headers=headers,
            json=payload,
            params={"organization_id": ORGANIZATION_ID},
            timeout=30,
        )
        logger.error("Zoho PO response %s: %s", r.status_code, r.text)
        r.raise_for_status()
        data = r.json()

        if data.get("code") != 0:
            raise ValueError(f"Zoho error: {data.get('message', 'Unknown error')}")

        po_doc = data["purchaseorder"]
        now = datetime.now()
        db[PURCHASE_ORDERS_COLLECTION].update_one(
            {"purchaseorder_id": po_doc.get("purchaseorder_id")},
            {
                "$set": {**po_doc, "updated_at": now},
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

        if body.draft_id:
            try:
                from bson import ObjectId
                db.get_collection(DRAFT_PURCHASE_ORDERS_COLLECTION).update_one(
                    {"_id": ObjectId(body.draft_id)},
                    {"$set": {
                        "po_created": True,
                        "po_number": po_doc.get("purchaseorder_number"),
                        "po_status": po_doc.get("status"),
                        "updated_at": now,
                    }},
                )
            except Exception:
                pass

        return po_doc

    try:
        po_doc = await asyncio.to_thread(_create)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except requests.HTTPError as e:
        body = e.response.text if e.response is not None else ""
        raise HTTPException(status_code=502, detail=f"Zoho API error: {e} — {body}")

    return JSONResponse(
        status_code=201,
        content=serialize_mongo_document(
            {
                "purchaseorder_id": po_doc.get("purchaseorder_id"),
                "purchaseorder_number": po_doc.get("purchaseorder_number"),
                "vendor_name": po_doc.get("vendor_name"),
                "date": po_doc.get("date"),
                "status": po_doc.get("status"),
                "total": po_doc.get("total"),
                "currency_code": po_doc.get("currency_code"),
            }
        ),
    )
