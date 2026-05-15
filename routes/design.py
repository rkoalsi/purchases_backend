import io
import math
import os
import re
import zipfile
import logging
import asyncio
from datetime import datetime
from typing import Optional

import boto3
from botocore.config import Config as BotocoreConfig
from bson import ObjectId
from fastapi import APIRouter, Depends, File, Form, HTTPException, Query, UploadFile, status
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel
from pymongo.errors import PyMongoError

from ..database import get_database, serialize_mongo_document

router = APIRouter()
logger = logging.getLogger(__name__)

PRODUCTS_COLLECTION = "products"
BRAND_ORDERS_COLLECTION = "brand_orders"
CATEGORIES_COLLECTION = "designer_upload_categories"
PO_COLLECTION = "purchase_orders"
VENDORS_COLLECTION = "vendors"
BRANDS_COLLECTION = "brands"
S3_BUCKET = os.getenv("S3_BUCKET", "pupscribe-purchases")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
DESIGNER_URL_EXPIRY = 7 * 24 * 3600  # 1 week
DEFAULT_CATEGORIES = ["products", "catalogue"]


class AddCategoryRequest(BaseModel):
    name: str


def _slugify(text: str) -> str:
    text = text.lower().strip()
    text = re.sub(r"[^\w\s-]", "", text)
    text = re.sub(r"[\s_-]+", "_", text)
    return text


def _s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        endpoint_url=f"https://s3.{AWS_REGION}.amazonaws.com",
        config=BotocoreConfig(signature_version="s3v4"),
    )


def _upload_to_s3(file_bytes: bytes, s3_key: str, content_type: str) -> None:
    boto3.client("s3", region_name=AWS_REGION).put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=file_bytes,
        ContentType=content_type,
    )


def _presign_s3(s3_key: str, expires: int = DESIGNER_URL_EXPIRY) -> str:
    return _s3_client().generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": s3_key},
        ExpiresIn=expires,
    )


def _delete_from_s3(s3_key: str) -> None:
    boto3.client("s3", region_name=AWS_REGION).delete_object(
        Bucket=S3_BUCKET, Key=s3_key
    )


# ─── new-items (existing endpoint) ───────────────────────────────────────────

@router.get("/new-items")
def get_new_items(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: str = Query(None, description="Search by product name or cf_sku_code"),
    brand: str = Query(None, description="Filter by brand"),
    zoho_status: str = Query(None, description="Filter by Zoho status (active/inactive)"),
    purchase_status: str = Query(None, description="Filter by purchase status"),
):
    """Returns non-combo products sorted by created_at descending (newest first)."""
    try:
        db = get_database()
        collection = db[PRODUCTS_COLLECTION]

        query_filter: dict = {"is_combo_product": False}

        if search:
            search_regex = {"$regex": re.escape(search), "$options": "i"}
            query_filter["$or"] = [
                {"name": search_regex},
                {"cf_sku_code": search_regex},
                {"sku": search_regex},
            ]

        if brand:
            query_filter["brand"] = brand

        if zoho_status:
            query_filter["status"] = zoho_status.lower()

        if purchase_status:
            query_filter["purchase_status"] = purchase_status

        total_count = collection.count_documents(query_filter)
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        skip = (page - 1) * limit

        cursor = (
            collection.find(query_filter)
            .sort("created_at", -1)
            .skip(skip)
            .limit(limit)
        )
        products = serialize_mongo_document(list(cursor))

        return JSONResponse(content={
            "products": products,
            "pagination": {
                "currentPage": page,
                "totalPages": total_pages,
                "totalProducts": total_count,
                "limit": limit,
            },
        })

    except PyMongoError as e:
        logger.error(f"DB error in design/new-items: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        )
    except Exception as e:
        logger.error(f"Unexpected error in design/new-items: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e),
        )


# ─── designer orders ──────────────────────────────────────────────────────────

@router.get("/documents/search")
async def search_designer_documents(q: str, db=Depends(get_database)):
    """Search designer_documents filenames across all brand orders."""
    def _search():
        pipeline = [
            {"$unwind": "$designer_documents"},
            {"$match": {"designer_documents.filename": {"$regex": re.escape(q.strip()), "$options": "i"}}},
            {"$project": {
                "order_id": {"$toString": "$_id"},
                "order_name": "$name",
                "brand": "$brand",
                "doc": "$designer_documents",
            }},
            {"$limit": 25},
        ]
        return list(db[BRAND_ORDERS_COLLECTION].aggregate(pipeline))

    results = await asyncio.to_thread(_search)
    return serialize_mongo_document(results)


@router.get("/vendor-brands")
async def get_vendor_brands(db=Depends(get_database)):
    """Return a map of vendor_id → [brand names] from the brands collection."""
    def _fetch():
        result: dict[str, list[str]] = {}
        for brand in db[BRANDS_COLLECTION].find({"vendor_id": {"$exists": True, "$ne": None}}, {"name": 1, "vendor_id": 1}):
            vid = brand.get("vendor_id")
            name = brand.get("name")
            if not vid or not name:
                continue
            result.setdefault(vid, [])
            if name not in result[vid]:
                result[vid].append(name)
        for vid in result:
            result[vid].sort()
        return result
    return await asyncio.to_thread(_fetch)


@router.get("/orders")
async def list_designer_orders(db=Depends(get_database)):
    """Return all brand orders with designer_documents, omitting purchase team documents."""
    def _fetch():
        pipeline = [
            {"$addFields": {"doc_count": {"$size": {"$ifNull": ["$designer_documents", []]}}}},
            {"$project": {"documents": 0}},
            {"$lookup": {
                "from": VENDORS_COLLECTION,
                "localField": "vendor_id",
                "foreignField": "contact_id",
                "as": "_vendor",
            }},
            {"$addFields": {
                "vendor_name": {"$arrayElemAt": ["$_vendor.contact_name", 0]},
            }},
            {"$project": {"_vendor": 0}},
            {"$addFields": {
                "_sort_num": {
                    "$convert": {
                        "input": {"$trim": {"input": {"$arrayElemAt": [{"$split": ["$name", "#"]}, 1]}}},
                        "to": "int",
                        "onError": 0,
                        "onNull": 0,
                    }
                }
            }},
            {"$sort": {"brand": 1, "_sort_num": -1}},
            {"$project": {"_sort_num": 0}},
        ]
        return list(db[BRAND_ORDERS_COLLECTION].aggregate(pipeline))

    orders = await asyncio.to_thread(_fetch)
    return serialize_mongo_document(orders)


@router.get("/categories")
async def get_designer_categories(db=Depends(get_database)):
    def _fetch():
        cats = list(db[CATEGORIES_COLLECTION].find({}, {"name": 1, "_id": 0}))
        if not cats:
            db[CATEGORIES_COLLECTION].insert_many([{"name": n} for n in DEFAULT_CATEGORIES])
            return DEFAULT_CATEGORIES
        return [c["name"] for c in cats]
    return await asyncio.to_thread(_fetch)


@router.post("/categories")
async def add_designer_category(request: AddCategoryRequest, db=Depends(get_database)):
    name = request.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Category name required")
    def _add():
        if not db[CATEGORIES_COLLECTION].find_one({"name": name}):
            db[CATEGORIES_COLLECTION].insert_one({"name": name})
    await asyncio.to_thread(_add)
    return {"name": name}


@router.delete("/categories/{name}")
async def delete_designer_category(name: str, db=Depends(get_database)):
    def _delete():
        result = db[CATEGORIES_COLLECTION].delete_one({"name": name})
        return result.deleted_count
    deleted = await asyncio.to_thread(_delete)
    if not deleted:
        raise HTTPException(status_code=404, detail="Category not found")
    return {"name": name, "deleted": True}


@router.get("/{order_id}/line-items")
async def get_order_line_items(order_id: str, db=Depends(get_database)):
    def _fetch():
        order = db[BRAND_ORDERS_COLLECTION].find_one(
            {"_id": ObjectId(order_id)}, {"purchaseorder_number": 1}
        )
        if not order or not order.get("purchaseorder_number"):
            return []
        po = db[PO_COLLECTION].find_one(
            {"purchaseorder_number": order["purchaseorder_number"]},
            {"line_items": 1}
        )
        if not po:
            return []
        return po.get("line_items", [])
    items = await asyncio.to_thread(_fetch)
    return serialize_mongo_document(items)


@router.post("/{order_id}/designer-documents")
async def upload_designer_document(
    order_id: str,
    file: UploadFile = File(...),
    relative_path: Optional[str] = Form(None),
    category: Optional[str] = Form(None),
    item_id: Optional[str] = Form(None),
    item_name: Optional[str] = Form(None),
    db=Depends(get_database),
):
    file_bytes = await file.read()
    filename = file.filename or "file"
    content_type = file.content_type or "application/octet-stream"
    cat = (category or "general").strip()

    def _process():
        doc = db[BRAND_ORDERS_COLLECTION].find_one(
            {"_id": ObjectId(order_id)}, {"brand": 1, "name": 1}
        )
        if not doc:
            raise ValueError("Order not found")

        brand_slug = _slugify(doc["brand"])
        name_slug = _slugify(doc["name"])
        cat_slug = _slugify(cat)

        if relative_path:
            parts = relative_path.replace("\\", "/").strip("/").split("/")
            safe_parts = [re.sub(r"[^\w.\-]", "_", p) for p in parts if p]
            s3_key = f"designer_uploads/{brand_slug}/{name_slug}/{cat_slug}/{'/'.join(safe_parts)}"
        else:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = re.sub(r"[^\w.\-]", "_", filename)
            s3_key = f"designer_uploads/{brand_slug}/{name_slug}/{cat_slug}/{ts}_{safe_filename}"

        _upload_to_s3(file_bytes, s3_key, content_type)

        doc_entry: dict = {
            "doc_id": str(ObjectId()),
            "filename": filename,
            "s3_key": s3_key,
            "content_type": content_type,
            "size": len(file_bytes),
            "uploaded_at": datetime.now(),
            "category": cat,
        }
        if item_id:
            doc_entry["item_id"] = item_id
        if item_name:
            doc_entry["item_name"] = item_name

        db[BRAND_ORDERS_COLLECTION].update_one(
            {"_id": ObjectId(order_id)},
            {
                "$push": {"designer_documents": doc_entry},
                "$set": {"updated_at": datetime.now()},
            },
        )
        return doc_entry

    try:
        doc_entry = await asyncio.to_thread(_process)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("upload_designer_document failed")
        raise HTTPException(status_code=500, detail=str(e))

    return serialize_mongo_document(doc_entry)


@router.get("/{order_id}/designer-documents/zip")
async def download_designer_documents_zip(
    order_id: str,
    item_id: Optional[str] = Query(None),
    db=Depends(get_database),
):
    def _fetch_docs():
        doc = db[BRAND_ORDERS_COLLECTION].find_one(
            {"_id": ObjectId(order_id)},
            {"designer_documents": 1, "name": 1, "brand": 1},
        )
        if not doc:
            raise ValueError("Order not found")
        return doc

    try:
        order_doc = await asyncio.to_thread(_fetch_docs)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    documents = order_doc.get("designer_documents", [])
    if item_id:
        documents = [d for d in documents if d.get("item_id") == item_id]
    if not documents:
        raise HTTPException(status_code=404, detail="No designer documents to download")

    def _build_zip():
        s3 = boto3.client("s3", region_name=AWS_REGION)
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for d in documents:
                s3_key = d.get("s3_key")
                if not s3_key:
                    continue
                try:
                    obj = s3.get_object(Bucket=S3_BUCKET, Key=s3_key)
                    data = obj["Body"].read()
                    arcname = d.get("filename") or s3_key.split("/")[-1]
                    zf.writestr(arcname, data)
                except Exception:
                    logger.warning("Skipping %s in zip — download failed", s3_key)
        buf.seek(0)
        return buf

    zip_buf = await asyncio.to_thread(_build_zip)
    safe_name = re.sub(r"[^\w.\-]", "_", order_doc.get("name", order_id))
    return StreamingResponse(
        zip_buf,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{safe_name}_designer.zip"'},
    )


@router.patch("/{order_id}/designer-documents/{doc_id}")
async def update_designer_document(order_id: str, doc_id: str, body: dict, db=Depends(get_database)):
    category = (body.get("category") or "").strip()
    if not category:
        raise HTTPException(status_code=400, detail="category is required")
    def _update():
        result = db[BRAND_ORDERS_COLLECTION].update_one(
            {"_id": ObjectId(order_id), "designer_documents.doc_id": doc_id},
            {"$set": {"designer_documents.$.category": category, "updated_at": datetime.now()}},
        )
        return result.matched_count
    matched = await asyncio.to_thread(_update)
    if not matched:
        raise HTTPException(status_code=404, detail="Document not found")
    return {"doc_id": doc_id, "category": category}


@router.get("/{order_id}/designer-documents/{doc_id}/url")
async def get_designer_document_url(order_id: str, doc_id: str, db=Depends(get_database)):
    def _fetch():
        doc = db[BRAND_ORDERS_COLLECTION].find_one(
            {"_id": ObjectId(order_id), "designer_documents.doc_id": doc_id},
            {"designer_documents.$": 1},
        )
        if not doc or not doc.get("designer_documents"):
            return None
        return doc["designer_documents"][0].get("s3_key")

    s3_key = await asyncio.to_thread(_fetch)
    if not s3_key:
        raise HTTPException(status_code=404, detail="Document not found")

    try:
        url = await asyncio.to_thread(_presign_s3, s3_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"url": url}


@router.delete("/{order_id}/designer-documents/{doc_id}")
async def delete_designer_document(order_id: str, doc_id: str, db=Depends(get_database)):
    def _delete():
        doc = db[BRAND_ORDERS_COLLECTION].find_one(
            {"_id": ObjectId(order_id), "designer_documents.doc_id": doc_id},
            {"designer_documents.$": 1},
        )
        if not doc or not doc.get("designer_documents"):
            return False
        s3_key = doc["designer_documents"][0].get("s3_key")
        if s3_key:
            try:
                _delete_from_s3(s3_key)
            except Exception:
                pass
        db[BRAND_ORDERS_COLLECTION].update_one(
            {"_id": ObjectId(order_id)},
            {
                "$pull": {"designer_documents": {"doc_id": doc_id}},
                "$set": {"updated_at": datetime.now()},
            },
        )
        return True

    found = await asyncio.to_thread(_delete)
    if not found:
        raise HTTPException(status_code=404, detail="Document not found")
    return {"order_id": order_id, "doc_id": doc_id, "deleted": True}
