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
from pymongo.errors import PyMongoError

from ..database import get_database, serialize_mongo_document

router = APIRouter()
logger = logging.getLogger(__name__)

PRODUCTS_COLLECTION = "products"
BRAND_ORDERS_COLLECTION = "brand_orders"
S3_BUCKET = os.getenv("S3_BUCKET", "pupscribe-purchases")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
DESIGNER_URL_EXPIRY = 7 * 24 * 3600  # 1 week


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


@router.get("/orders")
async def list_designer_orders(db=Depends(get_database)):
    """Return all brand orders with designer_documents, omitting purchase team documents."""
    def _fetch():
        pipeline = [
            {"$addFields": {"doc_count": {"$size": {"$ifNull": ["$designer_documents", []]}}}},
            {"$project": {"documents": 0}},
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


@router.post("/{order_id}/designer-documents")
async def upload_designer_document(
    order_id: str,
    file: UploadFile = File(...),
    relative_path: Optional[str] = Form(None),
    db=Depends(get_database),
):
    file_bytes = await file.read()
    filename = file.filename or "file"
    content_type = file.content_type or "application/octet-stream"

    def _process():
        doc = db[BRAND_ORDERS_COLLECTION].find_one(
            {"_id": ObjectId(order_id)}, {"brand": 1, "name": 1}
        )
        if not doc:
            raise ValueError("Order not found")

        brand_slug = _slugify(doc["brand"])
        name_slug = _slugify(doc["name"])

        if relative_path:
            parts = relative_path.replace("\\", "/").strip("/").split("/")
            safe_parts = [re.sub(r"[^\w.\-]", "_", p) for p in parts if p]
            s3_key = f"designer_uploads/{brand_slug}/{name_slug}/{'/'.join(safe_parts)}"
        else:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = re.sub(r"[^\w.\-]", "_", filename)
            s3_key = f"designer_uploads/{brand_slug}/{name_slug}/{ts}_{safe_filename}"

        _upload_to_s3(file_bytes, s3_key, content_type)

        doc_entry = {
            "doc_id": str(ObjectId()),
            "filename": filename,
            "s3_key": s3_key,
            "content_type": content_type,
            "size": len(file_bytes),
            "uploaded_at": datetime.now(),
        }

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
async def download_designer_documents_zip(order_id: str, db=Depends(get_database)):
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
