import io
import math
import os
import re
import zipfile
import logging
import asyncio
from datetime import datetime
from typing import Any, List, Optional

import openpyxl
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter

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
DESIGN_CATALOGUE_COLLECTION = "design_catalogue"
S3_BUCKET = os.getenv("S3_BUCKET", "pupscribe-purchases")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
S3_PUBLIC_URL = os.getenv("S3_PUBLIC_URL", f"https://{os.getenv('S3_BUCKET', 'pupscribe-purchases')}.s3.{os.getenv('AWS_REGION', 'ap-south-1')}.amazonaws.com")
DESIGNER_URL_EXPIRY = 7 * 24 * 3600  # 1 week
DEFAULT_CATEGORIES = ["products", "catalogue"]
MAX_IMAGE_MB = 10
MAX_VIDEO_MB = 200

# Public order-form bucket for product images/videos
PUBLIC_S3_BUCKET = os.getenv("PUBLIC_S3_BUCKET", "order-form")
PUBLIC_S3_REGION = os.getenv("PUBLIC_S3_REGION", "ap-south-1")
PUBLIC_S3_URL = os.getenv("PUBLIC_S3_URL", "https://assets.pupscribe.in")


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


def _public_s3_client():
    return boto3.client(
        "s3",
        region_name=PUBLIC_S3_REGION,
        aws_access_key_id=os.getenv("PUBLIC_S3_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("PUBLIC_S3_SECRET_KEY"),
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

_CATALOGUE_LOOKUP_FIELDS = {
    "bb_code": 1, "product_category": 1, "sub_category": 1,
    "mrp": 1, "dimensions": 1, "material": 1, "features": 1,
    "image_links": 1, "squeaker": 1, "catnip": 1,
    "age_group": 1, "pet_size": 1, "chewing_style": 1,
    "size_chart": 1, "ingredient_list": 1, "nutritional_analysis": 1,
    "hsn_code": 1, "gst_percentage": 1,
}


@router.get("/new-items")
def get_new_items(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: str = Query(None, description="Search by product name, SKU, or BB code"),
    brand: str = Query(None, description="Filter by brand"),
    zoho_status: str = Query(None, description="Filter by Zoho status (active/inactive)"),
    purchase_status: str = Query(None, description="Filter by purchase status"),
    category: str = Query(None, description="Filter by catalogue product_category"),
):
    """Returns non-combo products sorted by created_at desc, joined with catalogue data."""
    try:
        db = get_database()
        col = db[PRODUCTS_COLLECTION]

        query_filter: dict = {"is_combo_product": False}

        if search:
            rgx = {"$regex": re.escape(search), "$options": "i"}
            cat_ids = db[DESIGN_CATALOGUE_COLLECTION].distinct(
                "product_id", {"bb_code": rgx}
            )
            query_filter["$or"] = [
                {"name": rgx},
                {"cf_sku_code": rgx},
                {"sku": rgx},
                {"_id": {"$in": cat_ids}},
            ]

        if brand:
            query_filter["brand"] = brand
        if zoho_status:
            query_filter["status"] = zoho_status.lower()
        if purchase_status:
            query_filter["purchase_status"] = purchase_status
        if category:
            cat_ids_for_cat = db[DESIGN_CATALOGUE_COLLECTION].distinct(
                "product_id", {"product_category": category}
            )
            query_filter["_id"] = {"$in": cat_ids_for_cat}

        total_count = col.count_documents(query_filter)
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        skip = (page - 1) * limit

        pipeline = [
            {"$match": query_filter},
            {"$sort": {"created_at": -1}},
            {"$skip": skip},
            {"$limit": limit},
            {"$lookup": {
                "from": DESIGN_CATALOGUE_COLLECTION,
                "localField": "_id",
                "foreignField": "product_id",
                "as": "_cat",
                "pipeline": [{"$project": _CATALOGUE_LOOKUP_FIELDS}],
            }},
            {"$addFields": {"catalogue": {"$arrayElemAt": ["$_cat", 0]}}},
            {"$project": {"_cat": 0}},
        ]
        products = serialize_mongo_document(list(col.aggregate(pipeline)))

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
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in design/new-items: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ─── design catalogue ────────────────────────────────────────────────────────

class CatalogueItemPatch(BaseModel):
    image_links: Optional[List[str]] = None
    video_links: Optional[List[str]] = None
    features: Optional[List[Any]] = None
    ingredient_list: Optional[str] = None
    nutritional_analysis: Optional[str] = None
    age_group: Optional[str] = None
    pet_size: Optional[str] = None
    chewing_style: Optional[str] = None
    material: Optional[str] = None
    squeaker: Optional[bool] = None
    catnip: Optional[bool] = None
    size_chart: Optional[str] = None
    dimensions: Optional[dict] = None


@router.get("/catalogue-items")
def get_catalogue_items(
    page: int = Query(1, ge=1),
    limit: int = Query(24, ge=1, le=100),
    search: str = Query(None),
    category: str = Query(None),
    has_images: bool = Query(None),
):
    try:
        db = get_database()
        col = db[DESIGN_CATALOGUE_COLLECTION]

        q: dict = {}
        if search:
            rgx = {"$regex": re.escape(search), "$options": "i"}
            q["$or"] = [{"product_name": rgx}, {"bb_code": rgx}]
        if category:
            q["product_category"] = category
        if has_images is True:
            q["image_links.0"] = {"$exists": True}
        elif has_images is False:
            q["$or"] = [{"image_links": {"$exists": False}}, {"image_links": []}]

        total = col.count_documents(q)
        skip  = (page - 1) * limit

        pipeline = [
            {"$match": q},
            {"$sort": {"bb_code": 1}},
            {"$skip": skip},
            {"$limit": limit},
            {"$lookup": {
                "from": PRODUCTS_COLLECTION,
                "localField": "product_id",
                "foreignField": "_id",
                "as": "_prod",
                "pipeline": [{"$project": {"images": 1, "image_url": 1}}],
            }},
            {"$addFields": {
                "product_images":    {"$ifNull": [{"$arrayElemAt": ["$_prod.images", 0]}, []]},
                "product_image_url": {"$arrayElemAt": ["$_prod.image_url", 0]},
            }},
            {"$project": {"_prod": 0}},
        ]
        docs = list(col.aggregate(pipeline))

        return JSONResponse(content={
            "items": serialize_mongo_document(docs),
            "pagination": {
                "currentPage": page,
                "totalPages": math.ceil(total / limit) if total else 1,
                "total": total,
                "limit": limit,
            },
        })
    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalogue-items/categories")
def get_catalogue_categories():
    try:
        db  = get_database()
        cats = db[DESIGN_CATALOGUE_COLLECTION].distinct("product_category")
        return JSONResponse(content={"categories": sorted(c for c in cats if c)})
    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/catalogue-items/{bb_code}")
def patch_catalogue_item(bb_code: str, body: CatalogueItemPatch):
    try:
        db = get_database()
        update = body.dict(exclude_unset=True)
        if not update:
            raise HTTPException(status_code=400, detail="No fields provided")
        update["updated_at"] = datetime.utcnow()
        res = db[DESIGN_CATALOGUE_COLLECTION].update_one(
            {"bb_code": bb_code}, {"$set": update}
        )
        if res.matched_count == 0:
            raise HTTPException(status_code=404, detail="Catalogue item not found")
        return JSONResponse(content={"ok": True})
    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.patch("/products/{product_id}/images")
def patch_product_images(product_id: str, body: dict):
    """Update image_url (single), images array, and/or videos array for a product."""
    try:
        db = get_database()
        update: dict = {"updated_at": datetime.utcnow()}
        if "image_url" in body:
            update["image_url"] = body["image_url"] or None
        if "images" in body:
            if not isinstance(body["images"], list):
                raise HTTPException(status_code=400, detail="images must be a list")
            update["images"] = body["images"]
        if "videos" in body:
            if not isinstance(body["videos"], list):
                raise HTTPException(status_code=400, detail="videos must be a list")
            update["videos"] = body["videos"]
        if len(update) == 1:
            raise HTTPException(status_code=400, detail="No fields to update")
        db[PRODUCTS_COLLECTION].update_one(
            {"_id": ObjectId(product_id)},
            {"$set": update},
        )
        return JSONResponse(content={"ok": True})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/products/{product_id}/upload-image")
async def upload_product_image(product_id: str, file: UploadFile = File(...)):
    """Upload an image to the public S3 bucket and return its public URL."""
    if not file.content_type or not file.content_type.startswith("image/"):
        raise HTTPException(status_code=400, detail="File must be an image.")
    data = await file.read()
    if len(data) > MAX_IMAGE_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail=f"Image exceeds {MAX_IMAGE_MB} MB.")
    db = get_database()
    product = db[PRODUCTS_COLLECTION].find_one({"_id": ObjectId(product_id)}, {"item_id": 1, "images": 1, "image_url": 1})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found.")
    item_id = product.get("item_id") or product_id
    # sequential numbering: count existing images to pick next index
    existing_images = product.get("images") or []
    n = len(existing_images) + (1 if product.get("image_url") else 0) + 1
    ext = os.path.splitext(file.filename or "")[1] or ".jpg"
    key = f"product_images/{item_id}_{n}{ext}"
    try:
        _public_s3_client().put_object(
            Bucket=PUBLIC_S3_BUCKET, Key=key, Body=data,
            ContentType=file.content_type, ACL="public-read",
        )
    except Exception as e:
        logger.exception("S3 image upload failed: %s", e)
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {e}")
    return JSONResponse(content={"url": f"{PUBLIC_S3_URL}/{key}"})


@router.post("/products/{product_id}/upload-video")
async def upload_product_video(product_id: str, file: UploadFile = File(...)):
    """Upload a video to the public S3 bucket and return its public URL."""
    allowed = {"video/mp4", "video/quicktime", "video/webm", "video/x-msvideo", "video/mpeg"}
    if not file.content_type or file.content_type not in allowed:
        raise HTTPException(status_code=400, detail="File must be a supported video format (mp4, mov, webm, avi, mpeg).")
    data = await file.read()
    if len(data) > MAX_VIDEO_MB * 1024 * 1024:
        raise HTTPException(status_code=400, detail=f"Video exceeds {MAX_VIDEO_MB} MB.")
    db = get_database()
    product = db[PRODUCTS_COLLECTION].find_one({"_id": ObjectId(product_id)}, {"item_id": 1, "videos": 1})
    if not product:
        raise HTTPException(status_code=404, detail="Product not found.")
    item_id = product.get("item_id") or product_id
    n = len(product.get("videos") or []) + 1
    ext = os.path.splitext(file.filename or "")[1] or ".mp4"
    key = f"product_videos/{item_id}_{n}{ext}"
    try:
        _public_s3_client().put_object(
            Bucket=PUBLIC_S3_BUCKET, Key=key, Body=data,
            ContentType=file.content_type, ACL="public-read",
        )
    except Exception as e:
        logger.exception("S3 video upload failed: %s", e)
        raise HTTPException(status_code=500, detail=f"S3 upload failed: {e}")
    return JSONResponse(content={"url": f"{PUBLIC_S3_URL}/{key}"})


# ─── XLSX helpers ─────────────────────────────────────────────────────────────

def _style_header_row(ws, row: int, col_count: int, fill_hex: str = "6B21A8"):
    fill = PatternFill("solid", fgColor=fill_hex)
    font = Font(bold=True, color="FFFFFF", size=10)
    for col in range(1, col_count + 1):
        cell = ws.cell(row=row, column=col)
        cell.fill = fill
        cell.font = font
        cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)


def _auto_col_width(ws):
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            try:
                val = str(cell.value or "")
                max_len = max(max_len, len(val))
            except Exception:
                pass
        ws.column_dimensions[col_letter].width = min(max(max_len + 2, 10), 60)


def _make_unified_xlsx(products: list) -> io.BytesIO:
    """XLSX combining products fields + joined catalogue fields."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Products"

    headers = [
        "S.No", "Name", "SKU Code", "BB Code", "Brand",
        "Category", "Sub Category", "MRP (₹)",
        "Zoho Status", "Purchase Status",
        "Material", "Age Group", "Pet Size", "Chewing Style",
        "Squeaker", "Catnip",
        "L w/o Pkg (cm)", "B w/o Pkg (cm)", "H w/o Pkg (cm)", "Net Weight (g)",
        "Features",
        "Drive Image Links",
        "Image URL", "OF Images Count", "OF Videos Count",
        "Created", "Updated",
    ]
    ws.append(headers)
    _style_header_row(ws, 1, len(headers))
    ws.freeze_panes = "A2"

    thin = Side(style="thin", color="D1D5DB")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for i, p in enumerate(products, 1):
        cat = p.get("catalogue") or {}
        d_no = (cat.get("dimensions") or {}).get("without_packaging") or {}
        features = [f for f in (cat.get("features") or []) if f]
        of_imgs = p.get("images") or []
        of_vids = p.get("videos") or []
        ws.append([
            i,
            p.get("name") or "",
            p.get("cf_sku_code") or "",
            cat.get("bb_code") or "",
            p.get("brand") or "",
            cat.get("product_category") or "",
            cat.get("sub_category") or "",
            p.get("rate"),
            p.get("status") or "",
            p.get("purchase_status") or "",
            cat.get("material") or "",
            cat.get("age_group") or "",
            cat.get("pet_size") or "",
            cat.get("chewing_style") or "",
            "Yes" if cat.get("squeaker") else "",
            "Yes" if cat.get("catnip") else "",
            d_no.get("length_cm"),
            d_no.get("breadth_cm"),
            d_no.get("height_cm"),
            d_no.get("net_weight_g"),
            " | ".join(features),
            " | ".join(cat.get("image_links") or []),
            p.get("image_url") or "",
            len(of_imgs),
            len(of_vids),
            str(p.get("created_at") or "")[:10],
            str(p.get("updated_at") or "")[:10],
        ])
        for col_idx in range(1, len(headers) + 1):
            ws.cell(row=i + 1, column=col_idx).border = border
            ws.cell(row=i + 1, column=col_idx).alignment = Alignment(vertical="center")

    _auto_col_width(ws)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def _make_catalogue_xlsx(items: list) -> io.BytesIO:
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Catalogue"

    headers = [
        "S.No", "BB Code", "Product Name", "Category", "Sub Category",
        "MRP (₹)", "HSN Code", "GST %",
        "Material", "Age Group", "Pet Size", "Chewing Style",
        "Squeaker", "Catnip",
        "L w/o Pkg (cm)", "B w/o Pkg (cm)", "H w/o Pkg (cm)", "Net Weight (g)",
        "L w/ Pkg (cm)", "B w/ Pkg (cm)", "H w/ Pkg (cm)", "Gross Weight (g)",
        "Features",
        "Ingredient List", "Nutritional Analysis",
        "Image Links (Drive)", "Product Image URL",
    ]
    ws.append(headers)
    _style_header_row(ws, 1, len(headers))
    ws.freeze_panes = "A2"

    thin = Side(style="thin", color="D1D5DB")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for i, it in enumerate(items, 1):
        d_no = (it.get("dimensions") or {}).get("without_packaging") or {}
        d_wi = (it.get("dimensions") or {}).get("with_packaging") or {}
        features = [f for f in (it.get("features") or []) if f]
        ws.append([
            i,
            it.get("bb_code") or "",
            it.get("product_name") or "",
            it.get("product_category") or "",
            it.get("sub_category") or "",
            it.get("mrp"),
            it.get("hsn_code") or "",
            it.get("gst_percentage"),
            it.get("material") or "",
            it.get("age_group") or "",
            it.get("pet_size") or "",
            it.get("chewing_style") or "",
            "Yes" if it.get("squeaker") else "",
            "Yes" if it.get("catnip") else "",
            d_no.get("length_cm"),
            d_no.get("breadth_cm"),
            d_no.get("height_cm"),
            d_no.get("net_weight_g"),
            d_wi.get("length_cm"),
            d_wi.get("breadth_cm"),
            d_wi.get("height_cm"),
            d_wi.get("gross_weight_g"),
            " | ".join(features),
            it.get("ingredient_list") or "",
            it.get("nutritional_analysis") or "",
            " | ".join(it.get("image_links") or []),
            it.get("product_image_url") or "",
        ])
        for col in range(1, len(headers) + 1):
            c = ws.cell(row=i + 1, column=col)
            c.border = border
            c.alignment = Alignment(vertical="center", wrap_text=False)

    _auto_col_width(ws)
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


# ─── XLSX download endpoints ──────────────────────────────────────────────────

@router.get("/new-items/download")
def download_products_xlsx(
    search: str = Query(None),
    brand: str = Query(None),
    zoho_status: str = Query(None),
    purchase_status: str = Query(None),
    category: str = Query(None),
):
    """Download all matching products (with catalogue join) as XLSX."""
    try:
        db = get_database()
        col = db[PRODUCTS_COLLECTION]
        q: dict = {"is_combo_product": False}
        if search:
            rgx = {"$regex": re.escape(search), "$options": "i"}
            cat_ids = db[DESIGN_CATALOGUE_COLLECTION].distinct("product_id", {"bb_code": rgx})
            q["$or"] = [{"name": rgx}, {"cf_sku_code": rgx}, {"sku": rgx}, {"_id": {"$in": cat_ids}}]
        if brand:
            q["brand"] = brand
        if zoho_status:
            q["status"] = zoho_status.lower()
        if purchase_status:
            q["purchase_status"] = purchase_status
        if category:
            cat_ids_for_cat = db[DESIGN_CATALOGUE_COLLECTION].distinct("product_id", {"product_category": category})
            q["_id"] = {"$in": cat_ids_for_cat}

        pipeline = [
            {"$match": q},
            {"$sort": {"created_at": -1}},
            {"$lookup": {
                "from": DESIGN_CATALOGUE_COLLECTION,
                "localField": "_id",
                "foreignField": "product_id",
                "as": "_cat",
                "pipeline": [{"$project": _CATALOGUE_LOOKUP_FIELDS}],
            }},
            {"$addFields": {"catalogue": {"$arrayElemAt": ["$_cat", 0]}}},
            {"$project": {"_cat": 0}},
        ]
        products = serialize_mongo_document(list(col.aggregate(pipeline)))
        buf = _make_unified_xlsx(products)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="products.xlsx"'},
        )
    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalogue-items/download")
def download_catalogue_xlsx(
    search: str = Query(None),
    category: str = Query(None),
    has_images: bool = Query(None),
):
    """Download all matching catalogue items as XLSX (ignores pagination)."""
    try:
        db = get_database()
        col = db[DESIGN_CATALOGUE_COLLECTION]
        q: dict = {}
        if search:
            rgx = {"$regex": re.escape(search), "$options": "i"}
            q["$or"] = [{"product_name": rgx}, {"bb_code": rgx}]
        if category:
            q["product_category"] = category
        if has_images is True:
            q["image_links.0"] = {"$exists": True}
        elif has_images is False:
            q["$or"] = [{"image_links": {"$exists": False}}, {"image_links": []}]

        pipeline = [
            {"$match": q},
            {"$sort": {"bb_code": 1}},
            {"$lookup": {
                "from": PRODUCTS_COLLECTION,
                "localField": "product_id",
                "foreignField": "_id",
                "as": "_prod",
                "pipeline": [{"$project": {"image_url": 1}}],
            }},
            {"$addFields": {
                "product_image_url": {"$arrayElemAt": ["$_prod.image_url", 0]},
            }},
            {"$project": {"_prod": 0}},
        ]
        items = serialize_mongo_document(list(col.aggregate(pipeline)))
        buf = _make_catalogue_xlsx(items)
        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": 'attachment; filename="catalogue.xlsx"'},
        )
    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=str(e))


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
