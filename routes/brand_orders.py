from fastapi import APIRouter, HTTPException, Depends, UploadFile, File, Form
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime, timedelta
from bson import ObjectId
from ..database import get_database, serialize_mongo_document
import asyncio
import io
import os
import re
import zipfile
import boto3
from botocore.config import Config as BotocoreConfig
from typing import Optional
import logging

logger = logging.getLogger(__name__)
router = APIRouter()

COLLECTION = "brand_orders"
PO_COLLECTION = "purchase_orders"
VENDORS_COLLECTION = "vendors"
S3_BUCKET = os.getenv("S3_BUCKET", "pupscribe-purchases")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")


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


def _presign_s3(s3_key: str, expires: int = 3600) -> str:
    return _s3_client().generate_presigned_url(
        "get_object",
        Params={"Bucket": S3_BUCKET, "Key": s3_key},
        ExpiresIn=expires,
    )


def _delete_from_s3(s3_key: str) -> None:
    boto3.client("s3", region_name=AWS_REGION).delete_object(
        Bucket=S3_BUCKET, Key=s3_key
    )


# ─── orders CRUD ──────────────────────────────────────────────────────────────

def _validate_optional_dates(fields_map: dict) -> dict:
    out = {}
    for field, val in fields_map.items():
        if val:
            try:
                datetime.strptime(val, "%Y-%m-%d")
                out[field] = val
            except ValueError:
                raise HTTPException(status_code=400, detail=f"{field} must be YYYY-MM-DD")
        else:
            out[field] = None
    return out


@router.post("/")
async def create_order(
    brand: str = Form(...),
    vendor_id: Optional[str] = Form(None),
    order_date: Optional[str] = Form(None),
    shipment_eta: Optional[str] = Form(None),
    purchaseorder_number: Optional[str] = Form(None),
    initiation_date: Optional[str] = Form(None),
    proforma_date: Optional[str] = Form(None),
    ready_date: Optional[str] = Form(None),
    etd_date: Optional[str] = Form(None),
    eta_port_date: Optional[str] = Form(None),
    duty_payment_date: Optional[str] = Form(None),
    inward_date: Optional[str] = Form(None),
    db=Depends(get_database),
):
    if order_date:
        try:
            datetime.strptime(order_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="order_date must be YYYY-MM-DD")
    if shipment_eta:
        try:
            datetime.strptime(shipment_eta, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="shipment_eta must be YYYY-MM-DD")

    extra_dates = _validate_optional_dates({
        "initiation_date": initiation_date,
        "proforma_date": proforma_date,
        "ready_date": ready_date,
        "etd_date": etd_date,
        "eta_port_date": eta_port_date,
        "duty_payment_date": duty_payment_date,
        "inward_date": inward_date,
    })

    def _insert():
        brand_name = brand.strip()
        vid = (vendor_id or "").strip() or None
        count = db[COLLECTION].count_documents({"brand": brand_name, "vendor_id": vid})
        name = f"Order #{count + 1}"
        now = datetime.now()
        po_num = (purchaseorder_number or "").strip() or None
        doc = {
            "brand": brand_name,
            "vendor_id": vid,
            "name": name,
            "order_date": order_date or None,
            "shipment_eta": shipment_eta or None,
            "purchaseorder_number": po_num,
            "documents": [],
            "created_at": now,
            "updated_at": now,
            **extra_dates,
        }
        result = db[COLLECTION].insert_one(doc)
        return str(result.inserted_id), doc

    order_id, doc = await asyncio.to_thread(_insert)
    return JSONResponse(status_code=201, content={"_id": order_id, **serialize_mongo_document(doc)})


@router.get("/")
async def list_orders(brand: Optional[str] = None, db=Depends(get_database)):
    def _fetch():
        query = {"brand": brand} if brand else {}
        pipeline = [
            {"$match": query},
            {"$addFields": {"doc_count": {"$size": {"$ifNull": ["$documents", []]}}}},
            {"$project": {"documents": 0}},
            {"$lookup": {
                "from": PO_COLLECTION,
                "localField": "purchaseorder_number",
                "foreignField": "purchaseorder_number",
                "as": "_po",
            }},
            {"$lookup": {
                "from": VENDORS_COLLECTION,
                "localField": "vendor_id",
                "foreignField": "contact_id",
                "as": "_vendor",
            }},
            {"$addFields": {
                "po_status": {
                    "$let": {
                        "vars": {
                            "fmt": {"$arrayElemAt": ["$_po.order_status_formatted", 0]},
                            "raw": {"$arrayElemAt": ["$_po.order_status", 0]},
                        },
                        "in": {
                            "$cond": {
                                "if": {"$gt": [{"$strLenCP": {"$ifNull": ["$$fmt", ""]}}, 0]},
                                "then": "$$fmt",
                                "else": {
                                    "$cond": {
                                        "if": {"$gt": [{"$strLenCP": {"$ifNull": ["$$raw", ""]}}, 0]},
                                        "then": {"$concat": [
                                            {"$toUpper": {"$substrCP": ["$$raw", 0, 1]}},
                                            {"$substrCP": ["$$raw", 1, {"$subtract": [{"$strLenCP": "$$raw"}, 1]}]},
                                        ]},
                                        "else": None,
                                    }
                                },
                            }
                        },
                    }
                },
                "po_currency_code": {"$arrayElemAt": ["$_po.currency_code", 0]},
                "vendor_name": {"$arrayElemAt": ["$_vendor.contact_name", 0]},
            }},
            {"$project": {"_po": 0, "_vendor": 0}},
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
            {"$sort": {"_sort_num": -1}},
            {"$project": {"_sort_num": 0}},
        ]
        return list(db[COLLECTION].aggregate(pipeline))

    orders = await asyncio.to_thread(_fetch)
    return serialize_mongo_document(orders)


@router.get("/{order_id}")
async def get_order(order_id: str, db=Depends(get_database)):
    def _fetch():
        pipeline = [
            {"$match": {"_id": ObjectId(order_id)}},
            {"$lookup": {
                "from": PO_COLLECTION,
                "localField": "purchaseorder_number",
                "foreignField": "purchaseorder_number",
                "as": "_po",
            }},
            {"$lookup": {
                "from": VENDORS_COLLECTION,
                "localField": "vendor_id",
                "foreignField": "contact_id",
                "as": "_vendor",
            }},
            {"$addFields": {
                "po_status": {
                    "$let": {
                        "vars": {
                            "fmt": {"$arrayElemAt": ["$_po.order_status_formatted", 0]},
                            "raw": {"$arrayElemAt": ["$_po.order_status", 0]},
                        },
                        "in": {
                            "$cond": {
                                "if": {"$gt": [{"$strLenCP": {"$ifNull": ["$$fmt", ""]}}, 0]},
                                "then": "$$fmt",
                                "else": {
                                    "$cond": {
                                        "if": {"$gt": [{"$strLenCP": {"$ifNull": ["$$raw", ""]}}, 0]},
                                        "then": {"$concat": [
                                            {"$toUpper": {"$substrCP": ["$$raw", 0, 1]}},
                                            {"$substrCP": ["$$raw", 1, {"$subtract": [{"$strLenCP": "$$raw"}, 1]}]},
                                        ]},
                                        "else": None,
                                    }
                                },
                            }
                        },
                    }
                },
                "po_currency_code": {"$arrayElemAt": ["$_po.currency_code", 0]},
                "vendor_name": {"$arrayElemAt": ["$_vendor.contact_name", 0]},
            }},
            {"$project": {"_po": 0, "_vendor": 0}},
        ]
        docs = list(db[COLLECTION].aggregate(pipeline))
        return docs[0] if docs else None

    doc = await asyncio.to_thread(_fetch)
    if not doc:
        raise HTTPException(status_code=404, detail="Order not found")
    return serialize_mongo_document(doc)


@router.patch("/{order_id}")
async def update_order(
    order_id: str,
    brand: Optional[str] = Form(None),
    name: Optional[str] = Form(None),
    vendor_id: Optional[str] = Form(None),
    order_date: Optional[str] = Form(None),
    shipment_eta: Optional[str] = Form(None),
    purchaseorder_number: Optional[str] = Form(None),
    initiation_date: Optional[str] = Form(None),
    proforma_date: Optional[str] = Form(None),
    ready_date: Optional[str] = Form(None),
    etd_date: Optional[str] = Form(None),
    eta_port_date: Optional[str] = Form(None),
    duty_payment_date: Optional[str] = Form(None),
    inward_date: Optional[str] = Form(None),
    db=Depends(get_database),
):
    fields: dict = {"updated_at": datetime.now()}
    if brand is not None:
        fields["brand"] = brand.strip()
    if name is not None:
        fields["name"] = name.strip()
    if order_date is not None:
        if order_date:
            try:
                datetime.strptime(order_date, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="order_date must be YYYY-MM-DD")
        fields["order_date"] = order_date or None
    if shipment_eta is not None:
        if shipment_eta:
            try:
                datetime.strptime(shipment_eta, "%Y-%m-%d")
            except ValueError:
                raise HTTPException(status_code=400, detail="shipment_eta must be YYYY-MM-DD")
        fields["shipment_eta"] = shipment_eta or None
    if purchaseorder_number is not None:
        po_num = purchaseorder_number.strip()
        fields["purchaseorder_number"] = po_num or None
    if vendor_id is not None:
        fields["vendor_id"] = vendor_id.strip() or None
    for field, val in [
        ("initiation_date", initiation_date),
        ("proforma_date", proforma_date),
        ("ready_date", ready_date),
        ("etd_date", etd_date),
        ("eta_port_date", eta_port_date),
        ("duty_payment_date", duty_payment_date),
        ("inward_date", inward_date),
    ]:
        if val is not None:
            if val:
                try:
                    datetime.strptime(val, "%Y-%m-%d")
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"{field} must be YYYY-MM-DD")
            fields[field] = val or None

    def _update():
        result = db[COLLECTION].update_one(
            {"_id": ObjectId(order_id)}, {"$set": fields}
        )
        return result.matched_count

    matched = await asyncio.to_thread(_update)
    if not matched:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"_id": order_id, **{k: v for k, v in fields.items() if k != "updated_at"}}


@router.delete("/{order_id}")
async def delete_order(order_id: str, db=Depends(get_database)):
    def _delete():
        doc = db[COLLECTION].find_one({"_id": ObjectId(order_id)}, {"documents": 1})
        if not doc:
            return None
        s3 = boto3.client("s3", region_name=AWS_REGION)
        for d in doc.get("documents", []):
            if d.get("s3_key"):
                try:
                    s3.delete_object(Bucket=S3_BUCKET, Key=d["s3_key"])
                except Exception:
                    pass
        db[COLLECTION].delete_one({"_id": ObjectId(order_id)})
        return True

    result = await asyncio.to_thread(_delete)
    if result is None:
        raise HTTPException(status_code=404, detail="Order not found")
    return {"_id": order_id, "deleted": True}


# ─── backfill vendor IDs ──────────────────────────────────────────────────────

@router.post("/backfill-vendor-ids")
async def backfill_vendor_ids(db=Depends(get_database)):
    """Assign vendor_id to brand orders missing one, by matching PO currency to the brand's vendors."""
    def _run():
        updated = 0
        orders = list(db[COLLECTION].find(
            {"vendor_id": {"$in": [None, ""]}, "purchaseorder_number": {"$ne": None}},
            {"_id": 1, "brand": 1, "purchaseorder_number": 1},
        ))

        for order in orders:
            po_num = order.get("purchaseorder_number")
            if not po_num:
                continue

            po = db[PO_COLLECTION].find_one({"purchaseorder_number": po_num}, {"currency_code": 1})
            if not po:
                continue
            currency = po.get("currency_code")

            brand_doc = db["brands"].find_one({"name": order["brand"]}, {"vendor_id": 1, "vendor_ids": 1})
            if not brand_doc:
                continue

            vendor_ids = brand_doc.get("vendor_ids") or []
            if not vendor_ids and brand_doc.get("vendor_id"):
                vendor_ids = [brand_doc["vendor_id"]]
            if not vendor_ids:
                continue

            matched = None
            if currency:
                for vid in vendor_ids:
                    v = db[VENDORS_COLLECTION].find_one({"contact_id": vid}, {"contact_id": 1, "currency_code": 1})
                    if v and v.get("currency_code") == currency:
                        matched = v["contact_id"]
                        break

            if not matched and len(vendor_ids) == 1:
                matched = vendor_ids[0]

            if matched:
                db[COLLECTION].update_one(
                    {"_id": order["_id"]},
                    {"$set": {"vendor_id": matched, "updated_at": datetime.now()}},
                )
                updated += 1

        return updated

    count = await asyncio.to_thread(_run)
    return {"backfilled": count}


# ─── PO search ────────────────────────────────────────────────────────────────

@router.get("/po/search")
async def search_po(q: str, db=Depends(get_database)):
    """Search purchase_orders by purchaseorder_number prefix, excluding POs already attached to an order."""
    def _search():
        used_pos = {
            o["purchaseorder_number"]
            for o in db[COLLECTION].find(
                {"purchaseorder_number": {"$nin": [None, ""]}},
                {"purchaseorder_number": 1, "_id": 0},
            )
            if o.get("purchaseorder_number")
        }
        match_filter: dict = {
            "purchaseorder_number": {"$regex": f"^{re.escape(q.strip())}", "$options": "i"},
        }
        if used_pos:
            match_filter["purchaseorder_number"]["$nin"] = list(used_pos)
        pipeline = [
            {"$match": match_filter},
            {"$addFields": {
                "order_status_formatted": {
                    "$cond": {
                        "if": {"$gt": [{"$strLenCP": {"$ifNull": ["$order_status_formatted", ""]}}, 0]},
                        "then": "$order_status_formatted",
                        "else": {
                            "$cond": {
                                "if": {"$gt": [{"$strLenCP": {"$ifNull": ["$order_status", ""]}}, 0]},
                                "then": {
                                    "$concat": [
                                        {"$toUpper": {"$substrCP": ["$order_status", 0, 1]}},
                                        {"$substrCP": ["$order_status", 1, {"$subtract": [{"$strLenCP": "$order_status"}, 1]}]},
                                    ]
                                },
                                "else": "",
                            }
                        },
                    }
                }
            }},
            {"$project": {
                "purchaseorder_number": 1,
                "order_status_formatted": 1,
                "vendor_id": 1,
                "bill_date": {"$arrayElemAt": ["$bills.date", 0]},
                "po_date": {
                    "$cond": {
                        "if": {"$eq": [{"$type": "$date"}, "date"]},
                        "then": {"$dateToString": {"format": "%Y-%m-%d", "date": "$date"}},
                        "else": "$date",
                    }
                },
                "_id": 0,
            }},
            {"$limit": 10},
        ]
        return list(db[PO_COLLECTION].aggregate(pipeline))

    results = await asyncio.to_thread(_search)
    return serialize_mongo_document(results)


# ─── document search ──────────────────────────────────────────────────────────

@router.get("/documents/search")
async def search_documents(q: str, db=Depends(get_database)):
    """Search document filenames across all brand orders."""
    def _search():
        pipeline = [
            {"$unwind": "$documents"},
            {"$match": {"documents.filename": {"$regex": re.escape(q.strip()), "$options": "i"}}},
            {"$project": {
                "order_id": {"$toString": "$_id"},
                "order_name": "$name",
                "brand": "$brand",
                "doc": "$documents",
            }},
            {"$limit": 25},
        ]
        return list(db[COLLECTION].aggregate(pipeline))

    results = await asyncio.to_thread(_search)
    return serialize_mongo_document(results)


# ─── documents ────────────────────────────────────────────────────────────────

@router.post("/{order_id}/documents")
async def upload_document(
    order_id: str,
    file: UploadFile = File(...),
    relative_path: Optional[str] = Form(None),
    db=Depends(get_database),
):
    file_bytes = await file.read()
    filename = file.filename or "document"
    content_type = file.content_type or "application/octet-stream"

    def _process():
        doc = db[COLLECTION].find_one({"_id": ObjectId(order_id)}, {"brand": 1, "name": 1})
        if not doc:
            raise ValueError("Order not found")

        brand_slug = _slugify(doc["brand"])
        name_slug = _slugify(doc["name"])

        if relative_path:
            parts = relative_path.replace("\\", "/").strip("/").split("/")
            safe_parts = [re.sub(r"[^\w.\-]", "_", p) for p in parts if p]
            s3_key = f"brand_orders/{brand_slug}/{name_slug}/{'/'.join(safe_parts)}"
        else:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            safe_filename = re.sub(r"[^\w.\-]", "_", filename)
            s3_key = f"brand_orders/{brand_slug}/{name_slug}/{ts}_{safe_filename}"

        _upload_to_s3(file_bytes, s3_key, content_type)

        doc_entry = {
            "doc_id": str(ObjectId()),
            "filename": filename,
            "s3_key": s3_key,
            "content_type": content_type,
            "size": len(file_bytes),
            "uploaded_at": datetime.now(),
        }

        db[COLLECTION].update_one(
            {"_id": ObjectId(order_id)},
            {
                "$push": {"documents": doc_entry},
                "$set": {"updated_at": datetime.now()},
            },
        )
        return doc_entry

    try:
        doc_entry = await asyncio.to_thread(_process)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.exception("upload_document failed")
        raise HTTPException(status_code=500, detail=str(e))

    return serialize_mongo_document(doc_entry)


@router.get("/{order_id}/documents/zip")
async def download_documents_zip(order_id: str, db=Depends(get_database)):
    """Download all documents for an order as a ZIP archive."""
    def _fetch_docs():
        doc = db[COLLECTION].find_one({"_id": ObjectId(order_id)}, {"documents": 1, "name": 1, "brand": 1})
        if not doc:
            raise ValueError("Order not found")
        return doc

    try:
        order_doc = await asyncio.to_thread(_fetch_docs)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    documents = order_doc.get("documents", [])
    if not documents:
        raise HTTPException(status_code=404, detail="No documents to download")

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
                    # Preserve relative path inside zip if key contains subfolders beyond order prefix
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
        headers={"Content-Disposition": f'attachment; filename="{safe_name}.zip"'},
    )


@router.get("/{order_id}/documents/{doc_id}/url")
async def get_document_url(order_id: str, doc_id: str, db=Depends(get_database)):
    def _fetch():
        doc = db[COLLECTION].find_one(
            {"_id": ObjectId(order_id), "documents.doc_id": doc_id},
            {"documents.$": 1},
        )
        if not doc or not doc.get("documents"):
            return None
        return doc["documents"][0].get("s3_key")

    s3_key = await asyncio.to_thread(_fetch)
    if not s3_key:
        raise HTTPException(status_code=404, detail="Document not found")

    try:
        url = await asyncio.to_thread(_presign_s3, s3_key)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    return {"url": url}


@router.delete("/{order_id}/documents/{doc_id}")
async def delete_document(order_id: str, doc_id: str, db=Depends(get_database)):
    def _delete():
        doc = db[COLLECTION].find_one(
            {"_id": ObjectId(order_id), "documents.doc_id": doc_id},
            {"documents.$": 1},
        )
        if not doc or not doc.get("documents"):
            return False
        s3_key = doc["documents"][0].get("s3_key")
        if s3_key:
            try:
                _delete_from_s3(s3_key)
            except Exception:
                pass
        db[COLLECTION].update_one(
            {"_id": ObjectId(order_id)},
            {
                "$pull": {"documents": {"doc_id": doc_id}},
                "$set": {"updated_at": datetime.now()},
            },
        )
        return True

    found = await asyncio.to_thread(_delete)
    if not found:
        raise HTTPException(status_code=404, detail="Document not found")
    return {"order_id": order_id, "doc_id": doc_id, "deleted": True}
