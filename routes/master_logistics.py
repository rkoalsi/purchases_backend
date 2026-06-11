from fastapi import (
    APIRouter,
    Body,
    HTTPException,
    status,
    Depends,
    Query,
    UploadFile,
    File,
)
from fastapi.responses import StreamingResponse, JSONResponse
from typing import Dict, Any
import io
import math
import logging
import pandas as pd
from ..database import get_database

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/transfer-customers")
def get_transfer_customers(db=Depends(get_database)):
    """Return transfer-order customer names from the transfer_order_customers collection."""
    col = db.get_collection("transfer_order_customers")
    names = [
        doc["name"] for doc in col.find({}, {"name": 1, "_id": 0}) if doc.get("name")
    ]
    return {"customers": names}


@router.post("/transfer-customers")
def add_transfer_customer(payload: dict = Body(...), db=Depends(get_database)):
    """Add a customer name to the transfer-order customers list."""
    name = (payload.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name is required")
    col = db.get_collection("transfer_order_customers")
    if col.find_one({"name": name}):
        raise HTTPException(status_code=409, detail="Customer already exists")
    col.insert_one({"name": name})
    return {"message": "Added", "name": name}


@router.delete("/transfer-customers/{name:path}")
def delete_transfer_customer(name: str, db=Depends(get_database)):
    """Remove a customer name from the transfer-order customers list."""
    col = db.get_collection("transfer_order_customers")
    result = col.delete_one({"name": name})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Customer not found")
    return {"message": "Deleted", "name": name}


@router.get("/brands")
def get_available_brands(db=Depends(get_database)):
    """Get list of available brands from products collection"""
    try:
        brands = db.get_collection("brands")
        brands = brands.find({}).distinct("name")
        return {
            "brands": [
                {"value": brand, "label": brand.title()} for brand in brands if brand
            ]
        }
    except Exception as e:
        logger.error(f"Error getting brands: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving brands")


@router.post("/import-product-logistics")
def import_product_logistics(
    db=Depends(get_database),
):
    """
    Import Status (Remark col), CBM, and Case Pack from two sheets in the product logistics
    Google Sheet: "Master" and "Discontinued Items". Row 0 is skipped (not headers); headers
    are on row 1. SKU matched via "SKU Code (Final)" column against products.cf_sku_code.

    Master sheet:       Col A = Remark/Status, Col DG (110) = CBM, Col DH (111) = Case Pack
    Discontinued sheet: Col A = Remark/Status, Col CY (102) = CBM, Col CZ (103) = Case Pack
    """
    import os
    import gspread
    from google.oauth2.service_account import Credentials

    SHEET_KEY = "1tn_Lj3KR0zXY8B-8ZUkSznZgE4YzyjtAkcpdHzBCgt4"

    # Sheet name -> (cbm_col_index, case_pack_col_index)
    SHEET_CONFIG = {
        "Master": (110, 111),
        "Discontinued Items": (102, 103),
    }

    def _map_status(raw: str) -> str:
        s = raw.strip().lower()
        if s == "active":
            return "active"
        elif s == "discontinued":
            return "inactive"
        elif "until stock" in s:
            return "discontinued until stock lasts"
        return ""

    def _to_float(val: str) -> float:
        try:
            return float(val) if val else 0
        except (ValueError, TypeError):
            return 0

    try:
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        creds_path = os.path.join(backend_dir, "creds.json")

        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(credentials)

        sh = gc.open_by_key(SHEET_KEY)
        products_collection = db.get_collection("products")

        updated_count = 0
        skipped_count = 0

        for sheet_name, (cbm_col, case_pack_col) in SHEET_CONFIG.items():
            ws = sh.worksheet(sheet_name)
            all_rows = ws.get_all_values()

            # Row 0 is skipped; row 1 is the header row
            if len(all_rows) < 2:
                continue

            header = [h.strip() for h in all_rows[1]]

            def _col(name: str) -> int:
                try:
                    return header.index(name)
                except ValueError:
                    return -1

            sku_idx = _col("SKU Code (Final)")
            if sku_idx == -1:
                logger.warning(
                    f"'SKU Code (Final)' column not found in sheet '{sheet_name}'"
                )
                continue

            for row in all_rows[2:]:  # skip row 0 (filler) and row 1 (header)
                if len(row) <= sku_idx:
                    continue

                sku_code = row[sku_idx].strip()
                if not sku_code:
                    continue

                raw_status = row[0].strip() if len(row) > 0 else ""
                cbm_raw = row[cbm_col] if len(row) > cbm_col else ""
                case_pack_raw = row[case_pack_col] if len(row) > case_pack_col else ""

                update_doc: dict = {
                    "cbm": _to_float(cbm_raw),
                    "case_pack": _to_float(case_pack_raw),
                }
                mapped_status = _map_status(raw_status)
                if mapped_status:
                    update_doc["purchase_status"] = mapped_status

                result = products_collection.update_many(
                    {"cf_sku_code": sku_code},
                    {"$set": update_doc},
                )

                if result.modified_count > 0 or result.matched_count > 0:
                    updated_count += 1
                else:
                    skipped_count += 1

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Imported status, CBM and Case Pack from product logistics sheet",
                "updated": updated_count,
                "skipped": skipped_count,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error importing product logistics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to import product logistics: {str(e)}",
        )


# ─── Brand Logistics CRUD ───────────────────────────────────────────


@router.get("/brand-logistics")
def get_brand_logistics(db=Depends(get_database)):
    """Get all brand logistics settings"""
    try:
        collection = db.get_collection("brand_logistics")
        docs = list(collection.find({}, {"_id": 0}))
        return JSONResponse(status_code=200, content={"data": docs})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/brand-logistics")
def create_brand_logistics(
    brand: str = Query(...),
    lead_time: float = Query(60),
    safety_days_fast: float = Query(40),
    safety_days_medium: float = Query(25),
    safety_days_slow: float = Query(15),
    order_processing: float = Query(10),
    db=Depends(get_database),
):
    """Create or update brand logistics settings"""
    try:
        collection = db.get_collection("brand_logistics")
        doc = {
            "brand": brand.strip(),
            "lead_time": lead_time,
            "safety_days_fast": safety_days_fast,
            "safety_days_medium": safety_days_medium,
            "safety_days_slow": safety_days_slow,
            "order_processing": order_processing,
        }
        collection.update_one(
            {"brand": brand.strip()},
            {"$set": doc},
            upsert=True,
        )
        return JSONResponse(
            status_code=200,
            content={"message": f"Brand logistics saved for {brand}", "data": doc},
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/brand-logistics")
def delete_brand_logistics(
    brand: str = Query(...),
    db=Depends(get_database),
):
    """Delete brand logistics settings"""
    try:
        collection = db.get_collection("brand_logistics")
        result = collection.delete_one({"brand": brand.strip()})
        if result.deleted_count == 0:
            raise HTTPException(status_code=404, detail=f"Brand '{brand}' not found")
        return JSONResponse(
            status_code=200, content={"message": f"Deleted logistics for {brand}"}
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Product Logistics (CBM / Case Pack) CRUD ──────────────────────


@router.get("/product-logistics")
def get_product_logistics_list(
    search: str = Query("", description="Search by SKU or product name"),
    brand: str = Query("", description="Filter by brand"),
    status: str = Query("", description="Filter by purchase status"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db=Depends(get_database),
):
    """Get products with CBM and case_pack data, with pagination, search and brand filter"""
    try:
        products_collection = db.get_collection("products")

        # Always filter out products without a valid SKU code
        base_filter = {"cf_sku_code": {"$exists": True, "$ne": ""}}
        if brand:
            base_filter["brand"] = brand
        if status:
            base_filter["purchase_status"] = status

        if search:
            query = {
                "$and": [
                    base_filter,
                    {
                        "$or": [
                            {"cf_sku_code": {"$regex": search, "$options": "i"}},
                            {"name": {"$regex": search, "$options": "i"}},
                        ]
                    },
                ]
            }
        else:
            query = base_filter

        total = products_collection.count_documents(query)
        skip = (page - 1) * page_size

        raw_products = list(
            products_collection.find(
                query,
                {
                    "item_id": 1,
                    "cf_sku_code": 1,
                    "name": 1,
                    "brand": 1,
                    "cbm": 1,
                    "case_pack": 1,
                    "purchase_status": 1,
                    "stock_in_transit_1": 1,
                    "stock_in_transit_2": 1,
                    "stock_in_transit_3": 1,
                    "purchase_price": 1,
                    "currency": 1,
                    "_id": 0,
                },
            )
            .skip(skip)
            .limit(page_size)
            .sort("cf_sku_code", 1)
        )

        # Ensure numeric fields default to 0 when missing from DB
        products = []
        for p in raw_products:
            products.append(
                {
                    "item_id": str(p.get("item_id", "")),
                    "cf_sku_code": p.get("cf_sku_code", ""),
                    "name": p.get("name", ""),
                    "brand": p.get("brand", ""),
                    "cbm": p.get("cbm", 0) or 0,
                    "case_pack": p.get("case_pack", 0) or 0,
                    "purchase_status": p.get("purchase_status", ""),
                    "stock_in_transit_1": p.get("stock_in_transit_1", 0) or 0,
                    "stock_in_transit_2": p.get("stock_in_transit_2", 0) or 0,
                    "stock_in_transit_3": p.get("stock_in_transit_3", 0) or 0,
                    "purchase_price": p.get("purchase_price", 0) or 0,
                    "currency": p.get("currency", "") or "",
                }
            )

        return JSONResponse(
            status_code=200,
            content={
                "data": products,
                "total": total,
                "page": page,
                "page_size": page_size,
                "total_pages": math.ceil(total / page_size) if total > 0 else 1,
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/product-logistics/download")
def download_product_logistics(
    search: str = Query("", description="Search by SKU or product name"),
    brand: str = Query("", description="Filter by brand"),
    status: str = Query("", description="Filter by purchase status"),
    db=Depends(get_database),
):
    """Download all product logistics as an XLSX file"""
    try:
        products_collection = db.get_collection("products")

        base_filter = {"cf_sku_code": {"$exists": True, "$ne": ""}}
        if brand:
            base_filter["brand"] = brand
        if status:
            base_filter["purchase_status"] = status

        if search:
            query = {
                "$and": [
                    base_filter,
                    {
                        "$or": [
                            {"cf_sku_code": {"$regex": search, "$options": "i"}},
                            {"name": {"$regex": search, "$options": "i"}},
                        ]
                    },
                ]
            }
        else:
            query = base_filter

        raw_products = list(
            products_collection.find(
                query,
                {
                    "item_id": 1,
                    "cf_sku_code": 1,
                    "name": 1,
                    "brand": 1,
                    "cbm": 1,
                    "case_pack": 1,
                    "purchase_status": 1,
                    "stock_in_transit_1": 1,
                    "stock_in_transit_2": 1,
                    "stock_in_transit_3": 1,
                    "purchase_price": 1,
                    "currency": 1,
                    "_id": 0,
                },
            ).sort("cf_sku_code", 1)
        )

        rows = []
        for p in raw_products:
            rows.append(
                {
                    "SKU Code": p.get("cf_sku_code", ""),
                    "Product Name": p.get("name", ""),
                    "Brand": p.get("brand", ""),
                    "Status": p.get("purchase_status", ""),
                    "CBM": p.get("cbm", 0) or 0,
                    "Case Pack": p.get("case_pack", 0) or 0,
                    "Currency": p.get("currency", "") or "",
                    "Purchase Price": p.get("purchase_price", 0) or 0,
                    "Stock in Transit 1": p.get("stock_in_transit_1", 0) or 0,
                    "Stock in Transit 2": p.get("stock_in_transit_2", 0) or 0,
                    "Stock in Transit 3": p.get("stock_in_transit_3", 0) or 0,
                }
            )

        df = pd.DataFrame(rows)
        # Keep SKU Code as string so Excel doesn't convert to scientific notation
        df["SKU Code"] = df["SKU Code"].astype(str)
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Product Logistics", index=False)
            ws = writer.sheets["Product Logistics"]
            for cell in ws["A"][1:]:  # column A = SKU Code, skip header
                cell.number_format = "@"
        buffer.seek(0)

        return StreamingResponse(
            buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=product_logistics.xlsx"
            },
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/product-logistics")
def update_product_logistics(
    item_id: str = Query(...),
    sku_code: str = Query(None),
    cbm: float = Query(None),
    case_pack: float = Query(None),
    purchase_status: str = Query(None),
    purchase_price: float = Query(None),
    currency: str = Query(None),
    db=Depends(get_database),
):
    """Update product logistics fields. purchase_status is applied to all products sharing
    the same sku_code; all other fields update only the specific item by item_id."""
    try:
        products_collection = db.get_collection("products")

        # Fields that apply only to the specific item (by item_id)
        item_fields: Dict[str, Any] = {}
        if cbm is not None:
            item_fields["cbm"] = cbm
        if case_pack is not None:
            item_fields["case_pack"] = case_pack
        if purchase_price is not None:
            item_fields["purchase_price"] = purchase_price
        if currency is not None:
            item_fields["currency"] = currency

        if not item_fields and purchase_status is None:
            raise HTTPException(status_code=400, detail="No fields to update")

        matched = 0

        # Update item-specific fields by item_id
        if item_fields:
            iid: Any = int(item_id) if item_id.isdigit() else item_id
            result = products_collection.update_one(
                {"item_id": iid}, {"$set": item_fields}
            )
            if result.matched_count == 0:
                result = products_collection.update_one(
                    {"item_id": item_id}, {"$set": item_fields}
                )
            matched = max(matched, result.matched_count)

        # Update purchase_status across ALL products with the same sku_code
        if purchase_status is not None:
            sku_filter = (
                {"cf_sku_code": sku_code.strip()}
                if sku_code
                else {"item_id": (int(item_id) if item_id.isdigit() else item_id)}
            )
            result = products_collection.update_many(
                sku_filter, {"$set": {"purchase_status": purchase_status}}
            )
            matched = max(matched, result.matched_count)

        if matched == 0:
            raise HTTPException(
                status_code=404, detail=f"Product with item_id '{item_id}' not found"
            )

        return JSONResponse(
            status_code=200,
            content={"message": f"Updated logistics for item {item_id}"},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/product-logistics/bulk-upload")
async def bulk_upload_product_logistics(
    file: UploadFile = File(...),
    db=Depends(get_database),
):
    """Accept an XLSX with columns 'SKU Code', 'Currency', 'Purchase Price' and bulk-update
    those two fields for matched products. All other columns are ignored."""
    try:
        contents = await file.read()
        buffer = io.BytesIO(contents)
        try:
            df = pd.read_excel(buffer, sheet_name=0, dtype=str)
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"Could not parse XLSX: {e}")

        df.columns = [c.strip() for c in df.columns]
        required = {"SKU Code", "Currency", "Purchase Price"}
        missing = required - set(df.columns)
        if missing:
            raise HTTPException(
                status_code=400,
                detail=f"Missing required columns: {', '.join(sorted(missing))}. "
                "The file must contain 'SKU Code', 'Currency', and 'Purchase Price' columns.",
            )

        products_collection = db.get_collection("products")

        updated = 0
        skipped = 0
        errors: list[str] = []

        for _, row in df.iterrows():
            sku = str(row.get("SKU Code", "") or "").strip()
            if not sku:
                skipped += 1
                continue

            currency_val = str(row.get("Currency", "") or "").strip()
            price_raw = str(row.get("Purchase Price", "") or "").strip()

            fields: Dict[str, Any] = {}
            if currency_val and currency_val.lower() not in ("nan", "none"):
                fields["currency"] = currency_val
            try:
                if price_raw not in ("", "nan", "None"):
                    fields["purchase_price"] = float(price_raw)
            except ValueError:
                errors.append(f"SKU {sku}: invalid purchase price '{price_raw}'")
                skipped += 1
                continue

            if not fields:
                skipped += 1
                continue

            result = products_collection.update_many(
                {"cf_sku_code": sku},
                {"$set": fields},
            )
            if result.matched_count > 0:
                updated += result.matched_count
            else:
                skipped += 1

        return JSONResponse(
            status_code=200,
            content={
                "message": f"Bulk upload complete: {updated} product(s) updated, {skipped} row(s) skipped.",
                "updated": updated,
                "skipped": skipped,
                "errors": errors[:20],
            },
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
