import math
import re
import logging
from fastapi import APIRouter, HTTPException, Query, status
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document

router = APIRouter()
logger = logging.getLogger(__name__)

PRODUCTS_COLLECTION = "products"


@router.get("/new-items")
def get_new_items(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    search: str = Query(None, description="Search by product name or cf_sku_code"),
    brand: str = Query(None, description="Filter by brand"),
    zoho_status: str = Query(None, description="Filter by Zoho status (active/inactive)"),
    purchase_status: str = Query(None, description="Filter by purchase status"),
):
    """
    Returns non-combo products sorted by created_at descending (newest first).
    Read-only endpoint for the design team.
    """
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
