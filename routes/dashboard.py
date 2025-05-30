# main.py
from datetime import datetime
from typing import List, Optional
from fastapi import Query
from pydantic import BaseModel


from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document

# --- Configuration ---
# Use environment variables for production

AMAZON_SKU_COLLECTION = "amazon_sku_mapping"
AMAZON_SALES_COLLECTION = "amazon_sales"
AMAZON_INVENTORY_COLLECTION = "amazon_inventory"
AMAZON_REPORT_COLLECTION = "amazon_sales_inventory_reports"


BLINKIT_SKU_COLLECTION = "blinkit_sku_mapping"
BLINKIT_SALES_COLLECTION = "blinkit_sales"
BLINKIT_INVENTORY_COLLECTION = "blinkit_inventory"
BLINKIT_REPORT_COLLECTION = "blinkit_sales_inventory_reports"

router = APIRouter()


class ProductMetrics(BaseModel):
    avg_daily_on_stock_days: float
    avg_weekly_on_stock_days: float
    avg_monthly_on_stock_days: float
    total_sales_in_period: int
    days_of_coverage: float
    days_with_inventory: int
    closing_stock: int


class TopProduct(BaseModel):
    item_id: int
    item_name: str
    sku_code: str
    city: str
    warehouse: str
    metrics: ProductMetrics
    year: int
    month: int


class TopProductsResponse(BaseModel):
    products: List[TopProduct]
    total_count: int
    current_month: int
    current_year: int


@router.get("/top-products", response_model=TopProductsResponse)
async def get_top_performing_products(
    limit: int = Query(10, ge=1, le=50, description="Number of top products to return"),
    year: Optional[int] = Query(
        None, description="Year to filter by (default: current year)"
    ),
    month: Optional[int] = Query(
        None, ge=1, le=12, description="Month to filter by (default: current month)"
    ),
    city: Optional[str] = Query(None, description="City to filter by"),
    warehouse: Optional[str] = Query(None, description="Warehouse to filter by"),
):
    """
    Get top performing products based on total sales in period.
    Returns products sorted by total_sales_in_period in descending order.
    """
    try:
        # Get current date if year/month not provided
        current_date = datetime.now()
        target_year = year or current_date.year
        target_month = month or current_date.month

        # Get database connection
        db = get_database()

        # Build the query filter
        query_filter = {"year": target_year, "month": target_month}

        # Add optional filters
        if city:
            query_filter["city"] = city
        if warehouse:
            query_filter["warehouse"] = warehouse

        # MongoDB aggregation pipeline
        pipeline = [
            # Match documents for the specified month/year and optional filters
            {"$match": query_filter},
            # Sort by total sales in descending order
            {"$sort": {"metrics.total_sales_in_period": -1}},
            # Limit to top N products
            {"$limit": limit},
            # Project only needed fields
            {
                "$project": {
                    "_id": 0,
                    "item_id": 1,
                    "item_name": 1,
                    "sku_code": 1,
                    "city": 1,
                    "warehouse": 1,
                    "metrics": 1,
                    "year": 1,
                    "month": 1,
                }
            },
        ]

        # Execute aggregation - you'll need to specify which collection to use
        # I'm assuming you have a main products collection, adjust as needed
        collection_name = BLINKIT_REPORT_COLLECTION
        collection = db[collection_name]

        cursor = collection.aggregate(pipeline)
        products = list(cursor)

        # Get total count for the current month (optional, for pagination info)
        total_count = collection.count_documents(query_filter)

        # Serialize the results
        serialized_products = []
        for product in products:
            serialized_product = serialize_mongo_document(product)
            serialized_products.append(serialized_product)

        return TopProductsResponse(
            products=serialized_products,
            total_count=total_count,
            current_month=target_month,
            current_year=target_year,
        )

    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@router.get("/top-products/summary")
async def get_top_products_summary(
    year: Optional[int] = Query(
        None, description="Year to filter by (default: current year)"
    ),
    month: Optional[int] = Query(
        None, ge=1, le=12, description="Month to filter by (default: current month)"
    ),
):
    """
    Get summary statistics for top performing products.
    """
    try:
        current_date = datetime.now()
        target_year = year or current_date.year
        target_month = month or current_date.month

        db = get_database()
        collection_name = BLINKIT_REPORT_COLLECTION
        collection = db[collection_name]

        # Aggregation pipeline for summary statistics
        pipeline = [
            {"$match": {"year": target_year, "month": target_month}},
            {
                "$group": {
                    "_id": None,
                    "total_products": {"$sum": 1},
                    "total_sales": {"$sum": "$metrics.total_sales_in_period"},
                    "avg_sales": {"$avg": "$metrics.total_sales_in_period"},
                    "max_sales": {"$max": "$metrics.total_sales_in_period"},
                    "min_sales": {"$min": "$metrics.total_sales_in_period"},
                    "total_closing_stock": {"$sum": "$metrics.closing_stock"},
                }
            },
        ]

        result = list(collection.aggregate(pipeline))

        if not result:
            return {
                "total_products": 0,
                "total_sales": 0,
                "avg_sales": 0,
                "max_sales": 0,
                "min_sales": 0,
                "total_closing_stock": 0,
                "year": target_year,
                "month": target_month,
            }

        summary = result[0]
        summary.pop("_id", None)  # Remove the _id field
        summary["year"] = target_year
        summary["month"] = target_month

        return summary

    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )
