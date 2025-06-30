# main.py
from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import Query
from pydantic import BaseModel
import calendar

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
    metrics: ProductMetrics
    year: int
    month: int


class TopProductsResponse(BaseModel):
    products: List[TopProduct]
    total_count: int
    current_month: int
    current_year: int


def get_month_date_range(year: int, month: int):
    """Get the start and end dates for a given month and year."""
    start_date = datetime(year, month, 1)
    # Get the last day of the month
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day, 23, 59, 59)
    return start_date, end_date


@router.get("/top-products", response_model=TopProductsResponse)
async def get_top_performing_products(
    limit: int = Query(10, ge=1, le=50, description="Number of top products to return"),
    start_date: Optional[str] = Query(
        None, description="Start date (YYYY-MM-DD format, default: current month start)"
    ),
    end_date: Optional[str] = Query(
        None, description="End date (YYYY-MM-DD format, default: current month end)"
    ),
    year: Optional[int] = Query(
        None, description="Year to filter by (legacy, use start_date/end_date instead)"
    ),
    month: Optional[int] = Query(
        None,
        ge=1,
        le=12,
        description="Month to filter by (legacy, use start_date/end_date instead)",
    ),
    # city: Optional[str] = Query(None, description="City to filter by"),
):
    """
    Get top performing products based on total sales in period.
    Returns products sorted by total_sales_in_period in descending order.
    """
    try:
        # Handle date parameters (prioritize start_date/end_date over year/month)
        if start_date and end_date:
            # Parse custom date range
            try:
                parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                # Set end_date to end of day
                parsed_end_date = parsed_end_date.replace(hour=23, minute=59, second=59)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date format. Use YYYY-MM-DD format.",
                )
        else:
            # Fall back to year/month logic or default to current month
            current_date = datetime.now()
            target_year = year or current_date.year
            target_month = month or current_date.month
            parsed_start_date, parsed_end_date = get_month_date_range(
                target_year, target_month
            )

        # Get database connection
        db = get_database()
        sales_collection = db[BLINKIT_SALES_COLLECTION]
        inventory_collection = db[BLINKIT_INVENTORY_COLLECTION]

        # Build base filters
        sales_filter = {
            "order_date": {"$gte": parsed_start_date, "$lte": parsed_end_date}
        }
        inventory_filter = {
            "date": {"$gte": parsed_start_date, "$lte": parsed_end_date}
        }

        # Step 1: Aggregate sales data
        sales_pipeline = [
            {"$match": sales_filter},
            {
                "$group": {
                    "_id": {
                        "item_id": "$item_id",
                        "sku_code": "$sku_code",
                        # "city": "$city",
                        "item_name": "$item_name",
                    },
                    "total_sales_in_period": {"$sum": "$quantity"},
                }
            },
        ]

        sales_results = list(sales_collection.aggregate(sales_pipeline))

        # Step 2: Aggregate inventory data
        inventory_pipeline = [
            {"$match": inventory_filter},
            {
                "$group": {
                    "_id": {
                        "item_id": "$item_id",
                        "sku_code": "$sku_code",
                        # "city": "$city",
                        "item_name": "$item_name",
                    },
                    "avg_inventory": {"$avg": "$warehouse_inventory"},
                    "closing_stock": {"$last": "$warehouse_inventory"},
                    "days_with_inventory": {
                        "$sum": {"$cond": [{"$gt": ["$warehouse_inventory", 0]}, 1, 0]}
                    },
                    "inventory_records": {"$push": "$warehouse_inventory"},
                }
            },
        ]

        inventory_results = list(inventory_collection.aggregate(inventory_pipeline))

        # Step 3: Combine sales and inventory data
        combined_data = {}

        # Create lookup dict for inventory data
        inventory_lookup = {}
        for inv in inventory_results:
            key = (inv["_id"]["item_id"], inv["_id"]["sku_code"])
            inventory_lookup[key] = inv

        # Combine with sales data
        for sale in sales_results:
            key = (sale["_id"]["item_id"], sale["_id"]["sku_code"])

            inventory_data = inventory_lookup.get(key, {})

            # Calculate metrics
            total_sales = sale["total_sales_in_period"]
            closing_stock = inventory_data.get("closing_stock", 0)
            days_with_inventory = inventory_data.get("days_with_inventory", 0)
            avg_inventory = inventory_data.get("avg_inventory", 0)

            # Calculate days in the selected period
            days_in_period = (parsed_end_date - parsed_start_date).days + 1
            avg_daily_sales = total_sales / days_in_period if days_in_period > 0 else 0
            days_of_coverage = (
                closing_stock / avg_daily_sales if avg_daily_sales > 0 else 0
            )

            # Calculate on-stock ratios
            avg_daily_on_stock_days = (
                days_with_inventory / days_in_period if days_in_period > 0 else 0
            )
            avg_weekly_on_stock_days = avg_daily_on_stock_days * 7
            avg_monthly_on_stock_days = avg_daily_on_stock_days * 30

            combined_data[key] = {
                "item_id": sale["_id"]["item_id"],
                "item_name": sale["_id"]["item_name"],
                "sku_code": sale["_id"]["sku_code"],
                "metrics": {
                    "avg_daily_on_stock_days": round(avg_daily_on_stock_days, 2),
                    "avg_weekly_on_stock_days": round(avg_weekly_on_stock_days, 2),
                    "avg_monthly_on_stock_days": round(avg_monthly_on_stock_days, 2),
                    "total_sales_in_period": total_sales,
                    "days_of_coverage": round(days_of_coverage, 2),
                    "days_with_inventory": days_with_inventory,
                    "closing_stock": closing_stock,
                },
                "year": parsed_start_date.year,
                "month": parsed_start_date.month,
            }

        # Step 4: Sort by total sales and limit
        sorted_products = sorted(
            combined_data.values(),
            key=lambda x: x["metrics"]["total_sales_in_period"],
            reverse=True,
        )[:limit]

        return TopProductsResponse(
            products=sorted_products,
            total_count=len(combined_data),
            current_month=parsed_start_date.month,
            current_year=parsed_start_date.year,
        )

    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )
    except Exception as e:
        print(e)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@router.get("/top-products/summary")
async def get_top_products_summary(
    start_date: Optional[str] = Query(
        None, description="Start date (YYYY-MM-DD format, default: current month start)"
    ),
    end_date: Optional[str] = Query(
        None, description="End date (YYYY-MM-DD format, default: current month end)"
    ),
    year: Optional[int] = Query(
        None, description="Year to filter by (legacy, use start_date/end_date instead)"
    ),
    month: Optional[int] = Query(
        None,
        ge=1,
        le=12,
        description="Month to filter by (legacy, use start_date/end_date instead)",
    ),
):
    """
    Get summary statistics for top performing products.
    """
    try:
        # Handle date parameters (prioritize start_date/end_date over year/month)
        if start_date and end_date:
            # Parse custom date range
            try:
                parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                # Set end_date to end of day
                parsed_end_date = parsed_end_date.replace(hour=23, minute=59, second=59)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date format. Use YYYY-MM-DD format.",
                )
        else:
            # Fall back to year/month logic or default to current month
            current_date = datetime.now()
            target_year = year or current_date.year
            target_month = month or current_date.month
            parsed_start_date, parsed_end_date = get_month_date_range(
                target_year, target_month
            )

        db = get_database()
        sales_collection = db[BLINKIT_SALES_COLLECTION]
        inventory_collection = db[BLINKIT_INVENTORY_COLLECTION]

        # Aggregation pipeline for sales summary
        sales_pipeline = [
            {
                "$match": {
                    "order_date": {"$gte": parsed_start_date, "$lte": parsed_end_date}
                }
            },
            {
                "$group": {
                    "_id": {
                        "item_id": "$item_id",
                        "sku_code": "$sku_code",
                        "city": "$city",
                    },
                    "total_sales": {"$sum": "$quantity"},
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_products": {"$sum": 1},
                    "total_sales": {"$sum": "$total_sales"},
                    "avg_sales": {"$avg": "$total_sales"},
                    "max_sales": {"$max": "$total_sales"},
                    "min_sales": {"$min": "$total_sales"},
                }
            },
        ]

        # Aggregation pipeline for inventory summary
        inventory_pipeline = [
            {"$match": {"date": {"$gte": parsed_start_date, "$lte": parsed_end_date}}},
            {
                "$group": {
                    "_id": {
                        "item_id": "$item_id",
                        "sku_code": "$sku_code",
                        "city": "$city",
                    },
                    "latest_stock": {"$last": "$warehouse_inventory"},
                }
            },
            {"$group": {"_id": None, "total_closing_stock": {"$sum": "$latest_stock"}}},
        ]

        # Execute aggregations
        sales_result = list(sales_collection.aggregate(sales_pipeline))
        inventory_result = list(inventory_collection.aggregate(inventory_pipeline))

        if not sales_result:
            sales_summary = {
                "total_products": 0,
                "total_sales": 0,
                "avg_sales": 0,
                "max_sales": 0,
                "min_sales": 0,
            }
        else:
            sales_summary = sales_result[0]
            sales_summary.pop("_id", None)

        if not inventory_result:
            inventory_summary = {"total_closing_stock": 0}
        else:
            inventory_summary = inventory_result[0]
            inventory_summary.pop("_id", None)

        # Combine both summaries
        final_summary = {
            **sales_summary,
            **inventory_summary,
            "year": parsed_start_date.year,
            "month": parsed_start_date.month,
            "start_date": parsed_start_date.strftime("%Y-%m-%d"),
            "end_date": parsed_end_date.strftime("%Y-%m-%d"),
        }

        return final_summary

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
