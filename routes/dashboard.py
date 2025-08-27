from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Set
from fastapi import Query
from pydantic import BaseModel
import calendar
import asyncio
import logging
from collections import defaultdict

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document

logger = logging.getLogger(__name__)

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
    sources: List[str]  # Added to show which sources contributed
    city: Optional[str] = "Multiple"  # Default for multi-source data


class TopProductsResponse(BaseModel):
    products: List[TopProduct]
    total_count: int
    current_month: int
    current_year: int


class MultiSourceDashboardService:
    """Service to handle multi-source data fetching for dashboard"""

    def __init__(self, database):
        self.database = database
        self._product_name_cache = {}

    async def get_blinkit_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch Blinkit sales and inventory data"""
        try:
            sales_collection = self.database[BLINKIT_SALES_COLLECTION]
            inventory_collection = self.database[BLINKIT_INVENTORY_COLLECTION]

            sales_filter = {
                "order_date": {"$gte": start_date, "$lte": end_date}
            }
            inventory_filter = {
                "date": {"$gte": start_date, "$lte": end_date}
            }

            # Aggregate sales data
            sales_pipeline = [
                {"$match": sales_filter},
                {
                    "$group": {
                        "_id": {
                            "item_id": "$item_id",
                            "sku_code": "$sku_code",
                            "item_name": "$item_name",
                        },
                        "total_sales_in_period": {"$sum": "$quantity"},
                    }
                },
            ]

            # Aggregate inventory data
            inventory_pipeline = [
                {"$match": inventory_filter},
                {
                    "$group": {
                        "_id": {
                            "item_id": "$item_id",
                            "sku_code": "$sku_code",
                            "item_name": "$item_name",
                        },
                        "avg_inventory": {"$avg": "$warehouse_inventory"},
                        "closing_stock": {"$last": "$warehouse_inventory"},
                        "days_with_inventory": {
                            "$sum": {"$cond": [{"$gt": ["$warehouse_inventory", 0]}, 1, 0]}
                        },
                    }
                },
            ]

            sales_results = list(sales_collection.aggregate(sales_pipeline))
            inventory_results = list(inventory_collection.aggregate(inventory_pipeline))

            # Combine results
            inventory_lookup = {}
            for inv in inventory_results:
                key = (inv["_id"]["item_id"], inv["_id"]["sku_code"])
                inventory_lookup[key] = inv

            combined_data = []
            for sale in sales_results:
                key = (sale["_id"]["item_id"], sale["_id"]["sku_code"])
                inventory_data = inventory_lookup.get(key, {})

                combined_data.append({
                    "source": "blinkit",
                    "item_id": sale["_id"]["item_id"],
                    "item_name": sale["_id"]["item_name"],
                    "sku_code": sale["_id"]["sku_code"],
                    "units_sold": sale["total_sales_in_period"],
                    "closing_stock": inventory_data.get("closing_stock", 0),
                    "days_with_inventory": inventory_data.get("days_with_inventory", 0),
                    "avg_inventory": inventory_data.get("avg_inventory", 0)
                })

            return combined_data

        except Exception as e:
            logger.error(f"Error fetching Blinkit data: {e}")
            return []

    async def get_amazon_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch Amazon sales and inventory data"""
        try:
            from .amazon import generate_report_by_date_range
            
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            report_data = await generate_report_by_date_range(
                start_date=start_date_str,
                end_date=end_date_str,
                database=self.database,
                report_type="all"
            )

            amazon_data = []
            if isinstance(report_data, list):
                for item in report_data:
                    if isinstance(item, dict):
                        amazon_data.append({
                            "source": "amazon",
                            "item_id": item.get("asin", ""),
                            "item_name": item.get("item_name", "Unknown Item"),
                            "sku_code": item.get("sku_code", "Unknown SKU"),
                            "units_sold": self.safe_float(item.get("units_sold", 0)),
                            "closing_stock": self.safe_float(item.get("closing_stock", 0)),
                            "days_with_inventory": self.safe_int(item.get("total_days_in_stock", 0)),
                            "avg_inventory": self.safe_float(item.get("stock", 0))
                        })

            return amazon_data

        except Exception as e:
            logger.error(f"Error fetching Amazon data: {e}")
            return []

    async def get_zoho_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Fetch Zoho sales data"""
        try:
            from .zoho import get_sales_report_fast
            
            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")
            
            response = await get_sales_report_fast(
                start_date=start_date_str,
                end_date=end_date_str,
                db=self.database
            )

            report_data = []
            if hasattr(response, "body"):
                import json
                content = json.loads(response.body)
                report_data = content.get("data", [])
            elif isinstance(response, dict):
                report_data = response.get("data", [])
            else:
                report_data = response if isinstance(response, list) else []

            zoho_data = []
            for item in report_data:
                if isinstance(item, dict):
                    zoho_data.append({
                        "source": "zoho",
                        "item_id": str(item.get("item_id", "")),
                        "item_name": item.get("item_name", "Unknown Item"),
                        "sku_code": item.get("sku_code", "Unknown SKU"),
                        "units_sold": self.safe_float(item.get("units_sold", 0)),
                        "closing_stock": self.safe_float(item.get("closing_stock", 0)),
                        "days_with_inventory": self.safe_int(item.get("total_days_in_stock", 0)),
                        "avg_inventory": self.safe_float(item.get("closing_stock", 0))
                    })

            return zoho_data

        except Exception as e:
            logger.error(f"Error fetching Zoho data: {e}")
            return []

    async def batch_load_product_names(self, sku_codes: Set[str]) -> Dict[str, str]:
        """Batch load all product names in a single database query"""
        if not sku_codes:
            return {}
        
        try:
            valid_skus = {sku for sku in sku_codes if sku and sku != "Unknown SKU"}
            
            if not valid_skus:
                return {}

            products_collection = self.database.get_collection("products")
            
            cursor = products_collection.find(
                {"cf_sku_code": {"$in": list(valid_skus)}},
                {"cf_sku_code": 1, "name": 1, "_id": 0}
            )
            
            products = list(cursor)
            
            sku_to_name = {}
            for product in products:
                sku_code = product.get("cf_sku_code")
                name = product.get("name")
                if sku_code and name:
                    sku_to_name[sku_code] = name
            
            self._product_name_cache.update(sku_to_name)
            
            for sku in valid_skus:
                if sku not in sku_to_name:
                    sku_to_name[sku] = "Unknown Item"
                    self._product_name_cache[sku] = "Unknown Item"
            
            return sku_to_name
            
        except Exception as e:
            logger.error(f"Error in batch loading product names: {e}")
            return {}

    @staticmethod
    def safe_float(value: Any, default: float = 0.0) -> float:
        """Safely convert value to float"""
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def safe_int(value: Any, default: int = 0) -> int:
        """Safely convert value to int"""
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def combine_multi_source_data(self, all_data: List[List[Dict]], product_name_map: Dict[str, str], start_date: datetime, end_date: datetime) -> List[Dict]:
        """Combine data from all sources by SKU"""
        sku_data = defaultdict(lambda: {
            "item_id": "",
            "sku_code": "",
            "item_name": "Unknown Item",
            "sources": set(),
            "total_units_sold": 0.0,
            "total_closing_stock": 0.0,
            "total_days_with_inventory": 0,
            "avg_inventory": 0.0
        })

        # Combine all data by SKU
        for source_data in all_data:
            for item in source_data:
                sku = item.get("sku_code", "Unknown SKU")
                
                if sku_data[sku]["sku_code"] == "":
                    sku_data[sku]["sku_code"] = sku
                    sku_data[sku]["item_id"] = str(item.get("item_id", ""))
                    # Use canonical name from products collection
                    sku_data[sku]["item_name"] = product_name_map.get(sku, item.get("item_name", "Unknown Item"))

                sku_data[sku]["sources"].add(item.get("source", "unknown"))
                sku_data[sku]["total_units_sold"] += self.safe_float(item.get("units_sold", 0))
                sku_data[sku]["total_closing_stock"] += self.safe_float(item.get("closing_stock", 0))
                sku_data[sku]["total_days_with_inventory"] += self.safe_int(item.get("days_with_inventory", 0))
                sku_data[sku]["avg_inventory"] += self.safe_float(item.get("avg_inventory", 0))

        # Convert to the expected format and calculate metrics
        days_in_period = (end_date - start_date).days + 1
        result = []

        for sku, data in sku_data.items():
            if data["total_units_sold"] <= 0:  # Skip items with no sales
                continue

            # Calculate metrics similar to original dashboard
            avg_daily_sales = data["total_units_sold"] / days_in_period if days_in_period > 0 else 0
            days_of_coverage = data["total_closing_stock"] / avg_daily_sales if avg_daily_sales > 0 else 0

            avg_daily_on_stock_days = data["total_days_with_inventory"] / days_in_period if days_in_period > 0 else 0
            avg_weekly_on_stock_days = avg_daily_on_stock_days * 7
            avg_monthly_on_stock_days = avg_daily_on_stock_days * 30

            sources_list = list(data["sources"])
            
            # Determine city based on sources
            city = "Multiple" if len(sources_list) > 1 else ("Amazon" if "amazon" in sources_list else "Blinkit" if "blinkit" in sources_list else "Zoho")

            product_data = {
                "item_id": int(data["item_id"]) if data["item_id"].isdigit() else 0,
                "item_name": data["item_name"],
                "sku_code": data["sku_code"],
                "city": city,
                "sources": sources_list,
                "metrics": {
                    "avg_daily_on_stock_days": round(avg_daily_on_stock_days, 2),
                    "avg_weekly_on_stock_days": round(avg_weekly_on_stock_days, 2),
                    "avg_monthly_on_stock_days": round(avg_monthly_on_stock_days, 2),
                    "total_sales_in_period": int(data["total_units_sold"]),
                    "days_of_coverage": round(days_of_coverage, 2),
                    "days_with_inventory": int(data["total_days_with_inventory"]),
                    "closing_stock": int(data["total_closing_stock"]),
                },
                "year": start_date.year,
                "month": start_date.month,
            }
            result.append(product_data)

        return result


def get_month_date_range(year: int, month: int):
    """Get the start and end dates for a given month and year."""
    start_date = datetime(year, month, 1)
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
    include_blinkit: bool = Query(True, description="Include Blinkit data"),
    include_amazon: bool = Query(True, description="Include Amazon data"),
    include_zoho: bool = Query(True, description="Include Zoho data"),
    db=Depends(get_database),
):
    """
    Get top performing products from multiple sources based on total sales in period.
    Returns products sorted by total_sales_in_period in descending order.
    """
    try:
        # Handle date parameters (prioritize start_date/end_date over year/month)
        if start_date and end_date:
            try:
                parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                parsed_end_date = parsed_end_date.replace(hour=23, minute=59, second=59)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date format. Use YYYY-MM-DD format.",
                )
        else:
            current_date = datetime.now()
            target_year = year or current_date.year
            target_month = month or current_date.month
            parsed_start_date, parsed_end_date = get_month_date_range(
                target_year, target_month
            )

        logger.info(f"Generating multi-source dashboard for {parsed_start_date} to {parsed_end_date}")

        # Initialize service
        dashboard_service = MultiSourceDashboardService(db)

        # Step 1: Fetch all data in parallel
        tasks = []
        if include_blinkit:
            tasks.append(dashboard_service.get_blinkit_data(parsed_start_date, parsed_end_date))
        if include_amazon:
            tasks.append(dashboard_service.get_amazon_data(parsed_start_date, parsed_end_date))
        if include_zoho:
            tasks.append(dashboard_service.get_zoho_data(parsed_start_date, parsed_end_date))

        if not tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one data source must be selected",
            )

        # Execute all data fetches in parallel
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error in parallel data fetching: {e}")
            results = []

        # Process results
        all_source_data = []
        for result in results:
            if isinstance(result, Exception):
                logger.error(f"Task failed: {str(result)}")
                continue
            if isinstance(result, list):
                all_source_data.append(result)

        if not all_source_data:
            return TopProductsResponse(
                products=[],
                total_count=0,
                current_month=parsed_start_date.month,
                current_year=parsed_start_date.year,
            )

        # Step 2: Extract SKU codes and batch load product names
        all_sku_codes = set()
        for source_data in all_source_data:
            for item in source_data:
                if isinstance(item, dict):
                    sku = item.get("sku_code")
                    if sku and sku.strip():
                        all_sku_codes.add(sku.strip())

        product_name_map = await dashboard_service.batch_load_product_names(all_sku_codes)

        # Step 3: Combine data from all sources
        combined_data = dashboard_service.combine_multi_source_data(
            all_source_data, product_name_map, parsed_start_date, parsed_end_date
        )

        # Step 4: Sort by total sales and limit
        sorted_products = sorted(
            combined_data,
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
        logger.error(f"Error in multi-source dashboard: {e}")
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
    include_blinkit: bool = Query(True, description="Include Blinkit data"),
    include_amazon: bool = Query(True, description="Include Amazon data"),
    include_zoho: bool = Query(True, description="Include Zoho data"),
    db=Depends(get_database),
):
    """
    Get summary statistics for top performing products from multiple sources.
    """
    try:
        # Handle date parameters (prioritize start_date/end_date over year/month)
        if start_date and end_date:
            try:
                parsed_start_date = datetime.strptime(start_date, "%Y-%m-%d")
                parsed_end_date = datetime.strptime(end_date, "%Y-%m-%d")
                parsed_end_date = parsed_end_date.replace(hour=23, minute=59, second=59)
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid date format. Use YYYY-MM-DD format.",
                )
        else:
            current_date = datetime.now()
            target_year = year or current_date.year
            target_month = month or current_date.month
            parsed_start_date, parsed_end_date = get_month_date_range(
                target_year, target_month
            )

        # Initialize service and fetch data
        dashboard_service = MultiSourceDashboardService(db)
        
        tasks = []
        if include_blinkit:
            tasks.append(dashboard_service.get_blinkit_data(parsed_start_date, parsed_end_date))
        if include_amazon:
            tasks.append(dashboard_service.get_amazon_data(parsed_start_date, parsed_end_date))
        if include_zoho:
            tasks.append(dashboard_service.get_zoho_data(parsed_start_date, parsed_end_date))

        if not tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one data source must be selected",
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process and aggregate results
        total_products = 0
        total_sales = 0
        total_closing_stock = 0
        source_counts = {}
        
        for result in results:
            if isinstance(result, Exception):
                continue
            if isinstance(result, list):
                source_name = result[0].get("source", "unknown") if result else "unknown"
                source_counts[source_name] = len(result)
                total_products += len(result)
                
                for item in result:
                    total_sales += dashboard_service.safe_float(item.get("units_sold", 0))
                    total_closing_stock += dashboard_service.safe_float(item.get("closing_stock", 0))

        avg_sales = total_sales / total_products if total_products > 0 else 0

        return {
            "total_products": total_products,
            "total_sales": round(total_sales, 2),
            "avg_sales": round(avg_sales, 2),
            "total_closing_stock": round(total_closing_stock, 2),
            "source_counts": source_counts,
            "sources_included": list(source_counts.keys()),
            "year": parsed_start_date.year,
            "month": parsed_start_date.month,
            "start_date": parsed_start_date.strftime("%Y-%m-%d"),
            "end_date": parsed_end_date.strftime("%Y-%m-%d"),
        }

    except PyMongoError as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Error in multi-source summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )