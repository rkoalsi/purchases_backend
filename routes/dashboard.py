from datetime import datetime
from typing import List, Optional, Dict, Any, Set
from fastapi import Query
from pydantic import BaseModel
import calendar
import asyncio
import logging
import traceback
from collections import defaultdict

from fastapi import APIRouter, HTTPException, status, Depends
from pymongo.errors import PyMongoError
from ..database import get_database

logger = logging.getLogger(__name__)

# --- Configuration ---
# Use environment variables for production

AMAZON_SKU_COLLECTION = "amazon_sku_mapping"
AMAZON_SALES_COLLECTION = "amazon_sales"
AMAZON_INVENTORY_COLLECTION = "amazon_inventory"
AMAZON_REPORT_COLLECTION = "amazon_sales_inventory_reports"

BLINKIT_SKU_COLLECTION = "blinkit_sku_mapping"
BLINKIT_SALES_COLLECTION = "blinkit_sales_temp"
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
    """Dashboard service that uses the exact same logic as the master report"""

    def __init__(self, database):
        self.database = database
        self._product_name_cache = {}

    async def get_blinkit_data(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict]:
        """Fetch Blinkit data using the SAME function as master report"""
        try:
            from .blinkit import generate_report_by_date_range

            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")

            # Use EXACT same function as master report
            report_data = await generate_report_by_date_range(
                start_date=start_date_str, end_date=end_date_str, database=self.database
            )

            # Extract data from report response - handle different return formats
            if isinstance(report_data, dict):
                return report_data.get("data", [])
            elif isinstance(report_data, list):
                return report_data
            else:
                return []

        except Exception as e:
            logger.error(f"Error fetching Blinkit data: {e}")
            return []

    async def get_amazon_data(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict]:
        """Fetch Amazon data using the SAME function as master report"""
        try:
            from .amazon import generate_report_by_date_range

            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")

            # Use EXACT same function as master report
            report_data = await generate_report_by_date_range(
                start_date=start_date_str,
                end_date=end_date_str,
                database=self.database,
                report_type="all",
            )

            return report_data if isinstance(report_data, list) else []

        except Exception as e:
            logger.error(f"Error fetching Amazon data: {e}")
            return []

    async def get_zoho_data(
        self, start_date: datetime, end_date: datetime
    ) -> List[Dict]:
        """Fetch Zoho data using the SAME function as master report"""
        try:
            from .zoho import get_sales_report_fast

            start_date_str = start_date.strftime("%Y-%m-%d")
            end_date_str = end_date.strftime("%Y-%m-%d")

            # Use EXACT same function as master report
            response = await get_sales_report_fast(
                start_date=start_date_str, end_date=end_date_str, db=self.database
            )

            # Handle response EXACTLY like master report
            report_data = []
            if hasattr(response, "body"):
                import json

                content = json.loads(response.body)
                report_data = content.get("data", [])
            elif isinstance(response, dict):
                report_data = response.get("data", [])
            else:
                report_data = response if isinstance(response, list) else []

            return report_data

        except Exception as e:
            logger.error(f"Error fetching Zoho data: {e}")
            return []

    async def batch_load_product_names(self, sku_codes: Set[str]) -> Dict[str, str]:
        """Batch load product names - SAME as master report"""
        if not sku_codes:
            return {}

        try:
            # Filter out invalid SKUs
            valid_skus = {sku for sku in sku_codes if sku and sku != "Unknown SKU"}

            if not valid_skus:
                return {}

            products_collection = self.database.get_collection("products")

            # Single query to get all products
            cursor = products_collection.find(
                {"cf_sku_code": {"$in": list(valid_skus)}},
                {"cf_sku_code": 1, "name": 1, "_id": 0},
            )

            products = list(cursor)

            # Create mapping
            sku_to_name = {}
            for product in products:
                sku_code = product.get("cf_sku_code")
                name = product.get("name")
                if sku_code and name:
                    sku_to_name[sku_code] = name

            # Cache the results
            self._product_name_cache.update(sku_to_name)

            # Add unknown items to cache to avoid future queries
            for sku in valid_skus:
                if sku not in sku_to_name:
                    sku_to_name[sku] = "Unknown Item"
                    self._product_name_cache[sku] = "Unknown Item"

            logger.info(f"Batch loaded {len(sku_to_name)} product names")
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

    def normalize_single_source_data(
        self, source: str, data: List[Dict], product_name_map: Dict[str, str]
    ) -> List[Dict]:
        """Use EXACT same normalization as master report"""
        if not isinstance(data, list):
            return []

        normalized_data = []

        for item in data:
            if not isinstance(item, dict):
                continue

            try:
                # Get SKU and canonical name
                sku_code = (item.get("sku_code") or "Unknown SKU").strip()
                if not sku_code:
                    sku_code = "Unknown SKU"

                canonical_name = product_name_map.get(sku_code, "Unknown Item")

                # Source-specific normalization - EXACT same logic as master report
                if source == "blinkit":
                    metrics = item.get("metrics", {}) or {}
                    normalized_item = {
                        "source": "blinkit",
                        "item_name": canonical_name,
                        "item_id": str(item.get("item_id", "")),
                        "sku_code": sku_code,
                        "city": item.get("city", "Unknown City"),
                        "warehouse": item.get("warehouse", "Unknown Warehouse"),
                        "units_sold": self.safe_float(
                            metrics.get("total_sales_in_period")
                        ),
                        "units_returned": self.safe_float(
                            metrics.get("total_returns_in_period")
                        ),
                        "total_amount": 0.0,
                        "closing_stock": self.safe_float(metrics.get("closing_stock")),
                        "days_in_stock": self.safe_int(
                            metrics.get("days_with_inventory")
                        ),
                        "daily_run_rate": self.safe_float(
                            metrics.get("avg_daily_on_stock_days")
                        ),
                        "days_of_coverage": self.safe_float(
                            metrics.get("days_of_coverage")
                        ),
                        "sessions": 0,
                    }

                elif source == "amazon":
                    warehouses = item.get("warehouses", []) or []
                    warehouse_str = (
                        ", ".join(warehouses)
                        if isinstance(warehouses, list) and warehouses
                        else "Unknown Warehouse"
                    )

                    normalized_item = {
                        "source": "amazon",
                        "item_name": canonical_name,
                        "item_id": item.get("asin", ""),
                        "sku_code": sku_code,
                        "city": "Multiple",
                        "warehouse": warehouse_str,
                        "units_sold": self.safe_float(item.get("units_sold")),
                        "units_returned": self.safe_float(item.get("units_returned")),
                        "total_amount": self.safe_float(item.get("total_amount")),
                        "closing_stock": self.safe_float(item.get("closing_stock")),
                        "days_in_stock": self.safe_int(item.get("total_days_in_stock")),
                        "daily_run_rate": self.safe_float(item.get("drr")),
                        "days_of_coverage": 0.0,
                        "sessions": self.safe_int(item.get("sessions")),
                    }

                elif source == "zoho":
                    normalized_item = {
                        "source": "zoho",
                        "item_name": canonical_name,
                        "item_id": str(item.get("item_id", "")),
                        "sku_code": sku_code,
                        "city": "Multiple",
                        "warehouse": "Multiple",
                        "units_sold": self.safe_float(item.get("units_sold")),
                        "units_returned": self.safe_float(item.get("units_returned")),
                        "total_amount": self.safe_float(item.get("total_amount")),
                        "closing_stock": self.safe_float(item.get("closing_stock")),
                        "days_in_stock": self.safe_int(item.get("total_days_in_stock")),
                        "daily_run_rate": self.safe_float(item.get("drr")),
                        "days_of_coverage": 0.0,
                        "sessions": 0,
                    }
                else:
                    continue

                # Calculate days of coverage if needed
                if (
                    normalized_item["daily_run_rate"] > 0
                    and normalized_item["days_of_coverage"] == 0
                ):
                    normalized_item["days_of_coverage"] = round(
                        normalized_item["closing_stock"]
                        / normalized_item["daily_run_rate"],
                        2,
                    )

                # Only add items with sales or returns
                if (
                    normalized_item["units_sold"] > 0
                    or normalized_item["units_returned"] > 0
                ):
                    normalized_data.append(normalized_item)

            except Exception as e:
                logger.error(f"Error normalizing item from {source}: {e}")
                continue

        return normalized_data

    def combine_normalized_data_for_dashboard(
        self,
        all_normalized_data: List[List[Dict]],
        start_date: datetime,
        end_date: datetime,
    ) -> List[Dict]:
        """Combine normalized data and convert to dashboard format - FIXED VERSION"""
        sku_data = defaultdict(
            lambda: {
                "sku_code": "",
                "item_name": "Unknown Item",
                "item_id": "",
                "sources": set(),
                "combined_metrics": {
                    "total_units_sold": 0.0,
                    "total_units_returned": 0.0,
                    "total_amount": 0.0,
                    "total_closing_stock": 0.0,
                    "total_sessions": 0.0,
                    "total_days_in_stock": 0.0,
                    "avg_daily_run_rate": 0.0,
                    "avg_days_of_coverage": 0.0,
                },
            }
        )

        # Single pass through all data - SAME as master report
        for source_data in all_normalized_data:
            if not isinstance(source_data, list):
                continue

            for item in source_data:
                if not isinstance(item, dict):
                    continue

                sku = item.get("sku_code", "Unknown SKU") or "Unknown SKU"
                source = item.get("source", "unknown")

                # Initialize if first time seeing this SKU
                if sku_data[sku]["sku_code"] == "":
                    sku_data[sku]["sku_code"] = sku
                    sku_data[sku]["item_name"] = item.get("item_name", "Unknown Item")
                    sku_data[sku]["item_id"] = str(item.get("item_id", ""))

                # Add source
                sku_data[sku]["sources"].add(source)

                # Update metrics efficiently
                metrics = sku_data[sku]["combined_metrics"]
                metrics["total_units_sold"] += self.safe_float(item.get("units_sold"))
                metrics["total_units_returned"] += self.safe_float(
                    item.get("units_returned")
                )
                metrics["total_amount"] += self.safe_float(item.get("total_amount"))
                metrics["total_closing_stock"] += self.safe_float(
                    item.get("closing_stock")
                )
                metrics["total_sessions"] += self.safe_float(item.get("sessions"))
                metrics["total_days_in_stock"] += self.safe_float(
                    item.get("days_in_stock")
                )

        # Calculate metrics and convert to dashboard format
        days_in_period = (end_date - start_date).days + 1
        result = []

        for sku, data in sku_data.items():
            if (
                data["combined_metrics"]["total_units_sold"] <= 0
            ):  # Skip items with no sales
                continue

            # Calculate metrics
            metrics = data["combined_metrics"]
            total_units = metrics["total_units_sold"] + metrics["total_units_returned"]
            total_days = metrics["total_days_in_stock"]

            # Calculate averages
            if total_days > 0:
                metrics["avg_daily_run_rate"] = round(total_units / total_days, 2)

            if metrics["avg_daily_run_rate"] > 0:
                metrics["avg_days_of_coverage"] = round(
                    metrics["total_closing_stock"] / metrics["avg_daily_run_rate"], 2
                )

            # Dashboard-specific calculations
            avg_daily_sales = (
                metrics["total_units_sold"] / days_in_period
                if days_in_period > 0
                else 0
            )
            days_of_coverage = (
                metrics["total_closing_stock"] / avg_daily_sales
                if avg_daily_sales > 0
                else 0
            )

            avg_daily_on_stock_days = (
                metrics["total_days_in_stock"] / days_in_period
                if days_in_period > 0
                else 0
            )
            avg_weekly_on_stock_days = avg_daily_on_stock_days * 7
            avg_monthly_on_stock_days = avg_daily_on_stock_days * 30

            sources_list = list(data["sources"])
            city = "Multiple" if len(sources_list) > 1 else sources_list[0].title()

            # FIXED: Ensure ALL items have consistent metrics structure
            product_data = {
                "item_id": (
                    int(data["item_id"]) if str(data["item_id"]).isdigit() else 0
                ),
                "item_name": data["item_name"],
                "sku_code": data["sku_code"],
                "city": city,
                "sources": sources_list,
                "metrics": {
                    "avg_daily_on_stock_days": round(avg_daily_on_stock_days, 2),
                    "avg_weekly_on_stock_days": round(avg_weekly_on_stock_days, 2),
                    "avg_monthly_on_stock_days": round(avg_monthly_on_stock_days, 2),
                    "total_sales_in_period": int(metrics["total_units_sold"]),
                    "total_returns_in_period": int(metrics["total_units_returned"]),
                    "total_amount": round(metrics["total_amount"], 2),
                    "days_of_coverage": round(days_of_coverage, 2),
                    "days_with_inventory": int(metrics["total_days_in_stock"]),
                    "closing_stock": int(metrics["total_closing_stock"]),
                    "sessions": int(metrics["total_sessions"]),  # Added for consistency
                    "daily_run_rate": round(
                        metrics["avg_daily_run_rate"], 2
                    ),  # Added for consistency
                },
                "year": start_date.year,
                "month": start_date.month,
            }
            result.append(product_data)

            # Debug logging to verify metrics structure
            logger.debug(
                f"Created metrics for {sku} from sources {sources_list}: {product_data['metrics']}"
            )

        logger.info(
            f"Combined {len(result)} products with consistent metrics structure"
        )
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
    FIXED: Get top performing products with consistent metrics structure for all sources.
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

        logger.info(
            f"FIXED: Generating dashboard with consistent metrics for {parsed_start_date} to {parsed_end_date}"
        )

        # Initialize service
        dashboard_service = MultiSourceDashboardService(db)

        # Step 1: Fetch all data
        tasks = []
        if include_blinkit:
            tasks.append(
                dashboard_service.get_blinkit_data(parsed_start_date, parsed_end_date)
            )
        if include_amazon:
            tasks.append(
                dashboard_service.get_amazon_data(parsed_start_date, parsed_end_date)
            )
        if include_zoho:
            tasks.append(
                dashboard_service.get_zoho_data(parsed_start_date, parsed_end_date)
            )

        if not tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one data source must be selected",
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results - filter out exceptions and empty results
        raw_source_data = []
        source_names = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} failed: {str(result)}")
                continue
            if isinstance(result, list) and result:
                raw_source_data.append(result)
                # Determine source name based on order
                if include_blinkit and len(raw_source_data) == 1:
                    source_names.append("blinkit")
                elif include_amazon and len(raw_source_data) == (
                    2 if include_blinkit else 1
                ):
                    source_names.append("amazon")
                elif include_zoho:
                    source_names.append("zoho")

        if not raw_source_data:
            return TopProductsResponse(
                products=[],
                total_count=0,
                current_month=parsed_start_date.month,
                current_year=parsed_start_date.year,
            )

        # Step 2: Extract SKU codes and batch load product names
        all_sku_codes = set()
        for source_data in raw_source_data:
            for item in source_data:
                if isinstance(item, dict):
                    sku = item.get("sku_code")
                    if sku and sku.strip():
                        all_sku_codes.add(sku.strip())

        product_name_map = await dashboard_service.batch_load_product_names(
            all_sku_codes
        )
        logger.info(f"Loaded names for {len(product_name_map)} products")

        # Step 3: Normalize data
        all_normalized_data = []

        for i, source_data in enumerate(raw_source_data):
            # Map to correct source names
            if include_blinkit and i == 0:
                source_name = "blinkit"
            elif include_amazon and i == (1 if include_blinkit else 0):
                source_name = "amazon"
            elif include_zoho and i == (
                (1 if include_blinkit else 0) + (1 if include_amazon else 0)
            ):
                source_name = "zoho"
            else:
                source_name = f"unknown_{i}"
            normalized = dashboard_service.normalize_single_source_data(
                source_name, source_data, product_name_map
            )
            if normalized:
                all_normalized_data.append(normalized)
                logger.info(f"Normalized {len(normalized)} {source_name} items")

        if not all_normalized_data:
            return TopProductsResponse(
                products=[],
                total_count=0,
                current_month=parsed_start_date.month,
                current_year=parsed_start_date.year,
            )

        # Step 4: Combine normalized data - FIXED VERSION
        combined_data = dashboard_service.combine_normalized_data_for_dashboard(
            all_normalized_data, parsed_start_date, parsed_end_date
        )

        logger.info(f"Combined data for {len(combined_data)} unique SKUs")

        # VALIDATION: Check that all items have metrics structure
        items_without_metrics = [
            item for item in combined_data if "metrics" not in item
        ]
        if items_without_metrics:
            logger.error(
                f"FOUND {len(items_without_metrics)} items without metrics structure!"
            )
            for item in items_without_metrics[:3]:  # Log first 3 for debugging
                logger.error(
                    f"Item without metrics: {item.get('sku_code', 'Unknown')} - {item.keys()}"
                )
        else:
            logger.info(
                f"✓ ALL {len(combined_data)} items have consistent metrics structure"
            )

        # Step 5: Sort by total sales and limit
        sorted_products = sorted(
            combined_data,
            key=lambda x: x.get("metrics", {}).get(
                "total_sales_in_period", 0
            ),  # Safe access
            reverse=True,
        )[:limit]
        # Log summary for verification
        total_sales = sum(
            p.get("metrics", {}).get("total_sales_in_period", 0)
            for p in sorted_products
        )
        total_stock = sum(
            p.get("metrics", {}).get("closing_stock", 0) for p in sorted_products
        )
        total_amount = sum(
            p.get("metrics", {}).get("total_amount", 0) for p in sorted_products
        )

        logger.info(
            f"DASHBOARD TOTALS - Sales: {total_sales}, Stock: {total_stock}, Amount: {total_amount}"
        )
        logger.info(
            f"Returning {len(sorted_products)} products from {len(combined_data)} total SKUs"
        )

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
        logger.error(f"Error in FIXED top products: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@router.get("/top-products/summary")
async def get_top_products_summary_fixed(
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
    FIXED: Get summary statistics with consistent metrics access.
    """
    try:
        # Handle date parameters (same as top products)
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

        logger.info(
            f"FIXED: Generating summary with consistent metrics for {parsed_start_date} to {parsed_end_date}"
        )

        # Use the EXACT same logic as top products up to the combination step
        dashboard_service = MultiSourceDashboardService(db)

        tasks = []
        if include_blinkit:
            tasks.append(
                dashboard_service.get_blinkit_data(parsed_start_date, parsed_end_date)
            )
        if include_amazon:
            tasks.append(
                dashboard_service.get_amazon_data(parsed_start_date, parsed_end_date)
            )
        if include_zoho:
            tasks.append(
                dashboard_service.get_zoho_data(parsed_start_date, parsed_end_date)
            )

        if not tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one data source must be selected",
            )

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results (same as top products)
        raw_source_data = []
        source_names = []

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Task {i} failed: {str(result)}")
                continue
            if isinstance(result, list) and result:
                raw_source_data.append(result)
                if include_blinkit and len(raw_source_data) == 1:
                    source_names.append("blinkit")
                elif include_amazon and len(raw_source_data) == (
                    2 if include_blinkit else 1
                ):
                    source_names.append("amazon")
                elif include_zoho:
                    source_names.append("zoho")

        if not raw_source_data:
            return {
                "total_products": 0,
                "total_sales": 0.0,
                "avg_sales": 0.0,
                "total_closing_stock": 0.0,
                "total_amount": 0.0,
                "source_counts": {},
                "sources_included": [],
                "year": parsed_start_date.year,
                "month": parsed_start_date.month,
                "start_date": parsed_start_date.strftime("%Y-%m-%d"),
                "end_date": parsed_end_date.strftime("%Y-%m-%d"),
            }

        # Extract SKUs and normalize (same as top products)
        all_sku_codes = set()
        for source_data in raw_source_data:
            for item in source_data:
                if isinstance(item, dict):
                    sku = item.get("sku_code")
                    if sku and sku.strip():
                        all_sku_codes.add(sku.strip())

        product_name_map = await dashboard_service.batch_load_product_names(
            all_sku_codes
        )

        all_normalized_data = []
        source_counts = {}

        for i, source_data in enumerate(raw_source_data):
            if include_blinkit and i == 0:
                source_name = "blinkit"
            elif include_amazon and i == (1 if include_blinkit else 0):
                source_name = "amazon"
            elif include_zoho and i == (
                (1 if include_blinkit else 0) + (1 if include_amazon else 0)
            ):
                source_name = "zoho"
            else:
                source_name = f"unknown_{i}"

            normalized = dashboard_service.normalize_single_source_data(
                source_name, source_data, product_name_map
            )
            if normalized:
                all_normalized_data.append(normalized)
                source_counts[source_name] = len(normalized)
                logger.info(f"Normalized {len(normalized)} {source_name} items")

        if not all_normalized_data:
            return {
                "total_products": 0,
                "total_sales": 0.0,
                "avg_sales": 0.0,
                "total_closing_stock": 0.0,
                "total_amount": 0.0,
                "source_counts": {},
                "sources_included": [],
                "year": parsed_start_date.year,
                "month": parsed_start_date.month,
                "start_date": parsed_start_date.strftime("%Y-%m-%d"),
                "end_date": parsed_end_date.strftime("%Y-%m-%d"),
            }

        # FIXED: Use the SAME combined data logic as top products
        combined_data = dashboard_service.combine_normalized_data_for_dashboard(
            all_normalized_data, parsed_start_date, parsed_end_date
        )

        logger.info(f"Combined data for {len(combined_data)} unique SKUs")

        # FIXED: Aggregate from the same metrics structure as top products
        total_products = len(combined_data)
        total_sales = 0.0
        total_closing_stock = 0.0
        total_amount = 0.0

        for item in combined_data:
            metrics = item.get("metrics", {})
            if not metrics:  # Log items without metrics for debugging
                logger.warning(
                    f"Item {item.get('sku_code', 'Unknown')} has no metrics structure"
                )
                continue

            total_sales += dashboard_service.safe_float(
                metrics.get("total_sales_in_period", 0)
            )
            total_closing_stock += dashboard_service.safe_float(
                metrics.get("closing_stock", 0)
            )
            total_amount += dashboard_service.safe_float(metrics.get("total_amount", 0))

        avg_sales = total_sales / total_products if total_products > 0 else 0

        # Validation logging
        logger.info(
            f"SUMMARY TOTALS - Products: {total_products}, Sales: {total_sales}, Stock: {total_closing_stock}, Amount: {total_amount}"
        )

        return {
            "total_products": total_products,
            "total_sales": round(total_sales, 2),
            "avg_sales": round(avg_sales, 2),
            "total_closing_stock": round(total_closing_stock, 2),
            "total_amount": round(total_amount, 2),
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
        logger.error(f"Error in FIXED summary: {e}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )
