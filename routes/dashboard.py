from datetime import datetime, timedelta
from typing import List, Optional, Dict, Any, Set, Tuple
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

AMAZON_SKU_COLLECTION = "amazon_sku_mapping"
AMAZON_SALES_COLLECTION = "amazon_sales"
AMAZON_INVENTORY_COLLECTION = "amazon_inventory"
AMAZON_REPORT_COLLECTION = "amazon_sales_inventory_reports"

BLINKIT_SKU_COLLECTION = "blinkit_sku_mapping"
BLINKIT_SALES_COLLECTION = "blinkit_sales_temp"
BLINKIT_INVENTORY_COLLECTION = "blinkit_inventory"
BLINKIT_REPORT_COLLECTION = "blinkit_sales_inventory_reports"

router = APIRouter()

_RISK_CRITICAL_DAYS = 7
_RISK_LOW_DAYS = 21


class ProductMetrics(BaseModel):
    in_stock_rate: float             # fraction of period days with inventory (replaces avg_daily_on_stock_days)
    avg_weekly_on_stock_days: float  # in_stock_rate * 7
    avg_monthly_on_stock_days: float # in_stock_rate * 30
    total_sales_in_period: int
    total_returns_in_period: int
    total_amount: float
    days_of_coverage: float          # closing_stock / stock-day DRR (calendar fallback)
    days_with_inventory: int
    closing_stock: int
    sessions: int
    daily_run_rate: float
    risk_level: str                  # "critical" ≤7d, "low" 8-21d, "healthy" >21d
    suggested_order_qty: int         # max(0, target_days * drr - closing_stock)
    missed_sales_qty: float          # units of missed/cancelled demand in period
    mom_growth_pct: Optional[float] = None  # % change vs prior equivalent period


class TopProduct(BaseModel):
    item_id: int
    item_name: str
    sku_code: str
    metrics: ProductMetrics
    year: int
    month: int
    sources: List[str]
    city: Optional[str] = "Multiple"


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

    async def get_blinkit_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        try:
            from .blinkit import generate_report_by_date_range
            report_data = await generate_report_by_date_range(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                database=self.database,
            )
            if isinstance(report_data, dict):
                return report_data.get("data", [])
            return report_data if isinstance(report_data, list) else []
        except Exception as e:
            logger.error(f"Error fetching Blinkit data: {e}")
            return []

    async def get_amazon_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        try:
            from .amazon import generate_report_by_date_range
            report_data = await generate_report_by_date_range(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                database=self.database,
                report_type="all",
            )
            return report_data if isinstance(report_data, list) else []
        except Exception as e:
            logger.error(f"Error fetching Amazon data: {e}")
            return []

    async def get_zoho_data(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        try:
            from .zoho import get_sales_report_fast
            response = await get_sales_report_fast(
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d"),
                db=self.database,
            )
            if hasattr(response, "body"):
                import json
                content = json.loads(response.body)
                return content.get("data", [])
            if isinstance(response, dict):
                return response.get("data", [])
            return response if isinstance(response, list) else []
        except Exception as e:
            logger.error(f"Error fetching Zoho data: {e}")
            return []

    async def get_missed_sales_by_sku(
        self, start_date: datetime, end_date: datetime
    ) -> Dict[str, float]:
        """Return {sku_code: total_missed_sales_qty} for the period."""
        try:
            collection = self.database["missed_sales"]

            def _fetch():
                return list(collection.find(
                    {
                        "date": {
                            "$gte": start_date,
                            "$lte": end_date.replace(hour=23, minute=59, second=59),
                        }
                    },
                    {"item_code": 1, "missed_sales_quantity": 1, "_id": 0},
                ))

            docs = await asyncio.to_thread(_fetch)
            result: Dict[str, float] = defaultdict(float)
            for doc in docs:
                sku = doc.get("item_code")
                qty = doc.get("missed_sales_quantity", 0)
                if sku:
                    result[sku] += float(qty or 0)
            return dict(result)
        except Exception as e:
            logger.error(f"Error fetching missed sales: {e}")
            return {}

    async def batch_load_product_names(self, sku_codes: Set[str]) -> Dict[str, str]:
        if not sku_codes:
            return {}
        try:
            valid_skus = {sku for sku in sku_codes if sku and sku != "Unknown SKU"}
            if not valid_skus:
                return {}

            products_collection = self.database.get_collection("products")

            def _fetch_products():
                return list(products_collection.find(
                    {"cf_sku_code": {"$in": list(valid_skus)}},
                    {"cf_sku_code": 1, "name": 1, "_id": 0},
                ))

            products = await asyncio.to_thread(_fetch_products)
            sku_to_name = {
                p["cf_sku_code"]: p["name"]
                for p in products
                if p.get("cf_sku_code") and p.get("name")
            }
            self._product_name_cache.update(sku_to_name)
            for sku in valid_skus:
                if sku not in sku_to_name:
                    sku_to_name[sku] = "Unknown Item"
                    self._product_name_cache[sku] = "Unknown Item"

            logger.info(f"Batch loaded {len(sku_to_name)} product names")
            return sku_to_name
        except Exception as e:
            logger.error(f"Error in batch loading product names: {e}")
            return {}

    async def get_active_skus(self) -> Set[str]:
        try:
            products_collection = self.database.get_collection("products")

            def _fetch():
                return list(products_collection.find(
                    {"purchase_status": "active"},
                    {"cf_sku_code": 1, "_id": 0},
                ))

            docs = await asyncio.to_thread(_fetch)
            active = {d["cf_sku_code"] for d in docs if d.get("cf_sku_code")}
            logger.info(f"Found {len(active)} active SKUs")
            return active
        except Exception as e:
            logger.error(f"Error fetching active SKUs: {e}")
            return set()

    @staticmethod
    def safe_float(value: Any, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (ValueError, TypeError):
            return default

    @staticmethod
    def safe_int(value: Any, default: int = 0) -> int:
        try:
            if value is None or value == "":
                return default
            return int(float(value))
        except (ValueError, TypeError):
            return default

    def normalize_single_source_data(
        self, source: str, data: List[Dict], product_name_map: Dict[str, str]
    ) -> List[Dict]:
        """Normalize source data to a common schema."""
        if not isinstance(data, list):
            return []

        normalized_data = []

        for item in data:
            if not isinstance(item, dict):
                continue

            try:
                sku_code = (item.get("sku_code") or "Unknown SKU").strip() or "Unknown SKU"
                canonical_name = product_name_map.get(sku_code, "Unknown Item")

                if source == "blinkit":
                    metrics = item.get("metrics", {}) or {}
                    normalized_item = {
                        "source": "blinkit",
                        "item_name": canonical_name,
                        "item_id": str(item.get("item_id", "")),
                        "sku_code": sku_code,
                        "city": item.get("city", "Unknown City"),
                        "warehouse": item.get("warehouse", "Unknown Warehouse"),
                        "units_sold": self.safe_float(metrics.get("total_sales_in_period")),
                        "units_returned": self.safe_float(metrics.get("total_returns_in_period")),
                        "total_amount": 0.0,
                        "closing_stock": self.safe_float(metrics.get("closing_stock")),
                        "days_in_stock": self.safe_int(metrics.get("days_with_inventory")),
                        "daily_run_rate": self.safe_float(metrics.get("avg_daily_on_stock_days")),
                        "days_of_coverage": self.safe_float(metrics.get("days_of_coverage")),
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

                if normalized_item["daily_run_rate"] > 0 and normalized_item["days_of_coverage"] == 0:
                    normalized_item["days_of_coverage"] = round(
                        normalized_item["closing_stock"] / normalized_item["daily_run_rate"], 2
                    )

                if normalized_item["units_sold"] > 0 or normalized_item["units_returned"] > 0:
                    normalized_data.append(normalized_item)

            except Exception as e:
                logger.error(f"Error normalizing item from {source}: {e}")
                continue

        return normalized_data

    @staticmethod
    def _compute_risk_level(days_of_coverage: float) -> str:
        if days_of_coverage <= _RISK_CRITICAL_DAYS:
            return "critical"
        if days_of_coverage <= _RISK_LOW_DAYS:
            return "low"
        return "healthy"

    def combine_normalized_data_for_dashboard(
        self,
        all_normalized_data: List[List[Dict]],
        start_date: datetime,
        end_date: datetime,
        target_days: int = 30,
        missed_sales_map: Optional[Dict[str, float]] = None,
        prior_sales_by_sku: Optional[Dict[str, float]] = None,
    ) -> List[Dict]:
        sku_data: Dict[str, Dict] = defaultdict(lambda: {
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
            },
        })

        for source_data in all_normalized_data:
            if not isinstance(source_data, list):
                continue
            for item in source_data:
                if not isinstance(item, dict):
                    continue

                sku = item.get("sku_code", "Unknown SKU") or "Unknown SKU"

                if sku_data[sku]["sku_code"] == "":
                    sku_data[sku]["sku_code"] = sku
                    sku_data[sku]["item_name"] = item.get("item_name", "Unknown Item")
                    sku_data[sku]["item_id"] = str(item.get("item_id", ""))

                sku_data[sku]["sources"].add(item.get("source", "unknown"))

                m = sku_data[sku]["combined_metrics"]
                m["total_units_sold"] += self.safe_float(item.get("units_sold"))
                m["total_units_returned"] += self.safe_float(item.get("units_returned"))
                m["total_amount"] += self.safe_float(item.get("total_amount"))
                m["total_closing_stock"] += self.safe_float(item.get("closing_stock"))
                m["total_sessions"] += self.safe_float(item.get("sessions"))
                m["total_days_in_stock"] += self.safe_float(item.get("days_in_stock"))

        days_in_period = (end_date - start_date).days + 1
        result = []

        for sku, data in sku_data.items():
            m = data["combined_metrics"]
            if m["total_units_sold"] <= 0:
                continue

            total_days = m["total_days_in_stock"]

            # DRR: net units sold / stock days (calendar-day fallback).
            # Matches the master report's avg_daily_run_rate logic exactly.
            if total_days > 0:
                drr = round(m["total_units_sold"] / total_days, 2)
            elif days_in_period > 0:
                drr = round(m["total_units_sold"] / days_in_period, 4)
            else:
                drr = 0.0

            # days_of_coverage using stock-day DRR (same denominator as master report)
            days_of_coverage = round(m["total_closing_stock"] / drr, 2) if drr > 0 else 0.0

            # In-stock rate: fraction of period days the item had stock
            in_stock_rate = round(total_days / days_in_period, 4) if days_in_period > 0 else 0.0

            # Suggested order to reach target_days of coverage
            suggested_order_qty = max(0, int(round(target_days * drr - m["total_closing_stock"])))

            risk_level = self._compute_risk_level(days_of_coverage)
            missed_qty = float((missed_sales_map or {}).get(sku, 0))

            # MOM / period-over-period growth
            mom_growth_pct: Optional[float] = None
            if prior_sales_by_sku is not None:
                prior_sales = prior_sales_by_sku.get(sku, 0.0)
                if prior_sales > 0:
                    mom_growth_pct = round(
                        (m["total_units_sold"] - prior_sales) / prior_sales * 100, 1
                    )
                elif m["total_units_sold"] > 0:
                    mom_growth_pct = 100.0  # new product (no prior sales)

            sources_list = list(data["sources"])
            city = "Multiple" if len(sources_list) > 1 else sources_list[0].title()

            product_data = {
                "item_id": int(data["item_id"]) if str(data["item_id"]).isdigit() else 0,
                "item_name": data["item_name"],
                "sku_code": data["sku_code"],
                "city": city,
                "sources": sources_list,
                "metrics": {
                    "in_stock_rate": in_stock_rate,
                    "avg_weekly_on_stock_days": round(in_stock_rate * 7, 2),
                    "avg_monthly_on_stock_days": round(in_stock_rate * 30, 2),
                    "total_sales_in_period": int(m["total_units_sold"]),
                    "total_returns_in_period": int(m["total_units_returned"]),
                    "total_amount": round(m["total_amount"], 2),
                    "days_of_coverage": days_of_coverage,
                    "days_with_inventory": int(total_days),
                    "closing_stock": int(m["total_closing_stock"]),
                    "sessions": int(m["total_sessions"]),
                    "daily_run_rate": drr,
                    "risk_level": risk_level,
                    "suggested_order_qty": suggested_order_qty,
                    "missed_sales_qty": missed_qty,
                    "mom_growth_pct": mom_growth_pct,
                },
                "year": start_date.year,
                "month": start_date.month,
            }
            result.append(product_data)

        logger.info(f"Combined {len(result)} products")
        return result

    @staticmethod
    def _extract_sales_by_sku(combined_data: List[Dict]) -> Dict[str, float]:
        """Extract {sku_code: total_units_sold} from already-combined data."""
        return {
            item["sku_code"]: float(item["metrics"]["total_sales_in_period"])
            for item in combined_data
            if item.get("sku_code") and item.get("metrics")
        }


def get_month_date_range(year: int, month: int):
    start_date = datetime(year, month, 1)
    last_day = calendar.monthrange(year, month)[1]
    end_date = datetime(year, month, last_day, 23, 59, 59)
    return start_date, end_date


def _parse_date_params(
    start_date: Optional[str],
    end_date: Optional[str],
    year: Optional[int],
    month: Optional[int],
) -> Tuple[datetime, datetime]:
    if start_date and end_date:
        try:
            parsed_start = datetime.strptime(start_date, "%Y-%m-%d")
            parsed_end = datetime.strptime(end_date, "%Y-%m-%d").replace(
                hour=23, minute=59, second=59
            )
            return parsed_start, parsed_end
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use YYYY-MM-DD.",
            )
    cur = datetime.now()
    return get_month_date_range(year or cur.year, month or cur.month)



_SORT_OPTIONS = {
    "total_sales": (lambda x: x.get("metrics", {}).get("total_sales_in_period", 0), True),
    "days_of_coverage": (lambda x: x.get("metrics", {}).get("days_of_coverage", 0), False),
    "drr": (lambda x: x.get("metrics", {}).get("daily_run_rate", 0), True),
    "missed_sales": (lambda x: x.get("metrics", {}).get("missed_sales_qty", 0), True),
}


@router.get("/top-products", response_model=TopProductsResponse)
async def get_top_performing_products(
    limit: int = Query(10, ge=1, le=50, description="Number of top products to return"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    year: Optional[int] = Query(None, description="Year (legacy)"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Month (legacy)"),
    include_blinkit: bool = Query(True),
    include_amazon: bool = Query(True),
    include_zoho: bool = Query(True),
    sort_by: str = Query(
        "total_sales",
        description="Sort field: total_sales | days_of_coverage (urgent first) | drr | missed_sales",
    ),
    target_days: int = Query(
        30, ge=1, le=365, description="Target days of coverage for suggested_order_qty"
    ),
    include_trend: bool = Query(
        False, description="Include period-over-period growth % (fetches prior period in parallel)"
    ),
    db=Depends(get_database),
):
    try:
        parsed_start, parsed_end = _parse_date_params(start_date, end_date, year, month)

        service = MultiSourceDashboardService(db)

        # Build source task list (order preserved, no index guessing)
        source_tasks: List = []
        task_sources: List[str] = []
        if include_blinkit:
            source_tasks.append(service.get_blinkit_data(parsed_start, parsed_end))
            task_sources.append("blinkit")
        if include_amazon:
            source_tasks.append(service.get_amazon_data(parsed_start, parsed_end))
            task_sources.append("amazon")
        if include_zoho:
            source_tasks.append(service.get_zoho_data(parsed_start, parsed_end))
            task_sources.append("zoho")

        if not source_tasks:
            raise HTTPException(status_code=400, detail="At least one data source must be selected")

        # Prior period for trend (same duration, ending the day before start)
        prior_source_tasks: List = []
        prior_task_sources: List[str] = []
        prior_start = prior_end = None
        if include_trend:
            period_days = (parsed_end.date() - parsed_start.date()).days + 1
            prior_end = parsed_start - timedelta(days=1)
            prior_start = prior_end - timedelta(days=period_days - 1)
            for src in task_sources:
                if src == "blinkit":
                    prior_source_tasks.append(service.get_blinkit_data(prior_start, prior_end))
                elif src == "amazon":
                    prior_source_tasks.append(service.get_amazon_data(prior_start, prior_end))
                elif src == "zoho":
                    prior_source_tasks.append(service.get_zoho_data(prior_start, prior_end))
                prior_task_sources.append(src)

        # All parallel: active_skus + missed_sales + source data [+ prior period]
        all_results = await asyncio.gather(
            service.get_active_skus(),
            service.get_missed_sales_by_sku(parsed_start, parsed_end),
            *source_tasks,
            *prior_source_tasks,
            return_exceptions=True,
        )

        active_skus: Set[str] = all_results[0] if not isinstance(all_results[0], Exception) else set()
        missed_sales_map: Dict[str, float] = (
            all_results[1] if not isinstance(all_results[1], Exception) else {}
        )
        source_results = all_results[2:2 + len(source_tasks)]
        prior_results = all_results[2 + len(source_tasks):]

        # Collect valid source data (tagged by source name)
        raw_source_data: List[Tuple[str, List[Dict]]] = []
        for src_name, result in zip(task_sources, source_results):
            if isinstance(result, Exception):
                logger.error(f"{src_name} fetch failed: {result}")
                continue
            if isinstance(result, list) and result:
                raw_source_data.append((src_name, result))

        if not raw_source_data:
            return TopProductsResponse(
                products=[],
                total_count=0,
                current_month=parsed_start.month,
                current_year=parsed_start.year,
            )

        # Batch-load product names from all sources
        all_sku_codes = {
            item.get("sku_code", "").strip()
            for _, source_data in raw_source_data
            for item in source_data
            if isinstance(item, dict) and item.get("sku_code", "").strip()
        }
        product_name_map = await service.batch_load_product_names(all_sku_codes)

        # Normalize current period
        all_normalized: List[List[Dict]] = []
        for src_name, source_data in raw_source_data:
            normalized = service.normalize_single_source_data(src_name, source_data, product_name_map)
            if normalized:
                all_normalized.append(normalized)
                logger.info(f"Normalized {len(normalized)} {src_name} items")

        if not all_normalized:
            return TopProductsResponse(
                products=[],
                total_count=0,
                current_month=parsed_start.month,
                current_year=parsed_start.year,
            )

        # Prior period for trend
        prior_sales_by_sku: Optional[Dict[str, float]] = None
        if include_trend and prior_results and prior_start and prior_end:
            prior_normalized: List[List[Dict]] = []
            for src_name, result in zip(prior_task_sources, prior_results):
                if isinstance(result, Exception):
                    continue
                if isinstance(result, list) and result:
                    n = service.normalize_single_source_data(src_name, result, product_name_map)
                    if n:
                        prior_normalized.append(n)
            if prior_normalized:
                prior_combined = service.combine_normalized_data_for_dashboard(
                    prior_normalized, prior_start, prior_end, target_days=target_days
                )
                prior_sales_by_sku = service._extract_sales_by_sku(prior_combined)

        # Combine current period
        combined_data = service.combine_normalized_data_for_dashboard(
            all_normalized,
            parsed_start,
            parsed_end,
            target_days=target_days,
            missed_sales_map=missed_sales_map,
            prior_sales_by_sku=prior_sales_by_sku,
        )

        # Filter to active SKUs
        if active_skus:
            before = len(combined_data)
            combined_data = [d for d in combined_data if d.get("sku_code") in active_skus]
            logger.info(
                f"Filtered to {len(combined_data)} active SKUs (removed {before - len(combined_data)})"
            )

        # Sort
        sort_key, reverse = _SORT_OPTIONS.get(sort_by, _SORT_OPTIONS["total_sales"])
        sorted_products = sorted(combined_data, key=sort_key, reverse=reverse)[:limit]

        logger.info(
            f"Returning {len(sorted_products)} of {len(combined_data)} SKUs, sorted by {sort_by}"
        )

        return TopProductsResponse(
            products=sorted_products,
            total_count=len(combined_data),
            current_month=parsed_start.month,
            current_year=parsed_start.year,
        )

    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in top products: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


@router.get("/top-products/summary")
async def get_top_products_summary(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    year: Optional[int] = Query(None, description="Year (legacy)"),
    month: Optional[int] = Query(None, ge=1, le=12, description="Month (legacy)"),
    include_blinkit: bool = Query(True),
    include_amazon: bool = Query(True),
    include_zoho: bool = Query(True),
    target_days: int = Query(30, ge=1, le=365),
    db=Depends(get_database),
):
    try:
        parsed_start, parsed_end = _parse_date_params(start_date, end_date, year, month)

        service = MultiSourceDashboardService(db)

        source_tasks: List = []
        task_sources: List[str] = []
        if include_blinkit:
            source_tasks.append(service.get_blinkit_data(parsed_start, parsed_end))
            task_sources.append("blinkit")
        if include_amazon:
            source_tasks.append(service.get_amazon_data(parsed_start, parsed_end))
            task_sources.append("amazon")
        if include_zoho:
            source_tasks.append(service.get_zoho_data(parsed_start, parsed_end))
            task_sources.append("zoho")

        if not source_tasks:
            raise HTTPException(status_code=400, detail="At least one data source must be selected")

        all_results = await asyncio.gather(
            service.get_active_skus(),
            service.get_missed_sales_by_sku(parsed_start, parsed_end),
            *source_tasks,
            return_exceptions=True,
        )

        active_skus: Set[str] = all_results[0] if not isinstance(all_results[0], Exception) else set()
        missed_sales_map: Dict[str, float] = (
            all_results[1] if not isinstance(all_results[1], Exception) else {}
        )
        source_results = all_results[2:]

        empty_response = {
            "total_products": 0,
            "total_sales": 0.0,
            "avg_sales": 0.0,
            "total_closing_stock": 0.0,
            "total_amount": 0.0,
            "total_missed_sales_qty": 0.0,
            "total_suggested_order_qty": 0,
            "by_risk_level": {"critical": 0, "low": 0, "healthy": 0},
            "source_counts": {},
            "sources_included": [],
            "year": parsed_start.year,
            "month": parsed_start.month,
            "start_date": parsed_start.strftime("%Y-%m-%d"),
            "end_date": parsed_end.strftime("%Y-%m-%d"),
        }

        raw_source_data: List[Tuple[str, List[Dict]]] = []
        source_counts: Dict[str, int] = {}
        for src_name, result in zip(task_sources, source_results):
            if isinstance(result, Exception):
                logger.error(f"{src_name} fetch failed: {result}")
                continue
            if isinstance(result, list) and result:
                raw_source_data.append((src_name, result))

        if not raw_source_data:
            return empty_response

        all_sku_codes = {
            item.get("sku_code", "").strip()
            for _, source_data in raw_source_data
            for item in source_data
            if isinstance(item, dict) and item.get("sku_code", "").strip()
        }
        product_name_map = await service.batch_load_product_names(all_sku_codes)

        all_normalized: List[List[Dict]] = []
        for src_name, source_data in raw_source_data:
            normalized = service.normalize_single_source_data(src_name, source_data, product_name_map)
            if normalized:
                all_normalized.append(normalized)
                source_counts[src_name] = len(normalized)

        if not all_normalized:
            return empty_response

        combined_data = service.combine_normalized_data_for_dashboard(
            all_normalized,
            parsed_start,
            parsed_end,
            target_days=target_days,
            missed_sales_map=missed_sales_map,
        )

        if active_skus:
            combined_data = [d for d in combined_data if d.get("sku_code") in active_skus]

        total_products = len(combined_data)
        total_sales = 0.0
        total_closing_stock = 0.0
        total_amount = 0.0
        total_missed = 0.0
        total_suggested = 0
        by_risk: Dict[str, int] = {"critical": 0, "low": 0, "healthy": 0}

        for item in combined_data:
            metrics = item.get("metrics", {})
            if not metrics:
                continue
            total_sales += service.safe_float(metrics.get("total_sales_in_period", 0))
            total_closing_stock += service.safe_float(metrics.get("closing_stock", 0))
            total_amount += service.safe_float(metrics.get("total_amount", 0))
            total_missed += service.safe_float(metrics.get("missed_sales_qty", 0))
            total_suggested += int(metrics.get("suggested_order_qty", 0))
            risk = metrics.get("risk_level", "healthy")
            by_risk[risk] = by_risk.get(risk, 0) + 1

        avg_sales = total_sales / total_products if total_products > 0 else 0.0

        logger.info(
            f"SUMMARY: {total_products} active SKUs, sales={total_sales}, "
            f"stock={total_closing_stock}, amount={total_amount}"
        )

        return {
            "total_products": total_products,
            "total_sales": round(total_sales, 2),
            "avg_sales": round(avg_sales, 2),
            "total_closing_stock": round(total_closing_stock, 2),
            "total_amount": round(total_amount, 2),
            "total_missed_sales_qty": round(total_missed, 2),
            "total_suggested_order_qty": total_suggested,
            "by_risk_level": by_risk,
            "source_counts": source_counts,
            "sources_included": list(source_counts.keys()),
            "year": parsed_start.year,
            "month": parsed_start.month,
            "start_date": parsed_start.strftime("%Y-%m-%d"),
            "end_date": parsed_end.strftime("%Y-%m-%d"),
        }

    except PyMongoError as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")
    except Exception as e:
        logger.error(f"Error in summary: {e}\n{traceback.format_exc()}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")
