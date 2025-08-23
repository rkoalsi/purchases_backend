from fastapi import APIRouter, HTTPException, status, Depends, Query
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime
from typing import List, Dict, Any, Optional
import asyncio
import pandas as pd
import io
from ..database import get_database, serialize_mongo_document
import logging
import traceback

logger = logging.getLogger(__name__)

router = APIRouter()


class MasterReportService:
    """Service to handle master reporting across Blinkit, Amazon, and Zoho"""

    def __init__(self, database):
        self.database = database
        self._product_name_cache = {}  # Cache for product lookups

    async def get_blinkit_report(
        self, start_date: str, end_date: str
    ) -> Dict[str, Any]:
        """Get Blinkit report data"""
        try:
            # Import Blinkit router functions - fix import path
            from .blinkit import generate_report_by_date_range

            report_data = await generate_report_by_date_range(
                start_date=start_date, end_date=end_date, database=self.database
            )

            return {
                "source": "blinkit",
                "data": report_data.get("data", []) if report_data else [],
                "success": True,
                "error": None,
            }

        except Exception as e:
            logger.error(f"Error fetching Blinkit report: {e}")
            logger.error(f"Blinkit error traceback: {traceback.format_exc()}")
            return {"source": "blinkit", "data": [], "success": False, "error": str(e)}

    async def get_amazon_report(
        self, start_date: str, end_date: str, report_type: str = "all"
    ) -> Dict[str, Any]:
        """Get Amazon report data"""
        try:
            # Import Amazon router functions - fix import path
            from .amazon import generate_report_by_date_range

            report_data = await generate_report_by_date_range(
                start_date=start_date,
                end_date=end_date,
                database=self.database,
                report_type=report_type,
            )

            return {
                "source": "amazon",
                "data": report_data if isinstance(report_data, list) else [],
                "success": True,
                "error": None,
            }

        except Exception as e:
            logger.error(f"Error fetching Amazon report: {e}")
            logger.error(f"Amazon error traceback: {traceback.format_exc()}")
            return {"source": "amazon", "data": [], "success": False, "error": str(e)}

    async def get_zoho_report(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get Zoho report data"""
        try:
            # Import Zoho router functions - fix import path
            from .zoho import get_sales_report_fast

            # Call the Zoho fast sales report API
            response = await get_sales_report_fast(
                start_date=start_date, end_date=end_date, db=self.database
            )

            # Extract data from JSONResponse if needed
            report_data = []
            if hasattr(response, "body"):
                import json

                content = json.loads(response.body)
                report_data = content.get("data", [])
            elif isinstance(response, dict):
                report_data = response.get("data", [])
            else:
                # If response is already the data
                report_data = response if isinstance(response, list) else []

            return {
                "source": "zoho",
                "data": report_data,
                "success": True,
                "error": None,
            }

        except Exception as e:
            logger.error(f"Error fetching Zoho report: {e}")
            logger.error(f"Zoho error traceback: {traceback.format_exc()}")
            return {"source": "zoho", "data": [], "success": False, "error": str(e)}

    async def get_canonical_product_name(self, sku_code: str) -> str:
        """Get canonical product name from products collection based on cf_sku_code"""
        if not sku_code or sku_code == "Unknown SKU":
            return "Unknown Item"

    def safe_get_value(self, item: Dict, key: str, default: Any = "") -> Any:
        """Safely get value from dictionary with None handling"""
        if not isinstance(item, dict):
            return default
        value = item.get(key, default)
        return value if value is not None else default

    async def get_canonical_product_name(self, sku_code: str) -> str:
        """Get canonical product name from products collection based on cf_sku_code"""
        if not sku_code or sku_code == "Unknown SKU":
            return "Unknown Item"

        # Check cache first
        if sku_code in self._product_name_cache:
            return self._product_name_cache[sku_code]

        try:
            products_collection = self.database.get_collection("products")
            products = list(products_collection.find(
                {"cf_sku_code": sku_code}, {"name": 1, "_id": 0}
            ))
            print(products)
            if products and len(products)>0:
                canonical_name = products[-1].get("name")
                self._product_name_cache[sku_code] = canonical_name
                return canonical_name
            else:
                # Cache the "not found" result to avoid repeated queries
                self._product_name_cache[sku_code] = "Unknown Item"
                return "Unknown Item"

        except Exception as e:
            logger.error(f"Error looking up product name for SKU {sku_code}: {e}")
            return "Unknown Item"

    async def normalize_data_structure(
        self, source: str, data: List[Dict]
    ) -> List[Dict]:
        """Normalize data structure from different sources with canonical product names"""
        normalized_data = []

        if not isinstance(data, list):
            logger.warning(f"Expected list for {source} data, got {type(data)}")
            return normalized_data

        for item in data:
            if not isinstance(item, dict):
                logger.warning(f"Skipping non-dict item in {source} data: {item}")
                continue

            try:
                # Get SKU first to lookup canonical product name
                sku_code = self.safe_get_value(item, "sku_code", "Unknown SKU")
                if not sku_code or sku_code == "":
                    sku_code = "Unknown SKU"

                # Get canonical product name from products collection
                canonical_item_name = await self.get_canonical_product_name(sku_code)

                if source == "blinkit":
                    metrics = item.get("metrics", {}) or {}
                    normalized_item = {
                        "source": "blinkit",
                        "item_name": canonical_item_name,  # Use canonical name
                        "item_id": str(self.safe_get_value(item, "item_id", "")),
                        "sku_code": sku_code,
                        "city": self.safe_get_value(item, "city", "Unknown City"),
                        "warehouse": self.safe_get_value(
                            item, "warehouse", "Unknown Warehouse"
                        ),
                        "units_sold": float(
                            metrics.get("total_sales_in_period", 0) or 0
                        ),
                        "total_amount": 0,  # Blinkit doesn't have amount data
                        "closing_stock": float(metrics.get("closing_stock", 0) or 0),
                        "days_in_stock": int(
                            metrics.get("days_with_inventory", 0) or 0
                        ),
                        "daily_run_rate": float(
                            metrics.get("avg_daily_on_stock_days", 0) or 0
                        ),
                        "days_of_coverage": float(
                            metrics.get("days_of_coverage", 0) or 0
                        ),
                        "sessions": 0,  # Not applicable for Blinkit
                        "additional_metrics": {
                            "avg_weekly_on_stock_days": float(
                                metrics.get("avg_weekly_on_stock_days", 0) or 0
                            ),
                            "performance_vs_prev_7_days_pct": float(
                                metrics.get("performance_vs_prev_7_days_pct", 0) or 0
                            ),
                            "performance_vs_prev_30_days_pct": float(
                                metrics.get("performance_vs_prev_30_days_pct", 0) or 0
                            ),
                            "best_performing_month": self.safe_get_value(
                                item, "best_performing_month", ""
                            ),
                        },
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
                        "item_name": canonical_item_name,  # Use canonical name
                        "item_id": self.safe_get_value(
                            item, "asin", ""
                        ),  # Amazon uses ASIN
                        "sku_code": sku_code,
                        "city": "Multiple",  # Amazon doesn't have city-specific data
                        "warehouse": warehouse_str,
                        "units_sold": float(self.safe_get_value(item, "units_sold", 0)),
                        "total_amount": float(
                            self.safe_get_value(item, "total_amount", 0)
                        ),
                        "closing_stock": float(
                            self.safe_get_value(item, "closing_stock", 0)
                        ),
                        "days_in_stock": int(
                            self.safe_get_value(item, "total_days_in_stock", 0)
                        ),
                        "daily_run_rate": float(self.safe_get_value(item, "drr", 0)),
                        "days_of_coverage": 0,  # Calculate if needed
                        "sessions": int(self.safe_get_value(item, "sessions", 0)),
                        "additional_metrics": {
                            "data_source_type": self.safe_get_value(
                                item, "data_source", ""
                            ),
                            "stock": float(self.safe_get_value(item, "stock", 0)),
                        },
                    }

                elif source == "zoho":
                    normalized_item = {
                        "source": "zoho",
                        "item_name": canonical_item_name,  # Use canonical name
                        "item_id": str(self.safe_get_value(item, "item_id", "")),
                        "sku_code": sku_code,
                        "city": "Multiple",  # Zoho doesn't have city-specific data
                        "warehouse": "Multiple",  # Zoho aggregates across warehouses
                        "units_sold": float(self.safe_get_value(item, "units_sold", 0)),
                        "total_amount": float(
                            self.safe_get_value(item, "total_amount", 0)
                        ),
                        "closing_stock": float(
                            self.safe_get_value(item, "closing_stock", 0)
                        ),
                        "days_in_stock": int(
                            self.safe_get_value(item, "total_days_in_stock", 0)
                        ),
                        "daily_run_rate": float(self.safe_get_value(item, "drr", 0)),
                        "days_of_coverage": 0,  # Calculate if needed
                        "sessions": 0,  # Not applicable for Zoho
                        "additional_metrics": {},
                    }
                else:
                    logger.warning(f"Unknown source: {source}")
                    continue

                # Calculate days of coverage if not available
                if (
                    normalized_item["daily_run_rate"] > 0
                    and normalized_item["days_of_coverage"] == 0
                ):
                    normalized_item["days_of_coverage"] = round(
                        normalized_item["closing_stock"]
                        / normalized_item["daily_run_rate"],
                        2,
                    )

                if normalized_item["units_sold"] > 0:
                    normalized_data.append(normalized_item)
                else:
                    logger.debug(f"Skipping {source} item with 0 units sold: SKU {normalized_item['sku_code']}")

            except Exception as e:
                logger.error(f"Error normalizing item from {source}: {e}")
                logger.error(f"Problematic item: {item}")
                continue

        return normalized_data

    def combine_data_by_sku(self, all_normalized_data: List[List[Dict]]) -> List[Dict]:
        """Combine data from all sources by SKU code"""
        sku_combined_data = {}

        # Flatten all data
        flattened_data = []
        for source_data in all_normalized_data:
            if isinstance(source_data, list):
                flattened_data.extend(source_data)

        # Group by SKU code
        for item in flattened_data:
            if not isinstance(item, dict):
                continue

            sku = self.safe_get_value(item, "sku_code", "Unknown SKU")
            if not sku or sku == "":
                sku = "Unknown SKU"

            if sku not in sku_combined_data:
                sku_combined_data[sku] = {
                    "sku_code": sku,
                    "item_name": self.safe_get_value(item, "item_name", "Unknown Item"),
                    "sources": [],
                    "combined_metrics": {
                        "total_units_sold": 0.0,
                        "total_amount": 0.0,
                        "total_closing_stock": 0.0,
                        "total_sessions": 0.0,
                        "avg_daily_run_rate": 0.0,
                        "avg_days_of_coverage": 0.0,
                        "total_days_in_stock": 0.0,
                    },
                    "source_breakdown": {
                        "blinkit": {
                            "units_sold": 0.0,
                            "closing_stock": 0.0,
                            "amount": 0.0,
                        },
                        "amazon": {
                            "units_sold": 0.0,
                            "closing_stock": 0.0,
                            "amount": 0.0,
                        },
                        "zoho": {
                            "units_sold": 0.0,
                            "closing_stock": 0.0,
                            "amount": 0.0,
                        },
                    },
                }

            # Add to sources if not already present
            source = self.safe_get_value(item, "source", "unknown")
            if source and source not in sku_combined_data[sku]["sources"]:
                sku_combined_data[sku]["sources"].append(source)

            # Update combined metrics
            try:
                sku_combined_data[sku]["combined_metrics"]["total_units_sold"] += float(
                    self.safe_get_value(item, "units_sold", 0)
                )
                sku_combined_data[sku]["combined_metrics"]["total_amount"] += float(
                    self.safe_get_value(item, "total_amount", 0)
                )
                sku_combined_data[sku]["combined_metrics"][
                    "total_closing_stock"
                ] += float(self.safe_get_value(item, "closing_stock", 0))
                sku_combined_data[sku]["combined_metrics"]["total_sessions"] += float(
                    self.safe_get_value(item, "sessions", 0)
                )
                sku_combined_data[sku]["combined_metrics"][
                    "total_days_in_stock"
                ] += float(self.safe_get_value(item, "days_in_stock", 0))

                # Update source breakdown
                if source in sku_combined_data[sku]["source_breakdown"]:
                    sku_combined_data[sku]["source_breakdown"][source][
                        "units_sold"
                    ] += float(self.safe_get_value(item, "units_sold", 0))
                    sku_combined_data[sku]["source_breakdown"][source][
                        "closing_stock"
                    ] += float(self.safe_get_value(item, "closing_stock", 0))
                    sku_combined_data[sku]["source_breakdown"][source][
                        "amount"
                    ] += float(self.safe_get_value(item, "total_amount", 0))
            except (ValueError, TypeError) as e:
                logger.error(f"Error updating metrics for SKU {sku}: {e}")
                continue

        # Calculate averages
        for sku_data in sku_combined_data.values():
            try:
                # Calculate weighted average for DRR
                total_units = sku_data["combined_metrics"]["total_units_sold"]
                total_days = sku_data["combined_metrics"]["total_days_in_stock"]

                if total_days > 0:
                    sku_data["combined_metrics"]["avg_daily_run_rate"] = round(
                        total_units / total_days, 2
                    )

                # Calculate average days of coverage
                if sku_data["combined_metrics"]["avg_daily_run_rate"] > 0:
                    sku_data["combined_metrics"]["avg_days_of_coverage"] = round(
                        sku_data["combined_metrics"]["total_closing_stock"]
                        / sku_data["combined_metrics"]["avg_daily_run_rate"],
                        2,
                    )
            except (ValueError, TypeError, ZeroDivisionError) as e:
                logger.error(
                    f"Error calculating averages for SKU {sku_data.get('sku_code', 'Unknown')}: {e}"
                )
                continue

        return list(sku_combined_data.values())

    async def normalize_data_async(self, source: str, data: List[Dict]) -> List[Dict]:
        """Async wrapper for data normalization to allow parallel processing"""
        return await self.normalize_data_structure(source, data)


@router.get("/master-report")
async def get_master_report(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    include_blinkit: bool = Query(True, description="Include Blinkit data"),
    include_amazon: bool = Query(True, description="Include Amazon data"),
    include_zoho: bool = Query(True, description="Include Zoho data"),
    amazon_report_type: str = Query(
        "all", description="Amazon report type (fba, seller_flex, vendor_central, all)"
    ),
    db=Depends(get_database),
):
    """
    Get master report combining data from Blinkit, Amazon, and Zoho.
    Returns both individual source data and combined metrics by SKU.
    """
    try:
        # Validate date format
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use YYYY-MM-DD",
            )

        logger.info(f"Generating master report for {start_date} to {end_date}")
        start_time = datetime.now()

        # Initialize service
        report_service = MasterReportService(db)

        # Create tasks for parallel execution
        tasks = []

        if include_blinkit:
            tasks.append(report_service.get_blinkit_report(start_date, end_date))

        if include_amazon:
            tasks.append(
                report_service.get_amazon_report(
                    start_date, end_date, amazon_report_type
                )
            )

        if include_zoho:
            tasks.append(report_service.get_zoho_report(start_date, end_date))

        if not tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one data source must be selected",
            )

        # Execute all tasks in parallel with timeout
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=120.0,  # 2 minute timeout
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail="Report generation timed out after 2 minutes",
            )

        # Process results and normalization in parallel
        individual_reports = {}
        normalization_tasks = []
        errors = []

        for result in results:
            if isinstance(result, Exception):
                error_msg = f"Task failed: {str(result)}"
                errors.append(error_msg)
                logger.error(error_msg)
                continue

            if not isinstance(result, dict):
                error_msg = f"Invalid result format: {type(result)}"
                errors.append(error_msg)
                logger.error(error_msg)
                continue

            source = result.get("source", "unknown")
            individual_reports[source] = result

            if result.get("success") and result.get("data"):
                # Create normalization task for parallel processing
                normalization_tasks.append(
                    asyncio.create_task(
                        report_service.normalize_data_async(source, result["data"])
                    )
                )
            else:
                if result.get("error"):
                    errors.append(f"{source}: {result['error']}")

        # Execute normalization in parallel
        all_normalized_data = []
        if normalization_tasks:
            try:
                normalization_results = await asyncio.gather(
                    *normalization_tasks, return_exceptions=True
                )

                for norm_result in normalization_results:
                    if isinstance(norm_result, Exception):
                        error_msg = f"Normalization failed: {str(norm_result)}"
                        errors.append(error_msg)
                        logger.error(error_msg)
                        continue

                    if norm_result and isinstance(norm_result, list):
                        all_normalized_data.append(norm_result)
                        logger.info(f"Normalized {len(norm_result)} items")

            except Exception as e:
                error_msg = f"Error in parallel normalization: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

        # Combine data by SKU with threading for CPU-intensive work
        combined_data = []
        if all_normalized_data:
            try:
                loop = asyncio.get_event_loop()
                combined_data = await loop.run_in_executor(
                    None, report_service.combine_data_by_sku, all_normalized_data
                )
                logger.info(f"Combined data for {len(combined_data)} unique SKUs")
            except Exception as e:
                error_msg = f"Error combining data: {str(e)}"
                errors.append(error_msg)
                logger.error(error_msg)

        # Calculate summary statistics with safe operations
        total_skus = len(combined_data)
        total_units_sold = 0.0
        total_amount = 0.0
        total_closing_stock = 0.0
        total_drr = 0.0

        for item in combined_data:
            if isinstance(item, dict) and "combined_metrics" in item:
                metrics = item["combined_metrics"]
                total_units_sold += float(metrics.get("total_units_sold", 0) or 0)
                total_amount += float(metrics.get("total_amount", 0) or 0)
                total_closing_stock += float(metrics.get("total_closing_stock", 0) or 0)
                total_drr += float(metrics.get("avg_daily_run_rate", 0) or 0)

        # Calculate average DRR
        avg_drr = total_drr / total_skus if total_skus > 0 else 0

        # Count by source
        source_counts = {}
        for source, reports in individual_reports.items():
            if reports.get("success") and reports.get("data"):
                source_counts[source] = len(reports["data"])

        execution_time = (datetime.now() - start_time).total_seconds()

        # Safe sorting with proper None handling
        try:
            sorted_combined_data = sorted(
                combined_data,
                key=lambda x: (
                    (x.get("sku_code") or "Unknown SKU")
                    if isinstance(x, dict)
                    else "Unknown SKU"
                ),
            )
        except Exception as e:
            logger.error(f"Error sorting combined data: {e}")
            sorted_combined_data = combined_data  # Use unsorted data

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Master report generated successfully for {start_date} to {end_date}",
                "date_range": {"start_date": start_date, "end_date": end_date},
                "summary": {
                    "total_unique_skus": total_skus,
                    "total_units_sold": round(total_units_sold, 2),
                    "total_amount": round(total_amount, 2),
                    "avg_drr": round(
                        avg_drr, 2
                    ),  # Changed from total_closing_stock to avg_drr
                    "sources_included": list(individual_reports.keys()),
                    "source_record_counts": source_counts,
                },
                "individual_reports": individual_reports,
                "combined_data": sorted_combined_data,
                "errors": errors,
                "meta": {
                    "execution_time_seconds": round(execution_time, 2),
                    "timestamp": datetime.now().isoformat(),
                    "query_type": "master_report",
                },
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in master report generation: {e}")
        logger.error(f"Master report error traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate master report: {str(e)}",
        )


@router.get("/master-report/download")
async def download_master_report(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    include_blinkit: bool = Query(True, description="Include Blinkit data"),
    include_amazon: bool = Query(True, description="Include Amazon data"),
    include_zoho: bool = Query(True, description="Include Zoho data"),
    amazon_report_type: str = Query("all", description="Amazon report type"),
    db=Depends(get_database),
):
    """
    Download master report as Excel file with multiple sheets.
    """
    try:
        # Get the master report data
        response = await get_master_report(
            start_date=start_date,
            end_date=end_date,
            include_blinkit=include_blinkit,
            include_amazon=include_amazon,
            include_zoho=include_zoho,
            amazon_report_type=amazon_report_type,
            db=db,
        )

        # Parse response content
        if hasattr(response, "body"):
            import json

            content = json.loads(response.body)
        else:
            content = response

        combined_data = content.get("combined_data", [])
        individual_reports = content.get("individual_reports", {})
        summary = content.get("summary", {})

        if not combined_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data found to download",
            )

        # Create Excel file with multiple sheets
        excel_buffer = io.BytesIO()

        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            # Sheet 1: Combined Summary
            combined_df_data = []
            for item in combined_data:
                if not isinstance(item, dict):
                    continue

                combined_df_data.append(
                    {
                        "SKU Code": item.get("sku_code", ""),
                        "Item Name": item.get("item_name", ""),
                        "Sources": ", ".join(item.get("sources", [])),
                        "Total Units Sold": item.get("combined_metrics", {}).get(
                            "total_units_sold", 0
                        ),
                        "Total Amount": item.get("combined_metrics", {}).get(
                            "total_amount", 0
                        ),
                        "Total Closing Stock": item.get("combined_metrics", {}).get(
                            "total_closing_stock", 0
                        ),
                        "Total Sessions": item.get("combined_metrics", {}).get(
                            "total_sessions", 0
                        ),
                        "Avg Daily Run Rate": item.get("combined_metrics", {}).get(
                            "avg_daily_run_rate", 0
                        ),
                        "Avg Days of Coverage": item.get("combined_metrics", {}).get(
                            "avg_days_of_coverage", 0
                        ),
                        "Blinkit Units": item.get("source_breakdown", {})
                        .get("blinkit", {})
                        .get("units_sold", 0),
                        "Amazon Units": item.get("source_breakdown", {})
                        .get("amazon", {})
                        .get("units_sold", 0),
                        "Zoho Units": item.get("source_breakdown", {})
                        .get("zoho", {})
                        .get("units_sold", 0),
                        "Blinkit Stock": item.get("source_breakdown", {})
                        .get("blinkit", {})
                        .get("closing_stock", 0),
                        "Amazon Stock": item.get("source_breakdown", {})
                        .get("amazon", {})
                        .get("closing_stock", 0),
                        "Zoho Stock": item.get("source_breakdown", {})
                        .get("zoho", {})
                        .get("closing_stock", 0),
                    }
                )

            if combined_df_data:
                combined_df = pd.DataFrame(combined_df_data)
                combined_df.to_excel(writer, sheet_name="Combined Summary", index=False)

            # Sheet 2: Summary Statistics
            summary_data = [
                ["Metric", "Value"],
                ["Report Period", f"{start_date} to {end_date}"],
                ["Total Unique SKUs", summary.get("total_unique_skus", 0)],
                ["Total Units Sold", summary.get("total_units_sold", 0)],
                ["Total Amount", summary.get("total_amount", 0)],
                ["Total Closing Stock", summary.get("total_closing_stock", 0)],
                ["Sources Included", ", ".join(summary.get("sources_included", []))],
                ["", ""],
                ["Source Record Counts", ""],
            ]

            for source, count in summary.get("source_record_counts", {}).items():
                summary_data.append([f"{source.title()} Records", count])

            summary_df = pd.DataFrame(summary_data[1:], columns=summary_data[0])
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # Individual source sheets
            for source, report_data in individual_reports.items():
                if report_data.get("success") and report_data.get("data"):
                    try:
                        source_df = pd.DataFrame(report_data["data"])
                        sheet_name = f"{source.title()} Data"
                        source_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    except Exception as e:
                        logger.error(f"Error creating sheet for {source}: {e}")

        excel_buffer.seek(0)

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"master_report_{start_date}_to_{end_date}_{timestamp}.xlsx"

        return StreamingResponse(
            excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except Exception as e:
        logger.error(f"Error generating master report Excel: {e}")
        logger.error(f"Excel generation error traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate Excel report: {str(e)}",
        )
