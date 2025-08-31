from fastapi import APIRouter, HTTPException, status, Depends, Query
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime
from typing import List, Dict, Any, Set
import asyncio
import pandas as pd
import io
from collections import defaultdict
from ..database import get_database
import logging
import traceback

logger = logging.getLogger(__name__)

router = APIRouter()


class OptimizedMasterReportService:
    """Optimized service to handle master reporting across Blinkit, Amazon, and Zoho"""

    def __init__(self, database):
        self.database = database
        self._product_name_cache = {}

    async def get_blinkit_report(
        self, start_date: str, end_date: str
    ) -> Dict[str, Any]:
        """Get Blinkit report data"""
        try:
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
            return {"source": "blinkit", "data": [], "success": False, "error": str(e)}

    async def get_amazon_report(
        self, start_date: str, end_date: str, report_type: str = "all"
    ) -> Dict[str, Any]:
        """Get Amazon report data"""
        try:
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
            return {"source": "amazon", "data": [], "success": False, "error": str(e)}

    async def get_zoho_report(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get Zoho report data"""
        try:
            from .zoho import get_sales_report_fast

            response = await get_sales_report_fast(
                start_date=start_date, end_date=end_date, db=self.database
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

            return {
                "source": "zoho",
                "data": report_data,
                "success": True,
                "error": None,
            }

        except Exception as e:
            logger.error(f"Error fetching Zoho report: {e}")
            return {"source": "zoho", "data": [], "success": False, "error": str(e)}

    async def batch_load_product_names(self, sku_codes: Set[str]) -> Dict[str, str]:
        """Batch load all product names in a single database query"""
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
                {"cf_sku_code": 1, "name": 1, "_id": 0}
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

    def extract_all_sku_codes(self, all_reports: List[Dict[str, Any]]) -> Set[str]:
        """Extract all unique SKU codes from all reports"""
        sku_codes = set()
        
        for report in all_reports:
            if not report.get("success") or not report.get("data"):
                continue
                
            data = report["data"]
            if not isinstance(data, list):
                continue
                
            for item in data:
                if isinstance(item, dict):
                    sku = item.get("sku_code")
                    if sku and sku.strip():
                        sku_codes.add(sku.strip())
        
        return sku_codes

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
        """Optimized normalization for a single data source with conditional SKU-level aggregation"""
        if not isinstance(data, list):
            return []

        # For Zoho, data is already aggregated by the API, so don't re-aggregate
        if source == "zoho":
            normalized_data = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                
                try:
                    sku_code = (item.get("sku_code") or "Unknown SKU").strip()
                    if not sku_code:
                        sku_code = "Unknown SKU"
                    
                    canonical_name = product_name_map.get(sku_code, "Unknown Item")
                    
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
                        "sessions": 0,
                        "days_of_coverage": 0.0,
                        "additional_metrics": {},
                    }
                    
                    # Calculate days of coverage
                    if normalized_item["daily_run_rate"] > 0:
                        normalized_item["days_of_coverage"] = round(
                            normalized_item["closing_stock"] / normalized_item["daily_run_rate"], 2
                        )
                    
                    # Only add items with sales or returns
                    if normalized_item["units_sold"] > 0 or normalized_item["units_returned"] > 0:
                        normalized_data.append(normalized_item)
                        
                except Exception as e:
                    logger.error(f"Error normalizing Zoho item: {e}")
                    continue
            
            return normalized_data

        # For Blinkit and Amazon, use SKU-level aggregation as before
        sku_aggregated = defaultdict(lambda: {
            "sku_code": "",
            "item_name": "",
            "item_id": "",
            "city": "",
            "warehouse": set(),
            "units_sold": 0.0,
            "units_returned": 0.0,
            "total_amount": 0.0,
            "closing_stock": 0.0,
            "sessions": 0,
            "days_in_stock": 0,
            "daily_run_rate": 0.0,
            "days_of_coverage": 0.0,
            "additional_metrics": {}
        })
        
        # Aggregate by SKU for Blinkit and Amazon
        for item in data:
            if not isinstance(item, dict):
                continue

            try:
                sku_code = (item.get("sku_code") or "Unknown SKU").strip()
                if not sku_code:
                    sku_code = "Unknown SKU"
                    
                agg = sku_aggregated[sku_code]
                
                # Set basic info (first occurrence)
                if not agg["sku_code"]:
                    agg["sku_code"] = sku_code
                    agg["item_name"] = product_name_map.get(sku_code, "Unknown Item")
                    if source == "amazon":
                        agg["item_id"] = item.get("asin", "")
                    else:
                        agg["item_id"] = str(item.get("item_id", ""))
                    agg["city"] = item.get("city", "Multiple" if source == "amazon" else "Unknown City")
                
                # Source-specific aggregation
                if source == "blinkit":
                    metrics = item.get("metrics", {}) or {}
                    agg["units_sold"] += self.safe_float(metrics.get("total_sales_in_period"))
                    agg["units_returned"] += self.safe_float(metrics.get("total_returns_in_period"))
                    agg["closing_stock"] += self.safe_float(metrics.get("closing_stock"))
                    agg["days_in_stock"] += self.safe_int(metrics.get("days_with_inventory"))
                    # Take last/max for rates
                    agg["daily_run_rate"] = max(agg["daily_run_rate"], self.safe_float(metrics.get("avg_daily_on_stock_days")))
                    warehouse = item.get("warehouse", "Unknown Warehouse")
                    if warehouse:
                        agg["warehouse"].add(warehouse)
                        
                elif source == "amazon":
                    agg["units_sold"] += self.safe_float(item.get("units_sold"))
                    agg["units_returned"] += self.safe_float(item.get("total_returns"))
                    agg["total_amount"] += self.safe_float(item.get("total_amount"))
                    agg["closing_stock"] += self.safe_float(item.get("closing_stock"))
                    agg["sessions"] += self.safe_int(item.get("sessions"))
                    agg["days_in_stock"] += self.safe_int(item.get("total_days_in_stock"))
                    agg["daily_run_rate"] = max(agg["daily_run_rate"], self.safe_float(item.get("drr")))
                    
                    warehouses = item.get("warehouses", []) or []
                    if isinstance(warehouses, list):
                        agg["warehouse"].update(warehouses)
                        
            except Exception as e:
                logger.error(f"Error aggregating item from {source}: {e}")
                continue
        
        # Convert aggregated data to normalized format for Blinkit and Amazon
        normalized_data = []
        for sku_code, agg in sku_aggregated.items():
            try:
                # Convert warehouse set to string
                warehouse_str = ", ".join(sorted(agg["warehouse"])) if agg["warehouse"] else "Unknown Warehouse"
                
                normalized_item = {
                    "source": source,
                    "item_name": agg["item_name"],
                    "item_id": agg["item_id"],
                    "sku_code": agg["sku_code"],
                    "city": agg["city"],
                    "warehouse": warehouse_str,
                    "units_sold": agg["units_sold"],
                    "units_returned": agg["units_returned"],
                    "total_amount": agg["total_amount"],
                    "closing_stock": agg["closing_stock"],
                    "days_in_stock": agg["days_in_stock"],
                    "daily_run_rate": agg["daily_run_rate"],
                    "sessions": agg["sessions"],
                    "days_of_coverage": 0.0,
                    "additional_metrics": agg["additional_metrics"],
                }
                
                # Calculate days of coverage
                if normalized_item["daily_run_rate"] > 0:
                    normalized_item["days_of_coverage"] = round(
                        normalized_item["closing_stock"] / normalized_item["daily_run_rate"], 2
                    )
                
                # Only add items with sales or returns
                if normalized_item["units_sold"] > 0 or normalized_item["units_returned"] > 0:
                    normalized_data.append(normalized_item)
                    
            except Exception as e:
                logger.error(f"Error creating normalized item for {sku_code} from {source}: {e}")
                continue

        return normalized_data
    def combine_data_by_sku_optimized(self, all_normalized_data: List[List[Dict]]) -> List[Dict]:
        """Optimized SKU combination using defaultdict and single pass"""
        sku_data = defaultdict(lambda: {
            "sku_code": "",
            "item_name": "Unknown Item",
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
            "source_breakdown": {
                "blinkit": {"units_sold": 0.0, "units_returned": 0.0, "closing_stock": 0.0, "amount": 0.0},
                "amazon": {"units_sold": 0.0, "units_returned": 0.0, "closing_stock": 0.0, "amount": 0.0},
                "zoho": {"units_sold": 0.0, "units_returned": 0.0, "closing_stock": 0.0, "amount": 0.0},
            },
        })

        # Single pass through all data
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

                # Add source
                sku_data[sku]["sources"].add(source)

                # Update metrics efficiently
                metrics = sku_data[sku]["combined_metrics"]
                metrics["total_units_sold"] += self.safe_float(item.get("units_sold"))
                metrics["total_units_returned"] += self.safe_float(item.get("units_returned"))
                metrics["total_amount"] += self.safe_float(item.get("total_amount"))
                metrics["total_closing_stock"] += self.safe_float(item.get("closing_stock"))
                metrics["total_sessions"] += self.safe_float(item.get("sessions"))
                metrics["total_days_in_stock"] += self.safe_float(item.get("days_in_stock"))

                # Update source breakdown
                if source in sku_data[sku]["source_breakdown"]:
                    breakdown = sku_data[sku]["source_breakdown"][source]
                    breakdown["units_sold"] += self.safe_float(item.get("units_sold"))
                    breakdown["units_returned"] += self.safe_float(item.get("units_returned"))
                    breakdown["closing_stock"] += self.safe_float(item.get("closing_stock"))
                    breakdown["amount"] += self.safe_float(item.get("total_amount"))

        # Calculate averages in a single pass
        result = []
        for sku, data in sku_data.items():
            # Convert sources set to list
            data["sources"] = list(data["sources"])
            
            # Calculate averages
            metrics = data["combined_metrics"]
            total_units = metrics["total_units_sold"] + metrics["total_units_returned"]
            total_days = metrics["total_days_in_stock"]

            if total_days > 0:
                metrics["avg_daily_run_rate"] = round(total_units / total_days, 2)

            if metrics["avg_daily_run_rate"] > 0:
                metrics["avg_days_of_coverage"] = round(
                    metrics["total_closing_stock"] / metrics["avg_daily_run_rate"], 2
                )

            # Round all metrics
            for key, value in metrics.items():
                if isinstance(value, float):
                    metrics[key] = round(value, 2)

            result.append(data)

        return sorted(result, key=lambda x: x["sku_code"])


@router.get("/master-report")
async def get_master_report(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    include_blinkit: bool = Query(True, description="Include Blinkit data"),
    include_amazon: bool = Query(True, description="Include Amazon data"),
    include_zoho: bool = Query(True, description="Include Zoho data"),
    amazon_report_type: str = Query("all", description="Amazon report type"),
    db=Depends(get_database),
):
    """
    Optimized master report with batch processing and parallel execution
    """
    try:
        # Validate dates
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use YYYY-MM-DD",
            )

        logger.info(f"Generating optimized master report for {start_date} to {end_date}")
        start_time = datetime.now()

        # Initialize service
        report_service = OptimizedMasterReportService(db)

        # Step 1: Fetch all reports in parallel
        tasks = []
        if include_blinkit:
            tasks.append(report_service.get_blinkit_report(start_date, end_date))
        if include_amazon:
            tasks.append(report_service.get_amazon_report(start_date, end_date, amazon_report_type))
        if include_zoho:
            tasks.append(report_service.get_zoho_report(start_date, end_date))

        if not tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one data source must be selected",
            )

        # Execute all report fetches in parallel
        try:
            results = await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=60.0  # Reduced timeout since we're optimizing
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail="Report generation timed out",
            )

        # Process results
        individual_reports = {}
        successful_reports = []
        errors = []

        for result in results:
            if isinstance(result, Exception):
                errors.append(f"Task failed: {str(result)}")
                continue

            if isinstance(result, dict):
                source = result.get("source", "unknown")
                individual_reports[source] = result
                
                if result.get("success") and result.get("data"):
                    successful_reports.append(result)
                elif result.get("error"):
                    errors.append(f"{source}: {result['error']}")

        if not successful_reports:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No successful reports generated",
            )

        # Step 2: Extract all SKU codes and batch load product names
        all_sku_codes = report_service.extract_all_sku_codes(successful_reports)
        logger.info(f"Found {len(all_sku_codes)} unique SKU codes")

        # Batch load product names
        product_name_map = await report_service.batch_load_product_names(all_sku_codes)

        # Step 3: Normalize all data in parallel
        normalization_tasks = []
        for report in successful_reports:
            source = report["source"]
            data = report["data"]
            
            # Create task for parallel normalization
            task = asyncio.create_task(
                asyncio.to_thread(
                    report_service.normalize_single_source_data,
                    source, data, product_name_map
                )
            )
            normalization_tasks.append(task)

        # Execute normalization in parallel
        try:
            normalized_results = await asyncio.gather(*normalization_tasks, return_exceptions=True)
        except Exception as e:
            errors.append(f"Normalization error: {str(e)}")
            normalized_results = []

        # Filter successful normalization results
        all_normalized_data = []
        for result in normalized_results:
            if isinstance(result, Exception):
                errors.append(f"Normalization failed: {str(result)}")
                continue
            if isinstance(result, list):
                all_normalized_data.append(result)
                logger.info(f"Normalized {len(result)} items")

        # Step 4: Combine data by SKU (CPU intensive, use thread)
        if all_normalized_data:
            try:
                combined_data = await asyncio.to_thread(
                    report_service.combine_data_by_sku_optimized,
                    all_normalized_data
                )
                logger.info(f"Combined data for {len(combined_data)} unique SKUs")
            except Exception as e:
                errors.append(f"Data combination error: {str(e)}")
                combined_data = []
        else:
            combined_data = []

        # Step 5: Calculate summary statistics
        total_skus = len(combined_data)
        summary_stats = {
            "total_units_sold": 0.0,
            "total_units_returned": 0.0,
            "total_amount": 0.0,
            "total_closing_stock": 0.0,
            "avg_drr": 0.0,
        }

        if combined_data:
            for item in combined_data:
                metrics = item.get("combined_metrics", {})
                summary_stats["total_units_sold"] += metrics.get("total_units_sold", 0)
                summary_stats["total_units_returned"] += metrics.get("total_units_returned", 0)
                summary_stats["total_amount"] += metrics.get("total_amount", 0)
                summary_stats["total_closing_stock"] += metrics.get("total_closing_stock", 0)
                summary_stats["avg_drr"] += metrics.get("avg_daily_run_rate", 0)

            if total_skus > 0:
                summary_stats["avg_drr"] = round(summary_stats["avg_drr"] / total_skus, 2)

        # Source counts
        source_counts = {
            source: len(report["data"]) 
            for source, report in individual_reports.items()
            if report.get("success") and report.get("data")
        }

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Master report completed in {execution_time:.2f} seconds")

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Optimized master report generated for {start_date} to {end_date}",
                "date_range": {"start_date": start_date, "end_date": end_date},
                "summary": {
                    "total_unique_skus": total_skus,
                    "total_units_sold": round(summary_stats["total_units_sold"], 2),
                    "total_units_returned": round(summary_stats["total_units_returned"], 2),
                    "total_amount": round(summary_stats["total_amount"], 2),
                    "total_closing_stock": round(summary_stats["total_closing_stock"], 2),
                    "avg_drr": summary_stats["avg_drr"],
                    "sources_included": list(individual_reports.keys()),
                    "source_record_counts": source_counts,
                },
                "individual_reports": individual_reports,
                "combined_data": combined_data,
                "errors": errors,
                "meta": {
                    "execution_time_seconds": round(execution_time, 2),
                    "timestamp": datetime.now().isoformat(),
                    "query_type": "optimized_master_report",
                    "optimization_applied": True,
                },
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in optimized master report: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate optimized master report: {str(e)}",
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
