from fastapi import APIRouter, HTTPException, status, Depends, Query
from fastapi.responses import JSONResponse, Response
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set
import asyncio
import pandas as pd
import io
import json
import math
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

    async def get_zoho_report(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get Zoho report data"""
        try:
            from .zoho import get_sales_report_fast

            response = await get_sales_report_fast(
                start_date=start_date, end_date=end_date, db=self.database,
                exclude_customers=False,
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

    TRANSFER_ORDER_CUSTOMERS = [
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (KA)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (MH)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (TL)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (TN)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (WB)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (HY)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (DL)",
        "Pupscribe Enterprises Private Limited (Blinkit Maharashtra)",
        "Pupscribe Enterprises Private Limited (Blinkit Karnataka)",
        "Pupscribe Enterprises Private Limited (Blinkit Telangana)",
        "Pupscribe Enterprises Private Limited (Blinkit Tamil Nadu)",
        "Pupscribe Enterprises Private Limited (Blinkit Haryana)",
        "Pupscribe Enterprises Private Limited (Blinkit West Bengal)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (UP)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (GJ)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (KL)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (AD)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (GA)",
        "(amzb2b) Pupscribe Enterprises Pvt Ltd (PB)",
    ]

    async def fetch_transfer_orders(self, start_date: str, end_date: str) -> Dict[str, float]:
        """Fetch transfer order quantities from invoices for specific customer names.
        Returns dict of {sku_code: total_quantity}."""
        try:
            invoices_collection = self.database["invoices"]
            composite_collection = self.database.get_collection("composite_products")
            products_collection = self.database.get_collection("products")

            def _fetch():
                # credit_notes stores date as ISODate, invoices as string
                from datetime import datetime
                cn_start = datetime.strptime(start_date, "%Y-%m-%d")
                cn_end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

                pipeline = [
                    {
                        "$match": {
                            "$or": [
                                {"date": {"$gte": start_date, "$lte": end_date}},
                                {"date": {"$gte": cn_start, "$lte": cn_end}},
                            ],
                            "status": {"$nin": ["void"]},
                            "customer_name": {"$in": self.TRANSFER_ORDER_CUSTOMERS},
                        }
                    },
                    {"$addFields": {"doc_type": "invoice"}},
                    {
                        "$unionWith": {
                            "coll": "credit_notes",
                            "pipeline": [
                                {
                                    "$match": {
                                        "$or": [
                                            {"date": {"$gte": start_date, "$lte": end_date}},
                                            {"date": {"$gte": cn_start, "$lte": cn_end}},
                                        ],
                                        "customer_name": {"$in": self.TRANSFER_ORDER_CUSTOMERS},
                                    }
                                },
                                {"$addFields": {"doc_type": "credit_note"}},
                            ],
                        }
                    },
                    {"$unwind": "$line_items"},
                    {
                        "$group": {
                            "_id": "$line_items.item_id",
                            "total_quantity": {
                                "$sum": {
                                    "$cond": [
                                        {"$eq": ["$doc_type", "credit_note"]},
                                        {"$multiply": ["$line_items.quantity", -1]},
                                        "$line_items.quantity",
                                    ]
                                }
                            },
                        }
                    },
                ]
                # Keep original item_id types (could be int or str in MongoDB)
                item_quantities = {}
                for doc in invoices_collection.aggregate(pipeline):
                    item_id = doc["_id"]
                    qty = doc.get("total_quantity", 0) or 0
                    if item_id and qty != 0:
                        item_quantities[item_id] = float(qty)

                if not item_quantities:
                    return {}

                # Load composite products map (keyed by composite_item_id)
                composites = {}
                for doc in composite_collection.find({}, {"composite_item_id": 1, "components": 1, "_id": 0}):
                    cid = doc.get("composite_item_id")
                    if cid and doc.get("components"):
                        composites[cid] = doc["components"]
                        composites[str(cid)] = doc["components"]

                # Map item_id → sku_code using products collection
                # Query with both original types and string versions to handle type mismatches
                raw_ids = list(item_quantities.keys())
                str_ids = [str(i) for i in raw_ids]
                all_query_ids = list(set(raw_ids + str_ids))
                item_id_to_sku = {}
                for doc in products_collection.find(
                    {"item_id": {"$in": all_query_ids}},
                    {"item_id": 1, "cf_sku_code": 1, "_id": 0},
                ):
                    iid = doc.get("item_id")
                    sku = doc.get("cf_sku_code", "")
                    if iid and sku:
                        # Store both original and string key so lookup always works
                        item_id_to_sku[iid] = sku
                        item_id_to_sku[str(iid)] = sku

                logger.info(f"Transfer orders: {len(item_quantities)} item_ids, mapped {len(item_id_to_sku)//2} to SKUs")

                # Build sku → quantity, expanding composites
                sku_quantities: Dict[str, float] = {}
                for item_id, qty in item_quantities.items():
                    # Check if composite FIRST (composite items won't be in products collection)
                    components = composites.get(item_id) or composites.get(str(item_id))
                    if components:
                        for comp in components:
                            comp_sku = comp.get("sku_code", "")
                            comp_qty = float(comp.get("quantity", 1))
                            if comp_sku:
                                sku_quantities[comp_sku] = sku_quantities.get(comp_sku, 0) + (qty * comp_qty)
                        continue

                    # Regular product — need SKU mapping
                    sku = item_id_to_sku.get(item_id) or item_id_to_sku.get(str(item_id), "")
                    if not sku:
                        continue
                    sku_quantities[sku] = sku_quantities.get(sku, 0) + qty

                return sku_quantities

            result = await asyncio.to_thread(_fetch)
            logger.info(f"Fetched transfer orders for {len(result)} SKUs")
            return result

        except Exception as e:
            logger.error(f"Error fetching transfer orders: {e}")
            return {}

    async def fetch_fba_closing_stock(self, end_date: str) -> Dict[str, float]:
        """Fetch FBA closing stock from amazon_ledger, mapped by SKU code.
        Returns dict of {sku_code: closing_stock}."""
        try:
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
            ledger_collection = self.database["amazon_ledger"]
            sku_mapping_collection = self.database["amazon_sku_mapping"]

            def _fetch():
                # Get latest ending_warehouse_balance per ASIN from ledger
                pipeline = [
                    {
                        "$match": {
                            "date": {"$lte": end_datetime},
                            "disposition": "SELLABLE",
                        }
                    },
                    {"$sort": {"asin": 1, "date": -1}},
                    {
                        "$group": {
                            "_id": "$asin",
                            "closing_stock": {"$first": "$ending_warehouse_balance"},
                        }
                    },
                ]
                asin_stock = {}
                for doc in ledger_collection.aggregate(pipeline):
                    asin = doc["_id"]
                    stock = doc.get("closing_stock", 0) or 0
                    if stock > 0:
                        asin_stock[asin] = float(stock)

                if not asin_stock:
                    return {}

                # Map ASINs to SKU codes
                sku_docs = list(sku_mapping_collection.find(
                    {"item_id": {"$in": list(asin_stock.keys())}},
                    {"item_id": 1, "sku_code": 1, "_id": 0},
                ))

                sku_stock = {}
                for doc in sku_docs:
                    asin = doc.get("item_id")
                    sku = doc.get("sku_code")
                    if asin and sku and asin in asin_stock:
                        sku_stock[sku] = sku_stock.get(sku, 0) + asin_stock[asin]

                return sku_stock

            result = await asyncio.to_thread(_fetch)
            logger.info(f"Fetched FBA closing stock for {len(result)} SKUs")
            return result

        except Exception as e:
            logger.error(f"Error fetching FBA closing stock: {e}")
            return {}

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

            # Single query to get all products - run in thread to avoid blocking event loop
            def _fetch_products():
                return list(products_collection.find(
                    {"cf_sku_code": {"$in": list(valid_skus)}},
                    {"cf_sku_code": 1, "name": 1, "_id": 0},
                ))

            products = await asyncio.to_thread(_fetch_products)

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

    async def batch_load_all_product_data(self, sku_codes: Set[str]) -> Dict[str, Dict]:
        """Batch load all product data (name, rate, brand, cbm, case_pack) in a single query.
        Returns a dict keyed by sku_code with all fields."""
        if not sku_codes:
            return {}

        try:
            valid_skus = {sku for sku in sku_codes if sku and sku != "Unknown SKU"}
            if not valid_skus:
                return {}

            products_collection = self.database.get_collection("products")

            def _fetch_all():
                return list(products_collection.find(
                    {"cf_sku_code": {"$in": list(valid_skus)}},
                    {"cf_sku_code": 1, "name": 1, "rate": 1, "brand": 1, "cbm": 1, "case_pack": 1, "_id": 0},
                ))

            products = await asyncio.to_thread(_fetch_all)

            result = {}
            for product in products:
                sku_code = product.get("cf_sku_code")
                if not sku_code:
                    continue
                result[sku_code] = {
                    "name": product.get("name"),
                    "rate": product.get("rate"),
                    "brand": product.get("brand", ""),
                    "cbm": self.safe_float(product.get("cbm")),
                    "case_pack": self.safe_float(product.get("case_pack")),
                }

            logger.info(f"Batch loaded all product data for {len(result)} products")
            return result

        except Exception as e:
            logger.error(f"Error batch loading all product data: {e}")
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
                        "credit_notes": self.safe_float(item.get("credit_notes")),
                        "total_amount": self.safe_float(item.get("total_amount")),
                        "closing_stock": self.safe_float(item.get("closing_stock")),
                        "days_in_stock": self.safe_int(item.get("total_days_in_stock")),
                        "daily_run_rate": self.safe_float(item.get("drr")),
                        "sessions": 0,
                        "days_of_coverage": 0.0,
                        "additional_metrics": {},
                        "last_90_days_dates": item.get("last_90_days_dates", ""),
                    }

                    # Calculate days of coverage
                    if normalized_item["daily_run_rate"] > 0:
                        normalized_item["days_of_coverage"] = round(
                            normalized_item["closing_stock"]
                            / normalized_item["daily_run_rate"],
                            2,
                        )

                    # Add items with sales, returns, or closing stock
                    if (
                        normalized_item["units_sold"] > 0
                        or normalized_item["units_returned"] > 0
                        or normalized_item["closing_stock"] > 0
                    ):
                        normalized_data.append(normalized_item)

                except Exception as e:
                    logger.error(f"Error normalizing Zoho item: {e}")
                    continue

            return normalized_data

        # For Blinkit and Amazon, use SKU-level aggregation as before
        sku_aggregated = defaultdict(
            lambda: {
                "sku_code": "",
                "item_name": "",
                "item_id": "",
                "city": "",
                "warehouse": set(),
                "units_sold": 0.0,
                "units_returned": 0.0,
                "credit_notes": 0.0,
                "total_amount": 0.0,
                "closing_stock": 0.0,
                "sessions": 0,
                "days_in_stock": 0,
                "daily_run_rate": 0.0,
                "days_of_coverage": 0.0,
                "additional_metrics": {},
                "last_90_days_dates": "",
                "amazon_data_source": "",  # Track specific Amazon platform (vendor_central, fba, etc.)
            }
        )

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
                    agg["city"] = item.get(
                        "city", "Multiple" if source == "amazon" else "Unknown City"
                    )

                # Capture last_90_days_dates if present (check on every item, not just first)
                # Always capture if the key exists, even if empty string
                if "last_90_days_dates" in item:
                    # Only update if we have data or if current is empty
                    if item.get("last_90_days_dates") or not agg["last_90_days_dates"]:
                        agg["last_90_days_dates"] = item.get("last_90_days_dates", "")

                # Source-specific aggregation
                if source == "blinkit":
                    metrics = item.get("metrics", {}) or {}
                    agg["units_sold"] += self.safe_float(
                        metrics.get("total_sales_in_period")
                    )
                    agg["units_returned"] += self.safe_float(
                        metrics.get("total_returns_in_period")
                    )
                    agg["closing_stock"] += self.safe_float(
                        metrics.get("closing_stock")
                    )
                    agg["days_in_stock"] += self.safe_int(
                        metrics.get("days_with_inventory")
                    )
                    # Take last/max for rates
                    agg["daily_run_rate"] = max(
                        agg["daily_run_rate"],
                        self.safe_float(metrics.get("avg_daily_on_stock_days")),
                    )
                    warehouse = item.get("warehouse", "Unknown Warehouse")
                    if warehouse:
                        agg["warehouse"].add(warehouse)

                elif source == "amazon":
                    agg["units_sold"] += self.safe_float(item.get("units_sold"))
                    agg["units_returned"] += self.safe_float(item.get("total_returns"))
                    agg["total_amount"] += self.safe_float(item.get("total_amount"))
                    agg["closing_stock"] += self.safe_float(item.get("closing_stock"))
                    agg["sessions"] += self.safe_int(item.get("sessions"))
                    agg["days_in_stock"] += self.safe_int(
                        item.get("total_days_in_stock")
                    )
                    agg["daily_run_rate"] = max(
                        agg["daily_run_rate"], self.safe_float(item.get("drr"))
                    )

                    warehouses = item.get("warehouses", []) or []
                    if isinstance(warehouses, list):
                        agg["warehouse"].update(warehouses)

                    # Track specific Amazon data source (Vendor Central vs FBA)
                    data_source = item.get("data_source", "")
                    if data_source:
                        agg["amazon_data_source"] = data_source

            except Exception as e:
                logger.error(f"Error aggregating item from {source}: {e}")
                continue

        # Convert aggregated data to normalized format for Blinkit and Amazon
        normalized_data = []
        for sku_code, agg in sku_aggregated.items():
            try:
                # Convert warehouse set to string
                warehouse_str = (
                    ", ".join(sorted(agg["warehouse"]))
                    if agg["warehouse"]
                    else "Unknown Warehouse"
                )

                # Determine specific source label for Amazon
                item_source = source
                if source == "amazon" and agg.get("amazon_data_source"):
                    amazon_ds = agg["amazon_data_source"]
                    if amazon_ds in ("vendor_central", "vendor_only"):
                        item_source = "amazon_vendor_central"
                    elif amazon_ds in ("fba", "fba_only", "fba+seller_flex"):
                        item_source = "amazon_fba"
                    elif amazon_ds == "seller_flex":
                        item_source = "amazon_seller_flex"
                    elif amazon_ds == "combined":
                        item_source = "amazon_vc_fba"  # Item has data from both VC and FBA

                normalized_item = {
                    "source": item_source,
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
                    "last_90_days_dates": agg.get("last_90_days_dates", ""),
                }

                # Calculate days of coverage
                if normalized_item["daily_run_rate"] > 0:
                    normalized_item["days_of_coverage"] = round(
                        normalized_item["closing_stock"]
                        / normalized_item["daily_run_rate"],
                        2,
                    )

                # Add items with sales, returns, or closing stock
                if (
                    normalized_item["units_sold"] > 0
                    or normalized_item["units_returned"] > 0
                    or normalized_item["closing_stock"] > 0
                ):
                    normalized_data.append(normalized_item)

            except Exception as e:
                logger.error(
                    f"Error creating normalized item for {sku_code} from {source}: {e}"
                )
                continue

        return normalized_data

    def combine_data_by_sku_optimized(
        self, all_normalized_data: List[List[Dict]], period_days: int = 30,
        composite_products_map: Dict = None, fba_stock_by_sku: Dict = None,
        transfer_orders_by_sku: Dict = None
    ) -> List[Dict]:
        """Optimized SKU combination using defaultdict and single pass, with composite product handling"""
        sku_data = defaultdict(
            lambda: {
                "sku_code": "",
                "item_name": "Unknown Item",
                "sources": set(),
                "combined_metrics": {
                    "total_units_sold": 0.0,
                    "total_units_returned": 0.0,
                    "total_credit_notes": 0.0,
                    "total_amount": 0.0,
                    "total_closing_stock": 0.0,
                    "total_days_in_stock": 0.0,
                    "avg_daily_run_rate": 0.0,
                    "avg_days_of_coverage": 0.0,
                },
                # Internal only: track Zoho units/stock for DRR calc
                "_zoho_units_sold": 0.0,
                "_zoho_closing_stock": 0.0,
                "in_stock": False,
            }
        )

        if composite_products_map is None:
            composite_products_map = {}
        if fba_stock_by_sku is None:
            fba_stock_by_sku = {}
        if transfer_orders_by_sku is None:
            transfer_orders_by_sku = {}

        def process_item_data(self, item, sku_code, item_name, multiplier=1.0):
            """Process individual item data with optional quantity multiplier"""
            source = item.get("source", "unknown")
            entry = sku_data[sku_code]

            # Initialize if first time seeing this SKU
            if entry["sku_code"] == "":
                entry["sku_code"] = sku_code
                entry["item_name"] = item_name

            entry["sources"].add(source)

            # Update metrics with multiplier
            metrics = entry["combined_metrics"]
            metrics["total_units_sold"] += self.safe_float(item.get("units_sold")) * multiplier
            metrics["total_units_returned"] += self.safe_float(item.get("units_returned")) * multiplier
            metrics["total_credit_notes"] += self.safe_float(item.get("credit_notes")) * multiplier
            metrics["total_amount"] += self.safe_float(item.get("total_amount")) * multiplier
            metrics["total_closing_stock"] += self.safe_float(item.get("closing_stock")) * multiplier
            metrics["total_days_in_stock"] += self.safe_float(item.get("days_in_stock"))

            # Track Zoho units/stock for DRR calculation
            if source == "zoho":
                entry["_zoho_units_sold"] += self.safe_float(item.get("units_sold")) * multiplier
                entry["_zoho_closing_stock"] += self.safe_float(item.get("closing_stock")) * multiplier

        # Single pass through all data
        for source_data in all_normalized_data:
            if not isinstance(source_data, list):
                continue

            for item in source_data:
                if not isinstance(item, dict):
                    continue

                original_sku = item.get("sku_code", "Unknown SKU") or "Unknown SKU"
                item_id = str(item.get("item_id", ""))

                # Check if this is a composite product
                composite_components = composite_products_map.get(item_id)

                if composite_components:
                    for component in composite_components:
                        process_item_data(
                            self, item,
                            component.get("sku_code", "Unknown Component SKU"),
                            component.get("name", "Unknown Component"),
                            float(component.get("quantity", 1)),
                        )
                else:
                    process_item_data(self, item, original_sku, item.get("item_name", "Unknown Item"))

        # Calculate averages in a single pass
        result = []
        for sku, data in sku_data.items():
            data["sources"] = list(data["sources"])
            metrics = data["combined_metrics"]

            # DRR based on Zoho sales / days in stock
            zoho_units = data.pop("_zoho_units_sold")
            zoho_stock = data.pop("_zoho_closing_stock")
            fba_stock = fba_stock_by_sku.get(sku, 0)

            days_in_stock = metrics.get("total_days_in_stock", 0)
            if days_in_stock > 0:
                metrics["avg_daily_run_rate"] = round(zoho_units / days_in_stock, 2)
            elif period_days > 0:
                metrics["avg_daily_run_rate"] = round(zoho_units / period_days, 2)

            # Closing stock = Pupscribe WH (Zoho) + FBA stock
            metrics["total_closing_stock"] = round(zoho_stock + fba_stock, 2)
            metrics["fba_closing_stock"] = round(fba_stock, 2)
            metrics["pupscribe_wh_stock"] = round(zoho_stock, 2)

            # Transfer orders and total sales
            transfer_qty = transfer_orders_by_sku.get(sku, 0)
            metrics["transfer_orders"] = round(transfer_qty, 2)
            credit_notes = metrics.get("total_credit_notes", 0)
            metrics["total_sales"] = round(metrics["total_units_sold"] - credit_notes, 2)

            if metrics["avg_daily_run_rate"] > 0:
                metrics["avg_days_of_coverage"] = round(
                    metrics["total_closing_stock"] / metrics["avg_daily_run_rate"], 2
                )

            data["in_stock"] = zoho_stock > 0 or fba_stock > 0

            # Round all float metrics
            for key, value in metrics.items():
                if isinstance(value, float):
                    metrics[key] = round(value, 2)

            result.append(data)

        return sorted(result, key=lambda x: x["sku_code"])

    async def get_brand_logistics(self) -> Dict[str, Dict]:
        """Load brand logistics settings (lead_time, safety_days per class) from brand_logistics collection"""
        try:
            collection = self.database.get_collection("brand_logistics")

            def _fetch():
                return list(collection.find({}, {"_id": 0}))

            docs = await asyncio.to_thread(_fetch)

            result = {}
            for doc in docs:
                brand = doc.get("brand", "")
                if brand:
                    result[brand.lower()] = {
                        "lead_time": self.safe_float(doc.get("lead_time", 60)),
                        "safety_days_fast": self.safe_float(doc.get("safety_days_fast", 40)),
                        "safety_days_medium": self.safe_float(doc.get("safety_days_medium", 25)),
                        "safety_days_slow": self.safe_float(doc.get("safety_days_slow", 15)),
                    }

            return result

        except Exception as e:
            logger.error(f"Error fetching brand logistics: {e}")
            return {}

    async def get_product_brands(self, sku_codes: Set[str]) -> Dict[str, str]:
        """Batch load brand info for products"""
        if not sku_codes:
            return {}

        try:
            products_collection = self.database.get_collection("products")

            def _fetch():
                return list(products_collection.find(
                    {"cf_sku_code": {"$in": list(sku_codes)}},
                    {"cf_sku_code": 1, "brand": 1, "_id": 0}
                ))

            products = await asyncio.to_thread(_fetch)
            return {p.get("cf_sku_code"): p.get("brand", "") for p in products if p.get("cf_sku_code")}

        except Exception as e:
            logger.error(f"Error fetching product brands: {e}")
            return {}

    async def fetch_sku_to_item_id_map(self, sku_codes: Set[str]) -> Dict[str, str]:
        """Build reverse mapping: sku_code → zoho item_id from products collection"""
        if not sku_codes:
            return {}

        try:
            products_collection = self.database.get_collection("products")

            def _fetch():
                docs = products_collection.find(
                    {"cf_sku_code": {"$in": list(sku_codes)}},
                    {"item_id": 1, "cf_sku_code": 1, "_id": 0}
                )
                result = {}
                for doc in docs:
                    sku = doc.get("cf_sku_code", "")
                    iid = doc.get("item_id")
                    if sku and iid:
                        result[sku] = iid
                return result

            return await asyncio.to_thread(_fetch)
        except Exception as e:
            logger.error(f"Error fetching sku to item_id map: {e}")
            return {}

    async def fetch_previous_period_drr(
        self,
        skus_needing_lookback: Dict[str, str],
        start_date: str,
        period_days: int,
    ) -> Dict[str, Dict]:
        """
        Look back at previous periods to find representative DRR for items with < 60 days in stock.
        Uses TWO batch aggregations (stock + sales) covering all 6 periods at once,
        instead of 12 separate queries.
        """
        from .zoho import fetch_stock_data_for_items_batch, fetch_zoho_lookback_sales_batch

        if not skus_needing_lookback:
            return {}

        all_item_ids = list(set(skus_needing_lookback.values()))
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")

        # Pre-compute all 6 lookback period date ranges
        max_lookbacks = 6
        periods = []
        cur_start = start_dt
        for _ in range(max_lookbacks):
            p_end = cur_start - timedelta(days=1)
            p_start = p_end - timedelta(days=period_days - 1)
            periods.append((p_start, p_end))
            cur_start = p_start

        logger.info(
            f"DRR lookback: 2 batch queries covering {max_lookbacks} periods for "
            f"{len(skus_needing_lookback)} SKUs"
        )

        # TWO queries total: one for stock, one for sales — both cover all 6 periods
        stock_by_period, sales_by_period = await asyncio.gather(
            fetch_stock_data_for_items_batch(self.database, periods, all_item_ids),
            fetch_zoho_lookback_sales_batch(self.database, periods, all_item_ids),
        )

        # Process results in priority order (period 0 = most recent lookback first)
        results = {}
        resolved_skus = set()

        for i in range(max_lookbacks):
            stock_data = stock_by_period.get(i, {})
            sales_data = sales_by_period.get(i, {})
            p_start, p_end = periods[i]
            p_start_str = p_start.strftime("%Y-%m-%d")
            p_end_str = p_end.strftime("%Y-%m-%d")

            for sku, item_id in skus_needing_lookback.items():
                if sku in resolved_skus:
                    continue

                stock_info = stock_data.get(item_id, {})
                days_in_stock = stock_info.get("total_days_in_stock", 0)

                if days_in_stock >= 60:
                    units_sold = sales_data.get(str(item_id), 0)
                    drr = round(units_sold / days_in_stock, 2) if days_in_stock > 0 else 0
                    results[sku] = {
                        "drr": drr,
                        "lookback_period": f"{p_start_str} to {p_end_str}",
                        "days_in_stock": days_in_stock,
                        "found": True,
                    }
                    resolved_skus.add(sku)

            logger.info(
                f"DRR lookback period {i+1} ({p_start_str} to {p_end_str}): "
                f"{len(resolved_skus)} total resolved"
            )

        # Mark remaining SKUs as not found
        for sku in skus_needing_lookback:
            if sku not in resolved_skus:
                results[sku] = {
                    "drr": 0,
                    "lookback_period": "",
                    "days_in_stock": 0,
                    "found": False,
                }

        return results

    @staticmethod
    def classify_movement(
        combined_data: List[Dict],
        brand_logistics: Dict[str, Dict],
        product_brands: Dict[str, str],
    ) -> List[Dict]:
        """Classify each SKU as Fast/Medium/Slow mover based on volume and revenue percentiles.
        Uses brand-specific safety_days and lead_time from brand_logistics collection."""
        if not combined_data:
            return combined_data

        # Default settings when no brand config exists
        default_settings = {
            "lead_time": 60,
            "safety_days_fast": 40,
            "safety_days_medium": 25,
            "safety_days_slow": 15,
        }

        # Extract units sold and amount for ranking
        items_with_metrics = []
        for item in combined_data:
            metrics = item.get("combined_metrics", {})
            items_with_metrics.append({
                "units_sold": metrics.get("total_units_sold", 0),
                "amount": metrics.get("total_amount", 0),
            })

        total_count = len(items_with_metrics)
        if total_count == 0:
            return combined_data

        # Rank by volume (descending - highest sales gets rank 1)
        volume_sorted = sorted(range(total_count), key=lambda i: items_with_metrics[i]["units_sold"], reverse=True)
        volume_ranks = [0] * total_count
        for rank, idx in enumerate(volume_sorted):
            volume_ranks[idx] = (rank + 1) / total_count

        # Rank by revenue (descending - highest revenue gets rank 1)
        revenue_sorted = sorted(range(total_count), key=lambda i: items_with_metrics[i]["amount"], reverse=True)
        revenue_ranks = [0] * total_count
        for rank, idx in enumerate(revenue_sorted):
            revenue_ranks[idx] = (rank + 1) / total_count

        # Classify each item
        for i, item in enumerate(combined_data):
            vol_pct = volume_ranks[i]
            rev_pct = revenue_ranks[i]

            # Get brand-specific settings
            sku = item.get("sku_code", "")
            brand = product_brands.get(sku, "")
            brand_settings = brand_logistics.get(brand.lower(), default_settings) if brand else default_settings

            if vol_pct <= 0.2 or rev_pct <= 0.2:
                mover_class = 1
                movement = "Fast Mover"
                safety_days = brand_settings.get("safety_days_fast", 40)
            elif vol_pct <= 0.5 or rev_pct <= 0.5:
                mover_class = 2
                movement = "Medium Mover"
                safety_days = brand_settings.get("safety_days_medium", 25)
            else:
                mover_class = 3
                movement = "Slow Mover"
                safety_days = brand_settings.get("safety_days_slow", 15)

            item["movement"] = movement
            item["mover_class"] = mover_class
            item["safety_days"] = safety_days
            item["lead_time"] = brand_settings.get("lead_time", 60)

        return combined_data

    async def get_stock_in_transit(self) -> Dict[str, Dict]:
        """Get stock in transit from open purchase orders, grouped by SKU"""
        try:
            po_collection = self.database.get_collection("purchase_orders")

            def _fetch_open_pos():
                return list(po_collection.find(
                    {"status": "issued"},
                    {"line_items": 1, "purchaseorder_number": 1, "_id": 0}
                ))

            open_pos = await asyncio.to_thread(_fetch_open_pos)

            # Group transit quantities by SKU
            sku_transit: Dict[str, List[float]] = defaultdict(list)

            for po in open_pos:
                line_items = po.get("line_items", [])
                for li in line_items:
                    # Extract SKU from custom fields
                    sku_code = ""
                    for cf in li.get("item_custom_fields", []):
                        if cf.get("api_name") == "cf_sku_code":
                            sku_code = cf.get("value", "")
                            break

                    if not sku_code:
                        continue

                    qty = self.safe_float(li.get("quantity", 0))
                    qty_received = self.safe_float(li.get("quantity_received", 0))
                    transit_qty = qty - qty_received

                    if transit_qty > 0:
                        sku_transit[sku_code].append(transit_qty)

            # Build result with up to 3 transit entries per SKU
            result = {}
            for sku, quantities in sku_transit.items():
                transit_1 = quantities[0] if len(quantities) > 0 else 0
                transit_2 = quantities[1] if len(quantities) > 1 else 0
                transit_3 = quantities[2] if len(quantities) > 2 else 0
                result[sku] = {
                    "transit_1": transit_1,
                    "transit_2": transit_2,
                    "transit_3": transit_3,
                    "total": sum(quantities),
                }

            logger.info(f"Found stock in transit for {len(result)} SKUs from {len(open_pos)} open POs")
            return result

        except Exception as e:
            logger.error(f"Error fetching stock in transit: {e}")
            return {}

    async def get_product_logistics(self, sku_codes: Set[str]) -> Dict[str, Dict]:
        """Batch load CBM and case_pack from products collection"""
        if not sku_codes:
            return {}

        try:
            products_collection = self.database.get_collection("products")

            def _fetch_logistics():
                return list(products_collection.find(
                    {"cf_sku_code": {"$in": list(sku_codes)}},
                    {"cf_sku_code": 1, "cbm": 1, "case_pack": 1, "_id": 0}
                ))

            products = await asyncio.to_thread(_fetch_logistics)

            result = {}
            for p in products:
                sku = p.get("cf_sku_code")
                if sku:
                    result[sku] = {
                        "cbm": self.safe_float(p.get("cbm")),
                        "case_pack": self.safe_float(p.get("case_pack")),
                    }

            return result

        except Exception as e:
            logger.error(f"Error fetching product logistics: {e}")
            return {}

    @staticmethod
    def enrich_with_order_calculations(
        combined_data: List[Dict],
        transit_data: Dict[str, Dict],
        logistics_data: Dict[str, Dict],
    ) -> List[Dict]:
        """Add order calculation columns to each item in combined_data"""
        for item in combined_data:
            sku = item.get("sku_code", "")
            metrics = item.get("combined_metrics", {})
            drr = metrics.get("avg_daily_run_rate", 0)
            closing_stock = metrics.get("total_closing_stock", 0)

            # Transit data
            transit = transit_data.get(sku, {})
            item["stock_in_transit_1"] = transit.get("transit_1", 0)
            item["stock_in_transit_2"] = transit.get("transit_2", 0)
            item["stock_in_transit_3"] = transit.get("transit_3", 0)
            total_transit = transit.get("total", 0)
            item["total_stock_in_transit"] = total_transit

            # Days coverage
            on_hand_days = metrics.get("avg_days_of_coverage", 0)
            item["on_hand_days_coverage"] = round(on_hand_days, 2)

            current_days_coverage = 0
            if drr > 0:
                current_days_coverage = round((closing_stock + total_transit) / drr, 2)
            item["current_days_coverage"] = current_days_coverage

            # Target days = lead_time + safety_days + review_days(10)
            safety_days = item.get("safety_days", 15)
            lead_time = item.get("lead_time", 60)
            target_days = lead_time + safety_days + 10
            item["target_days"] = target_days

            # Excess or Order
            if drr == 0:
                item["excess_or_order"] = "NO MOVEMENT"
            elif current_days_coverage < target_days:
                item["excess_or_order"] = "ORDER"
            else:
                item["excess_or_order"] = "EXCESS"

            # Order quantity
            order_qty = max(0, (target_days - current_days_coverage) * drr)
            item["order_qty"] = round(order_qty, 2)

            # Logistics (CBM / Case Pack)
            logistics = logistics_data.get(sku, {})
            cbm = logistics.get("cbm", 0)
            case_pack = logistics.get("case_pack", 0)
            item["cbm"] = cbm
            item["case_pack"] = case_pack

            # Order qty rounded up to case pack
            if case_pack > 0:
                item["order_qty_rounded"] = math.ceil(order_qty / case_pack) * case_pack
            else:
                item["order_qty_rounded"] = round(order_qty, 0)

            order_qty_rounded = item["order_qty_rounded"]

            # Total CBM
            if case_pack > 0 and cbm > 0:
                item["total_cbm"] = round((order_qty_rounded / case_pack) * cbm, 4)
            else:
                item["total_cbm"] = 0

            # Days current order will last
            if drr > 0:
                item["days_current_order_lasts"] = round(order_qty_rounded / drr, 2)
            else:
                item["days_current_order_lasts"] = 0

            # Days total inventory will last
            if drr > 0:
                item["days_total_inventory_lasts"] = round(
                    (closing_stock + total_transit + order_qty_rounded) / drr, 2
                )
            else:
                item["days_total_inventory_lasts"] = 0

        return combined_data


async def _generate_master_report_data(
    start_date: str,
    end_date: str,
    include_zoho: bool,
    db,
    brand: str = None,
):
    """
    Internal function that generates full master report data (with raw individual_reports).
    Used by both the API endpoint and the download endpoint.
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

        logger.info(
            f"Generating optimized master report for {start_date} to {end_date}"
        )
        start_time = datetime.now()

        # Initialize service
        report_service = OptimizedMasterReportService(db)

        # Step 1: Fetch Zoho report + composite products in parallel
        tasks = []
        if include_zoho:
            tasks.append(report_service.get_zoho_report(start_date, end_date))

        if not tasks:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="At least one data source must be selected",
            )

        # Fetch ALL composite products in parallel with reports (collection is small)
        async def _fetch_all_composites():
            try:
                composite_collection = db.get_collection("composite_products")
                def _query():
                    result = {}
                    for doc in composite_collection.find({}, {"composite_item_id": 1, "components": 1, "_id": 0}):
                        cid = doc.get("composite_item_id")
                        if cid and doc.get("components"):
                            result[str(cid)] = doc["components"]
                    return result
                return await asyncio.to_thread(_query)
            except Exception as e:
                logger.error(f"Error loading composite products: {e}")
                return {}

        # Execute all report fetches + composites + FBA stock + transfer orders in parallel
        try:
            all_results = await asyncio.wait_for(
                asyncio.gather(
                    *tasks,
                    _fetch_all_composites(),
                    report_service.fetch_fba_closing_stock(end_date),
                    report_service.fetch_transfer_orders(start_date, end_date),
                    return_exceptions=True,
                ),
                timeout=180.0,  # Allow enough time for large date ranges
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail="Report generation timed out",
            )

        # Last three results are composite_products_map, fba_stock, and transfer_orders
        results = all_results[:-3]
        composite_products_map_raw = all_results[-3]
        fba_stock_by_sku = all_results[-2]
        transfer_orders_by_sku = all_results[-1]
        if isinstance(composite_products_map_raw, Exception):
            logger.error(f"Composite products fetch failed: {composite_products_map_raw}")
            composite_products_map_raw = {}
        if isinstance(fba_stock_by_sku, Exception):
            logger.error(f"FBA stock fetch failed: {fba_stock_by_sku}")
            fba_stock_by_sku = {}
        if isinstance(transfer_orders_by_sku, Exception):
            logger.error(f"Transfer orders fetch failed: {transfer_orders_by_sku}")
            transfer_orders_by_sku = {}

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

        # Step 2: Extract all SKU codes and batch load all product data in one query
        all_sku_codes = report_service.extract_all_sku_codes(successful_reports)
        logger.info(f"Found {len(all_sku_codes)} unique SKU codes")

        # Single query to load all product data (name, rate, brand, cbm, case_pack)
        all_product_data = await report_service.batch_load_all_product_data(all_sku_codes)

        # Build product name map for normalization
        product_name_map = {}
        for sku, data in all_product_data.items():
            name = data.get("name")
            if name:
                product_name_map[sku] = name
            else:
                product_name_map[sku] = "Unknown Item"
        # Mark unknown SKUs
        for sku in all_sku_codes:
            if sku not in product_name_map:
                product_name_map[sku] = "Unknown Item"

        # Step 3: Normalize all data in parallel
        normalization_tasks = []
        for report in successful_reports:
            source = report["source"]
            data = report["data"]

            # Create task for parallel normalization
            task = asyncio.create_task(
                asyncio.to_thread(
                    report_service.normalize_single_source_data,
                    source,
                    data,
                    product_name_map,
                )
            )
            normalization_tasks.append(task)

        # Execute normalization in parallel
        try:
            normalized_results = await asyncio.gather(
                *normalization_tasks, return_exceptions=True
            )
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
        # Calculate the number of days in the period for DRR calculation
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        period_days = (end_dt - start_dt).days + 1  # +1 to include both start and end dates

        if all_normalized_data:
            try:
                # Use pre-fetched composite products map (loaded in parallel with reports)
                composite_products_map = composite_products_map_raw
                logger.info(f"Using {len(composite_products_map)} pre-loaded composite products")

                combined_data = await asyncio.to_thread(
                    report_service.combine_data_by_sku_optimized, all_normalized_data, period_days,
                    composite_products_map, fba_stock_by_sku, transfer_orders_by_sku
                )
                logger.info(f"Combined data for {len(combined_data)} unique SKUs")
            except Exception as e:
                errors.append(f"Data combination error: {str(e)}")
                combined_data = []
        else:
            combined_data = []

        # Step 4a+4b: DRR lookback + enrichment data fetched in parallel
        if combined_data:
            # Only do lookback and highlighting for date ranges >= 90 days
            skus_needing_lookback_list = []
            if period_days >= 90:
                for item in combined_data:
                    days = item.get("combined_metrics", {}).get("total_days_in_stock", 0)
                    sku = item.get("sku_code", "")
                    if days < 60 and sku:
                        skus_needing_lookback_list.append(sku)

            combined_sku_codes = {item.get("sku_code", "") for item in combined_data if item.get("sku_code")}
            new_skus = combined_sku_codes - all_sku_codes

            # Fire off ALL independent DB queries in parallel:
            # 1. sku_to_item_id (for lookback)
            # 2. brand_logistics (for enrichment)
            # 3. transit_data (for enrichment)
            # 4. extra_product_data (if new SKUs from composite expansion)
            parallel_tasks = [
                report_service.get_brand_logistics(),
                report_service.get_stock_in_transit(),
            ]
            if skus_needing_lookback_list:
                parallel_tasks.append(report_service.fetch_sku_to_item_id_map(set(skus_needing_lookback_list)))
            if new_skus:
                parallel_tasks.append(report_service.batch_load_all_product_data(new_skus))

            parallel_results = await asyncio.gather(*parallel_tasks, return_exceptions=True)

            brand_logistics = parallel_results[0] if not isinstance(parallel_results[0], Exception) else {}
            transit_data = parallel_results[1] if not isinstance(parallel_results[1], Exception) else {}
            idx = 2
            sku_to_item_id = {}
            if skus_needing_lookback_list:
                sku_to_item_id = parallel_results[idx] if not isinstance(parallel_results[idx], Exception) else {}
                idx += 1
            if new_skus and idx < len(parallel_results) and not isinstance(parallel_results[idx], Exception):
                all_product_data.update(parallel_results[idx])

            # DRR lookback (needs sku_to_item_id from above)
            try:
                if skus_needing_lookback_list and sku_to_item_id:
                    skus_with_ids = {
                        sku: sku_to_item_id[sku]
                        for sku in skus_needing_lookback_list
                        if sku in sku_to_item_id
                    }

                    if skus_with_ids:
                        logger.info(
                            f"DRR lookback needed for {len(skus_with_ids)} SKUs "
                            f"with < 60 days in stock"
                        )
                        lookback_results = await report_service.fetch_previous_period_drr(
                            skus_with_ids, start_date, period_days
                        )

                        # Apply lookback results to combined_data
                        for item in combined_data:
                            sku = item.get("sku_code", "")
                            days = item.get("combined_metrics", {}).get("total_days_in_stock", 0)
                            metrics = item.get("combined_metrics", {})

                            if days >= 60:
                                item["drr_source"] = "current_period"
                                item["drr_lookback_period"] = ""
                                item["highlight"] = None
                            elif sku in lookback_results:
                                lb = lookback_results[sku]
                                if lb["found"]:
                                    metrics["avg_daily_run_rate"] = lb["drr"]
                                    if lb["drr"] > 0:
                                        metrics["avg_days_of_coverage"] = round(
                                            metrics.get("total_closing_stock", 0) / lb["drr"], 2
                                        )
                                    item["drr_source"] = "previous_period"
                                    item["drr_lookback_period"] = lb["lookback_period"]
                                    item["highlight"] = "yellow"
                                else:
                                    item["drr_source"] = "insufficient_stock"
                                    item["drr_lookback_period"] = ""
                                    item["highlight"] = "red"
                            else:
                                item["drr_source"] = "current_period"
                                item["drr_lookback_period"] = ""
                                item["highlight"] = None

                        logger.info(
                            f"DRR lookback complete: "
                            f"{sum(1 for r in lookback_results.values() if r['found'])} found, "
                            f"{sum(1 for r in lookback_results.values() if not r['found'])} not found"
                        )
                    else:
                        for item in combined_data:
                            days = item.get("combined_metrics", {}).get("total_days_in_stock", 0)
                            item["drr_source"] = "current_period" if days >= 60 else "insufficient_stock"
                            item["drr_lookback_period"] = ""
                            item["highlight"] = "red" if days < 60 else None
                elif not skus_needing_lookback_list:
                    for item in combined_data:
                        item["drr_source"] = "current_period"
                        item["drr_lookback_period"] = ""
                        item["highlight"] = None
                else:
                    for item in combined_data:
                        days = item.get("combined_metrics", {}).get("total_days_in_stock", 0)
                        item["drr_source"] = "current_period" if days >= 60 else "insufficient_stock"
                        item["drr_lookback_period"] = ""
                        item["highlight"] = "red" if days < 60 else None

            except Exception as e:
                logger.error(f"DRR lookback error: {e}")
                logger.error(traceback.format_exc())
                errors.append(f"DRR lookback error: {str(e)}")
                for item in combined_data:
                    item.setdefault("drr_source", "current_period")
                    item.setdefault("drr_lookback_period", "")
                    item.setdefault("highlight", None)

            # Enrichment (uses brand_logistics + transit_data fetched in parallel above)
            try:
                product_rates = {}
                product_brands = {}
                logistics_data = {}
                for sku, pdata in all_product_data.items():
                    rate = pdata.get("rate")
                    if rate is not None:
                        try:
                            product_rates[sku] = float(rate)
                        except (ValueError, TypeError):
                            pass
                    product_brands[sku] = pdata.get("brand", "")
                    logistics_data[sku] = {
                        "cbm": pdata.get("cbm", 0),
                        "case_pack": pdata.get("case_pack", 0),
                    }

                if brand:
                    combined_data = [
                        item for item in combined_data
                        if product_brands.get(item.get("sku_code", ""), "").lower() == brand.lower()
                    ]
                    logger.info(f"Filtered to {len(combined_data)} items for brand '{brand}'")

                for item in combined_data:
                    metrics = item.get("combined_metrics", {})
                    if metrics.get("total_amount", 0) == 0 and metrics.get("total_units_sold", 0) > 0:
                        sku = item.get("sku_code", "")
                        rate = product_rates.get(sku, 0)
                        if rate > 0:
                            metrics["total_amount"] = round(rate * metrics["total_units_sold"], 2)

                combined_data = report_service.classify_movement(combined_data, brand_logistics, product_brands)
                combined_data = report_service.enrich_with_order_calculations(
                    combined_data, transit_data, logistics_data
                )
                logger.info(f"Enriched {len(combined_data)} items with movement and order calculations")
            except Exception as e:
                logger.error(f"Error enriching data: {e}")
                errors.append(f"Enrichment error: {str(e)}")

        # Step 5: Calculate summary statistics
        total_skus = len(combined_data)
        summary_stats = {
            "total_units_sold": 0.0,
            "total_units_returned": 0.0,
            "total_credit_notes": 0.0,
            "total_amount": 0.0,
            "total_closing_stock": 0.0,
            "avg_drr": 0.0,
            "total_transfer_orders": 0.0,
            "total_total_sales": 0.0,
        }

        if combined_data:
            for item in combined_data:
                metrics = item.get("combined_metrics", {})
                summary_stats["total_units_sold"] += metrics.get("total_units_sold", 0)
                summary_stats["total_units_returned"] += metrics.get(
                    "total_units_returned", 0
                )
                summary_stats["total_credit_notes"] += metrics.get(
                    "total_credit_notes", 0
                )
                summary_stats["total_amount"] += metrics.get("total_amount", 0)
                summary_stats["total_closing_stock"] += metrics.get(
                    "total_closing_stock", 0
                )
                summary_stats["avg_drr"] += metrics.get("avg_daily_run_rate", 0)
                summary_stats["total_transfer_orders"] += metrics.get("transfer_orders", 0)
                summary_stats["total_total_sales"] += metrics.get("total_sales", 0)

            if total_skus > 0:
                summary_stats["avg_drr"] = round(
                    summary_stats["avg_drr"] / total_skus, 2
                )

        # Source counts
        source_counts = {
            source: len(report["data"])
            for source, report in individual_reports.items()
            if report.get("success") and report.get("data")
        }

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"Master report completed in {execution_time:.2f} seconds")

        return {
            "message": f"Optimized master report generated for {start_date} to {end_date}",
            "date_range": {"start_date": start_date, "end_date": end_date},
            "summary": {
                "total_unique_skus": total_skus,
                "total_units_sold": round(summary_stats["total_units_sold"], 2),
                "total_units_returned": round(
                    summary_stats["total_units_returned"], 2
                ),
                "total_credit_notes": round(
                    summary_stats["total_credit_notes"], 2
                ),
                "total_net_units_sold": round(
                    summary_stats["total_units_sold"] - summary_stats["total_credit_notes"], 2
                ),
                "total_amount": round(summary_stats["total_amount"], 2),
                "total_closing_stock": round(
                    summary_stats["total_closing_stock"], 2
                ),
                "avg_drr": summary_stats["avg_drr"],
                "total_transfer_orders": round(
                    summary_stats["total_transfer_orders"], 2
                ),
                "total_total_sales": round(
                    summary_stats["total_total_sales"], 2
                ),
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
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in optimized master report: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate optimized master report: {str(e)}",
        )


@router.get("/master-report")
async def get_master_report(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    include_zoho: bool = Query(True, description="Include Zoho data"),
    brand: str = Query(None, description="Filter by brand name"),
    db=Depends(get_database),
):
    """
    Optimized master report with batch processing and parallel execution
    """
    content = await _generate_master_report_data(
        start_date, end_date, include_zoho, db, brand=brand,
    )

    # Strip raw data from individual_reports for the API response (keep metadata only)
    content["individual_reports"] = {
        source: {
            "source": report.get("source", source),
            "success": report.get("success", False),
            "record_count": len(report.get("data", [])),
            "error": report.get("error"),
        }
        for source, report in content["individual_reports"].items()
    }

    return JSONResponse(status_code=status.HTTP_200_OK, content=content)


@router.get("/master-report/download")
async def download_master_report(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    include_zoho: bool = Query(True, description="Include Zoho data"),
    brand: str = Query(None, description="Filter by brand name"),
    db=Depends(get_database),
):
    """
    Download master report as Excel file with multiple sheets.
    """
    try:
        # Get the full master report data (with raw individual_reports for Excel sheets)
        content = await _generate_master_report_data(
            start_date=start_date,
            end_date=end_date,
            include_zoho=include_zoho,
            db=db,
            brand=brand,
        )

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
                        "Total Units Sold": item.get("combined_metrics", {}).get(
                            "total_units_sold", 0
                        ),
                        "Total Units Returned": item.get("combined_metrics", {}).get(
                            "total_units_returned", 0
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
                        "Days in Stock": item.get("combined_metrics", {}).get(
                            "total_days_in_stock", 0
                        ),
                        "Avg Daily Run Rate": item.get("combined_metrics", {}).get(
                            "avg_daily_run_rate", 0
                        ),
                        "DRR Source": item.get("drr_source", "current_period"),
                        "DRR Lookback Period": item.get("drr_lookback_period", ""),
                        "Avg Days of Coverage": item.get("combined_metrics", {}).get(
                            "avg_days_of_coverage", 0
                        ),
                        "Pupscribe WH Stock": item.get("combined_metrics", {}).get(
                            "pupscribe_wh_stock", 0
                        ),
                        "FBA Stock": item.get("combined_metrics", {}).get(
                            "fba_closing_stock", 0
                        ),
                        "In Stock": "Yes" if item.get("in_stock", False) else "No",
                        "Transfer Orders": item.get("combined_metrics", {}).get(
                            "transfer_orders", 0
                        ),
                        "Total Sales": item.get("combined_metrics", {}).get(
                            "total_sales", 0
                        ),
                        # Movement & Order Calculation columns
                        "Movement": item.get("movement", ""),
                        "Safety Days": item.get("safety_days", 0),
                        "Lead Time": item.get("lead_time", 0),
                        "On-Hand Days Coverage": item.get("on_hand_days_coverage", 0),
                        "Stock in Transit 1": item.get("stock_in_transit_1", 0),
                        "Stock in Transit 2": item.get("stock_in_transit_2", 0),
                        "Stock in Transit 3": item.get("stock_in_transit_3", 0),
                        "Total Stock in Transit": item.get("total_stock_in_transit", 0),
                        "Current Days Coverage": item.get("current_days_coverage", 0),
                        "Target Days": item.get("target_days", 0),
                        "Excess / Order": item.get("excess_or_order", ""),
                        "Order Qty": item.get("order_qty", 0),
                        "CBM": item.get("cbm", 0),
                        "Case Pack": item.get("case_pack", 0),
                        "Order Qty (Rounded)": item.get("order_qty_rounded", 0),
                        "Total CBM": item.get("total_cbm", 0),
                        "Days Current Order Lasts": item.get("days_current_order_lasts", 0),
                        "Days Total Inventory Lasts": item.get("days_total_inventory_lasts", 0),
                    }
                )

            if combined_df_data:
                combined_df = pd.DataFrame(combined_df_data)
                combined_df.to_excel(writer, sheet_name="Combined Summary", index=False)

                # Apply row highlighting based on DRR source
                from openpyxl.styles import PatternFill
                yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
                red_fill = PatternFill(start_color="FF6B6B", end_color="FF6B6B", fill_type="solid")

                ws = writer.sheets["Combined Summary"]
                drr_source_col_idx = list(combined_df.columns).index("DRR Source") + 1  # 1-based

                for row_idx in range(2, len(combined_df_data) + 2):  # Skip header row
                    drr_source_val = ws.cell(row=row_idx, column=drr_source_col_idx).value
                    if drr_source_val == "previous_period":
                        for col_idx in range(1, len(combined_df.columns) + 1):
                            ws.cell(row=row_idx, column=col_idx).fill = yellow_fill
                    elif drr_source_val == "insufficient_stock":
                        for col_idx in range(1, len(combined_df.columns) + 1):
                            ws.cell(row=row_idx, column=col_idx).fill = red_fill

            # Sheet 2: Summary Statistics
            summary_data = [
                ["Metric", "Value"],
                ["Report Period", f"{start_date} to {end_date}"],
                ["Total Unique SKUs", summary.get("total_unique_skus", 0)],
                ["Total Units Sold", summary.get("total_units_sold", 0)],
                ["Total Units Returned", summary.get("total_units_returned", 0)],
                ["Total Amount", summary.get("total_amount", 0)],
                ["Total Closing Stock", summary.get("total_closing_stock", 0)],
                ["Average Daily Run Rate", summary.get("avg_drr", 0)],
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
        file_bytes = excel_buffer.read()

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"master_report_{start_date}_to_{end_date}_{timestamp}.xlsx"

        return Response(
            content=file_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(file_bytes)),
            },
        )

    except Exception as e:
        logger.error(f"Error generating master report Excel: {e}")
        logger.error(f"Excel generation error traceback: {traceback.format_exc()}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate Excel report: {str(e)}",
        )


@router.get("/brands")
def get_available_brands(db=Depends(get_database)):
    """Get list of available brands from products collection"""
    try:
        products_collection = db.get_collection("products")
        brands = products_collection.find({"status": "active"}).distinct("brand")
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
    Import CBM and Case Pack data from PSR Sheet.xlsx Master sheet into products collection.
    Maps BBCode (col B) to cf_sku_code and updates cbm (col BX) and case_pack (col BY).
    """
    try:
        import openpyxl
        import os

        # Find PSR Sheet.xlsx in the backend directory
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        psr_path = os.path.join(backend_dir, "PSR Sheet.xlsx")

        if not os.path.exists(psr_path):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="PSR Sheet.xlsx not found in backend directory",
            )

        wb = openpyxl.load_workbook(psr_path, read_only=True)
        ws = wb["Master"]

        products_collection = db.get_collection("products")
        updated_count = 0
        skipped_count = 0

        for row in ws.iter_rows(min_row=2, values_only=True):
            row = list(row)
            if len(row) < 77:
                continue

            bbcode = row[1]  # Col B = BBCode (SKU)
            cbm = row[75]    # Col BX = CBM
            case_pack = row[76]  # Col BY = Case Pack

            if not bbcode:
                continue

            try:
                cbm_val = float(cbm) if cbm is not None else 0
                case_pack_val = float(case_pack) if case_pack is not None else 0
            except (ValueError, TypeError):
                cbm_val = 0
                case_pack_val = 0

            result = products_collection.update_one(
                {"cf_sku_code": str(bbcode).strip()},
                {"$set": {"cbm": cbm_val, "case_pack": case_pack_val}},
            )

            if result.modified_count > 0 or result.matched_count > 0:
                updated_count += 1
            else:
                skipped_count += 1

        wb.close()

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Imported CBM and Case Pack data",
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
        }
        collection.update_one(
            {"brand": brand.strip()},
            {"$set": doc},
            upsert=True,
        )
        return JSONResponse(status_code=200, content={"message": f"Brand logistics saved for {brand}", "data": doc})
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
        return JSONResponse(status_code=200, content={"message": f"Deleted logistics for {brand}"})
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Product Logistics (CBM / Case Pack) CRUD ──────────────────────


@router.get("/product-logistics")
def get_product_logistics_list(
    search: str = Query("", description="Search by SKU or product name"),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db=Depends(get_database),
):
    """Get products with CBM and case_pack data, with pagination and search"""
    try:
        products_collection = db.get_collection("products")

        # Always filter out products without a valid SKU code
        base_filter = {"cf_sku_code": {"$exists": True, "$ne": ""}}

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
                {"cf_sku_code": 1, "name": 1, "brand": 1, "cbm": 1, "case_pack": 1, "_id": 0},
            )
            .skip(skip)
            .limit(page_size)
            .sort("cf_sku_code", 1)
        )

        # Ensure cbm and case_pack default to 0 when missing from DB
        products = []
        for p in raw_products:
            products.append({
                "cf_sku_code": p.get("cf_sku_code", ""),
                "name": p.get("name", ""),
                "brand": p.get("brand", ""),
                "cbm": p.get("cbm", 0) or 0,
                "case_pack": p.get("case_pack", 0) or 0,
            })

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


@router.put("/product-logistics")
def update_product_logistics(
    sku_code: str = Query(...),
    cbm: float = Query(None),
    case_pack: float = Query(None),
    db=Depends(get_database),
):
    """Update CBM and/or case_pack for a product"""
    try:
        products_collection = db.get_collection("products")

        update_fields = {}
        if cbm is not None:
            update_fields["cbm"] = cbm
        if case_pack is not None:
            update_fields["case_pack"] = case_pack

        if not update_fields:
            raise HTTPException(status_code=400, detail="No fields to update")

        result = products_collection.update_one(
            {"cf_sku_code": sku_code.strip()},
            {"$set": update_fields},
        )

        if result.matched_count == 0:
            raise HTTPException(status_code=404, detail=f"Product with SKU '{sku_code}' not found")

        return JSONResponse(
            status_code=200,
            content={"message": f"Updated logistics for {sku_code}", "updated_fields": update_fields},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
