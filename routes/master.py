from fastapi import APIRouter, HTTPException, status, Depends, Query
from fastapi.responses import JSONResponse, Response, StreamingResponse
from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Optional
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
                any_last_90_days=False,  # Explicit False avoids the Query() default being truthy
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

    async def fetch_latest_zoho_wh_stock(self) -> Dict:
        """Fetch the two most recent Pupscribe WH stock snapshots per SKU from
        zoho_warehouse_stock.
        Returns {
            "by_sku": {sku: qty},        # latest date
            "latest_date": "YYYY-MM-DD",
            "prev_by_sku": {sku: qty},   # one snapshot before latest
            "prev_date": "YYYY-MM-DD" | None,
        }."""
        try:
            stock_collection = self.database["zoho_warehouse_stock"]
            products_collection = self.database["products"]

            def _fetch():
                # Step 1: cheaply find the latest date (O(1) with a date index).
                latest_doc = stock_collection.find_one(
                    {},
                    sort=[("date", -1)],
                    projection={"date": 1},
                )
                if not latest_doc:
                    return {"by_sku": {}, "latest_date": None, "prev_by_sku": {}, "prev_date": None}
                latest_date = latest_doc["date"]

                # Step 1b: find the previous available date (snapshot before latest).
                prev_doc = stock_collection.find_one(
                    {"date": {"$lt": latest_date}},
                    sort=[("date", -1)],
                    projection={"date": 1},
                )
                prev_date = prev_doc["date"] if prev_doc else None

                def _agg_date(target_date):
                    """Aggregate Pupscribe WH stock by zoho_item_id for target_date."""
                    pipeline = [
                        {"$match": {"date": target_date, "zoho_item_id": {"$ne": None}}},
                        {
                            "$group": {
                                "_id": "$zoho_item_id",
                                "stock": {
                                    "$first": {
                                        "$ifNull": [
                                            "$warehouses.Pupscribe Enterprises Private Limited",
                                            0,
                                        ]
                                    }
                                },
                            }
                        },
                    ]
                    result: Dict[str, float] = {}
                    for doc in stock_collection.aggregate(pipeline, allowDiskUse=True):
                        result[str(doc["_id"])] = float(doc.get("stock", 0) or 0)
                    return result

                # Aggregate both snapshots in sequence (same thread, avoids two round-trips).
                by_item_id = _agg_date(latest_date)
                prev_by_item_id = _agg_date(prev_date) if prev_date is not None else {}

                latest_date_str = latest_date.strftime("%Y-%m-%d")
                prev_date_str = prev_date.strftime("%Y-%m-%d") if prev_date is not None else None

                if not by_item_id:
                    return {
                        "by_sku": {}, "latest_date": latest_date_str,
                        "prev_by_sku": {}, "prev_date": prev_date_str,
                    }

                # Step 3: collect all item_ids from both snapshots, resolve SKUs once.
                all_item_ids = set(by_item_id) | set(prev_by_item_id)
                item_id_to_sku: Dict[str, str] = {}
                for doc in products_collection.find(
                    {"item_id": {"$in": list(all_item_ids)}},
                    {"item_id": 1, "cf_sku_code": 1, "_id": 0},
                ):
                    iid = str(doc.get("item_id", ""))
                    sku = doc.get("cf_sku_code", "")
                    if iid and sku:
                        item_id_to_sku[iid] = sku

                def _map_to_sku(item_id_map):
                    by_sku: Dict[str, float] = {}
                    for iid, qty in item_id_map.items():
                        sku = item_id_to_sku.get(iid)
                        if sku:
                            by_sku[sku] = by_sku.get(sku, 0) + qty
                    return by_sku

                return {
                    "by_sku": _map_to_sku(by_item_id),
                    "latest_date": latest_date_str,
                    "prev_by_sku": _map_to_sku(prev_by_item_id),
                    "prev_date": prev_date_str,
                }

            result = await asyncio.to_thread(_fetch)
            logger.info(
                f"Fetched latest Zoho WH stock for {len(result.get('by_sku', {}))} SKUs, "
                f"date: {result.get('latest_date')}; prev snapshot: {result.get('prev_date')}"
            )
            return result

        except Exception as e:
            logger.error(f"Error fetching latest Zoho WH stock: {e}")
            return {"by_sku": {}, "latest_date": None, "prev_by_sku": {}, "prev_date": None}

    async def fetch_latest_fba_stock(self) -> Dict:
        """Fetch the two most recent FBA stock snapshots per SKU from amazon_ledger.
        Returns {
            "by_sku": {sku: qty},        # latest date
            "latest_date": "YYYY-MM-DD" | None,
            "prev_by_sku": {sku: qty},   # one snapshot before latest
            "prev_date": "YYYY-MM-DD" | None,
        }."""
        try:
            ledger_collection = self.database["amazon_ledger"]
            sku_mapping_collection = self.database["amazon_sku_mapping"]

            def _fetch():
                _filter = {"disposition": "SELLABLE", "location": {"$ne": "VKSX"}}
                # Step 1: find latest date
                latest_doc = ledger_collection.find_one(_filter, sort=[("date", -1)], projection={"date": 1})
                if not latest_doc:
                    return {"by_sku": {}, "latest_date": None, "prev_by_sku": {}, "prev_date": None}
                latest_date = latest_doc["date"]

                # Step 1b: find previous available date
                prev_doc = ledger_collection.find_one(
                    {**_filter, "date": {"$lt": latest_date}},
                    sort=[("date", -1)],
                    projection={"date": 1},
                )
                prev_date = prev_doc["date"] if prev_doc else None

                def _agg_date(target_date):
                    pipeline = [
                        {"$match": {"date": {"$eq": target_date}, "disposition": "SELLABLE", "location": {"$ne": "VKSX"}}},
                        {"$group": {"_id": "$asin", "closing_stock": {"$sum": "$ending_warehouse_balance"}}},
                    ]
                    result = {}
                    for doc in ledger_collection.aggregate(pipeline):
                        stock = float(doc.get("closing_stock", 0) or 0)
                        if stock > 0:
                            result[doc["_id"]] = stock
                    return result

                asin_stock = _agg_date(latest_date)
                prev_asin_stock = _agg_date(prev_date) if prev_date is not None else {}

                # Single SKU mapping lookup for both snapshots
                all_asins = set(asin_stock) | set(prev_asin_stock)
                sku_docs = list(sku_mapping_collection.find(
                    {"item_id": {"$in": list(all_asins)}},
                    {"item_id": 1, "sku_code": 1, "_id": 0},
                ))
                asin_to_sku = {doc["item_id"]: doc["sku_code"] for doc in sku_docs if doc.get("item_id") and doc.get("sku_code")}

                def _map_to_sku(asin_map):
                    by_sku: Dict[str, float] = {}
                    for asin, qty in asin_map.items():
                        sku = asin_to_sku.get(asin)
                        if sku:
                            by_sku[sku] = by_sku.get(sku, 0) + qty
                    return by_sku

                return {
                    "by_sku": _map_to_sku(asin_stock),
                    "latest_date": latest_date.strftime("%Y-%m-%d"),
                    "prev_by_sku": _map_to_sku(prev_asin_stock),
                    "prev_date": prev_date.strftime("%Y-%m-%d") if prev_date else None,
                }

            result = await asyncio.to_thread(_fetch)
            logger.info(
                f"Fetched latest FBA stock for {len(result.get('by_sku', {}))} SKUs "
                f"(latest: {result.get('latest_date')}, prev: {result.get('prev_date')})"
            )
            return result

        except Exception as e:
            logger.error(f"Error fetching latest FBA stock: {e}")
            return {"by_sku": {}, "latest_date": None, "prev_by_sku": {}, "prev_date": None}

    async def fetch_missed_sales(self, start_date: str, end_date: str) -> Dict[str, float]:
        """Fetch total missed_sales_quantity from missed_sales collection grouped by item_code (= cf_sku_code).
        Composite SKUs are expanded into their component SKUs before summing.
        Returns dict of {sku_code: total_missed_sales_quantity}."""
        try:
            collection = self.database.get_collection("missed_sales")
            composite_collection = self.database.get_collection("composite_products")
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

            def _fetch():
                pipeline = [
                    {"$match": {
                        "date": {"$gte": start_dt, "$lte": end_dt},
                        "customer_name": {"$not": {"$regex": "ETRADE", "$options": "i"}},
                    }},
                    {
                        "$group": {
                            "_id": "$item_code",
                            "total_missed_sales": {"$sum": "$missed_sales_quantity"},
                        }
                    },
                ]
                result = {}
                for doc in collection.aggregate(pipeline):
                    item_code = doc["_id"]
                    qty = doc.get("total_missed_sales", 0) or 0
                    if item_code and qty > 0:
                        result[item_code] = float(qty)
                return result

            def _fetch_composites(sku_codes):
                composite_map = {}
                for doc in composite_collection.find(
                    {"sku_code": {"$in": sku_codes}},
                    {"sku_code": 1, "components": 1, "_id": 0},
                ):
                    sku = doc.get("sku_code")
                    if sku and doc.get("components"):
                        composite_map[sku] = doc["components"]
                return composite_map

            raw = await asyncio.to_thread(_fetch)

            if not raw:
                return {}

            composite_map = await asyncio.to_thread(_fetch_composites, list(raw.keys()))

            # Expand composite SKUs into their components
            result: Dict[str, float] = {}
            for sku, total_qty in raw.items():
                components = composite_map.get(sku)
                if components:
                    for comp in components:
                        comp_sku = comp.get("sku_code")
                        if comp_sku:
                            comp_qty = total_qty * float(comp.get("quantity", 1))
                            result[comp_sku] = result.get(comp_sku, 0) + comp_qty
                else:
                    result[sku] = result.get(sku, 0) + total_qty

            logger.info(f"Fetched missed sales for {len(result)} SKUs (after composite expansion)")
            return result

        except Exception as e:
            logger.error(f"Error fetching missed sales: {e}")
            return {}

    async def fetch_fba_closing_stock(self, end_date: str) -> Dict[str, float]:
        """Fetch FBA closing stock from amazon_ledger, mapped by SKU code.
        Returns dict of {sku_code: closing_stock}."""
        try:
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
            ledger_collection = self.database["amazon_ledger"]
            sku_mapping_collection = self.database["amazon_sku_mapping"]

            def _fetch():
                # Find the latest date in the ledger that is <= end_date.
                # The ledger is typically ingested with a 1-day lag, so an exact
                # match on end_date would return nothing if that date hasn't been
                # uploaded yet.
                latest_doc = ledger_collection.find_one(
                    {
                        "date": {"$lte": end_datetime},
                        "disposition": "SELLABLE",
                        "location": {"$ne": "VKSX"},
                    },
                    sort=[("date", -1)],
                    projection={"date": 1},
                )
                if not latest_doc:
                    return {}
                effective_date = latest_doc["date"]

                # Get latest ending_warehouse_balance per ASIN from ledger
                pipeline = [
                    {
                        "$match": {
                            "date": {"$eq": effective_date},
                            "disposition": "SELLABLE",
                            "location": {"$ne": "VKSX"},
                        }
                    },
                    {
                        "$group": {
                            "_id": "$asin",
                            "closing_stock": {"$sum": "$ending_warehouse_balance"},
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
            logger.info(f"Fetched FBA closing stock for {len(result)} SKUs (end_date={end_date})")
            return result

        except Exception as e:
            logger.error(f"Error fetching FBA closing stock: {e}")
            return {}

    async def fetch_vendor_central_by_sku(self, start_date: str, end_date: str) -> Dict[str, Dict]:
        """Fetch Vendor Central (Etrade) closing stock and DRR per SKU for the given period.
        ASINs are mapped to SKU codes via amazon_sku_mapping.
        Returns {sku_code: {closing_stock, drr}, "__latest_inv_date__": "YYYY-MM-DD"}."""
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")

            vc_sales_col = self.database.get_collection("amazon_vendor_sales")
            vc_inv_col = self.database.get_collection("amazon_vendor_inventory")
            sku_mapping_col = self.database.get_collection("amazon_sku_mapping")

            def _fetch():
                # Aggregate VC sales per ASIN over the period
                pipeline = [
                    {"$match": {"date": {"$gte": start, "$lte": end}}},
                    {
                        "$group": {
                            "_id": "$asin",
                            "total_units_sold": {"$sum": "$orderedUnits"},
                            "days_in_stock": {
                                "$sum": {
                                    "$cond": [{"$gt": ["$orderedUnits", 0]}, 1, 0]
                                }
                            },
                        }
                    },
                ]
                sales_by_asin = {}
                for doc in vc_sales_col.aggregate(pipeline):
                    asin = doc["_id"]
                    if asin:
                        sales_by_asin[asin] = {
                            "units_sold": doc.get("total_units_sold", 0) or 0,
                            "days_in_stock": doc.get("days_in_stock", 0) or 0,
                        }

                if not sales_by_asin:
                    return {}

                asins = list(sales_by_asin.keys())

                # Find the latest available inventory date across all ASINs
                latest_inv_doc = vc_inv_col.find_one(
                    {"asin": {"$in": asins}, "date": {"$lte": end}},
                    sort=[("date", -1)],
                    projection={"date": 1},
                )
                latest_inv_date = ""
                if latest_inv_doc and latest_inv_doc.get("date"):
                    d = latest_inv_doc["date"]
                    if isinstance(d, datetime):
                        latest_inv_date = d.strftime("%Y-%m-%d")
                    else:
                        latest_inv_date = str(d)[:10]

                # Fetch the latest closing stock per ASIN on or before end_date
                inv_pipeline = [
                    {"$match": {"asin": {"$in": asins}, "date": {"$lte": end}}},
                    {"$sort": {"date": -1}},
                    {
                        "$group": {
                            "_id": "$asin",
                            "closing_stock": {"$first": "$sellableOnHandInventoryUnits"},
                        }
                    },
                ]
                stock_by_asin = {}
                for doc in vc_inv_col.aggregate(inv_pipeline):
                    asin = doc["_id"]
                    if asin:
                        stock_by_asin[asin] = float(doc.get("closing_stock", 0) or 0)

                # Map ASINs → SKU codes
                sku_docs = list(sku_mapping_col.find(
                    {"item_id": {"$in": asins}},
                    {"item_id": 1, "sku_code": 1, "_id": 0},
                ).sort("_id", 1))

                asin_to_sku = {}
                for doc in sku_docs:
                    asin = doc.get("item_id")
                    sku = doc.get("sku_code")
                    if asin and sku:
                        asin_to_sku[asin] = sku

                # Build result by SKU (sum if multiple ASINs share a SKU)
                result = {}
                for asin, sales in sales_by_asin.items():
                    sku = asin_to_sku.get(asin)
                    if not sku:
                        continue
                    stock = stock_by_asin.get(asin, 0)
                    units = sales["units_sold"]
                    days = sales["days_in_stock"]
                    if sku not in result:
                        result[sku] = {"closing_stock": 0.0, "units_sold": 0.0, "days_in_stock": 0}
                    result[sku]["closing_stock"] += stock
                    result[sku]["units_sold"] += units
                    result[sku]["days_in_stock"] += days

                # Compute DRR per SKU
                period_days = (end - start).days + 1
                for sku, d in result.items():
                    days = d["days_in_stock"] or period_days
                    d["drr"] = round(d["units_sold"] / days, 4) if days > 0 else 0.0

                result["__latest_inv_date__"] = latest_inv_date
                return result

            data = await asyncio.to_thread(_fetch)
            logger.info(f"Fetched Vendor Central data for {len(data)} SKUs")
            return data

        except Exception as e:
            logger.error(f"Error fetching vendor central by SKU: {e}")
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
                ).sort("_id", 1))

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
                    {"cf_sku_code": 1, "item_id": 1, "name": 1, "rate": 1, "brand": 1, "cbm": 1, "case_pack": 1,
                     "cf_item_code": 1,
                     "purchase_status": 1, "stock_in_transit_1": 1, "stock_in_transit_2": 1,
                     "stock_in_transit_3": 1, "created_at": 1, "_id": 0},
                ).sort("_id", 1))

            products = await asyncio.to_thread(_fetch_all)

            three_months_ago = datetime.utcnow() - timedelta(days=90)

            result = {}
            for product in products:
                sku_code = product.get("cf_sku_code")
                if not sku_code:
                    continue
                created_at = product.get("created_at")
                if isinstance(created_at, str):
                    try:
                        created_at = datetime.fromisoformat(created_at.replace("Z", "+00:00")).replace(tzinfo=None)
                    except (ValueError, AttributeError):
                        created_at = None
                is_new = isinstance(created_at, datetime) and created_at >= three_months_ago
                result[sku_code] = {
                    "name": product.get("name"),
                    "item_id": product.get("item_id"),
                    "rate": product.get("rate"),
                    "brand": product.get("brand", ""),
                    "cbm": self.safe_float(product.get("cbm")),
                    "case_pack": self.safe_float(product.get("case_pack")),
                    "manufacturer_code": product.get("cf_item_code", "") or "",
                    "purchase_status": product.get("purchase_status", ""),
                    "stock_in_transit_1": self.safe_float(product.get("stock_in_transit_1")),
                    "stock_in_transit_2": self.safe_float(product.get("stock_in_transit_2")),
                    "stock_in_transit_3": self.safe_float(product.get("stock_in_transit_3")),
                    "is_new": is_new,
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
            credit_notes = metrics.get("total_credit_notes", 0)
            zoho_stock = data.pop("_zoho_closing_stock")
            fba_stock = fba_stock_by_sku.get(sku, 0)

            days_in_stock = metrics.get("total_days_in_stock", 0)
            if days_in_stock > 0:
                metrics["avg_daily_run_rate"] = round(round(metrics["total_units_sold"] - credit_notes, 2) / days_in_stock, 2)
            elif period_days > 0:
                metrics["avg_daily_run_rate"] = round(round(metrics["total_units_sold"] - credit_notes, 2) / period_days, 2)

            # Closing stock = Pupscribe WH (Zoho) + FBA stock
            metrics["total_closing_stock"] = round(zoho_stock + fba_stock, 2)
            metrics["fba_closing_stock"] = round(fba_stock, 2)
            metrics["pupscribe_wh_stock"] = round(zoho_stock, 2)

            # Transfer orders and total sales
            transfer_qty = transfer_orders_by_sku.get(sku, 0)
            metrics["transfer_orders"] = round(transfer_qty, 2)
            metrics["total_sales"] = round(metrics["total_units_sold"] - credit_notes - transfer_qty, 2)

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
                ).sort("_id", 1))

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
                ).sort("_id", 1)
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
                        "units_sold": units_sold,
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

    async def fetch_past_90d_drr(
        self,
        sku_to_item_id: Dict[str, str],
        start_date: str,
    ) -> Dict[str, float]:
        """
        Fetch Zoho-based DRR for the 90-day window immediately before start_date.
        Used to compute the growth rate column.
        Returns dict of sku_code -> past_drr (0.0 when no history found).
        """
        from .zoho import fetch_stock_data_for_items_batch, fetch_zoho_lookback_sales_batch

        if not sku_to_item_id:
            return {}

        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            past_end = start_dt - timedelta(days=1)
            past_start = past_end - timedelta(days=89)  # 90 days inclusive
            periods = [(past_start, past_end)]
            all_item_ids = list(set(sku_to_item_id.values()))

            stock_by_period, sales_by_period = await asyncio.gather(
                fetch_stock_data_for_items_batch(self.database, periods, all_item_ids),
                fetch_zoho_lookback_sales_batch(self.database, periods, all_item_ids),
            )

            stock_data = stock_by_period.get(0, {})
            sales_data = sales_by_period.get(0, {})

            result: Dict[str, float] = {}
            for sku, item_id in sku_to_item_id.items():
                stock_info = stock_data.get(item_id, {})
                days_in_stock = stock_info.get("total_days_in_stock", 0)
                units_sold = sales_data.get(str(item_id), 0)
                result[sku] = round(units_sold / days_in_stock, 4) if days_in_stock > 0 else 0.0

            logger.info(f"Fetched past 90d DRR for {len(result)} SKUs")
            return result
        except Exception as e:
            logger.error(f"Error fetching past 90d DRR: {e}")
            return {}

    @staticmethod
    def classify_movement(
        combined_data: List[Dict],
        brand_logistics: Dict[str, Dict],
        product_brands: Dict[str, str],
        logistics_data: Dict[str, Dict] = None,
    ) -> List[Dict]:
        """Classify each SKU as Fast/Medium/Slow mover based on volume and revenue percentiles
        computed within each brand group. Products are only ranked against other products of
        the same brand, so brand size does not skew classifications.
        Only active products (purchase_status == 'active') participate in percentile ranking;
        inactive/discontinued products are always classified as Slow Mover."""
        if not combined_data:
            return combined_data

        default_settings = {
            "lead_time": 60,
            "safety_days_fast": 40,
            "safety_days_medium": 25,
            "safety_days_slow": 15,
        }

        # Group item indices by brand
        brand_groups: Dict[str, List[int]] = defaultdict(list)
        for i, item in enumerate(combined_data):
            sku = item.get("sku_code", "")
            brand = product_brands.get(sku, "") or ""
            brand_groups[brand.lower()].append(i)

        # Rank and classify within each brand group independently
        for brand_key, indices in brand_groups.items():
            brand_settings = brand_logistics.get(brand_key, default_settings)

            # Extract metrics for this brand's items only
            # If an item was in stock < 60 days, use MAX(current sales, lookback sales)
            # so stockouts don't suppress the movement ranking
            group_metrics = []
            for i in indices:
                item_metrics = combined_data[i].get("combined_metrics", {})
                days_in_stock = item_metrics.get("total_days_in_stock", 0)
                sales_90 = item_metrics.get("total_units_sold", 0)
                if days_in_stock >= 60:
                    movement_units = sales_90
                else:
                    lookback_sales = combined_data[i].get("drr_lookback_sales", 0) or 0
                    movement_units = max(sales_90, lookback_sales)
                group_metrics.append({
                    "units_sold": movement_units,
                    "amount": item_metrics.get("total_amount", 0),
                })

            # Only rank active products (purchase_status == "active") against each other
            _logistics = logistics_data or {}
            active_indices = [
                j for j in range(len(indices))
                if _logistics.get(combined_data[indices[j]].get("sku_code", ""), {}).get("purchase_status", "active") == "active"
            ]
            active_size = len(active_indices)

            vol_pct = [1.0] * len(indices)  # inactive default → Slow Mover
            rev_pct = [1.0] * len(indices)

            if active_size > 0:
                # Rank by volume within active products (highest → rank 1)
                vol_order = sorted(active_indices, key=lambda j: group_metrics[j]["units_sold"], reverse=True)
                for rank, j in enumerate(vol_order):
                    vol_pct[j] = (rank + 1) / active_size

                # Rank by revenue within active products (highest → rank 1)
                rev_order = sorted(active_indices, key=lambda j: group_metrics[j]["amount"], reverse=True)
                for rank, j in enumerate(rev_order):
                    rev_pct[j] = (rank + 1) / active_size

            # Classify each item in this brand group
            for j, global_idx in enumerate(indices):
                item = combined_data[global_idx]
                if vol_pct[j] <= 0.2 or rev_pct[j] <= 0.2:
                    mover_class = 1
                    movement = "Fast Mover"
                    safety_days = brand_settings.get("safety_days_fast", 40)
                elif vol_pct[j] <= 0.5 or rev_pct[j] <= 0.5:
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
                item["brand"] = product_brands.get(item.get("sku_code", ""), "") or ""

        return combined_data

    async def get_stock_in_transit(self) -> Dict[str, Dict]:
        """Get stock in transit from open purchase orders, grouped by SKU"""
        try:
            po_collection = self.database.get_collection("purchase_orders")

            def _fetch_open_pos():
                return list(po_collection.find(
                    {"order_status_formatted": "Issued"},
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
                    {"cf_sku_code": 1, "cbm": 1, "case_pack": 1, "purchase_status": 1,
                     "stock_in_transit_1": 1, "stock_in_transit_2": 1, "stock_in_transit_3": 1, "_id": 0}
                ).sort("_id", 1))

            products = await asyncio.to_thread(_fetch_logistics)

            result = {}
            for p in products:
                sku = p.get("cf_sku_code")
                if sku:
                    result[sku] = {
                        "cbm": self.safe_float(p.get("cbm")),
                        "case_pack": self.safe_float(p.get("case_pack")),
                        "purchase_status": p.get("purchase_status", ""),
                        "stock_in_transit_1": self.safe_float(p.get("stock_in_transit_1")),
                        "stock_in_transit_2": self.safe_float(p.get("stock_in_transit_2")),
                        "stock_in_transit_3": self.safe_float(p.get("stock_in_transit_3")),
                    }

            return result

        except Exception as e:
            logger.error(f"Error fetching product logistics: {e}")
            return {}

    async def fetch_latest_po_unit_prices(self, sku_codes: Set[str]) -> Dict[str, Dict]:
        """Fetch unit price (rate) and currency_code from the latest purchase order per SKU.
        Pass 1: matches via line_items.item_custom_fields where api_name == 'cf_sku_code'.
        Pass 2 (fallback): for SKUs not found in pass 1, matches by line_items.item_id
                           via a products-collection lookup (handles POs without custom fields).
        Returns {sku_code: {"rate": float, "currency_code": str}}."""
        if not sku_codes:
            return {}
        try:
            po_collection = self.database.get_collection("purchase_orders")
            products_collection = self.database.get_collection("products")
            sku_list = [s for s in sku_codes if s]

            def _fetch():
                # Pass 1: match via item_custom_fields.cf_sku_code
                pipeline = [
                    {"$unwind": "$line_items"},
                    {"$match": {"line_items.rate": {"$gt": 0}}},
                    {"$unwind": "$line_items.item_custom_fields"},
                    {"$match": {
                        "line_items.item_custom_fields.api_name": "cf_sku_code",
                        "line_items.item_custom_fields.value": {"$in": sku_list},
                    }},
                    {"$sort": {"date": -1}},
                    {"$group": {
                        "_id": "$line_items.item_custom_fields.value",
                        "rate": {"$first": "$line_items.rate"},
                        "currency_code": {"$first": "$currency_code"},
                    }},
                ]
                result = {}
                for doc in po_collection.aggregate(pipeline):
                    sku = doc.get("_id")
                    if sku:
                        result[sku] = {
                            "rate": float(doc.get("rate") or 0),
                            "currency_code": doc.get("currency_code", "") or "",
                        }

                # Pass 2: fallback for SKUs not found (PO line items without cf_sku_code)
                missing_skus = [s for s in sku_list if s not in result]
                if missing_skus:
                    # Build item_id → sku map for missing SKUs from products collection
                    item_id_to_sku: Dict[str, str] = {}
                    for doc in products_collection.find(
                        {"cf_sku_code": {"$in": missing_skus}},
                        {"item_id": 1, "cf_sku_code": 1, "_id": 0},
                    ):
                        iid = doc.get("item_id")
                        sku = doc.get("cf_sku_code", "")
                        if iid and sku:
                            item_id_to_sku[str(iid)] = sku

                    if item_id_to_sku:
                        str_ids = list(item_id_to_sku.keys())
                        int_ids = [int(i) for i in str_ids if i.isdigit()]
                        pipeline2 = [
                            {"$unwind": "$line_items"},
                            {"$match": {"$and": [
                                {"$or": [
                                    {"line_items.item_id": {"$in": str_ids}},
                                    {"line_items.item_id": {"$in": int_ids}},
                                ]},
                                {"line_items.rate": {"$gt": 0}},
                            ]}},
                            {"$sort": {"date": -1}},
                            {"$group": {
                                "_id": "$line_items.item_id",
                                "rate": {"$first": "$line_items.rate"},
                                "currency_code": {"$first": "$currency_code"},
                            }},
                        ]
                        for doc in po_collection.aggregate(pipeline2):
                            iid = str(doc.get("_id", ""))
                            sku = item_id_to_sku.get(iid)
                            if sku and sku not in result:
                                result[sku] = {
                                    "rate": float(doc.get("rate") or 0),
                                    "currency_code": doc.get("currency_code", "") or "",
                                }

                return result

            result = await asyncio.to_thread(_fetch)
            logger.info(f"Fetched latest PO unit prices for {len(result)} SKUs")
            return result
        except Exception as e:
            logger.error(f"Error fetching latest PO unit prices: {e}")
            return {}

    @staticmethod
    def enrich_with_order_calculations(
        combined_data: List[Dict],
        transit_data: Dict[str, Dict],
        logistics_data: Dict[str, Dict],
        missed_sales_by_sku: Dict[str, float] = None,
        period_days: int = 30,
        current_period_missed_sales_by_sku: Dict[str, float] = None,
    ) -> List[Dict]:
        """Add order calculation columns to each item in combined_data"""
        if missed_sales_by_sku is None:
            missed_sales_by_sku = {}
        if current_period_missed_sales_by_sku is None:
            current_period_missed_sales_by_sku = {}

        for item in combined_data:
            sku = item.get("sku_code", "")
            metrics = item.get("combined_metrics", {})
            drr = metrics.get("avg_daily_run_rate", 0)
            closing_stock = metrics.get("total_closing_stock", 0)

            # Transit data: always derived from open purchase orders
            logistics = logistics_data.get(sku, {})
            purchase_status = logistics.get("purchase_status", "")
            transit = transit_data.get(sku, {})
            sit_1 = transit.get("transit_1", 0)
            sit_2 = transit.get("transit_2", 0)
            sit_3 = transit.get("transit_3", 0)
            item["stock_in_transit_1"] = sit_1
            item["stock_in_transit_2"] = sit_2
            item["stock_in_transit_3"] = sit_3
            total_transit = transit.get("total", 0)  # sum of ALL open PO lines, not just 3
            item["total_stock_in_transit"] = total_transit

            # Days coverage
            on_hand_days = metrics.get("avg_days_of_coverage", 0)
            item["on_hand_days_coverage"] = round(on_hand_days, 2)

            current_days_coverage = 0
            if drr > 0:
                current_days_coverage = round((closing_stock + total_transit) / drr, 2)
            item["current_days_coverage"] = current_days_coverage

            # Target days = lead_time + safety_days + order_processing
            safety_days = item.get("safety_days", 15)
            lead_time = item.get("lead_time", 60)
            order_processing = 10
            item["order_processing"] = order_processing
            target_days = lead_time + safety_days + order_processing

            # Missed Sales columns (inserted between Current Days Coverage and Target Days)
            # For demand-override items use current-period missed sales; otherwise use
            # whatever is in missed_sales_by_sku (which may be lookback-period for those SKUs)
            if item.get("order_qty_demand_override"):
                missed_sales = current_period_missed_sales_by_sku.get(sku, 0)
            else:
                missed_sales = missed_sales_by_sku.get(sku, 0)
            item["missed_sales"] = missed_sales
            missed_sales_drr_raw = missed_sales / period_days if period_days > 0 else 0.0
            
            # Cap at 50% of true DRR to prevent over-ordering from missed sales spikes
            if drr > 0:
                missed_sales_drr_raw = min(missed_sales_drr_raw, 0.5 * drr)
            missed_sales_drr = round(missed_sales_drr_raw, 4)
            item["missed_sales_drr"] = missed_sales_drr
            extra_qty = round(missed_sales_drr * lead_time, 2)
            item["extra_qty"] = extra_qty

            # Net Target Days
            if current_days_coverage < lead_time:
                net_target_days = target_days
            else:
                net_target_days = target_days - current_days_coverage
            item["net_target_days"] = round(net_target_days, 2)

            item["target_days"] = target_days

            # Excess or Order
            if drr == 0:
                item["excess_or_order"] = "NO MOVEMENT"
            elif current_days_coverage < target_days:
                item["excess_or_order"] = "ORDER"
            else:
                item["excess_or_order"] = "EXCESS"

            # Logistics (CBM / Case Pack / Purchase Status / Is New)
            cbm = logistics.get("cbm", 0)
            case_pack = logistics.get("case_pack", 0)
            item["cbm"] = cbm
            item["case_pack"] = case_pack
            item["purchase_status"] = purchase_status
            item["is_new"] = logistics.get("is_new", False)

            # Skip order quantity calculations for inactive / discontinued items
            if purchase_status in ("inactive", "discontinued until stock lasts"):
                item["order_qty"] = 0
                item["order_qty_plus_extra_qty"] = round(extra_qty, 2)
                item["order_qty_plus_extra_qty_rounded"] = 0
                item["total_cbm"] = 0
                item["days_current_order_lasts"] = 0
                item["days_total_inventory_lasts"] = round(current_days_coverage, 2)
                item["order_qty_basis"] = ""
                continue

            # Order quantity
            order_qty = max(0, net_target_days * drr)

            # Demand-based override: when current-period sales exceed lookback sales,
            # use current-period sales as the order qty instead of DRR × target days
            if item.get("order_qty_demand_override"):
                order_qty = round(item.get("current_period_sales_for_override", 0), 2)
                item["order_qty_basis"] = "Current Period Sales"
            else:
                item["order_qty_basis"] = ""

            # Confidence tier dampening for red (insufficient_stock) items.
            # These products have < 60 days in stock across all lookback windows,
            # so their DRR is unreliable. Scale back the order qty based on how
            # much data we have and the product's movement classification.
            # Tiers are calibrated for a 90-day reporting period.
            if item.get("drr_source") == "insufficient_stock":
                days_in_stock = item.get("combined_metrics", {}).get("total_days_in_stock", 0)
                movement = item.get("movement", "Slow Mover")

                if days_in_stock < 15:
                    base_multiplier = 0.25
                elif days_in_stock < 30:
                    base_multiplier = 0.5
                elif days_in_stock < 60:
                    base_multiplier = 0.75
                else:
                    base_multiplier = 1.0

                confidence_multiplier = min(1.0, max(0.1, base_multiplier))
                order_qty = order_qty * confidence_multiplier
                item["confidence_multiplier"] = confidence_multiplier

                dampener_label = f"Dampened {int(round(confidence_multiplier * 100))}% ({days_in_stock}d, {movement})"
                if item.get("order_qty_demand_override"):
                    item["order_qty_basis"] = f"Current Period Sales · {dampener_label}"
                else:
                    item["order_qty_basis"] = dampener_label
            else:
                item["confidence_multiplier"] = 1.0

            item["order_qty"] = round(order_qty, 2)

            # Order Qty + Extra Qty
            item["order_qty_plus_extra_qty"] = round(order_qty + extra_qty, 2)

            # Order Qty + Extra Qty rounded down to case pack
            order_qty_plus_extra_qty = item["order_qty_plus_extra_qty"]
            if case_pack > 0:
                item["order_qty_plus_extra_qty_rounded"] = math.floor(order_qty_plus_extra_qty / case_pack) * case_pack
            else:
                item["order_qty_plus_extra_qty_rounded"] = round(order_qty_plus_extra_qty, 0)

            order_qty_rounded = item["order_qty_plus_extra_qty_rounded"]

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

            # Days total inventory will last = current coverage + days order lasts
            item["days_total_inventory_lasts"] = round(
                current_days_coverage + item["days_current_order_lasts"], 2
            )

        return combined_data


async def _generate_master_report_data(
    start_date: str,
    end_date: str,
    include_zoho: bool,
    db,
    brand: str = None,
    dashboard_mode: bool = False,
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

        # Kick off latest-stock fetches immediately as background tasks.
        # These scan 4-5M records (zoho_warehouse_stock) and are completely independent
        # of the report date range — running them in the background hides their latency
        # behind all the other processing (Zoho report, DRR lookback, enrichment, etc.).
        # Note: create_task needs a coroutine, so we create one task per coroutine.
        _zoho_stock_task = asyncio.create_task(report_service.fetch_latest_zoho_wh_stock())
        _fba_stock_task = asyncio.create_task(report_service.fetch_latest_fba_stock())

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

        # Execute core report fetches in parallel (Zoho + composites + period-specific data)
        try:
            all_results = await asyncio.wait_for(
                asyncio.gather(
                    *tasks,
                    _fetch_all_composites(),
                    report_service.fetch_fba_closing_stock(end_date),
                    report_service.fetch_transfer_orders(start_date, end_date),
                    report_service.fetch_missed_sales(start_date, end_date),
                    report_service.fetch_vendor_central_by_sku(start_date, end_date),
                    return_exceptions=True,
                ),
                timeout=180.0,
            )
        except asyncio.TimeoutError:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail="Report generation timed out",
            )

        # Last five suffix results: composite, fba_stock, transfer_orders, missed_sales, vc_by_sku
        results = all_results[:-5]
        composite_products_map_raw = all_results[-5]
        fba_stock_by_sku = all_results[-4]
        transfer_orders_by_sku = all_results[-3]
        missed_sales_by_sku = all_results[-2]
        vc_by_sku = all_results[-1]

        if isinstance(composite_products_map_raw, Exception):
            logger.error(f"Composite products fetch failed: {composite_products_map_raw}")
            composite_products_map_raw = {}
        if isinstance(fba_stock_by_sku, Exception):
            logger.error(f"FBA stock fetch failed: {fba_stock_by_sku}")
            fba_stock_by_sku = {}
        if isinstance(transfer_orders_by_sku, Exception):
            logger.error(f"Transfer orders fetch failed: {transfer_orders_by_sku}")
            transfer_orders_by_sku = {}
        if isinstance(missed_sales_by_sku, Exception):
            logger.error(f"Missed sales fetch failed: {missed_sales_by_sku}")
            missed_sales_by_sku = {}
        if isinstance(vc_by_sku, Exception):
            logger.error(f"Vendor Central fetch failed: {vc_by_sku}")
            vc_by_sku = {}
        vc_latest_inv_date = vc_by_sku.pop("__latest_inv_date__", "")

        # Preserve current-period missed sales before any lookback overrides
        current_period_missed_sales_by_sku = dict(missed_sales_by_sku)

        # Placeholders — filled after enrichment in a separate parallel step
        latest_zoho_by_sku: Dict[str, float] = {}
        latest_fba_by_sku: Dict[str, float] = {}
        latest_zoho_date: str = ""
        latest_fba_date: str = ""
        prev_zoho_by_sku: Dict[str, float] = {}
        prev_zoho_date: str = ""
        prev_fba_by_sku: Dict[str, float] = {}
        prev_fba_date: str = ""

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
            # Only do lookback and highlighting for date ranges >= 90 days.
            # dashboard_mode skips lookback entirely (saves 20-40s for the KPI endpoint).
            skus_needing_lookback_list = []
            if period_days >= 90 and not dashboard_mode:
                for item in combined_data:
                    days = item.get("combined_metrics", {}).get("total_days_in_stock", 0)
                    sku = item.get("sku_code", "")
                    if days < 60 and sku:
                        skus_needing_lookback_list.append(sku)

            combined_sku_codes = {item.get("sku_code", "") for item in combined_data if item.get("sku_code")}
            new_skus = combined_sku_codes - all_sku_codes

            # Fire off ALL independent DB queries in parallel:
            # 1. brand_logistics (for enrichment)
            # 2. transit_data (for enrichment)
            # 3. po_unit_prices (latest purchase order rate per SKU)
            # 4. sku_to_item_id (for DRR lookback, if needed)
            # 5. extra_product_data (if new SKUs from composite expansion)
            parallel_tasks = [
                report_service.get_brand_logistics(),
                report_service.get_stock_in_transit(),
                report_service.fetch_latest_po_unit_prices(combined_sku_codes),
            ]
            if skus_needing_lookback_list:
                parallel_tasks.append(report_service.fetch_sku_to_item_id_map(set(skus_needing_lookback_list)))
            if new_skus:
                parallel_tasks.append(report_service.batch_load_all_product_data(new_skus))

            parallel_results = await asyncio.gather(*parallel_tasks, return_exceptions=True)

            brand_logistics = parallel_results[0] if not isinstance(parallel_results[0], Exception) else {}
            transit_data = parallel_results[1] if not isinstance(parallel_results[1], Exception) else {}
            po_unit_prices = parallel_results[2] if not isinstance(parallel_results[2], Exception) else {}
            idx = 3
            sku_to_item_id = {}
            if skus_needing_lookback_list:
                sku_to_item_id = parallel_results[idx] if not isinstance(parallel_results[idx], Exception) else {}
                idx += 1
            if new_skus and idx < len(parallel_results) and not isinstance(parallel_results[idx], Exception):
                all_product_data.update(parallel_results[idx])

            # Start past-90d DRR fetch as a background task so it runs concurrently
            # with the DRR lookback below. all_product_data is now fully populated.
            _full_sku_to_item_id = {
                sku: pdata["item_id"]
                for sku, pdata in all_product_data.items()
                if pdata.get("item_id")
            }
            _past_drr_task = (
                asyncio.create_task(report_service.fetch_past_90d_drr(_full_sku_to_item_id, start_date))
                if _full_sku_to_item_id else None
            )

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
                                    item["drr_lookback_days_in_stock"] = lb["days_in_stock"]
                                    item["drr_lookback_sales"] = lb["units_sold"]
                                    item["highlight"] = "yellow"
                                    # If current-period sales exceed lookback sales, flag for
                                    # demand-based order qty override (order qty = current sales)
                                    current_net_sales = metrics.get("total_sales", 0)
                                    if lb["units_sold"] < current_net_sales:
                                        item["order_qty_demand_override"] = True
                                        item["current_period_sales_for_override"] = current_net_sales
                                else:
                                    item["drr_source"] = "insufficient_stock"
                                    item["drr_lookback_period"] = ""
                                    item["drr_lookback_days_in_stock"] = 0
                                    item["drr_lookback_sales"] = 0
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

                        # For SKUs using lookback DRR, fetch missed_sales from the same
                        # lookback date range so missed_sales_drr is consistent with DRR.
                        lookback_period_to_skus: Dict[str, List[str]] = {}
                        for item in combined_data:
                            if item.get("drr_source") == "previous_period":
                                period_str = item.get("drr_lookback_period", "")
                                if period_str and " to " in period_str:
                                    lookback_period_to_skus.setdefault(period_str, []).append(
                                        item.get("sku_code", "")
                                    )

                        if lookback_period_to_skus:
                            unique_lb_periods = list(lookback_period_to_skus.keys())
                            lb_missed_results = await asyncio.gather(
                                *[
                                    report_service.fetch_missed_sales(
                                        p.split(" to ")[0].strip(), p.split(" to ")[1].strip()
                                    )
                                    for p in unique_lb_periods
                                ],
                                return_exceptions=True,
                            )
                            for i, period_str in enumerate(unique_lb_periods):
                                result = lb_missed_results[i]
                                if isinstance(result, Exception):
                                    logger.error(
                                        f"Missed sales lookback fetch failed for {period_str}: {result}"
                                    )
                                    continue
                                for sku in lookback_period_to_skus[period_str]:
                                    missed_sales_by_sku[sku] = result.get(sku, 0.0)
                            logger.info(
                                f"Updated missed_sales for "
                                f"{sum(len(v) for v in lookback_period_to_skus.values())} "
                                f"lookback SKUs across {len(unique_lb_periods)} periods"
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
            # Initialise here so they are always defined even if the try-block below fails.
            product_rates = {}
            product_brands = {}
            logistics_data = {}
            try:
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
                        "purchase_status": pdata.get("purchase_status", ""),
                        "stock_in_transit_1": pdata.get("stock_in_transit_1", 0) or 0,
                        "stock_in_transit_2": pdata.get("stock_in_transit_2", 0) or 0,
                        "stock_in_transit_3": pdata.get("stock_in_transit_3", 0) or 0,
                        "is_new": pdata.get("is_new", False),
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

            except Exception as e:
                logger.error(f"Error enriching data: {e}")
                errors.append(f"Enrichment error: {str(e)}")

            # Await the latest-stock task that was started at the very beginning of this
            # function.  By now it has been running concurrently with all report processing,
            # so it should be close to (or already) complete.
            try:
                lz_res, lf_res = await asyncio.wait_for(
                    asyncio.gather(_zoho_stock_task, _fba_stock_task, return_exceptions=True),
                    timeout=45.0,
                )
                if not isinstance(lz_res, Exception):
                    latest_zoho_by_sku = lz_res.get("by_sku", {})
                    latest_zoho_date = lz_res.get("latest_date") or ""
                    prev_zoho_by_sku = lz_res.get("prev_by_sku", {})
                    prev_zoho_date = lz_res.get("prev_date") or ""
                else:
                    logger.error(f"Latest Zoho WH stock fetch failed: {lz_res}")
                if not isinstance(lf_res, Exception):
                    latest_fba_by_sku = lf_res.get("by_sku", {})
                    latest_fba_date = lf_res.get("latest_date") or ""
                    prev_fba_by_sku = lf_res.get("prev_by_sku", {})
                    prev_fba_date = lf_res.get("prev_date") or ""
                else:
                    logger.error(f"Latest FBA stock fetch failed: {lf_res}")
            except asyncio.TimeoutError:
                logger.warning("Latest stock fetch timed out — latest stock columns will be empty")
                _zoho_stock_task.cancel()
                _fba_stock_task.cancel()

            # Inject stub rows for SKUs that have FBA stock but zero sales in this period.
            # These ASINs exist in amazon_sku_mapping but never appeared in any sales report,
            # so combine_data_by_sku_optimized never created an entry for them.
            if latest_fba_by_sku:
                existing_skus = {item.get("sku_code") for item in combined_data if item.get("sku_code")}
                fba_only_skus = {sku for sku in latest_fba_by_sku if sku not in existing_skus}
                if fba_only_skus:
                    fba_only_product_data = await report_service.batch_load_all_product_data(fba_only_skus)
                    for sku in sorted(fba_only_skus):
                        pdata = fba_only_product_data.get(sku, {})
                        # Respect brand filter if active
                        stub_brand = pdata.get("brand", "") or ""
                        if brand and stub_brand.lower() != brand.lower():
                            continue
                        name = pdata.get("name") or "Unknown Item"
                        fba_end_stock = round(fba_stock_by_sku.get(sku, 0), 2)
                        # Register stub in product_brands so classify_movement can group it correctly,
                        # and in logistics_data so enrich_with_order_calculations picks up its metadata.
                        product_brands[sku] = stub_brand
                        logistics_data[sku] = {
                            "cbm": pdata.get("cbm", 0) or 0,
                            "case_pack": pdata.get("case_pack", 0) or 0,
                            "purchase_status": pdata.get("purchase_status", ""),
                            "stock_in_transit_1": 0,
                            "stock_in_transit_2": 0,
                            "stock_in_transit_3": 0,
                            "is_new": pdata.get("is_new", False),
                        }
                        stub = {
                            "sku_code": sku,
                            "item_name": name,
                            "sources": [],
                            "combined_metrics": {
                                "total_units_sold": 0.0,
                                "total_units_returned": 0.0,
                                "total_credit_notes": 0.0,
                                "total_amount": 0.0,
                                "total_closing_stock": fba_end_stock,
                                "total_days_in_stock": 0.0,
                                "avg_daily_run_rate": 0.0,
                                "avg_days_of_coverage": 0.0,
                                "fba_closing_stock": fba_end_stock,
                                "pupscribe_wh_stock": 0.0,
                                "transfer_orders": 0.0,
                                "total_sales": 0.0,
                            },
                            "in_stock": True,
                            "drr_source": "current_period",
                            "drr_lookback_period": "",
                            "highlight": None,
                        }
                        combined_data.append(stub)
                    logger.info(f"Injected {len(fba_only_skus)} FBA-only stub items into combined_data")

            # Classify movement and compute order metrics now that all items — including
            # FBA-only stubs — have been added.  Running here ensures every product is
            # ranked within its own brand group rather than across all brands.
            try:
                combined_data = report_service.classify_movement(combined_data, brand_logistics, product_brands, logistics_data)
                combined_data = report_service.enrich_with_order_calculations(
                    combined_data, transit_data, logistics_data, missed_sales_by_sku, period_days,
                    current_period_missed_sales_by_sku,
                )
                logger.info(f"Enriched {len(combined_data)} items with movement and order calculations")
            except Exception as e:
                logger.error(f"Error in movement classification and order calculations: {e}")
                errors.append(f"Movement/order enrichment error: {str(e)}")

            # Attach latest current stock, PO unit prices, manufacturer code, and Etrade VC data
            for item in combined_data:
                sku = item.get("sku_code", "")
                lz = round(latest_zoho_by_sku.get(sku, 0), 2)
                lf = round(latest_fba_by_sku.get(sku, 0), 2)
                item["latest_zoho_stock"] = lz
                item["latest_fba_stock"] = lf
                item["latest_total_stock"] = round(lz + lf, 2)
                item["prev_zoho_stock"] = round(prev_zoho_by_sku.get(sku, 0), 2)
                item["prev_fba_stock"] = round(prev_fba_by_sku.get(sku, 0), 2)
                po_price = po_unit_prices.get(sku, {})
                item["unit_price"] = po_price.get("rate", 0)
                item["unit_price_currency"] = po_price.get("currency_code", "")
                item["manufacturer_code"] = all_product_data.get(sku, {}).get("manufacturer_code", "")
                item["mrp"] = product_rates.get(sku)
                item["brand"] = product_brands.get(sku, "")
                # Etrade (Vendor Central) inventory
                vc_data = vc_by_sku.get(sku, {})
                vc_stock = round(vc_data.get("closing_stock", 0), 2)
                vc_drr = vc_data.get("drr", 0)
                metrics_ref = item.get("combined_metrics", {})
                metrics_ref["etrade_inventory"] = vc_stock
                metrics_ref["etrade_drr"] = round(vc_drr, 4)
                metrics_ref["etrade_days_inventory_lasts"] = round(vc_stock / vc_drr, 2) if vc_drr > 0 else 0.0

            # Recalculate days-coverage columns and all downstream order calculations
            # using latest DB stock instead of end-of-period closing stock.
            for item in combined_data:
                metrics = item.get("combined_metrics", {})
                drr = metrics.get("avg_daily_run_rate", 0)
                latest_stock = item.get("latest_total_stock", 0)
                total_transit = item.get("total_stock_in_transit", 0)
                purchase_status = item.get("purchase_status", "")

                # Avg Days of Coverage and On-Hand Days Coverage
                if drr > 0:
                    avg_doc = round(latest_stock / drr, 2)
                else:
                    avg_doc = 0.0
                metrics["avg_days_of_coverage"] = avg_doc
                item["on_hand_days_coverage"] = avg_doc

                # Current Days Coverage
                if drr > 0:
                    current_days_coverage = round((latest_stock + total_transit) / drr, 2)
                else:
                    current_days_coverage = 0.0
                item["current_days_coverage"] = current_days_coverage

                # Refresh Excess / Order flag
                target_days = item.get("target_days", 0)
                if drr == 0:
                    item["excess_or_order"] = "NO MOVEMENT"
                elif current_days_coverage < target_days:
                    item["excess_or_order"] = "ORDER"
                else:
                    item["excess_or_order"] = "EXCESS"

                # Inactive / discontinued: only days_total_inventory_lasts needs updating
                if purchase_status in ("inactive", "discontinued until stock lasts"):
                    item["days_total_inventory_lasts"] = round(current_days_coverage, 2)
                    continue

                # Order quantity
                extra_qty = item.get("extra_qty", 0)
                lead_time_val = item.get("lead_time", 60)
                if current_days_coverage < lead_time_val:
                    net_target_days = target_days
                else:
                    net_target_days = target_days - current_days_coverage
                item["net_target_days"] = round(net_target_days, 2)
                confidence_multiplier = item.get("confidence_multiplier", 1.0)
                if item.get("order_qty_demand_override"):
                    order_qty = item.get("current_period_sales_for_override", 0) * confidence_multiplier
                else:
                    order_qty = max(0, net_target_days * drr) * confidence_multiplier
                item["order_qty"] = round(order_qty, 2)
                item["order_qty_plus_extra_qty"] = round(order_qty + extra_qty, 2)

                case_pack = item.get("case_pack", 0)
                order_qty_plus_extra_qty = item["order_qty_plus_extra_qty"]
                if case_pack > 0:
                    item["order_qty_plus_extra_qty_rounded"] = math.floor(order_qty_plus_extra_qty / case_pack) * case_pack
                else:
                    item["order_qty_plus_extra_qty_rounded"] = round(order_qty_plus_extra_qty, 0)

                order_qty_rounded = item["order_qty_plus_extra_qty_rounded"]

                cbm = item.get("cbm", 0)
                if case_pack > 0 and cbm > 0:
                    item["total_cbm"] = round((order_qty_rounded / case_pack) * cbm, 4)
                else:
                    item["total_cbm"] = 0

                if drr > 0:
                    item["days_current_order_lasts"] = round(order_qty_rounded / drr, 2)
                else:
                    item["days_current_order_lasts"] = 0

                item["days_total_inventory_lasts"] = round(
                    current_days_coverage + item["days_current_order_lasts"], 2
                )

            # Await past 90d DRR and compute return_pct + growth_rate for every item
            past_drr_by_sku: Dict[str, float] = {}
            if _past_drr_task is not None:
                try:
                    past_drr_by_sku = await asyncio.wait_for(_past_drr_task, timeout=30.0)
                except Exception as e:
                    logger.warning(f"Past 90d DRR fetch failed or timed out: {e}")
                    _past_drr_task.cancel()

            for item in combined_data:
                metrics = item.get("combined_metrics", {})
                total_sold = metrics.get("total_units_sold", 0)
                total_returned = metrics.get("total_units_returned", 0)
                item["return_pct"] = round((total_returned / total_sold) * 100, 2) if total_sold > 0 else 0.0

                current_drr = metrics.get("avg_daily_run_rate", 0)
                past_drr = past_drr_by_sku.get(item.get("sku_code", ""), 0)
                # Lookback items have avg_daily_run_rate overwritten with a historical DRR,
                # so comparing it against past_drr (90d before start_date) is directionally
                # meaningless. Leave growth_rate blank for those items.
                if item.get("drr_source") == "previous_period":
                    item["growth_rate"] = None
                elif past_drr and past_drr > 0:
                    item["growth_rate"] = round(((current_drr - past_drr) / past_drr) * 100, 2)
                else:
                    item["growth_rate"] = None

        # If combined_data was empty the if-block above was skipped and the background
        # tasks were never awaited — cancel them to avoid dangling tasks.
        if not combined_data:
            if not _zoho_stock_task.done():
                _zoho_stock_task.cancel()
            if not _fba_stock_task.done():
                _fba_stock_task.cancel()

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
                "total_transfer_orders": round(
                    summary_stats["total_transfer_orders"], 2
                ),
                "total_net_units_sold": round(
                    summary_stats["total_units_sold"] - summary_stats["total_credit_notes"] - summary_stats["total_transfer_orders"], 2
                ),
                "total_amount": round(summary_stats["total_amount"], 2),
                "total_closing_stock": round(
                    summary_stats["total_closing_stock"], 2
                ),
                "avg_drr": summary_stats["avg_drr"],
                "total_total_sales": round(
                    summary_stats["total_total_sales"], 2
                ),
                "sources_included": list(individual_reports.keys()),
                "source_record_counts": source_counts,
            },
            "individual_reports": individual_reports,
            "combined_data": combined_data,
            "errors": errors,
            "latest_stock_dates": {
                "zoho": latest_zoho_date,
                "prev_zoho": prev_zoho_date,
                "fba": latest_fba_date,
                "prev_fba": prev_fba_date,
                "etrade": vc_latest_inv_date,
            },
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
    brand: str = Query(None, description="Filter by brand name (e.g. 'Truelove'). Omit for all brands."),
    db=Depends(get_database),
):
    """
    Generate the master inventory & sales report for a given date range.

    ## Processing Pipeline

    ### 1. Parallel Data Fetching
    All data sources are fetched concurrently to minimise latency:
    - **Zoho** – the **sole source of sales data**. Covers invoices and credit notes
      from the Pupscribe Warehouse for the period. Blinkit and Amazon sales are
      intentionally excluded; all sales are tracked through Zoho.
    - **Amazon FBA closing stock** – fetched from `amazon_ledger` for inventory
      purposes only (not sales). Represents FBA inventory as of `end_date`.
    - **Composite products map** – bundle-to-component expansion rules
    - **Transfer orders** – inter-warehouse transfers for the period (excluded from net sales)
    - **Missed sales** – unfulfilled demand logged for the period

    ### 2. SKU & Product Data Resolution
    All SKU codes extracted from Zoho and FBA stock are batch-loaded from the
    products collection in a single query — fetching name, rate, brand, CBM,
    case pack, purchase status, and stock-in-transit fields.

    ### 3. Per-Source Normalisation
    Zoho records are converted to a canonical structure
    (`{sku_code, units_sold, units_returned, amount, days_in_stock, closing_stock, …}`).
    Zoho separates invoice units from credit-note units explicitly.

    ### 4. Data Combination & Composite Expansion
    Normalised records from all sources are merged by `sku_code`.
    Composite / bundle SKUs are exploded into their components using the
    composite products map; each component's quantities are scaled by its
    `quantity` multiplier. Combined metrics computed per SKU:
    - `total_units_sold`, `total_credit_notes`, `total_amount`
    - `total_closing_stock` = Zoho Pupscribe Warehouse stock + FBA stock
    - `avg_daily_run_rate` (DRR) = net units sold ÷ days in stock (or period days if always in stock)
    - `total_sales` = units_sold − credit_notes − transfer_orders

    ### 5. DRR Lookback (≥ 90-day reports only)
    For SKUs with fewer than 60 days in stock during the current period
    (insufficient data for a reliable DRR), the system looks back up to 6
    previous 90-day windows to find a period where the SKU had ≥ 90 days
    in stock. If found:
    - DRR is replaced with the historical value (`drr_source = "previous_period"`, `highlight = "yellow"`)
    - Missed sales are also re-fetched from that same lookback window for consistency
    - **Demand override (green):** if current-period net sales exceed lookback-period sales,
      `order_qty` is set to current-period sales, `extra_qty` uses current-period missed sales,
      and `order_qty_demand_override = True` is set on the item (`highlight` remains `"yellow"` in
      the JSON but the row is coloured **green** in the Excel download to distinguish it from
      plain yellow lookback rows)
    - If no valid lookback period is found, the SKU is marked `drr_source = "insufficient_stock"`
      (`highlight = "red"`)

    ### 6. Brand Logistics Enrichment
    Lead time, safety days, and movement-class thresholds are loaded per brand
    from the `brand_logistics` collection and applied to each SKU.
    Open purchase orders are queried to derive stock-in-transit values
    (used as a fallback when the product record doesn't have manual SIT values).

    ### 7. Movement Classification
    Within each brand group, SKUs are ranked by volume and revenue percentiles:
    - **Fast Mover** – top tier by both volume and revenue
    - **Medium Mover** – mid tier
    - **Slow Mover** – bottom tier or zero movement

    ### 8. Order Quantity Calculations
    For each SKU (skipped for inactive / discontinued items):
    - `target_days` = lead_time + safety_days + order_processing (10 days fixed)
    - `current_days_coverage` = (closing_stock + stock_in_transit) ÷ DRR
    - `net_target_days` = target_days − current_days_coverage (floored at target_days if already under lead time)
    - `order_qty` = max(0, net_target_days × DRR)
    - `extra_qty` = missed_sales_drr × lead_time (capped at 50 % of DRR to prevent spikes)
    - `order_qty_plus_extra_qty_rounded` = rounded down to nearest case pack multiple
    - `total_cbm` = (rounded_qty ÷ case_pack) × CBM per case

    ### 9. Latest Stock Injection
    Real-time stock (latest record in DB, regardless of report period) is attached
    to each SKU (`latest_zoho_stock`, `latest_fba_stock`, `latest_total_stock`).
    All coverage and order calculations are then **recomputed** using this latest
    stock so recommendations reflect current inventory, not end-of-period snapshot.

    ### 10. FBA-Only Stub Rows
    SKUs that have FBA stock as of `end_date` but zero sales in the period are
    injected as stub rows so they appear in the report with their stock and
    order-coverage data even though no sales were recorded.

    ### 11. Growth Rate & Return %
    Calculated using a background-fetched 90-day trailing DRR compared against
    the report-period DRR. Return % = total_units_returned ÷ total_units_sold.

    ## Response
    Returns a JSON object with:
    - **`combined_data`** – array of enriched SKU objects (see field list below)
    - **`summary`** – totals/averages across all SKUs
    - **`individual_reports`** – per-source metadata (success, record count, errors)
    - **`latest_stock_dates`** – date of the most recent stock record in DB for Zoho and FBA
    - **`meta`** – execution time and timestamp

    ## Key Fields on Each SKU in `combined_data`
    | Field | Description |
    |---|---|
    | `sku_code` | Internal SKU identifier |
    | `avg_daily_run_rate` | Units sold per day (DRR) |
    | `drr_source` | `current_period` / `previous_period` / `insufficient_stock` |
    | `drr_lookback_period` | Date range used for lookback DRR (if applicable) |
    | `drr_lookback_days_in_stock` | Days in stock during the lookback period (if applicable) |
    | `drr_lookback_sales` | Units sold during the lookback period (if applicable) |
    | `highlight` | Row highlight hint: `null` (normal) / `"yellow"` (lookback DRR) / `"red"` (insufficient stock) |
    | `order_qty_demand_override` | `true` when current-period net sales exceed lookback sales — order qty is overridden to current-period sales; these rows are coloured **green** in the Excel download |
    | `current_period_sales_for_override` | Net sales used as order qty when demand override is active |
    | `total_closing_stock` | Zoho Pupscribe Warehouse stock + FBA stock as of `end_date` |
    | `latest_total_stock` | Zoho Pupscribe Warehouse stock + FBA stock as of latest DB record |
    | `current_days_coverage` | Days of inventory on hand at current DRR |
    | `order_qty_plus_extra_qty_rounded` | Final suggested order quantity (case-pack rounded) |
    | `missed_sales` | Unfulfilled demand units for the period |
    | `movement` | `Fast Mover` / `Medium Mover` / `Slow Mover` |
    | `excess_or_order` | `ORDER` / `EXCESS` / `NO MOVEMENT` |
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
    brand: str = Query(None, description="Filter by brand name (e.g. 'Truelove'). Omit for all brands."),
    db=Depends(get_database),
):
    """
    Download the master report as an Excel (.xlsx) file.

    Runs the exact same processing pipeline as `GET /master-report` and writes
    the result to a multi-sheet Excel workbook streamed back as an attachment.

    > **Note on data sources:** Sales data comes exclusively from **Zoho** (Pupscribe
    > Warehouse invoices and credit notes). Blinkit and Amazon sales are not included.
    > Amazon FBA is used only for closing stock / inventory figures.

    ## Excel Workbook Structure

    ### Master Sheet (`{brand} {start} to {end}` or `Master {start} to {end}`)
    One row per SKU. Columns are grouped as follows:

    **Identity**
    - `Purchase Status` – active / inactive / discontinued until stock lasts
    - `Is New` – Yes if the product was created within the report window
    - `SKU Code`, `Item Name`

    **Sales Metrics**
    - `Total Amount` – gross revenue for the period
    - `Total Units Sold` – gross units sold across all channels
    - `Total Units Returned` – customer returns
    - `Transfer Orders` – inter-warehouse transfers (excluded from net sales)
    - `Net Total Sales` – units_sold − returns − transfer_orders
    - `Return %`

    **DRR & Stock Quality**
    - `Days in Stock` – days the SKU was in stock during the period
    - `Lookback Days in Stock` – days in stock during the lookback period (if used)
    - `Lookback Sales` – units sold during the lookback period (if used)
    - `Avg Daily Run Rate` – DRR used for order calculations
    - `Growth Rate (%)` – change vs. trailing 90-day DRR
    - `DRR Source` – `current_period` / `previous_period` / `insufficient_stock`
    - `DRR Lookback Period` – date range of the lookback window (e.g. `2025-09-01 to 2025-12-01`)

    **Stock Snapshot (period end)**
    - `Total Stock ({end_date})` – Zoho Pupscribe Warehouse stock + FBA stock as of `end_date`
    - `Pupscribe WH Stock ({prev_zoho_date})` – Zoho Pupscribe Warehouse stock from the snapshot one day before the latest DB record
    - `FBA Stock ({end_date})`

    **Stock Snapshot (latest DB record)**
    - `Total Stock ({latest_date})` – Zoho Pupscribe Warehouse stock + FBA stock (most recent record in DB, used for order calculations)
    - `Pupscribe WH Stock ({latest_zoho_date})` – Zoho Pupscribe Warehouse stock as of latest DB record
    - `FBA Stock ({latest_fba_date})`

    **Inventory Status**
    - `In Stock` – Yes / No as of the latest record
    - `Movement` – Fast / Medium / Slow Mover (classified within brand group)

    **Order Parameters**
    - `Safety Days`, `Lead Time`, `Order Processing` (fixed 10 days), `Target Days`
    - `On-Hand Days Coverage` – days of stock based on period-end stock
    - `Stock in Transit 1 / 2 / 3`, `Total Stock in Transit`
    - `Current Days Coverage` – (latest stock + transit) ÷ DRR

    **Missed Sales**
    - `Missed Sales` – unfulfilled demand units for the period (or lookback period if demand-override)
    - `Missed Sales DRR` – missed units ÷ period days (capped at 50 % of DRR)
    - `Extra Qty` – additional buffer = missed_sales_drr × lead_time

    **Order Quantities**
    - `Net Target Days` – target_days − current_days_coverage
    - `Excess / Order` – ORDER / EXCESS / NO MOVEMENT
    - `Order Qty` – net_target_days × DRR (or current-period sales for demand-override rows)
    - `Order Qty + Extra Qty` – order_qty + extra_qty
    - `CBM`, `Case Pack`
    - `Order Qty + Extra Qty (Rounded)` – floored to nearest case pack multiple (row highlighted green when demand override was applied)
    - `Total CBM` – (rounded_qty ÷ case_pack) × CBM per case
    - `Days Current Order Lasts` – rounded_qty ÷ DRR
    - `Days Total Inventory Lasts` – current_days_coverage + days_current_order_lasts

    ## Conditional Row Formatting
    | Colour | Condition | Meaning |
    |---|---|---|
    | Green | `order_qty_demand_override = true` (takes precedence over yellow) | Lookback DRR is used **and** current-period net sales exceed lookback-period sales — order qty is overridden to current-period net sales (`order_qty_basis = "Current Period Sales"`) |
    | Yellow | `drr_source = "previous_period"` and no demand override | DRR is sourced from a previous lookback period due to insufficient stock days (< 60) in the current period |
    | Red | `drr_source = "insufficient_stock"` | Fewer than 60 days in stock in both the current period and all 6 lookback windows — DRR cannot be reliably estimated |
    | (no fill) | `drr_source = "current_period"` | DRR uses the current-period data (≥ 60 days in stock) |

    ## Excel Formulas
    Several columns are written as live Excel formulas rather than static values
    so the sheet remains interactive:
    - `Missed Sales DRR` = Missed Sales ÷ period days
    - `Extra Qty` = Missed Sales DRR × Lead Time
    - `Net Target Days` = Target Days − Current Days Coverage
    - `Order Qty` = max(0, Net Target Days × Avg DRR)
    - `Order Qty + Extra Qty` = Order Qty + Extra Qty
    - `Order Qty + Extra Qty (Rounded)` = FLOOR to case pack
    - `Total CBM` = (Rounded Qty ÷ Case Pack) × CBM
    - `Days Current Order Lasts` = Rounded Qty ÷ DRR
    - `Days Total Inventory Lasts` = Current Days Coverage + Days Current Order Lasts

    ## Response
    Returns a binary `.xlsx` file as a streaming attachment.
    Filename format: `master_report_{start_date}_to_{end_date}_{YYYYMMDD_HHMMSS}.xlsx`
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

        if not combined_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data found to download",
            )

        # Build a readable date-range sheet name (Excel max 31 chars) and stock column labels
        try:
            _sd = datetime.strptime(start_date, "%Y-%m-%d")
            _ed = datetime.strptime(end_date, "%Y-%m-%d")
            if _sd.year == _ed.year:
                _sheet_name = f"{_sd.strftime('%d %b')} - {_ed.strftime('%d %b %Y')}"
            else:
                _sheet_name = f"{_sd.strftime('%d %b %Y')} - {_ed.strftime('%d %b %Y')}"
            _sheet_name = _sheet_name[:31]
            _end_date_label = _ed.strftime("%d %b %Y")
        except Exception:
            _sheet_name = "Master Sheet"
            _end_date_label = end_date

        # Latest stock column labels from the actual latest dates in the DB
        latest_stock_dates = content.get("latest_stock_dates", {})
        def _fmt_date(d: str) -> str:
            try:
                return datetime.strptime(d, "%Y-%m-%d").strftime("%d %b %Y")
            except Exception:
                return d or "Latest"
        _latest_zoho_label = _fmt_date(latest_stock_dates.get("zoho", ""))
        _prev_zoho_label   = _fmt_date(latest_stock_dates.get("prev_zoho", ""))
        _latest_fba_label  = _fmt_date(latest_stock_dates.get("fba", ""))
        _prev_fba_label    = _fmt_date(latest_stock_dates.get("prev_fba", ""))
        # For total, use the more recent of the two
        _latest_total_label = _latest_fba_label if latest_stock_dates.get("fba", "") >= latest_stock_dates.get("zoho", "") else _latest_zoho_label
        _prev_total_label   = _prev_fba_label if latest_stock_dates.get("prev_fba", "") >= latest_stock_dates.get("prev_zoho", "") else _prev_zoho_label
        _etrade_inv_label = _fmt_date(latest_stock_dates.get("etrade", "")) or _end_date_label

        # Create Excel file
        excel_buffer = io.BytesIO()

        # Track which Excel data rows (0-based) are demand-override (green) rows
        demand_override_row_indices: set = set()

        # currency_code → Excel number format using symbol prefix (shared by master + draft sheets)
        _CURRENCY_FORMATS: Dict[str, str] = {
            "USD": '$#,##0.00',
            "EUR": '€#,##0.00',
            "GBP": '£#,##0.00',
            "CNY": '¥#,##0.00',
            "INR": '₹#,##0.00',
        }
        _DEFAULT_NUMBER_FMT = "#,##0.00"

        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            # Master Sheet
            combined_df_data = []
            _master_unit_price_currencies: list = []
            for item in combined_data:
                if not isinstance(item, dict):
                    continue
                if item.get("order_qty_demand_override"):
                    demand_override_row_indices.add(len(combined_df_data))

                metrics = item.get("combined_metrics", {})
                _master_unit_price_currencies.append(item.get("unit_price_currency", "") or "")
                combined_df_data.append(
                    {
                        "Purchase Status": item.get("purchase_status", ""),
                        "Is New": "Yes" if item.get("is_new", False) else "No",
                        "SKU Code": item.get("sku_code", ""),
                        "Brand": item.get("brand", ""),
                        "Item Name": item.get("item_name", ""),
                        "MRP": item.get("mrp") or 0,
                        "Unit Price": item.get("unit_price", 0),
                        "Total Amount": f"₹{metrics.get('total_amount', 0)}",
                        "Total Units Sold": metrics.get("total_units_sold", 0),
                        "Total Units Returned": metrics.get("total_units_returned", 0),
                        "Transfer Orders": metrics.get("transfer_orders", 0),
                        "Net Total Sales": metrics.get("total_sales", 0),
                        "Return %": item.get("return_pct", 0),
                        "Days in Stock": metrics.get("total_days_in_stock", 0),
                        "Lookback Days in Stock": item.get("drr_lookback_days_in_stock", 0),
                        "Lookback Sales": item.get("drr_lookback_sales", 0),
                        "Avg Daily Run Rate": metrics.get("avg_daily_run_rate", 0),
                        "Growth Rate (%)": item.get("growth_rate"),
                        "DRR Source": item.get("drr_source", "current_period"),
                        "DRR Lookback Period": item.get("drr_lookback_period", ""),
                        f"Pupscribe WH Stock ({_prev_zoho_label})": item.get("prev_zoho_stock", 0),
                        f"FBA Stock ({_prev_fba_label})": item.get("prev_fba_stock", 0),
                        f"Total Stock ({_prev_total_label})": 0,
                        f"Pupscribe WH Stock ({_latest_zoho_label})": item.get("latest_zoho_stock", 0),
                        f"FBA Stock ({_latest_fba_label})": item.get("latest_fba_stock", 0),
                        f"Total Stock ({_latest_total_label})": item.get("latest_total_stock", 0),
                        "In Stock": "Yes" if item.get("in_stock", False) else "No",
                        "Movement": item.get("movement", ""),
                        "Safety Days": item.get("safety_days", 0),
                        "Lead Time": item.get("lead_time", 0),
                        "Order Processing": item.get("order_processing", 10),
                        "Target Days": item.get("target_days", 0),
                        "On-Hand Days Coverage": item.get("on_hand_days_coverage", 0),
                        "Stock in Transit 1": item.get("stock_in_transit_1", 0),
                        "Stock in Transit 2": item.get("stock_in_transit_2", 0),
                        "Stock in Transit 3": item.get("stock_in_transit_3", 0),
                        "Total Stock in Transit": item.get("total_stock_in_transit", 0),
                        "Current Days Coverage": item.get("current_days_coverage", 0),
                        "Missed Sales": item.get("missed_sales", 0),
                        "Missed Sales DRR": item.get("missed_sales_drr", 0),
                        "Extra Qty": item.get("extra_qty", 0),
                        "Net Target Days": item.get("net_target_days", 0),
                        "Confidence Multiplier": item.get("confidence_multiplier", 1.0),
                        "Excess / Order": item.get("excess_or_order", ""),
                        "Order Qty": item.get("order_qty", 0),
                        "Order Qty Basis": item.get("order_qty_basis", ""),
                        "Order Qty + Extra Qty": item.get("order_qty_plus_extra_qty", 0),
                        "CBM": item.get("cbm", 0),
                        "Case Pack": item.get("case_pack", 0),
                        "Order Qty + Extra Qty (Rounded)": item.get("order_qty_plus_extra_qty_rounded", 0),
                        "Total CBM": item.get("total_cbm", 0),
                        "Days Current Order Lasts": item.get("days_current_order_lasts", 0),
                        "Days Total Inventory Lasts": item.get("days_total_inventory_lasts", 0),
                        f"Etrade Inventory ({_etrade_inv_label})": metrics.get("etrade_inventory", 0),
                        "Etrade DRR": metrics.get("etrade_drr", 0),
                        "Days Total Inventory Lasts (Etrade)": 0,
                        "Total MRP": 0,
                        "Collection Value": 0,
                    }
                )

            if combined_df_data:
                combined_df = pd.DataFrame(combined_df_data)
                combined_df.to_excel(writer, sheet_name=_sheet_name, index=False)

                # period_days for Missed Sales DRR formula
                try:
                    _period_days = (_ed - _sd).days + 1
                except Exception:
                    _period_days = 30

                # Inject Excel formulas for all computed columns
                from openpyxl.utils import get_column_letter
                from openpyxl.styles import PatternFill

                ws = writer.sheets[_sheet_name]
                cols_list = list(combined_df.columns)

                def _col(name):
                    return get_column_letter(cols_list.index(name) + 1)

                _A  = _col("Purchase Status")
                _brand_col = _col("Brand")
                _mrp_col        = _col("MRP")
                _total_mrp_col  = _col("Total MRP")
                _cv_col         = _col("Collection Value")
                _F  = _col("Total Units Sold")
                _G  = _col("Total Units Returned")
                _I  = _col("Net Total Sales")
                _J  = _col("Return %")
                _N  = _col("Avg Daily Run Rate")
                _S  = _col(f"Pupscribe WH Stock ({_prev_zoho_label})")
                _T  = _col(f"FBA Stock ({_prev_fba_label})")
                _R  = _col(f"Total Stock ({_prev_total_label})")
                _V  = _col(f"Pupscribe WH Stock ({_latest_zoho_label})")
                _W  = _col(f"FBA Stock ({_latest_fba_label})")
                _U  = _col(f"Total Stock ({_latest_total_label})")
                _AA = _col("Safety Days")
                _AB = _col("Lead Time")
                _AC = _col("Order Processing")
                _AD = _col("Target Days")
                _AE = _col("On-Hand Days Coverage")
                _AF = _col("Stock in Transit 1")
                _AG = _col("Stock in Transit 2")
                _AH = _col("Stock in Transit 3")
                _AI = _col("Total Stock in Transit")
                _AJ = _col("Current Days Coverage")
                _AK = _col("Missed Sales")
                _AL = _col("Missed Sales DRR")
                _AM = _col("Extra Qty")
                _AN = _col("Net Target Days")
                _AX = _col("Confidence Multiplier")
                _AO = _col("Excess / Order")
                _AP = _col("Order Qty")
                _AQ = _col("Order Qty + Extra Qty")
                _AR = _col("CBM")
                _AS = _col("Case Pack")
                _AT = _col("Order Qty + Extra Qty (Rounded)")
                _AU = _col("Total CBM")
                _AV = _col("Days Current Order Lasts")
                _AW = _col("Days Total Inventory Lasts")
                _etrade_inv  = _col(f"Etrade Inventory ({_etrade_inv_label})")
                _etrade_drr  = _col("Etrade DRR")
                _etrade_days = _col("Days Total Inventory Lasts (Etrade)")

                drr_source_col_idx = cols_list.index("DRR Source") + 1  # 1-based
                yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
                red_fill = PatternFill(start_color="FF6B6B", end_color="FF6B6B", fill_type="solid")
                green_fill = PatternFill(start_color="90EE90", end_color="90EE90", fill_type="solid")

                for row_idx in range(2, len(combined_df_data) + 2):  # Skip header row
                    r = row_idx
                    inactive = f'OR({_A}{r}="inactive",{_A}{r}="discontinued until stock lasts")'

                    # Return %
                    ws[f"{_J}{r}"] = f"=IF({_F}{r}>0,{_G}{r}/{_F}{r}*100,0)"

                    # Total MRP = MRP × Pupscribe WH Stock (latest)
                    ws[f"{_total_mrp_col}{r}"] = f"={_mrp_col}{r}*{_V}{r}"
                    ws[f"{_total_mrp_col}{r}"].number_format = '₹#,##0.00'

                    # Collection Value = Total MRP / 2
                    ws[f"{_cv_col}{r}"] = f"={_total_mrp_col}{r}/2"
                    ws[f"{_cv_col}{r}"].number_format = '₹#,##0.00'

                    # Total Stock (prev) = prev WH + prev FBA
                    ws[f"{_R}{r}"] = f"={_S}{r}+{_T}{r}"

                    # Total Stock (latest) = WH latest + FBA latest
                    ws[f"{_U}{r}"] = f"={_V}{r}+{_W}{r}"

                    # On-Hand Days Coverage = Latest Total Stock / DRR
                    ws[f"{_AE}{r}"] = f"=IF({_N}{r}>0,{_U}{r}/{_N}{r},0)"

                    # Total Stock in Transit = sum of 3 transit cols
                    ws[f"{_AI}{r}"] = f"={_AF}{r}+{_AG}{r}+{_AH}{r}"

                    # Current Days Coverage = (Latest Total Stock + Total Transit) / DRR
                    ws[f"{_AJ}{r}"] = f"=IF({_N}{r}>0,({_U}{r}+{_AI}{r})/{_N}{r},0)"

                    # Target Days = Lead Time + Safety Days + Order Processing
                    ws[f"{_AD}{r}"] = f"={_AB}{r}+{_AA}{r}+{_AC}{r}"

                    # Missed Sales DRR = MIN(Missed Sales / period_days, 0.5 * DRR), capped only if DRR > 0
                    ws[f"{_AL}{r}"] = f"=IF({_N}{r}>0,MIN({_AK}{r}/{_period_days},0.5*{_N}{r}),{_AK}{r}/{_period_days})"

                    # Extra Qty = Missed Sales DRR * Lead Time
                    ws[f"{_AM}{r}"] = f"={_AL}{r}*{_AB}{r}"

                    # Net Target Days = IF(Current Coverage < Lead Time, Target Days, Target Days - Current Coverage)
                    ws[f"{_AN}{r}"] = f"=IF({_AJ}{r}<{_AB}{r},{_AD}{r},{_AD}{r}-{_AJ}{r})"

                    # Excess / Order
                    ws[f"{_AO}{r}"] = f'=IF({_N}{r}=0,"NO MOVEMENT",IF({_AJ}{r}<{_AD}{r},"ORDER","EXCESS"))'

                    # Order Qty (0 for inactive/discontinued)
                    # For demand-override rows (green), use Net Total Sales × confidence multiplier
                    # For all other rows: MAX(0, Net Target Days × DRR) × confidence multiplier
                    if (row_idx - 2) in demand_override_row_indices:
                        ws[f"{_AP}{r}"] = f'=IF({_AO}{r}="EXCESS",0,{_I}{r}*{_AX}{r})'
                    else:
                        ws[f"{_AP}{r}"] = f"=IF({inactive},0,MAX(0,{_AN}{r}*{_N}{r})*{_AX}{r})"

                    # Order Qty + Extra Qty (only Extra Qty for inactive/discontinued)
                    ws[f"{_AQ}{r}"] = f"=IF({inactive},{_AM}{r},{_AP}{r}+{_AM}{r})"

                    # Order Qty + Extra Qty (Rounded) — FLOOR to nearest case pack
                    ws[f"{_AT}{r}"] = f"=IF({inactive},0,IF({_AS}{r}>0,FLOOR({_AQ}{r},{_AS}{r}),ROUND({_AQ}{r},0)))"

                    # Total CBM
                    ws[f"{_AU}{r}"] = f"=IF({inactive},0,IF({_AS}{r}>0,({_AT}{r}/{_AS}{r})*{_AR}{r},0))"

                    # Days Current Order Lasts
                    ws[f"{_AV}{r}"] = f"=IF({inactive},0,IF({_N}{r}>0,{_AT}{r}/{_N}{r},0))"

                    # Days Total Inventory Lasts = Current Days Coverage + Days Current Order Lasts
                    ws[f"{_AW}{r}"] = f"=IF({inactive},{_AJ}{r},{_AJ}{r}+{_AV}{r})"

                    # Days Total Inventory Lasts (Etrade) = Etrade Inventory / Etrade DRR
                    ws[f"{_etrade_days}{r}"] = f"=IF({_etrade_drr}{r}>0,{_etrade_inv}{r}/{_etrade_drr}{r},0)"

                    # Apply row highlighting based on DRR source / demand override
                    # row_idx is 1-based (row 2 = first data row); convert to 0-based for the set
                    data_row_idx = row_idx - 2
                    drr_source_val = ws.cell(row=r, column=drr_source_col_idx).value
                    if data_row_idx in demand_override_row_indices:
                        # Green: lookback DRR used but current-period sales are higher
                        # — order qty has been overridden to current-period sales
                        for col_idx in range(1, len(combined_df.columns) + 1):
                            ws.cell(row=r, column=col_idx).fill = green_fill
                    elif drr_source_val == "previous_period":
                        for col_idx in range(1, len(combined_df.columns) + 1):
                            ws.cell(row=r, column=col_idx).fill = yellow_fill
                    elif drr_source_val == "insufficient_stock":
                        for col_idx in range(1, len(combined_df.columns) + 1):
                            ws.cell(row=r, column=col_idx).fill = red_fill

                # Apply currency symbol number format to Unit Price column
                _up_col_letter = _col("Unit Price")
                for i, currency_code in enumerate(_master_unit_price_currencies):
                    num_fmt = _CURRENCY_FORMATS.get(currency_code.upper(), _DEFAULT_NUMBER_FMT)
                    ws[f"{_up_col_letter}{i + 2}"].number_format = num_fmt

                # Hide the Confidence Multiplier column (used by Order Qty formula, not for display)
                ws.column_dimensions[_AX].hidden = True

                # Hide Brand column for individual brand reports (redundant when filtered to one brand)
                if brand:
                    ws.column_dimensions[_brand_col].hidden = True

                # Auto-fit column widths: header text drives the width; skip formula cells
                for col_cells in ws.columns:
                    col_letter = get_column_letter(col_cells[0].column)
                    max_len = 0
                    for cell in col_cells:
                        if cell.value is None:
                            continue
                        val = str(cell.value)
                        if val.startswith("="):
                            continue  # skip formulas — display value is computed, not the formula string
                        max_len = max(max_len, len(val))
                    ws.column_dimensions[col_letter].width = min(max_len + 3, 30)

            # Draft Order sheet — only when a brand filter is active
            if brand and combined_df_data:
                _today_label = datetime.now().strftime("%d %b %Y")
                _draft_sheet_name = f"Draft Order {_today_label}"[:31]

                draft_rows = []
                for item in combined_data:
                    if not isinstance(item, dict):
                        continue
                    draft_rows.append({
                        "Manufacturer Code": item.get("manufacturer_code", ""),
                        "BBCode": item.get("sku_code", ""),
                        "Item Name": item.get("item_name", ""),
                        "Qty": None,
                        "Unit Price": item.get("unit_price", 0) or None,
                        "Total": None,
                        "Case Pack": item.get("case_pack", 0),
                        "Cartons": None,
                        "CBM": item.get("cbm", 0),
                        "Total CBM": None,
                        "_currency": item.get("unit_price_currency", ""),
                    })

                if draft_rows:
                    # Build DataFrame without the internal _currency helper column
                    draft_df = pd.DataFrame([{k: v for k, v in r.items() if k != "_currency"} for r in draft_rows])
                    draft_df.to_excel(writer, sheet_name=_draft_sheet_name, index=False)

                    ws_draft = writer.sheets[_draft_sheet_name]
                    draft_cols = list(draft_df.columns)

                    def _dcol(name):
                        return get_column_letter(draft_cols.index(name) + 1)

                    _dqty   = _dcol("Qty")
                    _dbbcode = _dcol("BBCode")
                    _dup   = _dcol("Unit Price")
                    _dtot  = _dcol("Total")
                    _dcp   = _dcol("Case Pack")
                    _dcar  = _dcol("Cartons")
                    _dcbm  = _dcol("CBM")
                    _dtcbm = _dcol("Total CBM")

                    # Escape single quotes in sheet name for cross-sheet formula reference
                    _master_sheet_ref = _sheet_name.replace("'", "''")
                    _master_sku_col = _col("SKU Code")

                    for r_idx, row in enumerate(draft_rows):
                        r = r_idx + 2  # 1-based, skip header
                        ws_draft[f"{_dqty}{r}"]  = (
                            f"=XLOOKUP({_dbbcode}{r},"
                            f"'{_master_sheet_ref}'!{_master_sku_col}:{_master_sku_col},"
                            f"'{_master_sheet_ref}'!{_AT}:{_AT},0)"
                        )
                        ws_draft[f"{_dtot}{r}"]  = f"={_dqty}{r}*{_dup}{r}"
                        ws_draft[f"{_dcar}{r}"]  = f"=IF({_dcp}{r}>0,{_dqty}{r}/{_dcp}{r},0)"
                        ws_draft[f"{_dtcbm}{r}"] = f"={_dcbm}{r}*{_dcar}{r}"

                        # Apply currency symbol number format to Unit Price and Total cells
                        currency_code = row.get("_currency", "") or ""
                        num_fmt = _CURRENCY_FORMATS.get(currency_code.upper(), _DEFAULT_NUMBER_FMT)
                        ws_draft[f"{_dup}{r}"].number_format = num_fmt
                        ws_draft[f"{_dtot}{r}"].number_format = num_fmt

                    # ── Totals row ──────────────────────────────────────────
                    from openpyxl.styles import Border, Side, Font

                    _last_data_row = len(draft_rows) + 1  # last data row (1-based)
                    _total_row     = _last_data_row + 1   # totals row

                    _thick_top = Border(
                        top=Side(border_style="medium", color="000000")
                    )
                    _bold_font = Font(bold=True)

                    # Label
                    _item_col = _dcol("Item Name")
                    _label_cell = ws_draft[f"{_item_col}{_total_row}"]
                    _label_cell.value = "Total"
                    _label_cell.font  = _bold_font
                    _label_cell.border = _thick_top

                    # SUM formulas with units
                    _totals: list[tuple[str, str, str]] = [
                        # (col_letter, formula,  number_format)
                        (_dqty,  f"=SUM({_dqty}{2}:{_dqty}{_last_data_row})",   "#,##0"),
                        (_dtot,  f"=SUM({_dtot}{2}:{_dtot}{_last_data_row})",   None),   # currency set below
                        (_dcar,  f"=SUM({_dcar}{2}:{_dcar}{_last_data_row})",   "#,##0.00"),
                        (_dtcbm, f"=SUM({_dtcbm}{2}:{_dtcbm}{_last_data_row})", '#,##0.00 "m³"'),
                    ]

                    # Determine currency format for the Total sum cell
                    _currencies = list({r.get("_currency", "") or "" for r in draft_rows})
                    _sum_currency_fmt = _CURRENCY_FORMATS.get(
                        _currencies[0].upper() if len(_currencies) == 1 else "",
                        _DEFAULT_NUMBER_FMT,
                    )

                    for _col_ltr, _formula, _fmt in _totals:
                        _c = ws_draft[f"{_col_ltr}{_total_row}"]
                        _c.value  = _formula
                        _c.font   = _bold_font
                        _c.border = _thick_top
                        if _col_ltr == _dtot:
                            _c.number_format = _sum_currency_fmt
                        elif _fmt:
                            _c.number_format = _fmt

                    # Apply the top border to all other columns in the totals row so the
                    # divider spans the full width of the table.
                    for _dc in draft_cols:
                        _cl = _dcol(_dc)
                        if _cl in {_item_col, _dqty, _dtot, _dcar, _dtcbm}:
                            continue  # already handled
                        _bc = ws_draft[f"{_cl}{_total_row}"]
                        _bc.border = _thick_top

                    # Auto-fit columns
                    for col_cells in ws_draft.columns:
                        col_letter = get_column_letter(col_cells[0].column)
                        max_len = 0
                        for cell in col_cells:
                            if cell.value is None:
                                continue
                            val = str(cell.value)
                            if val.startswith("="):
                                continue
                            max_len = max(max_len, len(val))
                        ws_draft.column_dimensions[col_letter].width = min(max_len + 3, 30)

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


def _classify_sku_stock(current_days_coverage: float, lead_time: float, drr: float) -> str:
    """Classify a SKU's stock health.
    Reorder Risk → Healthy → Heavy → Overstock → Dead (5 categories).
    """
    if drr == 0:
        return "dead"
    if current_days_coverage > 3 * lead_time:
        return "dead"
    if current_days_coverage > 2 * lead_time:
        return "overstock"
    if current_days_coverage > 1.5 * lead_time:
        return "heavy"
    if current_days_coverage >= lead_time:
        return "healthy"
    return "reorder_risk"


def _aggregate_brand_kpi(
    brand: str,
    items: list,
    brand_logistics_map: Dict,
    rate_map: Dict[str, float],
    period_days: int,
) -> Dict:
    """Aggregate all KPI metrics for a single brand."""
    bl = brand_logistics_map.get(brand.lower(), {})
    lead_time = bl.get("lead_time", 60)
    safety_days_medium = bl.get("safety_days_medium", 25)
    target_days = lead_time + safety_days_medium + 10

    units_sold = sum(i.get("combined_metrics", {}).get("total_units_sold", 0) for i in items)
    units_returned = sum(i.get("combined_metrics", {}).get("total_units_returned", 0) for i in items)
    credit_notes = sum(i.get("combined_metrics", {}).get("total_credit_notes", 0) for i in items)
    transfer_orders = sum(i.get("combined_metrics", {}).get("transfer_orders", 0) for i in items)
    net_sales = sum(i.get("combined_metrics", {}).get("total_sales", 0) for i in items)
    revenue = sum(i.get("combined_metrics", {}).get("total_amount", 0) for i in items)
    latest_net_stock = sum(i.get("latest_total_stock", 0) for i in items)
    latest_zoho = sum(i.get("latest_zoho_stock", 0) for i in items)
    latest_fba = sum(i.get("latest_fba_stock", 0) for i in items)
    stock_in_transit = sum(i.get("total_stock_in_transit", 0) for i in items)
    total_cbm = sum(i.get("total_cbm", 0) for i in items)

    # Inventory value and missed sales (need rate per SKU)
    net_inv_value = sum(
        i.get("latest_total_stock", 0) * rate_map.get(i.get("sku_code", ""), 0)
        for i in items
    )
    missed_sales_units = sum(i.get("missed_sales", 0) for i in items)
    missed_sales_daily_units = round(missed_sales_units / period_days, 2) if period_days > 0 else 0.0
    missed_sales_value_total = sum(
        i.get("missed_sales", 0) * rate_map.get(i.get("sku_code", ""), 0)
        for i in items
    )
    missed_sales_daily_value = round(missed_sales_value_total / period_days, 2) if period_days > 0 else 0.0

    # Return %
    return_pct = round(units_returned / units_sold * 100, 2) if units_sold > 0 else 0.0

    # Weighted-average growth rate (weight = per-SKU DRR)
    drr_w_sum = 0.0
    drr_w_total = 0.0
    for i in items:
        item_drr = i.get("combined_metrics", {}).get("avg_daily_run_rate", 0)
        item_gr = i.get("growth_rate")
        if item_gr is not None and item_drr > 0:
            drr_w_sum += item_gr * item_drr
            drr_w_total += item_drr
    growth_rate = round(drr_w_sum / drr_w_total, 2) if drr_w_total > 0 else None

    # Brand-level DRR = sum of per-SKU avg_daily_run_rate.
    # This matches the master report exactly: each SKU's DRR uses
    # (units_sold - credit_notes) / days_in_stock (calendar fallback),
    # so summing them gives total brand demand in units/day on the same basis.
    drr = round(sum(i.get("combined_metrics", {}).get("avg_daily_run_rate", 0) for i in items), 4)
    days_cover = round(latest_net_stock / drr, 2) if drr > 0 else 0.0
    current_days_coverage = round((latest_net_stock + stock_in_transit) / drr, 2) if drr > 0 else 0.0

    # Weighted-average days cover (weight = per-SKU on-hand + in-transit stock).
    # Exclude dead SKUs (drr==0 or coverage > 3× lead time) — they distort the
    # average without providing actionable reorder signal.
    _dead_threshold = 3 * lead_time
    stock_w_total = 0.0
    weighted_doc_sum = 0.0
    for i in items:
        item_drr = i.get("combined_metrics", {}).get("avg_daily_run_rate", 0)
        item_stock = i.get("latest_total_stock", 0) + i.get("total_stock_in_transit", 0)
        item_doc = i.get("current_days_coverage", 0)
        if item_drr > 0 and item_stock > 0 and item_doc <= _dead_threshold:
            weighted_doc_sum += item_stock * item_doc
            stock_w_total += item_stock
    weighted_avg_days_cover = round(weighted_doc_sum / stock_w_total, 2) if stock_w_total > 0 else 0.0

    # Alert level based on aggregate days cover
    if drr == 0:
        alert_level = 3  # no movement
    elif days_cover < lead_time:
        alert_level = 2  # red – critical
    elif days_cover < target_days:
        alert_level = 1  # yellow – caution
    else:
        alert_level = 0  # green – healthy

    # Stock classification per SKU
    classes = [
        _classify_sku_stock(
            i.get("current_days_coverage", 0),
            lead_time,
            i.get("combined_metrics", {}).get("avg_daily_run_rate", 0),
        )
        for i in items
    ]
    total_skus = len(items)
    class_counts = {
        "reorder_risk": sum(1 for c in classes if c == "reorder_risk"),
        "healthy": sum(1 for c in classes if c == "healthy"),
        "heavy": sum(1 for c in classes if c == "heavy"),
        "overstock": sum(1 for c in classes if c == "overstock"),
        "dead": sum(1 for c in classes if c == "dead"),
    }
    class_pct = {k: round(v / total_skus * 100, 1) if total_skus > 0 else 0.0 for k, v in class_counts.items()}

    # Per-SKU list for lowest/highest 10 (exclude dead/no-movement SKUs from both lists)
    sku_detail = sorted(
        [
            {
                "sku_code": i.get("sku_code", ""),
                "item_name": i.get("item_name", ""),
                "drr": round(i.get("combined_metrics", {}).get("avg_daily_run_rate", 0), 4),
                "net_stock": round(i.get("latest_total_stock", 0), 2),
                "days_cover": round(i.get("current_days_coverage", 0), 1),
                "excess_or_order": i.get("excess_or_order", ""),
                "movement": i.get("movement", ""),
                "stock_class": _classify_sku_stock(
                    i.get("current_days_coverage", 0),
                    lead_time,
                    i.get("combined_metrics", {}).get("avg_daily_run_rate", 0),
                ),
            }
            for i in items
            if i.get("combined_metrics", {}).get("avg_daily_run_rate", 0) > 0
        ],
        key=lambda x: x["days_cover"],
    )
    sku_lowest_10 = sku_detail[:10]
    _lowest_codes = {s["sku_code"] for s in sku_lowest_10}
    sku_highest_10 = [s for s in reversed(sku_detail) if s["sku_code"] not in _lowest_codes][:10]

    return {
        "brand": brand,
        "sku_count": total_skus,
        "units_sold": round(units_sold, 2),
        "units_returned": round(units_returned, 2),
        "credit_notes": round(credit_notes, 2),
        "transfer_orders": round(transfer_orders, 2),
        "net_sales": round(net_sales, 2),
        "revenue": round(revenue, 2),
        "return_pct": return_pct,
        "growth_rate": growth_rate,
        "drr": drr,
        "latest_net_stock": round(latest_net_stock, 2),
        "latest_zoho_stock": round(latest_zoho, 2),
        "latest_fba_stock": round(latest_fba, 2),
        "net_sellable_inventory_value": round(net_inv_value, 2),
        "stock_in_transit": round(stock_in_transit, 2),
        "total_cbm": round(total_cbm, 4),
        "days_cover": days_cover,
        "current_days_coverage": current_days_coverage,
        "weighted_avg_days_cover": weighted_avg_days_cover,
        "lead_time": lead_time,
        "safety_days": safety_days_medium,
        "target_days": target_days,
        "alert_level": alert_level,
        "order_count": sum(1 for i in items if i.get("excess_or_order") == "ORDER"),
        "excess_count": sum(1 for i in items if i.get("excess_or_order") == "EXCESS"),
        "no_movement_count": sum(1 for i in items if i.get("excess_or_order") == "NO MOVEMENT"),
        "fast_mover_count": sum(1 for i in items if i.get("movement") == "Fast Mover"),
        "medium_mover_count": sum(1 for i in items if i.get("movement") == "Medium Mover"),
        "slow_mover_count": sum(1 for i in items if i.get("movement") == "Slow Mover"),
        "missed_sales_units": round(missed_sales_units, 2),
        "missed_sales_daily_units": missed_sales_daily_units,
        "missed_sales_value_total": round(missed_sales_value_total, 2),
        "missed_sales_daily_value": missed_sales_daily_value,
        "stock_classification": {"counts": class_counts, "pct": class_pct},
        "sku_lowest_10": sku_lowest_10,
        "sku_highest_10": sku_highest_10,
    }


def _dashboard_kpi_default_dates() -> tuple[str, str]:
    """Return (start_date, end_date) for the default 90-day window ending on the previous Sunday."""
    today = datetime.now()
    # Python weekday(): Mon=0 … Sun=6.  We want the most recent Sunday that is not today.
    days_since_sunday = 7 if today.weekday() == 6 else (today.weekday() + 1)
    prev_sunday = today - timedelta(days=days_since_sunday)
    end = prev_sunday.strftime("%Y-%m-%d")
    start = (prev_sunday - timedelta(days=89)).strftime("%Y-%m-%d")
    return start, end


@router.get("/dashboard-kpi")
async def get_dashboard_kpi(
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (default: 90 days ending previous Sunday)"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD (default: previous Sunday)"),
    db=Depends(get_database),
):
    """
    Stock Cover KPI Dashboard – last 90 days, aggregated by brand.

    Runs the full master-report pipeline for the last 90 days (Zoho sales + latest stock)
    and aggregates the result by brand.

    ## Response Structure

    ### `period`
    - `start_date`, `end_date`, `days` – the 90-day window used

    ### `brands` (array, sorted most-critical first)
    One object per brand. Key fields:

    | Field | Description |
    |---|---|
    | `brand` | Brand name |
    | `sku_count` | Number of active SKUs in the brand |
    | `units_sold` | Gross units sold in the period |
    | `units_returned` | Gross units returned |
    | `credit_notes` | Credit-note units |
    | `transfer_orders` | Inter-warehouse transfer units |
    | `net_sales` | units_sold − credit_notes − transfer_orders |
    | `revenue` | Gross revenue (₹) |
    | `return_pct` | units_returned ÷ units_sold × 100 |
    | `growth_rate` | Weighted-average growth rate vs. trailing DRR (% change) |
    | `drr` | Brand-level DRR = sum of per-SKU avg_daily_run_rate |
    | `days_cover` | latest_net_stock ÷ DRR |
    | `current_days_coverage` | (latest_net_stock + stock_in_transit) ÷ DRR |
    | `weighted_avg_days_cover` | Stock-weighted average days cover across SKUs |
    | `latest_net_stock` | Zoho WH stock + FBA stock (most recent DB record) |
    | `latest_zoho_stock` | Zoho Pupscribe Warehouse stock (most recent DB record) |
    | `latest_fba_stock` | Amazon FBA stock (most recent DB record) |
    | `net_sellable_inventory_value` | latest_net_stock × per-SKU rate (₹) |
    | `stock_in_transit` | Open purchase-order stock in transit |
    | `total_cbm` | Total cubic metres for suggested orders |
    | `lead_time` | Brand lead time (days) from brand_logistics |
    | `safety_days` | Medium-mover safety days from brand_logistics |
    | `target_days` | lead_time + safety_days + 10 (order processing) |
    | `alert_level` | Colour-coded health indicator (see below) |
    | `order_count` | SKUs flagged as ORDER |
    | `excess_count` | SKUs flagged as EXCESS |
    | `no_movement_count` | SKUs flagged as NO MOVEMENT |
    | `fast_mover_count` / `medium_mover_count` / `slow_mover_count` | Movement breakdown |
    | `missed_sales_units` | Total unfulfilled demand units in the period |
    | `missed_sales_daily_units` | missed_sales_units ÷ period_days |
    | `missed_sales_value_total` | missed_sales_units × rate (₹) |
    | `missed_sales_daily_value` | missed_sales_value_total ÷ period_days (₹/day) |
    | `stock_classification` | `{ counts: {...}, pct: {...} }` with keys reorder_risk / healthy / heavy / overstock / dead |
    | `sku_lowest_10` | 10 SKUs with the lowest days cover (excluding dead/no-movement) |
    | `sku_highest_10` | 10 SKUs with the highest days cover (excluding those already in lowest 10) |

    ### `alert_level` – Brand Health Colour
    | Value | Colour | Condition |
    |---|---|---|
    | `0` | **Green** – Healthy | `days_cover ≥ target_days` (brand has sufficient stock for the full replenishment cycle) |
    | `1` | **Yellow** – Caution | `lead_time ≤ days_cover < target_days` (stock covers lead time but is below target buffer) |
    | `2` | **Red** – Critical | `0 < days_cover < lead_time` (stock will run out before a replenishment order can arrive) |
    | `3` | **Grey** – No Movement | `drr = 0` (no sales recorded in the period) |

    ### SKU Stock Classification (per `stock_classification`)
    | Class | Condition |
    |---|---|
    | `reorder_risk` | `0 < days_cover < lead_time` |
    | `healthy` | `lead_time ≤ days_cover < 2 × lead_time` |
    | `heavy` | `2 × lead_time ≤ days_cover < 3 × lead_time` |
    | `overstock` | `days_cover ≥ 3 × lead_time` |
    | `dead` | `drr = 0` (no movement) |

    ### `totals`
    Cross-brand aggregates for all the same fields listed above.

    ### `global_stock_classification`
    Aggregate counts and percentages across all brands.

    ### `latest_stock_dates`
    - `zoho` – date of the most recent Zoho WH stock record in DB
    - `fba` – date of the most recent FBA stock record in DB
    """
    # Resolve dates – fall back to default 90-day window ending previous Sunday
    if not start_date or not end_date:
        start_date, end_date = _dashboard_kpi_default_dates()

    try:
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        period_days = max(1, (end_dt - start_dt).days + 1)

        service = OptimizedMasterReportService(db)

        content, brand_logistics_map = await asyncio.gather(
            _generate_master_report_data(start_date, end_date, True, db),
            service.get_brand_logistics(),
        )

        combined_data = content.get("combined_data", [])

        # Batch-load product rates (for inventory value + missed-sales value)
        all_skus = {i.get("sku_code") for i in combined_data if i.get("sku_code")}
        product_data_map = await service.batch_load_all_product_data(all_skus) if all_skus else {}
        rate_map: Dict[str, float] = {
            sku: float(pdata.get("rate") or 0) for sku, pdata in product_data_map.items()
        }

        # Group by brand — skip products with no brand set
        brand_groups: Dict[str, list] = defaultdict(list)
        for item in combined_data:
            brand = (item.get("brand") or "").strip()
            if not brand:
                continue
            brand_groups[brand].append(item)

        brands_kpi = [
            _aggregate_brand_kpi(brand, items, brand_logistics_map, rate_map, period_days)
            for brand, items in brand_groups.items()
        ]

        # Sort: most critical first, then alphabetical
        brands_kpi.sort(key=lambda x: (-x["alert_level"], x["brand"]))

        # Global totals
        total_net_sales = sum(b["net_sales"] for b in brands_kpi)
        total_net_stock = sum(b["latest_net_stock"] for b in brands_kpi)
        total_units_sold = sum(b["units_sold"] for b in brands_kpi)
        total_units_returned = sum(b["units_returned"] for b in brands_kpi)
        total_drr = round(sum(b["drr"] for b in brands_kpi), 4)
        total_days_cover = round(total_net_stock / total_drr, 2) if total_drr > 0 else 0.0
        total_transit = sum(b["stock_in_transit"] for b in brands_kpi)
        total_current_coverage = round((total_net_stock + total_transit) / total_drr, 2) if total_drr > 0 else 0.0

        # Global weighted avg days cover (weight = brand net stock, exclude dead brands)
        global_stock_w = sum(b["latest_net_stock"] for b in brands_kpi if b["drr"] > 0)
        global_weighted_doc = sum(
            b["latest_net_stock"] * b["weighted_avg_days_cover"]
            for b in brands_kpi if b["drr"] > 0 and b["latest_net_stock"] > 0
        )
        global_weighted_avg_doc = round(global_weighted_doc / global_stock_w, 2) if global_stock_w > 0 else 0.0

        # Global stock classification counts
        global_class_counts: Dict[str, int] = {"reorder_risk": 0, "healthy": 0, "heavy": 0, "overstock": 0, "dead": 0}
        for b in brands_kpi:
            for k, v in b["stock_classification"]["counts"].items():
                global_class_counts[k] = global_class_counts.get(k, 0) + v
        total_global_skus = sum(global_class_counts.values())
        global_class_pct = {
            k: round(v / total_global_skus * 100, 1) if total_global_skus > 0 else 0.0
            for k, v in global_class_counts.items()
        }

        response_payload = {
            "period": {"start_date": start_date, "end_date": end_date, "days": period_days},
            "brands": brands_kpi,
            "totals": {
                "brand_count": len(brands_kpi),
                "sku_count": sum(b["sku_count"] for b in brands_kpi),
                "units_sold": round(total_units_sold, 2),
                "units_returned": round(total_units_returned, 2),
                "credit_notes": round(sum(b["credit_notes"] for b in brands_kpi), 2),
                "transfer_orders": round(sum(b["transfer_orders"] for b in brands_kpi), 2),
                "net_sales": round(total_net_sales, 2),
                "revenue": round(sum(b["revenue"] for b in brands_kpi), 2),
                "return_pct": round(total_units_returned / total_units_sold * 100, 2) if total_units_sold > 0 else 0.0,
                "drr": total_drr,
                "latest_net_stock": round(total_net_stock, 2),
                "latest_zoho_stock": round(sum(b["latest_zoho_stock"] for b in brands_kpi), 2),
                "latest_fba_stock": round(sum(b["latest_fba_stock"] for b in brands_kpi), 2),
                "net_sellable_inventory_value": round(sum(b["net_sellable_inventory_value"] for b in brands_kpi), 2),
                "stock_in_transit": round(total_transit, 2),
                "total_cbm": round(sum(b["total_cbm"] for b in brands_kpi), 4),
                "days_cover": total_days_cover,
                "current_days_coverage": total_current_coverage,
                "weighted_avg_days_cover": global_weighted_avg_doc,
                "missed_sales_units": round(sum(b["missed_sales_units"] for b in brands_kpi), 2),
                "missed_sales_daily_units": round(sum(b["missed_sales_daily_units"] for b in brands_kpi), 2),
                "missed_sales_value_total": round(sum(b["missed_sales_value_total"] for b in brands_kpi), 2),
                "missed_sales_daily_value": round(sum(b["missed_sales_daily_value"] for b in brands_kpi), 2),
            },
            "global_stock_classification": {
                "counts": global_class_counts,
                "pct": global_class_pct,
            },
            "latest_stock_dates": content.get("latest_stock_dates", {}),
            "meta": content.get("meta", {}),
        }
        return JSONResponse(status_code=200, content=response_payload)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating dashboard KPI: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to generate dashboard KPI: {str(e)}")


@router.get("/dashboard-kpi/download")
async def download_dashboard_kpi(
    breakdown: str = Query("brand", description="'brand' for brand-level or 'product' for SKU-level"),
    start_date: Optional[str] = Query(None, description="Start date YYYY-MM-DD (default: 90 days ending previous Sunday)"),
    end_date: Optional[str] = Query(None, description="End date YYYY-MM-DD (default: previous Sunday)"),
    db=Depends(get_database),
):
    """
    Download the Stock Cover KPI as an Excel (.xlsx) file.

    Runs the same 90-day master-report pipeline as `GET /dashboard-kpi` and writes
    the aggregated result to a single-sheet Excel workbook.

    ## Query Parameters
    - `breakdown=brand` *(default)* – one row per brand, aggregated KPIs
    - `breakdown=product` – one row per active SKU, individual KPIs

    ## Excel Sheet: `Brand KPI` (breakdown=brand)
    One row per brand. Columns:

    **Identity & Movers**
    - `Brand`, `SKUs`, `Fast Movers`, `Medium Movers`, `Slow Movers`

    **Sales**
    - `Units Sold`, `Units Returned`, `Return %`
    - `Credit Notes`, `Transfer Orders`, `Net Sales`, `Revenue (₹)`
    - `Growth Rate (%)`

    **Inventory & DRR**
    - `DRR (Net Sales / 90)` – brand-level DRR (sum of per-SKU DRR)
    - `Zoho WH Stock ({latest_date})`, `FBA Stock ({latest_date})`, `Latest Net Stock`
    - `Net Inv. Value (₹)` – latest stock × per-SKU rate
    - `Stock in Transit`, `Total CBM`

    **Coverage**
    - `Days Cover (Stock / DRR)` – latest_net_stock ÷ DRR
    - `Weighted Avg Days Cover` – stock-weighted average days cover across SKUs
    - `Current Days Coverage (Stock+Transit/DRR)`
    - `Lead Time (days)`, `Safety Days (Medium)`, `Target Days`

    **Stock Classification**
    - `SKUs - Reorder Risk / Healthy / Heavy / Overstock / Dead`
    - `% Reorder Risk / Healthy / Heavy / Overstock / Dead`
    - `SKUs Needing Order`, `SKUs in Excess`, `SKUs No Movement`

    **Missed Sales**
    - `Missed Sales (units, 90d)`, `Missed Sales (units/day)`
    - `Missed Sales Value (₹, 90d)`, `Missed Sales Value (₹/day)`

    ### Conditional Row Formatting (Brand KPI sheet)
    | Colour | Condition | Meaning |
    |---|---|---|
    | Red | `0 < Days Cover < Lead Time` | Critical – stock will run out before a new order can arrive |
    | Yellow | `Lead Time ≤ Days Cover < Target Days` | Caution – below the target safety buffer |
    | (no fill) | `Days Cover ≥ Target Days` or no movement | Healthy / no movement – no action required |

    ## Excel Sheet: `Product KPI` (breakdown=product)
    One row per active SKU. Columns:

    **Identity**
    - `Brand`, `Purchase Status`, `Movement`, `Stock Class`
    - `SKU Code`, `Item Name`

    **Sales**
    - `Units Sold`, `Units Returned`, `Return %`
    - `Credit Notes`, `Transfer Orders`, `Net Sales`, `Revenue (₹)`
    - `Growth Rate (%)`, `Avg Daily Run Rate`

    **Inventory**
    - `Zoho WH Stock ({latest_date})`, `FBA Stock ({latest_date})`, `Latest Net Stock`
    - `Net Inv. Value (₹)`, `Stock in Transit`

    **Coverage & Orders**
    - `Days Cover (Stock / DRR)`, `Current Days Coverage`
    - `Lead Time (days)`, `Safety Days`, `Target Days`
    - `Excess / Order`, `Order Qty`, `Order Qty + Extra Qty`, `Order Qty (Rounded)`
    - `Total CBM`, `Days Total Inventory Lasts`

    **Missed Sales**
    - `Missed Sales (units)`, `Missed Sales DRR (units/day)`
    - `Missed Sales Value (₹)`, `Missed Sales Daily Value (₹)`

    ## Response
    Returns a binary `.xlsx` file as a streaming attachment.
    Filename format: `stock_cover_kpi_{breakdown}_{start_date}_to_{end_date}_{YYYYMMDD_HHMMSS}.xlsx`
    """
    try:
        if not start_date or not end_date:
            start_date, end_date = _dashboard_kpi_default_dates()
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        period_days = max(1, (end_dt - start_dt).days + 1)

        service = OptimizedMasterReportService(db)
        content, brand_logistics_map = await asyncio.gather(
            _generate_master_report_data(start_date, end_date, True, db),
            service.get_brand_logistics(),
        )

        combined_data = content.get("combined_data", [])
        latest_stock_dates = content.get("latest_stock_dates", {})

        # Batch-load rates for inventory / missed-sales value columns
        all_skus = {i.get("sku_code") for i in combined_data if i.get("sku_code")}
        product_data_map = await service.batch_load_all_product_data(all_skus) if all_skus else {}
        rate_map: Dict[str, float] = {
            sku: float(pdata.get("rate") or 0) for sku, pdata in product_data_map.items()
        }

        def _fmt_date(d: str) -> str:
            try:
                return datetime.strptime(d, "%Y-%m-%d").strftime("%d %b %Y")
            except Exception:
                return d or "Latest"

        _latest_zoho_label = _fmt_date(latest_stock_dates.get("zoho", ""))
        _latest_fba_label = _fmt_date(latest_stock_dates.get("fba", ""))
        _etrade_inv_label = _fmt_date(latest_stock_dates.get("etrade", "")) or _fmt_date(end_date)

        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            if breakdown == "product":
                rows = []
                for item in combined_data:
                    m = item.get("combined_metrics", {})
                    sku = item.get("sku_code", "")
                    rate = rate_map.get(sku, 0)
                    lead_time_item = item.get("lead_time", 60)
                    item_drr = m.get("avg_daily_run_rate", 0)
                    item_doc = item.get("current_days_coverage", 0)
                    rows.append({
                        "Brand": item.get("brand", ""),
                        "Purchase Status": item.get("purchase_status", ""),
                        "Movement": item.get("movement", ""),
                        "Stock Class": _classify_sku_stock(item_doc, lead_time_item, item_drr),
                        "SKU Code": sku,
                        "Item Name": item.get("item_name", ""),
                        "Units Sold": m.get("total_units_sold", 0),
                        "Units Returned": m.get("total_units_returned", 0),
                        "Return %": round(m.get("total_units_returned", 0) / m.get("total_units_sold", 1) * 100, 2) if m.get("total_units_sold", 0) > 0 else 0,
                        "Credit Notes": m.get("total_credit_notes", 0),
                        "Transfer Orders": m.get("transfer_orders", 0),
                        "Net Sales": m.get("total_sales", 0),
                        "Revenue (₹)": m.get("total_amount", 0),
                        "Growth Rate (%)": item.get("growth_rate"),
                        "Avg Daily Run Rate": item_drr,
                        f"Zoho WH Stock ({_latest_zoho_label})": item.get("latest_zoho_stock", 0),
                        f"FBA Stock ({_latest_fba_label})": item.get("latest_fba_stock", 0),
                        "Latest Net Stock": item.get("latest_total_stock", 0),
                        "Net Inv. Value (₹)": round(item.get("latest_total_stock", 0) * rate, 2),
                        "Stock in Transit": item.get("total_stock_in_transit", 0),
                        "Days Cover (Stock / DRR)": m.get("avg_days_of_coverage", 0),
                        "Current Days Coverage": item_doc,
                        "Lead Time (days)": lead_time_item,
                        "Safety Days": item.get("safety_days", 0),
                        "Target Days": item.get("target_days", 0),
                        "Excess / Order": item.get("excess_or_order", ""),
                        "Order Qty": item.get("order_qty", 0),
                        "Order Qty + Extra Qty": item.get("order_qty_plus_extra_qty", 0),
                        "Order Qty (Rounded)": item.get("order_qty_plus_extra_qty_rounded", 0),
                        "Total CBM": item.get("total_cbm", 0),
                        "Days Total Inventory Lasts": item.get("days_total_inventory_lasts", 0),
                        f"Etrade Inventory ({_etrade_inv_label})": m.get("etrade_inventory", 0),
                        "Etrade DRR": m.get("etrade_drr", 0),
                        "Days Total Inventory Lasts (Etrade)": m.get("etrade_days_inventory_lasts", 0),
                        "Missed Sales (units)": item.get("missed_sales", 0),
                        "Missed Sales DRR (units/day)": item.get("missed_sales_drr", 0),
                        "Missed Sales Value (₹)": round(item.get("missed_sales", 0) * rate, 2),
                        "Missed Sales Daily Value (₹)": round(item.get("missed_sales_drr", 0) * rate, 2),
                    })
                if rows:
                    pd.DataFrame(rows).to_excel(writer, sheet_name="Product KPI", index=False)

            else:
                # Brand aggregation — skip products with no brand set
                brand_groups: Dict[str, list] = defaultdict(list)
                for item in combined_data:
                    brand = (item.get("brand") or "").strip()
                    if not brand:
                        continue
                    brand_groups[brand].append(item)

                rows = []
                for brand in sorted(brand_groups.keys()):
                    items = brand_groups[brand]
                    kpi = _aggregate_brand_kpi(brand, items, brand_logistics_map, rate_map, period_days)
                    cc = kpi["stock_classification"]["counts"]
                    cp = kpi["stock_classification"]["pct"]
                    rows.append({
                        "Brand": brand,
                        "SKUs": kpi["sku_count"],
                        "Fast Movers": kpi["fast_mover_count"],
                        "Medium Movers": kpi["medium_mover_count"],
                        "Slow Movers": kpi["slow_mover_count"],
                        "Units Sold": kpi["units_sold"],
                        "Units Returned": kpi["units_returned"],
                        "Return %": kpi["return_pct"],
                        "Credit Notes": kpi["credit_notes"],
                        "Transfer Orders": kpi["transfer_orders"],
                        "Net Sales": kpi["net_sales"],
                        "Revenue (₹)": kpi["revenue"],
                        "Growth Rate (%)": kpi["growth_rate"],
                        "DRR (Net Sales / 90)": kpi["drr"],
                        f"Zoho WH Stock ({_latest_zoho_label})": kpi["latest_zoho_stock"],
                        f"FBA Stock ({_latest_fba_label})": kpi["latest_fba_stock"],
                        "Latest Net Stock": kpi["latest_net_stock"],
                        "Net Inv. Value (₹)": kpi["net_sellable_inventory_value"],
                        "Stock in Transit": kpi["stock_in_transit"],
                        "Total CBM": kpi["total_cbm"],
                        "Days Cover (Stock / DRR)": kpi["days_cover"],
                        "Weighted Avg Days Cover": kpi["weighted_avg_days_cover"],
                        "Current Days Coverage (Stock+Transit/DRR)": kpi["current_days_coverage"],
                        "Lead Time (days)": kpi["lead_time"],
                        "Safety Days (Medium)": kpi["safety_days"],
                        "Target Days": kpi["target_days"],
                        "SKUs - Reorder Risk": cc.get("reorder_risk", 0),
                        "% Reorder Risk": cp.get("reorder_risk", 0),
                        "SKUs - Healthy": cc.get("healthy", 0),
                        "% Healthy": cp.get("healthy", 0),
                        "SKUs - Heavy": cc.get("heavy", 0),
                        "% Heavy": cp.get("heavy", 0),
                        "SKUs - Overstock": cc.get("overstock", 0),
                        "% Overstock": cp.get("overstock", 0),
                        "SKUs - Dead": cc.get("dead", 0),
                        "% Dead": cp.get("dead", 0),
                        "SKUs Needing Order": kpi["order_count"],
                        "SKUs in Excess": kpi["excess_count"],
                        "SKUs No Movement": kpi["no_movement_count"],
                        "Missed Sales (units, 90d)": kpi["missed_sales_units"],
                        "Missed Sales (units/day)": kpi["missed_sales_daily_units"],
                        "Missed Sales Value (₹, 90d)": kpi["missed_sales_value_total"],
                        "Missed Sales Value (₹/day)": kpi["missed_sales_daily_value"],
                    })

                if rows:
                    df = pd.DataFrame(rows)
                    df.to_excel(writer, sheet_name="Brand KPI", index=False)

                    from openpyxl.styles import PatternFill
                    red_fill = PatternFill(start_color="FF6B6B", end_color="FF6B6B", fill_type="solid")
                    yellow_fill = PatternFill(start_color="FFFF00", end_color="FFFF00", fill_type="solid")
                    ws = writer.sheets["Brand KPI"]
                    dc_col = list(df.columns).index("Days Cover (Stock / DRR)") + 1
                    lt_col = list(df.columns).index("Lead Time (days)") + 1
                    tg_col = list(df.columns).index("Target Days") + 1

                    for row_idx in range(2, len(rows) + 2):
                        dc = ws.cell(row=row_idx, column=dc_col).value or 0
                        lt = ws.cell(row=row_idx, column=lt_col).value or 60
                        tg = ws.cell(row=row_idx, column=tg_col).value or 95
                        fill = red_fill if dc > 0 and dc < lt else (yellow_fill if dc > 0 and dc < tg else None)
                        if fill:
                            for col_idx in range(1, len(df.columns) + 1):
                                ws.cell(row=row_idx, column=col_idx).fill = fill

        excel_buffer.seek(0)
        file_bytes = excel_buffer.read()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"stock_cover_kpi_{breakdown}_{start_date}_to_{end_date}_{timestamp}.xlsx"

        return Response(
            content=file_bytes,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Length": str(len(file_bytes)),
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating dashboard KPI download: {e}")
        logger.error(traceback.format_exc())
        raise HTTPException(status_code=500, detail=f"Failed to generate KPI download: {str(e)}")


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
    Import Item Status (col A), Stock in Transit 1, Stock in Transit 2, CBM, and Case Pack
    from the PSR Google Sheet Master tab, resolved by column header name.
    Maps BBCode (col B) to cf_sku_code. purchase_status, cbm, case_pack, and transit values
    are applied to ALL products sharing the same sku_code via update_many.
    """
    import os
    import gspread
    from google.oauth2.service_account import Credentials

    def _map_status(raw: str) -> str:
        s = raw.strip().lower()
        if s == "active":
            return "active"
        elif s == "discontinued":
            return "inactive"
        elif "until stock" in s:
            return "discontinued until stock lasts"
        return ""

    try:
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        creds_path = os.path.join(backend_dir, "creds.json")

        scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = Credentials.from_service_account_file(creds_path, scopes=scopes)
        gc = gspread.authorize(credentials)

        sh = gc.open_by_key("1xjPO-M8MScP4gLVI04QCdz1594DIkXh2_odAxtKlMWY")
        ws = sh.worksheet("Master")

        # Fetch all rows at once and resolve columns by header name
        all_rows = ws.get_all_values()

        if not all_rows:
            return JSONResponse(status_code=200, content={"message": "Sheet is empty", "updated": 0, "skipped": 0})

        header = [h.strip() for h in all_rows[0]]

        def _col(name: str) -> int:
            try:
                return header.index(name)
            except ValueError:
                return -1

        sit1_idx = _col("Stock in Transit 1")
        sit2_idx = _col("Stock in Transit 2")
        cbm_idx = _col("CBM")
        case_pack_idx = _col("Case Pack")

        products_collection = db.get_collection("products")
        updated_count = 0
        skipped_count = 0

        for row in all_rows[1:]:  # skip header
            if len(row) < 2:
                continue

            bbcode = row[1].strip() if len(row) > 1 else ""
            if not bbcode:
                continue

            raw_status = row[0].strip() if len(row) > 0 else ""
            sit1_raw = row[sit1_idx] if sit1_idx >= 0 and len(row) > sit1_idx else ""
            sit2_raw = row[sit2_idx] if sit2_idx >= 0 and len(row) > sit2_idx else ""
            cbm_raw = row[cbm_idx] if cbm_idx >= 0 and len(row) > cbm_idx else ""
            case_pack_raw = row[case_pack_idx] if case_pack_idx >= 0 and len(row) > case_pack_idx else ""

            def _to_float(val: str) -> float:
                try:
                    return float(val) if val else 0
                except (ValueError, TypeError):
                    return 0

            update_doc: dict = {
                "cbm": _to_float(cbm_raw),
                "case_pack": _to_float(case_pack_raw),
                "stock_in_transit_1": _to_float(sit1_raw),
                "stock_in_transit_2": _to_float(sit2_raw),
            }
            mapped_status = _map_status(raw_status)
            if mapped_status:
                update_doc["purchase_status"] = mapped_status

            result = products_collection.update_many(
                {"cf_sku_code": bbcode},
                {"$set": update_doc},
            )

            if result.modified_count > 0 or result.matched_count > 0:
                updated_count += 1
            else:
                skipped_count += 1

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Imported status, CBM, Case Pack and Stock in Transit from PSR Sheet",
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
                {"item_id": 1, "cf_sku_code": 1, "name": 1, "brand": 1, "cbm": 1, "case_pack": 1, "purchase_status": 1,
                 "stock_in_transit_1": 1, "stock_in_transit_2": 1, "stock_in_transit_3": 1, "_id": 0},
            )
            .skip(skip)
            .limit(page_size)
            .sort("cf_sku_code", 1)
        )

        # Ensure numeric fields default to 0 when missing from DB
        products = []
        for p in raw_products:
            products.append({
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
                {"item_id": 1, "cf_sku_code": 1, "name": 1, "brand": 1, "cbm": 1, "case_pack": 1,
                 "purchase_status": 1, "stock_in_transit_1": 1, "stock_in_transit_2": 1,
                 "stock_in_transit_3": 1, "_id": 0},
            ).sort("cf_sku_code", 1)
        )

        rows = []
        for p in raw_products:
            rows.append({
                "SKU Code": p.get("cf_sku_code", ""),
                "Product Name": p.get("name", ""),
                "Brand": p.get("brand", ""),
                "Status": p.get("purchase_status", ""),
                "CBM": p.get("cbm", 0) or 0,
                "Case Pack": p.get("case_pack", 0) or 0,
                "Stock in Transit 1": p.get("stock_in_transit_1", 0) or 0,
                "Stock in Transit 2": p.get("stock_in_transit_2", 0) or 0,
                "Stock in Transit 3": p.get("stock_in_transit_3", 0) or 0,
            })

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
            headers={"Content-Disposition": "attachment; filename=product_logistics.xlsx"},
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
    stock_in_transit_1: float = Query(None),
    stock_in_transit_2: float = Query(None),
    stock_in_transit_3: float = Query(None),
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
        if stock_in_transit_1 is not None:
            item_fields["stock_in_transit_1"] = stock_in_transit_1
        if stock_in_transit_2 is not None:
            item_fields["stock_in_transit_2"] = stock_in_transit_2
        if stock_in_transit_3 is not None:
            item_fields["stock_in_transit_3"] = stock_in_transit_3

        if not item_fields and purchase_status is None:
            raise HTTPException(status_code=400, detail="No fields to update")

        matched = 0

        # Update item-specific fields by item_id
        if item_fields:
            iid: Any = int(item_id) if item_id.isdigit() else item_id
            result = products_collection.update_one({"item_id": iid}, {"$set": item_fields})
            if result.matched_count == 0:
                result = products_collection.update_one({"item_id": item_id}, {"$set": item_fields})
            matched = max(matched, result.matched_count)

        # Update purchase_status across ALL products with the same sku_code
        if purchase_status is not None:
            sku_filter = {"cf_sku_code": sku_code.strip()} if sku_code else {"item_id": (int(item_id) if item_id.isdigit() else item_id)}
            result = products_collection.update_many(sku_filter, {"$set": {"purchase_status": purchase_status}})
            matched = max(matched, result.matched_count)

        if matched == 0:
            raise HTTPException(status_code=404, detail=f"Product with item_id '{item_id}' not found")

        return JSONResponse(
            status_code=200,
            content={"message": f"Updated logistics for item {item_id}"},
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
