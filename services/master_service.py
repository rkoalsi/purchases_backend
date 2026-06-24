from datetime import datetime, timedelta
from typing import List, Dict, Any, Set, Optional
import asyncio
import io
import json
import math
import os
import requests
import time
from collections import defaultdict
import logging
import traceback
from ..database import get_database
from ..routes.amazon import generate_report_by_date_range as amazon_generate_report

logger = logging.getLogger(__name__)

_IV_BOOKS_URL = os.getenv("BOOKS_URL")
_IV_CLIENT_ID = os.getenv("CLIENT_ID")
_IV_CLIENT_SECRET = os.getenv("CLIENT_SECRET")
_IV_BOOKS_REFRESH_TOKEN = os.getenv("BOOKS_REFRESH_TOKEN")
_IV_ORG_ID = os.getenv("ORGANIZATION_ID", "776755316")
_PUPSCRIBE_WH_LOCATION_ID = "3220178000143298047"


class OptimizedMasterReportService:
    """Optimized service to handle master reporting across Blinkit, Amazon, and Zoho"""

    def __init__(self, database):
        self.database = database

    async def get_zoho_report(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """Get Zoho report data"""
        try:
            from ..routes.zoho import get_sales_report_fast

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

    async def fetch_transfer_order_customers(self) -> List[str]:
        """Load transfer-order customer names from the transfer_order_customers collection."""
        try:
            col = self.database.get_collection("transfer_order_customers")
            def _query():
                return [doc["name"] for doc in col.find({}, {"name": 1, "_id": 0}) if doc.get("name")]
            return await asyncio.to_thread(_query)
        except Exception as e:
            logger.error(f"Error loading transfer order customers from DB: {e}")
            return []

    async def fetch_transfer_orders(self, start_date: str, end_date: str) -> Dict[str, float]:
        """Fetch transfer order quantities from invoices for specific customer names.
        Returns dict of {sku_code: total_quantity}."""
        try:
            invoices_collection = self.database["invoices"]
            composite_collection = self.database.get_collection("composite_products")
            products_collection = self.database.get_collection("products")

            transfer_customers = await self.fetch_transfer_order_customers()

            def _fetch():
                # credit_notes stores date as ISODate, invoices as string
                from datetime import datetime
                cn_start = datetime.strptime(start_date, "%Y-%m-%d")
                cn_end = datetime.strptime(end_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59)

                _transfer_customers_lower = [n.lower() for n in transfer_customers]
                pipeline = [
                    {
                        "$match": {
                            "$or": [
                                {"date": {"$gte": start_date, "$lte": end_date}},
                                {"date": {"$gte": cn_start, "$lte": cn_end}},
                            ],
                            "status": {"$nin": ["void"]},
                            "$expr": {"$in": [{"$toLower": "$customer_name"}, _transfer_customers_lower]},
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
                                        "$expr": {"$in": [{"$toLower": "$customer_name"}, _transfer_customers_lower]},
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

    async def fetch_latest_blinkit_inventory(self) -> Dict:
        """Fetch the most recent Blinkit inventory snapshot per SKU from blinkit_inventory.
        Returns {
            "by_sku": {sku_code: total_warehouse_inventory},
            "latest_date": "YYYY-MM-DD" | None,
        }."""
        try:
            def _fetch():
                db = self.database
                inv_col = db["blinkit_inventory"]

                # Find the latest date across all blinkit inventory records
                latest_doc = inv_col.find_one({}, sort=[("date", -1)], projection={"date": 1})
                if not latest_doc:
                    return {"by_sku": {}, "latest_date": None}
                latest_date = latest_doc["date"]

                # Aggregate warehouse_inventory by sku_code for that date
                pipeline = [
                    {"$match": {"date": latest_date, "sku_code": {"$ne": None}}},
                    {"$group": {
                        "_id": "$sku_code",
                        "total_inventory": {"$sum": "$warehouse_inventory"},
                    }},
                ]
                by_sku: Dict[str, float] = {}
                for doc in inv_col.aggregate(pipeline, allowDiskUse=True):
                    sku = doc.get("_id")
                    qty = float(doc.get("total_inventory", 0) or 0)
                    if sku and qty > 0:
                        by_sku[sku] = qty

                latest_date_str = (
                    latest_date.strftime("%Y-%m-%d")
                    if hasattr(latest_date, "strftime")
                    else str(latest_date)[:10]
                )
                return {"by_sku": by_sku, "latest_date": latest_date_str}

            result = await asyncio.to_thread(_fetch)
            logger.info(
                f"Fetched latest Blinkit inventory for {len(result.get('by_sku', {}))} SKUs "
                f"(date: {result.get('latest_date')})"
            )
            return result

        except Exception as e:
            logger.error(f"Error fetching latest Blinkit inventory: {e}")
            return {"by_sku": {}, "latest_date": None}

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

    async def fetch_amazon_sku_set(self) -> Set[str]:
        """Return set of all sku_codes present in amazon_sku_mapping (Live on Amazon)."""
        try:
            col = self.database.get_collection("amazon_sku_mapping")
            def _query():
                return {doc["sku_code"] for doc in col.find({}, {"sku_code": 1, "_id": 0}) if doc.get("sku_code")}
            return await asyncio.to_thread(_query)
        except Exception as e:
            logger.error(f"Error fetching amazon sku set: {e}")
            return set()

    async def fetch_blinkit_sku_set(self) -> Set[str]:
        """Return set of all sku_codes present in blinkit_sku_mapping (Live on Blinkit)."""
        try:
            col = self.database.get_collection("blinkit_sku_mapping")
            def _query():
                return {doc["sku_code"] for doc in col.find({}, {"sku_code": 1, "_id": 0}) if doc.get("sku_code")}
            return await asyncio.to_thread(_query)
        except Exception as e:
            logger.error(f"Error fetching blinkit sku set: {e}")
            return set()

    async def fetch_amazon_final_drr_by_sku(self, start_date: str, end_date: str) -> Dict[str, float]:
        """Fetch Amazon PSR Final DRR per SKU using the exact same computation as the PSR API (VC + FBA combined)."""
        try:
            items = await amazon_generate_report(
                start_date=start_date,
                end_date=end_date,
                database=self.database,
                report_type="all",
            )
            result: Dict[str, float] = {}
            for item in items:
                sku = item.get("sku_code")
                if not sku:
                    continue
                drr = item.get("drr", 0)
                result[sku] = round(float(drr), 4) if isinstance(drr, (int, float)) else 0.0
            return result
        except Exception as e:
            logger.error(f"Error fetching Amazon final DRR by SKU: {e}")
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
                     "cf_item_code": 1, "purchase_price": 1, "currency": 1,
                     "purchase_status": 1, "stock_in_transit_1": 1, "stock_in_transit_2": 1,
                     "stock_in_transit_3": 1, "created_at": 1,
                     "hsn_or_sac": 1, "is_combo_product": 1,
                     "category": 1, "sub_category": 1, "series": 1, "sku": 1, "_id": 0},
                ).sort("_id", 1))

            products = await asyncio.to_thread(_fetch_all)

            from datetime import timezone as _tz
            _now_utc = datetime.now(_tz.utc).replace(tzinfo=None)  # naive UTC
            three_months_ago = _now_utc - timedelta(days=90)

            result = {}
            for product in products:
                sku_code = product.get("cf_sku_code")
                if not sku_code:
                    continue
                created_at = product.get("created_at")
                if isinstance(created_at, str):
                    try:
                        dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                        if dt.tzinfo is not None:
                            # Convert to UTC naive so comparisons are always on the same basis
                            created_at = dt.astimezone(_tz.utc).replace(tzinfo=None)
                        else:
                            created_at = dt
                    except (ValueError, AttributeError):
                        created_at = None
                elif isinstance(created_at, datetime) and created_at.tzinfo is not None:
                    # PyMongo may return tz-aware datetimes in some configurations
                    created_at = created_at.astimezone(_tz.utc).replace(tzinfo=None)
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
                    "purchase_price": self.safe_float(product.get("purchase_price")),
                    "currency": product.get("currency", "") or "",
                    "stock_in_transit_1": self.safe_float(product.get("stock_in_transit_1")),
                    "stock_in_transit_2": self.safe_float(product.get("stock_in_transit_2")),
                    "stock_in_transit_3": self.safe_float(product.get("stock_in_transit_3")),
                    "is_new": is_new,
                    "hsn_or_sac": product.get("hsn_or_sac", ""),
                    "is_combo_product": product.get("is_combo_product", False),
                    "category": product.get("category", "") or "",
                    "sub_category": product.get("sub_category", "") or "",
                    "series": product.get("series", "") or "",
                    "sku": product.get("sku", "") or "",
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
                        "days_in_stock_any_wh": self.safe_int(item.get("total_days_in_stock_any_wh")),
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
                "days_in_stock_any_wh": 0,
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
                    agg["days_in_stock_any_wh"] += self.safe_int(
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
                    agg["days_in_stock_any_wh"] += self.safe_int(
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
                    "days_in_stock_any_wh": agg["days_in_stock_any_wh"],
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
                    "total_days_in_stock_any_wh": 0.0,
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
            metrics["total_days_in_stock_any_wh"] += self.safe_float(item.get("days_in_stock_any_wh"))

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

            # DRR based on net customer demand: units_sold − credit_notes − transfer_orders.
            # Transfer orders are inter-warehouse replenishment sends (not end-customer demand)
            # and must be excluded so DRR reflects true sell-through — matching the Excel formula
            # which uses Net Total Sales (= units_sold − returns − transfers) as the numerator.
            zoho_units = data.pop("_zoho_units_sold")
            credit_notes = metrics.get("total_credit_notes", 0)
            zoho_stock = data.pop("_zoho_closing_stock")
            fba_stock = fba_stock_by_sku.get(sku, 0)

            # Resolve transfer qty first so it feeds the DRR numerator
            transfer_qty = transfer_orders_by_sku.get(sku, 0)
            net_demand = round(metrics["total_units_sold"] - credit_notes - transfer_qty, 2)

            # Floor the DRR numerator at 0: when returns + transfers exceed sales,
            # net_demand goes negative and a negative DRR would slip past the
            # NO MOVEMENT check and trigger a minimum case-pack order downstream.
            # (total_sales keeps the true negative value for visibility.)
            days_in_stock = metrics.get("total_days_in_stock", 0)
            drr_demand = max(0.0, net_demand)
            if days_in_stock > 0:
                metrics["avg_daily_run_rate"] = round(drr_demand / days_in_stock, 2)
            elif period_days > 0:
                metrics["avg_daily_run_rate"] = round(drr_demand / period_days, 2)

            # Closing stock = Pupscribe WH (Zoho) + FBA stock
            metrics["total_closing_stock"] = round(zoho_stock + fba_stock, 2)
            metrics["fba_closing_stock"] = round(fba_stock, 2)
            metrics["pupscribe_wh_stock"] = round(zoho_stock, 2)

            # Transfer orders and total sales
            metrics["transfer_orders"] = round(transfer_qty, 2)
            metrics["total_sales"] = round(net_demand, 2)

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
                        "order_processing": self.safe_float(doc.get("order_processing", 10), 10),
                    }

            return result

        except Exception as e:
            logger.error(f"Error fetching brand logistics: {e}")
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
        from ..routes.zoho import fetch_stock_data_for_items_batch, fetch_zoho_lookback_sales_batch

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
                    item_sales = sales_data.get(str(item_id), {})
                    units_sold = item_sales.get("units_sold", 0) if isinstance(item_sales, dict) else int(item_sales)
                    returns = item_sales.get("returns", 0) if isinstance(item_sales, dict) else 0
                    net_units_sold = max(0, units_sold - returns)
                    drr = round(net_units_sold / days_in_stock, 2) if days_in_stock > 0 else 0
                    results[sku] = {
                        "drr": drr,
                        "lookback_period": f"{p_start_str} to {p_end_str}",
                        "days_in_stock": days_in_stock,
                        "units_sold": units_sold,
                        "returns": returns,
                        "net_units_sold": net_units_sold,
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
                    "units_sold": 0,
                    "returns": 0,
                    "net_units_sold": 0,
                    "found": False,
                }

        return results

    async def fetch_past_90d_drr(
        self,
        sku_to_item_id: Dict[str, str],
        start_date: str,
        period_days: int = 90,
    ) -> Dict[str, Dict]:
        """
        Fetch Zoho-based DRR for the comparison window immediately before start_date.
        Window length matches period_days (defaults to 90).
        Used to compute the growth rate column and to populate lookback sales for
        current_period/insufficient_stock rows.
        Returns dict of sku_code -> {drr, units_sold, returns, net_units_sold,
                                     days_in_stock, period}.
        """
        from ..routes.zoho import fetch_stock_data_for_items_batch, fetch_zoho_lookback_sales_batch

        if not sku_to_item_id:
            return {}

        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            past_end = start_dt - timedelta(days=1)
            past_start = past_end - timedelta(days=period_days - 1)
            period_str = f"{past_start.strftime('%Y-%m-%d')} to {past_end.strftime('%Y-%m-%d')}"
            periods = [(past_start, past_end)]
            all_item_ids = list(set(sku_to_item_id.values()))

            stock_by_period, sales_by_period = await asyncio.gather(
                fetch_stock_data_for_items_batch(self.database, periods, all_item_ids),
                fetch_zoho_lookback_sales_batch(self.database, periods, all_item_ids),
            )

            stock_data = stock_by_period.get(0, {})
            sales_data = sales_by_period.get(0, {})

            result: Dict[str, Dict] = {}
            for sku, item_id in sku_to_item_id.items():
                stock_info = stock_data.get(item_id, {})
                days_in_stock = stock_info.get("total_days_in_stock", 0)
                item_sales = sales_data.get(str(item_id), {})
                units_sold = item_sales.get("units_sold", 0) if isinstance(item_sales, dict) else int(item_sales)
                returns = item_sales.get("returns", 0) if isinstance(item_sales, dict) else 0
                net_units_sold = max(0, units_sold - returns)
                drr = round(net_units_sold / days_in_stock, 4) if days_in_stock > 0 else 0.0
                result[sku] = {
                    "drr": drr,
                    "units_sold": units_sold,
                    "returns": returns,
                    "net_units_sold": net_units_sold,
                    "days_in_stock": days_in_stock,
                    "period": period_str,
                }

            logger.info(f"Fetched past {period_days}d DRR for {len(result)} SKUs")
            return result
        except Exception as e:
            logger.error(f"Error fetching past {period_days}d DRR: {e}")
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
            "order_processing": 10,
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
                item["order_processing"] = brand_settings.get("order_processing", 10)
                item["brand"] = product_brands.get(item.get("sku_code", ""), "") or ""

        return combined_data

    async def get_stock_in_transit(self) -> Dict[str, Dict]:
        """Get stock in transit from open purchase orders, grouped by SKU"""
        try:
            po_collection = self.database.get_collection("purchase_orders")

            def _fetch_open_pos():
                # Sort by date ascending so slot 1 always corresponds to the
                # earliest-dated open PO per vendor (deterministic across runs).
                return list(po_collection.find(
                    {"order_status_formatted": "Issued"},
                    {"line_items": 1, "purchaseorder_number": 1, "date": 1, "vendor_id": 1, "_id": 0}
                ).sort("date", 1))

            open_pos = await asyncio.to_thread(_fetch_open_pos)

            # Build per-vendor PO ordering (already sorted by date ascending).
            # slot 0 = earliest PO for that vendor, slot 1 = next, etc.
            vendor_po_order: Dict[str, List[str]] = defaultdict(list)
            for po in open_pos:
                vid = po.get("vendor_id", "") or "unknown"
                po_num = po.get("purchaseorder_number", "")
                if po_num and po_num not in vendor_po_order[vid]:
                    vendor_po_order[vid].append(po_num)

            # For each SKU, accumulate qty keyed to the vendor's PO slot index (0-2).
            sku_transit_slots: Dict[str, Dict[int, float]] = defaultdict(lambda: defaultdict(float))
            sku_po_by_slot: Dict[str, Dict[int, str]] = defaultdict(dict)

            for po in open_pos:
                vid = po.get("vendor_id", "") or "unknown"
                po_num = po.get("purchaseorder_number", "")
                po_slots = vendor_po_order.get(vid, [])
                slot_idx = po_slots.index(po_num) if po_num in po_slots else -1
                if slot_idx < 0 or slot_idx >= 3:
                    continue  # only track first 3 POs per vendor

                for li in po.get("line_items", []):
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
                        sku_transit_slots[sku_code][slot_idx] += transit_qty
                        sku_po_by_slot[sku_code][slot_idx] = po_num

            # Build result: slot indices map directly to transit_1/2/3 so that all
            # SKUs from the same vendor share the same PO→slot assignment.
            result = {}
            for sku, slots in sku_transit_slots.items():
                po_map = sku_po_by_slot.get(sku, {})
                result[sku] = {
                    "transit_1": slots.get(0, 0),
                    "transit_2": slots.get(1, 0),
                    "transit_3": slots.get(2, 0),
                    "transit_1_po": po_map.get(0, ""),
                    "transit_2_po": po_map.get(1, ""),
                    "transit_3_po": po_map.get(2, ""),
                    "total": sum(slots.values()),
                }

            logger.info(f"Found stock in transit for {len(result)} SKUs from {len(open_pos)} open POs")
            return result

        except Exception as e:
            logger.error(f"Error fetching stock in transit: {e}")
            return {}

    async def fetch_latest_po_unit_prices(
        self,
        sku_codes: Set[str],
        sku_to_brand: Dict[str, str] = None,
    ) -> Dict[str, Dict]:
        """Fetch unit price (rate) and currency_code from the latest purchase order per SKU,
        filtered by the brand's vendor_id so prices don't bleed across vendors.

        For each SKU the brand name is resolved to a vendor_id via the brands collection,
        and only POs from that vendor are considered.  SKUs whose brand has no matching
        vendor entry fall back to unfiltered (any vendor) so nothing is silently dropped.

        Pass 1: matches via line_items.item_custom_fields where api_name == 'cf_sku_code'.
        Pass 2 (fallback): for SKUs not found in pass 1, matches by line_items.item_id
                           via a products-collection lookup (handles POs without custom fields).
        Returns {sku_code: {"rate": float, "currency_code": str}}."""
        if not sku_codes:
            return {}
        try:
            po_collection = self.database.get_collection("purchase_orders")
            products_collection = self.database.get_collection("products")
            brands_collection = self.database.get_collection("brands")
            sku_list = [s for s in sku_codes if s]

            def _fetch():
                # Build brand_name_lower → vendor_id from brands collection
                brand_to_vendor: Dict[str, str] = {}
                for bdoc in brands_collection.find({}, {"name": 1, "vendor_id": 1, "_id": 0}):
                    bname = bdoc.get("name", "")
                    vid = bdoc.get("vendor_id", "")
                    if bname and vid:
                        brand_to_vendor[bname.lower()] = vid

                # Group SKUs by vendor_id; those with no match go to unmatched_skus
                vendor_to_skus: Dict[str, List[str]] = defaultdict(list)
                unmatched_skus: List[str] = []
                if sku_to_brand:
                    for sku in sku_list:
                        brand = sku_to_brand.get(sku, "") or ""
                        vid = brand_to_vendor.get(brand.lower(), "") if brand else ""
                        if vid:
                            vendor_to_skus[vid].append(sku)
                        else:
                            unmatched_skus.append(sku)
                else:
                    unmatched_skus = sku_list

                result: Dict[str, Dict] = {}

                def _run_pass1(skus: List[str], vendor_id: str = None):
                    pipeline = [
                        *([ {"$match": {"vendor_id": vendor_id}} ] if vendor_id else []),
                        {"$unwind": "$line_items"},
                        {"$match": {"line_items.rate": {"$gt": 0}}},
                        {"$unwind": "$line_items.item_custom_fields"},
                        {"$match": {
                            "line_items.item_custom_fields.api_name": "cf_sku_code",
                            "line_items.item_custom_fields.value": {"$in": skus},
                        }},
                        {"$sort": {"date": -1}},
                        {"$group": {
                            "_id": "$line_items.item_custom_fields.value",
                            "rate": {"$first": "$line_items.rate"},
                            "currency_code": {"$first": "$currency_code"},
                        }},
                    ]
                    for doc in po_collection.aggregate(pipeline):
                        sku = doc.get("_id")
                        if sku:
                            result[sku] = {
                                "rate": float(doc.get("rate") or 0),
                                "currency_code": doc.get("currency_code", "") or "",
                            }

                # Pass 1: one pipeline per vendor group, then fallback for unmatched
                for vid, skus in vendor_to_skus.items():
                    _run_pass1(skus, vendor_id=vid)
                if unmatched_skus:
                    _run_pass1(unmatched_skus, vendor_id=None)

                # Pass 2: fallback for SKUs still not found (PO line items without cf_sku_code)
                missing_skus = [s for s in sku_list if s not in result]
                if missing_skus:
                    item_id_to_sku: Dict[str, str] = {}
                    item_id_to_vendor: Dict[str, str] = {}
                    for doc in products_collection.find(
                        {"cf_sku_code": {"$in": missing_skus}},
                        {"item_id": 1, "cf_sku_code": 1, "_id": 0},
                    ):
                        iid = doc.get("item_id")
                        sku = doc.get("cf_sku_code", "")
                        if iid and sku:
                            iid_str = str(iid)
                            item_id_to_sku[iid_str] = sku
                            if sku_to_brand:
                                brand = sku_to_brand.get(sku, "") or ""
                                vid = brand_to_vendor.get(brand.lower(), "") if brand else ""
                                if vid:
                                    item_id_to_vendor[iid_str] = vid

                    if item_id_to_sku:
                        vendor_to_item_ids: Dict[str, List[str]] = defaultdict(list)
                        unmatched_item_ids: List[str] = []
                        for iid_str in item_id_to_sku:
                            vid = item_id_to_vendor.get(iid_str, "")
                            if vid:
                                vendor_to_item_ids[vid].append(iid_str)
                            else:
                                unmatched_item_ids.append(iid_str)

                        def _run_pass2(str_ids: List[str], vendor_id: str = None):
                            int_ids = [int(i) for i in str_ids if i.isdigit()]
                            pipeline2 = [
                                *([ {"$match": {"vendor_id": vendor_id}} ] if vendor_id else []),
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

                        for vid, iids in vendor_to_item_ids.items():
                            _run_pass2(iids, vendor_id=vid)
                        if unmatched_item_ids:
                            _run_pass2(unmatched_item_ids, vendor_id=None)

                return result

            result = await asyncio.to_thread(_fetch)
            logger.info(f"Fetched latest PO unit prices for {len(result)} SKUs")
            return result
        except Exception as e:
            logger.error(f"Error fetching latest PO unit prices: {e}")
            return {}

    async def fetch_managed_unit_prices(self, sku_codes: Set[str]) -> Dict[str, Dict]:
        """Fetch buyer-managed unit prices from the `product_unit_prices` collection.

        This is the source of truth for the master report "Unit Price" column — set
        and maintained on the dedicated Unit Prices page. SKUs without a managed price
        are simply absent from the result (the report then shows blank/zero), so unit
        price is no longer derived from the latest purchase order rate.

        Returns {sku_code: {"rate": float, "currency_code": str}}."""
        if not sku_codes:
            return {}
        try:
            collection = self.database.get_collection("product_unit_prices")
            sku_list = [s for s in sku_codes if s]

            def _fetch():
                result: Dict[str, Dict] = {}
                for doc in collection.find(
                    {"sku_code": {"$in": sku_list}},
                    {"_id": 0, "sku_code": 1, "unit_price": 1, "currency": 1},
                ):
                    sku = doc.get("sku_code")
                    if not sku:
                        continue
                    result[sku] = {
                        "rate": float(doc.get("unit_price") or 0),
                        "currency_code": doc.get("currency", "") or "",
                    }
                return result

            result = await asyncio.to_thread(_fetch)
            logger.info(f"Fetched managed unit prices for {len(result)} SKUs")
            return result
        except Exception as e:
            logger.error(f"Error fetching managed unit prices: {e}")
            return {}

    @staticmethod
    def _apply_lookback_to_item(item: Dict, lb: Dict, start_date: str) -> None:
        """Apply a single DRR lookback result to an item, setting all drr/highlight fields in-place."""
        metrics = item.get("combined_metrics", {})
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
            item["drr_lookback_returns"] = lb.get("returns", 0)
            item["drr_net_lookback_sales"] = lb.get("net_units_sold", 0)
            lb_period = lb["lookback_period"]
            gt_180 = False
            seasonal_mismatch = False
            if lb_period and " to " in lb_period:
                lb_start_str = lb_period.split(" to ")[0].strip()
                try:
                    lb_start_dt = datetime.strptime(lb_start_str, "%Y-%m-%d")
                    report_start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    gt_180 = (report_start_dt - lb_start_dt).days > 180
                    seasonal_mismatch = (
                        (lb_start_dt.month - 1) // 3 != (report_start_dt.month - 1) // 3
                    )
                except ValueError:
                    pass
            item["drr_lookback_gt_180"] = gt_180
            item["drr_lookback_seasonal_mismatch"] = seasonal_mismatch
            item["highlight"] = "orange" if gt_180 else "yellow"
            current_net_sales = metrics.get("total_sales", 0)
            if lb.get("net_units_sold", lb["units_sold"]) < current_net_sales:
                item["order_qty_demand_override"] = True
                item["current_period_sales_for_override"] = current_net_sales
        else:
            item["drr_source"] = "insufficient_stock"
            item["drr_lookback_period"] = ""
            item["drr_lookback_days_in_stock"] = 0
            item["drr_lookback_sales"] = 0
            item["drr_lookback_returns"] = 0
            item["drr_net_lookback_sales"] = 0
            item["drr_lookback_gt_180"] = False
            item["drr_lookback_seasonal_mismatch"] = False
            item["highlight"] = "red"

    @staticmethod
    def enrich_with_order_calculations(
        combined_data: List[Dict],
        transit_data: Dict[str, Dict],
        logistics_data: Dict[str, Dict],
        missed_sales_by_sku: Dict[str, float] = None,
        period_days: int = 30,
        current_period_missed_sales_by_sku: Dict[str, float] = None,
    ) -> List[Dict]:
        """Set per-item logistics metadata, missed sales, and confidence labels.
        Coverage and order quantities are deferred to _compute_order_quantities,
        which runs once after latest stock is attached to each item."""
        if missed_sales_by_sku is None:
            missed_sales_by_sku = {}
        if current_period_missed_sales_by_sku is None:
            current_period_missed_sales_by_sku = {}

        for item in combined_data:
            sku = item.get("sku_code", "")
            metrics = item.get("combined_metrics", {})
            drr = metrics.get("avg_daily_run_rate", 0)

            # Transit data: always derived from open purchase orders
            logistics = logistics_data.get(sku, {})
            purchase_status = logistics.get("purchase_status", "")
            transit = transit_data.get(sku, {})
            item["stock_in_transit_1"] = transit.get("transit_1", 0)
            item["stock_in_transit_2"] = transit.get("transit_2", 0)
            item["stock_in_transit_3"] = transit.get("transit_3", 0)
            item["transit_1_po"] = transit.get("transit_1_po", "")
            item["transit_2_po"] = transit.get("transit_2_po", "")
            item["transit_3_po"] = transit.get("transit_3_po", "")
            item["total_stock_in_transit"] = transit.get("total", 0)

            # Target days = lead_time + safety_days + order_processing
            safety_days = item.get("safety_days", 15)
            lead_time = item.get("lead_time", 60)
            order_processing = item.get("order_processing", 10)
            item["order_processing"] = order_processing
            target_days = lead_time + safety_days + order_processing
            item["target_days"] = target_days

            # Missed Sales — sourced from lookback period for yellow/orange rows,
            # current period for white/red/green rows
            if item.get("order_qty_demand_override"):
                missed_sales = current_period_missed_sales_by_sku.get(sku, 0)
            else:
                missed_sales = missed_sales_by_sku.get(sku, 0)
            item["missed_sales"] = missed_sales
            missed_sales_drr_raw = missed_sales / period_days if period_days > 0 else 0.0
            if drr > 0:
                missed_sales_drr_raw = min(missed_sales_drr_raw, 0.5 * drr)
            missed_sales_drr = round(missed_sales_drr_raw, 4)
            item["missed_sales_drr"] = missed_sales_drr
            extra_qty = round(missed_sales_drr * lead_time, 2)
            item["extra_qty"] = extra_qty

            # Logistics metadata
            item["cbm"] = logistics.get("cbm", 0)
            item["case_pack"] = logistics.get("case_pack", 0)
            item["purchase_status"] = purchase_status
            item["is_new"] = logistics.get("is_new", False)
            item["hsn_or_sac"] = logistics.get("hsn_or_sac", "")
            item["is_combo_product"] = logistics.get("is_combo_product", False)

            # Inactive items: zero out order fields; _compute_order_quantities
            # will set days_total_inventory_lasts using latest stock.
            if purchase_status in ("inactive", "discontinued until stock lasts"):
                item["order_qty"] = 0
                item["order_qty_plus_extra_qty"] = round(extra_qty, 2)
                item["order_qty_plus_extra_qty_rounded"] = 0
                item["total_cbm"] = 0
                item["days_current_order_lasts"] = 0
                item["order_qty_basis"] = ""
                item["confidence_multiplier"] = 1.0
                continue

            # Confidence multiplier and DRR flag label (stock-independent)
            if item.get("drr_source") == "insufficient_stock":
                days_in_stock = metrics.get("total_days_in_stock", 0)
                if days_in_stock < 30:
                    confidence_multiplier = 0.0
                    drr_flag_label = "Manual buyer input required (0%)"
                elif days_in_stock < 45:
                    confidence_multiplier = 0.5
                    drr_flag_label = "System order, dampened (50%)"
                elif days_in_stock < 60:
                    confidence_multiplier = 0.75
                    drr_flag_label = "System order, near-normal (75%)"
                else:
                    confidence_multiplier = 1.0
                    drr_flag_label = ""
                if item.get("order_qty_demand_override"):
                    item["order_qty_basis"] = f"Current Period Sales · {drr_flag_label}" if drr_flag_label else "Current Period Sales"
                else:
                    item["order_qty_basis"] = drr_flag_label
            else:
                confidence_multiplier = 1.0
                item["order_qty_basis"] = "Current Period Sales" if item.get("order_qty_demand_override") else ""
            item["confidence_multiplier"] = confidence_multiplier

            if item.get("drr_lookback_seasonal_mismatch"):
                existing = item.get("order_qty_basis", "")
                item["order_qty_basis"] = f"{existing} · Seasonal mismatch" if existing else "Seasonal mismatch"

        return combined_data

    @staticmethod
    def _compute_order_quantities(combined_data: List[Dict]) -> None:
        """Compute coverage and order quantities using item.latest_total_stock.
        Modifies items in-place. Must be called after latest stock is attached."""
        for item in combined_data:
            metrics = item.get("combined_metrics", {})
            drr = metrics.get("avg_daily_run_rate", 0)
            latest_stock = item.get("latest_total_stock", 0)
            total_transit = item.get("total_stock_in_transit", 0)
            purchase_status = item.get("purchase_status", "")

            avg_doc = round(latest_stock / drr, 2) if drr > 0 else 0.0
            metrics["avg_days_of_coverage"] = avg_doc
            item["on_hand_days_coverage"] = avg_doc

            current_days_coverage = round((latest_stock + total_transit) / drr, 2) if drr > 0 else 0.0
            item["current_days_coverage"] = current_days_coverage

            target_days = item.get("target_days", 0)
            # <= 0 (not == 0): a negative DRR must not classify as ORDER and get
            # bumped to a minimum case pack by the zero-floor rounding rule below.
            if drr <= 0:
                item["excess_or_order"] = "NO MOVEMENT"
                item["order_qty"] = 0
                item["order_qty_plus_extra_qty"] = 0
                item["order_qty_plus_extra_qty_rounded"] = 0
                item["total_cbm"] = 0
                item["days_current_order_lasts"] = 0
                item["days_total_inventory_lasts"] = round(current_days_coverage, 2)
                continue
            elif current_days_coverage < target_days:
                item["excess_or_order"] = "ORDER"
            else:
                item["excess_or_order"] = "EXCESS"

            if purchase_status in ("inactive", "discontinued until stock lasts"):
                item["days_total_inventory_lasts"] = round(current_days_coverage, 2)
                continue

            extra_qty = item.get("extra_qty", 0)
            lead_time_val = item.get("lead_time", 60)
            net_target_days = target_days if current_days_coverage < lead_time_val else target_days - current_days_coverage
            item["net_target_days"] = round(net_target_days, 2)

            confidence_multiplier = item.get("confidence_multiplier", 1.0)
            if item.get("order_qty_demand_override"):
                order_qty = 0.0 if item.get("excess_or_order") == "EXCESS" else item.get("current_period_sales_for_override", 0) * confidence_multiplier
            else:
                order_qty = max(0, net_target_days * drr) * confidence_multiplier
            item["order_qty"] = round(order_qty, 2)
            item["order_qty_plus_extra_qty"] = round(order_qty + extra_qty, 2)

            case_pack = item.get("case_pack", 0)
            order_qty_plus_extra_qty = item["order_qty_plus_extra_qty"]
            if item.get("excess_or_order") == "EXCESS":
                item["order_qty_plus_extra_qty_rounded"] = 0
            elif case_pack > 0:
                rounded = math.floor(order_qty_plus_extra_qty / case_pack) * case_pack
                if rounded == 0 and item.get("excess_or_order") == "ORDER":
                    rounded = case_pack
                item["order_qty_plus_extra_qty_rounded"] = rounded
            else:
                item["order_qty_plus_extra_qty_rounded"] = round(order_qty_plus_extra_qty, 0)

            order_qty_rounded = item["order_qty_plus_extra_qty_rounded"]
            cbm = item.get("cbm", 0)
            item["total_cbm"] = round((order_qty_rounded / case_pack) * cbm, 4) if case_pack > 0 and cbm > 0 else 0
            item["days_current_order_lasts"] = round(order_qty_rounded / drr, 2) if drr > 0 else 0
            item["days_total_inventory_lasts"] = round(current_days_coverage + item["days_current_order_lasts"], 2)

    async def fetch_inventory_valuation_cogs(self, as_of_date: str) -> Dict:
        """Fetch inventory valuation from Zoho Books for Pupscribe WH only.
        Returns {
            "by_sku": {cf_sku_code: {"unit_cost": float, "asset_value": float, "qty": int}},
            "as_of_date": "YYYY-MM-DD",
        }
        """
        try:
            def _fetch():
                # Build two lookup maps from products collection:
                #   sku (EAN/barcode) → cf_sku_code
                #   item_id (Zoho Books item ID) → cf_sku_code  (primary, more reliable)
                sku_to_cf: Dict[str, str] = {}
                item_id_to_cf: Dict[str, str] = {}
                for p in self.database["products"].find(
                    {"cf_sku_code": {"$exists": True, "$ne": ""}},
                    {"sku": 1, "cf_sku_code": 1, "item_id": 1, "status": 1, "_id": 0},
                ):
                    cf = p.get("cf_sku_code", "")
                    if not cf:
                        continue
                    # Prefer active entries so they win on duplicate cf_sku_code
                    is_active = p.get("status") == "active"
                    sku_val = p.get("sku", "") or ""
                    item_id_val = str(p.get("item_id", "") or "")
                    if sku_val and (is_active or sku_val not in sku_to_cf):
                        sku_to_cf[sku_val] = cf
                    if item_id_val and (is_active or item_id_val not in item_id_to_cf):
                        item_id_to_cf[item_id_val] = cf

                # Get Zoho Books access token
                auth_url = _IV_BOOKS_URL.format(
                    books_refresh_token=_IV_BOOKS_REFRESH_TOKEN,
                    clientId=_IV_CLIENT_ID,
                    clientSecret=_IV_CLIENT_SECRET,
                    grantType="refresh_token",
                )
                r = requests.post(auth_url, timeout=30)
                r.raise_for_status()
                token = r.json()["access_token"]
                headers = {"Authorization": f"Zoho-oauthtoken {token}"}

                rule = json.dumps({
                    "columns": [{"index": 1, "field": "location_name", "value": [_PUPSCRIBE_WH_LOCATION_ID], "comparator": "in", "group": "branch"}],
                    "criteria_string": "1",
                })

                by_sku: Dict[str, Dict] = {}
                page = 1
                while True:
                    params = {
                        "organization_id": _IV_ORG_ID,
                        "filter_by": "TransactionDate.CustomDate",
                        "to_date": as_of_date,
                        "rule": rule,
                        "select_columns": '[{"field":"item_name","group":"report"},{"field":"quantity_available","group":"report"},{"field":"asset_value","group":"report"},{"field":"sku","group":"item"}]',
                        "group_by": '[{"field":"none","group":"report"}]',
                        "sort_column": "quantity_available",
                        "sort_order": "D",
                        "response_option": "1",
                        "page": page,
                        "per_page": 200,
                    }
                    resp = requests.get(
                        "https://books.zoho.com/api/v3/reports/inventoryvaluation",
                        headers=headers,
                        params=params,
                        timeout=30,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                    if data.get("code") != 0:
                        raise ValueError(f"Zoho Books IV API error: {data.get('message')}")

                    for row in data["inventory_valuation"][0]["item_details"]:
                        # Match by item_id first (most reliable), fall back to EAN sku
                        row_item_id = str(row.get("item_id") or row.get("item", {}).get("item_id") or "")
                        row_sku = row.get("sku") or row.get("item", {}).get("sku", "") or ""
                        cf_sku = item_id_to_cf.get(row_item_id) or sku_to_cf.get(row_sku)
                        if not cf_sku:
                            asset_val_check = row.get("asset_value", 0.0) or 0.0
                            if asset_val_check > 0:
                                logger.warning(f"IV unmatched: item_id={row_item_id!r} sku={row_sku!r} name={row.get('item_name')!r} asset_value={asset_val_check}")
                            continue
                        qty = row.get("quantity_available", 0) or 0
                        asset_val = row.get("asset_value", 0.0) or 0.0
                        unit_cost = round(asset_val / qty, 2) if qty > 0 else 0.0
                        existing = by_sku.get(cf_sku)
                        if existing is None or asset_val > existing.get("asset_value", 0):
                            by_sku[cf_sku] = {
                                "unit_cost": unit_cost,
                                "asset_value": round(asset_val, 2),
                                "qty": qty,
                            }

                    if not data["page_context"]["has_more_page"]:
                        break
                    page += 1
                    time.sleep(0.3)

                return {"by_sku": by_sku, "as_of_date": as_of_date}

            return await asyncio.to_thread(_fetch)
        except Exception as e:
            logger.error(f"Error fetching inventory valuation COGS: {e}")
            return {"by_sku": {}, "as_of_date": ""}

