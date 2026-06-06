from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import asyncio
import math
from collections import defaultdict
import logging
import traceback
from fastapi import HTTPException, status
from .master_service import OptimizedMasterReportService

logger = logging.getLogger(__name__)


async def _generate_master_report_data(
    start_date: str,
    end_date: str,
    include_zoho: bool,
    db,
    brand: str = None,
    dashboard_mode: bool = False,
    include_cogs: bool = False,
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
        _blinkit_inv_task = asyncio.create_task(report_service.fetch_latest_blinkit_inventory())
        _amazon_sku_task = asyncio.create_task(report_service.fetch_amazon_sku_set())
        _blinkit_sku_task = asyncio.create_task(report_service.fetch_blinkit_sku_set())
        _amazon_drr_task = asyncio.create_task(report_service.fetch_amazon_final_drr_by_sku(start_date, end_date))
        _cogs_task = asyncio.create_task(report_service.fetch_inventory_valuation_cogs(end_date)) if include_cogs else None

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
        latest_blinkit_by_sku: Dict[str, float] = {}
        latest_zoho_date: str = ""
        latest_fba_date: str = ""
        latest_blinkit_inv_date: str = ""
        prev_zoho_by_sku: Dict[str, float] = {}
        prev_zoho_date: str = ""
        prev_fba_by_sku: Dict[str, float] = {}
        prev_fba_date: str = ""
        amazon_sku_set: Set[str] = set()
        blinkit_sku_set: Set[str] = set()
        amazon_final_drr_by_sku: Dict[str, float] = {}
        cogs_by_sku: Dict[str, Dict] = {}
        cogs_date: str = ""

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
            # 3. sku_to_item_id (for DRR lookback, if needed)
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
                            if days >= 60 or sku not in lookback_results:
                                item["drr_source"] = "current_period"
                                item["drr_lookback_period"] = ""
                                item["drr_lookback_seasonal_mismatch"] = False
                                item["highlight"] = None
                            else:
                                report_service._apply_lookback_to_item(item, lookback_results[sku], start_date)

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
                            item["drr_lookback_returns"] = 0
                            item["drr_net_lookback_sales"] = 0
                            item["drr_lookback_gt_180"] = False
                            item["highlight"] = "red" if days < 60 else None
                elif not skus_needing_lookback_list:
                    for item in combined_data:
                        item["drr_source"] = "current_period"
                        item["drr_lookback_period"] = ""
                        item["drr_lookback_returns"] = 0
                        item["drr_net_lookback_sales"] = 0
                        item["drr_lookback_gt_180"] = False
                        item["highlight"] = None
                else:
                    for item in combined_data:
                        days = item.get("combined_metrics", {}).get("total_days_in_stock", 0)
                        item["drr_source"] = "current_period" if days >= 60 else "insufficient_stock"
                        item["drr_lookback_period"] = ""
                        item["drr_lookback_returns"] = 0
                        item["drr_net_lookback_sales"] = 0
                        item["drr_lookback_gt_180"] = False
                        item["highlight"] = "red" if days < 60 else None

            except Exception as e:
                logger.error(f"DRR lookback error: {e}")
                logger.error(traceback.format_exc())
                errors.append(f"DRR lookback error: {str(e)}")
                for item in combined_data:
                    item.setdefault("drr_source", "current_period")
                    item.setdefault("drr_lookback_period", "")
                    item.setdefault("drr_lookback_returns", 0)
                    item.setdefault("drr_net_lookback_sales", 0)
                    item.setdefault("drr_lookback_gt_180", False)
                    item.setdefault("highlight", None)

            # Enrichment (uses brand_logistics + transit_data fetched in parallel above)
            # Initialise here so they are always defined even if the try-block below fails.
            product_rates = {}
            product_brands = {}
            logistics_data = {}
            _po_prices_task = None
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
                        "hsn_or_sac": pdata.get("hsn_or_sac", ""),
                        "is_combo_product": pdata.get("is_combo_product", False),
                    }

                # Kick off vendor-filtered PO price fetch now that product_brands is ready
                _po_prices_task = asyncio.create_task(
                    report_service.fetch_latest_po_unit_prices(set(product_brands.keys()), product_brands)
                )

                if brand:
                    if brand.lower() == "petfest":
                        petfest_brands = {"dogfest", "catfest"}
                        combined_data = [
                            item for item in combined_data
                            if product_brands.get(item.get("sku_code", ""), "").lower() in petfest_brands
                        ]
                    else:
                        combined_data = [
                            item for item in combined_data
                            if product_brands.get(item.get("sku_code", ""), "").lower() == brand.lower()
                        ]
                    logger.info(f"Filtered to {len(combined_data)} items for brand '{brand}'")

                for item in combined_data:
                    metrics = item.get("combined_metrics", {})
                    sku = item.get("sku_code", "")
                    net_sales = metrics.get("total_sales", 0)
                    gross_units = metrics.get("total_units_sold", 0)
                    gross_amount = metrics.get("total_amount", 0)
                    if gross_units > 0 and gross_amount > 0:
                        # Derive avg selling rate from Zoho invoice data, apply to net sales
                        avg_rate = gross_amount / gross_units
                        metrics["total_amount"] = round(avg_rate * net_sales, 2)
                    elif net_sales > 0:
                        rate = product_rates.get(sku, 0)
                        if rate > 0:
                            metrics["total_amount"] = round(rate * net_sales, 2)
                    else:
                        metrics["total_amount"] = 0.0

            except Exception as e:
                logger.error(f"Error enriching data: {e}")
                errors.append(f"Enrichment error: {str(e)}")

            # Await the latest-stock task that was started at the very beginning of this
            # function.  By now it has been running concurrently with all report processing,
            # so it should be close to (or already) complete.
            try:
                lz_res, lf_res, lb_res, amz_sku_res, blk_sku_res, amz_drr_res = await asyncio.wait_for(
                    asyncio.gather(_zoho_stock_task, _fba_stock_task, _blinkit_inv_task, _amazon_sku_task, _blinkit_sku_task, _amazon_drr_task, return_exceptions=True),
                    timeout=60.0,
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
                if not isinstance(lb_res, Exception):
                    latest_blinkit_by_sku = lb_res.get("by_sku", {})
                    latest_blinkit_inv_date = lb_res.get("latest_date") or ""
                else:
                    logger.error(f"Latest Blinkit inventory fetch failed: {lb_res}")
                if not isinstance(amz_sku_res, Exception):
                    amazon_sku_set = amz_sku_res
                else:
                    logger.error(f"Amazon SKU set fetch failed: {amz_sku_res}")
                if not isinstance(blk_sku_res, Exception):
                    blinkit_sku_set = blk_sku_res
                else:
                    logger.error(f"Blinkit SKU set fetch failed: {blk_sku_res}")
                if not isinstance(amz_drr_res, Exception):
                    amazon_final_drr_by_sku = amz_drr_res
                else:
                    logger.error(f"Amazon final DRR fetch failed: {amz_drr_res}")
            except asyncio.TimeoutError:
                logger.warning("Latest stock fetch timed out — latest stock columns will be empty")
                _zoho_stock_task.cancel()
                _fba_stock_task.cancel()
                _blinkit_inv_task.cancel()
                _amazon_sku_task.cancel()
                _blinkit_sku_task.cancel()
                _amazon_drr_task.cancel()

            # Await COGS task separately (only when requested; it calls Zoho Books API)
            if _cogs_task is not None:
                try:
                    cogs_res = await asyncio.wait_for(_cogs_task, timeout=120.0)
                    cogs_by_sku = cogs_res.get("by_sku", {})
                    cogs_date = cogs_res.get("as_of_date", "")
                except Exception as e:
                    logger.error(f"COGS fetch failed: {e}")
                    if not _cogs_task.done():
                        _cogs_task.cancel()

            # Inject stub rows for SKUs that have FBA stock but zero sales in this period.
            # These ASINs exist in amazon_sku_mapping but never appeared in any sales report,
            # so combine_data_by_sku_optimized never created an entry for them.
            if latest_fba_by_sku:
                existing_skus = {item.get("sku_code") for item in combined_data if item.get("sku_code")}
                fba_only_skus = {sku for sku in latest_fba_by_sku if sku not in existing_skus}
                if fba_only_skus:
                    fba_only_product_data = await report_service.batch_load_all_product_data(fba_only_skus)
                    all_product_data.update(fba_only_product_data)
                    for _s, _pd in fba_only_product_data.items():
                        _r = _pd.get("rate")
                        if _r is not None:
                            try:
                                product_rates[_s] = float(_r)
                            except (ValueError, TypeError):
                                pass
                    for sku in sorted(fba_only_skus):
                        pdata = fba_only_product_data.get(sku, {})
                        # Respect brand filter if active
                        stub_brand = pdata.get("brand", "") or ""
                        if brand:
                            if brand.lower() == "petfest":
                                if stub_brand.lower() not in {"dogfest", "catfest"}:
                                    continue
                            elif stub_brand.lower() != brand.lower():
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
                            "hsn_or_sac": pdata.get("hsn_or_sac", ""),
                            "is_combo_product": pdata.get("is_combo_product", False),
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

            # Inject stub rows for any brand products that still have no entry (zero sales,
            # zero stock) so that every product for the requested brand always appears.
            if brand:
                existing_skus = {item.get("sku_code") for item in combined_data if item.get("sku_code")}

                def _fetch_brand_skus():
                    products_collection = report_service.database.get_collection("products")
                    if brand.lower() == "petfest":
                        brand_filter = {"brand": {"$in": ["Dogfest", "Catfest"]}}
                    else:
                        brand_filter = {"brand": {"$regex": f"^{brand}$", "$options": "i"}}
                    return list(products_collection.find(
                        brand_filter,
                        {"cf_sku_code": 1, "_id": 0},
                    ))

                brand_products_raw = await asyncio.to_thread(_fetch_brand_skus)
                brand_sku_set = {
                    doc["cf_sku_code"] for doc in brand_products_raw
                    if doc.get("cf_sku_code") and doc["cf_sku_code"] not in existing_skus
                }

                if brand_sku_set:
                    brand_only_product_data = await report_service.batch_load_all_product_data(brand_sku_set)
                    all_product_data.update(brand_only_product_data)
                    for _s, _pd in brand_only_product_data.items():
                        _r = _pd.get("rate")
                        if _r is not None:
                            try:
                                product_rates[_s] = float(_r)
                            except (ValueError, TypeError):
                                pass
                    injected = 0
                    for sku in sorted(brand_sku_set):
                        pdata = brand_only_product_data.get(sku, {})
                        stub_brand = pdata.get("brand", "") or ""
                        name = pdata.get("name") or "Unknown Item"
                        product_brands[sku] = stub_brand
                        logistics_data[sku] = {
                            "cbm": pdata.get("cbm", 0) or 0,
                            "case_pack": pdata.get("case_pack", 0) or 0,
                            "purchase_status": pdata.get("purchase_status", ""),
                            "stock_in_transit_1": pdata.get("stock_in_transit_1", 0) or 0,
                            "stock_in_transit_2": pdata.get("stock_in_transit_2", 0) or 0,
                            "stock_in_transit_3": pdata.get("stock_in_transit_3", 0) or 0,
                            "is_new": pdata.get("is_new", False),
                            "hsn_or_sac": pdata.get("hsn_or_sac", ""),
                            "is_combo_product": pdata.get("is_combo_product", False),
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
                                "total_closing_stock": 0.0,
                                "total_days_in_stock": 0.0,
                                "avg_daily_run_rate": 0.0,
                                "avg_days_of_coverage": 0.0,
                                "fba_closing_stock": 0.0,
                                "pupscribe_wh_stock": 0.0,
                                "transfer_orders": 0.0,
                                "total_sales": 0.0,
                            },
                            "in_stock": False,
                            "drr_source": "current_period",
                            "drr_lookback_period": "",
                            "highlight": None,
                        }
                        combined_data.append(stub)
                        injected += 1
                    logger.info(f"Injected {injected} zero-activity brand stub items into combined_data")

            # ── Second-pass DRR lookback for stub items ──────────────────────────────
            # FBA-only and brand stubs are injected AFTER the first lookback pass, so
            # they never enter skus_needing_lookback_list.  Run a targeted lookback now
            # for any stub with days_in_stock < 60 (which is all of them).
            if period_days >= 90 and not dashboard_mode:
                stub_skus_needing_lookback = [
                    item.get("sku_code", "")
                    for item in combined_data
                    if item.get("sku_code")
                    and item.get("combined_metrics", {}).get("total_days_in_stock", 0) < 60
                    and item.get("drr_source") == "current_period"
                    # Only target items that were never included in the first lookback
                    and item.get("sku_code") not in set(skus_needing_lookback_list)
                ]
                if stub_skus_needing_lookback:
                    logger.info(
                        f"Second-pass DRR lookback for {len(stub_skus_needing_lookback)} stub SKUs "
                        f"(injected after first lookback)"
                    )
                    try:
                        stub_sku_to_item_id = await report_service.fetch_sku_to_item_id_map(
                            set(stub_skus_needing_lookback)
                        )
                        stub_skus_with_ids = {
                            sku: stub_sku_to_item_id[sku]
                            for sku in stub_skus_needing_lookback
                            if sku in stub_sku_to_item_id
                        }
                        if stub_skus_with_ids:
                            stub_lookback_results = await report_service.fetch_previous_period_drr(
                                stub_skus_with_ids, start_date, period_days
                            )
                            for item in combined_data:
                                sku = item.get("sku_code", "")
                                if sku in stub_lookback_results:
                                    report_service._apply_lookback_to_item(item, stub_lookback_results[sku], start_date)
                            logger.info(
                                f"Stub second-pass lookback complete: "
                                f"{sum(1 for r in stub_lookback_results.values() if r['found'])} found, "
                                f"{sum(1 for r in stub_lookback_results.values() if not r['found'])} not found"
                            )
                    except Exception as _e:
                        logger.error(f"Stub second-pass DRR lookback error: {_e}")

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

            # Await vendor-filtered PO unit prices (started after product_brands was built)
            po_prices_by_sku: Dict[str, Dict] = {}
            if _po_prices_task is not None:
                try:
                    po_prices_by_sku = await asyncio.wait_for(_po_prices_task, timeout=30.0)
                    logger.info(f"PO unit prices resolved for {len(po_prices_by_sku)} SKUs")
                except Exception as _e:
                    logger.error(f"PO unit prices fetch failed: {_e}")
                    if not _po_prices_task.done():
                        _po_prices_task.cancel()

            # Attach latest current stock, PO unit prices, manufacturer code, and Etrade VC data
            for item in combined_data:
                sku = item.get("sku_code", "")
                lz = max(0, round(latest_zoho_by_sku.get(sku, 0), 2))
                lf = round(latest_fba_by_sku.get(sku, 0), 2)
                item["latest_zoho_stock"] = lz
                item["latest_fba_stock"] = lf
                item["latest_total_stock"] = round(lz + lf, 2)
                item["prev_zoho_stock"] = max(0, round(prev_zoho_by_sku.get(sku, 0), 2))
                item["prev_fba_stock"] = round(prev_fba_by_sku.get(sku, 0), 2)
                _pdata = all_product_data.get(sku, {})
                _po_price = po_prices_by_sku.get(sku, {})
                item["unit_price"] = _po_price.get("rate") or _pdata.get("purchase_price", 0) or 0
                item["unit_price_currency"] = _po_price.get("currency_code") or _pdata.get("currency", "") or ""
                item["manufacturer_code"] = all_product_data.get(sku, {}).get("manufacturer_code", "")
                item["mrp"] = product_rates.get(sku)
                item["brand"] = product_brands.get(sku, "")
                item["category"] = _pdata.get("category", "") or ""
                item["sub_category"] = _pdata.get("sub_category", "") or ""
                item["series"] = _pdata.get("series", "") or ""
                item["sku"] = _pdata.get("sku", "") or ""
                # Blinkit inventory
                item["blinkit_inventory"] = round(latest_blinkit_by_sku.get(sku, 0), 2)
                # COGS / inventory valuation (Pupscribe WH, Zoho Books)
                _cogs = cogs_by_sku.get(sku, {})
                item["cogs_unit_cost"] = _cogs.get("unit_cost", 0.0)
                item["cogs_asset_value"] = _cogs.get("asset_value", 0.0)
                item["cogs_qty"] = _cogs.get("qty", 0)
                # Platform live flags
                item["live_on_amazon"] = sku in amazon_sku_set
                item["live_on_blinkit"] = sku in blinkit_sku_set
                item["amazon_final_drr"] = amazon_final_drr_by_sku.get(sku, 0.0)
                # Etrade (Vendor Central) inventory
                vc_data = vc_by_sku.get(sku, {})
                vc_stock = round(vc_data.get("closing_stock", 0), 2)
                vc_drr = vc_data.get("drr", 0)
                metrics_ref = item.get("combined_metrics", {})
                metrics_ref["etrade_inventory"] = vc_stock
                metrics_ref["etrade_drr"] = round(vc_drr, 4)
                metrics_ref["etrade_days_inventory_lasts"] = round(vc_stock / vc_drr, 2) if vc_drr > 0 else 0.0

            # Compute coverage and order quantities once, using latest DB stock
            # (latest_total_stock was attached in the loop above).
            report_service._compute_order_quantities(combined_data)

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
            if not _amazon_sku_task.done():
                _amazon_sku_task.cancel()
            if not _blinkit_sku_task.done():
                _blinkit_sku_task.cancel()
            if not _amazon_drr_task.done():
                _amazon_drr_task.cancel()

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
                "blinkit": latest_blinkit_inv_date,
                "cogs": cogs_date,
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

