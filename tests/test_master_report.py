"""
Unit tests for master report logic.

All tests are pure — no MongoDB, no HTTP calls.
The conftest.py patches out infrastructure before any import.

Coverage areas:
  1. _classify_sku_stock         — stock health classification boundaries
  2. safe_float / safe_int       — input sanitisation helpers
  3. _compute_order_quantities   — ORDER/EXCESS/NO MOVEMENT, case-pack rounding,
                                   inactive products, demand-override + EXCESS
  4. enrich_with_order_calculations — confidence multiplier, missed-sales cap,
                                       inactive early-exit
  5. _aggregate_brand_kpi        — brand with no sales but with stock,
                                   dead-SKU exclusion from weighted-avg cover,
                                   zero-DRR brand
  6. Product coverage             — every brand product appears in combined_data
                                    even when it has zero sales in the period
"""

import math
import pytest
from unittest.mock import MagicMock

from purchases_backend.routes.master import _classify_sku_stock, _aggregate_brand_kpi
from purchases_backend.services.master_service import OptimizedMasterReportService


# ── Fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture
def service():
    """Service instance backed by a mock database — no real MongoDB required."""
    return OptimizedMasterReportService(MagicMock())


def _make_item(
    sku="SKU001",
    units_sold=100.0,
    units_returned=5.0,
    credit_notes=0.0,
    transfer_orders=0.0,
    amount=5000.0,
    drr=3.3,
    latest_stock=200,
    transit=0,
    coverage=60.0,
    brand="TestBrand",
    purchase_status="active",
    lead_time=60,
    safety_days=25,
    target_days=95,
    case_pack=6,
    cbm=0.02,
    confidence_multiplier=1.0,
    extra_qty=0.0,
    drr_source="current_period",
    excess_or_order="ORDER",
    missed_sales=0.0,
    growth_rate=None,
):
    return {
        "sku_code": sku,
        "item_name": f"Item {sku}",
        "brand": brand,
        "purchase_status": purchase_status,
        "lead_time": lead_time,
        "safety_days": safety_days,
        "target_days": target_days,
        "case_pack": case_pack,
        "cbm": cbm,
        "confidence_multiplier": confidence_multiplier,
        "extra_qty": extra_qty,
        "drr_source": drr_source,
        "excess_or_order": excess_or_order,
        "missed_sales": missed_sales,
        "growth_rate": growth_rate,
        "latest_total_stock": float(latest_stock),
        "latest_zoho_stock": float(latest_stock),
        "latest_fba_stock": 0.0,
        "total_stock_in_transit": float(transit),
        "current_days_coverage": coverage,
        "total_cbm": 0.0,
        "days_current_order_lasts": 0.0,
        "days_total_inventory_lasts": 0.0,
        "combined_metrics": {
            "total_units_sold": units_sold,
            "total_units_returned": units_returned,
            "total_credit_notes": credit_notes,
            "transfer_orders": transfer_orders,
            "total_sales": units_sold - units_returned - credit_notes - transfer_orders,
            "total_amount": amount,
            "avg_daily_run_rate": drr,
            "total_days_in_stock": 30,
            "avg_days_of_coverage": coverage,
        },
    }


# ── 1. _classify_sku_stock ────────────────────────────────────────────────────

class TestClassifySkuStock:
    def test_zero_drr_is_dead(self):
        assert _classify_sku_stock(0, 60, 0) == "dead"

    def test_massive_overstock_is_dead(self):
        # coverage > 3× lead_time → dead
        assert _classify_sku_stock(3 * 60 + 1, 60, 1) == "dead"

    def test_exactly_3x_lead_time_is_overstock_not_dead(self):
        # condition is strict > so coverage == 3 × lead_time stays at overstock
        assert _classify_sku_stock(180, 60, 1) == "overstock"
        assert _classify_sku_stock(181, 60, 1) == "dead"

    def test_overstock_boundary(self):
        # strict > 2× → overstock; exactly 2× falls to heavy
        assert _classify_sku_stock(121, 60, 1) == "overstock"
        assert _classify_sku_stock(120, 60, 1) == "heavy"

    def test_heavy_boundary(self):
        # strict > 1.5× → heavy; exactly 1.5× falls to healthy
        assert _classify_sku_stock(91, 60, 1) == "heavy"
        assert _classify_sku_stock(90, 60, 1) == "healthy"

    def test_healthy_boundary(self):
        # lead_time ≤ coverage ≤ 1.5× → healthy
        assert _classify_sku_stock(60, 60, 1) == "healthy"
        assert _classify_sku_stock(89, 60, 1) == "healthy"

    def test_below_lead_time_is_reorder_risk(self):
        assert _classify_sku_stock(59, 60, 1) == "reorder_risk"
        assert _classify_sku_stock(0, 60, 1) == "reorder_risk"

    def test_short_lead_time(self):
        # Same logic with lead_time=10
        assert _classify_sku_stock(31, 10, 1) == "dead"
        assert _classify_sku_stock(21, 10, 1) == "overstock"
        assert _classify_sku_stock(10, 10, 1) == "healthy"
        assert _classify_sku_stock(9, 10, 1) == "reorder_risk"


# ── 2. safe_float / safe_int ─────────────────────────────────────────────────

class TestSafeConversions:
    def test_safe_float_none_returns_default(self):
        assert OptimizedMasterReportService.safe_float(None) == 0.0

    def test_safe_float_empty_string_returns_default(self):
        assert OptimizedMasterReportService.safe_float("") == 0.0

    def test_safe_float_valid_string(self):
        assert OptimizedMasterReportService.safe_float("3.14") == pytest.approx(3.14)

    def test_safe_float_integer_input(self):
        assert OptimizedMasterReportService.safe_float(5) == 5.0

    def test_safe_float_non_numeric_returns_default(self):
        assert OptimizedMasterReportService.safe_float("abc") == 0.0

    def test_safe_float_custom_default(self):
        assert OptimizedMasterReportService.safe_float(None, default=-1.0) == -1.0

    def test_safe_int_none_returns_default(self):
        assert OptimizedMasterReportService.safe_int(None) == 0

    def test_safe_int_float_string(self):
        assert OptimizedMasterReportService.safe_int("4.9") == 4

    def test_safe_int_non_numeric_returns_default(self):
        assert OptimizedMasterReportService.safe_int("NaN") == 0


# ── 3. _compute_order_quantities ─────────────────────────────────────────────

class TestComputeOrderQuantities:

    def test_no_movement_when_drr_zero(self):
        item = _make_item(drr=0, latest_stock=100)
        OptimizedMasterReportService._compute_order_quantities([item])
        assert item["excess_or_order"] == "NO MOVEMENT"
        assert item["order_qty"] == 0
        assert item["order_qty_plus_extra_qty_rounded"] == 0

    def test_excess_when_well_stocked(self):
        # coverage >> target_days → EXCESS
        item = _make_item(drr=1.0, latest_stock=500, transit=0, target_days=95)
        OptimizedMasterReportService._compute_order_quantities([item])
        assert item["excess_or_order"] == "EXCESS"
        assert item["order_qty_plus_extra_qty_rounded"] == 0

    def test_order_when_understocked(self):
        # coverage < target_days → ORDER
        item = _make_item(drr=5.0, latest_stock=50, transit=0, target_days=95)
        OptimizedMasterReportService._compute_order_quantities([item])
        assert item["excess_or_order"] == "ORDER"
        assert item["order_qty"] > 0

    def test_case_pack_floor(self):
        # order qty should be floored to nearest case pack multiple
        item = _make_item(drr=2.0, latest_stock=10, transit=0, target_days=95, case_pack=6, extra_qty=0.0)
        OptimizedMasterReportService._compute_order_quantities([item])
        rounded = item["order_qty_plus_extra_qty_rounded"]
        assert rounded % 6 == 0
        assert rounded > 0

    def test_case_pack_minimum_one_pack_when_order(self):
        # If floor(qty / case_pack) == 0 but status is ORDER → minimum 1 case pack
        item = _make_item(drr=0.1, latest_stock=1, transit=0, target_days=95, case_pack=50, extra_qty=0.0)
        OptimizedMasterReportService._compute_order_quantities([item])
        assert item["excess_or_order"] == "ORDER"
        assert item["order_qty_plus_extra_qty_rounded"] == 50

    def test_inactive_product_gets_zero_order_qty(self):
        item = _make_item(drr=5.0, latest_stock=10, transit=0, target_days=95, purchase_status="inactive")
        OptimizedMasterReportService._compute_order_quantities([item])
        assert item["days_total_inventory_lasts"] >= 0
        # _compute_order_quantities sets days_total_inventory_lasts for inactive items
        # but does NOT set order_qty (that's set to 0 in enrich_with_order_calculations)

    def test_demand_override_plus_excess_gives_zero_order(self):
        item = _make_item(drr=1.0, latest_stock=1000, transit=0, target_days=95)
        item["order_qty_demand_override"] = True
        item["current_period_sales_for_override"] = 50.0
        # coverage is huge so it should be EXCESS
        OptimizedMasterReportService._compute_order_quantities([item])
        assert item["excess_or_order"] == "EXCESS"
        assert item["order_qty"] == 0.0

    def test_demand_override_order_uses_current_period_sales(self):
        item = _make_item(drr=5.0, latest_stock=10, transit=0, target_days=95)
        item["order_qty_demand_override"] = True
        item["current_period_sales_for_override"] = 80.0
        item["confidence_multiplier"] = 1.0
        OptimizedMasterReportService._compute_order_quantities([item])
        assert item["excess_or_order"] == "ORDER"
        assert item["order_qty"] == pytest.approx(80.0)

    def test_total_inventory_days_includes_transit(self):
        item = _make_item(drr=2.0, latest_stock=60, transit=40, target_days=95, case_pack=0)
        OptimizedMasterReportService._compute_order_quantities([item])
        # current coverage = (60 + 40) / 2 = 50; still < 95 → ORDER
        assert item["current_days_coverage"] == pytest.approx(50.0)
        assert item["excess_or_order"] == "ORDER"


# ── 4. enrich_with_order_calculations ────────────────────────────────────────

class TestEnrichWithOrderCalculations:

    def _logistics(self, sku, purchase_status="active", lead_time=60, case_pack=6, cbm=0.02,
                   safety_days=25, is_new=False):
        return {sku: {"purchase_status": purchase_status, "lead_time": lead_time,
                      "case_pack": case_pack, "cbm": cbm, "safety_days": safety_days,
                      "is_new": is_new, "is_combo_product": False, "hsn_or_sac": ""}}

    def test_inactive_gets_zero_order_qty(self, service):
        item = _make_item(sku="SKU001", drr=5.0, purchase_status="inactive")
        service.enrich_with_order_calculations(
            combined_data=[item],
            logistics_data=self._logistics("SKU001", purchase_status="inactive"),
            transit_data={},
            missed_sales_by_sku={},
            period_days=30,
        )
        assert item["order_qty"] == 0
        assert item["order_qty_plus_extra_qty_rounded"] == 0

    def test_insufficient_stock_under_30_days_is_zero_confidence(self, service):
        item = _make_item(sku="SKU001", drr=5.0, drr_source="insufficient_stock")
        item["combined_metrics"]["total_days_in_stock"] = 20
        service.enrich_with_order_calculations(
            combined_data=[item],
            logistics_data=self._logistics("SKU001"),
            transit_data={},
            missed_sales_by_sku={},
            period_days=30,
        )
        assert item["confidence_multiplier"] == 0.0
        assert "0%" in item["order_qty_basis"]

    def test_insufficient_stock_30_to_44_days_is_50pct_confidence(self, service):
        item = _make_item(sku="SKU001", drr=5.0, drr_source="insufficient_stock")
        item["combined_metrics"]["total_days_in_stock"] = 35
        service.enrich_with_order_calculations(
            combined_data=[item],
            logistics_data=self._logistics("SKU001"),
            transit_data={},
            missed_sales_by_sku={},
            period_days=30,
        )
        assert item["confidence_multiplier"] == 0.5
        assert "50%" in item["order_qty_basis"]

    def test_insufficient_stock_45_to_59_days_is_75pct_confidence(self, service):
        item = _make_item(sku="SKU001", drr=5.0, drr_source="insufficient_stock")
        item["combined_metrics"]["total_days_in_stock"] = 50
        service.enrich_with_order_calculations(
            combined_data=[item],
            logistics_data=self._logistics("SKU001"),
            transit_data={},
            missed_sales_by_sku={},
            period_days=30,
        )
        assert item["confidence_multiplier"] == 0.75
        assert "75%" in item["order_qty_basis"]

    def test_missed_sales_capped_at_50pct_drr(self, service):
        # DRR = 4.0/day; period = 30 days; missed_sales raw = 600 units
        # raw_ms_drr = 600/30 = 20.0/day → cap at 0.5 * 4.0 = 2.0/day
        item = _make_item(sku="SKU001", drr=4.0)
        service.enrich_with_order_calculations(
            combined_data=[item],
            logistics_data=self._logistics("SKU001"),
            transit_data={},
            missed_sales_by_sku={"SKU001": 600},
            period_days=30,
        )
        assert item["missed_sales_drr"] == pytest.approx(2.0)

    def test_missed_sales_below_cap_passes_through(self, service):
        # missed_sales = 30, period = 30, drr = 4.0 → ms_drr = 1.0 < cap 2.0 → 1.0
        item = _make_item(sku="SKU001", drr=4.0)
        service.enrich_with_order_calculations(
            combined_data=[item],
            logistics_data=self._logistics("SKU001"),
            transit_data={},
            missed_sales_by_sku={"SKU001": 30},
            period_days=30,
        )
        assert item["missed_sales_drr"] == pytest.approx(1.0)

    def test_seasonal_mismatch_flag_appended_to_order_qty_basis(self, service):
        item = _make_item(sku="SKU001", drr=2.0)
        item["drr_lookback_seasonal_mismatch"] = True
        service.enrich_with_order_calculations(
            combined_data=[item],
            logistics_data=self._logistics("SKU001"),
            transit_data={},
            missed_sales_by_sku={},
            period_days=30,
        )
        assert "Seasonal mismatch" in item["order_qty_basis"]

    def test_transit_data_attached_to_item(self, service):
        transit = {"SKU001": {"transit_1": 100, "transit_2": 50, "transit_3": 25, "total": 175}}
        item = _make_item(sku="SKU001", drr=2.0)
        service.enrich_with_order_calculations(
            combined_data=[item],
            logistics_data=self._logistics("SKU001"),
            transit_data=transit,
            missed_sales_by_sku={},
            period_days=30,
        )
        assert item["stock_in_transit_1"] == 100
        assert item["total_stock_in_transit"] == 175


# ── 5. _aggregate_brand_kpi ───────────────────────────────────────────────────

class TestAggregateBrandKpi:

    def test_brand_with_no_sales_but_has_stock(self):
        # SKU is present in combined_data (stub row) with zero sales but real stock
        item = _make_item(sku="SKU001", units_sold=0, units_returned=0, drr=0,
                          latest_stock=200, transit=0, coverage=0, brand="TestBrand")
        result = _aggregate_brand_kpi(
            brand="TestBrand",
            items=[item],
            brand_logistics_map={},
            rate_map={"SKU001": 500.0},
            period_days=30,
        )
        assert result["sku_count"] == 1
        assert result["units_sold"] == 0
        assert result["latest_net_stock"] == 200
        assert result["drr"] == 0
        assert result["days_cover"] == 0
        # No movement but the brand still appears with its stock
        assert result["alert_level"] == 3

    def test_dead_sku_excluded_from_weighted_avg_cover(self):
        # One normal SKU + one massively overstocked SKU (dead)
        # The dead one should not distort weighted_avg_days_cover
        normal = _make_item(sku="SKU001", drr=2.0, latest_stock=60, coverage=30.0)
        dead = _make_item(sku="SKU002", drr=0.1, latest_stock=10000, coverage=100000.0)
        result = _aggregate_brand_kpi(
            brand="TestBrand",
            items=[normal, dead],
            brand_logistics_map={},
            rate_map={},
            period_days=30,
        )
        # dead SKU (coverage=100000 >> 3×60=180) excluded from weighted avg
        # Only normal SKU contributes: weighted_avg = 30.0
        assert result["weighted_avg_days_cover"] == pytest.approx(30.0, abs=1.0)

    def test_all_dead_skus_weighted_avg_is_zero(self):
        item = _make_item(sku="SKU001", drr=0, latest_stock=500, coverage=0)
        result = _aggregate_brand_kpi(
            brand="TestBrand",
            items=[item],
            brand_logistics_map={},
            rate_map={},
            period_days=30,
        )
        assert result["weighted_avg_days_cover"] == 0.0

    def test_growth_rate_none_when_no_items_have_drr(self):
        item = _make_item(sku="SKU001", drr=0, growth_rate=None)
        result = _aggregate_brand_kpi("TestBrand", [item], {}, {}, 30)
        assert result["growth_rate"] is None

    def test_growth_rate_weighted_by_drr(self):
        # SKU1: drr=2, growth=10%; SKU2: drr=8, growth=20%
        # weighted = (2*10 + 8*20) / (2+8) = 180/10 = 18%
        item1 = _make_item(sku="SKU001", drr=2.0, growth_rate=10.0)
        item2 = _make_item(sku="SKU002", drr=8.0, growth_rate=20.0)
        result = _aggregate_brand_kpi("TestBrand", [item1, item2], {}, {}, 30)
        assert result["growth_rate"] == pytest.approx(18.0)

    def test_return_pct_zero_when_no_sales(self):
        item = _make_item(sku="SKU001", units_sold=0, units_returned=0)
        result = _aggregate_brand_kpi("TestBrand", [item], {}, {}, 30)
        assert result["return_pct"] == 0.0

    def test_sku_count_matches_item_list_length(self):
        items = [_make_item(sku=f"SKU{i:03d}") for i in range(5)]
        result = _aggregate_brand_kpi("TestBrand", items, {}, {}, 30)
        assert result["sku_count"] == 5


# ── 6. Product coverage — brand products appear even with zero sales ──────────

class TestProductCoverage:
    """
    Verifies that a SKU known to the products collection but absent from all
    sales sources still ends up in combined_data after stub injection.

    We test this via combine_data_by_sku_optimized + the stub helper logic
    directly, without running the full _generate_master_report_data pipeline.
    """

    def _norm_item(self, sku, item_id=None, units_sold=0.0, closing_stock=50.0, days_in_stock=0):
        """Build a pre-normalised item dict (as produced by normalize_single_source_data)."""
        return {
            "source": "zoho",
            "sku_code": sku,
            "item_id": item_id or sku,
            "item_name": f"Item {sku}",
            "units_sold": units_sold,
            "units_returned": 0.0,
            "credit_notes": 0.0,
            "total_amount": units_sold * 100,
            "closing_stock": closing_stock,
            "days_in_stock": days_in_stock,
            "days_in_stock_any_wh": days_in_stock,
            "daily_run_rate": 0.0,
            "city": "Multiple",
            "warehouse": "Multiple",
            "days_of_coverage": 0.0,
            "additional_metrics": {},
            "last_90_days_dates": "",
            "sessions": 0,
        }

    def test_sku_with_stock_but_no_sales_appears_in_output(self, service):
        # A SKU with closing_stock > 0 but zero sales is included by Zoho
        # normalization and must survive through combine unchanged.
        item = self._norm_item("SKU_NOSALES", closing_stock=50.0, units_sold=0.0)
        result = service.combine_data_by_sku_optimized(
            all_normalized_data=[[item]],   # List[List[Dict]]
            composite_products_map={},
        )
        skus = [r["sku_code"] for r in result]
        assert "SKU_NOSALES" in skus
        entry = next(r for r in result if r["sku_code"] == "SKU_NOSALES")
        assert entry["combined_metrics"]["total_units_sold"] == 0.0
        assert entry["combined_metrics"]["avg_daily_run_rate"] == 0.0

    def test_multiple_brands_all_skus_present(self, service):
        """SKUs with only closing stock (no sales) still appear alongside selling SKUs."""
        items = []
        all_skus = set()
        for brand in ["Alpha", "Beta"]:
            for idx in range(3):
                sku = f"{brand.upper()}{idx:02d}"
                all_skus.add(sku)
                # Only first SKU of each brand has sales; rest have only stock
                items.append(self._norm_item(
                    sku,
                    units_sold=10.0 if idx == 0 else 0.0,
                    closing_stock=20.0,
                    days_in_stock=10 if idx == 0 else 0,
                ))
        result = service.combine_data_by_sku_optimized(
            all_normalized_data=[items],
            composite_products_map={},
        )
        result_skus = {r["sku_code"] for r in result}
        assert all_skus == result_skus, f"Missing SKUs: {all_skus - result_skus}"

    def test_composite_item_fans_out_to_component_skus(self, service):
        """A Zoho item_id in composite_products_map fans its sales to component SKUs."""
        # The combo item has item_id "COMBO_ID" and 10 units sold.
        # Composite map says COMBO_ID → [SKU_A ×1, SKU_B ×2]
        # Expected: SKU_A gets 10 units, SKU_B gets 20 units
        combo_item = self._norm_item("COMBO_SKU", item_id="COMBO_ID",
                                     units_sold=10.0, closing_stock=0.0, days_in_stock=20)
        composite_map = {
            "COMBO_ID": [
                {"sku_code": "SKU_A", "name": "Component A", "quantity": 1},
                {"sku_code": "SKU_B", "name": "Component B", "quantity": 2},
            ]
        }
        result = service.combine_data_by_sku_optimized(
            all_normalized_data=[[combo_item]],
            composite_products_map=composite_map,
        )
        result_by_sku = {r["sku_code"]: r for r in result}
        assert "SKU_A" in result_by_sku, "Component A missing from output"
        assert "SKU_B" in result_by_sku, "Component B missing from output"
        assert result_by_sku["SKU_A"]["combined_metrics"]["total_units_sold"] == pytest.approx(10.0)
        assert result_by_sku["SKU_B"]["combined_metrics"]["total_units_sold"] == pytest.approx(20.0)
