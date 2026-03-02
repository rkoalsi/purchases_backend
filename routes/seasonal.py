import asyncio
import io
import logging
from datetime import datetime
from typing import Dict, List

from fastapi import APIRouter, Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

from ..database import get_database

logger = logging.getLogger(__name__)
router = APIRouter()

MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
               "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# ---------------------------------------------------------------------------
# Data fetchers
# ---------------------------------------------------------------------------

async def _fetch_products(db, brand: str = None) -> Dict[str, Dict]:
    """Return {sku_code: {name, item_id, brand}} optionally filtered by brand."""
    products_collection = db["products"]
    query: dict = {}
    if brand:
        query["brand"] = {"$regex": f"^{brand}$", "$options": "i"}

    def _run():
        return list(products_collection.find(
            query,
            {"cf_sku_code": 1, "item_id": 1, "name": 1, "brand": 1, "purchase_status": 1, "_id": 0},
        ))

    docs = await asyncio.to_thread(_run)
    result: Dict[str, Dict] = {}
    for doc in docs:
        sku = doc.get("cf_sku_code")
        item_id = doc.get("item_id")
        if sku and item_id:
            result[sku] = {
                "name": doc.get("name", "Unknown"),
                "item_id": str(item_id),
                "brand": doc.get("brand", ""),
                "purchase_status": doc.get("purchase_status", ""),
            }
    return result


def _composite_expansion_stages(target_ids: List[str]) -> list:
    """Shared pipeline stages that unwind line items and expand composites."""
    return [
        {"$unwind": "$line_items"},
        {
            "$lookup": {
                "from": "composite_products",
                "localField": "line_items.item_id",
                "foreignField": "composite_item_id",
                "as": "composite_match",
            }
        },
        {
            "$addFields": {
                "composite_info": {"$arrayElemAt": ["$composite_match", 0]},
            }
        },
        {
            "$project": {
                "date": 1,
                "items_to_process": {
                    "$cond": [
                        {"$gt": [{"$size": "$composite_match"}, 0]},
                        {
                            "$map": {
                                "input": "$composite_info.components",
                                "as": "comp",
                                "in": {
                                    "item_id": "$$comp.item_id",
                                    "quantity": {
                                        "$multiply": [
                                            "$line_items.quantity",
                                            "$$comp.quantity",
                                        ]
                                    },
                                    "date": "$date",
                                },
                            }
                        },
                        [
                            {
                                "item_id": "$line_items.item_id",
                                "quantity": "$line_items.quantity",
                                "date": "$date",
                            }
                        ],
                    ]
                },
            }
        },
        {"$unwind": "$items_to_process"},
        {"$replaceRoot": {"newRoot": "$items_to_process"}},
        {"$match": {"item_id": {"$in": target_ids}}},
    ]


async def _fetch_base_drr(
    db,
    sku_to_item_id: Dict[str, str],
    start_date: str,
    end_date: str,
    period_days: int,
) -> Dict[str, float]:
    """Base DRR = total Zoho invoice units in the report period / period_days."""
    if not sku_to_item_id or period_days <= 0:
        return {}

    invoices_collection = db["invoices"]
    target_ids = list(sku_to_item_id.values())

    pipeline = [
        {
            "$match": {
                "date": {"$gte": start_date, "$lte": end_date},
                "status": {"$nin": ["draft", "void"]},
            }
        },
        *_composite_expansion_stages(target_ids),
        {
            "$group": {
                "_id": "$item_id",
                "total_units": {"$sum": "$quantity"},
            }
        },
    ]

    def _run():
        return list(invoices_collection.aggregate(pipeline, allowDiskUse=True))

    docs = await asyncio.to_thread(_run)
    item_id_to_sku = {v: k for k, v in sku_to_item_id.items()}

    result: Dict[str, float] = {}
    for doc in docs:
        item_id = str(doc["_id"])
        sku = item_id_to_sku.get(item_id)
        if sku:
            units = float(doc.get("total_units", 0))
            result[sku] = round(units / period_days, 4)
    return result


async def _fetch_monthly_avg_sales(
    db,
    sku_to_item_id: Dict[str, str],
    end_date: str,
) -> Dict[str, Dict[int, float]]:
    """
    For each SKU return {month_num: avg_units} averaged across years.
    Looks back 2 full years from end_date.
    """
    if not sku_to_item_id:
        return {}

    invoices_collection = db["invoices"]
    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
    lookback_start = end_dt.replace(year=end_dt.year - 2).strftime("%Y-%m-%d")
    target_ids = [str(iid) for iid in sku_to_item_id.values()]

    pipeline = [
        {
            "$match": {
                "date": {"$gte": lookback_start, "$lte": end_date},
                "status": {"$nin": ["draft", "void"]},
            }
        },
        *_composite_expansion_stages(target_ids),
        {
            "$addFields": {
                "parsed_date": {
                    "$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "item_id": "$item_id",
                    "year": {"$year": "$parsed_date"},
                    "month": {"$month": "$parsed_date"},
                },
                "units": {"$sum": "$quantity"},
            }
        },
    ]

    def _run():
        raw: Dict[str, Dict[int, Dict[int, float]]] = {}
        for doc in invoices_collection.aggregate(pipeline, allowDiskUse=True):
            item_id = str(doc["_id"]["item_id"])
            year = doc["_id"]["year"]
            month = doc["_id"]["month"]
            units = float(doc.get("units", 0))
            raw.setdefault(item_id, {}).setdefault(year, {})[month] = units
        return raw

    raw = await asyncio.to_thread(_run)
    item_id_to_sku = {str(v): k for k, v in sku_to_item_id.items()}

    result: Dict[str, Dict[int, float]] = {}
    for item_id, year_data in raw.items():
        sku = item_id_to_sku.get(item_id)
        if not sku:
            continue
        monthly_avg: Dict[int, float] = {}
        for month in range(1, 13):
            values = [
                year_data[yr][month]
                for yr in year_data
                if month in year_data[yr]
            ]
            if values:
                monthly_avg[month] = round(sum(values) / len(values), 4)
        if monthly_avg:
            result[sku] = monthly_avg

    return result


# ---------------------------------------------------------------------------
# Excel builder
# ---------------------------------------------------------------------------

# Column definitions: (header label, width)
_COLUMNS = [
    ("SKU Code",                               14),
    ("Item Name",                              32),
    ("Brand",                                  16),
    ("Status",                                 18),
    ("Month",                                  8),
    ("Historical Avg Units\n(Same Month, Last 2 Yrs)", 20),
    ("Overall Monthly Avg\n= SUM(Jan..Dec Avg) / 12",   20),
    ("Seasonal Index\n= Month Avg / Overall Avg",       18),
    ("Base DRR\n(Period Units / Period Days)",           18),
    ("Seasonal DRR\n= Base DRR × Seasonal Index",       20),
]

# Header fill colours per column (0-based index)
_HEADER_COLOURS = [
    "1F4E79",  # SKU Code     – dark blue
    "1F4E79",  # Item Name
    "1F4E79",  # Brand
    "1F4E79",  # Status
    "1F4E79",  # Month
    "2E75B6",  # Hist Avg     – mid blue
    "2E75B6",  # Overall Avg
    "375623",  # Seasonal Idx – dark green
    "C55A11",  # Base DRR     – orange
    "7030A0",  # Seasonal DRR – purple
]

# Alternating row shades for even/odd SKU blocks
_SHADE_EVEN = "EBF3FB"
_SHADE_ODD  = "FDFEFE"


def _cell_fill(colour: str) -> PatternFill:
    return PatternFill(start_color=colour, end_color=colour, fill_type="solid")


def _build_excel(
    products: Dict[str, Dict],
    base_drr_by_sku: Dict[str, float],
    monthly_avg_by_sku: Dict[str, Dict[int, float]],
    start_date: str,
    end_date: str,
) -> io.BytesIO:
    wb = Workbook()
    ws = wb.active
    ws.title = "Seasonal Report"

    center = Alignment(horizontal="center", vertical="center", wrap_text=True)
    left   = Alignment(horizontal="left",   vertical="center", wrap_text=True)

    # ── header row ──────────────────────────────────────────────────────────
    ws.row_dimensions[1].height = 48
    for col_idx, ((label, width), colour) in enumerate(
        zip(_COLUMNS, _HEADER_COLOURS), start=1
    ):
        cell = ws.cell(row=1, column=col_idx, value=label)
        cell.font      = Font(bold=True, color="FFFFFF", size=10)
        cell.fill      = _cell_fill(colour)
        cell.alignment = center
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    # Freeze header
    ws.freeze_panes = "A2"

    # ── legend rows (rows 2-3) ───────────────────────────────────────────────
    legend_fill = _cell_fill("FFF2CC")
    legend_font = Font(italic=True, size=9, color="595959")

    legend_lines = [
        "STEP 1: Historical Avg Units  → average of the same calendar month across the last 2 years of Zoho invoice data.",
        "STEP 2: Overall Monthly Avg   → =SUM(Jan Avg … Dec Avg)/12  (total of all 12 monthly averages divided by 12).",
        "STEP 3: Seasonal Index        → =Month Avg / Overall Monthly Avg   (1.0 = normal month; >1.0 = above average; <1.0 = below average).",
        "STEP 4: Base DRR              → Total Zoho units sold in the selected period / number of days in the period.",
        "STEP 5: Seasonal DRR          → =Base DRR × Seasonal Index   (projected daily run rate adjusted for seasonality).",
    ]
    for i, line in enumerate(legend_lines):
        r = 2 + i
        ws.row_dimensions[r].height = 15
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=len(_COLUMNS))
        cell = ws.cell(row=r, column=1, value=line)
        cell.font      = legend_font
        cell.fill      = legend_fill
        cell.alignment = Alignment(horizontal="left", vertical="center")

    # ── data rows start at row 7 (1 header + 5 legend) ─────────────────────
    DATA_START = 2 + len(legend_lines)  # = 7

    # Sort SKUs by brand then item name
    sorted_skus = sorted(
        products.keys(),
        key=lambda s: (products[s].get("brand", ""), products[s].get("name", "")),
    )

    current_row = DATA_START
    for sku_idx, sku in enumerate(sorted_skus):
        pdata           = products[sku]
        name            = pdata.get("name", "Unknown")
        brand           = pdata.get("brand", "")
        purchase_status = pdata.get("purchase_status", "")
        base_drr        = base_drr_by_sku.get(sku, 0.0)
        monthly_avg     = monthly_avg_by_sku.get(sku, {})

        block_start = current_row
        block_end   = current_row + 11  # 12 rows inclusive

        shade = _SHADE_EVEN if sku_idx % 2 == 0 else _SHADE_ODD
        row_fill = _cell_fill(shade)

        for month_num in range(1, 13):
            row     = current_row + month_num - 1
            avg_val = monthly_avg.get(month_num, 0.0)

            ws.cell(row=row, column=1, value=sku)
            ws.cell(row=row, column=2, value=name)
            ws.cell(row=row, column=3, value=brand)
            ws.cell(row=row, column=4, value=purchase_status)
            ws.cell(row=row, column=5, value=MONTH_NAMES[month_num - 1])

            # Col 6: Historical Avg Units (raw value)
            ws.cell(row=row, column=6, value=round(avg_val, 4))

            # Col 7: Overall Monthly Avg  →  Excel formula summing the block's 12 avg cells / 12
            ws.cell(row=row, column=7, value=f"=SUM($F${block_start}:$F${block_end})/12")

            # Col 8: Seasonal Index  →  Excel formula: Month Avg / Overall Avg
            ws.cell(row=row, column=8, value=f"=IF(G{row}=0,1,F{row}/G{row})")

            # Col 9: Base DRR (same for all months of this SKU)
            ws.cell(row=row, column=9, value=round(base_drr, 4))

            # Col 10: Seasonal DRR  →  Excel formula: Base DRR × Seasonal Index
            ws.cell(row=row, column=10, value=f"=I{row}*H{row}")

            # Styling
            for col in range(1, len(_COLUMNS) + 1):
                cell           = ws.cell(row=row, column=col)
                cell.fill      = row_fill
                cell.alignment = (left if col == 2 else center)
                cell.font      = Font(size=10)

        # Number formatting for formula columns
        for row in range(block_start, block_end + 1):
            ws.cell(row=row, column=7).number_format = "0.0000"
            ws.cell(row=row, column=8).number_format = "0.0000"
            ws.cell(row=row, column=10).number_format = "0.0000"

        # Thick bottom border to visually separate SKU blocks
        border_bottom = None
        try:
            from openpyxl.styles import Border, Side
            thin = Side(style="thin", color="BFBFBF")
            thick = Side(style="medium", color="595959")
            for col in range(1, len(_COLUMNS) + 1):
                cell = ws.cell(row=block_end, column=col)
                existing = cell.border
                cell.border = Border(
                    top=existing.top if existing else None,
                    left=existing.left if existing else None,
                    right=existing.right if existing else None,
                    bottom=thick,
                )
        except Exception:
            pass  # non-critical styling

        current_row += 12

    output = io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output


# ---------------------------------------------------------------------------
# Endpoint
# ---------------------------------------------------------------------------

@router.get("/download")
async def download_seasonal_report(
    start_date: str = Query(..., description="Start date YYYY-MM-DD"),
    end_date: str = Query(..., description="End date YYYY-MM-DD"),
    brand: str = Query(None, description="Filter by brand (optional)"),
    db=Depends(get_database),
):
    """
    Download the Seasonal DRR report as an Excel file.

    For each SKU, shows 12 rows (one per calendar month) with:
    - Historical average units sold (same month averaged across last 2 years)
    - Overall monthly average (Excel formula)
    - Seasonal Index (Excel formula)
    - Base DRR for the selected period
    - Seasonal DRR (Excel formula)
    """
    try:
        start_dt   = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt     = datetime.strptime(end_date, "%Y-%m-%d")
        period_days = (end_dt - start_dt).days + 1

        if period_days <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="end_date must be on or after start_date",
            )

        # 1. Products
        products = await _fetch_products(db, brand)
        if not products:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No products found for the given filters",
            )

        sku_to_item_id = {sku: pdata["item_id"] for sku, pdata in products.items()}

        # 2. Base DRR + historical monthly averages (parallel)
        base_drr_by_sku, monthly_avg_by_sku = await asyncio.gather(
            _fetch_base_drr(db, sku_to_item_id, start_date, end_date, period_days),
            _fetch_monthly_avg_sales(db, sku_to_item_id, end_date),
        )

        # 3. Build Excel (CPU work off the event loop)
        excel_buffer = await asyncio.to_thread(
            _build_excel,
            products,
            base_drr_by_sku,
            monthly_avg_by_sku,
            start_date,
            end_date,
        )

        filename = f"seasonal_report_{start_date}_to_{end_date}"
        if brand:
            filename += f"_{brand.replace(' ', '_')}"
        filename += ".xlsx"

        return StreamingResponse(
            excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating seasonal report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )
