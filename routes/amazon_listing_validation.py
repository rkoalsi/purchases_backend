import io
import logging
import asyncio
from typing import Optional

from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from openpyxl import load_workbook, Workbook
from openpyxl.utils import column_index_from_string
from openpyxl.styles import PatternFill, Font

from ..database import get_database

logger = logging.getLogger(__name__)
router = APIRouter()

# GST tax code mapping (Amazon code → percentage)
TAX_CODE_MAP = {
    "A_GEN_STANDARD": 18.0,
    "A_GEN_SUPERREDUCED": 5.0,
    "A_GEN_REDUCED": 12.0,
}

# Seller Central Template column letters
SC_COLS = {
    "sku": "A",
    "hsn": "DD",
    "tax_code": "EJ",
    "mrp": "FL",
    "sp": "FK",
}

# Vendor Central Template column letters
VC_COLS = {
    "sku": "B",
    "hsn": "DI",
    "mrp": "EQ",
}


def _clean(val) -> str:
    """Normalise a cell value to a stripped string."""
    if val is None:
        return ""
    return str(val).strip().strip("'")


def _find_template_sheet(wb):
    """
    Return the Template sheet from the workbook.
    Prefers an exact case-insensitive match of 'Template' first,
    then falls back to any sheet whose name starts with 'Template'.
    """
    # Exact match first (e.g. "Template" or "Template-PET_TOY")
    for name in wb.sheetnames:
        if name.strip().lower() == "template":
            return wb[name]
    # Starts-with match (e.g. "Template-PET_TOY")
    for name in wb.sheetnames:
        if name.strip().lower().startswith("template"):
            return wb[name]
    raise ValueError(f"No Template sheet found. Available sheets: {wb.sheetnames}")


def _col_idx(letter: str) -> int:
    return column_index_from_string(letter)


def _parse_seller_central(file_bytes: bytes) -> list[dict]:
    """
    Parse the Seller Central xlsm file.
    Header is on row 4, data starts on row 6.
    Returns list of dicts with keys: sku, hsn, tax_code, mrp, sp.
    """
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, keep_vba=True)
    ws = _find_template_sheet(wb)

    col_map = {k: _col_idx(v) for k, v in SC_COLS.items()}
    rows = []

    for row in ws.iter_rows(min_row=6, values_only=True):
        sku = _clean(row[col_map["sku"] - 1])
        if not sku:
            continue
        rows.append(
            {
                "sku": sku,
                "hsn": _clean(row[col_map["hsn"] - 1]),
                "tax_code": _clean(row[col_map["tax_code"] - 1]),
                "mrp": _clean(row[col_map["mrp"] - 1]),
                "sp": _clean(row[col_map["sp"] - 1]),
            }
        )
    return rows


def _parse_vendor_central(file_bytes: bytes) -> list[dict]:
    """
    Parse the Vendor Central xlsm file.
    Header is on row 3, data starts on row 6.
    Returns list of dicts with keys: sku, hsn, mrp.
    """
    wb = load_workbook(io.BytesIO(file_bytes), read_only=True, keep_vba=True)
    ws = _find_template_sheet(wb)

    col_map = {k: _col_idx(v) for k, v in VC_COLS.items()}
    rows = []

    for row in ws.iter_rows(min_row=6, values_only=True):
        sku = _clean(row[col_map["sku"] - 1])
        if not sku:
            continue
        rows.append(
            {
                "sku": sku,
                "hsn": _clean(row[col_map["hsn"] - 1]),
                "mrp": _clean(row[col_map["mrp"] - 1]),
            }
        )
    return rows


def _load_products_by_sku(skus: set[str]) -> dict[str, dict]:
    """Batch-load products from MongoDB keyed by cf_sku_code."""
    db = get_database()
    products = list(
        db["products"].find(
            {"cf_sku_code": {"$in": list(skus)}},
            {
                "_id": 0,
                "cf_sku_code": 1,
                "hsn_or_sac": 1,
                "rate": 1,
                "item_tax_preferences": 1,
                "item_name": 1,
            },
        )
    )
    return {str(p["cf_sku_code"]).strip(): p for p in products}


def _get_db_gst(product: dict) -> Optional[float]:
    """Extract the intra GST percentage from item_tax_preferences."""
    prefs = product.get("item_tax_preferences") or []
    for pref in prefs:
        spec = pref.get("tax_specification", "")
        if spec == "intra":
            return pref.get("tax_percentage")
    # Fallback: return first available percentage
    if prefs:
        return prefs[0].get("tax_percentage")
    return None


def _compare_hsn(file_val: str, db_val: str) -> bool:
    """Compare HSN values loosely – strip spaces, commas, leading zeros."""
    def norm(v):
        return v.replace(" ", "").replace(",", "").lstrip("0").lower()
    return norm(file_val) == norm(db_val)


def _compare_numeric(file_val: str, db_val) -> bool:
    """Compare numeric values after rounding to 2 decimal places."""
    try:
        return round(float(file_val), 2) == round(float(db_val), 2)
    except (TypeError, ValueError):
        return str(file_val).strip() == str(db_val).strip()


def _validate_seller_central(rows: list[dict], products: dict[str, dict]) -> list[dict]:
    mismatches = []
    for row in rows:
        sku = row["sku"]
        product = products.get(sku)

        if not product:
            mismatches.append(
                {
                    "source": "Seller Central",
                    "sku": sku,
                    "field": "SKU",
                    "file_value": sku,
                    "db_value": "—",
                    "issue": "SKU not found in database",
                }
            )
            continue

        db_hsn = str(product.get("hsn_or_sac") or "").strip()
        db_mrp = product.get("rate")
        db_gst = _get_db_gst(product)
        item_name = product.get("item_name", "")

        # HSN check
        if row["hsn"] and not _compare_hsn(row["hsn"], db_hsn):
            mismatches.append(
                {
                    "source": "Seller Central",
                    "sku": sku,
                    "item_name": item_name,
                    "field": "HSN",
                    "file_value": row["hsn"],
                    "db_value": db_hsn or "—",
                    "issue": "HSN mismatch",
                }
            )

        # GST / Tax Code check
        file_tax_pct = TAX_CODE_MAP.get(row["tax_code"])
        if row["tax_code"] and row["tax_code"] not in TAX_CODE_MAP:
            mismatches.append(
                {
                    "source": "Seller Central",
                    "sku": sku,
                    "item_name": item_name,
                    "field": "GST",
                    "file_value": row["tax_code"],
                    "db_value": f"{db_gst}%" if db_gst is not None else "—",
                    "issue": "Unrecognised tax code",
                }
            )
        elif file_tax_pct is not None and db_gst is not None and file_tax_pct != db_gst:
            mismatches.append(
                {
                    "source": "Seller Central",
                    "sku": sku,
                    "item_name": item_name,
                    "field": "GST",
                    "file_value": f"{row['tax_code']} ({file_tax_pct}%)",
                    "db_value": f"{db_gst}%",
                    "issue": "GST rate mismatch",
                }
            )

        # MRP check
        if row["mrp"] and db_mrp is not None and not _compare_numeric(row["mrp"], db_mrp):
            mismatches.append(
                {
                    "source": "Seller Central",
                    "sku": sku,
                    "item_name": item_name,
                    "field": "MRP",
                    "file_value": row["mrp"],
                    "db_value": str(db_mrp),
                    "issue": "MRP mismatch",
                }
            )

    return mismatches


def _validate_vendor_central(rows: list[dict], products: dict[str, dict]) -> list[dict]:
    mismatches = []
    for row in rows:
        sku = row["sku"]
        product = products.get(sku)

        if not product:
            mismatches.append(
                {
                    "source": "Vendor Central",
                    "sku": sku,
                    "field": "SKU",
                    "file_value": sku,
                    "db_value": "—",
                    "issue": "SKU not found in database",
                }
            )
            continue

        db_hsn = str(product.get("hsn_or_sac") or "").strip()
        db_mrp = product.get("rate")
        item_name = product.get("item_name", "")

        # HSN check
        if row["hsn"] and not _compare_hsn(row["hsn"], db_hsn):
            mismatches.append(
                {
                    "source": "Vendor Central",
                    "sku": sku,
                    "item_name": item_name,
                    "field": "HSN",
                    "file_value": row["hsn"],
                    "db_value": db_hsn or "—",
                    "issue": "HSN mismatch",
                }
            )

        # MRP check
        if row["mrp"] and db_mrp is not None and not _compare_numeric(row["mrp"], db_mrp):
            mismatches.append(
                {
                    "source": "Vendor Central",
                    "sku": sku,
                    "item_name": item_name,
                    "field": "MRP",
                    "file_value": row["mrp"],
                    "db_value": str(db_mrp),
                    "issue": "MRP mismatch",
                }
            )

    return mismatches


def _build_excel(sc_mismatches: list[dict], vc_mismatches: list[dict]) -> bytes:
    wb = Workbook()

    red_fill = PatternFill(start_color="FFCCCC", end_color="FFCCCC", fill_type="solid")
    header_font = Font(bold=True)

    HEADERS = ["SKU", "Item Name", "Field", "File Value", "DB Value", "Issue"]

    for sheet_name, data in [
        ("Seller Central", sc_mismatches),
        ("Vendor Central", vc_mismatches),
    ]:
        ws = wb.create_sheet(title=sheet_name)
        ws.append(HEADERS)
        for cell in ws[1]:
            cell.font = header_font

        for row in data:
            ws.append(
                [
                    row.get("sku", ""),
                    row.get("item_name", ""),
                    row.get("field", ""),
                    row.get("file_value", ""),
                    row.get("db_value", ""),
                    row.get("issue", ""),
                ]
            )
            # Highlight the row in light red
            for cell in ws[ws.max_row]:
                cell.fill = red_fill

        # Auto-width columns
        for col in ws.columns:
            max_len = max((len(str(cell.value or "")) for cell in col), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 60)

    # Remove default blank sheet
    if "Sheet" in wb.sheetnames:
        del wb["Sheet"]

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


async def _run_validation(
    seller_central_file: Optional[UploadFile],
    vendor_central_file: Optional[UploadFile],
) -> tuple[list, list, list, list]:
    """
    Core validation logic. Either file may be None.
    Returns (sc_rows, vc_rows, sc_mismatches, vc_mismatches).
    """
    if not seller_central_file and not vendor_central_file:
        raise HTTPException(
            status_code=400,
            detail="At least one file (Seller Central or Vendor Central) must be provided.",
        )

    sc_rows: list[dict] = []
    vc_rows: list[dict] = []

    if seller_central_file:
        sc_bytes = await seller_central_file.read()
        try:
            sc_rows = await asyncio.to_thread(_parse_seller_central, sc_bytes)
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Failed to parse Seller Central file: {e}"
            )

    if vendor_central_file:
        vc_bytes = await vendor_central_file.read()
        try:
            vc_rows = await asyncio.to_thread(_parse_vendor_central, vc_bytes)
        except Exception as e:
            raise HTTPException(
                status_code=400, detail=f"Failed to parse Vendor Central file: {e}"
            )

    all_skus = {r["sku"] for r in sc_rows} | {r["sku"] for r in vc_rows}
    if not all_skus:
        raise HTTPException(
            status_code=400,
            detail="No SKU data found in the uploaded file(s). Ensure data rows start at row 6.",
        )

    products = await asyncio.to_thread(_load_products_by_sku, all_skus)
    sc_mismatches = _validate_seller_central(sc_rows, products) if sc_rows else []
    vc_mismatches = _validate_vendor_central(vc_rows, products) if vc_rows else []

    return sc_rows, vc_rows, sc_mismatches, vc_mismatches


@router.post("/validate")
async def validate_amazon_listings(
    seller_central_file: Optional[UploadFile] = File(None),
    vendor_central_file: Optional[UploadFile] = File(None),
):
    """
    Validate Amazon listing files against the products collection in MongoDB.
    Either file is optional — at least one must be supplied.
    """
    sc_rows, vc_rows, sc_mismatches, vc_mismatches = await _run_validation(
        seller_central_file, vendor_central_file
    )
    return {
        "summary": {
            "seller_central_rows": len(sc_rows),
            "vendor_central_rows": len(vc_rows),
            "seller_central_mismatches": len(sc_mismatches),
            "vendor_central_mismatches": len(vc_mismatches),
        },
        "seller_central": sc_mismatches,
        "vendor_central": vc_mismatches,
    }


@router.post("/validate/download")
async def download_amazon_listing_validation(
    seller_central_file: Optional[UploadFile] = File(None),
    vendor_central_file: Optional[UploadFile] = File(None),
):
    """
    Same as /validate but returns a highlighted Excel file.
    Either file is optional — at least one must be supplied.
    """
    sc_rows, vc_rows, sc_mismatches, vc_mismatches = await _run_validation(
        seller_central_file, vendor_central_file
    )
    excel_bytes = await asyncio.to_thread(_build_excel, sc_mismatches, vc_mismatches)
    return StreamingResponse(
        io.BytesIO(excel_bytes),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={
            "Content-Disposition": "attachment; filename=amazon_listing_validation.xlsx"
        },
    )
