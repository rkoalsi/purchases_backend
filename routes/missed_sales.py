import pandas as pd
from datetime import datetime, date
from io import BytesIO
import logging
import asyncio

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Query
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime, timezone
from ..database import get_database, serialize_mongo_document

logger = logging.getLogger(__name__)

MISSED_SALES_COLLECTION = "missed_sales"

router = APIRouter()


def convert_date_field(date_value):
    """Convert date value to datetime object"""
    if pd.isna(date_value):
        return None
    try:
        if isinstance(date_value, datetime):
            return date_value
        elif isinstance(date_value, pd.Timestamp):
            return date_value.to_pydatetime()
        elif isinstance(date_value, date):
            return datetime.combine(date_value, datetime.min.time())
        elif isinstance(date_value, str):
            date_formats = [
                "%d-%m-%Y", "%Y-%m-%d", "%m/%d/%Y",
                "%d/%m/%Y", "%Y/%m/%d", "%d.%m.%Y",
            ]
            for fmt in date_formats:
                try:
                    return datetime.strptime(date_value.strip(), fmt)
                except ValueError:
                    continue
            return pd.to_datetime(date_value, dayfirst=True).to_pydatetime()
        else:
            return pd.to_datetime(date_value).to_pydatetime()
    except Exception as e:
        logger.warning(f"Could not convert date value '{date_value}': {e}")
        return None


def _str_or_none(val):
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except Exception:
        pass
    return str(val).strip() or None


def _num_or_zero(val):
    try:
        if pd.isna(val):
            return 0
        return float(val)
    except Exception:
        return 0


def process_missed_sales_excel(file_content: bytes):
    """Parse the missed sales Excel file and return records list."""
    df = pd.read_excel(BytesIO(file_content), sheet_name=0, engine="openpyxl")

    if df.empty:
        return []

    # Strip whitespace from column headers
    df.columns = [str(col).strip() for col in df.columns]

    records = []
    for _, row in df.iterrows():
        # Skip entirely empty rows
        if row.isna().all():
            continue

        record = {
            "date": convert_date_field(row.get("Date")),
            "data_source": _str_or_none(row.get("Data Source")),
            "item_status": _str_or_none(row.get("Item Status")),
            "asin": _str_or_none(row.get("ASIN")),
            "item_code": _str_or_none(row.get("Item Code")),
            "item_name": _str_or_none(row.get("Item Name")),
            "estimate_no": _str_or_none(row.get("Estimate No.")),
            "customer_name": _str_or_none(row.get("Customer Name")),
            "po_no": _str_or_none(row.get("PO No.")),
            "quantity_ordered": _num_or_zero(row.get("Quantity Ordered")),
            "quantity_cancelled": _num_or_zero(row.get("Quantity Cancelled")),
            "missed_sales_quantity": _num_or_zero(row.get("Missed Sales Quantity")),
            "uploaded_at": datetime.now(timezone.utc),
        }
        records.append(record)

    return records


@router.post("/upload")
async def upload_missed_sales(file: UploadFile = File(...)):
    """Upload and process missed sales Excel file."""
    try:
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only Excel files (.xlsx, .xls) are allowed",
            )

        file_content = await file.read()

        if len(file_content) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Uploaded file is empty",
            )

        processed_data = await asyncio.to_thread(
            process_missed_sales_excel, file_content
        )

        if not processed_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid data found in the Excel file",
            )

        db = get_database()
        collection = db[MISSED_SALES_COLLECTION]

        dates = [r["date"] for r in processed_data if r.get("date") is not None]
        delete_result = None
        date_range_info = {"start_date": None, "end_date": None}

        if dates:
            min_date = min(dates)
            max_date = max(dates)
            date_range_info = {
                "start_date": min_date.strftime("%Y-%m-%d"),
                "end_date": max_date.strftime("%Y-%m-%d"),
            }

            def _delete():
                return collection.delete_many(
                    {"date": {"$gte": min_date, "$lte": max_date}}
                )

            delete_result = await asyncio.to_thread(_delete)
            logger.info(
                f"Deleted {delete_result.deleted_count} existing records for "
                f"{date_range_info['start_date']} to {date_range_info['end_date']}"
            )

        def _insert():
            return collection.insert_many(processed_data)

        result = await asyncio.to_thread(_insert)

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "message": "File uploaded and processed successfully",
                "filename": file.filename,
                "total_records_processed": len(processed_data),
                "total_records_inserted": len(result.inserted_ids),
                "records_deleted": delete_result.deleted_count if delete_result else 0,
                "date_range_processed": date_range_info,
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in upload_missed_sales: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@router.get("")
async def get_missed_sales(
    start_date: str = Query(None),
    end_date: str = Query(None),
):
    """Get missed sales records with optional date range filter."""
    try:
        db = get_database()
        collection = db[MISSED_SALES_COLLECTION]

        query: dict = {}
        if start_date or end_date:
            date_filter: dict = {}
            if start_date:
                date_filter["$gte"] = datetime.strptime(start_date, "%Y-%m-%d")
            if end_date:
                date_filter["$lte"] = datetime.strptime(end_date, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59
                )
            query["date"] = date_filter

        def _fetch():
            return list(collection.find(query, {"_id": 0}).sort("date", 1))

        records = await asyncio.to_thread(_fetch)
        serialized = serialize_mongo_document(records)

        return JSONResponse(
            content={
                "data": serialized,
                "total": len(serialized),
            }
        )

    except Exception as e:
        logger.error(f"Error fetching missed sales: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


@router.get("/download")
async def download_missed_sales(
    start_date: str = Query(None),
    end_date: str = Query(None),
):
    """Download missed sales records as Excel."""
    try:
        db = get_database()
        collection = db[MISSED_SALES_COLLECTION]

        query: dict = {}
        if start_date or end_date:
            date_filter: dict = {}
            if start_date:
                date_filter["$gte"] = datetime.strptime(start_date, "%Y-%m-%d")
            if end_date:
                date_filter["$lte"] = datetime.strptime(end_date, "%Y-%m-%d").replace(
                    hour=23, minute=59, second=59
                )
            query["date"] = date_filter

        def _fetch():
            return list(
                collection.find(query, {"_id": 0, "uploaded_at": 0}).sort("date", 1)
            )

        records = await asyncio.to_thread(_fetch)

        if not records:
            raise HTTPException(
                status_code=404,
                detail="No records found for the specified date range",
            )

        rows = []
        for r in records:
            rows.append({
                "Date": r.get("date"),
                "Data Source": r.get("data_source"),
                "Item Status": r.get("item_status"),
                "ASIN": r.get("asin"),
                "Item Code": r.get("item_code"),
                "Item Name": r.get("item_name"),
                "Estimate No.": r.get("estimate_no"),
                "Customer Name.": r.get("customer_name"),
                "PO No.": r.get("po_no"),
                "Quantity Ordered": r.get("quantity_ordered"),
                "Quantity Cancelled": r.get("quantity_cancelled"),
                "Missed Sales Quantity": r.get("missed_sales_quantity"),
            })

        df = pd.DataFrame(rows)

        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Missed Sales")
        output.seek(0)

        filename = "missed_sales"
        if start_date:
            filename += f"_{start_date}"
        if end_date:
            filename += f"_to_{end_date}"
        filename += ".xlsx"

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error downloading missed sales: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )
