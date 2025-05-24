# main.py
import pandas as pd
from datetime import datetime, date, timedelta
import calendar, io, json
from io import BytesIO

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document

import asyncio
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from typing import List, Dict, Any
from pymongo import InsertOne, ReplaceOne, UpdateOne
from datetime import datetime
import numpy as np


# Optimize SKU mapping with caching
class SKUCache:
    def __init__(self):
        self._cache = None
        self._last_updated = None

    async def get_sku_mapping(self, sku_collection):
        # Cache for 5 minutes to avoid repeated DB calls
        if self._cache is None or (datetime.now() - self._last_updated).seconds > 300:

            self._cache = {
                doc["item_id"]: doc
                for doc in sku_collection.find(
                    {}, {"item_id": 1, "sku_code": 1, "item_name": 1}
                )
            }
            self._last_updated = datetime.now()
        return self._cache


# Global cache instance
sku_cache = SKUCache()
# --- Configuration ---
# Use environment variables for production

SKU_COLLECTION = "blinkit_sku_mapping"
SALES_COLLECTION = "blinkit_sales"
INVENTORY_COLLECTION = "blinkit_inventory"
REPORT_COLLECTION = "blinkit_sales_inventory_reports"

CITIES = [
    "Hyderabad",
    "Mumbai",
    "Haryana",
    "Pune",
    "Bengaluru",
    "Kolkata",
    "Chennai",
    "Faridabad",
    "Gurgaon",
    "Farukhnagar",
]

WAREHOUSE_CITY_MAP = {
    "Farukhnagar - SR Feeder": "Gurgaon"
    # Add other specific mappings here if needed
}

DAYS_IN_WEEK = 7

# --- FastAPI App ---
router = APIRouter()


# --- Helper Functions (Adapted for DB) ---


def get_city_from_warehouse(
    warehouse_name: str, cities_list: list, city_map: dict
) -> str:
    if warehouse_name in city_map:
        return city_map[warehouse_name]

    for city_candidate in cities_list:
        if city_candidate.lower() in warehouse_name.lower():
            return city_candidate

    return warehouse_name


def safe_strptime(date_input, fmt) -> datetime | None:
    """Safely parse date input (string or datetime), return None on failure."""
    if pd.isna(date_input):
        return None
    if isinstance(date_input, datetime):
        return date_input
    if isinstance(date_input, date):  # Handle date objects too
        return datetime.combine(date_input, datetime.min.time())
    try:
        # Try parsing string if it's a string
        if isinstance(date_input, str):
            return datetime.strptime(date_input, fmt)
        # If it's neither string nor datetime/date, parsing will likely fail below anyway
        return None  # Or let the next step handle it if pd.to_datetime is used
    except (ValueError, TypeError):
        return None


def get_total_days_in_month(year: int, month: int) -> int:
    if not 1 <= month <= 12:
        raise ValueError("Month must be between 1 and 12")
    if not isinstance(year, int) or not isinstance(month, int):
        raise TypeError("Year and month must be integers")

    _, num_days = calendar.monthrange(year, month)
    return num_days


# --- Data Upload Routes ---


@router.get("/get_blinkit_sku_mapping")
async def get_sku_mapping(database=Depends(get_database)):
    """
    Retrieves all documents from the SKU mapping collection.
    """
    try:
        sku_collection = database.get_collection(SKU_COLLECTION)
        sku_documents = list(sku_collection.find({}, {"_id": 0}).sort("item_name", 1))

        return JSONResponse(content=sku_documents)

    except PyMongoError as e:  # Catch database-specific errors
        print(f"Database error retrieving SKU mapping: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error retrieving SKU mapping: {e}",
        )
    except Exception as e:  # Catch any other unexpected errors
        print(f"Unexpected error retrieving SKU mapping: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}",
        )


@router.post("/upload-sku-mapping")
async def upload_sku_mapping(
    file: UploadFile = File(...), database=Depends(get_database)
):
    """
    Uploads SKU mapping data from an Excel file (.xlsx).
    Clears existing mapping and inserts new data.
    Expected columns: 'Item ID', 'SKU Code', 'Item Name'.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    try:
        file_content = await file.read()
        df = pd.read_excel(BytesIO(file_content))

        # Validate required columns
        required_cols = ["Item ID", "SKU Code", "Item Name"]
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing required columns: {', '.join(missing)}",
            )

        sku_collection = database.get_collection(SKU_COLLECTION)

        # Optional: Clear existing mapping or implement upsert logic
        # Clearing is simpler if the Excel is the source of truth for the full mapping
        delete_result = sku_collection.delete_many({})
        print(f"Deleted {delete_result.deleted_count} existing SKU mappings.")

        data_to_insert = []
        for _, row in df.iterrows():
            item_id = str(row["Item ID"]).strip() if pd.notna(row["Item ID"]) else None
            sku_code = (
                str(row["SKU Code"]).strip() if pd.notna(row["SKU Code"]) else None
            )
            item_name = (
                str(row["Item Name"]).strip() if pd.notna(row["Item Name"]) else None
            )

            if (
                item_id and sku_code and item_name
            ):  # Ensure essential fields are present
                data_to_insert.append(
                    {
                        "item_id": item_id,
                        "sku_code": sku_code,
                        "item_name": item_name,
                    }
                )
            else:
                print(f"Skipping SKU row due to missing data: {row.to_dict()}")

        if not data_to_insert:
            return {"message": "No valid SKU mapping data found to insert."}

        insert_result = sku_collection.insert_many(data_to_insert)
        print(f"Inserted {len(insert_result.inserted_ids)} new SKU mappings.")

        # Create index for efficient lookup
        sku_collection.create_index([("item_id", ASCENDING)], unique=True)

        return {
            "message": f"Successfully uploaded and stored {len(insert_result.inserted_ids)} SKU mappings."
        }

    except HTTPException as e:
        raise e  # Re-raise intended HTTP exceptions
    except Exception as e:
        print(f"Error uploading SKU mapping: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the file: {e}",
        )


@router.post("/upload_sales_data")
async def upload_sales_data(
    file: UploadFile = File(...), database=Depends(get_database)
):
    """
    Optimized sales data upload with batch processing and parallel operations.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    try:
        # Read file content
        file_content = await file.read()

        # Use thread pool for CPU-intensive pandas operations
        with ThreadPoolExecutor() as executor:
            df = await asyncio.get_event_loop().run_in_executor(
                executor,
                lambda: pd.read_excel(BytesIO(file_content), engine="openpyxl"),
            )

        # Validate required columns
        required_cols = [
            "Item ID",
            "Quantity",
            "Order Date",
            "Customer City ",
            "HSN Code",
        ]
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing required columns: {', '.join(missing)}",
            )

        sales_collection = database.get_collection(SALES_COLLECTION)
        sku_collection = database.get_collection(SKU_COLLECTION)

        # Get cached SKU mapping
        sku_map_dict = await sku_cache.get_sku_mapping(sku_collection)

        # Vectorized data processing with pandas
        df = df.dropna(
            subset=["Item ID", "Customer City "]
        )  # Remove rows with missing critical data

        # Convert data types efficiently
        df["Item ID"] = (
            pd.to_numeric(df["Item ID"], errors="coerce").fillna(0).astype(int)
        )
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
        df["Customer City "] = df["Customer City "].astype(str).str.strip()
        df["HSN Code"] = df["HSN Code"].astype(str).str.strip()

        # Process dates efficiently
        def process_dates(dates):
            processed = []
            for date in dates:
                parsed_date = safe_strptime(date, "%d %b %Y")
                if parsed_date:
                    processed.append(parsed_date)
                else:
                    processed.append(None)
            return processed

        with ThreadPoolExecutor() as executor:
            df["processed_date"] = await asyncio.get_event_loop().run_in_executor(
                executor, process_dates, df["Order Date"].tolist()
            )

        # Remove rows with invalid dates
        df = df.dropna(subset=["processed_date"])

        # City processing
        df["processed_city"] = df["Customer City "].replace("Faridabad", "Gurgaon")

        # SKU mapping vectorized
        df["sku_code"] = df["Item ID"].map(
            lambda x: sku_map_dict.get(x, {}).get("sku_code", "Unknown SKU")
        )
        df["item_name"] = df["Item ID"].map(
            lambda x: sku_map_dict.get(x, {}).get("item_name", "Unknown Item")
        )

        # Handle item names from file if available
        if "Item Name" in df.columns:
            df["item_name"] = df["Item Name"].fillna(df["item_name"])

        # Batch check for existing records
        unique_keys = df[
            ["Item ID", "processed_city", "processed_date"]
        ].drop_duplicates()

        # Build query for existing records
        existing_queries = []
        for _, row in unique_keys.iterrows():
            existing_queries.append(
                {
                    "item_id": int(row["Item ID"]),
                    "city": row["processed_city"],
                    "order_date": row["processed_date"],
                }
            )

        # Batch query existing records
        existing_docs = set()
        if existing_queries:
            cursor = sales_collection.find(
                {"$or": existing_queries}, {"item_id": 1, "city": 1, "order_date": 1}
            )
            for doc in cursor:
                key = (doc["item_id"], doc["city"], doc["order_date"])
                existing_docs.add(key)

        # Filter out existing records
        def is_new_record(row):
            key = (int(row["Item ID"]), row["processed_city"], row["processed_date"])
            return key not in existing_docs

        df["is_new"] = df.apply(is_new_record, axis=1)
        new_records_df = df[df["is_new"]]

        # Prepare bulk insert operations
        bulk_operations = []
        for _, row in new_records_df.iterrows():
            doc = {
                "item_id": int(row["Item ID"]),
                "sku_code": row["sku_code"],
                "hsn_code": row["HSN Code"],
                "item_name": row["item_name"],
                "quantity": float(row["Quantity"]),
                "city": row["processed_city"],
                "order_date": row["processed_date"],
            }
            bulk_operations.append(InsertOne(doc))

        # Execute bulk insert
        inserted_count = 0
        if bulk_operations:
            # Process in batches of 1000 to avoid memory issues
            batch_size = 1000
            for i in range(0, len(bulk_operations), batch_size):
                batch = bulk_operations[i : i + batch_size]
                result = sales_collection.bulk_write(batch, ordered=False)
                inserted_count += result.inserted_count

        # Create indexes if not exist (do this once)
        try:
            sales_collection.create_index([("order_date", 1)], background=True)
            sales_collection.create_index(
                [("sku_code", 1), ("city", 1), ("order_date", 1)], background=True
            )
        except Exception:
            pass  # Indexes might already exist

        skipped_count = len(df) - len(new_records_df)
        warning_count = len(df[df["sku_code"] == "Unknown SKU"])

        return {
            "message": f"Successfully processed sales data. Inserted {inserted_count} new records, skipped {skipped_count} existing records. Encountered {warning_count} warnings."
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error uploading sales data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the file: {e}",
        )


@router.post("/upload_inventory_data")
async def upload_inventory_data(
    file: UploadFile = File(...), database=Depends(get_database)
):
    """
    Optimized inventory data upload with efficient aggregation and batch upserts.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    try:
        file_content = await file.read()

        # Use thread pool for pandas operations
        with ThreadPoolExecutor() as executor:
            df = await asyncio.get_event_loop().run_in_executor(
                executor,
                lambda: pd.read_excel(BytesIO(file_content), engine="openpyxl"),
            )

        # Validate required columns
        required_cols = ["Item ID", "Backend Outlet", "Date", "Qty"]
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing required columns: {', '.join(missing)}",
            )

        inventory_collection = database.get_collection(INVENTORY_COLLECTION)
        sku_collection = database.get_collection(SKU_COLLECTION)

        # Get cached SKU mapping
        sku_map_dict = await sku_cache.get_sku_mapping(sku_collection)

        # Clean and process data efficiently
        df = df.dropna(subset=["Item ID"])
        df["Item ID"] = (
            pd.to_numeric(df["Item ID"], errors="coerce").fillna(0).astype(int)
        )
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
        df["Backend Outlet"] = df["Backend Outlet"].astype(str).str.strip()

        # Process dates
        def process_inventory_dates(dates):
            processed = []
            for date in dates:
                parsed_date = safe_strptime(date, "%Y-%m-%d")
                if parsed_date:
                    # Truncate to date for consistency
                    processed.append(
                        parsed_date.replace(hour=0, minute=0, second=0, microsecond=0)
                    )
                else:
                    processed.append(None)
            return processed

        with ThreadPoolExecutor() as executor:
            df["processed_date"] = await asyncio.get_event_loop().run_in_executor(
                executor, process_inventory_dates, df["Date"].tolist()
            )

        # Remove rows with invalid dates
        df = df.dropna(subset=["processed_date"])

        # Add SKU and city information
        df["sku_code"] = df["Item ID"].map(
            lambda x: sku_map_dict.get(x, {}).get("sku_code", "Unknown SKU")
        )
        df["item_name"] = df["Item ID"].map(
            lambda x: sku_map_dict.get(x, {}).get("item_name", "Unknown Item")
        )

        # Handle item names from file if available
        if "Item Name" in df.columns:
            df["item_name"] = df["Item Name"].fillna(df["item_name"])

        # Add city mapping
        df["city"] = df["Backend Outlet"].apply(
            lambda warehouse: get_city_from_warehouse(
                warehouse, CITIES, WAREHOUSE_CITY_MAP
            )
        )

        # Efficient aggregation using pandas groupby
        aggregation_columns = [
            "sku_code",
            "Backend Outlet",
            "city",
            "processed_date",
            "Item ID",
            "item_name",
        ]
        aggregated_df = (
            df.groupby(aggregation_columns).agg({"Qty": "sum"}).reset_index()
        )

        aggregated_df.rename(
            columns={"Qty": "warehouse_inventory", "Backend Outlet": "warehouse"},
            inplace=True,
        )

        # Prepare bulk upsert operations
        bulk_operations = []
        for _, row in aggregated_df.iterrows():
            filter_query = {
                "sku_code": row["sku_code"],
                "warehouse": row["warehouse"],
                "city": row["city"],
                "date": row["processed_date"],
            }

            document = {
                "sku_code": row["sku_code"],
                "warehouse": row["warehouse"],
                "city": row["city"],
                "date": row["processed_date"],
                "item_id": int(row["Item ID"]),
                "item_name": row["item_name"],
                "warehouse_inventory": float(row["warehouse_inventory"]),
            }

            bulk_operations.append(ReplaceOne(filter_query, document, upsert=True))

        # Execute bulk upsert
        upsert_count = 0
        if bulk_operations:
            # Process in batches
            batch_size = 1000
            for i in range(0, len(bulk_operations), batch_size):
                batch = bulk_operations[i : i + batch_size]
                result = inventory_collection.bulk_write(batch, ordered=False)
                upsert_count += result.upserted_count + result.modified_count

        # Create indexes if not exist
        try:
            inventory_collection.create_index([("date", 1)], background=True)
            inventory_collection.create_index(
                [("sku_code", 1), ("city", 1), ("warehouse", 1), ("date", 1)],
                unique=True,
                background=True,
            )
        except Exception:
            pass  # Indexes might already exist

        warning_count = len(aggregated_df[aggregated_df["sku_code"] == "Unknown SKU"])

        return {
            "message": f"Successfully uploaded and processed inventory data. Upserted {upsert_count} aggregated records. Encountered {warning_count} warnings."
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error uploading inventory data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the file: {e}",
        )


# --- Report Generation Route ---


def calculate_number_of_months(start_year, start_month, end_year, end_month):
    """Calculates the total number of months in a given range, inclusive."""
    return (end_year - start_year) * 12 + (end_month - start_month) + 1


@router.get("/generate_report")
async def generate_report(
    start_month: int,
    start_year: int,
    end_month: int,
    end_year: int,
    database=Depends(get_database),
):
    """
    Generates the Sales vs Inventory report dynamically based on the specified date range.
    Calculates metrics for each SKU/City pair without storing in database.
    Includes performance comparison with previous 7 and 30 days.
    Includes best performing month for each SKU/City pair.
    Returns the calculated report data directly.
    """
    if not (
        1 <= start_month <= 12
        and 1 <= end_month <= 12
        and start_year > 0
        and end_year > 0
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid month or year provided in the range.",
        )

    try:
        overall_start_date = datetime(start_year, start_month, 1)
        # Calculate end date of the end_month
        last_day_of_end_month = calendar.monthrange(end_year, end_month)[1]
        overall_end_date = datetime(
            end_year, end_month, last_day_of_end_month, 23, 59, 59, 999999
        )
    except ValueError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date components for start or end of range.",
        )

    if overall_start_date > overall_end_date:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Start date cannot be after end date.",
        )

    period_name = f"{calendar.month_name[start_month]} {start_year} to {calendar.month_name[end_month]} {end_year}"

    try:
        sales_collection = database.get_collection(SALES_COLLECTION)
        inventory_collection = database.get_collection(INVENTORY_COLLECTION)

        print(
            f"Querying sales and inventory data for {period_name} to generate dynamic report..."
        )

        # Query sales data for the entire period
        sales_cursor = sales_collection.find(
            {"order_date": {"$gte": overall_start_date, "$lte": overall_end_date}}
        )
        sales_data = list(sales_cursor)
        print(
            f"Found {len(sales_data)} sales records in DB for the period {period_name}."
        )

        # NEW: Query ALL sales data for lifetime best performing month calculation
        print("Querying lifetime sales data for best performing month calculation...")
        lifetime_sales_cursor = sales_collection.find({})
        lifetime_sales_data = list(lifetime_sales_cursor)
        print(
            f"Found {len(lifetime_sales_data)} total sales records for lifetime analysis."
        )

        # Query aggregated inventory data for the entire period
        inventory_cursor = inventory_collection.find(
            {
                "date": {
                    "$gte": overall_start_date.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ),
                    "$lte": overall_end_date.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ),
                }
            }
        )
        inventory_data = list(inventory_cursor)
        print(
            f"Found {len(inventory_data)} aggregated inventory records in DB for the period {period_name}."
        )

        if not sales_data and not inventory_data:
            return {
                "message": f"No sales or inventory data found for {period_name}. No report generated.",
                "data": [],
                "period_info": {
                    "start_month": start_month,
                    "start_year": start_year,
                    "end_month": end_month,
                    "end_year": end_year,
                    "period_name": period_name,
                },
            }

        # --- Process Sales Data for Current Period ---
        current_period_sales = {}
        sales_item_info = {}
        all_sales_dates = set()

        # NEW: Lifetime monthly sales tracking for best performing month calculation
        lifetime_monthly_sales = (
            {}
        )  # Structure: {(sku, city): {month_year_key: total_sales}}

        # Process lifetime sales data for best performing month calculation
        print("Processing lifetime sales data for best performing month calculation...")
        for record in lifetime_sales_data:
            sku = record.get("sku_code")
            city = record.get("city")
            order_date_dt = record.get("order_date")
            quantity = record.get("quantity", 0)
            item_name = record.get("item_name", "Unknown Item")
            item_id = record.get("item_id", "Unknown ID")

            if not sku or not city or not order_date_dt:
                continue
            quantity = float(quantity) if isinstance(quantity, (int, float)) else 0
            if isinstance(order_date_dt, datetime):
                # Create month-year key for monthly aggregation
                month_year_key = order_date_dt.strftime("%Y-%m")  # Format: "2024-01"
            else:
                continue

            if sku not in sales_item_info:
                sales_item_info[sku] = {"item_name": item_name, "item_id": item_id}

            # Aggregate lifetime monthly sales for best performing month calculation
            sku_city_key = (sku, city)
            if sku_city_key not in lifetime_monthly_sales:
                lifetime_monthly_sales[sku_city_key] = {}
            lifetime_monthly_sales[sku_city_key][month_year_key] = (
                lifetime_monthly_sales[sku_city_key].get(month_year_key, 0) + quantity
            )

        # Process current period sales data
        for record in sales_data:
            sku = record.get("sku_code")
            city = record.get("city")
            order_date_dt = record.get("order_date")
            quantity = record.get("quantity", 0)
            item_name = record.get("item_name", "Unknown Item")
            item_id = record.get("item_id", "Unknown ID")

            if not sku or not city or not order_date_dt:
                continue
            quantity = float(quantity) if isinstance(quantity, (int, float)) else 0
            if isinstance(order_date_dt, datetime):
                date_key_str = order_date_dt.strftime("%Y-%m-%d")
            else:
                continue

            if sku not in sales_item_info:
                sales_item_info[sku] = {"item_name": item_name, "item_id": item_id}

            key = (sku, city, date_key_str)
            current_period_sales[key] = current_period_sales.get(key, 0) + quantity
            all_sales_dates.add(date_key_str)

        # Process inventory data
        inventory_map = {}
        inventory_item_info = {}
        all_inventory_dates = set()

        for record in inventory_data:
            sku = record.get("sku_code")
            city = record.get("city")
            inventory_date_dt = record.get("date")
            inventory_qty = record.get("warehouse_inventory", 0)
            item_name = record.get("item_name", "Unknown Item")
            item_id = record.get("item_id", "Unknown ID")
            warehouse = record.get("warehouse", "Unknown Warehouse")

            if not sku or not city or not inventory_date_dt:
                continue
            inventory_qty = (
                float(inventory_qty) if isinstance(inventory_qty, (int, float)) else 0
            )
            if isinstance(inventory_date_dt, datetime):
                date_key_str = inventory_date_dt.strftime("%Y-%m-%d")
            else:
                continue
            key = (sku, city, date_key_str)
            inventory_map[key] = {
                "inventory": inventory_qty,
                "item_name": item_name,
                "item_id": item_id,
                "warehouse": warehouse,
            }
            all_inventory_dates.add(date_key_str)
            if sku not in inventory_item_info:
                inventory_item_info[sku] = {"item_name": item_name, "item_id": item_id}

        common_dates = sorted(list(all_sales_dates.intersection(all_inventory_dates)))
        n_common_days = len(common_dates)

        if n_common_days == 0:
            return {
                "message": f"Found data for {period_name}, but no common dates between sales and inventory data. No report generated.",
                "data": [],
                "period_info": {
                    "start_month": start_month,
                    "start_year": start_year,
                    "end_month": end_month,
                    "end_year": end_year,
                    "period_name": period_name,
                },
            }

        print(
            f"\nFound {n_common_days} common dates for calculation within {period_name}."
        )

        last_seven_days_sales = {}
        two_weeks_ago_seven_days_sales = {}
        current_30_day_comparison_sales = {}
        previous_30_day_comparison_sales = {}

        if common_dates:
            last_common_date_str = common_dates[-1]
            last_common_date_dt = datetime.strptime(
                last_common_date_str, "%Y-%m-%d"
            ).replace(hour=23, minute=59, second=59, microsecond=999999)

            seven_days_period_end = last_common_date_dt
            seven_days_period_start = last_common_date_dt - timedelta(
                days=DAYS_IN_WEEK - 1
            )
            prev_seven_days_period_end = seven_days_period_start - timedelta(
                microseconds=1
            )
            prev_seven_days_period_start = prev_seven_days_period_end - timedelta(
                days=DAYS_IN_WEEK - 1
            )

            current_30_day_period_end = last_common_date_dt
            current_30_day_period_start = last_common_date_dt - timedelta(days=30 - 1)
            previous_30_day_period_end = current_30_day_period_start - timedelta(
                microseconds=1
            )
            previous_30_day_period_start = previous_30_day_period_end - timedelta(
                days=30 - 1
            )

            print(f"Last common date in period: {last_common_date_str}")

            extended_sales_fetch_start_date = previous_30_day_period_start
            if prev_seven_days_period_start < extended_sales_fetch_start_date:
                extended_sales_fetch_start_date = prev_seven_days_period_start

            extended_sales_cursor = sales_collection.find(
                {
                    "order_date": {
                        "$gte": extended_sales_fetch_start_date.replace(
                            hour=0, minute=0, second=0, microsecond=0
                        ),
                        "$lte": last_common_date_dt,
                    }
                }
            )
            extended_sales_data = list(extended_sales_cursor)
            print(
                f"Found {len(extended_sales_data)} sales records for comparison periods ending {last_common_date_str}."
            )

            for record in extended_sales_data:
                sku = record.get("sku_code")
                city = record.get("city")
                order_date_dt = record.get("order_date")
                quantity = record.get("quantity", 0)

                if not sku or not city or not order_date_dt:
                    continue
                quantity = float(quantity) if isinstance(quantity, (int, float)) else 0
                key_pair = (sku, city)

                if seven_days_period_start <= order_date_dt <= seven_days_period_end:
                    last_seven_days_sales[key_pair] = (
                        last_seven_days_sales.get(key_pair, 0) + quantity
                    )
                if (
                    prev_seven_days_period_start
                    <= order_date_dt
                    <= prev_seven_days_period_end
                ):
                    two_weeks_ago_seven_days_sales[key_pair] = (
                        two_weeks_ago_seven_days_sales.get(key_pair, 0) + quantity
                    )
                if (
                    current_30_day_period_start
                    <= order_date_dt
                    <= current_30_day_period_end
                ):
                    current_30_day_comparison_sales[key_pair] = (
                        current_30_day_comparison_sales.get(key_pair, 0) + quantity
                    )
                if (
                    previous_30_day_period_start
                    <= order_date_dt
                    <= previous_30_day_period_end
                ):
                    previous_30_day_comparison_sales[key_pair] = (
                        previous_30_day_comparison_sales.get(key_pair, 0) + quantity
                    )

        valid_sku_city_pairs = set()
        for date_str in common_dates:
            for sku, city, date_s in current_period_sales.keys():
                if date_s == date_str:
                    valid_sku_city_pairs.add((sku, city))
            for sku, city, date_s in inventory_map.keys():
                if date_s == date_str:
                    valid_sku_city_pairs.add((sku, city))

        print(
            f"Calculating metrics for {len(valid_sku_city_pairs)} unique SKU/City pairs for period {period_name}..."
        )

        # NEW: Helper function to find best performing month from lifetime data
        def get_best_performing_month(sku_city_key):
            """
            Returns the best performing month for a given SKU/City pair based on lifetime sales data.
            Returns dict with month_name, year, quantity_sold, and formatted string.
            """
            if (
                sku_city_key not in lifetime_monthly_sales
                or not lifetime_monthly_sales[sku_city_key]
            ):
                return {
                    "month_name": "No Data",
                    "year": None,
                    "quantity_sold": 0,
                    "formatted": "No Data",
                }

            # Find the month with highest sales from lifetime data
            best_month_key = max(
                lifetime_monthly_sales[sku_city_key],
                key=lifetime_monthly_sales[sku_city_key].get,
            )
            best_month_quantity = lifetime_monthly_sales[sku_city_key][best_month_key]

            # Parse the month-year key back to readable format
            year, month = best_month_key.split("-")
            month_name = calendar.month_name[int(month)]

            return {
                "month_name": month_name,
                "year": int(year),
                "quantity_sold": round(best_month_quantity, 2),
                "formatted": f"{month_name} {year}",
            }

        report_data = []
        num_months_in_range = calculate_number_of_months(
            start_year, start_month, end_year, end_month
        )

        for sku, city in valid_sku_city_pairs:
            item_info = inventory_item_info.get(
                sku,
                sales_item_info.get(
                    sku, {"item_name": "Unknown Item", "item_id": "Unknown ID"}
                ),
            )
            item_name = item_info["item_name"]
            item_id = item_info["item_id"]
            warehouse = "Unknown Warehouse"

            # Try to get warehouse from the latest inventory record
            for date_str in reversed(common_dates):
                inventory_key = (sku, city, date_str)
                if (
                    inventory_key in inventory_map
                    and "warehouse" in inventory_map[inventory_key]
                ):
                    warehouse = inventory_map[inventory_key]["warehouse"]
                    break

            total_sales_on_common_days = 0
            total_sales_on_days_with_inventory = 0
            days_with_inventory = 0

            for date_str in common_dates:
                sales_key = (sku, city, date_str)
                inventory_key = (sku, city, date_str)
                sales_qty = current_period_sales.get(sales_key, 0)
                inventory_info_daily = inventory_map.get(inventory_key)
                inventory_qty = (
                    inventory_info_daily["inventory"] if inventory_info_daily else 0
                )

                total_sales_on_common_days += sales_qty
                if inventory_qty > 0:
                    total_sales_on_days_with_inventory += sales_qty
                    days_with_inventory += 1

            avg_daily_on_stock_days = (
                total_sales_on_days_with_inventory / days_with_inventory
                if days_with_inventory > 0
                else 0
            )
            avg_weekly_on_stock_days = avg_daily_on_stock_days * DAYS_IN_WEEK

            avg_monthly_val_for_period = 0
            if num_months_in_range > 0 and total_sales_on_common_days > 0:
                avg_monthly_val_for_period = (
                    total_sales_on_common_days / num_months_in_range
                )

            last_day_inventory = 0
            if common_dates:
                last_common_date_for_sku_city = common_dates[-1]
                inventory_at_lcd_key = (sku, city, last_common_date_for_sku_city)
                last_day_inventory_info = inventory_map.get(inventory_at_lcd_key)
                if last_day_inventory_info:
                    last_day_inventory = last_day_inventory_info["inventory"]

            doc_calc = 0
            if avg_daily_on_stock_days > 0:
                doc_calc = last_day_inventory / avg_daily_on_stock_days

            sku_city_key = (sku, city)
            sales_target_last_7_days = last_seven_days_sales.get(sku_city_key, 0)
            sales_baseline_prev_7_days = two_weeks_ago_seven_days_sales.get(
                sku_city_key, 0
            )
            weekly_comparison_pct = 0
            if sales_baseline_prev_7_days > 0:
                weekly_comparison_pct = (
                    (sales_target_last_7_days - sales_baseline_prev_7_days)
                    / sales_baseline_prev_7_days
                ) * 100
            elif sales_target_last_7_days > 0:
                weekly_comparison_pct = 0

            sales_target_current_30_days = current_30_day_comparison_sales.get(
                sku_city_key, 0
            )
            sales_baseline_previous_30_days = previous_30_day_comparison_sales.get(
                sku_city_key, 0
            )
            monthly_comparison_pct = 0
            if sales_baseline_previous_30_days > 0:
                monthly_comparison_pct = (
                    (sales_target_current_30_days - sales_baseline_previous_30_days)
                    / sales_baseline_previous_30_days
                ) * 100
            elif sales_target_current_30_days > 0:
                monthly_comparison_pct = 0

            # NEW: Get best performing month data
            best_month_info = get_best_performing_month(sku_city_key)

            # Create report item
            report_item = {
                "item_name": item_name,
                "item_id": item_id,
                "city": city,
                "sku_code": sku,
                "warehouse": warehouse,
                "best_performing_month": best_month_info[
                    "formatted"
                ],  # NEW: Main field for display
                "best_performing_month_details": {  # NEW: Detailed breakdown
                    "month_name": best_month_info["month_name"],
                    "year": best_month_info["year"],
                    "quantity_sold": best_month_info["quantity_sold"],
                },
                "metrics": {
                    "avg_daily_on_stock_days": round(avg_daily_on_stock_days, 2),
                    "avg_weekly_on_stock_days": round(avg_weekly_on_stock_days, 2),
                    "avg_monthly_on_stock_days": round(avg_monthly_val_for_period, 2),
                    "total_sales_in_period": round(total_sales_on_common_days, 2),
                    "days_of_coverage": round(doc_calc, 2),
                    "days_with_inventory": days_with_inventory,
                    "closing_stock": round(last_day_inventory, 2),
                    "sales_last_7_days_ending_lcd": round(sales_target_last_7_days, 2),
                    "sales_prev_7_days_before_that": round(
                        sales_baseline_prev_7_days, 2
                    ),
                    "performance_vs_prev_7_days_pct": (
                        round(weekly_comparison_pct, 2)
                        if weekly_comparison_pct != 0
                        else 0
                    ),
                    "sales_last_30_days_ending_lcd": round(
                        sales_target_current_30_days, 2
                    ),
                    "sales_prev_30_days_before_that": round(
                        sales_baseline_previous_30_days, 2
                    ),
                    "performance_vs_prev_30_days_pct": (
                        round(monthly_comparison_pct, 2)
                        if monthly_comparison_pct != 0
                        else 0
                    ),
                },
            }
            report_data.append(report_item)

        print(
            f"Successfully generated dynamic report with {len(report_data)} items for period {period_name}."
        )

        return {
            "message": f"Successfully generated dynamic report for {period_name} with {len(report_data)} items.",
            "data": sorted(report_data, key=lambda d: (d["item_name"], d["city"])),
            "period_info": {
                "start_month": start_month,
                "start_year": start_year,
                "end_month": end_month,
                "end_year": end_year,
                "period_name": period_name,
                "common_dates_count": n_common_days,
            },
        }

    except PyMongoError as e:
        print(f"Database error during report generation for {period_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error for {period_name}: {e}",
        )
    except Exception as e:
        print(f"Error generating report for {period_name}: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred generating report for {period_name}: {str(e)}",
        )


@router.get("/download_report")
async def download_report(
    start_month: int,
    start_year: int,
    end_month: int,
    end_year: int,
    database=Depends(get_database),
):
    """
    Generates report data dynamically for the specified date range,
    creates an Excel file, and returns it for download.
    """
    if not (
        1 <= start_month <= 12
        and 1 <= end_month <= 12
        and start_year > 0
        and end_year > 0
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid month or year provided in the range.",
        )

    try:
        # Generate report data dynamically by calling the generate_report function
        report_response = await generate_report(
            start_month=start_month,
            start_year=start_year,
            end_month=end_month,
            end_year=end_year,
            database=database,
        )

        report_data_list = report_response.get("data", [])
        period_info = report_response.get("period_info", {})

        if not report_data_list:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No report data found for {period_info.get('period_name', 'the specified period')} to download.",
            )

        # --- Generate Excel File in Memory ---
        flattened_data = []
        for item in report_data_list:
            metrics = item.get("metrics", {})
            print(json.dumps(metrics, indent=4))
            flat_item = {
                "Item Name": item.get("item_name", "Unknown Item"),
                "Item ID": item.get("item_id", "Unknown ID"),
                "City": item.get("city", "Unknown"),
                "Sku Code": item.get("sku_code", "Unknown"),
                "Warehouse": item.get("warehouse", "Unknown Warehouse"),
                "Start Month": period_info.get("start_month"),
                "Start Year": period_info.get("start_year"),
                "End Month": period_info.get("end_month"),
                "End Year": period_info.get("end_year"),
                "Period Name": period_info.get("period_name"),
                "Avg Daily Sales (Stock Days)": metrics.get(
                    "avg_daily_on_stock_days", 0
                ),
                "Avg Weekly Sales (Stock Days)": metrics.get(
                    "avg_weekly_on_stock_days", 0
                ),
                "Avg Monthly Sales (Stock Days)": metrics.get(
                    "avg_monthly_on_stock_days", 0
                ),
                "Total Sales (Period)": metrics.get("total_sales_in_period", 0),
                "Days of Coverage (DOC)": metrics.get("days_of_coverage", 0),
                "Days with Inventory": metrics.get("days_with_inventory", 0),
                "Closing Stock": metrics.get("closing_stock", 0),
                "Sales Last 7 Days": metrics.get("sales_last_7_days_ending_lcd", 0),
                "Sales Prev 7 Days": metrics.get("sales_prev_7_days_before_that", 0),
                "Performance vs Prev 7 Days (%)": metrics.get(
                    "performance_vs_prev_7_days_pct", 0
                ),
                "Sales Last 30 Days": metrics.get("sales_last_30_days_ending_lcd", 0),
                "Sales Prev 30 Days": metrics.get("sales_prev_30_days_before_that", 0),
                "Performance vs Prev 30 Days (%)": metrics.get(
                    "performance_vs_prev_30_days_pct", 0
                ),
                "Best Performing Month": item.get("best_performing_month", ""),
                "Quantity Sold in Best Performing Month": item.get(
                    "best_performing_month_details", {"quantity_sold": 0}
                ).get("quantity_sold"),
            }
            flattened_data.append(flat_item)

        # Create DataFrame
        df = pd.DataFrame(flattened_data)

        # Define desired column order
        column_order = [
            "Item Name",
            "Item ID",
            "Sku Code",
            "City",
            "Warehouse",
            "Start Month",
            "Start Year",
            "End Month",
            "End Year",
            "Period Name",
            "Avg Daily Sales (Stock Days)",
            "Avg Weekly Sales (Stock Days)",
            "Avg Monthly Sales (Stock Days)",
            "Total Sales (Period)",
            "Days of Coverage (DOC)",
            "Days with Inventory",
            "Closing Stock",
            "Sales Last 7 Days",
            "Sales Prev 7 Days",
            "Performance vs Prev 7 Days (%)",
            "Sales Last 30 Days",
            "Sales Prev 30 Days",
            "Performance vs Prev 30 Days (%)",
            "Best Performing Month",
            "Quantity Sold in Best Performing Month",
        ]

        df = df.reindex(columns=column_order, fill_value=None)

        # Create a BytesIO buffer to write the Excel file into memory
        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, sheet_name="Sales Inventory Report")
        excel_buffer.seek(0)

        # --- Return as StreamingResponse ---
        filename = f"sales_inventory_report_{start_year}_{start_month}_to_{end_year}_{end_month}.xlsx"

        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Access-Control-Expose-Headers": "Content-Disposition",
        }

        return StreamingResponse(
            excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers=headers,
        )

    except HTTPException as e:
        raise e
    except PyMongoError as e:
        print(f"Database error during report download generation: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        )
    except Exception as e:
        print(f"Error generating report Excel file: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An internal error occurred while generating the Excel file: {e}",
        )


@router.get("/get_report_data")
async def get_report_data(
    start_month: int,
    start_year: int,
    end_month: int,
    end_year: int,
    database=Depends(get_database),
):
    """
    Generates and returns report data dynamically for the specified date range.
    This endpoint is for frontend consumption to display the report data.
    """
    if not (
        1 <= start_month <= 12
        and 1 <= end_month <= 12
        and start_year > 0
        and end_year > 0
    ):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid month or year provided in the range.",
        )

    try:
        # Generate report data dynamically by calling the generate_report function
        report_response = await generate_report(
            start_month=start_month,
            start_year=start_year,
            end_month=end_month,
            end_year=end_year,
            database=database,
        )

        print(
            f"Retrieved dynamic report data for {report_response.get('period_info', {}).get('period_name', 'specified period')}."
        )

        return report_response

    except HTTPException as e:
        raise e
    except PyMongoError as e:
        print(f"Database error during report data retrieval: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        )
    except Exception as e:
        print(f"Error retrieving report data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred retrieving the report data: {e}",
        )
