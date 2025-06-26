# main.py
import pandas as pd
from datetime import datetime, date, timedelta
import calendar, io, json
from io import BytesIO

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from bson import ObjectId
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
        sku_documents = serialize_mongo_document(
            list(sku_collection.find({}).sort("item_name", 1))
        )

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


@router.post("/create_single_item")
async def create_single_item(body: dict):
    try:
        item_name = body.get("item_name")
        item_id = body.get("item_id")
        sku_code = body.get("sku_code")
        db = get_database()
        result = db[SKU_COLLECTION].find_one(
            {"item_name": item_name, "item_id": item_id}
        )
        if result:
            raise HTTPException(
                status_code=400, detail="Item Name and Item Id already Exists"
            )
        else:
            db[SKU_COLLECTION].insert_one(
                {"item_name": item_name, "item_id": int(item_id), "sku_code": sku_code}
            )
            return "Item Created"
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error uploading sales data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred creating the item: {e}",
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
            "Item Id",
            "Quantity",
            "Order Date",
            "Customer City",
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
            subset=["Item Id", "Customer City"]
        )  # Remove rows with missing critical data

        # Convert data types efficiently
        df["Item Id"] = (
            pd.to_numeric(df["Item Id"], errors="coerce").fillna(0).astype(int)
        )
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
        df["Customer City"] = df["Customer City"].astype(str).str.strip()
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
        df["processed_city"] = df["Customer City"].replace("Faridabad", "Gurgaon")

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
            key = (int(row["Item Id"]), row["processed_city"], row["processed_date"])
            return key not in existing_docs

        df["is_new"] = df.apply(is_new_record, axis=1)
        new_records_df = df[df["is_new"]]

        # Prepare bulk insert operations
        bulk_operations = []
        for _, row in new_records_df.iterrows():
            doc = {
                "item_id": int(row["Item Id"]),
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
    Optimized inventory data upload with city-level aggregation.
    FIXED: Ensures SKU mapping is applied correctly before aggregation.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    try:
        file_content = await file.read()

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
        print(f"Retrieved SKU mapping with {len(sku_map_dict)} entries")
        print(json.dumps(serialize_mongo_document(sku_map_dict), indent=4))
        # Clean and process data efficiently
        df = df.dropna(subset=["Item ID"])

        # CRITICAL FIX: Ensure Item ID conversion is consistent with sales data
        df["Item ID"] = (
            pd.to_numeric(df["Item ID"], errors="coerce").fillna(0).astype(int)
        )
        df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)

        # Clean Backend Outlet names
        df["Backend Outlet"] = df["Backend Outlet"].astype(str).str.strip()

        # Process dates
        def process_inventory_dates(dates):
            processed = []
            for date in dates:
                parsed_date = safe_strptime(date, "%Y-%m-%d")
                if parsed_date:
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

        # Apply SKU mapping with proper type conversion
        def get_sku_info(item_id):
            # Ensure item_id is converted to int for mapping lookup
            try:
                item_id_int = int(item_id)
                sku_info = sku_map_dict.get(item_id_int, {})
                return {
                    "sku_code": sku_info.get("sku_code", "Unknown SKU"),
                    "item_name": sku_info.get("item_name", "Unknown Item"),
                }
            except (ValueError, TypeError):
                print(f"Invalid Item ID for SKU mapping: {item_id}")
                return {"sku_code": "Unknown SKU", "item_name": "Unknown Item"}

        # Apply SKU mapping
        sku_mapping_results = df["Item ID"].apply(get_sku_info)
        df["sku_code"] = [result["sku_code"] for result in sku_mapping_results]
        df["item_name"] = [result["item_name"] for result in sku_mapping_results]

        # Handle item names from file if available
        if "Item Name" in df.columns:
            df["item_name"] = df["Item Name"].fillna(df["item_name"])

        # Add city mapping
        df["city"] = df["Backend Outlet"].apply(
            lambda warehouse: get_city_from_warehouse(
                warehouse, CITIES, WAREHOUSE_CITY_MAP
            )
        )

        # IMPORTANT: Filter out records with Unknown SKU BEFORE aggregation
        # This prevents Unknown SKU records from being created in the first place
        df_with_valid_sku = df[df["sku_code"] != "Unknown SKU"].copy()
        df_with_unknown_sku = df[df["sku_code"] == "Unknown SKU"].copy()

        print(f"Proceeding with {len(df_with_valid_sku)} records with valid SKUs")
        print(f"Excluding {len(df_with_unknown_sku)} records with Unknown SKUs")

        if len(df_with_unknown_sku) > 0:
            print("Records with Unknown SKU (will be excluded):")
            for _, row in df_with_unknown_sku.head(5).iterrows():
                print(
                    f"  Item ID: {row['Item ID']}, Warehouse: {row['Backend Outlet']}, Qty: {row['Qty']}"
                )

        # Proceed with city-level aggregation using only valid SKU records
        aggregation_columns = [
            "sku_code",
            "city",
            "processed_date",
            "Item ID",
            "item_name",
        ]

        # Perform city-level aggregation on valid SKU records only
        aggregated_df = (
            df_with_valid_sku.groupby(aggregation_columns, as_index=False)
            .agg(
                {
                    "Qty": "sum",
                    "Backend Outlet": lambda x: " + ".join(sorted(set(x))),
                }
            )
            .reset_index(drop=True)
        )

        print(f"After city aggregation: {len(aggregated_df)} records")

        # Verify no Unknown SKUs in final data
        final_unknown_count = len(
            aggregated_df[aggregated_df["sku_code"] == "Unknown SKU"]
        )
        if final_unknown_count > 0:
            print(
                f"WARNING: {final_unknown_count} Unknown SKU records still present after filtering!"
            )
        else:
            print("✓ No Unknown SKU records in final aggregated data")

        aggregated_df.rename(
            columns={
                "Qty": "warehouse_inventory",
                "Backend Outlet": "combined_warehouses",
            },
            inplace=True,
        )

        # Rest of the function remains the same...
        # (Delete old records and insert new ones)

        dates_to_process = list(aggregated_df["processed_date"].unique())
        skus_to_process = list(aggregated_df["sku_code"].unique())
        cities_to_process = list(aggregated_df["city"].unique())

        print(
            f"Cleaning up old records for {len(dates_to_process)} dates, {len(skus_to_process)} SKUs, {len(cities_to_process)} cities..."
        )

        delete_filter = {
            "date": {"$in": dates_to_process},
            "sku_code": {"$in": skus_to_process},
            "city": {"$in": cities_to_process},
        }

        delete_result = inventory_collection.delete_many(delete_filter)
        print(f"Deleted {delete_result.deleted_count} old inventory records")

        # Insert new city-aggregated records
        bulk_operations = []
        for _, row in aggregated_df.iterrows():
            document = {
                "sku_code": row["sku_code"],
                "city": row["city"],
                "date": row["processed_date"],
                "item_id": int(row["Item ID"]),
                "item_name": row["item_name"],
                "warehouse_inventory": float(row["warehouse_inventory"]),
                "combined_warehouses": row["combined_warehouses"],
            }
            bulk_operations.append(InsertOne(document))

        insert_count = 0
        if bulk_operations:
            batch_size = 1000
            for i in range(0, len(bulk_operations), batch_size):
                batch = bulk_operations[i : i + batch_size]
                result = inventory_collection.bulk_write(batch, ordered=False)
                insert_count += result.inserted_count

        excluded_unknown_count = len(df_with_unknown_sku)

        return {
            "message": f"Successfully uploaded and processed inventory data with city-level aggregation. "
            f"Deleted {delete_result.deleted_count} old records, inserted {insert_count} new city-aggregated records. "
            f"Excluded {excluded_unknown_count} records with Unknown SKU."
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
    Calculates metrics for each SKU/City pair using city-aggregated inventory data.
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

        # Query ALL sales data for lifetime best performing month calculation
        print("Querying lifetime sales data for best performing month calculation...")
        lifetime_sales_cursor = sales_collection.find({})
        lifetime_sales_data = list(lifetime_sales_cursor)
        print(
            f"Found {len(lifetime_sales_data)} total sales records for lifetime analysis."
        )

        # Query city-aggregated inventory data for the entire period
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
            f"Found {len(inventory_data)} city-aggregated inventory records in DB for the period {period_name}."
        )

        # Debug: Show city-aggregated inventory data structure
        print("Sample city-aggregated inventory records:")
        for record in inventory_data[:5]:
            combined_warehouses = record.get("combined_warehouses", "N/A")
            print(
                f"  SKU: {record.get('sku_code')}, City: {record.get('city')}, "
                f"Date: {record.get('date')}, Inventory: {record.get('warehouse_inventory')}, "
                f"Source Warehouses: {combined_warehouses}"
            )

        # Debug: Check if we have any city-aggregated records (multiple warehouses combined)
        aggregated_records = [
            r for r in inventory_data if "+" in r.get("combined_warehouses", "")
        ]
        print(
            f"Found {len(aggregated_records)} city-aggregated records (with multiple warehouses)"
        )
        if aggregated_records:
            print("Sample aggregated records:")
            for record in aggregated_records[:3]:
                print(
                    f"  SKU: {record.get('sku_code')}, City: {record.get('city')}, "
                    f"Total Inventory: {record.get('warehouse_inventory')}, "
                    f"Combined from: {record.get('combined_warehouses')}"
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

        # Lifetime monthly sales tracking for best performing month calculation
        lifetime_monthly_sales = {}

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
                month_year_key = order_date_dt.strftime("%Y-%m")
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

        # Process city-aggregated inventory data
        inventory_map = {}  # Structure: {(sku, city, date): inventory_info}
        inventory_item_info = {}
        all_inventory_dates = set()

        # NEW: Create a mapping to store combined_warehouses info for each SKU/City combination
        sku_city_warehouses_map = {}

        for record in inventory_data:
            sku = record.get("sku_code")
            city = record.get("city")
            inventory_date_dt = record.get("date")
            inventory_qty = record.get("warehouse_inventory", 0)
            item_name = record.get("item_name", "Unknown Item")
            item_id = record.get("item_id", "Unknown ID")
            combined_warehouses = record.get(
                "combined_warehouses", "Unknown Warehouses"
            )

            if not sku or not city or not inventory_date_dt:
                continue
            inventory_qty = (
                float(inventory_qty) if isinstance(inventory_qty, (int, float)) else 0
            )
            if isinstance(inventory_date_dt, datetime):
                date_key_str = inventory_date_dt.strftime("%Y-%m-%d")
            else:
                continue

            # Key is now city-level (no warehouse)
            key = (sku, city, date_key_str)
            inventory_map[key] = {
                "inventory": inventory_qty,
                "item_name": item_name,
                "item_id": item_id,
                "combined_warehouses": combined_warehouses,
            }
            all_inventory_dates.add(date_key_str)
            if sku not in inventory_item_info:
                inventory_item_info[sku] = {"item_name": item_name, "item_id": item_id}

            # NEW: Store the combined_warehouses info for this SKU/City combination
            sku_city_key = (sku, city)
            if sku_city_key not in sku_city_warehouses_map:
                sku_city_warehouses_map[sku_city_key] = combined_warehouses
            else:
                # If we have multiple records for the same SKU/City, prefer the most detailed one
                existing_warehouses = sku_city_warehouses_map[sku_city_key]
                if (
                    len(combined_warehouses) > len(existing_warehouses)
                    or existing_warehouses == "Unknown Warehouses"
                ):
                    sku_city_warehouses_map[sku_city_key] = combined_warehouses

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

        # Performance comparison calculations remain the same for sales data
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

        # Build valid SKU/City combinations (no more warehouse separation)
        valid_sku_city_pairs = set()

        # Get SKU/City combinations from sales data
        for sku, city, date_s in current_period_sales.keys():
            if date_s in common_dates:
                valid_sku_city_pairs.add((sku, city))

        # Get SKU/City combinations from inventory data
        for sku, city, date_s in inventory_map.keys():
            if date_s in common_dates:
                valid_sku_city_pairs.add((sku, city))

        print(
            f"Calculating metrics for {len(valid_sku_city_pairs)} unique SKU/City combinations for period {period_name}..."
        )

        # Helper function for best performing month
        def get_best_performing_month(sku_city_key):
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

            best_month_key = max(
                lifetime_monthly_sales[sku_city_key],
                key=lifetime_monthly_sales[sku_city_key].get,
            )
            best_month_quantity = lifetime_monthly_sales[sku_city_key][best_month_key]

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

        # Process each SKU/City combination (city-aggregated)
        for sku, city in valid_sku_city_pairs:
            item_info = inventory_item_info.get(
                sku,
                sales_item_info.get(
                    sku, {"item_name": "Unknown Item", "item_id": "Unknown ID"}
                ),
            )
            item_name = item_info["item_name"]
            item_id = item_info["item_id"]

            # FIXED: Get combined warehouses info from the pre-built mapping
            sku_city_key = (sku, city)
            combined_warehouses = sku_city_warehouses_map.get(
                sku_city_key, "Unknown Warehouses"
            )

            total_sales_on_common_days = 0
            total_sales_on_days_with_inventory = 0
            days_with_inventory = 0

            for date_str in common_dates:
                # Both sales and inventory are now at city level
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
                last_common_date = common_dates[-1]
                inventory_at_lcd_key = (sku, city, last_common_date)
                last_day_inventory_info = inventory_map.get(inventory_at_lcd_key)
                if last_day_inventory_info:
                    last_day_inventory = last_day_inventory_info["inventory"]
                    # Debug: Verify we're getting city-aggregated inventory
                    warehouses_info = last_day_inventory_info.get(
                        "combined_warehouses", "Unknown"
                    )
                    print(
                        f"  Closing stock for SKU {sku} in {city}: {last_day_inventory} (from {warehouses_info})"
                    )
                else:
                    # If no inventory found for last common date, try to find the most recent inventory
                    print(
                        f"  No inventory found for SKU {sku} in {city} on {last_common_date}, checking for most recent..."
                    )
                    most_recent_inventory = 0
                    most_recent_date = None
                    for date_str in reversed(
                        common_dates
                    ):  # Check dates in reverse order
                        check_key = (sku, city, date_str)
                        check_inventory_info = inventory_map.get(check_key)
                        if check_inventory_info:
                            most_recent_inventory = check_inventory_info["inventory"]
                            most_recent_date = date_str
                            warehouses_info = check_inventory_info.get(
                                "combined_warehouses", "Unknown"
                            )
                            print(
                                f"  Found most recent inventory for SKU {sku} in {city} on {most_recent_date}: {most_recent_inventory} (from {warehouses_info})"
                            )
                            break
                    last_day_inventory = most_recent_inventory

            doc_calc = 0
            if avg_daily_on_stock_days > 0:
                doc_calc = last_day_inventory / avg_daily_on_stock_days

            # Performance comparisons (city level)
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

            # Get best performing month data
            best_month_info = get_best_performing_month(sku_city_key)

            # Create report item with city-aggregated data
            report_item = {
                "item_name": item_name,
                "item_id": item_id,
                "city": city,
                "warehouse": combined_warehouses,  # Now properly shows combined warehouse names
                "sku_code": sku,
                "best_performing_month": best_month_info["formatted"],
                "best_performing_month_details": {
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

        # Sort by item name, city (no warehouse sorting needed)
        sorted_report_data = sorted(
            report_data, key=lambda d: (d["item_name"], d["city"])
        )

        return {
            "message": f"Successfully generated dynamic report for {period_name} with {len(report_data)} items.",
            "data": sorted_report_data,
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


@router.get("/generate_report_by_date_range")
async def generate_report_by_date_range(
    start_date: str,  # Format: YYYY-MM-DD
    end_date: str,  # Format: YYYY-MM-DD
    database=Depends(get_database),
):
    """
    Generates the Sales vs Inventory report dynamically based on the specified date range.
    Accepts any date range (start_date to end_date) instead of month ranges.
    """
    try:
        # Parse and validate dates
        try:
            start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
            print(start_date_dt, end_date_dt)
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use YYYY-MM-DD format.",
            )

        # Set time boundaries for the date range
        overall_start_date = start_date_dt.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        overall_end_date = end_date_dt.replace(
            hour=23, minute=59, second=59, microsecond=999999
        )

        if overall_start_date > overall_end_date:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Start date cannot be after end date.",
            )

        # Calculate the number of days in the range
        date_range_days = (end_date_dt - start_date_dt).days + 1

        period_name = f"{start_date} to {end_date}"

        sales_collection = database.get_collection(SALES_COLLECTION)
        inventory_collection = database.get_collection(INVENTORY_COLLECTION)

        print(
            f"Querying sales and inventory data for {period_name} to generate dynamic report..."
        )

        # Query sales data for the date range
        sales_cursor = sales_collection.find(
            {"order_date": {"$gte": overall_start_date, "$lte": overall_end_date}}
        )
        sales_data = list(sales_cursor)
        print(
            f"Found {len(sales_data)} sales records in DB for the period {period_name}."
        )

        # Query ALL sales data for lifetime best performing month calculation
        print("Querying lifetime sales data for best performing month calculation...")
        lifetime_sales_cursor = sales_collection.find({})
        lifetime_sales_data = list(lifetime_sales_cursor)
        print(
            f"Found {len(lifetime_sales_data)} total sales records for lifetime analysis."
        )

        # Query city-aggregated inventory data for the date range
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
            f"Found {len(inventory_data)} city-aggregated inventory records in DB for the period {period_name}."
        )

        if not sales_data and not inventory_data:
            return {
                "message": f"No sales or inventory data found for {period_name}. No report generated.",
                "data": [],
                "period_info": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "period_name": period_name,
                    "days_in_range": date_range_days,
                },
            }

        # --- Process Sales Data for Current Period ---
        current_period_sales = {}
        sales_item_info = {}
        all_sales_dates = set()

        # Lifetime monthly sales tracking for best performing month calculation
        lifetime_monthly_sales = {}

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
                month_year_key = order_date_dt.strftime("%Y-%m")
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

        # Process city-aggregated inventory data
        inventory_map = {}
        inventory_item_info = {}
        all_inventory_dates = set()
        sku_city_warehouses_map = {}

        for record in inventory_data:
            sku = record.get("sku_code")
            city = record.get("city")
            inventory_date_dt = record.get("date")
            inventory_qty = record.get("warehouse_inventory", 0)
            item_name = record.get("item_name", "Unknown Item")
            item_id = record.get("item_id", "Unknown ID")
            combined_warehouses = record.get(
                "combined_warehouses", "Unknown Warehouses"
            )

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
                "combined_warehouses": combined_warehouses,
            }
            all_inventory_dates.add(date_key_str)
            if sku not in inventory_item_info:
                inventory_item_info[sku] = {"item_name": item_name, "item_id": item_id}

            sku_city_key = (sku, city)
            if sku_city_key not in sku_city_warehouses_map:
                sku_city_warehouses_map[sku_city_key] = combined_warehouses
            else:
                existing_warehouses = sku_city_warehouses_map[sku_city_key]
                if (
                    len(combined_warehouses) > len(existing_warehouses)
                    or existing_warehouses == "Unknown Warehouses"
                ):
                    sku_city_warehouses_map[sku_city_key] = combined_warehouses

        common_dates = sorted(list(all_sales_dates.intersection(all_inventory_dates)))
        n_common_days = len(common_dates)

        if n_common_days == 0:
            return {
                "message": f"Found data for {period_name}, but no common dates between sales and inventory data. No report generated.",
                "data": [],
                "period_info": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "period_name": period_name,
                    "days_in_range": date_range_days,
                },
            }

        print(
            f"\nFound {n_common_days} common dates for calculation within {period_name}."
        )

        # Performance comparison calculations
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

        # Build valid SKU/City combinations
        valid_sku_city_pairs = set()

        for sku, city, date_s in current_period_sales.keys():
            if date_s in common_dates:
                valid_sku_city_pairs.add((sku, city))

        for sku, city, date_s in inventory_map.keys():
            if date_s in common_dates:
                valid_sku_city_pairs.add((sku, city))

        print(
            f"Calculating metrics for {len(valid_sku_city_pairs)} unique SKU/City combinations for period {period_name}..."
        )

        # Helper function for best performing month
        def get_best_performing_month(sku_city_key):
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

            best_month_key = max(
                lifetime_monthly_sales[sku_city_key],
                key=lifetime_monthly_sales[sku_city_key].get,
            )
            best_month_quantity = lifetime_monthly_sales[sku_city_key][best_month_key]

            year, month = best_month_key.split("-")
            month_name = calendar.month_name[int(month)]

            return {
                "month_name": month_name,
                "year": int(year),
                "quantity_sold": round(best_month_quantity, 2),
                "formatted": f"{month_name} {year}",
            }

        report_data = []

        # Process each SKU/City combination
        for sku, city in valid_sku_city_pairs:
            item_info = inventory_item_info.get(
                sku,
                sales_item_info.get(
                    sku, {"item_name": "Unknown Item", "item_id": "Unknown ID"}
                ),
            )
            item_name = item_info["item_name"]
            item_id = item_info["item_id"]

            sku_city_key = (sku, city)
            combined_warehouses = sku_city_warehouses_map.get(
                sku_city_key, "Unknown Warehouses"
            )

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

            # Calculate average based on the number of days in the date range
            avg_daily_for_period = (
                total_sales_on_common_days / n_common_days if n_common_days > 0 else 0
            )

            # Calculate monthly average based on 30-day periods
            avg_monthly_val_for_period = avg_daily_for_period * 30

            last_day_inventory = 0
            if common_dates:
                last_common_date = common_dates[-1]
                inventory_at_lcd_key = (sku, city, last_common_date)
                last_day_inventory_info = inventory_map.get(inventory_at_lcd_key)
                if last_day_inventory_info:
                    last_day_inventory = last_day_inventory_info["inventory"]
                else:
                    # Find the most recent inventory
                    most_recent_inventory = 0
                    for date_str in reversed(common_dates):
                        check_key = (sku, city, date_str)
                        check_inventory_info = inventory_map.get(check_key)
                        if check_inventory_info:
                            most_recent_inventory = check_inventory_info["inventory"]
                            break
                    last_day_inventory = most_recent_inventory

            doc_calc = 0
            if avg_daily_on_stock_days > 0:
                doc_calc = last_day_inventory / avg_daily_on_stock_days

            # Performance comparisons
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

            # Get best performing month data
            best_month_info = get_best_performing_month(sku_city_key)

            # Create report item
            report_item = {
                "item_name": item_name,
                "item_id": item_id,
                "city": city,
                "warehouse": combined_warehouses,
                "sku_code": sku,
                "best_performing_month": best_month_info["formatted"],
                "best_performing_month_details": {
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

        # Sort by item name, city
        sorted_report_data = sorted(
            report_data, key=lambda d: (d["item_name"], d["city"])
        )

        return {
            "message": f"Successfully generated dynamic report for {period_name} with {len(report_data)} items.",
            "data": sorted_report_data,
            "period_info": {
                "start_date": start_date,
                "end_date": end_date,
                "period_name": period_name,
                "common_dates_count": n_common_days,
                "days_in_range": date_range_days,
            },
        }

    except PyMongoError as e:
        print(f"Database error during report generation for {period_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error for {period_name}: {e}",
        )
    except Exception as e:
        # print(f"Error generating report for {period_name}: {e}")
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred generating report for : {str(e)}",
        )


@router.get("/download_report_by_date_range")
async def download_report_by_date_range(
    start_date: str,  # Format: YYYY-MM-DD
    end_date: str,  # Format: YYYY-MM-DD
    database=Depends(get_database),
):
    """
    Downloads report for a specific date range as Excel file.
    """
    try:
        # Generate report data dynamically
        report_response = await generate_report_by_date_range(
            start_date=start_date,
            end_date=end_date,
            database=database,
        )

        report_data_list = report_response.get("data", [])
        period_info = report_response.get("period_info", {})

        if not report_data_list:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No report data found for {period_info.get('period_name', 'the specified period')} to download.",
            )

        # Generate Excel file (same logic as existing download_report)
        flattened_data = []
        for item in report_data_list:
            metrics = item.get("metrics", {})
            flat_item = {
                "Item Name": item.get("item_name", "Unknown Item"),
                "Item ID": item.get("item_id", "Unknown ID"),
                "City": item.get("city", "Unknown"),
                "Sku Code": item.get("sku_code", "Unknown"),
                "Warehouse": item.get("warehouse", "Unknown Warehouse"),
                "Start Date": period_info.get("start_date"),
                "End Date": period_info.get("end_date"),
                "Period Name": period_info.get("period_name"),
                "Days in Range": period_info.get("days_in_range"),
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

        # Create DataFrame and Excel file
        df = pd.DataFrame(flattened_data)

        column_order = [
            "Item Name",
            "Item ID",
            "Sku Code",
            "City",
            "Warehouse",
            "Start Date",
            "End Date",
            "Period Name",
            "Days in Range",
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

        excel_buffer = io.BytesIO()
        df.to_excel(excel_buffer, index=False, sheet_name="Sales Inventory Report")
        excel_buffer.seek(0)

        filename = f"sales_inventory_report_{start_date}_to_{end_date}.xlsx"
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
    except Exception as e:
        print(f"Error generating report Excel file: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An internal error occurred while generating the Excel file: {e}",
        )


@router.get("/get_report_data_by_date_range")
async def get_report_data_by_date_range(
    start_date: str,  # Format: YYYY-MM-DD
    end_date: str,  # Format: YYYY-MM-DD
    database=Depends(get_database),
):
    """
    Returns report data for a specific date range for frontend consumption.
    """
    try:
        report_response = await generate_report_by_date_range(
            start_date=start_date,
            end_date=end_date,
            database=database,
        )

        print(
            f"Retrieved dynamic report data for {report_response.get('period_info', {}).get('period_name', 'specified period')}."
        )
        return report_response

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error retrieving report data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred retrieving the report data: {e}",
        )


@router.delete("/delete_item/{item_id}")
async def delete_item(item_id: str):
    try:
        db = get_database()
        result = db[SKU_COLLECTION].delete_one({"_id": ObjectId(item_id)})
        if result.deleted_count == 1:
            return JSONResponse(status_code=200, content="Item Deleted Successfully")

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error uploading sales data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the file: {e}",
        )
