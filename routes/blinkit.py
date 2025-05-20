# main.py
import pandas as pd
from datetime import datetime, date
import calendar
import os, io
from io import BytesIO

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from ..database import get_database

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
        sku_documents = list(sku_collection.find({}, {"_id": 0}))

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
    Uploads sales data from an Excel file (.xlsx).
    Inserts records if a document with the same Item ID, City, and Order Date doesn't already exist.
    Expected columns: 'Item ID', 'Quantity', 'Order Date', 'Customer City ', 'HSN Code', 'Item Name' (Item Name is not strictly required if SKU mapping is uploaded).
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

        # Load SKU mapping from DB into a dict for quick lookup
        sku_map_dict = {
            doc["item_id"]: doc
            for doc in sku_collection.find(
                {}, {"item_id": 1, "sku_code": 1, "item_name": 1}
            )
        }

        data_to_insert = []
        inserted_count = 0
        skipped_count = 0
        warning_count = 0

        for index, row in df.iterrows():
            item_id = (
                int(str(row["Item ID"]).strip()) if pd.notna(row["Item ID"]) else None
            )
            quantity_raw = row["Quantity"]
            raw_order_date = row["Order Date"]
            city = (
                str(row["Customer City "]).strip()
                if pd.notna(row["Customer City "])
                else None
            )
            hsn_raw = row.get("HSN Code")
            hsn = (
                str(hsn_raw).strip() if pd.notna(hsn_raw) else None
            )  # HSN might be numeric or string

            if not item_id or not city:
                # print(f"Warning: Skipping sales row {index+2} due to missing Item ID or City.") # Too noisy
                warning_count += 1
                continue

            # Process quantity - handle errors
            try:
                quantity = pd.to_numeric(quantity_raw)
                if pd.isna(quantity):
                    quantity = 0
            except (ValueError, TypeError):
                print(
                    f"Warning: Non-numeric sales quantity '{quantity_raw}' in row {index+2} for Item ID '{item_id}'. Using 0."
                )
                quantity = 0
                warning_count += 1

            # Process date - convert to datetime object
            order_date_dt = safe_strptime(
                raw_order_date, "%d %b %Y"
            )  # Attempt to parse dd/mm/YYYY first if string

            if order_date_dt is None:
                print(
                    f"Warning: Skipping sales row {index+2} for Item ID '{item_id}' due to unparseable Order Date '{raw_order_date}'."
                )
                warning_count += 1
                continue

            # Adjust city name if needed
            processed_city = city if city != "Faridabad" else "Gurgaon"

            # Lookup SKU and Item Name from DB mapping, fallback to file if present
            sku_code = None
            item_name = (
                str(row["Item Name"]).strip()
                if "Item Name" in df.columns and pd.notna(row["Item Name"])
                else "Unknown Item"
            )

            if int(item_id) in sku_map_dict:
                sku_code = sku_map_dict[int(item_id)].get("sku_code")
                # Prefer item name from SKU mapping if available
                if sku_map_dict[int(item_id)].get("item_name"):
                    item_name = sku_map_dict[int(item_id)].get("item_name")

            if sku_code is None:
                print(
                    f"Warning: SKU not found in mapping for Item ID: '{item_id}' in row {index+2}."
                )
                warning_count += 1
                # Still include the record, but with unknown SKU
                sku_code = "Unknown SKU"

            # Define unique key for sales record: Item ID, City, Order Date
            # Query MongoDB to check if a document with this key exists
            existing_doc = sales_collection.find_one(
                {
                    "item_id": item_id,
                    "city": processed_city,
                    "order_date": order_date_dt,  # Compare datetime objects
                }
            )

            if existing_doc is None:
                # Document does not exist, prepare to insert
                data_to_insert.append(
                    {
                        "item_id": item_id,
                        "sku_code": sku_code,
                        "hsn_code": hsn,
                        "item_name": item_name,
                        "quantity": quantity,
                        "city": processed_city,
                        "order_date": order_date_dt,  # Store as BSON datetime
                    }
                )
            else:
                # Document exists, check if it's different to decide whether to "update"
                # The requirement is "if it does it will not change them if there is no change".
                # For raw sales, this is tricky. A simple check: are quantity, hsn, sku_code, item_name the same?
                # This might be too complex. A simpler interpretation matching the prompt
                # "If it does not then it will create them, if it does it will not change them"
                # is to simply skip insertion if the unique key (item_id, city, order_date) exists.
                # This prevents duplicate *key* entries but won't update other fields if they changed.
                # Let's stick to the simpler "skip if key exists" for now.
                skipped_count += 1
                # print(f"Skipping sales row {index+2} - duplicate key (Item ID, City, Date) found: {item_id}, {processed_city}, {order_date_dt.strftime('%Y-%m-%d')}") # Too noisy
                pass  # Skip inserting if exists based on the defined key

        if data_to_insert:
            insert_result = sales_collection.insert_many(data_to_insert)
            inserted_count = len(insert_result.inserted_ids)
            print(f"Inserted {inserted_count} new sales records.")

        # Create indexes for efficient querying
        sales_collection.create_index([("order_date", ASCENDING)])
        sales_collection.create_index(
            [("sku_code", ASCENDING), ("city", ASCENDING), ("order_date", ASCENDING)]
        )

        return {
            "message": f"Successfully processed sales data. Inserted {inserted_count} new records, skipped {skipped_count} existing records based on (Item ID, City, Order Date). Encountered {warning_count} warnings (missing data/SKU/date issues)."
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
    Uploads inventory data from an Excel file (.xlsx).
    Aggregates data by SKU, Warehouse, City, and Date.
    For each aggregated record, it inserts or updates the record in the database
    based on the unique key (sku_code, warehouse, city, date).
    Expected columns: 'Item ID', 'Backend Outlet', 'Date', 'Qty', 'Item Name' (Item Name is not strictly required).
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    try:
        file_content = await file.read()
        # Use engine='openpyxl' specifically as recommended
        df = pd.read_excel(BytesIO(file_content), engine="openpyxl")

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

        # Load SKU mapping from DB into a dict for quick lookup
        sku_map_dict = {
            doc["item_id"]: doc
            for doc in sku_collection.find(
                {}, {"item_id": 1, "sku_code": 1, "item_name": 1}
            )
        }

        # --- Aggregation Logic (Similar to original read_inventory_data_aggregated) ---
        aggregated_inventory = {}
        warning_count = 0

        for index, row in df.iterrows():
            item_id_raw = row.get("Item ID")
            item_id = int(str(item_id_raw).strip()) if pd.notna(item_id_raw) else None

            if not item_id:
                # print(f"Warning: Skipping inventory row {index+2} due to missing Item ID.") # Too noisy
                warning_count += 1
                continue

            # Lookup SKU and Item Name from DB mapping, fallback to file if present
            sku_code = None
            item_name = (
                str(row.get("Item Name")).strip()
                if "Item Name" in df.columns and pd.notna(row.get("Item Name"))
                else "Unknown Item"
            )

            if item_id in sku_map_dict:
                sku_code = sku_map_dict[item_id].get("sku_code")
                # Prefer item name from SKU mapping if available
                if sku_map_dict[item_id].get("item_name"):
                    item_name = sku_map_dict[item_id].get("item_name")

            if sku_code is None:
                print(
                    f"Warning: SKU not found in mapping for Item ID: '{item_id}' in row {index+2}."
                )
                warning_count += 1
                sku_code = (
                    "Unknown SKU"  # Still include the record, but with unknown SKU
                )

            warehouse_raw = row.get("Backend Outlet")
            warehouse = (
                str(warehouse_raw).strip()
                if pd.notna(warehouse_raw)
                else "Unknown Warehouse"
            )

            date_raw = row.get("Date")
            date_dt = safe_strptime(
                date_raw, "%Y-%m-%d"
            )  # Assuming YYYY-MM-DD format from Excel or datetime object

            if date_dt is None:
                print(
                    f"Warning: Skipping inventory row {index+2} for Item ID '{item_id}' due to unparseable Date '{date_raw}'."
                )
                warning_count += 1
                continue

            quantity_raw = row.get("Qty")
            try:
                quantity = pd.to_numeric(quantity_raw)
                if pd.isna(quantity):
                    quantity = 0
            except (ValueError, TypeError):
                print(
                    f"Warning: Non-numeric quantity '{quantity_raw}' in row {index+2} for Item ID '{item_id}'. Using 0."
                )
                quantity = 0
                warning_count += 1

            city = get_city_from_warehouse(warehouse, CITIES, WAREHOUSE_CITY_MAP)

            # Use date object (truncated to date) for aggregation key consistency
            group_key = (
                sku_code,
                warehouse,
                city,
                date_dt.replace(hour=0, minute=0, second=0, microsecond=0),
            )

            if group_key in aggregated_inventory:
                aggregated_inventory[group_key]["warehouse_inventory"] += quantity
            else:
                aggregated_inventory[group_key] = {
                    "sku_code": sku_code,
                    "warehouse": warehouse,
                    "city": city,
                    "date": date_dt.replace(hour=0, minute=0, second=0, microsecond=0),
                    "item_id": item_id,
                    "item_name": item_name,
                    "warehouse_inventory": quantity,
                }

        upsert_count = 0
        processed_keys = (
            set()
        )  # Track keys to avoid redundant upserts if multiple rows in Excel map to the same key

        for key, data in aggregated_inventory.items():
            filter_query = {
                "sku_code": data["sku_code"],
                "warehouse": data["warehouse"],
                "city": data["city"],
                "date": data["date"],
            }
            update_result = inventory_collection.replace_one(
                filter_query, data, upsert=True
            )
            if update_result.matched_count > 0 or update_result.upserted_id is not None:
                upsert_count += 1

        print(f"Successfully processed inventory data aggregation.")
        print(f"Upserted/Matched {upsert_count} aggregated inventory records.")
        print(f"Encountered {warning_count} warnings (missing data/SKU/date issues).")

        # Create indexes for efficient querying
        inventory_collection.create_index([("date", ASCENDING)])
        inventory_collection.create_index(
            [
                ("sku_code", ASCENDING),
                ("city", ASCENDING),
                ("warehouse", ASCENDING),
                ("date", ASCENDING),
            ],
            unique=True,
        )  # Unique constraint on the aggregation key

        return {
            "message": f"Successfully uploaded and processed inventory data. Upserted {upsert_count} aggregated records."
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


@router.get("/generate_report")
async def generate_report(month: int, year: int, database=Depends(get_database)):
    """
    Generates the Sales vs Inventory report based on data stored in the database
    for the specified month and year.
    Calculates metrics for each SKU/City pair and saves/updates the results
    in the database.
    Returns a confirmation message upon successful saving.
    """
    if not 1 <= month <= 12 or year <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid month or year provided.",
        )

    try:
        start_date = datetime(year, month, 1)
        # Calculate end date of the month
        last_day = calendar.monthrange(year, month)[1]
        end_date = datetime(
            year, month, last_day, 23, 59, 59, 999999
        )  # Include the whole last day

        sales_collection = database.get_collection(SALES_COLLECTION)
        inventory_collection = database.get_collection(INVENTORY_COLLECTION)
        report_collection = database.get_collection(REPORT_COLLECTION)

        print(
            f"Querying sales and inventory data for {calendar.month_name[month]} {year} to generate report..."
        )

        # Query sales data for the month
        sales_cursor = sales_collection.find(
            {"order_date": {"$gte": start_date, "$lte": end_date}}
        )
        sales_data = list(sales_cursor)
        print(f"Found {len(sales_data)} sales records in DB for the period.")

        # Query aggregated inventory data for the month
        inventory_cursor = inventory_collection.find(
            {
                "date": {
                    "$gte": start_date.replace(
                        hour=0, minute=0, second=0, microsecond=0
                    ),  # Compare date parts
                    "$lte": end_date.replace(hour=0, minute=0, second=0, microsecond=0),
                }
            }
        )
        inventory_data = list(inventory_cursor)
        print(
            f"Found {len(inventory_data)} aggregated inventory records in DB for the period."
        )

        if not sales_data and not inventory_data:
            # Clear any existing report data for this month/year if no source data exists
            delete_result = report_collection.delete_many(
                {"year": year, "month": month}
            )
            if delete_result.deleted_count > 0:
                print(
                    f"Cleared {delete_result.deleted_count} existing report documents for {calendar.month_name[month]} {year} as no source data was found."
                )
            return {
                "message": f"No sales or inventory data found for {calendar.month_name[month]} {year}. No report generated or saved."
            }

        # --- Replicate Aggregation and Calculation Logic ---
        # This part is the same as before - it calculates the metrics into combined_data

        aggregated_sales = {}
        sales_item_info = {}
        all_sales_dates = set()  # Dates as 'YYYY-MM-DD' string

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

            key = (sku, city, date_key_str)
            aggregated_sales[key] = aggregated_sales.get(key, 0) + quantity
            all_sales_dates.add(date_key_str)

            if sku not in sales_item_info:
                sales_item_info[sku] = {"item_name": item_name, "item_id": item_id}

        inventory_map = {}
        inventory_item_info = {}
        all_inventory_dates = set()  # Dates as 'YYYY-MM-DD' string

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

        # Find Common Dates and Sort
        common_dates = sorted(list(all_sales_dates.intersection(all_inventory_dates)))
        n_common_days = len(common_dates)

        if n_common_days == 0:
            # Check if any data existed before clearing
            if sales_data or inventory_data:
                # Still might have some data, but no common dates for calculation
                # Clear any previous report data for this month/year
                delete_result = report_collection.delete_many(
                    {"year": year, "month": month}
                )
                if delete_result.deleted_count > 0:
                    print(
                        f"Cleared {delete_result.deleted_count} existing report documents for {calendar.month_name[month]} {year} due to no common dates."
                    )
                return {
                    "message": f"Found data for {calendar.month_name[month]} {year}, but no common dates between sales and inventory data. No report generated or saved."
                }
            else:
                # No source data at all (already handled above)
                return {
                    "message": f"No sales or inventory data found for {calendar.month_name[month]} {year}. No report generated or saved."
                }

        print(f"\nFound {n_common_days} common dates for calculation.")

        # Identify all unique SKU/City pairs present on common dates
        valid_sku_city_pairs = set()
        for date_str in common_dates:
            for sku, city, date_s in aggregated_sales.keys():
                if date_s == date_str:
                    valid_sku_city_pairs.add((sku, city))
            for sku, city, date_s in inventory_map.keys():
                if date_s == date_str:
                    valid_sku_city_pairs.add((sku, city))

        print(
            f"Calculating metrics for {len(valid_sku_city_pairs)} unique SKU/City pairs..."
        )

        combined_data = {}  # This will store the calculated metrics per (sku, city)

        for sku, city in valid_sku_city_pairs:
            # Get item info, preferring inventory info if available
            item_info = inventory_item_info.get(
                sku,
                sales_item_info.get(
                    sku, {"item_name": "Unknown Item", "item_id": "Unknown ID"}
                ),
            )
            item_name = item_info["item_name"]
            item_id = item_info["item_id"]

            # Find a warehouse associated with this SKU/City on a common date
            warehouse = "Unknown Warehouse"
            for date_str in common_dates:
                inventory_key = (sku, city, date_str)
                if (
                    inventory_key in inventory_map
                    and "warehouse" in inventory_map[inventory_key]
                ):
                    warehouse = inventory_map[inventory_key]["warehouse"]
                    break

            daily_details = {}
            total_sales_on_common_days = 0
            total_sales_on_days_with_inventory = 0
            days_with_inventory = 0

            for date_str in common_dates:
                sales_key = (sku, city, date_str)
                inventory_key = (sku, city, date_str)

                sales_qty = aggregated_sales.get(sales_key, 0)
                inventory_info_daily = inventory_map.get(inventory_key)
                inventory_qty = (
                    inventory_info_daily["inventory"] if inventory_info_daily else 0
                )

                total_sales_on_common_days += sales_qty

                if inventory_qty > 0:
                    total_sales_on_days_with_inventory += sales_qty
                    days_with_inventory += 1

                daily_details[date_str] = {
                    "sales": sales_qty,
                    "inventory": inventory_qty,
                }

            # Calculate Averages based ONLY on days with inventory > 0
            avg_daily = (
                total_sales_on_days_with_inventory / days_with_inventory
                if days_with_inventory > 0
                else 0
            )
            avg_weekly = avg_daily * DAYS_IN_WEEK
            avg_monthly = avg_daily * get_total_days_in_month(year, month)

            last_day_inventory = (
                daily_details.get(common_dates[-1], {}).get("inventory", 0)
                if common_dates
                else 0
            )

            doc_calc = 0
            if avg_daily > 0:
                doc_calc = last_day_inventory / avg_daily
            # Handle the edge case where avg_daily is 0 but closing_stock > 0
            # If there's stock but no sales on days with stock, DOC is effectively infinite.
            # We'll return 0 in this case as before for simplicity, or you could represent it differently.

            # Store calculated metrics in combined_data dictionary (this dictionary is temporary)
            combined_data[(sku, city)] = {
                "item_name": item_name,
                "item_id": item_id,
                "city": city,
                "sku_code": sku,
                "warehouse": warehouse,
                "metrics": {
                    "avg_daily_on_stock_days": round(avg_daily, 2),
                    "avg_weekly_on_stock_days": round(avg_weekly, 2),
                    "avg_monthly_on_stock_days": round(avg_monthly, 2),
                    "total_sales_in_period": round(total_sales_on_common_days, 2),
                    "days_of_coverage": round(doc_calc, 2),
                    "days_with_inventory": days_with_inventory,
                    "closing_stock": round(last_day_inventory, 2),
                },
                # Note: daily_data is *not* stored in the report document to keep it lighter
            }

        # --- Database Saving Logic ---
        # Now, iterate through the calculated results in combined_data and save them to the report collection
        print(
            f"Saving/Updating {len(combined_data)} report documents in the database..."
        )
        upserted_count = 0
        processed_keys = (
            set()
        )  # To avoid redundant upserts if needed, though combined_data keys are already unique

        for (sku, city), data in combined_data.items():
            # Define the filter query for finding an existing report document
            filter_query = {
                "year": year,
                "month": month,
                "sku_code": sku,
                "city": city,
            }

            # The document to be saved/replaced
            # Include year, month, and generation timestamp
            report_document = {
                "year": year,
                "month": month,
                "generated_at": datetime.utcnow(),  # Timestamp for when this report was generated
                "sku_code": sku,
                "city": city,
                "item_name": data.get("item_name", "Unknown Item"),  # Include item name
                "item_id": data.get("item_id", "Unknown ID"),  # Include item ID
                "warehouse": data.get(
                    "warehouse", "Unknown Warehouse"
                ),  # Include warehouse
                "metrics": data["metrics"],  # Include the calculated metrics
            }

            # Use replace_one with upsert=True
            # This checks if a document matching filter_query exists:
            # - If yes, it replaces the existing document with report_document.
            # - If no, it inserts report_document as a new document.
            # This effectively updates the report data for this specific (month, year, sku, city)
            # or creates it if it's the first time generating the report for this combo.
            update_result = report_collection.replace_one(
                filter_query, report_document, upsert=True
            )

            if update_result.matched_count > 0 or update_result.upserted_id is not None:
                upserted_count += 1

        print(f"Successfully saved/updated {upserted_count} report documents.")

        # Create a compound index for efficient querying of reports
        report_collection.create_index(
            [
                ("year", ASCENDING),
                ("month", ASCENDING),
                ("sku_code", ASCENDING),
                ("city", ASCENDING),
            ],
            unique=True,
        )

        # Return a success message indicating data was saved
        return {
            "message": f"Successfully generated report for {calendar.month_name[month]} {year} and saved/updated {upserted_count} documents in the '{REPORT_COLLECTION}' collection."
        }

    except PyMongoError as e:
        print(f"Database error during report generation and saving: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        )
    except Exception as e:
        print(f"Error generating and saving report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred generating and saving the report: {e}",
        )


@router.get("/download_report")
async def download_report(month: int, year: int, database=Depends(get_database)):
    """
    Retrieves saved report data for the specified month/year from the database,
    generates an Excel file, and returns it for download.
    """
    if not 1 <= month <= 12 or year <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid month or year provided.",
        )

    try:
        report_collection = database[REPORT_COLLECTION]

        # Fetch the saved report data for the month/year
        # Exclude _id and generated_at as they might not be needed or require formatting in Excel
        report_cursor = report_collection.find(
            {"year": year, "month": month}, {"_id": 0, "generated_at": 0}
        ).sort([("date", ASCENDING), ("item_name", ASCENDING)])

        report_data_list = list(report_cursor)

        if not report_data_list:
            # Return a 404 if no saved data exists for this period
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No saved report data found for {calendar.month_name[month]} {year} to download.",
            )

        # --- Generate Excel File in Memory ---

        # Prepare data for DataFrame by flattening the 'metrics' dictionary
        flattened_data = []
        for item in report_data_list:
            # Start with the top-level fields you want in the Excel
            flat_item = {
                "Item Name": item.get("item_name", "Unknown Item"),
                "Item ID": item.get("item_id", "Unknown ID"),
                "City": item.get("city", "Unknown"),
                "Sku Code": item.get("sku_code", "Unknown"),
                "Warehouse": item.get("warehouse", "Unknown Warehouse"),
                "Report Month": item.get("month"),  # Include month/year for clarity
                "Report Year": item.get("year"),
            }
            # Add metrics fields from the 'metrics' dictionary
            metrics = item.get("metrics", {})
            flat_item["Avg Daily (Stock Days)"] = metrics.get(
                "avg_daily_on_stock_days", 0
            )
            flat_item["Avg Weekly (Stock Days)"] = metrics.get(
                "avg_weekly_on_stock_days", 0
            )
            flat_item["Avg Monthly (Stock Days)"] = metrics.get(
                "avg_monthly_on_stock_days", 0
            )
            flat_item["Total Sales (Period)"] = metrics.get("total_sales_in_period", 0)
            flat_item["Days of Coverage (DOC)"] = metrics.get("days_of_coverage", 0)
            flat_item["Days with Inventory"] = metrics.get("days_with_inventory", 0)
            flat_item["Closing Stock"] = (
                metrics.get("closing_stock", 0),
            )  # Note: get() returns a tuple here due to trailing comma, fix needed!

            # Correcting the metrics part to not have the trailing comma
            metrics = item.get("metrics", {})
            flat_item = {
                "Item Name": item.get("item_name", "Unknown Item"),
                "Item ID": item.get("item_id", "Unknown ID"),
                "City": item.get("city", "Unknown"),
                "Sku Code": item.get("sku_code", "Unknown"),
                "Warehouse": item.get("warehouse", "Unknown Warehouse"),
                "Report Month": item.get("month"),
                "Report Year": item.get("year"),
                "Avg Daily (Stock Days)": metrics.get("avg_daily_on_stock_days", 0),
                "Avg Weekly (Stock Days)": metrics.get("avg_weekly_on_stock_days", 0),
                "Avg Monthly (Stock Days)": metrics.get("avg_monthly_on_stock_days", 0),
                "Total Sales (Period)": metrics.get("total_sales_in_period", 0),
                "Days of Coverage (DOC)": metrics.get("days_of_coverage", 0),
                "Days with Inventory": metrics.get("days_with_inventory", 0),
                "Closing Stock": metrics.get(
                    "closing_stock", 0
                ),  # Removed trailing comma
            }

            flattened_data.append(flat_item)

        # Create DataFrame
        df = pd.DataFrame(flattened_data)

        # Define desired column order (optional, but good for consistent output)
        column_order = [
            "Item Name",
            "Item ID",
            "Sku Code",
            "City",
            "Warehouse",
            "Report Month",
            "Report Year",
            "Avg Daily (Stock Days)",
            "Avg Weekly (Stock Days)",
            "Avg Monthly (Stock Days)",
            "Total Sales (Period)",
            "Days of Coverage (DOC)",
            "Days with Inventory",
            "Closing Stock",
        ]
        # Reindex the DataFrame to enforce the order, handling missing columns gracefully
        # Use .reindex(columns=...) instead of just df[column_order]
        df = df.reindex(columns=column_order, fill_value=None)

        # Create a BytesIO buffer to write the Excel file into memory
        excel_buffer = io.BytesIO()
        # Write the DataFrame to the buffer using openpyxl engine
        # index=False prevents writing the DataFrame index to the Excel file
        df.to_excel(excel_buffer, index=False, sheet_name="Report Summary")
        # Seek to the beginning of the buffer so the StreamingResponse can read from the start
        excel_buffer.seek(0)

        # --- Return as StreamingResponse ---
        filename = f"sales_inventory_report_{year}_{month}.xlsx"

        # Set headers for file download
        headers = {
            # Tells the browser to treat this as an attachment and suggests the filename
            "Content-Disposition": f'attachment; filename="{filename}"',
            # Essential for CORS if frontend and backend are on different origins
            # Allows the frontend JavaScript to read the Content-Disposition header
            "Access-Control-Expose-Headers": "Content-Disposition",
        }

        return StreamingResponse(
            excel_buffer,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",  # Correct MIME type for .xlsx
            headers=headers,
        )

    except PyMongoError as e:
        print(f"Database error during report download generation: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        )
    except HTTPException as e:
        # Re-raise HTTPException, e.g., 404 from no data found
        raise e
    except Exception as e:
        # Catch any other unexpected errors during file generation
        print(f"Error generating report Excel file: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An internal error occurred while generating the Excel file: {e}",
        )


# --- Optional: Add a route to retrieve the saved report data ---
# You might want an endpoint to *read* the generated report data back out.
@router.get("/get_saved_report")
async def get_saved_report(month: int, year: int, database=Depends(get_database)):
    """
    Retrieves the previously saved Sales vs Inventory report data
    for the specified month and year.
    """
    if not 1 <= month <= 12 or year <= 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid month or year provided.",
        )

    try:
        report_collection = database.get_collection(REPORT_COLLECTION)

        report_cursor = report_collection.find(
            {"year": year, "month": month},
            {"_id": 0},
        ).sort([("date", ASCENDING), ("item_name", ASCENDING)])

        report_data = list(report_cursor)

        if not report_data:
            return {
                "message": f"No saved report found for {calendar.month_name[month]} {year}."
            }

        print(
            f"Retrieved {len(report_data)} saved report documents for {calendar.month_name[month]} {year}."
        )

        return report_data  # Return the list of report documents as JSON

    except PyMongoError as e:
        print(f"Database error during report retrieval: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {e}",
        )
    except Exception as e:
        print(f"Error retrieving saved report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred retrieving the saved report: {e}",
        )
