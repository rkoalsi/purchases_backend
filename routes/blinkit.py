# main.py
import pandas as pd
from datetime import datetime, date, timedelta
import calendar, io, json
from io import BytesIO

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from bson import ObjectId
from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from pymongo import InsertOne
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

from typing import Dict, List


def format_date_ranges(dates: List[datetime]) -> str:
    """
    Format a list of dates into ranges or individual dates.
    Continuous dates are shown as ranges (e.g., "1 Jan 2025 - 5 Jan 2025")
    Discontinuous dates are shown separated by commas.

    Args:
        dates: List of datetime objects

    Returns:
        Formatted string with date ranges
    """
    if not dates:
        return ""

    # Sort dates
    sorted_dates = sorted(dates)

    ranges = []
    start_date = sorted_dates[0]
    end_date = sorted_dates[0]

    for i in range(1, len(sorted_dates)):
        current_date = sorted_dates[i]
        # Check if current date is consecutive to end_date
        if (current_date - end_date).days == 1:
            end_date = current_date
        else:
            # Save the current range
            if start_date == end_date:
                ranges.append(start_date.strftime("%d %b %Y"))
            else:
                ranges.append(f"{start_date.strftime('%d %b %Y')} - {end_date.strftime('%d %b %Y')}")
            # Start new range
            start_date = current_date
            end_date = current_date

    # Add the last range
    if start_date == end_date:
        ranges.append(start_date.strftime("%d %b %Y"))
    else:
        ranges.append(f"{start_date.strftime('%d %b %Y')} - {end_date.strftime('%d %b %Y')}")

    return ", ".join(ranges)


async def fetch_last_n_days_in_stock_blinkit(
    db, end_datetime: datetime, sku_city_pairs: list, n_days: int = 90
) -> Dict:
    """
    Fetch the last N days an item was in stock for Blinkit inventory, regardless of when those days occurred.
    Returns formatted date ranges for each SKU/City combination.

    Args:
        db: Database connection
        end_datetime: End date to look back from
        sku_city_pairs: List of (sku_code, city) tuples
        n_days: Number of days to fetch (default 90)

    Returns:
        Dict mapping (sku_code, city) to formatted date ranges string
    """
    try:
        inventory_collection = db["blinkit_inventory"]

        # CRITICAL OPTIMIZATION: Limit lookback to prevent scanning millions of records
        # Look back maximum 1 year to find the last 90 days in stock
        lookback_limit = end_datetime - timedelta(days=365)

        logger.info(f"Fetching last {n_days} days in stock for {len(sku_city_pairs)} SKU/City pairs from {lookback_limit.date()} to {end_datetime.date()}")

        # Extract unique SKUs and cities
        skus = list(set([pair[0] for pair in sku_city_pairs]))
        cities = list(set([pair[1] for pair in sku_city_pairs]))

        # Aggregate to get the last N days where stock > 0 for each SKU/City
        pipeline = [
            {
                "$match": {
                    "sku_code": {"$in": skus},
                    "city": {"$in": cities},
                    "date": {"$gte": lookback_limit, "$lte": end_datetime},
                }
            },
            {
                "$project": {
                    "sku_code": 1,
                    "city": 1,
                    "date": 1,
                    "warehouse_inventory": 1,
                }
            },
            # Filter only days with stock > 0
            {
                "$match": {
                    "warehouse_inventory": {"$gt": 0}
                }
            },
            # Sort by SKU, city, and date descending (most recent first)
            {"$sort": {"sku_code": 1, "city": 1, "date": -1}},
            # Group by SKU and city, collect last N dates
            {
                "$group": {
                    "_id": {"sku_code": "$sku_code", "city": "$city"},
                    "stock_dates": {
                        "$push": "$date"
                    }
                }
            },
            # Limit to last N days
            {
                "$project": {
                    "_id": 1,
                    "stock_dates": {
                        "$slice": ["$stock_dates", n_days]
                    }
                }
            }
        ]

        cursor = inventory_collection.aggregate(pipeline, allowDiskUse=True, batchSize=10000)

        # Convert to dictionary with formatted date ranges
        stock_date_ranges = {}
        processed_count = 0
        for doc in cursor:
            processed_count += 1
            sku_city_key = (doc["_id"]["sku_code"], doc["_id"]["city"])
            dates = doc.get("stock_dates", [])

            # Debug logging for first few items
            if processed_count <= 3:
                logger.debug(f"DEBUG (Blinkit): SKU/City {sku_city_key} has {len(dates)} stock dates")

            if dates:
                # Format the dates into ranges
                formatted_ranges = format_date_ranges(dates)
                stock_date_ranges[sku_city_key] = formatted_ranges

                # Debug log for first item
                if processed_count == 1:
                    logger.debug(f"DEBUG (Blinkit): First item formatted ranges: {formatted_ranges[:100]}...")
            else:
                stock_date_ranges[sku_city_key] = ""

        logger.info(f"Fetched last {n_days} days in stock (Blinkit): processed {processed_count} SKU/City pairs, returned {len(stock_date_ranges)} pairs with data")

        # Debug: log sample keys
        if stock_date_ranges:
            sample_keys = list(stock_date_ranges.keys())[:3]
            logger.debug(f"DEBUG (Blinkit): Sample SKU/City pairs in result: {sample_keys}")

        return stock_date_ranges

    except Exception as e:
        logger.error(f"Error fetching last N days in stock (Blinkit): {e}", exc_info=True)
        return {}


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
RETURN_COLLECTION = "blinkit_returns"
TEMP_SALES_COLLECTION = "blinkit_sales_temp"
INVENTORY_COLLECTION = "blinkit_inventory"

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
        logger.info(f"Database error retrieving SKU mapping: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error retrieving SKU mapping: {e}",
        )
    except Exception as e:  # Catch any other unexpected errors
        logger.info(f"Unexpected error retrieving SKU mapping: {e}", exc_info=True)
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
        delete_result = sku_collection.delete_many({})
        logger.info(f"Deleted {delete_result.deleted_count} existing SKU mappings.")

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
                logger.info(f"Skipping SKU row due to missing data: {row.to_dict()}")

        if not data_to_insert:
            return {"message": "No valid SKU mapping data found to insert."}

        insert_result = sku_collection.insert_many(data_to_insert)
        logger.info(f"Inserted {len(insert_result.inserted_ids)} new SKU mappings.")

        # Create index for efficient lookup
        sku_collection.create_index([("item_id", ASCENDING)], unique=True)

        return {
            "message": f"Successfully uploaded and stored {len(insert_result.inserted_ids)} SKU mappings."
        }

    except HTTPException as e:
        raise e  # Re-raise intended HTTP exceptions
    except Exception as e:
        logger.info(f"Error uploading SKU mapping: {e}")
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
        logger.info(f"Error uploading sales data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred creating the item: {e}",
        )


@router.get("/status")
async def sync_status(start_date: str, end_date: str, db=Depends(get_database)):
    try:
        # Parse date range
        start = datetime.strptime(start_date, "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
        )

        # Initialize date range variables
        first_sales_date = None
        last_sales_date = None
        first_inventory_date = None
        last_inventory_date = None

        sales_collection = TEMP_SALES_COLLECTION
        inventory_collection = INVENTORY_COLLECTION
        # Get first and last sales dates in the date range
        sales_date_pipeline = [
            {"$match": {"order_date": {"$gte": start, "$lte": end}}},
            {
                "$group": {
                    "_id": None,
                    "first_date": {"$min": "$order_date"},
                    "last_date": {"$max": "$order_date"},
                }
            },
        ]

        sales_dates = list(db[sales_collection].aggregate(sales_date_pipeline))
        if sales_dates:
            first_sales_date = (
                sales_dates[0]["first_date"].isoformat()
                if sales_dates[0]["first_date"]
                else None
            )
            last_sales_date = (
                sales_dates[0]["last_date"].isoformat()
                if sales_dates[0]["last_date"]
                else None
            )

        # Get first and last inventory dates in the date range
        inventory_date_pipeline = [
            {"$match": {"date": {"$gte": start, "$lte": end}}},
            {
                "$group": {
                    "_id": None,
                    "first_date": {"$min": "$date"},
                    "last_date": {"$max": "$date"},
                }
            },
        ]

        inventory_dates = list(
            db[inventory_collection].aggregate(inventory_date_pipeline)
        )
        if inventory_dates:
            first_inventory_date = (
                inventory_dates[0]["first_date"].isoformat()
                if inventory_dates[0]["first_date"]
                else None
            )
            last_inventory_date = (
                inventory_dates[0]["last_date"].isoformat()
                if inventory_dates[0]["last_date"]
                else None
            )

        # Get total counts (you might want to filter these by date range too)
        sales_count = db[sales_collection].count_documents(
            {"date": {"$gte": start, "$lte": end}}
        )
        inventory_count = db[inventory_collection].count_documents(
            {"date": {"$gte": start, "$lte": end}}
        )

        # Get distinct dates for sales (using order_date field for sales)
        sales_distinct_dates = list(db[sales_collection].distinct(
            "order_date", {"order_date": {"$gte": start, "$lte": end}}
        ))
        sales_dates_set = set(d.strftime("%Y-%m-%d") if isinstance(d, datetime) else d for d in sales_distinct_dates)

        # Get distinct dates for inventory
        inventory_distinct_dates = list(db[inventory_collection].distinct(
            "date", {"date": {"$gte": start, "$lte": end}}
        ))
        inventory_dates_set = set(d.strftime("%Y-%m-%d") if isinstance(d, datetime) else d for d in inventory_distinct_dates)

        # Calculate all dates in the range
        from datetime import timedelta
        all_dates = set()
        current_date = start.replace(hour=0, minute=0, second=0, microsecond=0)
        end_check = end.replace(hour=0, minute=0, second=0, microsecond=0)
        while current_date <= end_check:
            all_dates.add(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        # Find missing dates
        missing_sales_dates = sorted(all_dates - sales_dates_set)
        missing_inventory_dates = sorted(all_dates - inventory_dates_set)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "date_range": {"start_date": start_date, "end_date": end_date},
                "sales_data": {
                    "records_count": sales_count,
                    "first_sales_date": first_sales_date,
                    "last_sales_date": last_sales_date,
                    "missing_dates": missing_sales_dates,
                },
                "inventory_data": {
                    "records_count": inventory_count,
                    "first_inventory_date": first_inventory_date,
                    "last_inventory_date": last_inventory_date,
                    "missing_dates": missing_inventory_dates,
                },
            },
        )

    except Exception as e:
        logger.info(f"Error getting sync status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


async def process_orders_file(file_content: bytes, database, config: dict) -> dict:
    """
    Common function to process order files (both sales and return orders).
    """
    try:
        # Get collections
        collection = database.get_collection(config["collection_name"])
        sku_collection = database.get_collection(SKU_COLLECTION)

        # Use thread pool for CPU-intensive pandas operations
        with ThreadPoolExecutor() as executor:
            df = await asyncio.get_event_loop().run_in_executor(
                executor,
                lambda: pd.read_excel(
                    BytesIO(file_content),
                    sheet_name=config["sheet_name"],
                    skiprows=4,
                    engine="openpyxl",
                ),
            )

        # Check if dataframe is empty
        if df.empty:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"The {config['order_type']} sheet appears to be empty or contains no data rows.",
            )

        # Rename columns to match expected format
        df = df.rename(columns=config["column_mapping"])

        # Handle dynamic date column detection for return orders
        actual_date_column = config["date_column"]
        if config["order_type"] == "Return Orders":
            # Check which date column exists in the dataframe
            possible_date_columns = ["Return Order Date", "Return Invoice Date"]
            available_date_columns = [
                col for col in possible_date_columns if col in df.columns
            ]

            if not available_date_columns:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Neither 'Return Order Date' nor 'Return Invoice Date' found in the return orders data. Available columns: {list(df.columns)}",
                )

            # Use the first available date column
            actual_date_column = available_date_columns[0]
            logger.info(
                f"Using date column: {actual_date_column} for return orders processing"
            )

        # Validate required columns (use actual_date_column instead of config date_column)
        required_cols = [
            "Item Id",
            "Quantity",
            actual_date_column,
            "Customer City",
            "HSN Code",
        ]
        missing_cols = [col for col in required_cols if col not in df.columns]

        if missing_cols:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing required columns: {', '.join(missing_cols)}. Available columns: {list(df.columns)}",
            )

        original_count = len(df)

        # Check if data is empty after initial validation
        if original_count == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No valid data rows found in {config['order_type']} sheet.",
            )

        # Clean and process the data
        df = df.dropna(subset=["Item Id", "Customer City"])
        after_cleanup_count = len(df)

        # Check if data is empty after cleanup
        if after_cleanup_count == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No valid data rows remaining after cleanup in {config['order_type']} sheet. All rows had missing Item Id or Customer City.",
            )

        # Convert data types efficiently
        df["Item Id"] = (
            pd.to_numeric(df["Item Id"], errors="coerce").fillna(0).astype(int)
        )
        df["Quantity"] = pd.to_numeric(df["Quantity"], errors="coerce").fillna(0)
        df["Customer City"] = df["Customer City"].astype(str).str.strip()
        df["Customer Name"] = (
            df["Customer Name"].astype(str).str.strip()
            if "Customer Name" in df.columns
            else "Unknown Customer"
        )
        df["Customer State"] = (
            df["Customer State"].astype(str).str.strip()
            if "Customer State" in df.columns
            else "Unknown State"
        )
        df["HSN Code"] = df["HSN Code"].astype(str).str.strip()

        # Handle supply state/city for return orders
        if "Supply City" in df.columns:
            df["Supply City"] = df["Supply City"].astype(str).str.strip()
        if "Supply State" in df.columns:
            df["Supply State"] = df["Supply State"].astype(str).str.strip()

        # Get cached SKU mapping
        sku_map_dict = await sku_cache.get_sku_mapping(sku_collection)

        # SKU mapping vectorized
        df["sku_code"] = df["Item Id"].map(
            lambda x: sku_map_dict.get(x, {}).get("sku_code", "Unknown SKU")
        )
        df["item_name_from_sku"] = df["Item Id"].map(
            lambda x: sku_map_dict.get(x, {}).get("item_name", "Unknown Item")
        )

        # Process dates efficiently (use actual_date_column)
        def safe_strptime(date_string):
            try:
                if pd.isna(date_string) or date_string == "":
                    return None

                date_string = str(date_string).strip()

                # Try multiple date formats
                formats = [
                    "%d %B %Y",  # "30 June 2025"
                    "%d %b %Y",  # "30 Jun 2025"
                    "%Y-%m-%d",  # "2025-06-30"
                    "%d/%m/%Y",  # "30/06/2025"
                    "%m/%d/%Y",  # "06/30/2025"
                    "%d-%m-%Y",  # "30-06-2025"
                ]

                for fmt in formats:
                    try:
                        return datetime.strptime(date_string, fmt)
                    except ValueError:
                        continue

                # If none work, try pandas
                parsed = pd.to_datetime(date_string, errors="coerce")
                return parsed.to_pydatetime() if pd.notna(parsed) else None

            except Exception:
                return None

        def process_dates(dates):
            return [safe_strptime(date) for date in dates]

        with ThreadPoolExecutor() as executor:
            df["processed_date"] = await asyncio.get_event_loop().run_in_executor(
                executor, process_dates, df[actual_date_column].tolist()
            )

        # Remove rows with invalid dates
        df = df.dropna(subset=["processed_date"])
        after_date_cleanup_count = len(df)

        # Check if data is empty after date processing
        if after_date_cleanup_count == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"No valid data rows remaining after date processing in {config['order_type']} sheet. All rows had invalid or missing dates in column '{actual_date_column}'.",
            )

        # City processing
        df["processed_city"] = df["Customer City"].replace("Faridabad", "Gurgaon")

        # Use Product Name if available, fallback to SKU mapping
        if "Item Name" in df.columns:
            df["final_item_name"] = df["Item Name"].fillna(df["item_name_from_sku"])
        else:
            df["final_item_name"] = df["item_name_from_sku"]

        # Batch check for existing records to prevent duplicates
        unique_keys = df[
            ["Item Id", "processed_city", "processed_date"]
        ].drop_duplicates()

        # Build query for existing records
        existing_queries = []
        for _, row in unique_keys.iterrows():
            existing_queries.append(
                {
                    "item_id": int(row["Item Id"]),
                    "city": row["processed_city"],
                    config["date_field_name"]: row["processed_date"],
                }
            )

        # Batch query existing records
        existing_docs = set()
        if existing_queries:
            cursor = collection.find(
                {"$or": existing_queries},
                {"item_id": 1, "city": 1, config["date_field_name"]: 1},
            )
            for doc in cursor:
                key = (doc["item_id"], doc["city"], doc[config["date_field_name"]])
                existing_docs.add(key)

        # Filter out existing records
        def is_new_record(row):
            key = (int(row["Item Id"]), row["processed_city"], row["processed_date"])
            return key not in existing_docs

        df["is_new"] = df.apply(is_new_record, axis=1)
        new_records_df = df[df["is_new"]]

        # Prepare bulk insert operations
        bulk_operations = []
        processed_records = []

        for _, row in new_records_df.iterrows():
            # Document for database insertion
            doc = {
                "item_id": int(row["Item Id"]),
                "sku_code": row["sku_code"],
                "hsn_code": str(row["HSN Code"]),
                "item_name": row["final_item_name"],
                "quantity": float(row["Quantity"]),
                "customer_name": (
                    str(row["Customer Name"])
                    if pd.notna(row.get("Customer Name"))
                    else "Unknown Customer"
                ),
                "customer_state": (
                    str(row["Customer State"])
                    if pd.notna(row.get("Customer State"))
                    else "Unknown State"
                ),
                "city": row["processed_city"],
                config["date_field_name"]: row[
                    "processed_date"
                ],  # This will be stored as datetime
                "created_at": config["created_at_func"](),
            }

            # Add the actual date column used for processing (for return orders tracking)
            if config["order_type"] == "Return Orders":
                doc["date_column_used"] = actual_date_column

            # Add optional fields if they exist and are not null
            optional_fields = {
                "Invoice ID": "invoice_id",
                "Order ID": "order_id",
                "Order Status": "order_status",
                "Selling Price": "selling_price",
                "MRP": "mrp",
            }

            for excel_col, doc_field in optional_fields.items():
                if excel_col in row and pd.notna(row[excel_col]):
                    doc[doc_field] = row[excel_col]

            bulk_operations.append(InsertOne(doc))

            # Record for response (with ISO format date for JSON serialization)
            response_record = doc.copy()
            response_record[config["date_field_name"]] = (
                row["processed_date"].isoformat() if row["processed_date"] else None
            )
            response_record[f"original_{config['date_field_name']}"] = str(
                row[actual_date_column]
            )
            if "created_at" in response_record:
                response_record["created_at"] = response_record[
                    "created_at"
                ].isoformat()

            processed_records.append(response_record)

        # Execute bulk insert
        inserted_count = 0
        if bulk_operations:
            # Process in batches of 1000 to avoid memory issues
            batch_size = 1000
            for i in range(0, len(bulk_operations), batch_size):
                batch = bulk_operations[i : i + batch_size]
                result = collection.bulk_write(batch, ordered=False)
                inserted_count += result.inserted_count

        # Create indexes if not exist
        try:
            collection.create_index([(config["date_field_name"], 1)], background=True)
            collection.create_index(
                [("sku_code", 1), ("city", 1), (config["date_field_name"], 1)],
                background=True,
            )
            collection.create_index(
                [("item_id", 1), ("city", 1), (config["date_field_name"], 1)],
                background=True,
            )
            collection.create_index([("customer_state", 1)], background=True)
        except Exception:
            pass  # Indexes might already exist

        # Generate statistics from all processed data
        city_stats = df["processed_city"].value_counts().head(10).to_dict()
        customer_stats = df["Customer Name"].value_counts().head(10).to_dict()
        state_stats = df["Customer State"].value_counts().head(10).to_dict()
        sku_stats = df["sku_code"].value_counts().head(10).to_dict()
        date_range = {
            "earliest": (
                df["processed_date"].min().isoformat() if not df.empty else None
            ),
            "latest": df["processed_date"].max().isoformat() if not df.empty else None,
        }

        skipped_count = len(df) - len(new_records_df)
        warning_count = len(df[df["sku_code"] == "Unknown SKU"])

        return {
            "processing_stats": {
                "original_rows": original_count,
                "after_cleanup": after_cleanup_count,
                "after_date_processing": after_date_cleanup_count,
                "final_processed": len(df),
                "new_records": len(new_records_df),
                "inserted_count": inserted_count,
                "skipped_duplicates": skipped_count,
                "warning_count": warning_count,
                "dropped_rows": original_count - len(df),
                "date_column_used": (
                    actual_date_column
                    if config["order_type"] == "Return Orders"
                    else config["date_column"]
                ),
            },
            "data_overview": {
                "date_range": date_range,
                "cities": city_stats,
                "customers": customer_stats,
                "states": state_stats,
                "sku_codes": sku_stats,
                "total_quantity": float(df["Quantity"].sum()),
                "unique_items": int(df["Item Id"].nunique()),
                "unique_customers": int(df["Customer Name"].nunique()),
                "unique_cities": int(df["processed_city"].nunique()),
                "unique_states": int(df["Customer State"].nunique()),
            },
            "sample_inserted_records": processed_records[:5],
            "message": f"Successfully processed {config['order_type']} data using date column '{actual_date_column}'. Inserted {inserted_count} new records, skipped {skipped_count} duplicates. Encountered {warning_count} unknown SKU warnings.",
        }

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error processing {config['order_type']} data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the {config['order_type']} file: {e}",
        )


def get_return_orders_config():
    """Configuration for return orders processing."""
    return {
        "collection_name": RETURN_COLLECTION,
        "sheet_name": "Cancelled or Returned Orders",
        "date_column": None,  # Will be dynamically determined
        "date_field_name": "return_date",
        "order_type": "Return Orders",
        "created_at_func": datetime.now,
        "column_mapping": {
            "Forward Invoice ID": "Forward Invoice Id",
            "Forward Invoice Date": "Forward Invoice Date",
            "Return Invoice ID": "Return Invoice Id",
            "Return Order ID": "Return Order Id",
            "Return Order Date": "Return Order Date",
            "Return Invoice Date": "Return Invoice Date",
            "Customer Name": "Customer Name",
            "Supply City": "Supply City",
            "Customer City": "Customer City",
            "Supply State": "Supply State",
            "Customer State": "Customer State",
            "Quantity": "Quantity",
            "HSN Code": "HSN Code",
            "Product Name": "Item Name",
            "Item ID": "Item Id",
            "Order ID": "Order ID",
            "Order Status": "Order Status",
            "Selling Price (Rs)": "Selling Price",
            "MRP (Rs)": "MRP",
        },
    }


def get_sales_orders_config():
    """Configuration for sales orders processing."""
    return {
        "collection_name": TEMP_SALES_COLLECTION,
        "sheet_name": "Forward Orders",
        "date_column": "Order Date",
        "date_field_name": "order_date",
        "order_type": "Forward Orders",
        "created_at_func": datetime.now,
        "column_mapping": {
            "Item ID": "Item Id",
            "Quantity": "Quantity",
            "Order Date": "Order Date",
            "Customer City": "Customer City",
            "Customer Name": "Customer Name",
            "Customer State": "Customer State",
            "HSN Code": "HSN Code",
            "Product Name": "Item Name",
            "Invoice ID": "Invoice ID",
            "Order ID": "Order ID",
            "Order Status": "Order Status",
            "Selling Price (Rs)": "Selling Price",
            "MRP (Rs)": "MRP",
        },
    }


@router.post("/upload_return_data")
async def upload_return_orders(
    file: UploadFile = File(...), database=Depends(get_database)
):
    """Upload Return Orders Excel data and save to database."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    file_content = await file.read()
    config = get_return_orders_config()

    return await process_orders_file(file_content, database, config)


@router.post("/upload_sales_data")
async def upload_sales_orders(
    file: UploadFile = File(...), database=Depends(get_database)
):
    """Upload Forward Orders Excel data and save to database."""
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    file_content = await file.read()
    config = get_sales_orders_config()

    return await process_orders_file(file_content, database, config)


@router.post("/upload_inventory_data")
async def upload_inventory_data(
    file: UploadFile = File(...), database=Depends(get_database)
):
    """
    Optimized inventory data upload with city-level aggregation.
    Supports both OLD and NEW Blinkit inventory file formats.

    Supported Formats:

    1. OLD FORMAT (Legacy):
       - Standard Excel file with columns: Item ID, Backend Outlet, Date, Qty
       - Header row is the first row
       - Date column already present

    2. NEW FORMAT (Current - auto-detected):
       - First row contains timestamp: "This sheet was generated at YYYY-MM-DD HH:MM:SS"
       - Second row contains section headers (Product details, Warehouse details, etc.)
       - Third row contains column headers
       - Key columns: Item ID, Warehouse Facility Name (→ Backend Outlet),
                      Total sellable (→ Qty), Item Name
       - Date is automatically extracted from the timestamp

    The function automatically detects which format is being used and processes accordingly.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    try:
        import re

        file_content = await file.read()

        # Detect file format by checking the first row
        def detect_and_parse_format(file_content):
            # Read first row to detect format
            df_check = pd.read_excel(BytesIO(file_content), engine="openpyxl", header=None, nrows=1)
            first_cell = str(df_check.iloc[0, 0])

            # New format detection: first cell contains timestamp
            if "This sheet was generated at" in first_cell:
                logger.info("Detected NEW inventory format (with timestamp in first cell)")

                # Extract date from timestamp using regex
                match = re.search(r'(\d{4}-\d{2}-\d{2})', first_cell)
                if match:
                    extracted_date = match.group(1)
                    logger.info(f"Successfully extracted date from timestamp: {extracted_date}")
                else:
                    # Fallback to today's date if extraction fails
                    extracted_date = datetime.now().strftime("%Y-%m-%d")
                    logger.warning(f"Could not extract date from timestamp '{first_cell}', using today's date: {extracted_date}")

                # Read actual data (skip first 2 rows: row 0 = timestamp, row 1 = section headers, row 2 = column headers)
                df = pd.read_excel(BytesIO(file_content), engine="openpyxl", skiprows=2)

                logger.info(f"New format columns found: {list(df.columns)[:10]}...")

                # Map new format columns to old format column names
                if "Warehouse Facility Name" in df.columns:
                    df["Backend Outlet"] = df["Warehouse Facility Name"]
                    logger.info("Mapped 'Warehouse Facility Name' → 'Backend Outlet'")
                else:
                    logger.warning("'Warehouse Facility Name' column not found in new format")

                if "Total sellable" in df.columns:
                    df["Qty"] = df["Total sellable"]
                    logger.info("Mapped 'Total sellable' → 'Qty'")
                else:
                    logger.warning("'Total sellable' column not found in new format")

                # Also map Item Name if present
                if "Item Name" in df.columns and "Item Name" not in df.columns:
                    logger.info("Item Name column found in new format")

                # Add Date column with extracted date
                df["Date"] = extracted_date
                logger.info(f"Added 'Date' column with value: {extracted_date}")

                return df
            else:
                logger.info("Detected OLD inventory format (standard Excel format)")
                # Old format: read normally
                df = pd.read_excel(BytesIO(file_content), engine="openpyxl")
                logger.info(f"Old format columns found: {list(df.columns)}")
                return df

        with ThreadPoolExecutor() as executor:
            df = await asyncio.get_event_loop().run_in_executor(
                executor,
                detect_and_parse_format,
                file_content
            )

        # Validate required columns
        required_cols = ["Item ID", "Backend Outlet", "Date", "Qty"]
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Missing required columns: {', '.join(missing)}. Available columns: {', '.join(df.columns)}",
            )

        inventory_collection = database.get_collection(INVENTORY_COLLECTION)
        sku_collection = database.get_collection(SKU_COLLECTION)

        # Get cached SKU mapping
        sku_map_dict = await sku_cache.get_sku_mapping(sku_collection)
        logger.info(f"Retrieved SKU mapping with {len(sku_map_dict)} entries")
        logger.info(json.dumps(serialize_mongo_document(sku_map_dict), indent=4))
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
                logger.info(f"Invalid Item ID for SKU mapping: {item_id}")
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

        logger.info(f"Proceeding with {len(df_with_valid_sku)} records with valid SKUs")
        logger.info(f"Excluding {len(df_with_unknown_sku)} records with Unknown SKUs")

        if len(df_with_unknown_sku) > 0:
            logger.info("Records with Unknown SKU (will be excluded):")
            for _, row in df_with_unknown_sku.head(5).iterrows():
                logger.info(
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

        logger.info(f"After city aggregation: {len(aggregated_df)} records")

        # Verify no Unknown SKUs in final data
        final_unknown_count = len(
            aggregated_df[aggregated_df["sku_code"] == "Unknown SKU"]
        )
        if final_unknown_count > 0:
            logger.info(
                f"WARNING: {final_unknown_count} Unknown SKU records still present after filtering!"
            )
        else:
            logger.info("✓ No Unknown SKU records in final aggregated data")

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

        logger.info(
            f"Cleaning up old records for {len(dates_to_process)} dates, {len(skus_to_process)} SKUs, {len(cities_to_process)} cities..."
        )

        delete_filter = {
            "date": {"$in": dates_to_process},
            "sku_code": {"$in": skus_to_process},
            "city": {"$in": cities_to_process},
        }

        delete_result = inventory_collection.delete_many(delete_filter)
        logger.info(f"Deleted {delete_result.deleted_count} old inventory records")

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
        logger.info(f"Error uploading inventory data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the file: {e}",
        )


# --- Report Generation Route ---


def calculate_number_of_months(start_year, start_month, end_year, end_month):
    """Calculates the total number of months in a given range, inclusive."""
    return (end_year - start_year) * 12 + (end_month - start_month) + 1


@router.get("/generate_report_by_date_range")
async def generate_report_by_date_range(
    start_date: str,  # Format: YYYY-MM-DD
    end_date: str,  # Format: YYYY-MM-DD
    any_last_90_days: bool = False,  # Include last 90 days in stock with date ranges
    database=Depends(get_database),
):
    """
    Generates the Sales vs Inventory report dynamically based on the specified date range.
    Accepts any date range (start_date to end_date) instead of month ranges.
    Now includes returns data for the specified date range.

    When any_last_90_days=True, includes a new field 'last_90_days_dates' with formatted date ranges
    showing the last 90 days the item was in stock, regardless of when those days occurred.
    """
    try:
        # Parse and validate dates
        try:
            start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
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

        sales_collection = database.get_collection(TEMP_SALES_COLLECTION)
        inventory_collection = database.get_collection(INVENTORY_COLLECTION)
        returns_collection = database.get_collection(RETURN_COLLECTION)

        logger.info(
            f"Querying sales, inventory, and returns data for {period_name} to generate dynamic report..."
        )

        # Run all four DB queries in parallel using threads
        def _fetch_sales():
            return list(sales_collection.find(
                {"order_date": {"$gte": overall_start_date, "$lte": overall_end_date}}
            ))

        def _fetch_returns():
            return list(returns_collection.find({
                "return_date": {
                    "$gte": overall_start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                    "$lte": overall_end_date.replace(hour=0, minute=0, second=0, microsecond=0),
                }
            }))

        def _fetch_lifetime_monthly():
            lifetime_agg_pipeline = [
                {"$match": {"sku_code": {"$exists": True, "$ne": None}, "city": {"$exists": True, "$ne": None}, "order_date": {"$exists": True}}},
                {"$project": {"sku_code": 1, "city": 1, "quantity": 1, "order_date": 1, "item_name": 1, "item_id": 1, "_id": 0}},
                {"$group": {
                    "_id": {
                        "sku": "$sku_code",
                        "city": "$city",
                        "month": {"$dateToString": {"format": "%Y-%m", "date": "$order_date"}}
                    },
                    "total_quantity": {"$sum": {"$toDouble": {"$ifNull": ["$quantity", 0]}}},
                    "item_name": {"$first": "$item_name"},
                    "item_id": {"$first": "$item_id"},
                }}
            ]
            result = {}
            item_info = {}
            for doc in sales_collection.aggregate(lifetime_agg_pipeline, allowDiskUse=True):
                sku = doc["_id"]["sku"]
                city = doc["_id"]["city"]
                month_key = doc["_id"]["month"]
                sku_city_key = (sku, city)
                if sku_city_key not in result:
                    result[sku_city_key] = {}
                result[sku_city_key][month_key] = doc["total_quantity"]
                if sku not in item_info:
                    item_info[sku] = {"item_name": doc.get("item_name", "Unknown Item"), "item_id": doc.get("item_id", "Unknown ID")}
            return result, item_info

        def _fetch_inventory():
            return list(inventory_collection.find({
                "date": {
                    "$gte": overall_start_date.replace(hour=0, minute=0, second=0, microsecond=0),
                    "$lte": overall_end_date.replace(hour=0, minute=0, second=0, microsecond=0),
                }
            }))

        logger.info("Running sales, returns, lifetime aggregation, and inventory queries in parallel...")
        sales_data, returns_data, (lifetime_monthly_sales, lifetime_item_info), inventory_data = await asyncio.gather(
            asyncio.to_thread(_fetch_sales),
            asyncio.to_thread(_fetch_returns),
            asyncio.to_thread(_fetch_lifetime_monthly),
            asyncio.to_thread(_fetch_inventory),
        )
        logger.info(
            f"Found {len(sales_data)} sales, {len(returns_data)} returns, {len(inventory_data)} inventory records. "
            f"Aggregated lifetime monthly sales for {len(lifetime_monthly_sales)} SKU/city pairs."
        )

        if not sales_data and not inventory_data and not returns_data:
            return {
                "message": f"No sales, inventory, or returns data found for {period_name}. No report generated.",
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
        # Merge lifetime item info into sales_item_info
        sales_item_info.update(lifetime_item_info)
        all_sales_dates = set()

        # --- Process Returns Data for Current Period ---
        current_period_returns = {}
        returns_item_info = {}

        # lifetime_monthly_sales already populated by aggregation pipeline above

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

        # Process current period returns data
        logger.info("Processing returns data for the current period...")
        for record in returns_data:
            sku = record.get("sku_code")
            city = record.get("city")
            return_date_dt = record.get("return_date")
            quantity = record.get("quantity", 0)
            item_name = record.get("item_name", "Unknown Item")
            item_id = record.get("item_id", "Unknown ID")

            if not sku or not city or not return_date_dt:
                continue
            quantity = float(quantity) if isinstance(quantity, (int, float)) else 0

            if sku not in returns_item_info:
                returns_item_info[sku] = {"item_name": item_name, "item_id": item_id}

            # Aggregate returns by sku_code and city for the entire period
            sku_city_key = (sku, city)
            current_period_returns[sku_city_key] = (
                current_period_returns.get(sku_city_key, 0) + quantity
            )

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

        logger.info(
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

            logger.info(f"Last common date in period: {last_common_date_str}")

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
            logger.info(
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

        # Add SKU/City combinations from returns data
        for sku_city_key in current_period_returns.keys():
            valid_sku_city_pairs.add(sku_city_key)

        logger.info(
            f"Calculating metrics for {len(valid_sku_city_pairs)} unique SKU/City combinations for period {period_name}..."
        )

        # Fetch last 90 days in stock if requested
        last_90_days_data = {}
        if any_last_90_days and valid_sku_city_pairs:
            sku_city_list = list(valid_sku_city_pairs)
            logger.info(f"Fetching last 90 days in stock for {len(sku_city_list)} SKU/City pairs")
            logger.debug(f"DEBUG (Blinkit): Sample SKU/City pairs: {sku_city_list[:3]}")

            last_90_days_data = await fetch_last_n_days_in_stock_blinkit(database, overall_end_date, sku_city_list, 90)
            logger.debug(f"DEBUG (Blinkit): Received {len(last_90_days_data)} SKU/City pairs from fetch_last_n_days_in_stock_blinkit")

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

        # Precompute most recent inventory per SKU/city (avoids O(n*m) reversed loop per item)
        most_recent_inventory_map = {}
        if common_dates:
            for sku, city in valid_sku_city_pairs:
                for date_str in reversed(common_dates):
                    inv_info = inventory_map.get((sku, city, date_str))
                    if inv_info:
                        most_recent_inventory_map[(sku, city)] = inv_info["inventory"]
                        break

        # Process each SKU/City combination
        for sku, city in valid_sku_city_pairs:
            # Determine item info from available sources
            item_info = inventory_item_info.get(
                sku,
                sales_item_info.get(
                    sku,
                    returns_item_info.get(
                        sku, {"item_name": "Unknown Item", "item_id": "Unknown ID"}
                    ),
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
                    # Use precomputed most recent inventory per SKU/city
                    last_day_inventory = most_recent_inventory_map.get((sku, city), 0)

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

            # Get returns data for this SKU/City combination
            total_returns_in_period = current_period_returns.get(sku_city_key, 0)

            # Get last 90 days in stock dates if available
            last_90_days_dates = last_90_days_data.get(sku_city_key, "")

            # Create report item
            report_item = {
                "item_name": item_name,
                "item_id": item_id,
                "city": city,
                "warehouse": combined_warehouses,
                "sku_code": sku,
                "last_90_days_dates": last_90_days_dates,
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
                    "total_returns_in_period": round(total_returns_in_period, 2),
                    "net_sales_in_period": round(
                        total_sales_on_common_days - total_returns_in_period, 2
                    ),
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

        logger.info(
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
                "total_returns_records": len(returns_data),
            },
        }

    except PyMongoError as e:
        logger.info(f"Database error during report generation for {period_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error for {period_name}: {e}",
        )
    except Exception as e:
        # logger.info(f"Error generating report for {period_name}: {e}")
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
    any_last_90_days: bool = False,  # Include last 90 days in stock with date ranges
    database=Depends(get_database),
):
    """
    Downloads report for a specific date range as Excel file.
    Now includes returns data in the Excel export.

    When any_last_90_days=True, includes a new column 'Last 90 Days In Stock (Dates)' with formatted date ranges.
    """
    try:
        # Generate report data dynamically
        report_response = await generate_report_by_date_range(
            start_date=start_date,
            end_date=end_date,
            any_last_90_days=any_last_90_days,
            database=database,
        )

        report_data_list = report_response.get("data", [])
        period_info = report_response.get("period_info", {})

        if not report_data_list:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No report data found for {period_info.get('period_name', 'the specified period')} to download.",
            )

        # Generate Excel file with returns data included
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
                "Total Returns (Period)": metrics.get("total_returns_in_period", 0),
                "Net Sales (Period)": metrics.get("net_sales_in_period", 0),
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

            # Add Last 90 Days In Stock column if requested
            if any_last_90_days:
                flat_item["Last 90 Days In Stock (Dates)"] = item.get("last_90_days_dates", "")

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
            "Total Returns (Period)",
            "Net Sales (Period)",
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

        # Add Last 90 Days column to column order if requested
        if any_last_90_days:
            column_order.append("Last 90 Days In Stock (Dates)")

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
        logger.info(f"Error generating report Excel file: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An internal error occurred while generating the Excel file: {e}",
        )
        
@router.get("/get_report_data_by_date_range")
async def get_report_data_by_date_range(
    start_date: str,  # Format: YYYY-MM-DD
    end_date: str,  # Format: YYYY-MM-DD
    any_last_90_days: bool = False,  # Include last 90 days in stock with date ranges
    database=Depends(get_database),
):
    """
    Returns report data for a specific date range for frontend consumption.

    When any_last_90_days=True, includes a new field 'last_90_days_dates' with formatted date ranges.
    """
    try:
        report_response = await generate_report_by_date_range(
            start_date=start_date,
            end_date=end_date,
            any_last_90_days=any_last_90_days,
            database=database,
        )

        logger.info(
            f"Retrieved dynamic report data for {report_response.get('period_info', {}).get('period_name', 'specified period')}."
        )
        return report_response

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.info(f"Error retrieving report data: {e}")
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
        logger.info(f"Error uploading sales data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the file: {e}",
        )
