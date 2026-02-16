import pandas as pd
from datetime import datetime, timedelta
import io, logging, math, json, re
from fastapi import (
    APIRouter,
    File,
    HTTPException,
    status,
    Depends,
    Query,
    BackgroundTasks,
)
import threading
from fastapi.responses import JSONResponse, StreamingResponse
import concurrent.futures
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document
from pydantic import BaseModel, validator
from typing import List

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Request model
class ReportRequest(BaseModel):
    start_date: str
    end_date: str
    brand: str
    exclude_customers: bool

    @validator("start_date", "end_date")
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")

    @validator("end_date")
    def validate_date_range(cls, v, values):
        if "start_date" in values:
            start = datetime.strptime(values["start_date"], "%Y-%m-%d")
            end = datetime.strptime(v, "%Y-%m-%d")
            if start > end:
                raise ValueError("End date must be after start date")
        return v


# --- Configuration ---
# Use environment variables for production

PRODUCTS_COLLECTION = "products"
INVOICES_COLLECTION = "invoices"
PURCHASE_ORDER_COLLECTION = "purchase_orders"

# Standardized excluded customers list - used across multiple endpoints
EXCLUDED_CUSTOMERS_LIST = [
    # "(amzb2b) Pupscribe Enterprises Pvt Ltd",
    # "Pupscribe Enterprises Private Limited",
    # "(OSAMP) Office samples",
    # "(PUPEV) PUPSCRIBE EVENTS",
    # "(SSAM) Sales samples",
    # "(RS) Retail samples",
    # "Pupscribe Enterprises Private Limited (Blinkit Haryana)",
    # "Pupscribe Enterprises Private Limited (Blinkit Karnataka)",
]

router = APIRouter()


@router.get("/excluded-customers")
async def get_excluded_customers():
    """
    Get the standardized list of excluded customers.
    This list is used across multiple endpoints for filtering out internal/sample customers.
    """
    return {
        "excluded_customers": EXCLUDED_CUSTOMERS_LIST,
        "count": len(EXCLUDED_CUSTOMERS_LIST)
    }


@router.get("/products")
def get_products(
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    limit: int = Query(
        10, ge=1, le=100, description="Number of items per page (max 100)"
    ),
    search: str = Query(None, description="Search term for product name or SKU"),
    category: str = Query(None, description="Filter by category"),
    status: str = Query(None, description="Filter by status (active/inactive)"),
    sort_by: str = Query(
        "name", description="Sort by field (name, price, stock, created_date)"
    ),
    sort_order: str = Query(
        "asc", regex="^(asc|desc)$", description="Sort order (asc/desc)"
    ),
):
    """
    Get products with pagination, search, filtering, and sorting capabilities.

    Args:
        page: Page number (1-based)
        limit: Items per page (1-100)
        search: Search in product name, SKU, or description
        category: Filter by category name
        status: Filter by active/inactive status
        sort_by: Field to sort by
        sort_order: Sort order (asc/desc)

    Returns:
        JSON response with products, pagination info, and metadata
    """
    try:
        db = get_database()
        collection = db[PRODUCTS_COLLECTION]

        # Build query filter
        query_filter = {}

        # Search functionality
        if search:
            search_regex = {"$regex": search, "$options": "i"}
            query_filter["$or"] = [
                {"name": search_regex},
                {"sku": search_regex},
                {"item_id": search_regex},
                {"description": search_regex},
            ]

        # Category filter
        if category:
            query_filter["$or"] = query_filter.get("$or", []) + [
                {"category_name": {"$regex": category, "$options": "i"}},
                {"item_type": {"$regex": category, "$options": "i"}},
            ]

        # Status filter
        if status:
            if status.lower() == "active":
                query_filter["$or"] = query_filter.get("$or", []) + [
                    {"status": "active"},
                    {"is_active": True},
                ]
            elif status.lower() == "inactive":
                query_filter["$or"] = query_filter.get("$or", []) + [
                    {"status": {"$ne": "active"}},
                    {"is_active": {"$ne": True}},
                ]

        # Sort configuration
        sort_field_mapping = {
            "name": "name",
            "price": "rate",
            "stock": "stock_on_hand",
            "created_date": "created_time",
        }

        actual_sort_field = sort_field_mapping.get(sort_by, "name")
        sort_direction = 1 if sort_order.lower() == "asc" else -1

        # Calculate pagination
        skip = (page - 1) * limit

        # Get total count for pagination
        total_count = collection.count_documents(query_filter)

        # Calculate total pages
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1

        # Get products with pagination
        cursor = (
            collection.find(query_filter)
            .sort(actual_sort_field, sort_direction)
            .skip(skip)
            .limit(limit)
        )
        products = list(cursor)

        # Serialize MongoDB documents
        serialized_products = serialize_mongo_document(products)

        # Prepare response
        response_data = {
            "products": serialized_products,
            "pagination": {
                "currentPage": page,
                "totalPages": total_pages,
                "totalProducts": total_count,
                "limit": limit,
                "hasNextPage": page < total_pages,
                "hasPrevPage": page > 1,
                "nextPage": page + 1 if page < total_pages else None,
                "prevPage": page - 1 if page > 1 else None,
            },
            "filters": {
                "search": search,
                "category": category,
                "status": status,
                "sort_by": sort_by,
                "sort_order": sort_order,
            },
            "meta": {
                "timestamp": datetime.now().isoformat(),
                "resultsOnPage": len(serialized_products),
            },
        }

        return JSONResponse(content=response_data)

    except PyMongoError as e:
        logger.info(f"MongoDB Error Getting Products: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error occurred while fetching products: {str(e)}",
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.info(f"Error Getting Products: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while getting the products: {str(e)}",
        )


@router.get("/products/summary")
def get_products_summary():
    """
    Get a summary of products including total count, active/inactive counts, and categories.

    Returns:
        JSON response with product summary statistics
    """
    try:
        db = get_database()
        collection = db[PRODUCTS_COLLECTION]

        # Get total count
        total_count = collection.count_documents({})

        # Get active/inactive counts
        active_count = collection.count_documents({"status": "active"})

        inactive_count = total_count - active_count

        # Get categories
        categories_pipeline = [
            {
                "$group": {
                    "_id": {
                        "$ifNull": [
                            "$category",
                            {"$ifNull": ["$item_type", "Uncategorized"]},
                        ]
                    },
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"count": -1}},
        ]

        # Get stock status
        low_stock_count = collection.count_documents({"stock": {"$lt": 10}})

        out_of_stock_count = collection.count_documents({"stock": 0})

        summary_data = {
            "totalProducts": total_count,
            "activeProducts": active_count,
            "inactiveProducts": inactive_count,
            "lowStockProducts": low_stock_count,
            "outOfStockProducts": out_of_stock_count,
            "timestamp": datetime.now().isoformat(),
        }

        return JSONResponse(content=summary_data)

    except Exception as e:
        logger.info(f"Error Getting Products Summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred getting the products summary: {str(e)}",
        )


@router.get("/sales")
def get_sales(
    page: int = Query(1, ge=1, description="Page number (starts from 1)"),
    limit: int = Query(
        10, ge=1, le=100, description="Number of items per page (max 100)"
    ),
    search: str = Query(
        None, description="Search term for invoice number or customer name"
    ),
    status: str = Query(
        None, description="Filter by status (paid, pending, overdue, etc.)"
    ),
    date_from: str = Query(None, description="Start date filter (YYYY-MM-DD)"),
    date_to: str = Query(None, description="End date filter (YYYY-MM-DD)"),
    sort_by: str = Query(
        "invoice_date",
        description="Sort by field (invoice_date, total, customer_name, status)",
    ),
    sort_order: str = Query(
        "desc", regex="^(asc|desc)$", description="Sort order (asc/desc)"
    ),
):
    """
    Get sales/invoices with pagination, search, filtering, and sorting capabilities.

    Args:
        page: Page number (1-based)
        limit: Items per page (1-100)
        search: Search in invoice number or customer name
        status: Filter by invoice status
        date_from: Filter invoices from this date
        date_to: Filter invoices up to this date
        sort_by: Field to sort by
        sort_order: Sort order (asc/desc)

    Returns:
        JSON response with sales data, pagination info, and metadata
    """
    try:
        db = get_database()
        collection = db[INVOICES_COLLECTION]

        # Build query filter
        query_filter = {}

        # Search functionality - search in invoice number and customer name
        if search:
            search_regex = {"$regex": search, "$options": "i"}
            query_filter["$or"] = [
                {"invoice_number": search_regex},
                {"number": search_regex},
                {"customer_name": search_regex},
                {"customer.display_name": search_regex},
                {"customer.name": search_regex},
            ]

        # Status filter
        if status:
            query_filter["status"] = {"$regex": status, "$options": "i"}

        # Date range filter
        if date_from or date_to:
            date_filter = {}
            if date_from:
                try:
                    from_date = datetime.strptime(date_from, "%Y-%m-%d")
                    date_filter["$gte"] = from_date
                except ValueError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Invalid date_from format. Use YYYY-MM-DD",
                    )

            if date_to:
                try:
                    to_date = datetime.strptime(date_to, "%Y-%m-%d")
                    # Add 1 day and subtract 1 second to include the entire end date
                    to_date = to_date + timedelta(days=1) - timedelta(seconds=1)
                    date_filter["$lt"] = to_date
                except ValueError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Invalid date_to format. Use YYYY-MM-DD",
                    )

            # Apply date filter to possible date fields
            if date_filter:
                query_filter["$or"] = query_filter.get("$or", []) + [
                    {"invoice_date": date_filter},
                    {"date": date_filter},
                    {"created_time": date_filter},
                ]

        # Sort configuration
        sort_field_mapping = {
            "invoice_date": "invoice_date",
            "date": "date",
            "total": "total",
            "amount": "total",
            "customer_name": "customer_name",
            "status": "status",
            "created_date": "created_time",
        }

        actual_sort_field = sort_field_mapping.get(sort_by, "invoice_date")
        sort_direction = 1 if sort_order.lower() == "asc" else -1

        # Calculate pagination
        skip = (page - 1) * limit

        # Get total count for pagination
        total_count = collection.count_documents(query_filter)

        # Calculate total pages
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1

        # Get sales with pagination
        cursor = (
            collection.find(query_filter)
            .sort(actual_sort_field, sort_direction)
            .skip(skip)
            .limit(limit)
        )
        sales = list(cursor)

        # Process sales data to ensure consistent field names
        processed_sales = []
        for sale in sales:
            processed_sale = sale.copy()

            # Normalize customer name
            if not processed_sale.get("customer_name"):
                if processed_sale.get("customer", {}).get("display_name"):
                    processed_sale["customer_name"] = processed_sale["customer"][
                        "display_name"
                    ]
                elif processed_sale.get("customer", {}).get("name"):
                    processed_sale["customer_name"] = processed_sale["customer"]["name"]

            # Normalize customer email
            if not processed_sale.get("customer_email"):
                if processed_sale.get("customer", {}).get("email"):
                    processed_sale["customer_email"] = processed_sale["customer"][
                        "email"
                    ]

            # Normalize date fields
            if not processed_sale.get("invoice_date") and processed_sale.get("date"):
                processed_sale["invoice_date"] = processed_sale["date"]

            # Normalize amount/total
            if not processed_sale.get("total") and processed_sale.get("amount"):
                processed_sale["total"] = processed_sale["amount"]

            # Calculate days overdue if applicable
            if processed_sale.get("due_date") and processed_sale.get(
                "status", ""
            ).lower() not in ["paid", "sent"]:
                try:
                    due_date = processed_sale["due_date"]
                    if isinstance(due_date, str):
                        due_date = datetime.strptime(due_date.split("T")[0], "%Y-%m-%d")

                    days_diff = (datetime.now() - due_date).days
                    if days_diff > 0:
                        processed_sale["days_overdue"] = days_diff
                    else:
                        processed_sale["days_overdue"] = 0
                except:
                    processed_sale["days_overdue"] = 0
            else:
                processed_sale["days_overdue"] = 0

            processed_sales.append(processed_sale)

        # Serialize MongoDB documents
        serialized_sales = serialize_mongo_document(processed_sales)

        # Calculate summary statistics
        total_amount = sum(sale.get("total", 0) for sale in processed_sales)
        paid_count = len(
            [
                sale
                for sale in processed_sales
                if sale.get("status", "").lower() == "paid"
            ]
        )
        pending_count = len(
            [
                sale
                for sale in processed_sales
                if sale.get("status", "").lower() == "pending"
            ]
        )
        overdue_count = len(
            [sale for sale in processed_sales if sale.get("days_overdue", 0) > 0]
        )

        # Prepare response
        response_data = {
            "sales": serialized_sales,
            "pagination": {
                "currentPage": page,
                "totalPages": total_pages,
                "totalSales": total_count,
                "limit": limit,
                "hasNextPage": page < total_pages,
                "hasPrevPage": page > 1,
                "nextPage": page + 1 if page < total_pages else None,
                "prevPage": page - 1 if page > 1 else None,
            },
            "filters": {
                "search": search,
                "status": status,
                "date_from": date_from,
                "date_to": date_to,
                "sort_by": sort_by,
                "sort_order": sort_order,
            },
            "summary": {
                "totalAmount": total_amount,
                "paidCount": paid_count,
                "pendingCount": pending_count,
                "overdueCount": overdue_count,
                "currentPageAmount": sum(
                    sale.get("total", 0) for sale in serialized_sales
                ),
            },
            "meta": {
                "timestamp": datetime.now().isoformat(),
                "resultsOnPage": len(serialized_sales),
            },
        }

        return JSONResponse(content=response_data)

    except PyMongoError as e:
        logger.info(f"MongoDB Error Getting Sales: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error occurred while fetching sales: {str(e)}",
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.info(f"Error Getting Sales: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while getting the sales: {str(e)}",
        )


_item_name_cache = {}
_cache_ttl = 300  # 5 minutes
_cache_lock = threading.Lock()


def get_item_names_by_brand(db, brand: str) -> List[str]:
    """
    Cached version of item name lookup with TTL for better performance.
    """
    current_time = datetime.now().timestamp()

    with _cache_lock:
        if brand in _item_name_cache:  # Change cache name
            cache_entry = _item_name_cache[brand]
            if current_time - cache_entry["timestamp"] < _cache_ttl:
                logger.info(f"Using cached item names for brand {brand}")
                return cache_entry["item_names"]

    # Fetch from database
    try:
        products_collection = db.get_collection(PRODUCTS_COLLECTION)

        pipeline = [
            {"$match": {"brand": brand}},
            {"$project": {"name": 1, "_id": 0}},  # Get item names instead of SKUs
            {"$group": {"_id": None, "item_names": {"$addToSet": "$name"}}},
        ]

        result = list(products_collection.aggregate(pipeline))

        if result and "item_names" in result[0]:
            item_names = result[0]["item_names"]
            # Filter out None/null values
            item_names = [name for name in item_names if name is not None]

            # Cache the result
            with _cache_lock:
                _item_name_cache[brand] = {
                    "item_names": item_names,
                    "timestamp": current_time,
                }

            logger.info(f"Found {len(item_names)} item names for brand {brand}")
            return item_names
        else:
            logger.warning(f"No item names found for brand {brand}")
            return []

    except Exception as e:
        logger.error(f"Error getting item names for brand {brand}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error retrieving item names for brand {brand}"
        )


def query_invoices_for_item_names(
    db,
    item_names: List[str],
    start_date: datetime,
    end_date: datetime,
    exclude_customers: bool,
) -> List[dict]:
    """
    Query invoices by matching item names directly - much simpler!
    """
    try:
        invoices_collection = db.get_collection(INVOICES_COLLECTION)

        match_statement = (
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$gte": [{"$toDate": "$created_date"}, start_date]},
                            {"$lte": [{"$toDate": "$created_date"}, end_date]},
                        ]
                    },
                    "status": {"$nin": ["draft", "void"]},
                    # "customer_name": {"$nin": EXCLUDED_CUSTOMERS_LIST},
                    # Much simpler - just match item names directly
                    "line_items": {"$elemMatch": {"name": {"$in": item_names}}},
                }
            }
            if exclude_customers
            else {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$gte": [{"$toDate": "$created_date"}, start_date]},
                            {"$lte": [{"$toDate": "$created_date"}, end_date]},
                        ]
                    },
                    "status": {"$nin": ["draft", "void"]},
                    # Just match item names
                    "line_items": {"$elemMatch": {"name": {"$in": item_names}}},
                }
            }
        )

        # Simplified aggregation pipeline
        pipeline = [
            # Stage 1: Match documents
            match_statement,
            # Stage 2: Filter line items to only matching ones
            {
                "$addFields": {
                    "_matchingItems": {
                        "$filter": {
                            "input": "$line_items",
                            "as": "item",
                            "cond": {"$in": ["$$item.name", item_names]},
                        }
                    }
                }
            },
            # Stage 3: Unwind matching items
            {"$unwind": "$_matchingItems"},
            # Stage 4: Project final fields - much simpler!
            {
                "$project": {
                    "_id": 1,
                    "customer_name": 1,
                    "created_date_str": "$created_date",
                    "created_at": 1,
                    "quantity": "$_matchingItems.quantity",
                    "item_name": "$_matchingItems.name",
                    "sku_code": {
                        "$ifNull": [
                            "$_matchingItems.sku",  # Try direct SKU field first
                            {
                                "$arrayElemAt": [
                                    "$_matchingItems.item_custom_fields.value",
                                    0,
                                ]
                            },  # Fallback to custom fields if available
                        ]
                    },
                }
            },
        ]

        logger.info(f"Executing item name aggregation for {len(item_names)} items")
        results = list(invoices_collection.aggregate(pipeline, allowDiskUse=True))
        logger.info(f"Found {len(results)} total matching invoices for all items")

        # Process results
        all_results = []
        for doc in results:
            all_results.append(
                {
                    "Invoice ID": str(doc["_id"]),
                    "Customer": doc.get("customer_name", "N/A"),
                    "Item": doc.get("item_name", "N/A"),
                    "Quantity": doc.get("quantity", 0),
                    "Created Date (DB String)": doc.get("created_date_str", "N/A"),
                    "Created At (Timestamp)": doc.get("created_at", "N/A"),
                    "SKU": doc.get("sku_code", "N/A"),
                }
            )

        return all_results

    except Exception as e:
        logger.error(f"Error in item name invoice query: {e}")
        raise HTTPException(status_code=500, detail="Error querying invoice data")


def create_excel_file(data: List[dict]) -> io.BytesIO:
    """
    Create Excel file with parallel processing for large datasets.
    """
    try:
        if not data:
            raise HTTPException(
                status_code=404, detail="No data found for the specified criteria"
            )

        logger.info(f"Creating Excel file with {len(data)} records")

        # For large datasets, process in parallel
        if len(data) > 10000:
            # Split data into chunks for parallel processing
            chunk_size = len(data) // 4  # 4 chunks
            chunks = [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]

            # Process chunks in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
                df_chunks = list(executor.map(pd.DataFrame, chunks))

            # Combine chunks
            df = pd.concat(df_chunks, ignore_index=True)
        else:
            # For smaller datasets, process normally
            df = pd.DataFrame(data)

        # Format timestamp column if it exists
        if "Created At (Timestamp)" in df.columns and not df.empty:
            if pd.api.types.is_datetime64_any_dtype(df["Created At (Timestamp)"]):
                df["Created At (Timestamp)"] = df["Created At (Timestamp)"].dt.strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            else:
                df["Created At (Timestamp)"] = df["Created At (Timestamp)"].astype(str)

        # Create Excel file in memory
        excel_buffer = io.BytesIO()

        # Use xlsxwriter for better performance on large files
        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Invoice Report", index=False)

            # Optional: Add formatting for better presentation
            workbook = writer.book
            worksheet = writer.sheets["Invoice Report"]

            # Auto-adjust column widths
            for column in df:
                column_length = max(df[column].astype(str).map(len).max(), len(column))
                col_idx = df.columns.get_loc(column)
                worksheet.column_dimensions[chr(65 + col_idx)].width = min(
                    column_length + 2, 50
                )

        excel_buffer.seek(0)
        logger.info("Excel file created successfully")
        return excel_buffer

    except Exception as e:
        logger.error(f"Error creating Excel file: {e}")
        raise HTTPException(status_code=500, detail="Error generating Excel file")


@router.post("/generate-invoice-report")
def generate_invoice_report(
    request: ReportRequest, background_tasks: BackgroundTasks, db=Depends(get_database)
):
    try:
        start_time = datetime.now()
        logger.info(
            f"Starting report generation for brand: {request.brand}, date range: {request.start_date} to {request.end_date}"
        )

        # Parse dates
        start_date = datetime.strptime(request.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(request.end_date, "%Y-%m-%d")

        # STEP 1: Get item names instead of SKUs
        item_name_start = datetime.now()
        item_names = get_item_names_by_brand(db, request.brand)
        item_name_duration = (datetime.now() - item_name_start).total_seconds()
        logger.info(f"Item name lookup took {item_name_duration:.2f} seconds")

        if not item_names:
            raise HTTPException(
                status_code=404, detail=f"No items found for brand '{request.brand}'"
            )

        # STEP 2: Query invoices by item names
        query_start = datetime.now()
        invoice_data = query_invoices_for_item_names(
            db, item_names, start_date, end_date, request.exclude_customers
        )
        query_duration = (datetime.now() - query_start).total_seconds()
        logger.info(f"Invoice query took {query_duration:.2f} seconds")

        if not invoice_data:
            raise HTTPException(
                status_code=404, detail="No invoices found for the specified criteria"
            )
        # STEP 3: Create Excel file (parallel processing for large datasets)
        excel_start = datetime.now()
        excel_file = create_excel_file(invoice_data)
        excel_duration = (datetime.now() - excel_start).total_seconds()
        logger.info(f"Excel generation took {excel_duration:.2f} seconds")

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"invoice_report_{request.brand}_{timestamp}.xlsx"

        total_duration = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Report generated successfully in {total_duration:.2f} seconds with {len(invoice_data)} records"
        )

        # Return Excel file as streaming response
        return StreamingResponse(
            io.BytesIO(excel_file.read()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error generating report: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")


@router.get("/brands")
def get_available_brands(db=Depends(get_database)):
    """
    Get list of available brands.
    Adjust this based on your actual data structure.
    """
    try:
        products_collection = db.get_collection(PRODUCTS_COLLECTION)

        # Get distinct brands
        brands = products_collection.find({"status": "active"}).distinct("brand")
        return {
            "brands": [
                {"value": brand, "label": brand.title()} for brand in brands if brand
            ]
        }

    except Exception as e:
        logger.error(f"Error getting brands: {e}")
        raise HTTPException(status_code=500, detail="Error retrieving brands")


class SalesReportRequest(BaseModel):
    start_date: str
    end_date: str

    @validator("start_date", "end_date")
    def validate_date_format(cls, v):
        try:
            datetime.strptime(v, "%Y-%m-%d")
            return v
        except ValueError:
            raise ValueError("Date must be in YYYY-MM-DD format")

    @validator("end_date")
    def validate_date_range(cls, v, values):
        if "start_date" in values:
            start = datetime.strptime(values["start_date"], "%Y-%m-%d")
            end = datetime.strptime(v, "%Y-%m-%d")
            if start > end:
                raise ValueError("End date must be after start date")
        return v


# Response models
class SalesReportItem(BaseModel):
    item_name: str
    sku_code: str
    units_sold: int
    units_returned: int
    credit_notes: int = 0
    total_amount: float
    closing_stock: int
    total_days_in_stock: int
    drr: float
    last_90_days_dates: str = ""  # Optional field for last 90 days in stock date ranges


class SalesReportResponse(BaseModel):
    data: List[SalesReportItem]
    summary: dict
    meta: dict


import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any, Optional
from functools import lru_cache

# Thread pool for parallel operations
executor = ThreadPoolExecutor(max_workers=4)


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


async def fetch_last_n_days_in_stock(
    db, end_datetime: datetime, item_ids: list, n_days: int = 90
) -> Dict:
    """
    Fetch the last N days an item was in stock, regardless of when those days occurred.
    Returns formatted date ranges for each item.

    Args:
        db: Database connection
        end_datetime: End date to look back from
        item_ids: List of zoho_item_ids to fetch stock for
        n_days: Number of days to fetch (default 90)

    Returns:
        Dict mapping item_id to formatted date ranges string
    """
    try:
        stock_collection = db["zoho_warehouse_stock"]

        # CRITICAL OPTIMIZATION: Limit lookback to prevent scanning millions of records
        # Look back maximum 1 year to find the last 90 days in stock
        lookback_limit = end_datetime - timedelta(days=365)

        logger.info(f"Fetching last {n_days} days in stock for {len(item_ids)} items from {lookback_limit.date()} to {end_datetime.date()}")

        # Aggregate to get the last N days where stock > 0 for each item
        pipeline = [
            {
                "$match": {
                    "zoho_item_id": {"$in": item_ids},
                    "date": {"$gte": lookback_limit, "$lte": end_datetime},  # Only last 1 year
                }
            },
            {
                "$project": {
                    "zoho_item_id": 1,
                    "date": 1,
                    "pupscribe_stock": {
                        "$ifNull": [
                            "$warehouses.Pupscribe Enterprises Private Limited",
                            0,
                        ]
                    },
                }
            },
            # Filter only days with stock > 0
            {
                "$match": {
                    "pupscribe_stock": {"$gt": 0}
                }
            },
            # Sort by item and date descending (most recent first)
            {"$sort": {"zoho_item_id": 1, "date": -1}},
            # Group by item and collect last N dates
            {
                "$group": {
                    "_id": "$zoho_item_id",
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

        def _run_aggregation():
            cursor = stock_collection.aggregate(pipeline, allowDiskUse=True, batchSize=10000)
            stock_date_ranges = {}
            processed_count = 0
            for doc in cursor:
                processed_count += 1
                item_id = doc["_id"]
                dates = doc.get("stock_dates", [])

                if processed_count <= 3:
                    logger.debug(f"DEBUG: Item {item_id} has {len(dates)} stock dates")

                if dates:
                    formatted_ranges = format_date_ranges(dates)
                    stock_date_ranges[item_id] = formatted_ranges

                    if processed_count == 1:
                        logger.debug(f"DEBUG: First item formatted ranges: {formatted_ranges[:100]}...")
                else:
                    stock_date_ranges[item_id] = ""

            logger.info(f"Fetched last {n_days} days in stock: processed {processed_count} items, returned {len(stock_date_ranges)} items with data")

            if stock_date_ranges:
                sample_keys = list(stock_date_ranges.keys())[:3]
                logger.debug(f"DEBUG: Sample item IDs in result: {sample_keys}")

            return stock_date_ranges

        return await asyncio.to_thread(_run_aggregation)

    except Exception as e:
        logger.error(f"Error fetching last N days in stock: {e}", exc_info=True)
        return {}


async def fetch_stock_data_for_items(
    db, start_datetime: datetime, end_datetime: datetime, item_ids: list, fetch_last_90_days: bool = False
) -> Dict:
    """
    Fetch stock data ONLY for specific items (not all items in database).
    This is MUCH faster than scanning all 4.3M records.

    Args:
        db: Database connection
        start_datetime: Start date for days in stock calculation
        end_datetime: End date for closing stock
        item_ids: List of zoho_item_ids to fetch stock for
        fetch_last_90_days: If True, also fetch last 90 days in stock with date ranges

    Returns:
        Dict mapping item_id to {closing_stock, total_days_in_stock, last_90_days_dates (optional)}
    """
    try:
        stock_collection = db["zoho_warehouse_stock"]

        logger.info(
            f"Fetching stock for {len(item_ids)} specific items up to {end_datetime.date()}"
        )

        # CRITICAL OPTIMIZATION: Only query stock for items that have sales
        pipeline = [
            # Match ONLY the specific items we need
            {
                "$match": {
                    "zoho_item_id": {"$in": item_ids},  # ONLY these items!
                    "date": {"$lte": end_datetime},
                }
            },
            # Project only needed fields early
            {
                "$project": {
                    "zoho_item_id": 1,
                    "date": 1,
                    "pupscribe_stock": {
                        "$ifNull": [
                            "$warehouses.Pupscribe Enterprises Private Limited",
                            0,
                        ]
                    },
                }
            },
            # Sort by item and date descending
            {"$sort": {"zoho_item_id": 1, "date": -1}},
            # Group and calculate using $sum (no arrays)
            {
                "$group": {
                    "_id": "$zoho_item_id",
                    "closing_stock": {"$first": "$pupscribe_stock"},
                    "latest_date": {"$first": "$date"},
                    "total_days_in_stock": {
                        "$sum": {
                            "$cond": {
                                "if": {
                                    "$and": [
                                        {"$gte": ["$date", start_datetime]},
                                        {"$lte": ["$date", end_datetime]},
                                        {"$gt": ["$pupscribe_stock", 0]},
                                    ]
                                },
                                "then": 1,
                                "else": 0,
                            }
                        }
                    },
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "closing_stock": 1,
                    "total_days_in_stock": 1,
                    "latest_date": 1,
                }
            },
        ]

        # Execute with optimized settings - run in thread to avoid blocking event loop
        def _run_aggregation():
            cursor = stock_collection.aggregate(
                pipeline, allowDiskUse=True, batchSize=10000
            )
            result = {}
            for doc in cursor:
                result[doc["_id"]] = {
                    "closing_stock": doc.get("closing_stock", 0),
                    "total_days_in_stock": doc.get("total_days_in_stock", 0),
                }
            return result

        stock_data = await asyncio.to_thread(_run_aggregation)

        # If requested, also fetch last 90 days in stock with date ranges
        if fetch_last_90_days:
            last_90_days_data = await fetch_last_n_days_in_stock(db, end_datetime, item_ids, 90)
            # Merge the last 90 days data into stock_data
            for item_id, date_ranges in last_90_days_data.items():
                if item_id in stock_data:
                    stock_data[item_id]["last_90_days_dates"] = date_ranges
                else:
                    stock_data[item_id] = {
                        "closing_stock": 0,
                        "total_days_in_stock": 0,
                        "last_90_days_dates": date_ranges,
                    }

        logger.info(f"Fetched stock data for {len(stock_data)} items")
        return stock_data

    except Exception as e:
        logger.error(f"Error fetching stock data for items: {e}")
        return {}


async def fetch_stock_data_optimized(
    db, start_datetime: datetime, end_datetime: datetime
) -> Dict:
    """
    Optimized stock data fetching with better aggregation pipeline.
    - Requires zoho_item_id field in stock records (run migration script first)
    - Closing Stock: Most recent stock value on or before end_date
    - Days in Stock: Count of days within date range where stock > 0

    Performance optimizations:
    - Limits lookback to 730 days (2 years) before start date for closing stock
    - Uses $sum instead of $push to avoid building arrays
    - Early projection to reduce data processing
    """
    try:
        stock_collection = db["zoho_warehouse_stock"]

        # PERFORMANCE: Limit lookback to 2 years for closing stock
        lookback_limit = start_datetime - timedelta(days=730)

        logger.info(
            f"Fetching stock data from {lookback_limit.date()} to {end_datetime.date()}"
        )

        # Highly optimized pipeline using $sum instead of $push to avoid building arrays
        pipeline = [
            # OPTIMIZATION 1: Filter with 2-year lookback limit
            {
                "$match": {
                    "date": {
                        "$gte": lookback_limit,  # 2 years lookback
                        "$lte": end_datetime,
                    },
                    "zoho_item_id": {"$exists": True, "$ne": None},
                }
            },
            # OPTIMIZATION 2: Project only needed fields and compute pupscribe_stock early
            {
                "$project": {
                    "zoho_item_id": 1,
                    "date": 1,
                    "pupscribe_stock": {
                        "$ifNull": [
                            "$warehouses.Pupscribe Enterprises Private Limited",
                            0,
                        ]
                    },
                }
            },
            # OPTIMIZATION 3: Sort by item and date descending (latest first)
            {"$sort": {"zoho_item_id": 1, "date": -1}},
            # OPTIMIZATION 4: Group and use $sum for days calculation (no arrays!)
            {
                "$group": {
                    "_id": "$zoho_item_id",
                    "closing_stock": {
                        "$first": "$pupscribe_stock"
                    },  # Most recent stock
                    "latest_date": {"$first": "$date"},
                    # Count days in stock using $sum instead of $push
                    "total_days_in_stock": {
                        "$sum": {
                            "$cond": {
                                "if": {
                                    "$and": [
                                        {"$gte": ["$date", start_datetime]},
                                        {"$lte": ["$date", end_datetime]},
                                        {"$gt": ["$pupscribe_stock", 0]},
                                    ]
                                },
                                "then": 1,
                                "else": 0,
                            }
                        }
                    },
                }
            },
            # OPTIMIZATION 5: Final projection
            {
                "$project": {
                    "_id": 1,
                    "closing_stock": 1,
                    "total_days_in_stock": 1,
                    "latest_date": 1,
                }
            },
        ]

        # Use cursor with optimized settings - run in thread to avoid blocking event loop
        def _run_aggregation():
            cursor = stock_collection.aggregate(
                pipeline,
                allowDiskUse=True,
                batchSize=10000,
            )
            stock_data = {}
            for doc in cursor:
                stock_data[doc["_id"]] = {
                    "closing_stock": doc.get("closing_stock", 0),
                    "total_days_in_stock": doc.get("total_days_in_stock", 0),
                }
            return stock_data

        stock_data = await asyncio.to_thread(_run_aggregation)

        logger.info(
            f"Fetched {len(stock_data)} stock records with zoho_item_id for date range {start_datetime.date()} to {end_datetime.date()}"
        )
        return stock_data

    except Exception as e:
        logger.error(f"Error fetching stock data: {e}")
        return {}


async def fetch_all_products_indexed(db) -> Dict:
    """
    Fetch all products and create an indexed map for O(1) lookups.
    """
    try:
        products_collection = db["products"]

        def _fetch():
            cursor = products_collection.find(
                {
                    "$and": [
                        {"name": {"$ne": ""}},
                        {"name": {"$ne": "amazon"}},
                    ]
                },
                {
                    "item_id": 1,
                    "cf_sku_code": 1,
                    "name": 1,
                    "_id": 0,
                },
                batch_size=5000,
            )
            products_map = {}
            for product in cursor:
                products_map[product.get("item_id")] = product.get("cf_sku_code", "")
            return products_map

        products_map = await asyncio.to_thread(_fetch)
        logger.info(f"Indexed {len(products_map)} products")
        return products_map

    except Exception as e:
        logger.error(f"Error fetching products: {e}")
        return {}


def process_batch(batch: List[Dict], stock_data: Dict, products_map: Dict) -> Dict:
    items = []
    total_units = 0
    total_amount = 0
    total_stock = 0

    for item in batch:
        item_id = item["_id"]
        units_sold = item.get("total_units_sold", 0)
        units_returned = item.get("total_units_returned", 0)
        amount = item.get("total_amount", 0.0)

        # Get SKU and stock data...
        sku_code = products_map.get(item_id, "")
        stock_info = stock_data.get(item_id, {})
        closing_stock = stock_info.get("closing_stock", 0)
        days_in_stock = stock_info.get("total_days_in_stock", 0)
        drr = round(units_sold / days_in_stock, 2) if days_in_stock > 0 else 0

        # Only accumulate totals for items with names
        if item.get("item_name") != "":
            total_units += units_sold
            total_amount += amount
            total_stock += closing_stock

            items.append(
                SalesReportItem(
                    item_name=item.get("item_name") or "",
                    sku_code=sku_code,
                    units_sold=units_sold,
                    total_amount=round(amount, 2),
                    closing_stock=closing_stock,
                    total_days_in_stock=days_in_stock,
                    drr=drr,
                )
            )

    return {
        "items": items,
        "units": total_units,
        "amount": total_amount,
        "stock": total_stock,
    }


@lru_cache(maxsize=100)
def get_excluded_customer_list() -> str:
    patterns = [
        # "(EC)",
        # "(NA)",
        # "(amzb2b)",
        # "(amz2b2)",
        # "(PUPEV)",
        # "(RS)",
        # "(MKT)",
        # "(SPUR)",
        # "(SSAM)",
        # "(OSAM)",
        # "(OSAMP)",
        # "(DON)",
        # "(Jiomart b2c)",
        # "Blinkit",
        # "KIRANAKART",
        # "Mr. Tikoti Laxman",
        # "Rushil Kalsi",
        # "ETRADE",
        # "Pupscribe",
        # "Flipkart",
    ]

    escaped_patterns = [re.escape(pattern) for pattern in patterns]
    regex_pattern = "|".join(escaped_patterns)

    return regex_pattern


async def get_cached_report(cache_key: str, db) -> Optional[Dict]:
    """
    Get cached report if available and fresh (less than 1 hour old).
    """
    try:
        cache_collection = db.get_collection("report_cache")

        def _fetch():
            return cache_collection.find_one(
                {
                    "_id": cache_key,
                    "created_at": {"$gte": datetime.now() - timedelta(hours=1)},
                }
            )

        cached = await asyncio.to_thread(_fetch)

        if cached:
            return cached.get("data")

    except Exception as e:
        logger.warning(f"Cache retrieval failed: {e}")

    return None


async def cache_report(cache_key: str, data: Dict, db):
    """
    Cache report for future use.
    """
    try:
        cache_collection = db.get_collection("report_cache")

        def _write():
            cache_collection.replace_one(
                {"_id": cache_key},
                {"_id": cache_key, "data": data, "created_at": datetime.now()},
                upsert=True,
            )

        await asyncio.to_thread(_write)

        logger.info(f"Report cached with key: {cache_key}")

    except Exception as e:
        logger.warning(f"Failed to cache report: {e}")


def build_optimized_pipeline_with_credit_notes_and_composites(
    start_date: str, end_date: str, excluded_customers: str = None
) -> List[Dict]:
    """
    Build an optimized aggregation pipeline that incorporates both invoices and credit notes,
    and handles composite products by breaking them down into individual components.
    Credit notes logic remains exactly the same - only composite products are expanded.

    When excluded_customers is None or empty, no customer filtering is applied.
    """
    # Build invoice match conditions
    invoice_match_conditions = [
        {"date": {"$gte": start_date, "$lte": end_date}},
        {"status": {"$nin": ["draft", "void"]}},
        {"invoice_number": {"$regex": "INV/", "$options": "i"}},
    ]

    # Build credit note match conditions
    credit_note_match_conditions = [
        {"date": {"$gte": start_date, "$lte": end_date}},
    ]

    # Only add customer exclusion filter if excluded_customers is provided
    if excluded_customers:
        customer_filter = {
            "customer_name": {
                "$not": {
                    "$regex": excluded_customers,
                    "$options": "i",
                }
            }
        }
        invoice_match_conditions.append(customer_filter)
        credit_note_match_conditions.append(customer_filter)

    return [
        {
            "$match": {
                "$and": invoice_match_conditions
            }
        },
        {"$addFields": {"doc_type": "invoice"}},
        {
            "$unionWith": {
                "coll": "credit_notes",
                "pipeline": [
                    {
                        "$match": {
                            "$and": credit_note_match_conditions
                        }
                    },
                    {"$addFields": {"doc_type": "credit_note"}},
                ],
            }
        },
        {"$unwind": "$line_items"},
        {
            "$lookup": {
                "from": "composite_products",
                "localField": "line_items.name",
                "foreignField": "name",
                "as": "composite_match",
            }
        },
        {
            "$addFields": {
                "is_composite": {"$gt": [{"$size": "$composite_match"}, 0]},
                "composite_info": {"$arrayElemAt": ["$composite_match", 0]},
            }
        },
        {
            "$project": {
                "items_to_process": {
                    "$cond": [
                        # If it's a composite item
                        "$is_composite",
                        # Then: create array of component items
                        {
                            "$map": {
                                "input": "$composite_info.components",
                                "as": "component",
                                "in": {
                                    "doc_type": "$doc_type",
                                    "invoice_number": "$invoice_number",
                                    "creditnote_number": "$creditnote_number",
                                    "item_id": "$component.item_id",
                                    "item_name": "$component.name",
                                    # Multiply original quantity by component quantity
                                    "quantity": {
                                        "$multiply": [
                                            "$line_items.quantity",
                                            "$component.quantity",
                                        ]
                                    },
                                    # Distribute amount proportionally
                                    "item_total": {
                                        "$divide": [
                                            "$line_items.item_total",
                                            {"$size": "$composite_info.components"},
                                        ]
                                    },
                                },
                            }
                        },
                        # Else: keep as single regular item
                        [
                            {
                                "doc_type": "$doc_type",
                                "invoice_number": "$invoice_number",
                                "creditnote_number": "$creditnote_number",
                                "item_id": "$line_items.item_id",
                                "item_name": "$line_items.name",
                                "quantity": "$line_items.quantity",
                                "item_total": "$line_items.item_total",
                            }
                        ],
                    ]
                }
            }
        },
        {"$unwind": "$items_to_process"},
        {"$replaceRoot": {"newRoot": "$items_to_process"}},
        {
            "$group": {
                "_id": "$item_id",
                "item_name": {"$first": "$item_name"},
                "invoice_units": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$doc_type", "invoice"]},
                            "$quantity",
                            0,
                        ]
                    }
                },
                "credit_note_units": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$doc_type", "credit_note"]},
                            "$quantity",
                            0,
                        ]
                    }
                },
                "invoice_amount": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$doc_type", "invoice"]},
                            "$item_total",
                            0,
                        ]
                    }
                },
                "credit_note_amount": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$doc_type", "credit_note"]},
                            "$item_total",
                            0,
                        ]
                    }
                },
                "invoice_numbers": {
                    "$addToSet": {
                        "$cond": [
                            {"$eq": ["$doc_type", "invoice"]},
                            "$invoice_number",
                            None,
                        ]
                    }
                },
                "credit_note_numbers": {
                    "$addToSet": {
                        "$cond": [
                            {"$eq": ["$doc_type", "credit_note"]},
                            "$creditnote_number",
                            None,
                        ]
                    }
                },
            }
        },
        {
            "$project": {
                "_id": 1,
                "item_name": 1,
                "invoice_units": 1,
                "credit_note_units": 1,
                "invoice_amount": 1,
                "credit_note_amount": 1,
                "total_units_sold": "$invoice_units",
                "total_units_returned": "$credit_note_units",
                "total_amount": "$invoice_amount",
                "invoice_numbers": {
                    "$filter": {
                        "input": "$invoice_numbers",
                        "cond": {"$ne": ["$$this", None]},
                    }
                },
                "credit_note_numbers": {
                    "$filter": {
                        "input": "$credit_note_numbers",
                        "cond": {"$ne": ["$$this", None]},
                    }
                },
            }
        },
    ]


def process_batch_with_credit_notes(
    batch: List[Dict], stock_data: Dict, products_map: Dict
) -> Dict:
    """
    Process a batch of items that includes net calculations from invoices and credit notes.
    Maintains the same output structure as the original process_batch function.
    """
    items = []
    total_units = 0
    total_amount = 0
    total_stock = 0
    total_returns = 0
    for item in batch:
        item_id = item["_id"]

        # Net quantities (already calculated in the pipeline)
        units_sold = item.get("total_units_sold", 0)
        amount = item.get("total_amount", 0.0)

        # Additional data for debugging/logging (can be removed in production)
        invoice_units = item.get("invoice_units", 0)
        credit_note_units = item.get("credit_note_units", 0)
        invoice_numbers = item.get("invoice_numbers", [])
        credit_note_numbers = item.get("credit_note_numbers", [])
        # Get SKU and stock data (same as before)
        sku_code = products_map.get(item_id, "")
        stock_info = stock_data.get(item_id, {})
        closing_stock = stock_info.get("closing_stock", 0)
        days_in_stock = stock_info.get("total_days_in_stock", 0)
        last_90_days_dates = stock_info.get("last_90_days_dates", "")  # Get the new field
        drr = round(units_sold / days_in_stock, 2) if days_in_stock > 0 else 0

        # Only accumulate totals for items with names (same filtering as before)
        if item.get("item_name") != "":
            total_units += units_sold
            total_amount += amount
            total_stock += closing_stock
            total_returns += credit_note_units
            # Log credit note activity for items that have it (optional, only for debugging)
            if credit_note_units > 0:
                logger.debug(
                    f"Item {item.get('item_name')} - Invoice units: {invoice_units}, "
                    f"Credit note units: {credit_note_units}, Net units: {units_sold}"
                )

            items.append(
                SalesReportItem(
                    item_name=item.get("item_name") or "",
                    sku_code=sku_code,
                    units_returned=credit_note_units,
                    credit_notes=credit_note_units,
                    units_sold=units_sold,  # This is now net units (invoice - credit notes)
                    total_amount=round(amount, 2),  # This is now net amount
                    closing_stock=closing_stock,
                    total_days_in_stock=days_in_stock,
                    drr=drr,
                    last_90_days_dates=last_90_days_dates,  # Add the new field
                )
            )

    return {
        "items": items,
        "units": total_units,
        "amount": total_amount,
        "stock": total_stock,
        "returns": total_returns,
    }


@router.get("/sales-report")
async def get_sales_report_fast(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    any_last_90_days: bool = Query(False, description="Include last 90 days in stock with date ranges"),
    exclude_customers: bool = True,
    db=Depends(get_database),
):
    """
    Ultra-optimized sales report that incorporates credit notes.
    Net sales = Invoice quantities - Credit note quantities
    Response structure remains the same for frontend compatibility.

    When any_last_90_days=True, includes a new field 'last_90_days_dates' with formatted date ranges
    showing the last 90 days the item was in stock, regardless of when those days occurred.
    """

    try:
        # Validate dates
        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

        if start_datetime > end_datetime:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="End date must be after start date",
            )

        logger.info(
            f"Generating sales report with credit notes for {start_date} to {end_date}, any_last_90_days={any_last_90_days}"
        )
        start_time = datetime.now()

        # OPTIMIZATION 1: Run stock and products fetching in parallel with 2-year lookback
        stock_task = asyncio.create_task(
            fetch_stock_data_optimized(db, start_datetime, end_datetime)
        )
        products_task = asyncio.create_task(fetch_all_products_indexed(db))

        # OPTIMIZATION 2: Use efficient customer filtering
        excluded_customers = get_excluded_customer_list() if exclude_customers else None

        # OPTIMIZATION 3: Use the NEW pipeline that includes credit notes
        invoices_collection = db[INVOICES_COLLECTION]

        # Build the NEW pipeline with credit notes integration
        pipeline = build_optimized_pipeline_with_credit_notes_and_composites(
            start_date, end_date, excluded_customers
        )

        # Get parallel task results while pipeline is prepared
        logger.info("Executing aggregation pipeline with credit notes integration...")
        stock_data, products_map = await asyncio.gather(stock_task, products_task)

        # If any_last_90_days is requested, fetch the last 90 days in stock for all items
        if any_last_90_days and stock_data:
            item_ids = list(stock_data.keys())
            logger.info(f"Fetching last 90 days in stock for {len(item_ids)} items")
            logger.debug(f"DEBUG: Sample stock_data keys: {item_ids[:3]}")

            last_90_days_data = await fetch_last_n_days_in_stock(db, end_datetime, item_ids, 90)
            logger.debug(f"DEBUG: Received {len(last_90_days_data)} items from fetch_last_n_days_in_stock")

            # Merge into stock_data
            merged_count = 0
            for item_id, date_ranges in last_90_days_data.items():
                if item_id in stock_data:
                    stock_data[item_id]["last_90_days_dates"] = date_ranges
                    merged_count += 1

            logger.debug(f"DEBUG: Merged {merged_count} items with last_90_days_dates into stock_data")

        logger.info(
            f"Processing {len(stock_data)} stock items and {len(products_map)} products with credit notes"
        )

        # Process results in a thread to avoid blocking the event loop
        def _process_cursor_results(collection, pipeline, stock_data, products_map):
            cursor = collection.aggregate(
                pipeline,
                allowDiskUse=True,
                batchSize=1000,
            )
            sales_report_items = []
            total_units = 0
            total_returns = 0
            total_amount = 0.0
            total_closing_stock = 0

            batch = []
            batch_size = 500

            for item in cursor:
                batch.append(item)
                if len(batch) >= batch_size:
                    processed = process_batch_with_credit_notes(
                        batch, stock_data, products_map
                    )
                    sales_report_items.extend(processed["items"])
                    total_units += processed["units"]
                    total_amount += processed["amount"]
                    total_returns += processed["returns"]
                    total_closing_stock += processed["stock"]
                    batch = []

            # Process remaining items
            if batch:
                processed = process_batch_with_credit_notes(batch, stock_data, products_map)
                sales_report_items.extend(processed["items"])
                total_units += processed["units"]
                total_amount += processed["amount"]
                total_returns += processed["returns"]
                total_closing_stock += processed["stock"]

            sales_report_items.sort(key=lambda x: x.item_name)
            return sales_report_items, total_units, total_returns, total_amount, total_closing_stock

        sales_report_items, total_units, total_returns, total_amount, total_closing_stock = await asyncio.to_thread(
            _process_cursor_results, invoices_collection, pipeline, stock_data, products_map
        )

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Sales report with credit notes generated in {execution_time:.2f} seconds with {len(sales_report_items)} items"
        )

        # Calculate summary (same logic as before, but now with net values)
        avg_drr = (
            sum(item.drr for item in sales_report_items) / len(sales_report_items)
            if sales_report_items
            else 0
        )
        items_with_stock = sum(
            1 for item in sales_report_items if item.closing_stock > 0
        )
        items_out_of_stock = sum(
            1 for item in sales_report_items if item.closing_stock == 0
        )

        # Prepare response (SAME STRUCTURE as before - no frontend changes needed)
        response_data = {
            "data": [item.dict() for item in sales_report_items],
            "summary": {
                "total_items": len(sales_report_items),
                "total_units_sold": total_units,  # Now net units (invoice - credit notes)
                "total_units_returned": total_returns,  # Now net units (invoice - credit notes)
                "total_amount": round(total_amount, 2),  # Now net amount
                "total_closing_stock": total_closing_stock,
                "average_drr": round(avg_drr, 2),
                "items_with_stock": items_with_stock,
                "items_out_of_stock": items_out_of_stock,
                "date_range": {"start_date": start_date, "end_date": end_date},
            },
            "meta": {
                "timestamp": datetime.now().isoformat(),
                "execution_time_seconds": round(execution_time, 2),
                "query_type": "ultra_fast_sales_report_with_credit_notes",
                "from_cache": False,
                "optimizations": [
                    "parallel_aggregations",
                    "indexed_products",
                    "batch_processing",
                    "optimized_customer_filter",
                    "credit_notes_integration",  # NEW
                ],
                "credit_notes_integrated": True,  # NEW field to indicate credit notes are included
            },
        }

        return JSONResponse(content=response_data)

    except Exception as e:
        logger.error(f"Error in sales report with credit notes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/sales-report/download")
async def download_sales_report(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    any_last_90_days: bool = Query(False, description="Include last 90 days in stock with date ranges"),
    db=Depends(get_database),
):
    """
    Download sales report as XLSX file with credit notes integration.
    Net sales = Invoice quantities - Credit note quantities

    When any_last_90_days=True, includes a new column 'Last 90 Days In Stock (Dates)' with formatted date ranges
    showing the last 90 days the item was in stock, regardless of when those days occurred.
    """

    try:
        # Validate dates
        start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

        if start_datetime > end_datetime:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="End date must be after start date",
            )

        logger.info(
            f"Generating sales report download with credit notes for {start_date} to {end_date}, any_last_90_days={any_last_90_days}"
        )
        start_time = datetime.now()

        # Use the SAME optimized approach as the main API
        stock_task = asyncio.create_task(
            fetch_stock_data_optimized(db, start_datetime, end_datetime)
        )
        products_task = asyncio.create_task(fetch_all_products_indexed(db))

        excluded_customers = get_excluded_customer_list()
        invoices_collection = db[INVOICES_COLLECTION]

        # Use the NEW pipeline with credit notes
        pipeline = build_optimized_pipeline_with_credit_notes_and_composites(
            start_date, end_date, excluded_customers
        )

        # Process exactly the same way as the main API
        stock_data, products_map = await asyncio.gather(stock_task, products_task)

        # If any_last_90_days is requested, fetch the last 90 days in stock for all items
        if any_last_90_days and stock_data:
            item_ids = list(stock_data.keys())
            logger.info(f"Fetching last 90 days in stock for download: {len(item_ids)} items")
            logger.debug(f"DEBUG (download): Sample stock_data keys: {item_ids[:3]}")

            last_90_days_data = await fetch_last_n_days_in_stock(db, end_datetime, item_ids, 90)
            logger.debug(f"DEBUG (download): Received {len(last_90_days_data)} items from fetch_last_n_days_in_stock")

            # Merge into stock_data
            merged_count = 0
            for item_id, date_ranges in last_90_days_data.items():
                if item_id in stock_data:
                    stock_data[item_id]["last_90_days_dates"] = date_ranges
                    merged_count += 1

            logger.debug(f"DEBUG (download): Merged {merged_count} items with last_90_days_dates into stock_data")

        # Run cursor creation + iteration in thread to avoid blocking
        def _process_download_cursor(collection, pipeline, stock_data, products_map):
            result_cursor = collection.aggregate(
                pipeline,
                allowDiskUse=True,
                batchSize=1000,
            )
            sales_report_items = []
            total_units = 0
            total_returns = 0
            total_amount = 0.0
            total_closing_stock = 0

            batch = []
            batch_size = 500

            for item in result_cursor:
                batch.append(item)
                if len(batch) >= batch_size:
                    processed = process_batch_with_credit_notes(
                        batch, stock_data, products_map
                    )
                    sales_report_items.extend(processed["items"])
                    total_units += processed["units"]
                    total_returns += processed["returns"]
                    total_amount += processed["amount"]
                    total_closing_stock += processed["stock"]
                    batch = []

            if batch:
                processed = process_batch_with_credit_notes(batch, stock_data, products_map)
                sales_report_items.extend(processed["items"])
                total_units += processed["units"]
                total_returns += processed["returns"]
                total_amount += processed["amount"]
                total_closing_stock += processed["stock"]

            sales_report_items.sort(key=lambda x: x.item_name)
            return sales_report_items, total_units, total_returns, total_amount, total_closing_stock

        sales_report_items, total_units, total_returns, total_amount, total_closing_stock = await asyncio.to_thread(
            _process_download_cursor, invoices_collection, pipeline, stock_data, products_map
        )

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Download data with credit notes prepared in {execution_time:.2f} seconds with {len(sales_report_items)} items"
        )

        # Create Excel file
        output = io.BytesIO()

        # Convert SalesReportItem objects to dictionaries for DataFrame
        sales_data = []
        for item in sales_report_items:
            row_data = {
                "Item Name": item.item_name,
                "SKU Code": item.sku_code,
                "Units Sold": item.units_sold,
                "Units Returned": item.units_returned,
                "Total Amount (₹)": item.total_amount,
                "Closing Stock": item.closing_stock,
                "Days In Stock": item.total_days_in_stock,
                "DRR (Daily Run Rate)": item.drr,
            }
            # Add the Last 90 Days In Stock column only if requested
            if any_last_90_days:
                row_data["Last 90 Days In Stock (Dates)"] = item.last_90_days_dates
            sales_data.append(row_data)

        # Create DataFrame
        df = pd.DataFrame(sales_data)

        # Create Excel file with multiple sheets
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            # Main data sheet
            df.to_excel(writer, sheet_name="Sales Report", index=False)

            # Summary sheet
            summary_data = {
                "Metric": [
                    "Report Period",
                    "Generated On",
                    "Processing Method",
                    "Credit Notes Integrated",
                    "",
                    "NET SALES SUMMARY",
                    "Total Items",
                    "Net Units Sold",
                    "Net Units Returned",
                    "Net Amount (₹)",
                    "Total Closing Stock",
                    "Items With Stock",
                    "Items Out of Stock",
                    "Average DRR",
                ],
                "Value": [
                    f"{start_date} to {end_date}",
                    datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Optimized with Credit Notes Integration",
                    "Yes",
                    "",
                    "",
                    len(sales_report_items),
                    total_units,
                    total_returns,
                    f"₹{total_amount:,.2f}",
                    total_closing_stock,
                    sum(1 for item in sales_report_items if item.closing_stock > 0),
                    sum(1 for item in sales_report_items if item.closing_stock == 0),
                    (
                        f"{sum(item.drr for item in sales_report_items) / len(sales_report_items):.2f}"
                        if sales_report_items
                        else "0.00"
                    ),
                ],
            }

            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name="Summary", index=False)

            # Format the main sheet
            workbook = writer.book
            worksheet = writer.sheets["Sales Report"]

            # Auto-adjust column widths
            for column in df:
                column_length = max(df[column].astype(str).map(len).max(), len(column))
                col_idx = df.columns.get_loc(column)
                # Convert column index to Excel column letter
                col_letter = (
                    chr(65 + col_idx)
                    if col_idx < 26
                    else chr(65 + col_idx // 26 - 1) + chr(65 + col_idx % 26)
                )
                worksheet.column_dimensions[col_letter].width = min(
                    column_length + 2, 50
                )

        output.seek(0)

        # Generate filename with credit notes indicator
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"net_sales_report_{start_date}_to_{end_date}_{timestamp}.xlsx"

        logger.info(f"Excel file generated successfully: {filename}")

        # Return the Excel file as streaming response
        return StreamingResponse(
            io.BytesIO(output.read()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error generating sales report download with credit notes: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}",
        )


from functools import lru_cache
from typing import Optional, Dict, Any
import asyncio


@router.get("/data-metadata")
async def get_data_metadata(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    db=Depends(get_database),
):
    """
    Get metadata for the SELECTED date range (not overall data availability).
    """

    try:
        # Validate dates
        try:
            start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
            end_datetime = datetime.strptime(end_date, "%Y-%m-%d")

            if start_datetime > end_datetime:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="End date must be after start date",
                )
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid date format. Use YYYY-MM-DD",
            )

        # Get collections
        invoices_collection = db[INVOICES_COLLECTION]
        stock_collection = db["zoho_warehouse_stock"]

        # Query inventory and sales data for the SELECTED date range
        inventory_count = await asyncio.to_thread(
            stock_collection.count_documents,
            {"date": {"$gte": start_datetime, "$lte": end_datetime}},
        )

        invoice_count = await asyncio.to_thread(
            invoices_collection.count_documents,
            {"date": {"$gte": start_date, "$lte": end_date}},
        )

        # Get distinct dates for inventory
        inventory_distinct_dates = await asyncio.to_thread(
            lambda: list(stock_collection.distinct(
                "date", {"date": {"$gte": start_datetime, "$lte": end_datetime}}
            ))
        )
        inventory_dates_set = set(d.strftime("%Y-%m-%d") if isinstance(d, datetime) else d for d in inventory_distinct_dates)

        # Get distinct dates for sales
        sales_distinct_dates = await asyncio.to_thread(
            lambda: list(invoices_collection.distinct(
                "date", {"date": {"$gte": start_date, "$lte": end_date}}
            ))
        )
        sales_dates_set = set(d if isinstance(d, str) else d.strftime("%Y-%m-%d") for d in sales_distinct_dates)

        # Calculate all dates in the range
        from datetime import timedelta
        all_dates = set()
        current_date = start_datetime
        while current_date <= end_datetime:
            all_dates.add(current_date.strftime("%Y-%m-%d"))
            current_date += timedelta(days=1)

        # Find missing dates
        missing_inventory_dates = sorted(all_dates - inventory_dates_set)
        missing_sales_dates = sorted(all_dates - sales_dates_set)

        # Return metadata for selected date range
        metadata = {
            "inventory_data": {
                "first_inventory_date": start_date,
                "last_inventory_date": end_date,
                "total_stock_records": inventory_count,
                "missing_dates": missing_inventory_dates,
            },
            "sales_data": {
                "first_sales_date": start_date,
                "last_sales_date": end_date,
                "valid_invoices": invoice_count,
                "missing_dates": missing_sales_dates,
            },
            "date_range": {
                "start_date": start_date,
                "end_date": end_date,
                "filtered": True,
            },
            "last_updated": datetime.now().isoformat(),
        }

        return JSONResponse(
            content={
                "data": metadata,
                "meta": {
                    "timestamp": datetime.now().isoformat(),
                    "query_type": "date_range_filtered_metadata",
                    "date_filtered": True,
                },
            }
        )

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error fetching data metadata: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}",
        )


async def get_sales_metadata_optimized(
    collection,
    start_date: Optional[str],
    end_date: Optional[str],
    start_datetime: Optional[datetime],
    end_datetime: Optional[datetime],
) -> Dict[str, Any]:
    """
    Optimized sales metadata retrieval using a single aggregation pipeline.
    """
    try:
        # OPTIMIZATION 1: Single pipeline for all sales metrics
        pipeline = []

        # If dates are consistently formatted as YYYY-MM-DD strings, use string comparison
        if start_date and end_date:
            # Try string comparison first (faster)
            pipeline.append(
                {"$match": {"date": {"$gte": start_date, "$lte": end_date}}}
            )

        # Add facet for parallel processing of different metrics
        pipeline.append(
            {
                "$facet": {
                    # Get total count
                    "total_count": [{"$count": "count"}],
                    # Get valid invoices with dates
                    "valid_invoices": [
                        {"$match": {"status": {"$nin": ["draft", "void"]}}},
                        {
                            "$group": {
                                "_id": None,
                                "count": {"$sum": 1},
                                "first_date": {"$min": "$date"},
                                "last_date": {"$max": "$date"},
                            }
                        },
                    ],
                    # Get date range for all invoices
                    "date_range": [
                        {
                            "$group": {
                                "_id": None,
                                "min_date": {"$min": "$date"},
                                "max_date": {"$max": "$date"},
                            }
                        }
                    ],
                }
            }
        )

        # Execute the pipeline
        result = list(collection.aggregate(pipeline, allowDiskUse=True))

        if result:
            facet_result = result[0]

            # Extract total count
            total_invoices = (
                facet_result["total_count"][0]["count"]
                if facet_result.get("total_count")
                else 0
            )

            # Extract valid invoices data
            valid_data = facet_result.get("valid_invoices", [{}])[0]
            valid_count = valid_data.get("count", 0)

            # Parse dates if they're strings
            first_date = valid_data.get("first_date")
            last_date = valid_data.get("last_date")

            # Convert string dates to datetime if needed
            if first_date and isinstance(first_date, str):
                try:
                    first_date = datetime.strptime(first_date, "%Y-%m-%d")
                except:
                    pass

            if last_date and isinstance(last_date, str):
                try:
                    last_date = datetime.strptime(last_date, "%Y-%m-%d")
                except:
                    pass

            return {
                "first_sales_date": first_date.isoformat() if first_date else None,
                "last_sales_date": last_date.isoformat() if last_date else None,
                "total_invoices": total_invoices,
                "valid_invoices": valid_count,
            }

        # If no results, get basic counts
        total_invoices = await run_in_executor(collection.count_documents, {})
        valid_invoices = await run_in_executor(
            collection.count_documents, {"status": {"$nin": ["draft", "void"]}}
        )

        return {
            "first_sales_date": None,
            "last_sales_date": None,
            "total_invoices": total_invoices,
            "valid_invoices": valid_invoices,
        }

    except Exception as e:
        logger.error(f"Error in optimized sales metadata: {e}")
        return {
            "first_sales_date": None,
            "last_sales_date": None,
            "total_invoices": 0,
            "valid_invoices": 0,
        }


async def get_inventory_metadata_optimized(
    collection,
    start_datetime: Optional[datetime] = None,
    end_datetime: Optional[datetime] = None,
) -> Dict[str, Any]:
    """
    Optimized inventory metadata retrieval.
    Returns the actual date range available in the collection (not filtered by user's date selection).
    Note: start_datetime and end_datetime parameters are kept for API compatibility but not used.
    """
    try:
        # Don't filter by date - show the actual available data range
        # OPTIMIZATION: Single aggregation for all metrics
        pipeline = [
            {"$match": {}},  # No date filter - get all available data
            {
                "$group": {
                    "_id": None,
                    "count": {"$sum": 1},
                    "first_date": {"$min": "$date"},
                    "last_date": {"$max": "$date"},
                }
            },
        ]

        result = list(collection.aggregate(pipeline, allowDiskUse=True))

        if result:
            data = result[0]
            return {
                "first_inventory_date": (
                    data["first_date"].isoformat() if data.get("first_date") else None
                ),
                "last_inventory_date": (
                    data["last_date"].isoformat() if data.get("last_date") else None
                ),
                "total_stock_records": data.get("count", 0),
            }

        return {
            "first_inventory_date": None,
            "last_inventory_date": None,
            "total_stock_records": 0,
        }

    except Exception as e:
        logger.error(f"Error in optimized inventory metadata: {e}")
        return {
            "first_inventory_date": None,
            "last_inventory_date": None,
            "total_stock_records": 0,
        }


# Helper function for async execution
async def run_in_executor(func, *args):
    """Run blocking function in executor."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, func, *args)


# CACHED VERSION for frequently accessed metadata
@router.get("/data-metadata-cached")
async def get_data_metadata_cached(
    start_date: str = Query(
        None, description="Start date in YYYY-MM-DD format (optional)"
    ),
    end_date: str = Query(None, description="End date in YYYY-MM-DD format (optional)"),
    db=Depends(get_database),
    force_refresh: bool = Query(False, description="Force refresh cache"),
):
    """
    Cached version of data metadata for better performance.
    Cache is refreshed every 5 minutes or on demand.
    """

    cache_key = f"metadata_{start_date}_{end_date}"

    # Check if we need to refresh cache
    if force_refresh or not hasattr(get_data_metadata_cached, "_cache"):
        get_data_metadata_cached._cache = {}
        get_data_metadata_cached._cache_time = {}

    # Check if cached data exists and is fresh (5 minutes)
    current_time = datetime.now()
    cache_time = get_data_metadata_cached._cache_time.get(cache_key)

    if (
        not force_refresh
        and cache_key in get_data_metadata_cached._cache
        and cache_time
        and (current_time - cache_time).seconds < 300
    ):

        logger.info(f"Returning cached metadata for key: {cache_key}")
        cached_data = get_data_metadata_cached._cache[cache_key]
        cached_data["meta"]["from_cache"] = True
        cached_data["meta"]["cache_age_seconds"] = (current_time - cache_time).seconds
        return JSONResponse(content=cached_data)

    # Get fresh data
    logger.info(f"Fetching fresh metadata for key: {cache_key}")
    response = await get_data_metadata(start_date, end_date, db)

    # Parse response and cache it
    if hasattr(response, "body"):
        content = json.loads(response.body)
        get_data_metadata_cached._cache[cache_key] = content
        get_data_metadata_cached._cache_time[cache_key] = current_time
        content["meta"]["from_cache"] = False
        return JSONResponse(content=content)

    return response


@router.get("/estimates-vs-invoices")
def get_estimates_vs_invoices(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    db=Depends(get_database),
):
    """
    Compare estimates and invoices line items to calculate fill rate per item.

    Composite items are automatically expanded into their individual components.
    For example, if an estimate has 2 units of a combo product containing:
    - 1x Item A
    - 2x Item B

    This will be expanded to:
    - 2 units of Item A
    - 4 units of Item B

    Args:
        start_date: Start date for the report
        end_date: End date for the report

    Returns:
        List of items with estimated quantities, invoiced quantities, and fill rate percentage
    """
    try:
        # Validate date format
        try:
            start = datetime.strptime(start_date, "%Y-%m-%d")
            end = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(
                status_code=400, detail="Invalid date format. Use YYYY-MM-DD format."
            )

        if start > end:
            raise HTTPException(
                status_code=400, detail="Start date must be before or equal to end date"
            )

        # Get collections
        estimates_collection = db["estimates"]
        invoices_collection = db["invoices"]
        # Query estimates within date range - only invoiced estimates
        # Handle both string and Date object formats in the database
        estimates_query = {
            "customer_name": {"$nin": EXCLUDED_CUSTOMERS_LIST},
            "status": "invoiced",  # Only invoiced estimates
            "$or": [
                {"date": {"$gte": start, "$lte": end}},  # Date objects
                {"date": {"$gte": start_date, "$lte": end_date}},  # Strings
            ],
        }

        # Query invoices within date range
        # Handle both string and Date object formats in the database
        invoices_query = {
            "customer_name": {"$nin": EXCLUDED_CUSTOMERS_LIST},
            "status": {"$nin": ["void"]},
            "$or": [
                {"date": {"$gte": start, "$lte": end}},  # Date objects
                {"date": {"$gte": start_date, "$lte": end_date}},  # Strings
            ],
        }

        logger.info(f"Fetching estimates from {start_date} to {end_date}")
        logger.info(f"Estimates query: {estimates_query}")

        # Use aggregation to deduplicate estimates and handle composite items
        estimates_pipeline = [
            {"$match": estimates_query},
            {"$sort": {"_id": -1}},  # Sort by _id descending (latest first)
            {
                "$group": {
                    "_id": "$estimate_id",  # Group by estimate_id to deduplicate
                    "latest": {"$first": "$$ROOT"},  # Take first (latest) document
                }
            },
            {
                "$replaceRoot": {"newRoot": "$latest"}
            },  # Replace root with the latest document
            {"$unwind": "$line_items"},  # Unwind line items to process each one
            # Lookup composite products by name
            {
                "$lookup": {
                    "from": "composite_products",
                    "localField": "line_items.name",
                    "foreignField": "name",
                    "as": "composite_match",
                }
            },
            # Check if item is composite
            {
                "$addFields": {
                    "is_composite": {"$gt": [{"$size": "$composite_match"}, 0]},
                    "composite_info": {"$arrayElemAt": ["$composite_match", 0]},
                }
            },
            # Expand composite items or keep regular items
            {
                "$project": {
                    "items_to_process": {
                        "$cond": [
                            # If it's a composite item
                            "$is_composite",
                            # Then: create array of component items
                            {
                                "$map": {
                                    "input": "$composite_info.components",
                                    "as": "component",
                                    "in": {
                                        "item_id": "$$component.item_id",
                                        "name": "$$component.name",
                                        "sku": "$$component.sku_code",
                                        # Multiply original quantity by component quantity
                                        "quantity": {
                                            "$multiply": [
                                                "$line_items.quantity",
                                                "$$component.quantity",
                                            ]
                                        },
                                        "item_custom_fields": [],
                                    },
                                }
                            },
                            # Else: keep as single regular item
                            [
                                {
                                    "item_id": "$line_items.item_id",
                                    "name": "$line_items.name",
                                    "sku": "$line_items.sku",
                                    "quantity": "$line_items.quantity",
                                    "item_custom_fields": "$line_items.item_custom_fields",
                                }
                            ],
                        ]
                    }
                }
            },
            {"$unwind": "$items_to_process"},
            {"$replaceRoot": {"newRoot": "$items_to_process"}},
        ]
        estimates = list(estimates_collection.aggregate(estimates_pipeline, allowDiskUse=True, batchSize=1000))

        logger.info(f"Fetching invoices from {start_date} to {end_date}")

        # Use aggregation to deduplicate invoices and handle composite items
        invoices_pipeline = [
            {"$match": invoices_query},
            {"$sort": {"_id": -1}},  # Sort by _id descending (latest first)
            {
                "$group": {
                    "_id": "$invoice_id",  # Group by invoice_id to deduplicate
                    "latest": {"$first": "$$ROOT"},  # Take first (latest) document
                }
            },
            {
                "$replaceRoot": {"newRoot": "$latest"}
            },  # Replace root with the latest document
            {"$unwind": "$line_items"},  # Unwind line items to process each one
            # Lookup composite products by name
            {
                "$lookup": {
                    "from": "composite_products",
                    "localField": "line_items.name",
                    "foreignField": "name",
                    "as": "composite_match",
                }
            },
            # Check if item is composite
            {
                "$addFields": {
                    "is_composite": {"$gt": [{"$size": "$composite_match"}, 0]},
                    "composite_info": {"$arrayElemAt": ["$composite_match", 0]},
                }
            },
            # Expand composite items or keep regular items
            {
                "$project": {
                    "items_to_process": {
                        "$cond": [
                            # If it's a composite item
                            "$is_composite",
                            # Then: create array of component items
                            {
                                "$map": {
                                    "input": "$composite_info.components",
                                    "as": "component",
                                    "in": {
                                        "item_id": "$$component.item_id",
                                        "name": "$$component.name",
                                        "sku": "$$component.sku_code",
                                        # Multiply original quantity by component quantity
                                        "quantity": {
                                            "$multiply": [
                                                "$line_items.quantity",
                                                "$$component.quantity",
                                            ]
                                        },
                                        "item_custom_fields": [],
                                    },
                                }
                            },
                            # Else: keep as single regular item
                            [
                                {
                                    "item_id": "$line_items.item_id",
                                    "name": "$line_items.name",
                                    "sku": "$line_items.sku",
                                    "quantity": "$line_items.quantity",
                                    "item_custom_fields": "$line_items.item_custom_fields",
                                }
                            ],
                        ]
                    }
                }
            },
            {"$unwind": "$items_to_process"},
            {"$replaceRoot": {"newRoot": "$items_to_process"}},
        ]
        invoices = list(invoices_collection.aggregate(invoices_pipeline, allowDiskUse=True, batchSize=1000))

        logger.info(
            f"Found {len(estimates)} line items from estimates and {len(invoices)} line items from invoices (composite items expanded)"
        )

        # Debug logging for specific item
        logger.info(f"Processing aggregated line items with composite expansion...")

        # Get warehouse stock collection
        warehouse_stock_collection = db["zoho_warehouse_stock"]

        # Helper function to extract cf_sku_code from item_custom_fields
        def get_cf_sku_code(item):
            item_custom_fields = item.get("item_custom_fields", [])
            for field in item_custom_fields:
                if field.get("api_name") == "cf_sku_code":
                    return field.get("value", "")
            return item.get("sku", "")

        # Aggregate line items from estimates (already expanded by pipeline)
        estimate_items = {}
        debug_item_name = ""
        debug_total = 0

        for item in estimates:
            item_id = item.get("item_id")
            if not item_id:
                continue

            quantity = item.get("quantity", 0)
            item_name = item.get("name", "")

            # Debug logging for specific item
            if debug_item_name in item_name:
                debug_total += quantity
                logger.info(
                    f"Found estimate for {item_name}: qty={quantity}, running total={debug_total}"
                )

            if item_id not in estimate_items:
                estimate_items[item_id] = {
                    "item_id": item_id,
                    "item_name": item_name,
                    "sku": get_cf_sku_code(item),
                    "estimated_quantity": 0,
                }

            estimate_items[item_id]["estimated_quantity"] += quantity

        logger.info(f"Total estimated quantity for {debug_item_name}: {debug_total}")

        # Aggregate line items from invoices (already expanded by pipeline)
        invoice_items = {}
        for item in invoices:
            item_id = item.get("item_id")
            if not item_id:
                continue

            quantity = item.get("quantity", 0)

            if item_id not in invoice_items:
                invoice_items[item_id] = {
                    "item_id": item_id,
                    "item_name": item.get("name", ""),
                    "sku": get_cf_sku_code(item),
                    "invoiced_quantity": 0,
                }

            invoice_items[item_id]["invoiced_quantity"] += quantity

        # Get closing stock for all items on end_date
        all_item_ids = set(estimate_items.keys()) | set(invoice_items.keys())

        # Convert end_date string to datetime for comparison
        end_datetime = datetime.combine(end, datetime.min.time())

        logger.info(f"Fetching closing stock for {len(all_item_ids)} items")

        # Use aggregation pipeline to efficiently get the latest stock for each item
        closing_stock_map = {}
        stock_date = None

        try:
            pipeline = [
                # Match only items we care about and dates on or before end_date
                {
                    "$match": {
                        "zoho_item_id": {"$in": list(all_item_ids)},
                        "date": {"$lte": end_datetime},
                    }
                },
                # Sort by date descending to get latest first
                {"$sort": {"date": -1}},
                # Group by item and take the first (latest) record
                {
                    "$group": {
                        "_id": "$zoho_item_id",
                        "warehouses": {"$first": "$warehouses"},
                        "date": {"$first": "$date"},
                    }
                },
            ]

            warehouse_stocks = list(warehouse_stock_collection.aggregate(pipeline))

            logger.info(f"Found stock data for {len(warehouse_stocks)} items")

            # Create map of item_id to total closing stock and capture the stock date
            for stock in warehouse_stocks:
                item_id = stock.get("_id")
                if item_id:
                    warehouses = stock.get("warehouses", {})
                    total_stock = (
                        sum(warehouses.values()) if isinstance(warehouses, dict) else 0
                    )
                    closing_stock_map[item_id] = total_stock

                    # Capture stock date (all should have the same or similar date)
                    if stock_date is None and stock.get("date"):
                        stock_date = (
                            stock.get("date").strftime("%Y-%m-%d")
                            if hasattr(stock.get("date"), "strftime")
                            else str(stock.get("date"))
                        )

        except Exception as e:
            logger.error(f"Error fetching warehouse stock: {str(e)}")
            # Continue without stock data if there's an error
            pass

        # Combine and calculate fill rate
        result = []
        missed_items = 0
        over_delivered_items = 0

        for item_id in all_item_ids:
            estimate_data = estimate_items.get(item_id, {})
            invoice_data = invoice_items.get(item_id, {})

            estimated_qty = estimate_data.get("estimated_quantity", 0)
            invoiced_qty = invoice_data.get("invoiced_quantity", 0)
            closing_stock = closing_stock_map.get(item_id, 0)

            # Calculate fill rate
            fill_rate = 0
            if estimated_qty > 0:
                fill_rate = (invoiced_qty / estimated_qty) * 100

            # Count missed vs over-delivered
            if invoiced_qty < estimated_qty:
                missed_items += 1
            elif invoiced_qty > estimated_qty:
                over_delivered_items += 1

            # Get item details (prefer invoice data if available, else use estimate data)
            item_name = invoice_data.get("item_name") or estimate_data.get(
                "item_name", ""
            )
            sku = invoice_data.get("sku") or estimate_data.get("sku", "")

            result.append(
                {
                    "item_id": item_id,
                    "item_name": item_name,
                    "sku": sku,
                    "estimated_quantity": estimated_qty,
                    "invoiced_quantity": invoiced_qty,
                    "fill_rate": round(fill_rate, 2),
                    "closing_stock": closing_stock,
                    "missed_quantity": max(0, estimated_qty - invoiced_qty),
                }
            )

        # Sort by missed items first (invoiced < estimated), then by missed quantity descending
        result.sort(
            key=lambda x: (
                0 if x["invoiced_quantity"] < x["estimated_quantity"] else 1,
                -x["missed_quantity"],
            )
        )

        result.sort(key=lambda x:(x["item_name"]))
        return {
            "data": result,
            "meta": {
                "start_date": start_date,
                "end_date": end_date,
                "total_items": len(result),
                "total_estimates": len(estimates),
                "total_invoices": len(invoices),
                "missed_items": missed_items,
                "over_delivered_items": over_delivered_items,
                "fully_delivered_items": len(result)
                - missed_items
                - over_delivered_items,
                "stock_date": stock_date,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating estimates vs invoices report: {str(e)}")
        raise HTTPException(
            status_code=500, detail=f"Failed to generate report: {str(e)}"
        )
