import pandas as pd
from datetime import datetime, timedelta
import io, logging, math
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

router = APIRouter()


@router.get("/products")
async def get_products(
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
        print(f"MongoDB Error Getting Products: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error occurred while fetching products: {str(e)}",
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error Getting Products: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while getting the products: {str(e)}",
        )


@router.get("/products/summary")
async def get_products_summary():
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
        print(f"Error Getting Products Summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred getting the products summary: {str(e)}",
        )


@router.get("/sales")
async def get_sales(
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
        print(f"MongoDB Error Getting Sales: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error occurred while fetching sales: {str(e)}",
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error Getting Sales: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while getting the sales: {str(e)}",
        )


_sku_cache = {}
_cache_ttl = 300  # 5 minutes
_cache_lock = threading.Lock()


def get_skus_by_brand(db, brand: str) -> List[str]:
    """
    Cached version of SKU lookup with TTL for better performance.
    """
    current_time = datetime.now().timestamp()

    with _cache_lock:
        if brand in _sku_cache:
            cache_entry = _sku_cache[brand]
            if current_time - cache_entry["timestamp"] < _cache_ttl:
                logger.info(f"Using cached SKUs for brand {brand}")
                return cache_entry["skus"]

    # Fetch from database
    try:
        products_collection = db.get_collection(PRODUCTS_COLLECTION)

        pipeline = [
            {"$match": {"brand": brand}},
            {"$project": {"cf_sku_code": 1, "_id": 0}},
            {"$group": {"_id": None, "skus": {"$addToSet": "$cf_sku_code"}}},
        ]

        result = list(products_collection.aggregate(pipeline))

        if result and "skus" in result[0]:
            skus = result[0]["skus"]
            # Filter out None/null values
            skus = [sku for sku in skus if sku is not None]

            # Cache the result
            with _cache_lock:
                _sku_cache[brand] = {"skus": skus, "timestamp": current_time}

            logger.info(f"Found {len(skus)} SKUs for brand {brand}")
            return skus
        else:
            logger.warning(f"No SKUs found for brand {brand}")
            return []

    except Exception as e:
        logger.error(f"Error getting SKUs for brand {brand}: {e}")
        raise HTTPException(
            status_code=500, detail=f"Error retrieving SKUs for brand {brand}"
        )


def query_invoices_for_skus(
    db, skus: List[str], start_date: datetime, end_date: datetime
) -> List[dict]:
    """
    Optimized query that processes all SKUs in a single aggregation pipeline.
    This is the biggest performance improvement.
    """
    try:
        invoices_collection = db.get_collection(INVOICES_COLLECTION)

        # Single aggregation pipeline that handles ALL SKUs at once
        pipeline = [
            # Stage 1: Match documents by date range and status
            {
                "$match": {
                    "$expr": {
                        "$and": [
                            {"$gte": [{"$toDate": "$created_date"}, start_date]},
                            {"$lte": [{"$toDate": "$created_date"}, end_date]},
                        ]
                    },
                    "status": {"$nin": ["draft", "void"]},
                    # Match any line item that has any of our target SKUs
                    "line_items": {
                        "$elemMatch": {
                            "item_custom_fields": {
                                "$elemMatch": {"value": {"$in": skus}}
                            }
                        }
                    },
                }
            },
            # Stage 2: Add field with all matching items for ANY of our SKUs
            {
                "$addFields": {
                    "_matchingItems": {
                        "$filter": {
                            "input": "$line_items",
                            "as": "item",
                            "cond": {
                                "$anyElementTrue": {
                                    "$map": {
                                        "input": "$$item.item_custom_fields",
                                        "as": "customField",
                                        "in": {"$in": ["$$customField.value", skus]},
                                    }
                                }
                            },
                        }
                    }
                }
            },
            # Stage 3: Unwind matching items
            {"$unwind": "$_matchingItems"},
            # Stage 4: Add the actual SKU that matched
            {
                "$addFields": {
                    "_matchedSku": {
                        "$arrayElemAt": [
                            {
                                "$filter": {
                                    "input": "$_matchingItems.item_custom_fields",
                                    "as": "field",
                                    "cond": {"$in": ["$$field.value", skus]},
                                }
                            },
                            0,
                        ]
                    }
                }
            },
            # Stage 5: Project final fields
            {
                "$project": {
                    "_id": 1,
                    "customer_name": 1,
                    "created_date_str": "$created_date",
                    "created_at": 1,
                    "quantity": "$_matchingItems.quantity",
                    "item_name": "$_matchingItems.name",
                    "sku_code": "$_matchedSku.value",
                }
            },
        ]

        logger.info(f"Executing optimized aggregation for {len(skus)} SKUs")

        # Execute single aggregation
        results = list(invoices_collection.aggregate(pipeline, allowDiskUse=True))

        logger.info(f"Found {len(results)} total matching invoices for all SKUs")

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
        print(e)
        logger.error(f"Error in optimized invoice query: {e}")
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
async def generate_invoice_report(
    request: ReportRequest, background_tasks: BackgroundTasks, db=Depends(get_database)
):
    """
    Generate invoice report for the specified date range and brand.
    Returns an Excel file with the results.
    Optimized for better performance.
    """
    try:
        start_time = datetime.now()
        logger.info(
            f"Starting report generation for brand: {request.brand}, date range: {request.start_date} to {request.end_date}"
        )

        # Parse dates
        start_date = datetime.strptime(request.start_date, "%Y-%m-%d")
        end_date = datetime.strptime(request.end_date, "%Y-%m-%d")

        # STEP 1: Get SKUs (cached)
        sku_start = datetime.now()
        skus = get_skus_by_brand(db, request.brand)
        sku_duration = (datetime.now() - sku_start).total_seconds()
        logger.info(f"SKU lookup took {sku_duration:.2f} seconds")

        if not skus:
            raise HTTPException(
                status_code=404, detail=f"No SKUs found for brand '{request.brand}'"
            )

        # STEP 2: Query invoices (optimized single aggregation)
        query_start = datetime.now()
        invoice_data = query_invoices_for_skus(db, skus, start_date, end_date)
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
async def get_available_brands(db=Depends(get_database)):
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
