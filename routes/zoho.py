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
    db,
    skus: List[str],
    start_date: datetime,
    end_date: datetime,
    exclude_customers: bool,
) -> List[dict]:
    """
    Optimized query that processes all SKUs in a single aggregation pipeline.
    This is the biggest performance improvement.
    """
    customer_list = [
        "(amzb2b) Pupscribe Enterprises Pvt Ltd",
        "Pupscribe Enterprises Private Limited",
        "(OSAMP) Office samples",
        "(PUPEV) PUPSCRIBE EVENTS",
        "(SSAM) Sales samples",
        "(RS) Retail samples",
        "Pupscribe Enterprises Private Limited (Blinkit Haryana)",
        "Pupscribe Enterprises Private Limited (Blinkit Karnataka)",
    ]
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
                    "customer_name": {"$nin": customer_list},
                    # Match any line item that has any of our target SKUs
                    "line_items": {
                        "$elemMatch": {
                            "item_custom_fields": {
                                "$elemMatch": {"value": {"$in": skus}}
                            }
                        }
                    },
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
                    # Match any line item that has any of our target SKUs
                    "line_items": {
                        "$elemMatch": {
                            "item_custom_fields": {
                                "$elemMatch": {"value": {"$in": skus}}
                            }
                        }
                    },
                }
            }
        )
        # Single aggregation pipeline that handles ALL SKUs at once
        pipeline = [
            # Stage 1: Match documents by date range and status
            match_statement,
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
        invoice_data = query_invoices_for_skus(
            db, skus, start_date, end_date, request.exclude_customers
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
    total_amount: float
    closing_stock: int
    total_days_in_stock: int
    drr: float


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


@router.get("/sales-report")
async def get_sales_report_fast(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    db=Depends(get_database),
):
    """
    Ultra-optimized sales report that runs in 5-15 seconds instead of 60 seconds.
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

        # Check cache first
        # if use_cache:
        #     cache_key = f"sales_report_{start_date}_{end_date}"
        #     cached_result = await get_cached_report(cache_key, db)
        #     if cached_result:
        #         logger.info(f"Returning cached report for {start_date} to {end_date}")
        #         cached_result["meta"]["from_cache"] = True
        #         return JSONResponse(content=cached_result)

        logger.info(
            f"Generating ultra-fast sales report for {start_date} to {end_date}"
        )
        start_time = datetime.now()

        # OPTIMIZATION 1: Run stock and sales aggregations in parallel
        stock_task = asyncio.create_task(
            fetch_stock_data_optimized(db, start_datetime, end_datetime)
        )
        products_task = asyncio.create_task(fetch_all_products_indexed(db))

        # OPTIMIZATION 2: Use more efficient customer filtering
        excluded_customers = get_excluded_customer_list()
        print(excluded_customers)
        # OPTIMIZATION 3: Optimized main pipeline
        invoices_collection = db[INVOICES_COLLECTION]

        # Build the pipeline with all optimizations
        pipeline = build_optimized_pipeline(start_date, end_date, excluded_customers)

        # Execute main aggregation
        logger.info("Executing main aggregation pipeline...")
        result_cursor = invoices_collection.aggregate(
            pipeline,
            allowDiskUse=True,
            batchSize=1000,  # Larger batch size for better performance
        )

        # Get parallel task results
        stock_data, products_map = await asyncio.gather(stock_task, products_task)

        logger.info(
            f"Processing {len(stock_data)} stock items and {len(products_map)} products"
        )

        # Process results efficiently
        sales_report_items = []
        total_units = 0
        total_amount = 0.0
        total_closing_stock = 0

        # Process in batches for better memory management
        batch = []
        batch_size = 100

        for item in result_cursor:
            batch.append(item)
            if len(batch) >= batch_size:
                processed = process_batch(batch, stock_data, products_map)
                sales_report_items.extend(processed["items"])
                total_units += processed["units"]
                total_amount += processed["amount"]
                total_closing_stock += processed["stock"]
                batch = []

        # Process remaining items
        if batch:
            processed = process_batch(batch, stock_data, products_map)
            sales_report_items.extend(processed["items"])
            total_units += processed["units"]
            total_amount += processed["amount"]
            total_closing_stock += processed["stock"]

        # Sort results
        sales_report_items.sort(key=lambda x: x.item_name)

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Ultra-fast report generated in {execution_time:.2f} seconds with {len(sales_report_items)} items"
        )

        # Calculate summary
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

        # Prepare response
        response_data = {
            "data": [item.dict() for item in sales_report_items],
            "summary": {
                "total_items": len(sales_report_items),
                "total_units_sold": total_units,
                "total_amount": round(total_amount, 2),
                "total_closing_stock": total_closing_stock,
                "average_drr": round(avg_drr, 2),
                "items_with_stock": items_with_stock,
                "items_out_of_stock": items_out_of_stock,
                "date_range": {"start_date": start_date, "end_date": end_date},
            },
            "meta": {
                "timestamp": datetime.now().isoformat(),
                "execution_time_seconds": round(execution_time, 2),
                "query_type": "ultra_fast_sales_report",
                "from_cache": False,
                "optimizations": [
                    "parallel_aggregations",
                    "indexed_products",
                    "batch_processing",
                    "optimized_customer_filter",
                ],
            },
        }

        # # Cache the result for future use
        # if use_cache:
        #     await cache_report(cache_key, response_data, db)

        return JSONResponse(content=response_data)

    except Exception as e:
        logger.error(f"Error in ultra-fast sales report: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


def build_optimized_pipeline(
    start_date: str, end_date: str, excluded_customers: str
) -> List[Dict]:
    """
    Build an optimized aggregation pipeline with better filtering and projection.
    """
    return [
        # CRITICAL: Use compound match for better index utilization
        {
            "$match": {
                "$and": [
                    {"date": {"$gte": start_date, "$lte": end_date}},
                    {"status": {"$nin": ["draft", "void"]}},
                    # More efficient customer filtering
                  {
                        "customer_name": {
                            "$not": {
                                "$regex": excluded_customers,
                                "$options": "i"  # case insensitive
                            }
                        }
                    },

                ]
            }
        },
        # Early projection to reduce document size
        {"$project": {"line_items": 1, "_id": 0}},
        # Unwind and group
        {"$unwind": "$line_items"},
        {
            "$group": {
                "_id": "$line_items.item_id",
                "item_name": {"$first": "$line_items.name"},
                "total_units_sold": {"$sum": "$line_items.quantity"},
                "total_amount": {"$sum": "$line_items.item_total"},
            }
        },
        # Don't do lookups here - we'll join with pre-fetched data in memory
    ]


async def fetch_stock_data_optimized(
    db, start_datetime: datetime, end_datetime: datetime
) -> Dict:
    """
    Optimized stock data fetching with better aggregation pipeline.
    """
    try:
        stock_collection = db["zoho_stock"]

        # More efficient pipeline
        pipeline = [
            {"$match": {"date": {"$gte": start_datetime, "$lte": end_datetime}}},
            # Group first, then calculate
            {
                "$group": {
                    "_id": {
                        "item_id": "$zoho_item_id",
                        "date": {
                            "$dateToString": {"format": "%Y-%m-%d", "date": "$date"}
                        },
                    },
                    "daily_stock": {"$last": "$stock"},  # Last entry of the day
                }
            },
            # Re-group to get item-level stats
            {
                "$group": {
                    "_id": "$_id.item_id",
                    "closing_stock": {
                        "$last": "$daily_stock"
                    },  # Most recent day's stock
                    "stock_days": {
                        "$sum": {"$cond": [{"$gt": ["$daily_stock", 0]}, 1, 0]}
                    },
                    "all_daily_stocks": {"$push": "$daily_stock"},
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "closing_stock": 1,
                    "total_days_in_stock": "$stock_days",
                }
            },
        ]

        # Use cursor with batch processing
        cursor = stock_collection.aggregate(
            pipeline,
            allowDiskUse=True,
            batchSize=5000,  # Large batch for better performance
        )

        # Convert to dictionary efficiently
        stock_data = {}
        for doc in cursor:
            stock_data[doc["_id"]] = {
                "closing_stock": doc.get("closing_stock", 0),
                "total_days_in_stock": doc.get("total_days_in_stock", 0),
            }

        logger.info(f"Fetched {len(stock_data)} stock records")
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

        # Simple projection to get only what we need
        cursor = products_collection.find(
            {
                "$and": [
                    {"name": {"$ne": ""}},           # Not empty string
                    {"name": {"$ne": "amazon"}},     # Not exactly "amazon" (case sensitive)
                ]
            },
            {"item_id": 1, "cf_sku_code": 1, "name": 1, "_id": 0},  # Include name for debugging
            batch_size=5000,
        )


        # Create indexed map
        products_map = {}
        for product in cursor:
            products_map[product.get("item_id")] = product.get("cf_sku_code", "")

        logger.info(f"Indexed {len(products_map)} products")
        return products_map

    except Exception as e:
        logger.error(f"Error fetching products: {e}")
        return {}


def process_batch(batch: List[Dict], stock_data: Dict, products_map: Dict) -> Dict:
    """
    Process a batch of items efficiently.
    """
    items = []
    total_units = 0
    total_amount = 0
    total_stock = 0

    for item in batch:
        item_id = item["_id"]
        units_sold = item.get("total_units_sold", 0)
        amount = item.get("total_amount", 0.0)

        # Get SKU from pre-fetched map (O(1) lookup)
        sku_code = products_map.get(item_id, "")

        # Get stock data from pre-fetched map (O(1) lookup)
        stock_info = stock_data.get(item_id, {})
        closing_stock = stock_info.get("closing_stock", 0)
        days_in_stock = stock_info.get("total_days_in_stock", 0)

        # Calculate DRR
        drr = round(units_sold / days_in_stock, 2) if days_in_stock > 0 else 0

        total_units += units_sold
        total_amount += amount
        total_stock += closing_stock
        
        if item.get('item_name') != '':
            items.append(
                SalesReportItem(
                    item_name=item.get("item_name", ""),
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
        "EC",
        "NA",
        "amzb2b",
        "amz2b2",
        "PUPEV",
        "RS",
        "MKT",
        "SPUR",
        "SSAM",
        "OSAM",
        "Blinkit",
        "ETRADE"
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

        # Find cached report
        cached = cache_collection.find_one(
            {
                "_id": cache_key,
                "created_at": {"$gte": datetime.now() - timedelta(hours=1)},
            }
        )

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

        cache_collection.replace_one(
            {"_id": cache_key},
            {"_id": cache_key, "data": data, "created_at": datetime.now()},
            upsert=True,
        )

        logger.info(f"Report cached with key: {cache_key}")

    except Exception as e:
        logger.warning(f"Failed to cache report: {e}")


@router.get("/sales-report/download")
async def download_sales_report(
    start_date: str = Query(..., description="Start date in YYYY-MM-DD format"),
    end_date: str = Query(..., description="End date in YYYY-MM-DD format"),
    db=Depends(get_database),
):
    """
    Download sales report as XLSX file for the given date range.
    Uses the same optimized approach as the main sales report API.

    Returns an Excel file with two sheets:
    - Sales Data: Detailed item-wise sales information
    - Summary: Aggregated summary statistics
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

        logger.info(f"Generating optimized sales report download for {start_date} to {end_date}")
        start_time = datetime.now()

        # OPTIMIZATION 1: Run stock and sales aggregations in parallel (same as fast API)
        stock_task = asyncio.create_task(
            fetch_stock_data_optimized(db, start_datetime, end_datetime)
        )
        products_task = asyncio.create_task(fetch_all_products_indexed(db))

        # OPTIMIZATION 2: Use more efficient customer filtering (same as fast API)
        excluded_customers = get_excluded_customer_list()
        print(excluded_customers)
        # OPTIMIZATION 3: Use the same optimized pipeline as fast API
        invoices_collection = db[INVOICES_COLLECTION]
        pipeline = build_optimized_pipeline(start_date, end_date, excluded_customers)

        # Execute main aggregation
        logger.info("Executing optimized aggregation pipeline for download...")
        result_cursor = invoices_collection.aggregate(
            pipeline,
            allowDiskUse=True,
            batchSize=1000,  # Same as fast API
        )

        # Get parallel task results
        stock_data, products_map = await asyncio.gather(stock_task, products_task)

        logger.info(
            f"Processing {len(stock_data)} stock items and {len(products_map)} products for download"
        )

        # Process results efficiently using same batch logic as fast API
        sales_report_items = []
        total_units = 0
        total_amount = 0.0
        total_closing_stock = 0

        # Process in batches for better memory management (same as fast API)
        batch = []
        batch_size = 100

        for item in result_cursor:
            batch.append(item)
            if len(batch) >= batch_size:
                processed = process_batch(batch, stock_data, products_map)
                sales_report_items.extend(processed["items"])
                total_units += processed["units"]
                total_amount += processed["amount"]
                total_closing_stock += processed["stock"]
                batch = []

        # Process remaining items
        if batch:
            processed = process_batch(batch, stock_data, products_map)
            sales_report_items.extend(processed["items"])
            total_units += processed["units"]
            total_amount += processed["amount"]
            total_closing_stock += processed["stock"]

        # Sort results (same as fast API)
        sales_report_items.sort(key=lambda x: x.item_name)

        execution_time = (datetime.now() - start_time).total_seconds()
        logger.info(
            f"Optimized download data prepared in {execution_time:.2f} seconds with {len(sales_report_items)} items"
        )

        # Calculate summary statistics (same logic as fast API)
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

        # Create Excel file
        import io
        import xlsxwriter

        # Create a BytesIO buffer
        output = io.BytesIO()

        # Create workbook and worksheets
        workbook = xlsxwriter.Workbook(output, {"in_memory": True})

        # Define formats
        header_format = workbook.add_format(
            {
                "bold": True,
                "bg_color": "#D7E4BC",
                "border": 1,
                "align": "center",
                "valign": "vcenter",
            }
        )

        data_format = workbook.add_format(
            {"border": 1, "align": "left", "valign": "vcenter"}
        )

        number_format = workbook.add_format(
            {"border": 1, "align": "right", "valign": "vcenter", "num_format": "#,##0"}
        )

        currency_format = workbook.add_format(
            {
                "border": 1,
                "align": "right",
                "valign": "vcenter",
                "num_format": "₹#,##0.00",
            }
        )

        decimal_format = workbook.add_format(
            {"border": 1, "align": "right", "valign": "vcenter", "num_format": "0.00"}
        )

        # Create Sales Data worksheet
        sales_sheet = workbook.add_worksheet("Sales Data")

        # Write headers for sales data
        headers = [
            "Product Name",
            "SKU Code",
            "Units Sold",
            "Total Amount (₹)",
            "Closing Stock",
            "Days in Stock",
            "DRR",
        ]

        for col, header in enumerate(headers):
            sales_sheet.write(0, col, header, header_format)

        # Write sales data using the optimized data structure
        for row, item in enumerate(sales_report_items, 1):
            sales_sheet.write(row, 0, item.item_name, data_format)
            sales_sheet.write(row, 1, item.sku_code, data_format)
            sales_sheet.write(row, 2, item.units_sold, number_format)
            sales_sheet.write(row, 3, item.total_amount, currency_format)
            sales_sheet.write(row, 4, item.closing_stock, number_format)
            sales_sheet.write(row, 5, item.total_days_in_stock, number_format)
            sales_sheet.write(row, 6, item.drr, decimal_format)

        # Auto-adjust column widths
        sales_sheet.set_column("A:A", 30)  # Product Name
        sales_sheet.set_column("B:B", 15)  # SKU Code
        sales_sheet.set_column("C:C", 12)  # Units Sold
        sales_sheet.set_column("D:D", 15)  # Total Amount
        sales_sheet.set_column("E:E", 12)  # Closing Stock
        sales_sheet.set_column("F:F", 12)  # Days in Stock
        sales_sheet.set_column("G:G", 10)  # DRR

        # Create Summary worksheet
        summary_sheet = workbook.add_worksheet("Summary")

        # Summary data (same calculations as fast API)
        summary_data = [
            ["Report Period", f"{start_date} to {end_date}"],
            ["Generated On", datetime.now().strftime("%Y-%m-%d %H:%M:%S")],
            ["Execution Time", f"{execution_time:.2f} seconds"],
            ["Processing Method", "Optimized (Same as Fast API)"],
            ["", ""],
            ["SALES SUMMARY", ""],
            ["Total Items", len(sales_report_items)],
            ["Total Units Sold", total_units],
            ["Total Amount (₹)", total_amount],
            ["Total Closing Stock", total_closing_stock],
            ["Average DRR", round(avg_drr, 2)],
            ["", ""],
            ["STOCK SUMMARY", ""],
            ["Items with Stock", items_with_stock],
            ["Items Out of Stock", items_out_of_stock],
            [
                "Stock Coverage %",
                f"{(items_with_stock / len(sales_report_items) * 100):.1f}%" if sales_report_items else "0.0%",
            ],
        ]

        # Write summary data
        for row, (label, value) in enumerate(summary_data):
            if label == "" or label in ["SALES SUMMARY", "STOCK SUMMARY"]:
                summary_sheet.write(row, 0, label, header_format)
                summary_sheet.write(row, 1, value, header_format)
            else:
                summary_sheet.write(row, 0, label, data_format)
                if isinstance(value, (int, float)) and label not in [
                    "Generated On",
                    "Execution Time",
                    "Processing Method",
                ]:
                    if "Amount" in label:
                        summary_sheet.write(row, 1, value, currency_format)
                    elif "DRR" in label:
                        summary_sheet.write(row, 1, value, decimal_format)
                    else:
                        summary_sheet.write(row, 1, value, number_format)
                else:
                    summary_sheet.write(row, 1, str(value), data_format)

        # Auto-adjust column widths for summary
        summary_sheet.set_column("A:A", 25)
        summary_sheet.set_column("B:B", 20)

        # Close the workbook
        workbook.close()

        # Get the value from the BytesIO buffer
        output.seek(0)

        # Generate filename
        filename = f"sales_report_{start_date}_to_{end_date}.xlsx"

        # Create response
        from fastapi.responses import Response

        response = Response(
            content=output.getvalue(),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
        )

        logger.info(f"Optimized sales report Excel file generated: {filename}")
        return response

    except PyMongoError as e:
        logger.error(f"MongoDB Error in optimized sales report download: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error occurred while generating sales report download: {str(e)}",
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error generating optimized sales report download: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred while generating the sales report download: {str(e)}",
        )

from functools import lru_cache
from typing import Optional, Dict, Any
import asyncio


@router.get("/data-metadata")
async def get_data_metadata(
    start_date: str = Query(
        None, description="Start date in YYYY-MM-DD format (optional)"
    ),
    end_date: str = Query(None, description="End date in YYYY-MM-DD format (optional)"),
    db=Depends(get_database),
):
    """
    Optimized metadata retrieval for sales and inventory collections.
    """

    try:
        logger.info(f"Fetching optimized data metadata")
        if start_date and end_date:
            logger.info(f"Date range filter: {start_date} to {end_date}")

        # Validate dates if provided
        start_datetime = None
        end_datetime = None
        if start_date and end_date:
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
        stock_collection = db["zoho_stock"]

        # OPTIMIZATION: Run both aggregations in parallel
        sales_task = asyncio.create_task(
            get_sales_metadata_optimized(
                invoices_collection, start_date, end_date, start_datetime, end_datetime
            )
        )
        inventory_task = asyncio.create_task(
            get_inventory_metadata_optimized(
                stock_collection, start_datetime, end_datetime
            )
        )

        # Wait for both tasks to complete
        sales_metadata, inventory_metadata = await asyncio.gather(
            sales_task, inventory_task
        )

        # Prepare response
        metadata = {
            "sales_data": sales_metadata,
            "inventory_data": inventory_metadata,
            "date_range": {
                "start_date": start_date,
                "end_date": end_date,
                "filtered": bool(start_date and end_date),
            },
            "last_updated": datetime.now().isoformat(),
        }

        return JSONResponse(
            content={
                "data": metadata,
                "meta": {
                    "timestamp": datetime.now().isoformat(),
                    "query_type": "optimized_data_metadata",
                    "date_filtered": bool(start_date and end_date),
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
    collection, start_datetime: Optional[datetime], end_datetime: Optional[datetime]
) -> Dict[str, Any]:
    """
    Optimized inventory metadata retrieval.
    """
    try:
        # Build match condition
        match_condition = {}
        if start_datetime and end_datetime:
            match_condition = {"date": {"$gte": start_datetime, "$lte": end_datetime}}

        # OPTIMIZATION: Single aggregation for all metrics
        pipeline = [
            {"$match": match_condition} if match_condition else {"$match": {}},
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
