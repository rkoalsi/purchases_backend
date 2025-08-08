import pandas as pd
from io import BytesIO
from bson import ObjectId
from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from datetime import datetime, timedelta, timezone
from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document
import os
import requests
import json
import time, io
import gzip
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
import logging
from .amazon_vc import router as amazon_vendor_router
# --- Configuration ---
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ENV
load_dotenv()

SKU_COLLECTION = "amazon_sku_mapping"
SALES_COLLECTION = "amazon_sales_traffic"
INVENTORY_COLLECTION = "amazon_ledger"

router = APIRouter()

router.include_router(amazon_vendor_router,prefix='/vendor')

# SP API Configuration
REFRESH_TOKEN = os.getenv("SP_REFRESH_TOKEN")
GRANT_TYPE = os.getenv("SP_GRANT_TYPE", "refresh_token")
CLIENT_ID = os.getenv("SP_CLIENT_ID")
CLIENT_SECRET = os.getenv("SP_CLIENT_SECRET")
LOGIN_URL = os.getenv("SP_LOGIN_URL", "https://api.amazon.com/auth/o2/token")
MARKETPLACE_ID = os.getenv("SP_MARKETPLACE_ID", "A21TJRUUN4KGV")  # IN marketplace
SP_API_BASE_URL = os.getenv(
    "SP_API_BASE_URL", "https://sellingpartnerapi-na.amazon.com"
)

# Global variable to store access token and expiry
_sc_access_token = None
_sc_token_expires_at = None


def get_amazon_token() -> str:
    """
    Get a new access token from Amazon SP API using refresh token
    """
    global _sc_access_token, _sc_token_expires_at

    # Check if current token is still valid (with 5 minute buffer)
    if (
        _sc_access_token
        and _sc_token_expires_at
        and datetime.now() < (_sc_token_expires_at - timedelta(minutes=5))
    ):
        return _sc_access_token

    try:
        payload = {
            "grant_type": GRANT_TYPE,
            "refresh_token": REFRESH_TOKEN,
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
        }

        headers = {"Content-Type": "application/x-www-form-urlencoded"}

        response = requests.post(LOGIN_URL, data=payload, headers=headers)
        response.raise_for_status()

        token_data = response.json()
        _sc_access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
        _sc_token_expires_at = datetime.now() + timedelta(seconds=expires_in)

        logger.info("Successfully obtained new Amazon SP API token")
        return _sc_access_token

    except requests.exceptions.RequestException as e:
        logger.error(f"Error getting Amazon token: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to authenticate with Amazon SP API: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Unexpected error getting Amazon token: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred during authentication: {str(e)}",
        )


def make_sp_api_request(
    endpoint: str, method: str = "GET", params: Dict = None, data: Dict = None
) -> Dict:
    """
    Make a request to Amazon SP API with proper authentication
    """
    access_token = get_amazon_token()

    headers = {"x-amz-access-token": access_token, "Content-Type": "application/json"}

    url = f"{SP_API_BASE_URL}{endpoint}"

    try:
        if method.upper() == "GET":
            response = requests.get(url, headers=headers, params=params)
        elif method.upper() == "POST":
            response = requests.post(url, headers=headers, json=data, params=params)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")

        response.raise_for_status()
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"SP API request failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Amazon SP API request failed: {str(e)}",
        )


def create_report(
    report_type: str,
    start_date: str,
    end_date: str,
    marketplace_ids: List[str] = None,
    report_options: Dict = None,
) -> str:
    """
    Create a report request and return the report document ID
    """
    if marketplace_ids is None:
        marketplace_ids = [MARKETPLACE_ID]

    endpoint = "/reports/2021-06-30/reports"

    report_data = {
        "reportType": report_type,
        "dataStartTime": start_date,
        "dataEndTime": end_date,
        "marketplaceIds": marketplace_ids,
    }

    # Add report options if provided (for ledger reports)
    if report_options:
        report_data["reportOptions"] = report_options

    response = make_sp_api_request(endpoint, method="POST", data=report_data)
    return response.get("reportId")


def get_report_status(report_document_id: str) -> Dict:
    """
    Check the status of a report
    """
    endpoint = f"/reports/2021-06-30/reports/{report_document_id}"
    return make_sp_api_request(endpoint)


def download_report_data(report_document_id: str, is_gzipped: bool = False) -> str:
    """
    Download report data using the document ID
    """
    endpoint = f"/reports/2021-06-30/documents/{report_document_id}"
    document_info = make_sp_api_request(endpoint)

    # Get the download URL
    download_url = document_info.get("url")
    if not download_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No download URL found in report document",
        )

    # Download the actual report data
    response = requests.get(download_url)
    response.raise_for_status()

    # Handle gzipped content (for ledger reports)
    if is_gzipped:
        try:
            return gzip.decompress(response.content).decode("utf-8")
        except Exception as e:
            logger.error(f"Error decompressing gzipped content: {e}")
            # Fallback to regular content if not actually gzipped
            return response.text

    return response.text


def get_amazon_sales_traffic_data(
    start_date: str, end_date: str, db: Any, marketplace_ids: List[str] = None
) -> List[Dict]:
    """
    Fetch sales and traffic data from Amazon SP API (ASIN-wise daily data)
    """
    if marketplace_ids is None:
        marketplace_ids = [MARKETPLACE_ID]

    try:
        # Create sales and traffic report
        report_id = create_report(
            report_type="GET_SALES_AND_TRAFFIC_REPORT",
            start_date=start_date,
            end_date=end_date,
            marketplace_ids=marketplace_ids,
        )

        # Add validation for report_id
        if not report_id:
            raise ValueError("Failed to create report - no report ID returned")

        # Wait for report to be ready (with timeout)
        max_wait_time = 300  # 5 minutes
        wait_time = 0

        while wait_time < max_wait_time:
            report_status = get_report_status(report_id)

            # Add validation for report_status
            if not report_status:
                raise ValueError("Failed to get report status - empty response")

            processing_status = report_status.get("processingStatus")
            report_document_id = report_status.get("reportDocumentId")

            if processing_status == "DONE":
                break
            elif processing_status == "FATAL":
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Sales and traffic report processing failed",
                )

            time.sleep(10)
            wait_time += 10

        if wait_time >= max_wait_time:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail="Sales and traffic report processing timed out",
            )

        # Validate report_document_id before downloading
        if not report_document_id:
            raise ValueError("No report document ID available")

        # Download and parse report data (JSON format)
        report_data = download_report_data(report_document_id, is_gzipped=True)

        # Add validation for report_data
        if not report_data:
            raise ValueError("Downloaded report data is empty")

        # Parse JSON data with better error handling
        try:
            json_data = json.loads(report_data)
        except json.JSONDecodeError as json_error:
            raise ValueError(f"Failed to parse JSON report data: {json_error}")

        # Validate json_data structure
        if not isinstance(json_data, dict):
            raise ValueError("Invalid JSON structure - expected dictionary")

        # Extract ASIN-wise daily data
        sales_traffic_data = []

        # Process salesAndTrafficByAsin data
        if "salesAndTrafficByAsin" in json_data:
            asin_data_list = json_data["salesAndTrafficByAsin"]

            if not isinstance(asin_data_list, list):
                raise ValueError("salesAndTrafficByAsin is not a list")

            for asin_data in asin_data_list:
                asin = asin_data["parentAsin"]
                asin_data["date"] = datetime.fromisoformat(start_date)
                asin_data["created_at"] = datetime.now()
                result = db[SALES_COLLECTION].find_one(
                    {"parentAsin": asin, "date": asin_data["date"]}
                )
                if not result:
                    sales_traffic_data.append(asin_data)
        else:
            logger.warning("No 'salesAndTrafficByAsin' data found in report")

        return sales_traffic_data

    except HTTPException:
        # Re-raise HTTPExceptions as-is
        raise
    except ValueError as ve:
        logger.error(f"Validation error in sales and traffic data: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Data validation error: {str(ve)}",
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching sales and traffic data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch sales and traffic data: {str(e)}",
        )


def get_amazon_ledger_data(
    start_date: str,
    end_date: str,
    db: Any,
    marketplace_ids: List[str] = None,
    aggregate_by_location: str = "COUNTRY",
    aggregated_by_time_period: str = "DAILY",
) -> List[Dict]:
    print(start_date, end_date)
    """
    Fetch ledger summary data from Amazon SP API
    """
    if marketplace_ids is None:
        marketplace_ids = [MARKETPLACE_ID]

    try:
        # Report options for ledger data
        report_options = {
            "aggregateByLocation": aggregate_by_location,
            "aggregatedByTimePeriod": aggregated_by_time_period,
        }

        # Create ledger report
        report_id = create_report(
            report_type="GET_LEDGER_SUMMARY_VIEW_DATA",
            start_date=start_date,
            end_date=end_date,
            marketplace_ids=marketplace_ids,
            report_options=report_options,
        )

        # Wait for report to be ready (with timeout)
        max_wait_time = 300  # 5 minutes
        wait_time = 0
        report_document_id = ""
        while wait_time < max_wait_time:
            report_status = get_report_status(report_id)
            processing_status = report_status.get("processingStatus")
            report_document_id = report_status.get("reportDocumentId")

            if processing_status == "DONE":
                break
            elif processing_status == "FATAL":
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Ledger report processing failed",
                )

            time.sleep(10)
            wait_time += 10

        if wait_time >= max_wait_time:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail="Ledger report processing timed out",
            )

        # Download and parse report data (TSV format, gzipped)
        report_data = download_report_data(report_document_id, is_gzipped=True)

        # Convert TSV data to list of dictionaries
        df = pd.read_csv(BytesIO(report_data.encode("utf-8")), sep="\t")
        ledger_data = df.to_dict("records")
        # Add metadata
        normalized_data = []
        for record in ledger_data:
            normalized_record = {
                key.lower().replace(" ", "_"): value for key, value in record.items()
            }
            normalized_record["date"] = datetime.fromisoformat(start_date)
            normalized_record["created_at"] = datetime.now()
            normalized_record["report_type"] = "ledger"
            result = db.amazon_ledger.find_one(
                {
                    "date": normalized_record["date"],
                    "asin": normalized_record["asin"],
                    "location": normalized_record["location"],
                    "ending_warehouse_balance": normalized_record[
                        "ending_warehouse_balance"
                    ],
                }
            )
            if not result:
                normalized_data.append(normalized_record)

        return normalized_data

    except Exception as e:
        logger.error(f"Error fetching ledger data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch ledger data: {str(e)}",
        )


def get_amazon_inventory_data() -> List[Dict]:
    """
    Fetch inventory data from Amazon SP API
    """
    try:
        endpoint = "/fba/inventory/v1/summaries"
        params = {
            "granularityType": "Marketplace",
            "granularityId": MARKETPLACE_ID,
            "marketplaceIds": MARKETPLACE_ID,
        }

        response = make_sp_api_request(endpoint, params=params)
        inventory_summaries = response.get("inventorySummaries", [])

        # Add metadata
        for record in inventory_summaries:
            record["created_at"] = datetime.now()
            record["_report_type"] = "inventory"

        return inventory_summaries

    except Exception as e:
        logger.error(f"Error fetching inventory data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch inventory data: {str(e)}",
        )


async def insert_data_to_db(
    collection_name: str, data: List[Dict], db=Depends(get_database)
) -> int:
    """
    Insert data into MongoDB collection
    """
    try:
        if not data:
            return 0

        collection = db[collection_name]

        # Insert data
        result = collection.insert_many(data)
        logger.info(
            f"Inserted {len(result.inserted_ids)} records into {collection_name}"
        )

        return len(result.inserted_ids)

    except PyMongoError as e:
        logger.error(f"Database error inserting data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error: {str(e)}",
        )
    except Exception as e:
        logger.error(f"Unexpected error inserting data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {str(e)}",
        )


# API Endpoints


@router.post("/auth/token")
async def refresh_amazon_token():
    """
    Refresh Amazon SP API token
    """
    try:
        token = get_amazon_token()
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Token refreshed successfully",
                "token_expires_at": (
                    _sc_token_expires_at.isoformat() if _sc_token_expires_at else None
                ),
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.post("/sync/sales-traffic")
async def sync_sales_traffic_data(
    start_date: str,
    end_date: str,
    marketplace_ids: Optional[List[str]] = None,
    db=Depends(get_database),
):
    """
    Fetch ASIN-wise daily sales and traffic data from Amazon and insert into database
    Format: YYYY-MM-DD
    """
    try:
        # Validate date format
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        # Convert to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T23:59:59Z"

        # Fetch sales and traffic data
        sales_traffic_data = get_amazon_sales_traffic_data(
            start_datetime, end_datetime, db, marketplace_ids
        )
        # Insert into database
        inserted_count = await insert_data_to_db(
            SALES_COLLECTION, sales_traffic_data, db
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Sales and traffic data synced successfully",
                "records_inserted": inserted_count,
                "date_range": f"{start_date} to {end_date}",
                "marketplace_ids": marketplace_ids or [MARKETPLACE_ID],
            },
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD",
        )
    except Exception as e:
        logger.error(f"Error syncing sales and traffic data: {e}")
        raise


@router.post("/sync/ledger")
async def sync_ledger_data(
    start_date: str,
    end_date: str,
    marketplace_ids: Optional[List[str]] = None,
    aggregate_by_location: str = "FC",
    aggregated_by_time_period: str = "DAILY",
    db=Depends(get_database),
):
    """
    Fetch ledger summary data from Amazon and insert into database
    Format: YYYY-MM-DD
    """
    try:
        # Validate date format
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        # Convert to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T00:00:00Z"

        # Fetch ledger data
        ledger_data = get_amazon_ledger_data(
            start_datetime,
            end_datetime,
            db,
            marketplace_ids,
            aggregate_by_location,
            aggregated_by_time_period,
        )

        # Insert into database
        inserted_count = await insert_data_to_db(INVENTORY_COLLECTION, ledger_data, db)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Ledger data synced successfully",
                "records_inserted": inserted_count,
                "date_range": f"{start_date} to {end_date}",
                "marketplace_ids": marketplace_ids or [MARKETPLACE_ID],
                "aggregate_by_location": aggregate_by_location,
                "aggregated_by_time_period": aggregated_by_time_period,
            },
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD",
        )
    except Exception as e:
        logger.error(f"Error syncing ledger data: {e}")
        raise


@router.get("/status")
async def sync_status(
    start_date: str, end_date: str, report_type: str, db=Depends(get_database)
):
    try:
        # Parse date range
        start = datetime.strptime(start_date, "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
        )

        async def get_data_status(sales_collection, inventory_collection):
            """Helper function to get status for a collection pair"""
            # Get first and last sales dates in the date range
            sales_date_pipeline = [
                {
                    "$match": {
                        "date": {"$gte": start, "$lte": end}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "first_date": {"$min": "$date"},
                        "last_date": {"$max": "$date"}
                    }
                }
            ]
            
            sales_dates = list(db[sales_collection].aggregate(sales_date_pipeline))
            first_sales_date = None
            last_sales_date = None
            if sales_dates:
                first_sales_date = sales_dates[0]["first_date"].isoformat() if sales_dates[0]["first_date"] else None
                last_sales_date = sales_dates[0]["last_date"].isoformat() if sales_dates[0]["last_date"] else None

            # Get first and last inventory dates in the date range
            inventory_date_pipeline = [
                {
                    "$match": {
                        "date": {"$gte": start, "$lte": end}
                    }
                },
                {
                    "$group": {
                        "_id": None,
                        "first_date": {"$min": "$date"},
                        "last_date": {"$max": "$date"}
                    }
                }
            ]
            
            inventory_dates = list(db[inventory_collection].aggregate(inventory_date_pipeline))
            first_inventory_date = None
            last_inventory_date = None
            if inventory_dates:
                first_inventory_date = inventory_dates[0]["first_date"].isoformat() if inventory_dates[0]["first_date"] else None
                last_inventory_date = inventory_dates[0]["last_date"].isoformat() if inventory_dates[0]["last_date"] else None

            # Get total counts
            sales_count = db[sales_collection].count_documents({
                "date": {"$gte": start, "$lte": end}
            })
            inventory_count = db[inventory_collection].count_documents({
                "date": {"$gte": start, "$lte": end}
            })

            return {
                "sales_data": {
                    "records_count": sales_count,
                    "first_sales_date": first_sales_date,
                    "last_sales_date": last_sales_date,
                },
                "inventory_data": {
                    "records_count": inventory_count,
                    "first_inventory_date": first_inventory_date,
                    "last_inventory_date": last_inventory_date,
                }
            }

        # Handle different report types
        if report_type == "all":
            # Get both vendor_central and fba/seller_flex data
            vendor_status = await get_data_status("amazon_vendor_sales", "amazon_vendor_inventory")
            fba_status = await get_data_status(SALES_COLLECTION, INVENTORY_COLLECTION)
            
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "date_range": {
                        "start_date": start_date,
                        "end_date": end_date
                    },
                    "vendor_central": vendor_status,
                    "fba_seller_flex": fba_status,
                    "report_type": report_type,
                },
            )

        elif report_type == "vendor_central":
            status_data = await get_data_status("amazon_vendor_sales", "amazon_vendor_inventory")
        else:
            # fba, seller_flex, fba+seller_flex
            status_data = await get_data_status(SALES_COLLECTION, INVENTORY_COLLECTION)

        # For single report types, return the original structure
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "date_range": {
                    "start_date": start_date,
                    "end_date": end_date
                },
                "sales_data": status_data["sales_data"],
                "inventory_data": status_data["inventory_data"],
                "report_type": report_type,
            },
        )

    except Exception as e:
        logger.error(f"Error getting sync status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))
        
@router.get("/sales-traffic/{asin}")
async def get_asin_sales_data(
    asin: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db=Depends(get_database),
):
    """
    Get sales and traffic data for a specific ASIN
    """
    try:
        query = {"$or": [{"parentAsin": asin}, {"childAsin": asin}]}

        # Add date range filter if provided
        if start_date and end_date:
            query["date"] = {"$gte": start_date, "$lte": end_date}

        collection = db[SALES_COLLECTION]
        results = list(collection.find(query).sort("date", 1))

        # Serialize MongoDB documents
        serialized_results = [serialize_mongo_document(doc) for doc in results]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "asin": asin,
                "records_found": len(serialized_results),
                "data": serialized_results,
            },
        )

    except Exception as e:
        logger.error(f"Error fetching ASIN data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/ledger/summary")
async def get_ledger_summary(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    transaction_type: Optional[str] = None,
    db=Depends(get_database),
):
    """
    Get ledger summary data with optional filters
    """
    try:
        query = {}

        # Add date range filter if provided
        if start_date and end_date:
            query["Date"] = {"$gte": start_date, "$lte": end_date}

        # Add transaction type filter if provided
        if transaction_type:
            query["Transaction Type"] = transaction_type

        collection = db[INVENTORY_COLLECTION]
        results = list(collection.find(query).sort("Date", 1))

        # Serialize MongoDB documents
        serialized_results = [serialize_mongo_document(doc) for doc in results]

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "records_found": len(serialized_results),
                "filters": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "transaction_type": transaction_type,
                },
                "data": serialized_results,
            },
        )

    except Exception as e:
        logger.error(f"Error fetching ledger summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/get_amazon_sku_mapping")
async def get_sku_mapping(database=Depends(get_database)):
    """
    Retrieves all documents from the SKU mapping collection.
    """
    try:
        sku_collection = database.get_collection(SKU_COLLECTION)
        sku_documents = serialize_mongo_document(
            list(sku_collection.find({}).sort("item_name", -1))
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
    Expected columns: 'ASIN', 'SKU', 'Item Name'.
    """
    if not file.filename.endswith((".xlsx", ".xls")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid file type. Please upload an Excel file (.xlsx or .xls).",
        )

    try:
        file_content = await file.read()
        df = pd.read_excel(
            BytesIO(file_content), sheet_name="E-trade Inventory Tracker"
        )

        # Validate required columns
        required_cols = ["ASIN", "SKU", "Item Name"]
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
            item_id = str(row["ASIN"]).strip() if pd.notna(row["ASIN"]) else None
            sku_code = str(row["SKU"]).strip() if pd.notna(row["SKU"]) else None
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


async def generate_report_by_date_range(
    start_date: str, end_date: str, database: Any, report_type: str = "fba+seller_flex"
):
    try:
        start = datetime.strptime(start_date, "%Y-%m-%d").replace(
            hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc
        )
        end = datetime.strptime(end_date, "%Y-%m-%d").replace(
            hour=23, minute=59, second=59, microsecond=999999, tzinfo=timezone.utc
        )

        # Handle the new "all" report type
        if report_type == "all":
            # Get vendor_central data
            vendor_data = await generate_vendor_central_data(start, end, database)
            
            # Get fba+seller_flex data
            fba_seller_flex_data = await generate_fba_seller_flex_data(start, end, database, "fba+seller_flex")
            
            # Combine the data
            combined_data = combine_report_data(vendor_data, fba_seller_flex_data)
            
            return combined_data

        elif report_type == "vendor_central":
            return await generate_vendor_central_data(start, end, database)
        
        else:
            # Handle fba, seller_flex, fba+seller_flex
            return await generate_fba_seller_flex_data(start, end, database, report_type)

    except Exception as e:
        import traceback
        print(str(e))
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred generating Amazon report: {str(e)}",
        )


async def generate_vendor_central_data(start, end, database):
    """Generate vendor central report data"""
    vendor_pipeline = [
        {
            "$match": {
                "date": {"$gte": start, "$lte": end},
            }
        },
        {
            "$group": {
                "_id": {"asin": "$asin", "date": "$date"},
                "units_sold": {"$sum": "$orderedUnits"},
                "amount": {"$sum": "$orderedRevenue.amount"},
            }
        },
        {
            "$lookup": {
                "from": "amazon_vendor_inventory",
                "let": {"sales_asin": "$_id.asin", "sales_date": "$_id.date"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$asin", "$$sales_asin"]},
                                    {"$eq": ["$date", "$$sales_date"]},
                                ]
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": {"asin": "$asin", "date": "$date"},
                            "closing_stock": {
                                "$sum": "$sellableOnHandInventoryUnits"
                            },
                        }
                    },
                ],
                "as": "inventory_data",
            }
        },
        {"$addFields": {"inventory_data": {"$last": "$inventory_data"}}},
        {
            "$lookup": {
                "from": "amazon_sku_mapping",
                "localField": "_id.asin",
                "foreignField": "item_id",
                "as": "item_info",
            }
        },
        {"$addFields": {"item_info": {"$last": "$item_info"}}},
        {"$sort": {"_id.date": -1}},
        {
            "$group": {
                "_id": {"asin": "$_id.asin"},
                "total_units_sold": {"$sum": "$units_sold"},
                "total_amount": {"$sum": "$amount"},
                "last_day_closing_stock": {
                    "$first": {"$ifNull": ["$inventory_data.closing_stock", 0]}
                },
                "item_name": {"$last": "$item_info.item_name"},
                "sku_code": {"$last": "$item_info.sku_code"},
                "daily_data": {
                    "$push": {
                        "date": "$_id.date",
                        "closing_stock": {
                            "$ifNull": ["$inventory_data.closing_stock", 0]
                        },
                        "units_sold": "$units_sold",
                    }
                },
            }
        },
        {
            "$addFields": {
                "total_days_in_stock": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$ne": ["$daily_data", None]},
                                {"$gt": [{"$size": "$daily_data"}, 0]},
                            ]
                        },
                        "then": {
                            "$let": {
                                "vars": {
                                    "sorted_data": {
                                        "$sortArray": {
                                            "input": "$daily_data",
                                            "sortBy": {"date": 1},
                                        }
                                    }
                                },
                                "in": {
                                    "$size": {
                                        "$filter": {
                                            "input": "$$sorted_data",
                                            "cond": {
                                                "$gt": [
                                                    "$$this.closing_stock",
                                                    0,
                                                ]
                                            },
                                        }
                                    }
                                },
                            }
                        },
                        "else": 0,
                    }
                }
            }
        },
        {
            "$addFields": {
                "drr": {
                    "$cond": {
                        "if": {"$gt": ["$total_days_in_stock", 0]},
                        "then": {
                            "$divide": [
                                "$total_units_sold",
                                "$total_days_in_stock",
                            ]
                        },
                        "else": 0,
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "asin": "$_id.asin",
                "item_name": "$item_name",
                "sku_code": "$sku_code",
                "units_sold": "$total_units_sold",
                "closing_stock": "$last_day_closing_stock",
                "total_amount": {"$round": ["$total_amount", 2]},
                "stock": "$last_day_closing_stock",
                "total_days_in_stock": "$total_days_in_stock",
                "drr": {"$round": ["$drr", 2]},
                "data_source": {"$literal": "vendor_central"},  # Add source identifier
            }
        },
        {
            "$sort": {
                "sku_code": -1,
                "asin": -1,
            }
        },
    ]
    
    collection = database.get_collection("amazon_vendor_sales")
    cursor = list(collection.aggregate(vendor_pipeline))
    result = serialize_mongo_document(cursor)
    return result


async def generate_fba_seller_flex_data(start, end, database, report_type):
    """Generate FBA/Seller Flex report data"""
    ledger_match_conditions = [
        {"$eq": ["$asin", "$$sales_asin"]},
        {"$eq": ["$date", "$$sales_date"]},
        {"$eq": ["$disposition", "SELLABLE"]},
    ]

    if report_type == "fba":
        ledger_match_conditions.append({"$ne": ["$location", "VKSX"]})
    elif report_type == "seller_flex":
        ledger_match_conditions.append({"$eq": ["$location", "VKSX"]})

    base_pipeline = [
        {
            "$match": {
                "date": {"$gte": start, "$lte": end},
            }
        },
        {
            "$group": {
                "_id": {"asin": "$parentAsin", "date": "$date"},
                "units_sold": {"$sum": "$salesByAsin.unitsOrdered"},
                "amount": {"$sum": "$salesByAsin.orderedProductSales.amount"},
                "sessions": {
                    "$sum": {
                        "$add": [
                            {"$ifNull": ["$trafficByAsin.sessions", 0]},
                            {"$ifNull": ["$trafficByAsin.sessionsB2B", 0]},
                        ]
                    }
                },
            }
        },
        {
            "$lookup": {
                "from": "amazon_ledger",
                "let": {"sales_asin": "$_id.asin", "sales_date": "$_id.date"},
                "pipeline": [
                    {"$match": {"$expr": {"$and": ledger_match_conditions}}},
                    {
                        "$group": {
                            "_id": {"asin": "$asin", "date": "$date"},
                            "closing_stock": {"$sum": "$ending_warehouse_balance"},
                            "item_name": {"$last": "$title"},
                            "warehouses": {"$addToSet": "$location"},
                        }
                    },
                ],
                "as": "ledger_data",
            }
        },
        {"$addFields": {"ledger_data": {"$first": "$ledger_data"}}},
        {
            "$lookup": {
                "from": "amazon_sku_mapping",
                "localField": "_id.asin",
                "foreignField": "item_id",
                "as": "item_info",
            }
        },
        {"$addFields": {"item_info": {"$last": "$item_info"}}},
        {
            "$group": {
                "_id": {"asin": "$_id.asin"},
                "total_units_sold": {"$sum": "$units_sold"},
                "total_amount": {"$sum": "$amount"},
                "total_sessions": {"$sum": "$sessions"},
                "total_closing_stock": {
                    "$last": {"$ifNull": ["$ledger_data.closing_stock", 0]}
                },
                "item_name": {"$last": "$item_info.item_name"},
                "sku_code": {"$last": "$item_info.sku_code"},
                "all_warehouses": {"$addToSet": "$ledger_data.warehouses"},
                "daily_data": {
                    "$push": {
                        "date": "$_id.date",
                        "closing_stock": {
                            "$ifNull": ["$ledger_data.closing_stock", 0]
                        },
                        "units_sold": "$units_sold",
                        "sessions": "$sessions",
                    }
                },
            }
        },
        {
            "$addFields": {
                "total_days_in_stock": {
                    "$cond": {
                        "if": {
                            "$and": [
                                {"$ne": ["$daily_data", None]},
                                {"$gt": [{"$size": "$daily_data"}, 0]},
                            ]
                        },
                        "then": {
                            "$let": {
                                "vars": {
                                    "sorted_data": {
                                        "$sortArray": {
                                            "input": "$daily_data",
                                            "sortBy": {"date": 1},
                                        }
                                    }
                                },
                                "in": {
                                    "$size": {
                                        "$filter": {
                                            "input": "$$sorted_data",
                                            "cond": {
                                                "$gt": ["$$this.closing_stock", 0]
                                            },
                                        }
                                    }
                                },
                            }
                        },
                        "else": 0,
                    }
                },
                "warehouses": {
                    "$reduce": {
                        "input": "$all_warehouses",
                        "initialValue": [],
                        "in": {"$setUnion": ["$$value", "$$this"]},
                    }
                },
            }
        },
        {
            "$addFields": {
                "drr": {
                    "$cond": {
                        "if": {"$gt": ["$total_days_in_stock", 0]},
                        "then": {
                            "$divide": ["$total_units_sold", "$total_days_in_stock"]
                        },
                        "else": 0,
                    }
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "asin": "$_id.asin",
                "sku_code": "$sku_code",
                "item_name": "$item_name",
                "warehouses": "$warehouses",
                "units_sold": "$total_units_sold",
                "total_amount": "$total_amount",
                "sessions": "$total_sessions",
                "closing_stock": "$total_closing_stock",
                "total_days_in_stock": "$total_days_in_stock",
                "drr": {"$round": ["$drr", 2]},
                "data_source": {"$literal": report_type},  # Add source identifier
            }
        },
        {
            "$sort": {
                "sku_code": -1,
                "asin": -1,
            }
        },
    ]

    collection = database.get_collection(SALES_COLLECTION)
    cursor = list(collection.aggregate(base_pipeline))
    result = serialize_mongo_document(cursor)
    return result


def combine_report_data(vendor_data, fba_seller_flex_data):
    """Combine vendor central and fba/seller flex data"""
    # Create lookup dictionaries
    vendor_lookup = {}
    fba_lookup = {}
    
    # Index vendor data by sku_code and asin
    for item in vendor_data:
        key = (item.get('sku_code'), item.get('asin'))
        vendor_lookup[key] = item
    
    # Index fba data by sku_code and asin
    for item in fba_seller_flex_data:
        key = (item.get('sku_code'), item.get('asin'))
        fba_lookup[key] = item
    
    combined_results = []
    processed_keys = set()
    
    # Combine matching records
    for key in vendor_lookup:
        if key in fba_lookup:
            vendor_item = vendor_lookup[key]
            fba_item = fba_lookup[key]
            
            combined_item = {
                "asin": vendor_item.get('asin') or fba_item.get('asin'),
                "sku_code": vendor_item.get('sku_code') or fba_item.get('sku_code'),
                "item_name": vendor_item.get('item_name') or fba_item.get('item_name'),
                "warehouses": fba_item.get('warehouses', []),  # Only from FBA data
                "units_sold": (vendor_item.get('units_sold', 0) + fba_item.get('units_sold', 0)),
                "total_amount": round((vendor_item.get('total_amount', 0) + fba_item.get('total_amount', 0)), 2),
                "sessions": fba_item.get('sessions', 0),  # Only from FBA data
                "closing_stock": (vendor_item.get('closing_stock', 0) + fba_item.get('closing_stock', 0)),
                "stock": (vendor_item.get('stock', 0) + fba_item.get('closing_stock', 0)),  # Combined stock
                "total_days_in_stock": max(vendor_item.get('total_days_in_stock', 0), 
                                         fba_item.get('total_days_in_stock', 0)),
                "drr": 0,  # Will be recalculated
                "data_source": "combined",
            }
            
            # Recalculate DRR
            if combined_item['total_days_in_stock'] > 0:
                combined_item['drr'] = round(combined_item['units_sold'] / combined_item['total_days_in_stock'], 2)
            
            combined_results.append(combined_item)
            processed_keys.add(key)
    
    # Add vendor-only records
    for key, vendor_item in vendor_lookup.items():
        if key not in processed_keys:
            # Add missing fields with default values
            vendor_item['warehouses'] = []
            vendor_item['sessions'] = 0
            if 'stock' not in vendor_item:
                vendor_item['stock'] = vendor_item.get('closing_stock', 0)
            vendor_item['data_source'] = 'vendor_only'
            combined_results.append(vendor_item)
    
    # Add FBA-only records
    for key, fba_item in fba_lookup.items():
        if key not in processed_keys:
            # Add missing fields with default values
            fba_item['stock'] = fba_item.get('closing_stock', 0)
            fba_item['data_source'] = 'fba_only'
            combined_results.append(fba_item)
    
    # Sort the results
    combined_results.sort(
        key=lambda x: (
            x.get('sku_code') or '', 
            x.get('asin') or ''
        ), 
        reverse=True
    )
    
    return combined_results


@router.get("/get_report_data_by_date_range")
async def get_report_data_by_date_range(
    start_date: str,
    end_date: str,
    report_type: str = "fba+seller_flex",
    database=Depends(get_database),
):
    try:
        # Updated valid types to include "all"
        valid_types = ["fba+seller_flex", "fba", "seller_flex", "vendor_central", "all"]
        if report_type not in valid_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid type. Must be one of: {', '.join(valid_types)}",
            )

        report_response = await generate_report_by_date_range(
            start_date=start_date,
            end_date=end_date,
            database=database,
            report_type=report_type,
        )
        print(f"Retrieved dynamic report data of type: {report_type}")
        return report_response

    except HTTPException as e:
        raise e
    except Exception as e:
        print(f"Error retrieving report data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred retrieving the report data: {e}",
        )
        
def format_column_name(column_name):
    """Convert snake_case column names to proper formatted names"""
    # Replace underscores with spaces and title case
    formatted = column_name.replace("_", " ").title()

    # Handle specific cases for better formatting
    replacements = {
        "Asin": "ASIN",
        "Sku Code": "SKU Code",
        "Item Name": "Item Name",
        "Units Sold": "Units Sold",
        "Total Amount": "Total Amount",
        "Closing Stock": "Closing Stock",
        "Sessions": "Sessions",
        "Warehouses": "Warehouses",
    }

    return replacements.get(formatted, formatted)

@router.get("/download_report_by_date_range")
async def download_report_by_date_range(
    start_date: str,
    end_date: str,
    report_type: str = "fba+seller_flex",  # "fba+seller_flex", "fba", "seller_flex", "vendor_central", or "all"
    database=Depends(get_database),
):

    try:
        # Validate report_type parameter - Updated to include "all"
        valid_types = ["fba+seller_flex", "fba", "seller_flex", "vendor_central", "all"]
        if report_type not in valid_types:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid report_type. Must be one of: {', '.join(valid_types)}",
            )

        # Generate the report data using existing function
        report_data = await generate_report_by_date_range(
            start_date=start_date,
            end_date=end_date,
            database=database,
            report_type=report_type,
        )

        if not report_data:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No data found for the specified date range and report type",
            )

        # Convert to DataFrame
        df = pd.DataFrame(report_data)

        # Format column names
        column_mapping = {}
        for col in df.columns:
            column_mapping[col] = format_column_name(col)

        df = df.rename(columns=column_mapping)

        # Handle the warehouses column (convert list to string) - only for non-vendor reports
        if "Warehouses" in df.columns:
            df["Warehouses"] = df["Warehouses"].apply(
                lambda x: ", ".join(x) if isinstance(x, list) else str(x)
            )

        # Reorder columns for better presentation
        # Different column orders for different report types
        if report_type == "vendor_central":
            preferred_order = [
                "ASIN",
                "SKU Code",
                "Item Name",
                "Units Sold",
                "Total Amount",
                "Closing Stock",
                "Stock",  # vendor_central includes both closing_stock and stock
                "Total Days In Stock",
                "Drr",
            ]
        elif report_type == "all":
            # Combined report includes all fields from both vendor and FBA data
            preferred_order = [
                "ASIN",
                "SKU Code",
                "Item Name",
                "Warehouses",
                "Units Sold",
                "Total Amount",
                "Sessions",
                "Closing Stock",
                "Stock",  # Combined reports include both closing_stock and stock
                "Total Days In Stock",
                "Drr",
                "Data Source",  # Show which source the data came from
            ]
        else:
            # FBA, Seller Flex, and FBA+Seller Flex reports
            preferred_order = [
                "ASIN",
                "SKU Code",
                "Item Name",
                "Warehouses",
                "Units Sold",
                "Total Amount",
                "Sessions",
                "Closing Stock",
                "Total Days In Stock",
                "Drr",
            ]

        # Only include columns that exist in the DataFrame
        column_order = [col for col in preferred_order if col in df.columns]
        # Add any remaining columns not in preferred order
        remaining_cols = [col for col in df.columns if col not in column_order]
        final_column_order = column_order + remaining_cols

        df = df[final_column_order]

        # Create Excel file in memory
        excel_buffer = io.BytesIO()

        with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Report Data", index=False)

            # Get the workbook and worksheet for formatting
            workbook = writer.book
            worksheet = writer.sheets["Report Data"]

            # Auto-adjust column widths
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter

                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass

                adjusted_width = min(max_length + 2, 50)  # Cap at 50 characters
                worksheet.column_dimensions[column_letter].width = adjusted_width

            # Style the header row
            from openpyxl.styles import Font, PatternFill

            header_font = Font(bold=True, color="FFFFFF")
            header_fill = PatternFill(
                start_color="366092", end_color="366092", fill_type="solid"
            )

            for cell in worksheet[1]:
                cell.font = header_font
                cell.fill = header_fill

            # Special formatting for "all" report type - color code rows by data source
            if report_type == "all" and "Data Source" in df.columns:
                from openpyxl.styles import PatternFill
                
                # Define colors for different data sources
                color_mapping = {
                    "combined": PatternFill(start_color="E8F5E8", end_color="E8F5E8", fill_type="solid"),     # Light green
                    "vendor_only": PatternFill(start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"),  # Light yellow
                    "fba_only": PatternFill(start_color="E1F5FE", end_color="E1F5FE", fill_type="solid"),     # Light blue
                }
                
                # Apply color coding to rows based on data source
                data_source_col_index = df.columns.get_loc("Data Source") + 1  # +1 because Excel is 1-indexed
                
                for row_num in range(2, len(df) + 2):  # Start from row 2 (after header)
                    data_source_value = worksheet.cell(row=row_num, column=data_source_col_index).value
                    row_fill = color_mapping.get(data_source_value)
                    
                    if row_fill:
                        for col_num in range(1, len(df.columns) + 1):
                            worksheet.cell(row=row_num, column=col_num).fill = row_fill

        excel_buffer.seek(0)

        # Generate filename with timestamp and report type
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        type_suffix = f"_{report_type}" if report_type != "fba+seller_flex" else ""
        filename = (
            f"inventory_report_{start_date}_to_{end_date}{type_suffix}_{timestamp}.xlsx"
        )

        # Create streaming response
        response = StreamingResponse(
            io.BytesIO(excel_buffer.read()),
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )

        print(f"Generated Excel report for report_type: {report_type}, rows: {len(df)}")
        return response

    except HTTPException as e:
        print(str(e))
        raise e
    except Exception as e:
        print(f"Error generating Excel report: {e}")
        print(str(e))
        import traceback

        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred generating the Excel report: {e}",
        )


# Updated format_column_name function to handle the new "Data Source" column
def format_column_name(column_name):
    """Convert snake_case column names to proper formatted names"""
    # Replace underscores with spaces and title case
    formatted = column_name.replace("_", " ").title()

    # Handle specific cases for better formatting
    replacements = {
        "Asin": "ASIN",
        "Sku Code": "SKU Code",
        "Item Name": "Item Name",
        "Units Sold": "Units Sold",
        "Total Amount": "Total Amount",
        "Closing Stock": "Closing Stock",
        "Sessions": "Sessions",
        "Warehouses": "Warehouses",
        "Data Source": "Data Source",
        "Total Days In Stock": "Total Days In Stock",
        "Drr": "DRR",
        "Stock": "Stock",
    }

    return replacements.get(formatted, formatted)