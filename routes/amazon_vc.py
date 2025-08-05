from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from pymongo.errors import PyMongoError
from ..database import get_database
import os
import requests
import json
import time, io
import gzip
from typing import Optional, Dict, Any, List
from dotenv import load_dotenv
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ENV
load_dotenv()

SKU_COLLECTION = "amazon_sku_mapping"
SALES_COLLECTION = "amazon_vendor_sales"
INVENTORY_COLLECTION = "amazon_vendor_inventory"

router = APIRouter()

# VC API Configuration
REFRESH_TOKEN = os.getenv("VC_REFRESH_TOKEN")
GRANT_TYPE = os.getenv("VC_GRANT_TYPE", "refresh_token")
CLIENT_ID = os.getenv("VC_CLIENT_ID")
CLIENT_SECRET = os.getenv("VC_CLIENT_SECRET")
LOGIN_URL = os.getenv("VC_LOGIN_URL", "https://api.amazon.com/auth/o2/token")
MARKETPLACE_ID = os.getenv("VC_MARKETPLACE_ID", "A21TJRUUN4KGV")  # IN marketplace
VC_API_BASE_URL = os.getenv(
    "SP_API_BASE_URL", "https://sellingpartnerapi-na.amazon.com"
)

# Global variable to store access token and expiry
_vc_access_token = None
_vc_token_expires_at = None


def get_amazon_token() -> str:
    """
    Get a new access token from Amazon SP API using refresh token
    """
    global _vc_access_token, _vc_token_expires_at

    # Check if current token is still valid (with 5 minute buffer)
    if (
        _vc_access_token
        and _vc_token_expires_at
        and datetime.now() < (_vc_token_expires_at - timedelta(minutes=5))
    ):
        return _vc_access_token

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
        _vc_access_token = token_data.get("access_token")
        expires_in = token_data.get("expires_in", 3600)  # Default 1 hour
        _vc_token_expires_at = datetime.now() + timedelta(seconds=expires_in)

        logger.info("Successfully obtained new Amazon SP API token")
        return _vc_access_token

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

    url = f"{VC_API_BASE_URL}{endpoint}"

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
        logger.error(f"VC API request failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Amazon VC API request failed: {str(e)}",
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

    if report_options:
        report_data["reportOptions"] = report_options

    response = make_sp_api_request(endpoint, method="POST", data=report_data)
    return response.get("reportId")


def get_report_status(report_document_id: str) -> Dict:

    endpoint = f"/reports/2021-06-30/reports/{report_document_id}"
    return make_sp_api_request(endpoint)


def download_report_data(report_document_id: str, is_gzipped: bool = False) -> str:

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

    # Handle gzipped content (for inventory reports)
    if is_gzipped:
        try:
            return gzip.decompress(response.content).decode("utf-8")
        except Exception as e:
            logger.error(f"Error decompressing gzipped content: {e}")
            # Fallback to regular content if not actually gzipped
            return response.text

    return response.text


def get_vendor_sales(
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
            report_type="GET_VENDOR_SALES_REPORT",
            start_date=start_date,
            end_date=end_date,
            marketplace_ids=marketplace_ids,
            report_options={
                "reportPeriod": "DAY",
                "distributorView": "MANUFACTURING",
                "sellingProgram": "RETAIL",
            },
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

        if not report_document_id:
            raise ValueError("No report document ID available")

        report_data = download_report_data(report_document_id, is_gzipped=True)

        if not report_data:
            raise ValueError("Downloaded report data is empty")

        try:
            json_data = json.loads(report_data)
        except json.JSONDecodeError as json_error:
            raise ValueError(f"Failed to parse JSON report data: {json_error}")

        if not isinstance(json_data, dict):
            raise ValueError("Invalid JSON structure - expected dictionary")
        sales_by_asin = json_data.get("salesByAsin", [])
        sales_data = []
        if sales_by_asin:
            logger.info(f"Found {len(sales_by_asin)} items in salesByAsin")
            for asin_data in sales_by_asin:
                asin = asin_data["asin"]
                asin_data["date"] = datetime.fromisoformat(start_date)
                asin_data["created_at"] = datetime.now()
                result = db[SALES_COLLECTION].find_one(
                    {"asin": asin, "date": asin_data["date"]}
                )
                if not result:
                    sales_data.append(asin_data)
            return sales_by_asin
        else:
            logger.warning(f"No salesByAsin data found in report")
            return None

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


def get_vendor_inventory(
    start_date: str,
    end_date: str,
    db: Any,
    marketplace_ids: List[str] = None,
) -> List[Dict]:
    print(start_date, end_date)
    """
    Fetch inventory summary data from Amazon SP API
    """
    if marketplace_ids is None:
        marketplace_ids = [MARKETPLACE_ID]

    try:
        # Report options for inventory data
        report_options = {
            "reportPeriod": "DAY",
            "distributorView": "SOURCING",
            "sellingProgram": "RETAIL",
        }

        # Create inventory report
        report_id = create_report(
            report_type="GET_VENDOR_INVENTORY_REPORT",
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

        if not report_data:
            raise ValueError("Downloaded report data is empty")

        try:
            json_data = json.loads(report_data)
        except json.JSONDecodeError as json_error:
            raise ValueError(f"Failed to parse JSON report data: {json_error}")

        if not isinstance(json_data, dict):
            raise ValueError("Invalid JSON structure - expected dictionary")
        inventory_by_asin = json_data.get("inventoryByAsin", [])
        inventory_data = []
        if inventory_by_asin:
            logger.info(f"Found {len(inventory_by_asin)} items in inventoryByAsin")
            for asin_data in inventory_by_asin:
                asin = asin_data["asin"]
                asin_data["date"] = datetime.fromisoformat(start_date)
                asin_data["created_at"] = datetime.now()
                result = db[INVENTORY_COLLECTION].find_one(
                    {"asin": asin, "date": asin_data["date"]}
                )
                if not result:
                    inventory_data.append(asin_data)
            return inventory_by_asin
        else:
            logger.warning(f"No salesByAsin data found in report")
            return None
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
                    _vc_token_expires_at.isoformat() if _vc_token_expires_at else None
                ),
            },
        )
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.post("/sync/sales")
async def sync_sales_data(
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
        sales_traffic_data = get_vendor_sales(
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


@router.post("/sync/inventory")
async def sync_inventory_data(
    start_date: str,
    end_date: str,
    marketplace_ids: Optional[List[str]] = None,
    db=Depends(get_database),
):
    """
    Fetch inventory summary data from Amazon and insert into database
    Format: YYYY-MM-DD
    """
    try:
        # Validate date format
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        # Convert to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T00:00:00Z"

        # Fetch inventory data
        inventory_data = get_vendor_inventory(
            start_datetime,
            end_datetime,
            db,
            marketplace_ids,
        )

        # Insert into database
        inserted_count = await insert_data_to_db(
            INVENTORY_COLLECTION, inventory_data, db
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Ledger data synced successfully",
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
        logger.error(f"Error syncing inventory data: {e}")
        raise
