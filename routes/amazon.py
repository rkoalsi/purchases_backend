import pandas as pd
from io import BytesIO
from bson import ObjectId
from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends, Query
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
import re
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
FBA_RETURNS_COLLECTION = "amazon_fba_returns"
SC_RETURNS_COLLECTION = "amazon_seller_central_returns"

router = APIRouter()

router.include_router(amazon_vendor_router, prefix="/vendor")


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


async def fetch_last_n_days_amazon_ledger(
    db, end_datetime: datetime, asins: list, n_days: int = 90, report_type: str = "fba+seller_flex"
) -> Dict:
    """
    Fetch the last N days an item was in stock for Amazon ledger (FBA/Seller Flex).

    Args:
        db: Database connection
        end_datetime: End date to look back from
        asins: List of ASINs
        n_days: Number of days to fetch (default 90)
        report_type: Type of report (fba, seller_flex, fba+seller_flex)

    Returns:
        Dict mapping ASIN to formatted date ranges string
    """
    try:
        ledger_collection = db["amazon_ledger"]
        lookback_limit = end_datetime - timedelta(days=730)  # Look back 2 years

        logger.info(f"Fetching last {n_days} days in stock (Amazon Ledger) for {len(asins)} ASINs from {lookback_limit.date()} to {end_datetime.date()}")

        # Build match conditions based on report type
        match_condition = {
            "asin": {"$in": asins},
            "date": {"$gte": lookback_limit, "$lte": end_datetime},
            "disposition": "SELLABLE",
        }

        if report_type == "fba":
            match_condition["location"] = {"$ne": "VKSX"}
        elif report_type == "seller_flex":
            match_condition["location"] = "VKSX"

        pipeline = [
            {"$match": match_condition},
            {
                "$project": {
                    "asin": 1,
                    "date": 1,
                    "stock": "$ending_warehouse_balance",
                }
            },
            {"$match": {"stock": {"$gt": 0}}},
            {"$sort": {"asin": 1, "date": -1}},
            {
                "$group": {
                    "_id": "$asin",
                    "stock_dates": {"$push": "$date"}
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "stock_dates": {"$slice": ["$stock_dates", n_days]}
                }
            }
        ]

        cursor = ledger_collection.aggregate(pipeline, allowDiskUse=True)

        stock_date_ranges = {}
        for doc in cursor:
            asin = doc["_id"]
            dates = doc.get("stock_dates", [])
            if dates:
                stock_date_ranges[asin] = format_date_ranges(dates)
            else:
                stock_date_ranges[asin] = ""

        logger.info(f"Fetched last {n_days} days (Amazon Ledger): {len(stock_date_ranges)} ASINs with data")
        return stock_date_ranges

    except Exception as e:
        logger.error(f"Error fetching last N days (Amazon Ledger): {e}", exc_info=True)
        return {}


async def fetch_last_n_days_vendor_inventory(
    db, end_datetime: datetime, asins: list, n_days: int = 90
) -> Dict:
    """
    Fetch the last N days an item was in stock for Amazon Vendor Central inventory.

    Args:
        db: Database connection
        end_datetime: End date to look back from
        asins: List of ASINs
        n_days: Number of days to fetch (default 90)

    Returns:
        Dict mapping ASIN to formatted date ranges string
    """
    try:
        inventory_collection = db["amazon_vendor_inventory"]
        lookback_limit = end_datetime - timedelta(days=730)  # Look back 2 years

        logger.info(f"Fetching last {n_days} days in stock (Vendor) for {len(asins)} ASINs from {lookback_limit.date()} to {end_datetime.date()}")

        pipeline = [
            {
                "$match": {
                    "asin": {"$in": asins},
                    "date": {"$gte": lookback_limit, "$lte": end_datetime},
                }
            },
            {
                "$project": {
                    "asin": 1,
                    "date": 1,
                    "stock": "$sellableOnHandInventoryUnits",
                }
            },
            {"$match": {"stock": {"$gt": 0}}},
            {"$sort": {"asin": 1, "date": -1}},
            {
                "$group": {
                    "_id": "$asin",
                    "stock_dates": {"$push": "$date"}
                }
            },
            {
                "$project": {
                    "_id": 1,
                    "stock_dates": {"$slice": ["$stock_dates", n_days]}
                }
            }
        ]

        cursor = inventory_collection.aggregate(pipeline, allowDiskUse=True)

        stock_date_ranges = {}
        for doc in cursor:
            asin = doc["_id"]
            dates = doc.get("stock_dates", [])
            if dates:
                stock_date_ranges[asin] = format_date_ranges(dates)
            else:
                stock_date_ranges[asin] = ""

        logger.info(f"Fetched last {n_days} days (Vendor): {len(stock_date_ranges)} ASINs with data")
        return stock_date_ranges

    except Exception as e:
        logger.error(f"Error fetching last N days (Vendor): {e}", exc_info=True)
        return {}

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
        logger.info("Using cached access token")
        return _sc_access_token

    logger.info("Requesting new access token from Amazon")
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

        logger.info(f"Successfully obtained new Amazon SP API token (expires in {expires_in}s)")
        logger.info(f"Token prefix: {_sc_access_token[:20]}..." if _sc_access_token else "No token received")
        return _sc_access_token

    except requests.exceptions.RequestException as e:
        # Log detailed error information
        logger.error(f"Error getting Amazon token: {e}")

        # Try to get error response body if available
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_body = e.response.json()
                logger.error(f"Token error response: {json.dumps(error_body, indent=2)}")
            except:
                logger.error(f"Token error response text: {e.response.text}")

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
        # Log detailed error information
        error_details = {
            "error": str(e),
            "url": url,
            "method": method,
        }

        # Try to get error response body if available
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_body = e.response.json()
                error_details["response_body"] = error_body
                logger.error(f"SP API request failed: {e}")
                logger.error(f"Response body: {json.dumps(error_body, indent=2)}")
            except:
                error_details["response_text"] = e.response.text
                logger.error(f"SP API request failed: {e}")
                logger.error(f"Response text: {e.response.text}")
        else:
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
    logger.info(f"Processing dates - start: {start_date}, end: {end_date}")
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


def get_field_mapping_for_amazon_field(amazon_field: str) -> str:
    """
    Dynamically map Amazon field names to our expected field names
    """
    normalized = normalize_field_name(amazon_field)

    # Define mapping rules based on common patterns
    field_mappings = {
        # Order fields
        "order_id": "order_id",
        "orderid": "order_id",
        "amazon_order_id": "order_id",
        "amazonorderid": "order_id",
        # Dates
        "order_date": "order_date",
        "orderdate": "order_date",
        "return_request_date": "return_request_date",
        "returnrequestdate": "return_request_date",
        "return_delivery_date": "return_delivery_date",
        "returndeliverydate": "return_delivery_date",
        # Return information
        "return_request_status": "return_request_status",
        "returnrequeststatus": "return_request_status",
        "return_quantity": "return_quantity",
        "returnquantity": "return_quantity",
        "quantity": "return_quantity",
        "return_reason": "return_reason",
        "returnreason": "return_reason",
        "return_type": "return_type",
        "returntype": "return_type",
        # RMA fields
        "amazon_rma_id": "amazon_rma_id",
        "amazonrmaid": "amazon_rma_id",
        "merchant_rma_id": "merchant_rma_id",
        "merchantrmaid": "merchant_rma_id",
        # Shipping/Label fields
        "label_type": "label_type",
        "labeltype": "label_type",
        "label_cost": "label_cost",
        "labelcost": "label_cost",
        "currency_code": "currency_code",
        "currencycode": "currency_code",
        "return_carrier": "return_carrier",
        "returncarrier": "return_carrier",
        "tracking_id": "tracking_id",
        "trackingid": "tracking_id",
        "label_to_be_paid_by": "label_to_be_paid_by",
        "labeltobepaidby": "label_to_be_paid_by",
        # Claims and prime
        "a_to_z_claim": "a_to_z_claim",
        "atozclaim": "a_to_z_claim",
        "is_prime": "is_prime",
        "isprime": "is_prime",
        # Product fields
        "asin": "asin",
        "merchant_sku": "merchant_sku",
        "merchantsku": "merchant_sku",
        "sku": "merchant_sku",
        "item_name": "item_name",
        "itemname": "item_name",
        "product_name": "item_name",
        "productname": "item_name",
        "category": "category",
        # Policy and resolution
        "in_policy": "in_policy",
        "inpolicy": "in_policy",
        "resolution": "resolution",
        # Financial fields
        "order_amount": "order_amount",
        "orderamount": "order_amount",
        "order_quantity": "order_quantity",
        "orderquantity": "order_quantity",
        "refunded_amount": "refunded_amount",
        "refundedamount": "refunded_amount",
        # Other fields
        "invoice_number": "invoice_number",
        "invoicenumber": "invoice_number",
        "order_item_id": "order_item_id",
        "orderitemid": "order_item_id",
    }

    return field_mappings.get(normalized, normalized)


def normalize_amazon_sc_fields(record: Dict) -> Dict:
    """
    Normalize Amazon SC returns record to our expected structure
    """
    normalized_record = {}

    for amazon_field, value in record.items():
        # Get the mapped field name
        our_field = get_field_mapping_for_amazon_field(amazon_field)

        # Handle null/empty values
        if pd.isna(value) or value == "" or value == " ":
            normalized_record[our_field] = None
        else:
            # Handle special data type conversions
            if our_field in [
                "order_date",
                "return_request_date",
                "return_delivery_date",
            ]:
                try:
                    normalized_record[our_field] = pd.to_datetime(value).to_pydatetime()
                except:
                    normalized_record[our_field] = value
            elif our_field in ["label_cost"]:
                try:
                    normalized_record[our_field] = float(value)
                except:
                    normalized_record[our_field] = value
            elif our_field in ["return_quantity"]:
                try:
                    normalized_record[our_field] = int(float(value))
                except:
                    normalized_record[our_field] = value
            elif our_field == "tracking_id":
                try:
                    # Handle long tracking IDs
                    normalized_record[our_field] = int(float(value))
                except:
                    normalized_record[our_field] = value
            else:
                normalized_record[our_field] = value

    return normalized_record


def normalize_field_name(field_name: str) -> str:
    """
    Convert Amazon field names to our snake_case format
    """
    if not field_name:
        return ""

    # Remove extra spaces and convert to lowercase
    field_name = field_name.strip().lower()

    # Replace spaces, hyphens, and other separators with underscores
    field_name = re.sub(r"[\s\-\.]+", "_", field_name)

    # Remove special characters but keep underscores
    field_name = re.sub(r"[^a-z0-9_]", "", field_name)

    # Remove multiple consecutive underscores
    field_name = re.sub(r"_+", "_", field_name)

    # Remove leading/trailing underscores
    field_name = field_name.strip("_")

    return field_name


def normalize_amazon_fba_fields(record: Dict) -> Dict:
    """
    Normalize Amazon FBA returns record to our expected structure
    """
    normalized_record = {}

    # FBA field mappings
    fba_field_mappings = {
        "return_date": "return_date",
        "returndate": "return_date",
        "order_id": "amazon_order_id",
        "orderid": "amazon_order_id",
        "amazon_order_id": "amazon_order_id",
        "amazonorderid": "amazon_order_id",
        "sku": "sku_code",
        "sku_code": "sku_code",
        "skucode": "sku_code",
        "asin": "asin",
        "fnsku": "fnsku",
        "product_name": "product_name",
        "productname": "product_name",
        "item_name": "product_name",
        "itemname": "product_name",
        "quantity": "quantity",
        "fulfillment_center_id": "fulfillment_center_id",
        "fulfillmentcenterid": "fulfillment_center_id",
        "detailed_disposition": "detailed_disposition",
        "detaileddisposition": "detailed_disposition",
        "reason": "reason",
        "license_plate_number": "license_plate_number",
        "licenseplatenumber": "license_plate_number",
        "customer_comments": "customer_comments",
        "customercomments": "customer_comments",
    }

    for amazon_field, value in record.items():
        # Get normalized field name
        normalized_field = normalize_field_name(amazon_field)

        # Get our target field name
        our_field = fba_field_mappings.get(normalized_field, normalized_field)

        # Handle null/empty values
        if pd.isna(value) or value == "" or value == " ":
            normalized_record[our_field] = None
        else:
            # Handle special data type conversions
            if our_field == "return_date":
                try:
                    normalized_record[our_field] = pd.to_datetime(value).to_pydatetime()
                except:
                    normalized_record[our_field] = value
            elif our_field == "quantity":
                try:
                    normalized_record[our_field] = int(float(value))
                except:
                    normalized_record[our_field] = value
            else:
                normalized_record[our_field] = value

    return normalized_record


def get_amazon_fba_returns_data(
    start_date: str, end_date: str, db: Any, marketplace_ids: List[str] = None
) -> List[Dict]:
    """
    Fetch FBA returns data from Amazon SP API
    """
    if marketplace_ids is None:
        marketplace_ids = [MARKETPLACE_ID]

    try:
        # Create FBA returns report
        report_id = create_report(
            report_type="GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA",
            start_date=start_date,
            end_date=end_date,
            marketplace_ids=marketplace_ids,
        )

        # Add validation for report_id
        if not report_id:
            raise ValueError(
                "Failed to create FBA returns report - no report ID returned"
            )

        # Wait for report to be ready (with timeout)
        max_wait_time = 300  # 5 minutes
        wait_time = 0

        while wait_time < max_wait_time:
            report_status = get_report_status(report_id)

            # Add validation for report_status
            if not report_status:
                raise ValueError(
                    "Failed to get FBA returns report status - empty response"
                )

            processing_status = report_status.get("processingStatus")
            report_document_id = report_status.get("reportDocumentId")

            if processing_status == "DONE":
                break
            elif processing_status == "FATAL":
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="FBA returns report processing failed",
                )

            time.sleep(10)
            wait_time += 10

        if wait_time >= max_wait_time:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail="FBA returns report processing timed out",
            )

        # Validate report_document_id before downloading
        if not report_document_id:
            raise ValueError("No FBA returns report document ID available")

        # Download and parse report data (TSV format)
        report_data = download_report_data(report_document_id, is_gzipped=False)

        # Add validation for report_data
        if not report_data:
            raise ValueError("Downloaded FBA returns report data is empty")

        # Convert TSV data to list of dictionaries
        try:
            df = pd.read_csv(io.StringIO(report_data), sep="\t", low_memory=False)
            fba_returns_raw = df.to_dict("records")
        except Exception as parse_error:
            raise ValueError(f"Failed to parse FBA returns TSV data: {parse_error}")

        # Process and normalize data according to your document structure
        normalized_data = []
        for record in fba_returns_raw:
            # Apply dynamic field mapping
            normalized_record = normalize_amazon_fba_fields(record)

            # Check for duplicate using amazon_order_id + sku_code + return_date
            should_insert = True
            if all(
                key in normalized_record and normalized_record[key] is not None
                for key in ["amazon_order_id", "sku_code", "return_date"]
            ):
                duplicate_check = {
                    "amazon_order_id": normalized_record["amazon_order_id"],
                    "sku_code": normalized_record["sku_code"],
                    "return_date": normalized_record["return_date"],
                }

                existing_record = db[FBA_RETURNS_COLLECTION].find_one(duplicate_check)
                if existing_record:
                    should_insert = False
                    logger.debug(
                        f"Skipping duplicate FBA return: {normalized_record['amazon_order_id']} - {normalized_record['sku_code']}"
                    )

            if should_insert:
                normalized_data.append(normalized_record)

        logger.info(
            f"Processed {len(fba_returns_raw)} FBA returns records, {len(normalized_data)} new records to insert"
        )
        return normalized_data

    except HTTPException:
        # Re-raise HTTPExceptions as-is
        raise
    except ValueError as ve:
        logger.error(f"Validation error in FBA returns data: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"FBA returns data validation error: {str(ve)}",
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching FBA returns data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch FBA returns data: {str(e)}",
        )


def get_amazon_sc_returns_data(
    start_date: str, end_date: str, db: Any, marketplace_ids: List[str] = None
) -> List[Dict]:
    """
    Fetch Seller Central returns data from Amazon SP API
    """
    if marketplace_ids is None:
        marketplace_ids = [MARKETPLACE_ID]

    try:
        # Create Seller Central returns report
        report_id = create_report(
            report_type="GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE",
            start_date=start_date,
            end_date=end_date,
            marketplace_ids=marketplace_ids,
        )

        # Add validation for report_id
        if not report_id:
            raise ValueError(
                "Failed to create SC returns report - no report ID returned"
            )

        # Wait for report to be ready (with timeout)
        max_wait_time = 300  # 5 minutes
        wait_time = 0

        while wait_time < max_wait_time:
            report_status = get_report_status(report_id)

            # Add validation for report_status
            if not report_status:
                raise ValueError(
                    "Failed to get SC returns report status - empty response"
                )

            processing_status = report_status.get("processingStatus")
            report_document_id = report_status.get("reportDocumentId")

            if processing_status == "DONE":
                break
            elif processing_status == "FATAL":
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="SC returns report processing failed",
                )

            time.sleep(10)
            wait_time += 10

        if wait_time >= max_wait_time:
            raise HTTPException(
                status_code=status.HTTP_408_REQUEST_TIMEOUT,
                detail="SC returns report processing timed out",
            )

        # Validate report_document_id before downloading
        if not report_document_id:
            raise ValueError("No SC returns report document ID available")

        # Download and parse report data (TSV format)
        report_data = download_report_data(report_document_id, is_gzipped=False)

        # Add validation for report_data
        if not report_data:
            raise ValueError("Downloaded SC returns report data is empty")

        # Convert TSV data to list of dictionaries
        try:
            df = pd.read_csv(io.StringIO(report_data), sep="\t", low_memory=False)
            sc_returns_raw = df.to_dict("records")
        except Exception as parse_error:
            raise ValueError(f"Failed to parse SC returns TSV data: {parse_error}")

        # Process and normalize data according to your document structure
        normalized_data = []
        for record in sc_returns_raw:
            # Map Amazon column names to your document structure
            normalized_record = {}

            # Apply dynamic field mapping
            normalized_record = normalize_amazon_sc_fields(record)

            # Check for duplicate using order_id + merchant_sku + return_request_date
            should_insert = True
            if all(
                key in normalized_record and normalized_record[key] is not None
                for key in ["order_id", "merchant_sku", "return_request_date"]
            ):
                duplicate_check = {
                    "order_id": normalized_record["order_id"],
                    "merchant_sku": normalized_record["merchant_sku"],
                    "return_request_date": normalized_record["return_request_date"],
                }

                existing_record = db[SC_RETURNS_COLLECTION].find_one(duplicate_check)
                if existing_record:
                    should_insert = False
                    logger.debug(
                        f"Skipping duplicate SC return: {normalized_record['order_id']} - {normalized_record['merchant_sku']}"
                    )

            if should_insert:
                normalized_data.append(normalized_record)

        logger.info(
            f"Processed {len(sc_returns_raw)} SC returns records, {len(normalized_data)} new records to insert"
        )
        return normalized_data

    except HTTPException:
        # Re-raise HTTPExceptions as-is
        raise
    except ValueError as ve:
        logger.error(f"Validation error in SC returns data: {ve}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"SC returns data validation error: {str(ve)}",
        )
    except Exception as e:
        logger.error(f"Unexpected error fetching SC returns data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch SC returns data: {str(e)}",
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
                {"$match": {"date": {"$gte": start, "$lte": end}}},
                {
                    "$group": {
                        "_id": None,
                        "first_date": {"$min": "$date"},
                        "last_date": {"$max": "$date"},
                    }
                },
            ]

            sales_dates = list(db[sales_collection].aggregate(sales_date_pipeline))
            first_sales_date = None
            last_sales_date = None
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
            first_inventory_date = None
            last_inventory_date = None
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

            # Get total counts
            sales_count = db[sales_collection].count_documents(
                {"date": {"$gte": start, "$lte": end}}
            )
            inventory_count = db[inventory_collection].count_documents(
                {"date": {"$gte": start, "$lte": end}}
            )

            # Get distinct dates for sales
            sales_distinct_dates = list(db[sales_collection].distinct(
                "date", {"date": {"$gte": start, "$lte": end}}
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

            return {
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
            }

        # Handle different report types
        if report_type == "all":
            # Get both vendor_central and fba/seller_flex data
            vendor_status = await get_data_status(
                "amazon_vendor_sales", "amazon_vendor_inventory"
            )
            fba_status = await get_data_status(SALES_COLLECTION, INVENTORY_COLLECTION)

            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "date_range": {"start_date": start_date, "end_date": end_date},
                    "vendor_central": vendor_status,
                    "fba_seller_flex": fba_status,
                    "report_type": report_type,
                },
            )

        elif report_type == "vendor_central":
            status_data = await get_data_status(
                "amazon_vendor_sales", "amazon_vendor_inventory"
            )
        else:
            # fba, seller_flex, fba+seller_flex
            status_data = await get_data_status(SALES_COLLECTION, INVENTORY_COLLECTION)

        # For single report types, return the original structure
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "date_range": {"start_date": start_date, "end_date": end_date},
                "sales_data": status_data["sales_data"],
                "inventory_data": status_data["inventory_data"],
                "report_type": report_type,
            },
        )

    except Exception as e:
        logger.error(f"Error getting sync status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.post("/sync/fba-returns")
async def sync_fba_returns_data(
    start_date: str,
    end_date: str,
    marketplace_ids: Optional[List[str]] = None,
    db=Depends(get_database),
):
    """
    Fetch FBA returns data from Amazon and insert into database
    Format: YYYY-MM-DD
    """
    try:
        # Validate date format
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        # Convert to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T23:59:59Z"

        # Fetch FBA returns data
        fba_returns_data = get_amazon_fba_returns_data(
            start_datetime, end_datetime, db, marketplace_ids
        )

        # Insert into database
        inserted_count = await insert_data_to_db(
            FBA_RETURNS_COLLECTION, fba_returns_data, db
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "FBA returns data synced successfully",
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
        logger.error(f"Error syncing FBA returns data: {e}")
        raise


@router.post("/sync/sc-returns")
async def sync_sc_returns_data(
    start_date: str,
    end_date: str,
    marketplace_ids: Optional[List[str]] = None,
    db=Depends(get_database),
):
    """
    Fetch Seller Central returns data from Amazon and insert into database
    Format: YYYY-MM-DD
    """
    try:
        # Validate date format
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        # Convert to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T23:59:59Z"

        # Fetch SC returns data
        sc_returns_data = get_amazon_sc_returns_data(
            start_datetime, end_datetime, db, marketplace_ids
        )

        # Insert into database
        inserted_count = await insert_data_to_db(
            SC_RETURNS_COLLECTION, sc_returns_data, db
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Seller Central returns data synced successfully",
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
        logger.error(f"Error syncing SC returns data: {e}")
        raise


@router.post("/sync/daily-returns")
async def sync_daily_returns_data(
    db=Depends(get_database),
):
    """
    Fetch both FBA and SC returns data for a specific date (default: yesterday)
    This endpoint is designed for daily cron jobs
    Format: YYYY-MM-DD
    """
    try:
        start_date = datetime.now().replace(day=1).strftime("%Y-%m-%d")
        end_date = datetime.now().strftime("%Y-%m-%d")

        # Use the same date for start and end to get that specific day's data
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T23:59:59Z"

        # Fetch both types of returns data
        fba_returns_data = get_amazon_fba_returns_data(start_datetime, end_datetime, db)

        sc_returns_data = get_amazon_sc_returns_data(start_datetime, end_datetime, db)

        # Insert into respective databases
        fba_inserted_count = await insert_data_to_db(
            FBA_RETURNS_COLLECTION, fba_returns_data, db
        )

        sc_inserted_count = await insert_data_to_db(
            SC_RETURNS_COLLECTION, sc_returns_data, db
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Daily returns data synced successfully for {start_date}",
                "fba_returns": {
                    "records_inserted": fba_inserted_count,
                },
                "sc_returns": {
                    "records_inserted": sc_inserted_count,
                },
                "start_date": start_date,
                "end_date": end_date,
                "marketplace_ids": [MARKETPLACE_ID],
            },
        )

    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD",
        )
    except Exception as e:
        logger.error(f"Error syncing daily returns data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync daily returns data: {str(e)}",
        )


@router.post("/sync/monthly-returns")
async def sync_monthly_returns_data(
    year: int,
    month: int,
    marketplace_ids: Optional[List[str]] = None,
    db=Depends(get_database),
):
    """
    Fetch both FBA and SC returns data for the entire month till current date
    """
    try:
        # Calculate date range for the month
        from calendar import monthrange

        # Get first day of the month
        start_date = datetime(year, month, 1)

        # Get current date or last day of month, whichever is earlier
        current_date = datetime.now()
        last_day_of_month = monthrange(year, month)[1]

        if year == current_date.year and month == current_date.month:
            # Current month - sync till today
            end_date = current_date
        else:
            # Past month - sync entire month
            end_date = datetime(year, month, last_day_of_month)

        # Format dates for API
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        start_datetime = start_date.strftime("%Y-%m-%dT00:00:00Z")
        end_datetime = end_date.strftime("%Y-%m-%dT23:59:59Z")

        # Fetch both types of returns data
        fba_returns_data = get_amazon_fba_returns_data(
            start_datetime, end_datetime, db, marketplace_ids
        )

        sc_returns_data = get_amazon_sc_returns_data(
            start_datetime, end_datetime, db, marketplace_ids
        )

        # Insert into respective databases
        fba_inserted_count = await insert_data_to_db(
            FBA_RETURNS_COLLECTION, fba_returns_data, db
        )

        sc_inserted_count = await insert_data_to_db(
            SC_RETURNS_COLLECTION, sc_returns_data, db
        )

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Monthly returns data synced successfully",
                "fba_returns": {
                    "records_inserted": fba_inserted_count,
                },
                "sc_returns": {
                    "records_inserted": sc_inserted_count,
                },
                "date_range": f"{start_date_str} to {end_date_str}",
                "month": f"{year}-{month:02d}",
                "marketplace_ids": marketplace_ids or [MARKETPLACE_ID],
            },
        )

    except Exception as e:
        logger.error(f"Error syncing monthly returns data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync monthly returns data: {str(e)}",
        )


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
    try:
        sku_collection = database.get_collection(SKU_COLLECTION)
        sku_documents = serialize_mongo_document(
            list(sku_collection.find({}).sort("item_name", -1))
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
        logger.info(f"Deleted {delete_result.deleted_count} existing SKU mappings.")

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


async def generate_report_by_date_range(
    start_date: str, end_date: str, database: Any, report_type: str = "fba+seller_flex", any_last_90_days: bool = False
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
            vendor_data = await generate_vendor_central_data(start, end, database, any_last_90_days)

            # Get fba+seller_flex data
            fba_seller_flex_data = await generate_amazon_data(
                start, end, database, "fba+seller_flex", any_last_90_days
            )

            # Combine the data
            combined_data = combine_report_data(vendor_data, fba_seller_flex_data)

            return combined_data

        elif report_type == "vendor_central":
            return await generate_vendor_central_data(start, end, database, any_last_90_days)

        else:
            # Handle fba, seller_flex, fba+seller_flex
            return await generate_amazon_data(start, end, database, report_type, any_last_90_days)

    except Exception as e:
        import traceback

        logger.info(str(e))
        traceback.print_exc()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred generating Amazon report: {str(e)}",
        )


async def generate_vendor_central_data(start, end, database, any_last_90_days: bool = False):
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
                # Handle missing customerReturns field with $ifNull
                "customer_returns": {"$sum": {"$ifNull": ["$customerReturns", 0]}},
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
                            "closing_stock": {"$sum": "$sellableOnHandInventoryUnits"},
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
                "total_returns": {"$sum": "$customer_returns"},
                "last_day_closing_stock": {
                    "$last": {"$ifNull": ["$inventory_data.closing_stock", 0]}
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
                        "returns": "$customer_returns",  # ADD THIS for debugging
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
                "total_returns": {
                    "$ifNull": ["$total_returns", 0]
                },  # Handle null returns
                "data_source": {"$literal": "vendor_central"},
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

    # Fetch last 90 days in stock if requested
    if any_last_90_days and result:
        asins = [item["asin"] for item in result if "asin" in item]
        if asins:
            logger.info(f"Fetching last 90 days in stock for {len(asins)} ASINs (Vendor Central)")
            last_90_days_data = await fetch_last_n_days_vendor_inventory(database, end, asins, 90)

            # Add the last 90 days data to each item in the result
            for item in result:
                asin = item.get("asin")
                if asin:
                    item["last_90_days_dates"] = last_90_days_data.get(asin, "")

    return result


async def generate_amazon_data(start, end, database, report_type, any_last_90_days: bool = False):
    """Generate FBA/Seller Flex report data with returns information (FBA only)"""
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
    ]

    # Add FBA returns lookup for FBA reports (not seller_flex)
    if report_type in ["fba", "fba+seller_flex", "all"]:
        fba_returns_lookup_stage = {
            "$lookup": {
                "from": FBA_RETURNS_COLLECTION,
                "let": {"sales_asin": "$_id.asin"},
                "pipeline": [
                    {
                        "$match": {
                            "$expr": {
                                "$and": [
                                    {"$eq": ["$asin", "$$sales_asin"]},
                                    {"$gte": ["$return_date", start]},
                                    {"$lte": ["$return_date", end]},
                                ]
                            }
                        }
                    },
                    {
                        "$group": {
                            "_id": "$asin",
                            "fba_returns": {
                                "$sum": {"$toInt": {"$ifNull": ["$quantity", 1]}}
                            },
                        }
                    },
                ],
                "as": "fba_returns_data",
            }
        }
        base_pipeline.append(fba_returns_lookup_stage)
        base_pipeline.append(
            {"$addFields": {"fba_returns_data": {"$first": "$fba_returns_data"}}}
        )

    # Add SC returns lookup for all report types
    sc_returns_lookup_stage = {
        "$lookup": {
            "from": SC_RETURNS_COLLECTION,
            "let": {"sales_asin": "$_id.asin"},
            "pipeline": [
                {
                    "$match": {
                        "$expr": {
                            "$and": [
                                {"$eq": ["$asin", "$$sales_asin"]},
                                {"$gte": ["$order_date", start]},
                                {"$lte": ["$order_date", end]},
                            ]
                        }
                    }
                },
                {
                    "$group": {
                        "_id": "$asin",
                        "sc_returns": {
                            "$sum": {"$toInt": {"$ifNull": ["$return_quantity", 1]}}
                        },
                    }
                },
            ],
            "as": "sc_returns_data",
        }
    }
    base_pipeline.append(sc_returns_lookup_stage)
    base_pipeline.append(
        {"$addFields": {"sc_returns_data": {"$first": "$sc_returns_data"}}}
    )

    # Continue with the rest of the pipeline
    group_stage = {
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
                    "closing_stock": {"$ifNull": ["$ledger_data.closing_stock", 0]},
                    "units_sold": "$units_sold",
                    "sessions": "$sessions",
                }
            },
        }
    }

    # Add combined returns field
    if report_type in ["fba", "fba+seller_flex"]:
        # FBA reports: FBA returns + SC returns
        group_stage["$group"]["total_returns"] = {
            "$last": {
                "$add": [
                    {"$ifNull": ["$fba_returns_data.fba_returns", 0]},
                    {"$ifNull": ["$sc_returns_data.sc_returns", 0]},
                ]
            }
        }
    elif report_type == "seller_flex":
        # Seller Flex reports: Only SC returns (no FBA returns)
        group_stage["$group"]["total_returns"] = {
            "$last": {"$ifNull": ["$sc_returns_data.sc_returns", 0]}
        }

    base_pipeline.append(group_stage)

    # Add remaining pipeline stages
    base_pipeline.extend(
        [
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
        ]
    )

    # Build project stage dynamically based on report type
    project_stage = {
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
            "total_returns": "$total_returns",  # Always include total_returns
            "total_days_in_stock": "$total_days_in_stock",
            "drr": {"$round": ["$drr", 2]},
            "data_source": {"$literal": report_type},
        }
    }

    base_pipeline.extend(
        [
            project_stage,
            {
                "$sort": {
                    "sku_code": -1,
                    "asin": -1,
                }
            },
        ]
    )

    collection = database.get_collection(SALES_COLLECTION)
    cursor = list(collection.aggregate(base_pipeline))
    result = serialize_mongo_document(cursor)

    # Fetch last 90 days in stock if requested
    if any_last_90_days and result:
        asins = [item["asin"] for item in result if "asin" in item]
        if asins:
            logger.info(f"Fetching last 90 days in stock for {len(asins)} ASINs (Amazon Ledger - {report_type})")
            last_90_days_data = await fetch_last_n_days_amazon_ledger(database, end, asins, 90, report_type)

            # Add the last 90 days data to each item in the result
            for item in result:
                asin = item.get("asin")
                if asin:
                    item["last_90_days_dates"] = last_90_days_data.get(asin, "")

    return result


def combine_report_data(vendor_data, fba_seller_flex_data):
    """Combine vendor central and fba/seller flex data"""
    # Create lookup dictionaries
    vendor_lookup = {}
    fba_lookup = {}

    # Index vendor data by sku_code and asin
    for item in vendor_data:
        key = (item.get("sku_code"), item.get("asin"))
        vendor_lookup[key] = item

    # Index fba data by sku_code and asin
    for item in fba_seller_flex_data:
        key = (item.get("sku_code"), item.get("asin"))
        fba_lookup[key] = item

    combined_results = []
    processed_keys = set()

    # Combine matching records
    for key in vendor_lookup:
        if key in fba_lookup:
            vendor_item = vendor_lookup[key]
            fba_item = fba_lookup[key]

            combined_item = {
                "asin": vendor_item.get("asin") or fba_item.get("asin"),
                "sku_code": vendor_item.get("sku_code") or fba_item.get("sku_code"),
                "item_name": vendor_item.get("item_name") or fba_item.get("item_name"),
                "warehouses": fba_item.get("warehouses", []),  # Only from FBA data
                "units_sold": (
                    vendor_item.get("units_sold", 0) + fba_item.get("units_sold", 0)
                ),
                "total_amount": round(
                    (
                        vendor_item.get("total_amount", 0)
                        + fba_item.get("total_amount", 0)
                    ),
                    2,
                ),
                "sessions": fba_item.get("sessions", 0),  # Only from FBA data
                "closing_stock": (
                    vendor_item.get("closing_stock", 0)
                    + fba_item.get("closing_stock", 0)
                ),
                "stock": (
                    vendor_item.get("stock", 0) + fba_item.get("closing_stock", 0)
                ),  # Combined stock
                "total_days_in_stock": max(
                    vendor_item.get("total_days_in_stock", 0),
                    fba_item.get("total_days_in_stock", 0),
                ),
                "total_returns": (
                    vendor_item.get("total_returns", 0)
                    + fba_item.get("total_returns", 0)
                ),
                "drr": 0,  # Will be recalculated
                "data_source": "combined",
            }

            # Recalculate DRR
            if combined_item["total_days_in_stock"] > 0:
                combined_item["drr"] = round(
                    combined_item["units_sold"] / combined_item["total_days_in_stock"],
                    2,
                )

            combined_results.append(combined_item)
            processed_keys.add(key)

    # Add vendor-only records
    for key, vendor_item in vendor_lookup.items():
        if key not in processed_keys:
            # Add missing fields with default values
            vendor_item["warehouses"] = []
            vendor_item["sessions"] = 0
            if "stock" not in vendor_item:
                vendor_item["stock"] = vendor_item.get("closing_stock", 0)
            if "total_returns" not in vendor_item:
                vendor_item["total_returns"] = 0
            vendor_item["data_source"] = "vendor_only"
            combined_results.append(vendor_item)

    # Add FBA-only records
    for key, fba_item in fba_lookup.items():
        if key not in processed_keys:
            # Add missing fields with default values
            fba_item["stock"] = fba_item.get("closing_stock", 0)
            fba_item["data_source"] = "fba_only"
            combined_results.append(fba_item)

    # Sort the results
    combined_results.sort(
        key=lambda x: (x.get("sku_code") or "", x.get("asin") or ""), reverse=True
    )

    return combined_results


@router.get("/get_report_data_by_date_range")
async def get_report_data_by_date_range(
    start_date: str,
    end_date: str,
    report_type: str = "fba+seller_flex",
    any_last_90_days: bool = False,
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
            any_last_90_days=any_last_90_days,
        )
        logger.info(f"Retrieved dynamic report data of type: {report_type}")
        return report_response

    except HTTPException as e:
        raise e
    except Exception as e:
        logger.info(f"Error retrieving report data: {e}")
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
        "Last 90 Days Dates": "Last 90 Days Dates",
    }

    return replacements.get(formatted, formatted)


@router.get("/download_report_by_date_range")
async def download_report_by_date_range(
    start_date: str,
    end_date: str,
    report_type: str = "fba+seller_flex",  # "fba+seller_flex", "fba", "seller_flex", "vendor_central", or "all"
    any_last_90_days: bool = False,
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
            any_last_90_days=any_last_90_days,
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
                "Total Returns",
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
                "Total Returns",
                "Total Days In Stock",
                "Drr",
            ]

        # Add Last 90 Days column if requested
        if any_last_90_days:
            preferred_order.append("Last 90 Days Dates")

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
                    "combined": PatternFill(
                        start_color="E8F5E8", end_color="E8F5E8", fill_type="solid"
                    ),  # Light green
                    "vendor_only": PatternFill(
                        start_color="FFF2CC", end_color="FFF2CC", fill_type="solid"
                    ),  # Light yellow
                    "fba_only": PatternFill(
                        start_color="E1F5FE", end_color="E1F5FE", fill_type="solid"
                    ),  # Light blue
                }

                # Apply color coding to rows based on data source
                data_source_col_index = (
                    df.columns.get_loc("Data Source") + 1
                )  # +1 because Excel is 1-indexed

                for row_num in range(2, len(df) + 2):  # Start from row 2 (after header)
                    data_source_value = worksheet.cell(
                        row=row_num, column=data_source_col_index
                    ).value
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

        logger.info(
            f"Generated Excel report for report_type: {report_type}, rows: {len(df)}"
        )
        return response

    except HTTPException as e:
        logger.info(str(e))
        raise e
    except Exception as e:
        logger.info(f"Error generating Excel report: {e}")
        logger.info(str(e))
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
        "Total Returns": "Total Returns",
        "Stock": "Stock",
    }

    return replacements.get(formatted, formatted)

@router.post("/sync/settlements")
async def sync_settlement_reports(
    days_back: Optional[int] = 7,
    db=Depends(get_database),
):
    try:
        logger.info(f"Starting settlement reports sync for last {days_back} days")
        from io import StringIO
        import pandas as pd
        import time
        import requests
        from datetime import datetime, timedelta
        from fastapi.responses import JSONResponse
        from fastapi import status, HTTPException

        marketplace_endpoints = {
            "A21TJRUUN4KGV": "https://sellingpartnerapi-eu.amazon.com",
            "A1F83G8C2ARO7P": "https://sellingpartnerapi-eu.amazon.com",
            "A1PA6795UKMFR9": "https://sellingpartnerapi-eu.amazon.com",
            "ATVPDKIKX0DER": "https://sellingpartnerapi-na.amazon.com",
            "A2EUQ1WTGCTBG2": "https://sellingpartnerapi-na.amazon.com",
            "A1VC38T7YXB528": "https://sellingpartnerapi-fe.amazon.com",
        }

        base_url = marketplace_endpoints.get(
            MARKETPLACE_ID, "https://sellingpartnerapi-eu.amazon.com"
        )

        # Get access token
        access_token = get_amazon_token()

        # MongoDB collection
        collection = db["amazon_settlements"]

        # Get settlement reports
        reports_url = f"{base_url}/reports/2021-06-30/reports"
        headers = {"x-amz-access-token": access_token}

        created_since = datetime.now() - timedelta(days=days_back)

        params = {
            "reportTypes": "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2",
            "createdSince": created_since.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "pageSize": 100,
        }

        response = requests.get(reports_url, headers=headers, params=params)
        response.raise_for_status()

        reports = response.json().get("reports", [])

        if not reports:
            logger.info("No settlement reports found")
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content={
                    "message": "No settlement reports found",
                    "reports_processed": 0,
                    "records_inserted": 0,
                },
            )

        # Filter completed reports
        done_reports = [r for r in reports if r.get("processingStatus") == "DONE"]

        logger.info(f"Found {len(done_reports)} completed settlement reports")

        total_inserted = 0
        reports_processed = 0

        # Process each report
        for report in done_reports:
            report_id = report.get("reportId")
            report_date = report.get("dataStartTime", "")[:10]

            try:
                # Get report document ID
                report_url = f"{base_url}/reports/2021-06-30/reports/{report_id}"
                report_response = requests.get(report_url, headers=headers)
                report_response.raise_for_status()

                report_data = report_response.json()
                document_id = report_data.get("reportDocumentId")

                if not document_id:
                    logger.warning(f"No document ID for report {report_id}")
                    continue

                # Get document download URL
                doc_url = f"{base_url}/reports/2021-06-30/documents/{document_id}"
                doc_response = requests.get(doc_url, headers=headers)
                doc_response.raise_for_status()

                download_url = doc_response.json().get("url")

                # Download report data
                data_response = requests.get(download_url)
                data_response.raise_for_status()

                # Parse TSV data
                df = pd.read_csv(
                    StringIO(data_response.text),
                    sep="\t",
                    encoding="utf-8",
                    low_memory=False,
                )

                # Extract required fields
                required_fields = [
                    "order-id",
                    "amount-description",
                    "amount",
                    "sku",
                    "quantity-purchased",
                    "posted-date",
                ]

                existing_fields = [
                    field for field in required_fields if field in df.columns
                ]

                if not existing_fields:
                    logger.warning(f"No required fields found in report {report_id}")
                    continue

                extracted_df = df[existing_fields].copy()
                records = extracted_df.to_dict("records")

                # Process records
                processed_records = []

                for record in records:
                    new_record = {}

                    # Map fields
                    field_mapping = {
                        "order-id": "order_id",
                        "amount-description": "amount_description",
                        "amount": "amount",
                        "sku": "sku",
                        "quantity-purchased": "quantity_purchased",
                        "posted-date": "posted_date",
                    }

                    for old_key, new_key in field_mapping.items():
                        if old_key in record:
                            value = record[old_key]
                            new_record[new_key] = None if pd.isna(value) or value == "" else value

                    # Skip if order_id is missing
                    if not new_record.get("order_id"):
                        continue

                    # Add metadata
                    new_record["report_id"] = report_id
                    new_record["report_date"] = report_date
                    new_record["created_at"] = datetime.now()

                    # Data type conversions
                    try:
                        if new_record.get("amount") is not None:
                            new_record["amount"] = float(new_record["amount"])
                    except:
                        pass

                    try:
                        if new_record.get("posted_date") is not None:
                            new_record["posted_date"] = datetime.strptime(
                                new_record["posted_date"], "%d.%m.%Y"
                            )
                    except:
                        pass

                    try:
                        new_record["report_date"] = datetime.strptime(report_date, "%Y-%m-%d")
                    except:
                        pass

                    try:
                        if new_record.get("quantity_purchased") is not None:
                            new_record["quantity_purchased"] = int(new_record["quantity_purchased"])
                    except:
                        pass

                    processed_records.append(new_record)

                if not processed_records:
                    logger.warning(f"No valid records in report {report_id}")
                    continue

                # 🚀 Insert all records (no duplicate checking)
                result = collection.insert_many(processed_records, ordered=False)
                inserted = len(result.inserted_ids)
                total_inserted += inserted
                reports_processed += 1

                logger.info(f"Report {report_id}: Inserted {inserted} records (duplicates allowed)")

                # Small delay to avoid rate limiting
                time.sleep(0.5)

            except Exception as e:
                logger.error(f"Error processing report {report_id}: {e}")
                continue

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": "Settlement reports sync completed",
                "reports_processed": reports_processed,
                "records_inserted": total_inserted,
                "days_back": days_back,
            },
        )

    except Exception as e:
        logger.error(f"Error syncing settlement reports: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to sync settlement reports: {str(e)}",
        )


@router.get("/settlements/pivot")
async def get_settlements_pivot(
    start_date: Optional[str] = Query(
        None, description="Start date in YYYY-MM-DD format"
    ),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    sku: Optional[str] = Query(None, description="Filter by SKU"),
    order_id: Optional[str] = Query(None, description="Filter by order ID"),
    format: str = Query("json", description="Response format: 'json' or 'excel'"),
    db=Depends(get_database),
):
    """Get Amazon settlements data in pivot table format."""
    try:
        collection = db["amazon_settlements"]

        # Build MongoDB query
        query = {}

        # Step 1: If date range is specified, first find all order_ids within that range
        if start_date or end_date:
            date_query = {}
            if start_date:
                try:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    date_query["$gte"] = start_dt
                except ValueError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Invalid start_date format. Use YYYY-MM-DD",
                    )
            if end_date:
                try:
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    date_query["$lte"] = end_dt
                except ValueError:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Invalid end_date format. Use YYYY-MM-DD",
                    )

            # Find all order_ids that have at least one record in the date range
            orders_in_range = collection.distinct("order_id", {"posted_date": date_query})

            # Now fetch ALL records for those orders (regardless of date)
            query["order_id"] = {"$in": orders_in_range}

        if sku:
            query["sku"] = sku

        if order_id:
            # If specific order_id is provided, override the date-based order filtering
            query["order_id"] = order_id

        # Fetch data from MongoDB
        cursor = collection.find(query)
        settlements = list(cursor)

        if not settlements:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No settlement records found for the given criteria",
            )

        # Convert to DataFrame
        df = pd.DataFrame(settlements)
        df = df[
            [
                "order_id",
                "sku",
                "quantity_purchased",
                "amount_description",
                "amount",
            ]
        ]

        # Group by order+sku+amount_description and sum quantities and amounts
        # This aggregates all records across ALL dates for the same order
        grouping_cols = ["order_id", "sku", "amount_description"]
        df = df.groupby(grouping_cols, as_index=False).agg({
            "quantity_purchased": "sum",
            "amount": "sum"
        })

        # Get total quantity per order (max across all amount_descriptions to handle any discrepancies)
        quantity_per_order = df.groupby(["order_id", "sku"])["quantity_purchased"].max().reset_index()

        # Create pivot table - one row per order+sku
        pivot_df = df.pivot_table(
            index=["order_id", "sku"],
            columns="amount_description",
            values="amount",
            aggfunc="sum",
            fill_value=None,
        )

        pivot_df = pivot_df.reset_index()

        # Merge the quantity back in
        pivot_df = pivot_df.merge(
            quantity_per_order,
            on=["order_id", "sku"],
            how="left"
        )

        # Calculate Grand Total (sum of all amount columns)
        amount_columns = [
            col
            for col in pivot_df.columns
            if col not in ["order_id", "sku", "quantity_purchased"]
        ]
        pivot_df["Grand Total"] = pivot_df[amount_columns].sum(axis=1)

        # 🔧 FIX: Replace NaN/inf values with None for JSON serialization
        pivot_df = pivot_df.replace([float("nan"), float("inf"), float("-inf")], None)

        # Sort by order_id
        pivot_df = pivot_df.sort_values("order_id", ascending=False)

        # Format response based on requested format
        if format.lower() == "excel":
            output = BytesIO()

            with pd.ExcelWriter(output, engine="openpyxl") as writer:
                pivot_df.to_excel(writer, sheet_name="Sheet1", index=False)

                workbook = writer.book
                worksheet = writer.sheets["Sheet1"]

                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)
                    worksheet.column_dimensions[column_letter].width = adjusted_width

            output.seek(0)

            filename = "amazon_settlements_pivot"
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

        else:
            # Return as JSON
            result = pivot_df.to_dict(orient="records")

            return {
                "success": True,
                "count": len(result),
                "filters": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "sku": sku,
                    "order_id": order_id,
                },
                "data": result,
            }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating settlements pivot: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate settlements pivot: {str(e)}",
        )


@router.get("/settlements/pivot/columns")
async def get_pivot_columns(db=Depends(get_database)):
    """
    Get list of all unique amount_description values (column names in pivot table).

    This is useful for understanding what charge types exist in your data.
    """
    try:
        collection = db["amazon_settlements"]

        # Get distinct amount_description values
        columns = collection.distinct("amount_description")

        # Remove None values
        columns = [col for col in columns if col is not None]

        # Sort alphabetically
        columns.sort()

        return {"success": True, "count": len(columns), "columns": columns}

    except Exception as e:
        logger.error(f"Error fetching pivot columns: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to fetch pivot columns: {str(e)}",
        )


@router.get("/settlements/summary")
async def get_settlements_summary(
    start_date: Optional[str] = Query(
        None, description="Start date in YYYY-MM-DD format"
    ),
    end_date: Optional[str] = Query(None, description="End date in YYYY-MM-DD format"),
    group_by: str = Query(
        "amount_description",
        description="Group by: 'amount_description', 'sku', or 'date'",
    ),
    db=Depends(get_database),
):
    """
    Get summary statistics for settlements data.

    Query Parameters:
    - start_date: Filter by posted date (start)
    - end_date: Filter by posted date (end)
    - group_by: How to group the summary - 'amount_description', 'sku', or 'date'

    Returns summary totals grouped by the specified field.
    """
    try:
        collection = db["amazon_settlements"]

        # Build match stage
        match_stage = {}

        # Step 1: If date range is specified, first find all order_ids within that range
        if start_date or end_date:
            date_query = {}
            if start_date:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                date_query["$gte"] = start_dt
            if end_date:
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                date_query["$lte"] = end_dt

            # Find all order_ids that have at least one record in the date range
            orders_in_range = collection.distinct("order_id", {"posted_date": date_query})

            # Now match ALL records for those orders (regardless of date)
            match_stage["order_id"] = {"$in": orders_in_range}

        # Build aggregation pipeline

        group_field_map = {
            "amount_description": "$amount_description",
            "sku": "$sku",
            "date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$posted_date"}},
        }

        if group_by not in group_field_map:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid group_by value. Must be one of: {', '.join(group_field_map.keys())}",
            )

        pipeline = []

        if match_stage:
            pipeline.append({"$match": match_stage})

        pipeline.extend(
            [
                {
                    "$group": {
                        "_id": group_field_map[group_by],
                        "total_amount": {"$sum": "$amount"},
                        "count": {"$sum": 1},
                        "avg_amount": {"$avg": "$amount"},
                    }
                },
                {"$sort": {"total_amount": -1}},
            ]
        )

        results = list(collection.aggregate(pipeline))

        # Format results
        formatted_results = []
        for result in results:
            formatted_results.append(
                {
                    group_by: result["_id"],
                    "total_amount": (
                        round(result["total_amount"], 2)
                        if result["total_amount"]
                        else 0
                    ),
                    "count": result["count"],
                    "average_amount": (
                        round(result["avg_amount"], 2) if result["avg_amount"] else 0
                    ),
                }
            )

        return {
            "success": True,
            "group_by": group_by,
            "filters": {"start_date": start_date, "end_date": end_date},
            "count": len(formatted_results),
            "data": formatted_results,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error generating settlements summary: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate settlements summary: {str(e)}",
        )
