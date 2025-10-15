from fastapi import APIRouter, HTTPException, status, Depends, BackgroundTasks
from fastapi.responses import JSONResponse
from datetime import datetime, timedelta
from pymongo.errors import PyMongoError
from ..database import get_database
import os
import requests
import json
import time
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

    logger.info("🔑 Checking Amazon token validity...")
    
    # Check if current token is still valid (with 5 minute buffer)
    if (
        _vc_access_token
        and _vc_token_expires_at
        and datetime.now() < (_vc_token_expires_at - timedelta(minutes=5))
    ):
        logger.info("✅ Using existing valid token")
        return _vc_access_token

    logger.info("🔄 Requesting new Amazon SP API token...")
    
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

        logger.info("✅ Successfully obtained new Amazon SP API token")
        return _vc_access_token

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error getting Amazon token: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to authenticate with Amazon SP API: {str(e)}",
        )
    except Exception as e:
        logger.error(f"❌ Unexpected error getting Amazon token: {e}")
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
    logger.info(f"📡 Making {method} request to: {endpoint}")
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
        logger.info(f"✅ API request successful: {response.status_code}")
        return response.json()

    except requests.exceptions.RequestException as e:
        logger.error(f"❌ API request failed: {e}")
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
    Create a report request and return the report ID
    """
    logger.info(f"📊 Creating {report_type} report for {start_date} to {end_date}")
    
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
    report_id = response.get("reportId")
    logger.info(f"✅ Report created with ID: {report_id}")
    return report_id


def get_report_status(report_id: str) -> Dict:
    """Get report status"""
    endpoint = f"/reports/2021-06-30/reports/{report_id}"
    return make_sp_api_request(endpoint)


def wait_for_report_completion(report_id: str, report_type: str, max_wait_time: int = 900) -> str:
    """
    Wait for report to complete and return document ID
    Increased default wait time to 15 minutes with progressive backoff
    """
    start_time = time.time()
    check_interval = 30  # Start with 30 seconds
    
    logger.info(f"⏳ Waiting for {report_type} report {report_id} to complete (max: {max_wait_time}s)")
    
    while time.time() - start_time < max_wait_time:
        elapsed = int(time.time() - start_time)
        report_status = get_report_status(report_id)
        
        if not report_status:
            raise ValueError("Failed to get report status - empty response")

        processing_status = report_status.get("processingStatus")
        report_document_id = report_status.get("reportDocumentId")

        logger.info(f"📊 Report status: {processing_status} (elapsed: {elapsed}s / {max_wait_time}s)")

        if processing_status == "DONE":
            if not report_document_id:
                raise ValueError("Report completed but no document ID available")
            logger.info(f"✅ Report processing completed! Document ID: {report_document_id}")
            return report_document_id
            
        elif processing_status == "FATAL":
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"{report_type} report processing failed with FATAL status",
            )

        # Progressive backoff - check less frequently as time goes on
        if elapsed > 300:  # After 5 minutes
            check_interval = 60
        elif elapsed > 600:  # After 10 minutes
            check_interval = 120

        logger.info(f"⏳ Report still processing, waiting {check_interval} seconds...")
        time.sleep(check_interval)

    raise HTTPException(
        status_code=status.HTTP_408_REQUEST_TIMEOUT,
        detail=f"{report_type} report processing timed out after {max_wait_time}s",
    )


def download_report_data(report_document_id: str, is_gzipped: bool = False) -> str:
    """
    Download report data with timeout handling
    """
    logger.info(f"📥 Downloading report data for document: {report_document_id}")
    
    endpoint = f"/reports/2021-06-30/documents/{report_document_id}"
    document_info = make_sp_api_request(endpoint)

    download_url = document_info.get("url")
    if not download_url:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="No download URL found in report document",
        )

    logger.info("📥 Downloading report from Amazon...")
    # Increased timeout for large reports
    response = requests.get(download_url, timeout=300)
    response.raise_for_status()

    # Handle gzipped content
    if is_gzipped:
        try:
            logger.info("🗜️ Decompressing gzipped report data...")
            return gzip.decompress(response.content).decode("utf-8")
        except Exception as e:
            logger.warning(f"⚠️ Error decompressing gzipped content: {e}")
            # Fallback to regular content if not actually gzipped
            return response.text

    logger.info("✅ Report data downloaded successfully")
    return response.text


def get_vendor_sales(
    start_date: str, end_date: str, max_retries: int = 3
) -> Optional[List[Dict]]:
    """
    Fetch sales data from Amazon SP API - OPTIMIZED VERSION
    Returns raw data without database checks for better performance
    """
    logger.info(f"🛒 Starting sales data fetch for {start_date} to {end_date}")
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 60 * attempt
                logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries} after {wait_time}s wait")
                time.sleep(wait_time)

            # Create sales report
            report_id = create_report(
                report_type="GET_VENDOR_SALES_REPORT",
                start_date=start_date,
                end_date=end_date,
                marketplace_ids=[MARKETPLACE_ID],
                report_options={
                    "reportPeriod": "DAY",
                    "distributorView": "MANUFACTURING",
                    "sellingProgram": "RETAIL",
                },
            )

            if not report_id:
                raise ValueError("Failed to create report - no report ID returned")

            # Wait for report completion with longer timeout
            report_document_id = wait_for_report_completion(report_id, "SALES", max_wait_time=900)

            # Download and parse report
            report_data = download_report_data(report_document_id, is_gzipped=True)

            if not report_data:
                raise ValueError("Downloaded report data is empty")

            logger.info("📊 Parsing JSON report data...")
            json_data = json.loads(report_data)

            if not isinstance(json_data, dict):
                raise ValueError("Invalid JSON structure - expected dictionary")
                
            sales_by_asin = json_data.get("salesByAsin", [])
            
            if sales_by_asin:
                logger.info(f"✅ Found {len(sales_by_asin)} items in salesByAsin")
                return sales_by_asin
            else:
                logger.warning("⚠️ No salesByAsin data found in report")
                return None

        except HTTPException:
            raise
        except ValueError as ve:
            logger.error(f"❌ Validation error (attempt {attempt + 1}): {ve}")
            if attempt == max_retries - 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Data validation error: {str(ve)}",
                )
        except Exception as e:
            logger.error(f"❌ Error (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to fetch sales data after {max_retries} attempts: {str(e)}",
                )
    
    return None


def get_vendor_inventory(
    start_date: str, end_date: str, max_retries: int = 3
) -> Optional[List[Dict]]:
    """
    Fetch inventory data from Amazon SP API - OPTIMIZED VERSION
    Returns raw data without database checks for better performance
    """
    logger.info(f"📦 Starting inventory data fetch for {start_date} to {end_date}")
    
    for attempt in range(max_retries):
        try:
            if attempt > 0:
                wait_time = 60 * attempt
                logger.info(f"🔄 Retry attempt {attempt + 1}/{max_retries} after {wait_time}s wait")
                time.sleep(wait_time)

            # Create inventory report
            report_id = create_report(
                report_type="GET_VENDOR_INVENTORY_REPORT",
                start_date=start_date,
                end_date=end_date,
                marketplace_ids=[MARKETPLACE_ID],
                report_options={
                    "reportPeriod": "DAY",
                    "distributorView": "MANUFACTURING",
                    "sellingProgram": "RETAIL",
                },
            )

            if not report_id:
                raise ValueError("Failed to create report - no report ID returned")

            # Wait for report completion with longer timeout
            report_document_id = wait_for_report_completion(report_id, "INVENTORY", max_wait_time=900)

            # Download and parse report
            report_data = download_report_data(report_document_id, is_gzipped=True)

            if not report_data:
                raise ValueError("Downloaded report data is empty")

            logger.info("📦 Parsing JSON inventory data...")
            json_data = json.loads(report_data)

            if not isinstance(json_data, dict):
                raise ValueError("Invalid JSON structure - expected dictionary")
                
            inventory_by_asin = json_data.get("inventoryByAsin", [])
            
            if inventory_by_asin:
                logger.info(f"✅ Found {len(inventory_by_asin)} items in inventoryByAsin")
                return inventory_by_asin
            else:
                logger.warning("⚠️ No inventoryByAsin data found in report")
                return None

        except HTTPException:
            raise
        except ValueError as ve:
            logger.error(f"❌ Validation error (attempt {attempt + 1}): {ve}")
            if attempt == max_retries - 1:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Data validation error: {str(ve)}",
                )
        except Exception as e:
            logger.error(f"❌ Error (attempt {attempt + 1}): {e}")
            if attempt == max_retries - 1:
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to fetch inventory data after {max_retries} attempts: {str(e)}",
                )
    
    return None


def background_refetch_sales(start_date_str: str, end_date_str: str, marketplace_ids: List[str], db, task_id: str):
    """
    Background task to delete and refetch sales data for last 11 days - OPTIMIZED
    """
    logger.info(f"🚀 [TASK-{task_id}] Starting background refetch task for last 11 days")
    
    try:
        collection = db[SALES_COLLECTION]
        
        # Parse dates
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
        
        # Delete existing records for the entire date range at once
        delete_end = end_date + timedelta(days=1)
        
        logger.info(f"🗑️ [TASK-{task_id}] Deleting existing records from {start_date.date()} to {end_date.date()}")
        
        delete_result = collection.delete_many({
            "date": {
                "$gte": start_date,
                "$lt": delete_end
            }
        })
        
        deleted_count = delete_result.deleted_count
        logger.info(f"🗑️ [TASK-{task_id}] Deleted {deleted_count} existing records")
        
        # Process each day individually
        total_inserted = 0
        current_date = start_date
        
        while current_date <= end_date:
            date_str = current_date.strftime("%Y-%m-%d")
            logger.info(f"📅 [TASK-{task_id}] Processing date: {date_str}")
            
            try:
                start_datetime = f"{date_str}T00:00:00Z"
                end_datetime = f"{date_str}T23:59:59Z"
                
                # Get sales data (no DB checks in this function anymore)
                sales_traffic_data = get_vendor_sales(start_datetime, end_datetime)
                
                if sales_traffic_data:
                    # Prepare all documents at once
                    new_records = []
                    current_time = datetime.now()
                    date_obj = datetime.combine(current_date.date(), datetime.min.time())
                    
                    for asin_data in sales_traffic_data:
                        asin_data["date"] = date_obj
                        asin_data["created_at"] = current_time
                        new_records.append(asin_data)
                    
                    if new_records:
                        # Bulk insert all records at once
                        insert_result = collection.insert_many(new_records, ordered=False)
                        day_inserted = len(insert_result.inserted_ids)
                        total_inserted += day_inserted
                        logger.info(f"✅ [TASK-{task_id}] Inserted {day_inserted} records for {date_str}")
                    else:
                        logger.info(f"ℹ️ [TASK-{task_id}] No records to insert for {date_str}")
                else:
                    logger.info(f"⚠️ [TASK-{task_id}] No sales data received for {date_str}")
                
            except Exception as day_error:
                logger.error(f"❌ [TASK-{task_id}] Error processing {date_str}: {day_error}")
            
            current_date += timedelta(days=1)
            
            # Small delay between dates to avoid rate limits
            if current_date <= end_date:
                time.sleep(30)
        
        logger.info(f"✅ [TASK-{task_id}] Refetch completed! Deleted: {deleted_count}, Inserted: {total_inserted} records")
        
    except Exception as e:
        logger.error(f"❌ [TASK-{task_id}] Background refetch failed: {e}")


def background_sync_sales(start_datetime: str, end_datetime: str, marketplace_ids: List[str], db, task_id: str):
    """
    Background task for sales data sync - OPTIMIZED
    """
    logger.info(f"🚀 [TASK-{task_id}] Starting background sales sync task")
    try:
        # Get sales data (no DB checks in this function anymore)
        sales_traffic_data = get_vendor_sales(start_datetime, end_datetime)
        
        if sales_traffic_data:
            logger.info(f"💾 [TASK-{task_id}] Starting database insertion...")
            collection = db[SALES_COLLECTION]
            
            # Parse date from datetime string
            date_obj = datetime.fromisoformat(start_datetime.replace('Z', '+00:00')).replace(hour=0, minute=0, second=0, microsecond=0)
            current_time = datetime.now()
            
            # Get existing ASINs for this date in one query
            existing_asins = set(
                collection.distinct("asin", {"date": date_obj})
            )
            logger.info(f"📋 [TASK-{task_id}] Found {len(existing_asins)} existing ASINs for this date")
            
            # Filter out existing records
            new_records = []
            for asin_data in sales_traffic_data:
                if asin_data["asin"] not in existing_asins:
                    asin_data["date"] = date_obj
                    asin_data["created_at"] = current_time
                    new_records.append(asin_data)
            
            if new_records:
                # Bulk insert all at once
                result = collection.insert_many(new_records, ordered=False)
                logger.info(f"✅ [TASK-{task_id}] Background sales sync completed! Inserted {len(result.inserted_ids)} records")
            else:
                logger.info(f"ℹ️ [TASK-{task_id}] No new sales records to insert")
        else:
            logger.info(f"⚠️ [TASK-{task_id}] No sales data received")
            
    except Exception as e:
        logger.error(f"❌ [TASK-{task_id}] Background sales sync failed: {e}")


def background_sync_inventory(start_datetime: str, end_datetime: str, marketplace_ids: List[str], db, task_id: str):
    """
    Background task for inventory data sync - OPTIMIZED
    """
    logger.info(f"🚀 [TASK-{task_id}] Starting background inventory sync task")
    try:
        # Get inventory data (no DB checks in this function anymore)
        inventory_data = get_vendor_inventory(start_datetime, end_datetime)
        
        if inventory_data:
            logger.info(f"💾 [TASK-{task_id}] Starting database insertion...")
            collection = db[INVENTORY_COLLECTION]
            
            # Parse date from datetime string
            date_obj = datetime.fromisoformat(start_datetime.replace('Z', '+00:00')).replace(hour=0, minute=0, second=0, microsecond=0)
            current_time = datetime.now()
            
            # Get existing ASINs for this date in one query
            existing_asins = set(
                collection.distinct("asin", {"date": date_obj})
            )
            logger.info(f"📋 [TASK-{task_id}] Found {len(existing_asins)} existing ASINs for this date")
            
            # Filter out existing records
            new_records = []
            for asin_data in inventory_data:
                if asin_data["asin"] not in existing_asins:
                    asin_data["date"] = date_obj
                    asin_data["created_at"] = current_time
                    new_records.append(asin_data)
            
            if new_records:
                # Bulk insert all at once
                result = collection.insert_many(new_records, ordered=False)
                logger.info(f"✅ [TASK-{task_id}] Background inventory sync completed! Inserted {len(result.inserted_ids)} records")
            else:
                logger.info(f"ℹ️ [TASK-{task_id}] No new inventory records to insert")
        else:
            logger.info(f"⚠️ [TASK-{task_id}] No inventory data received")
            
    except Exception as e:
        logger.error(f"❌ [TASK-{task_id}] Background inventory sync failed: {e}")


# API Endpoints (unchanged)

@router.post("/auth/token")
async def refresh_amazon_token():
    """
    Refresh Amazon SP API token
    """
    logger.info("🔑 Token refresh endpoint called")
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
        logger.error(f"❌ Token refresh failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.post("/sync/sales")
async def sync_sales_data(
    start_date: str,
    end_date: str,
    background_tasks: BackgroundTasks,
    marketplace_ids: Optional[List[str]] = None,
    db=Depends(get_database),
):
    """
    Fetch ASIN-wise daily sales data from Amazon and insert into database (Background Task)
    Format: YYYY-MM-DD
    """
    logger.info(f"🛒 Sales sync endpoint called for {start_date} to {end_date}")
    
    try:
        # Validate date format
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        # Convert to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T23:59:59Z"

        # Generate a task ID for tracking
        task_id = f"sales_{start_date}_{end_date}_{int(time.time())}"
        
        # Add background task
        background_tasks.add_task(
            background_sync_sales, 
            start_datetime, 
            end_datetime, 
            marketplace_ids or [MARKETPLACE_ID], 
            db,
            task_id
        )

        logger.info(f"✅ Sales sync background task {task_id} started")

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "message": "Sales data sync started in background",
                "task_id": task_id,
                "date_range": f"{start_date} to {end_date}",
                "marketplace_ids": marketplace_ids or [MARKETPLACE_ID],
                "status": "processing",
                "note": "Check console logs for progress updates"
            },
        )

    except ValueError as e:
        logger.error(f"❌ Invalid date format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD",
        )
    except Exception as e:
        logger.error(f"❌ Error starting sales sync: {e}")
        raise


@router.post("/sync/inventory")
async def sync_inventory_data(
    start_date: str,
    end_date: str,
    background_tasks: BackgroundTasks,
    marketplace_ids: Optional[List[str]] = None,
    db=Depends(get_database),
):
    """
    Fetch inventory summary data from Amazon and insert into database (Background Task)
    Format: YYYY-MM-DD
    """
    logger.info(f"📦 Inventory sync endpoint called for {start_date} to {end_date}")
    
    try:
        # Validate date format
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")

        # Convert to ISO format for API
        start_datetime = f"{start_date}T00:00:00Z"
        end_datetime = f"{end_date}T00:00:00Z"

        # Generate a task ID for tracking
        task_id = f"inventory_{start_date}_{end_date}_{int(time.time())}"

        # Add background task
        background_tasks.add_task(
            background_sync_inventory, 
            start_datetime, 
            end_datetime, 
            marketplace_ids or [MARKETPLACE_ID], 
            db,
            task_id
        )

        logger.info(f"✅ Inventory sync background task {task_id} started")

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "message": "Inventory data sync started in background",
                "task_id": task_id,
                "date_range": f"{start_date} to {end_date}",
                "marketplace_ids": marketplace_ids or [MARKETPLACE_ID],
                "status": "processing",
                "note": "Check console logs for progress updates"
            },
        )

    except ValueError as e:
        logger.error(f"❌ Invalid date format: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Invalid date format. Use YYYY-MM-DD",
        )
    except Exception as e:
        logger.error(f"❌ Error starting inventory sync: {e}")
        raise
    
@router.post("/sync/cron/sales")
async def refetch_last_11_days_sales(
    background_tasks: BackgroundTasks,
    marketplace_ids: Optional[List[str]] = None,
    db=Depends(get_database),
):
    """
    Delete and refetch sales traffic data for the last 11 days (Background Task)
    This endpoint will:
    1. Calculate date range for last 11 days
    2. Delete existing records for this period
    3. Fetch fresh data from Amazon for each day
    4. Insert new data into database
    
    No parameters required - automatically processes last 11 days from today
    """
    logger.info(f"🔄 Refetch last 11 days sales endpoint called")
    
    try:
        # Calculate date range for last 11 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=10)  # 10 days back + today = 11 days
        
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")

        # Generate a task ID for tracking
        task_id = f"refetch_sales_{start_date_str}_{end_date_str}_{int(time.time())}"
        
        logger.info(f"📅 [TASK-{task_id}] Date range: {start_date_str} to {end_date_str}")
        
        # Add background task
        background_tasks.add_task(
            background_refetch_sales,
            start_date_str,
            end_date_str,
            marketplace_ids or [MARKETPLACE_ID],
            db,
            task_id
        )

        logger.info(f"✅ Refetch sales background task {task_id} started")

        return JSONResponse(
            status_code=status.HTTP_202_ACCEPTED,
            content={
                "message": "Sales data refetch for last 11 days started in background",
                "task_id": task_id,
                "date_range": f"{start_date_str} to {end_date_str}",
                "days_covered": 11,
                "marketplace_ids": marketplace_ids or [MARKETPLACE_ID],
                "status": "processing",
                "note": "Existing data will be deleted and refetched. Check console logs for progress updates"
            },
        )

    except Exception as e:
        logger.error(f"❌ Error starting sales refetch: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start sales refetch: {str(e)}"
        )