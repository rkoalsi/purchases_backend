# blinkit_ads.py
import pandas as pd
from datetime import datetime, date, timedelta
import calendar, io, json
from io import BytesIO
import re

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

ADS_COLLECTION = "blinkit_ads"

# --- FastAPI App ---
router = APIRouter()

def calculate_cpm(budget_consumed, impressions):
    """Calculate CPM using the formula: CPM = Spend/Impressions*1000"""
    if impressions == 0:
        return 0
    return (budget_consumed / impressions) * 1000


def convert_column_name(column_name):
    """Convert column name to lowercase with underscores"""
    if pd.isna(column_name) or column_name is None:
        return "unknown"

    # Convert to string and strip whitespace
    name = str(column_name).strip()

    # Replace spaces and special characters with underscores
    name = re.sub(r"[^a-zA-Z0-9]+", "_", name)

    # Convert to lowercase
    name = name.lower()

    # Remove leading/trailing underscores
    name = name.strip("_")

    return name


def convert_date_field(date_value):
    """Helper function to properly convert date fields to datetime objects for MongoDB"""
    if pd.isna(date_value):
        return None

    try:
        # If it's already a datetime object, return as is
        if isinstance(date_value, datetime):
            return date_value
        # If it's a pandas Timestamp, convert to datetime
        elif isinstance(date_value, pd.Timestamp):
            return date_value.to_pydatetime()
        # If it's a date object, convert to datetime (start of day)
        elif isinstance(date_value, date):
            return datetime.combine(date_value, datetime.min.time())
        # If it's a string, parse to datetime
        elif isinstance(date_value, str):
            # Handle different date formats
            date_formats = [
                "%d-%m-%Y",  # 03-07-2025
                "%Y-%m-%d",  # 2025-07-03
                "%m/%d/%Y",  # 07/03/2025
                "%d/%m/%Y",  # 03/07/2025
                "%Y/%m/%d",  # 2025/07/03
                "%d.%m.%Y",  # 03.07.2025
                "%Y.%m.%d",  # 2025.07.03
            ]

            for date_format in date_formats:
                try:
                    parsed_date = datetime.strptime(date_value.strip(), date_format)
                    return parsed_date
                except ValueError:
                    continue

            # If none of the formats work, try pandas to_datetime
            try:
                parsed_date = pd.to_datetime(
                    date_value, dayfirst=True
                )  # Assume day first for ambiguous dates
                return parsed_date.to_pydatetime()
            except:
                # Last resort - try pandas with infer_datetime_format
                parsed_date = pd.to_datetime(date_value, infer_datetime_format=True)
                return parsed_date.to_pydatetime()
        else:
            # Try to convert other formats to datetime using pandas
            parsed_date = pd.to_datetime(date_value)
            return parsed_date.to_pydatetime()

    except Exception as e:
        logger.warning(
            f"Could not convert date value '{date_value}' of type {type(date_value)}: {str(e)}"
        )
        return str(date_value)


def process_excel_data(file_content: bytes):
    """Process Excel file and return formatted data for both sheets"""
    try:
        # Read both sheets
        product_listing_df = pd.read_excel(
            BytesIO(file_content), sheet_name="PRODUCT_LISTING", engine="openpyxl"
        )

        product_recommendation_df = pd.read_excel(
            BytesIO(file_content),
            sheet_name="PRODUCT_RECOMMENDATION",
            engine="openpyxl",
        )

        processed_data = []

        # Return as string if all conversion attempts fail
        # Process PRODUCT_LISTING sheet
        if not product_listing_df.empty:
            # Convert column names to lowercase with underscores
            product_listing_df.columns = [
                convert_column_name(col) for col in product_listing_df.columns
            ]

            # Add sheet type identifier
            product_listing_df["sheet_type"] = "product_listing"

            # Convert to dictionary records
            for _, row in product_listing_df.iterrows():
                record = row.to_dict()

                # Convert date field properly
                if "date" in record:
                    record["date"] = convert_date_field(record["date"])

                # Handle NaN values
                for key, value in record.items():
                    if pd.isna(value):
                        if key in [
                            "impressions",
                            "direct_atc",
                            "indirect_atc",
                            "direct_quantities_sold",
                            "indirect_quantities_sold",
                            "direct_sales",
                            "indirect_sales",
                            "new_users_acquired",
                            "estimated_budget_consumed",
                            "direct_roas",
                            "total_roas",
                        ]:
                            record[key] = 0
                        else:
                            record[key] = None

                # Add upload timestamp
                record["uploaded_at"] = datetime.now(timezone.utc)

                processed_data.append(record)

        # Process PRODUCT_RECOMMENDATION sheet
        if not product_recommendation_df.empty:
            # Convert column names to lowercase with underscores
            product_recommendation_df.columns = [
                convert_column_name(col) for col in product_recommendation_df.columns
            ]

            # Add sheet type identifier
            product_recommendation_df["sheet_type"] = "product_recommendation"

            # Convert to dictionary records
            for _, row in product_recommendation_df.iterrows():
                record = row.to_dict()

                # Convert date field properly
                if "date" in record:
                    record["date"] = convert_date_field(record["date"])

                # Handle NaN values
                for key, value in record.items():
                    if pd.isna(value):
                        if key in [
                            "impressions",
                            "direct_atc",
                            "indirect_atc",
                            "direct_quantities_sold",
                            "indirect_quantities_sold",
                            "direct_sales",
                            "indirect_sales",
                            "new_users_acquired",
                            "estimated_budget_consumed",
                            "direct_roas",
                            "total_roas",
                        ]:
                            record[key] = 0
                        else:
                            record[key] = None

                # Add upload timestamp
                record["uploaded_at"] = datetime.now(timezone.utc)

                processed_data.append(record)

        return processed_data

    except Exception as e:
        logger.error(f"Error processing Excel data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Error processing Excel file: {str(e)}",
        )


@router.post("/upload")
async def upload_blinkit_ads(file: UploadFile = File(...)):
    """Upload and process blinkit ads Excel file with two sheets"""

    try:
        # Validate file type
        if not file.filename.endswith((".xlsx", ".xls")):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Only Excel files (.xlsx, .xls) are allowed",
            )

        # Read file content
        file_content = await file.read()

        if len(file_content) == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Uploaded file is empty"
            )

        # Process the Excel data
        processed_data = process_excel_data(file_content)

        if not processed_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No valid data found in the Excel file",
            )

        # Get database connection
        db = get_database()
        collection = db[ADS_COLLECTION]

        # Get date range from processed data
        dates = [
            record["date"]
            for record in processed_data
            if record.get("date") and record["date"] is not None
        ]
        delete_result = None
        date_range_info = {"start_date": None, "end_date": None}

        if dates:
            min_date = min(dates)
            max_date = max(dates)

            # Convert datetime objects to strings for JSON serialization
            date_range_info = {
                "start_date": (
                    min_date.strftime("%Y-%m-%d")
                    if isinstance(min_date, datetime)
                    else str(min_date)
                ),
                "end_date": (
                    max_date.strftime("%Y-%m-%d")
                    if isinstance(max_date, datetime)
                    else str(max_date)
                ),
            }

            # Delete existing data for this date range
            delete_filter = {"date": {"$gte": min_date, "$lte": max_date}}

            delete_result = collection.delete_many(delete_filter)
            logger.info(
                f"Deleted {delete_result.deleted_count} existing records for date range {min_date} to {max_date}"
            )

        # Insert data in batches
        batch_size = 1000
        total_inserted = 0

        for i in range(0, len(processed_data), batch_size):
            batch = processed_data[i : i + batch_size]
            try:
                result = collection.insert_many(batch)
                total_inserted += len(result.inserted_ids)
                logger.info(
                    f"Inserted batch {i//batch_size + 1}: {len(result.inserted_ids)} records"
                )
            except Exception as e:
                logger.error(f"Error inserting batch {i//batch_size + 1}: {str(e)}")
                # Continue with next batch instead of failing completely
                continue

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content={
                "message": "File uploaded and processed successfully",
                "filename": file.filename,
                "total_records_processed": len(processed_data),
                "total_records_inserted": total_inserted,
                "records_deleted": delete_result.deleted_count if delete_result else 0,
                "date_range_processed": date_range_info,
                "product_listing_records": len(
                    [
                        r
                        for r in processed_data
                        if r.get("sheet_type") == "product_listing"
                    ]
                ),
                "product_recommendation_records": len(
                    [
                        r
                        for r in processed_data
                        if r.get("sheet_type") == "product_recommendation"
                    ]
                ),
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Unexpected error in upload_blinkit_ads: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Internal server error: {str(e)}",
        )


def parse_date_string(date_str: str):
    """Parse date string to datetime object for MongoDB filtering"""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        try:
            return datetime.strptime(date_str, "%d-%m-%Y")
        except ValueError:
            try:
                return pd.to_datetime(date_str).to_pydatetime()
            except:
                raise ValueError(f"Unable to parse date: {date_str}")


@router.get("/campaigns/summary")
def get_campaigns_summary(
    start_date: str = None,
    end_date: str = None,
    sheet_type: str = None
):
    """Get high-level summary - UPDATED with calculated CPM and total units"""
    
    try:
        db = get_database()
        collection = db[ADS_COLLECTION]
        
        # Build match filter
        match_filter = {}
        
        if start_date and end_date:
            try:
                start_date_obj = parse_date_string(start_date)
                end_date_obj = parse_date_string(end_date)
                end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)
                
                match_filter["date"] = {
                    "$gte": start_date_obj,
                    "$lte": end_date_obj
                }
            except ValueError as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid date format: {str(e)}"
                )
        
        if sheet_type:
            match_filter["sheet_type"] = sheet_type

        # FIXED: Use the same grouping logic as campaigns API to get accurate count
        campaigns_count_pipeline = [
            {"$match": match_filter},
            {
                "$group": {
                    "_id": {
                        "campaign_name": "$campaign_name",
                        "campaign_id": "$campaign_id"
                    }
                }
            },
            {
                "$group": {
                    "_id": None,
                    "total_campaigns": {"$sum": 1}
                }
            }
        ]

        # Get overall statistics - UPDATED with calculated CPM and total units
        overall_pipeline = [
            {"$match": match_filter},
            {
                "$group": {
                    "_id": None,
                    "total_records": {"$sum": 1},
                    "total_impressions": {"$sum": "$impressions"},
                    "total_direct_sales": {"$sum": "$direct_sales"},
                    "total_indirect_sales": {"$sum": "$indirect_sales"},
                    "total_budget_consumed": {"$sum": "$estimated_budget_consumed"},
                    "total_direct_units": {"$sum": "$direct_quantities_sold"},
                    "total_indirect_units": {"$sum": "$indirect_quantities_sold"},
                    "total_direct_atc": {"$sum": "$direct_atc"},
                    "total_indirect_atc": {"$sum": "$indirect_atc"},
                    "sheet_types": {"$addToSet": "$sheet_type"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "total_records": 1,
                    "total_impressions": 1,
                    "total_direct_sales": {"$round": ["$total_direct_sales", 2]},
                    "total_indirect_sales": {"$round": ["$total_indirect_sales", 2]},
                    "total_sales": {"$round": [{"$add": ["$total_direct_sales", "$total_indirect_sales"]}, 2]},
                    "total_budget_consumed": {"$round": ["$total_budget_consumed", 2]},
                    "total_direct_units": 1,
                    "total_indirect_units": 1,
                    "total_units": {"$add": ["$total_direct_units", "$total_indirect_units"]},
                    "total_direct_atc": 1,
                    "total_indirect_atc": 1,
                    "total_atc": {"$add": ["$total_direct_atc", "$total_indirect_atc"]},
                    "calculated_cpm": {
                        "$cond": [
                            {"$eq": ["$total_impressions", 0]},
                            0,
                            {"$round": [{"$multiply": [{"$divide": ["$total_budget_consumed", "$total_impressions"]}, 1000]}, 2]}
                        ]
                    },
                    "sheet_types": 1
                }
            }
        ]

        # Execute both pipelines
        campaign_count_result = list(collection.aggregate(campaigns_count_pipeline))
        overall_result = list(collection.aggregate(overall_pipeline))

        # Extract results
        campaign_count = campaign_count_result[0]["total_campaigns"] if campaign_count_result else 0
        overall = overall_result[0] if overall_result else {}

        # Combine results
        summary = {
            "total_campaigns": campaign_count,
            "total_records": overall.get("total_records", 0),
            "total_impressions": overall.get("total_impressions", 0),
            "total_direct_sales": overall.get("total_direct_sales", 0),
            "total_indirect_sales": overall.get("total_indirect_sales", 0),
            "total_sales": overall.get("total_sales", 0),
            "total_budget_consumed": overall.get("total_budget_consumed", 0),
            "total_units": overall.get("total_units", 0),
            "total_atc": overall.get("total_atc", 0),
            "calculated_cpm": overall.get("calculated_cpm", 0),
            "sheet_types": overall.get("sheet_types", [])
        }

        # Get campaign summaries for the campaigns array - UPDATED
        campaign_summary_pipeline = [
            {"$match": match_filter},
            {
                "$group": {
                    "_id": {
                        "campaign_name": "$campaign_name",
                        "campaign_id": "$campaign_id"
                    },
                    "sheet_types": {"$addToSet": "$sheet_type"},
                    "total_impressions": {"$sum": "$impressions"},
                    "total_direct_sales": {"$sum": "$direct_sales"},
                    "total_indirect_sales": {"$sum": "$indirect_sales"},
                    "total_budget_consumed": {"$sum": "$estimated_budget_consumed"},
                    "total_direct_units": {"$sum": "$direct_quantities_sold"},
                    "total_indirect_units": {"$sum": "$indirect_quantities_sold"},
                    "total_direct_atc": {"$sum": "$direct_atc"},
                    "total_indirect_atc": {"$sum": "$indirect_atc"},
                    "avg_direct_roas": {"$avg": "$direct_roas"},
                    "avg_total_roas": {"$avg": "$total_roas"},
                    "unique_targeting_groups": {
                        "$addToSet": {
                            "$cond": [
                                {"$eq": ["$sheet_type", "product_listing"]},
                                "$targeting_value",
                                "$targeting_type"
                            ]
                        }
                    },
                    "date_range": {"$push": "$date"}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "campaign_name": "$_id.campaign_name",
                    "campaign_id": "$_id.campaign_id",
                    "sheet_types": 1,
                    "total_impressions": 1,
                    "total_direct_sales": {"$round": ["$total_direct_sales", 2]},
                    "total_indirect_sales": {"$round": ["$total_indirect_sales", 2]},
                    "total_sales": {"$round": [{"$add": ["$total_direct_sales", "$total_indirect_sales"]}, 2]},
                    "total_budget_consumed": {"$round": ["$total_budget_consumed", 2]},
                    "total_units": {"$add": ["$total_direct_units", "$total_indirect_units"]},
                    "total_atc": {"$add": ["$total_direct_atc", "$total_indirect_atc"]},
                    "calculated_cpm": {
                        "$cond": [
                            {"$eq": ["$total_impressions", 0]},
                            0,
                            {"$round": [{"$multiply": [{"$divide": ["$total_budget_consumed", "$total_impressions"]}, 1000]}, 2]}
                        ]
                    },
                    "avg_direct_roas": {"$round": ["$avg_direct_roas", 2]},
                    "avg_total_roas": {"$round": ["$avg_total_roas", 2]},
                    "targeting_groups_count": {"$size": "$unique_targeting_groups"},
                    "date_range": {
                        "start_date": {"$dateToString": {"format": "%Y-%m-%d", "date": {"$min": "$date_range"}}},
                        "end_date": {"$dateToString": {"format": "%Y-%m-%d", "date": {"$max": "$date_range"}}}
                    }
                }
            },
            {"$sort": {"total_sales": -1}}
        ]

        campaigns = list(collection.aggregate(campaign_summary_pipeline))

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "summary": summary,
                "campaigns": campaigns,
                "filters_applied": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "sheet_type": sheet_type
                }
            }
        )
        
    except Exception as e:
        logger.error(f"Error in get_campaigns_summary: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching campaign summary: {str(e)}"
        )


@router.get("/campaigns")
def get_campaigns_with_keywords(
    start_date: str = None,
    end_date: str = None,
    campaign_name: str = None,
    sheet_type: str = None,
):
    """Get campaign-wise data with keyword/targeting group information - UPDATED VERSION"""

    try:
        db = get_database()
        collection = db[ADS_COLLECTION]

        # Build match filter
        match_filter = {}

        if start_date and end_date:
            try:
                start_date_obj = parse_date_string(start_date)
                end_date_obj = parse_date_string(end_date)
                end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)

                match_filter["date"] = {"$gte": start_date_obj, "$lte": end_date_obj}
            except ValueError as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid date format: {str(e)}",
                )

        if campaign_name:
            match_filter["campaign_name"] = {"$regex": campaign_name, "$options": "i"}

        if sheet_type:
            match_filter["sheet_type"] = sheet_type

        # Aggregation pipeline - UPDATED with calculated CPM and new metrics
        pipeline = [
            {"$match": match_filter},
            {
                "$group": {
                    "_id": {
                        "campaign_name": "$campaign_name",
                        "campaign_id": "$campaign_id",
                        "targeting_group": {
                            "$cond": [
                                {"$eq": ["$sheet_type", "product_listing"]},
                                "$targeting_value",
                                "$targeting_type",
                            ]
                        },
                        "targeting_type": "$targeting_type",
                        "match_type": "$match_type",
                        "sheet_type": "$sheet_type",
                    },
                    "total_impressions": {"$sum": "$impressions"},
                    "total_direct_atc": {"$sum": "$direct_atc"},
                    "total_indirect_atc": {"$sum": "$indirect_atc"},
                    "total_direct_quantities_sold": {"$sum": "$direct_quantities_sold"},
                    "total_indirect_quantities_sold": {
                        "$sum": "$indirect_quantities_sold"
                    },
                    "total_direct_sales": {"$sum": "$direct_sales"},
                    "total_indirect_sales": {"$sum": "$indirect_sales"},
                    "total_new_users_acquired": {"$sum": "$new_users_acquired"},
                    "total_budget_consumed": {"$sum": "$estimated_budget_consumed"},
                    "avg_direct_roas": {"$avg": "$direct_roas"},
                    "avg_total_roas": {"$avg": "$total_roas"},
                    "date_range": {"$push": "$date"},
                    "record_count": {"$sum": 1},
                }
            },
            {
                "$group": {
                    "_id": {
                        "campaign_name": "$_id.campaign_name",
                        "campaign_id": "$_id.campaign_id",
                    },
                    "targeting_groups": {
                        "$push": {
                            "targeting_group": "$_id.targeting_group",
                            "targeting_type": "$_id.targeting_type",
                            "match_type": "$_id.match_type",
                            "sheet_type": "$_id.sheet_type",
                            "metrics": {
                                "total_impressions": "$total_impressions",
                                "total_direct_atc": "$total_direct_atc",
                                "total_indirect_atc": "$total_indirect_atc",
                                "total_atc": {"$add": ["$total_direct_atc", "$total_indirect_atc"]},
                                "total_direct_quantities_sold": "$total_direct_quantities_sold",
                                "total_indirect_quantities_sold": "$total_indirect_quantities_sold",
                                "total_units": {"$add": ["$total_direct_quantities_sold", "$total_indirect_quantities_sold"]},
                                "total_direct_sales": {
                                    "$round": ["$total_direct_sales", 2]
                                },
                                "total_indirect_sales": {
                                    "$round": ["$total_indirect_sales", 2]
                                },
                                "total_sales": {
                                    "$round": [{"$add": ["$total_direct_sales", "$total_indirect_sales"]}, 2]
                                },
                                "total_new_users_acquired": "$total_new_users_acquired",
                                "total_budget_consumed": {
                                    "$round": ["$total_budget_consumed", 2]
                                },
                                "calculated_cpm": {
                                    "$cond": [
                                        {"$eq": ["$total_impressions", 0]},
                                        0,
                                        {"$round": [{"$multiply": [{"$divide": ["$total_budget_consumed", "$total_impressions"]}, 1000]}, 2]}
                                    ]
                                },
                                "avg_direct_roas": {"$round": ["$avg_direct_roas", 2]},
                                "avg_total_roas": {"$round": ["$avg_total_roas", 2]},
                                "record_count": "$record_count",
                            },
                            "date_range": {
                                "start_date": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%d",
                                        "date": {"$min": "$date_range"},
                                    }
                                },
                                "end_date": {
                                    "$dateToString": {
                                        "format": "%Y-%m-%d",
                                        "date": {"$max": "$date_range"},
                                    }
                                },
                            },
                        }
                    },
                    "campaign_total_impressions": {"$sum": "$total_impressions"},
                    "campaign_total_direct_atc": {"$sum": "$total_direct_atc"},
                    "campaign_total_indirect_atc": {"$sum": "$total_indirect_atc"},
                    "campaign_total_direct_quantities_sold": {
                        "$sum": "$total_direct_quantities_sold"
                    },
                    "campaign_total_indirect_quantities_sold": {
                        "$sum": "$total_indirect_quantities_sold"
                    },
                    "campaign_total_direct_sales": {"$sum": "$total_direct_sales"},
                    "campaign_total_indirect_sales": {"$sum": "$total_indirect_sales"},
                    "campaign_total_new_users_acquired": {
                        "$sum": "$total_new_users_acquired"
                    },
                    "campaign_total_budget_consumed": {
                        "$sum": "$total_budget_consumed"
                    },
                    "campaign_avg_direct_roas": {"$avg": "$avg_direct_roas"},
                    "campaign_avg_total_roas": {"$avg": "$avg_total_roas"},
                }
            },
            {
                "$addFields": {
                    "campaign_totals": {
                        "total_impressions": "$campaign_total_impressions",
                        "total_direct_atc": "$campaign_total_direct_atc",
                        "total_indirect_atc": "$campaign_total_indirect_atc",
                        "total_atc": {"$add": ["$campaign_total_direct_atc", "$campaign_total_indirect_atc"]},
                        "total_direct_quantities_sold": "$campaign_total_direct_quantities_sold",
                        "total_indirect_quantities_sold": "$campaign_total_indirect_quantities_sold",
                        "total_units": {"$add": ["$campaign_total_direct_quantities_sold", "$campaign_total_indirect_quantities_sold"]},
                        "total_direct_sales": {
                            "$round": ["$campaign_total_direct_sales", 2]
                        },
                        "total_indirect_sales": {
                            "$round": ["$campaign_total_indirect_sales", 2]
                        },
                        "total_sales": {
                            "$round": [{"$add": ["$campaign_total_direct_sales", "$campaign_total_indirect_sales"]}, 2]
                        },
                        "total_new_users_acquired": "$campaign_total_new_users_acquired",
                        "total_budget_consumed": {
                            "$round": ["$campaign_total_budget_consumed", 2]
                        },
                        "calculated_cpm": {
                            "$cond": [
                                {"$eq": ["$campaign_total_impressions", 0]},
                                0,
                                {"$round": [{"$multiply": [{"$divide": ["$campaign_total_budget_consumed", "$campaign_total_impressions"]}, 1000]}, 2]}
                            ]
                        },
                        "avg_direct_roas": {"$round": ["$campaign_avg_direct_roas", 2]},
                        "avg_total_roas": {"$round": ["$campaign_avg_total_roas", 2]},
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "campaign_name": "$_id.campaign_name",
                    "campaign_id": "$_id.campaign_id",
                    "targeting_groups": 1,
                    "campaign_totals": 1,
                    "targeting_groups_count": {"$size": "$targeting_groups"},
                }
            },
            {"$sort": {"campaign_name": 1}},
        ]

        # Execute aggregation
        result = list(collection.aggregate(pipeline))

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "total_campaigns": len(result),
                "campaigns": result,
                "filters_applied": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "campaign_name": campaign_name,
                    "sheet_type": sheet_type,
                },
            },
        )

    except Exception as e:
        logger.error(f"Error in get_campaigns_with_keywords: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching campaign data: {str(e)}",
        )
        
@router.get("/data/raw")
def get_raw_data(
    start_date: str = None,
    end_date: str = None,
    campaign_name: str = None,
    sheet_type: str = None,
    limit: int = 100,
    skip: int = 0,
):
    """Get raw data from the database with filters"""

    try:
        db = get_database()
        collection = db[ADS_COLLECTION]

        # Build query filter
        query_filter = {}

        if start_date and end_date:
            # Convert string dates to datetime objects for proper filtering
            try:
                start_date_obj = parse_date_string(start_date)
                end_date_obj = parse_date_string(end_date)
                # Set end date to end of day for inclusive range
                end_date_obj = end_date_obj.replace(hour=23, minute=59, second=59)

                query_filter["date"] = {"$gte": start_date_obj, "$lte": end_date_obj}
            except ValueError as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid date format: {str(e)}",
                )

        if campaign_name:
            query_filter["campaign_name"] = {"$regex": campaign_name, "$options": "i"}

        if sheet_type:
            query_filter["sheet_type"] = sheet_type

        # Get total count
        total_count = collection.count_documents(query_filter)

        # Get data with pagination
        cursor = collection.find(query_filter).skip(skip).limit(limit).sort("date", -1)

        # Convert to list and serialize
        data = []
        for doc in cursor:
            serialized_doc = serialize_mongo_document(doc)
            data.append(serialized_doc)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "total_records": total_count,
                "returned_records": len(data),
                "skip": skip,
                "limit": limit,
                "data": data,
                "filters_applied": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "campaign_name": campaign_name,
                    "sheet_type": sheet_type,
                },
            },
        )

    except Exception as e:
        logger.error(f"Error in get_raw_data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error fetching raw data: {str(e)}",
        )


@router.delete("/data/clear")
def clear_all_data():
    """Clear all data from the collection (use with caution)"""

    try:
        db = get_database()
        collection = db[ADS_COLLECTION]

        result = collection.delete_many({})

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Successfully deleted {result.deleted_count} records",
                "deleted_count": result.deleted_count,
            },
        )

    except Exception as e:
        logger.error(f"Error in clear_all_data: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error clearing data: {str(e)}",
        )


@router.delete("/data/clear_date_range")
def clear_data_by_date_range(start_date: str, end_date: str):
    """Clear data for a specific date range"""

    try:
        db = get_database()
        collection = db[ADS_COLLECTION]

        # Convert string dates to date objects for proper MongoDB filtering
        try:
            start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid date format. Use YYYY-MM-DD format. Provided: start_date={start_date}, end_date={end_date}",
            )

        # Build delete filter
        delete_filter = {"date": {"$gte": start_date_obj, "$lte": end_date_obj}}

        result = collection.delete_many(delete_filter)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "message": f"Successfully deleted {result.deleted_count} records for date range {start_date} to {end_date}",
                "deleted_count": result.deleted_count,
                "date_range": {"start_date": start_date, "end_date": end_date},
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in clear_data_by_date_range: {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error clearing data by date range: {str(e)}",
        )


@router.get("")
def get_blinkit_ads():
    """Get basic information about the blinkit ads API"""

    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "message": "Blinkit Ads API",
            "version": "1.0.0",
            "endpoints": {
                "POST /upload": "Upload Excel file with PRODUCT_LISTING and PRODUCT_RECOMMENDATION sheets",
                "GET /campaigns": "Get campaign-wise data with targeting groups (keywords/recommendations)",
                "GET /campaigns/summary": "Get high-level summary of all campaigns",
                "GET /data/raw": "Get raw data with filters and pagination",
                "DELETE /data/clear": "Clear all data from collection",
                "DELETE /data/clear_date_range": "Clear data for specific date range",
            },
            "collection": ADS_COLLECTION,
        },
    )
