# items.py
import pandas as pd
from datetime import datetime, date
import calendar, io, json
from io import BytesIO

from fastapi import APIRouter, UploadFile, File, HTTPException, status, Depends
from fastapi.responses import JSONResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware

from pymongo import ASCENDING
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document

# --- Configuration ---
# Use environment variables for production

SKU_COLLECTION = "blinkit_sku_mapping"
SALES_COLLECTION = "blinkit_sales"
INVENTORY_COLLECTION = "blinkit_inventory"
REPORT_COLLECTION = "blinkit_sales_inventory_reports"
PRODUCTS_COLLECTION = "products"

router = APIRouter()


@router.get("/get_all_products")
async def get_all_products(database=Depends(get_database)):
    """
    Retrieves all documents from the Products collection.
    """
    try:
        products = database.get_collection(PRODUCTS_COLLECTION)
        products = list(products.find({}, {"_id": 0}))

        return serialize_mongo_document(products)

    except PyMongoError as e:  # Catch database-specific errors
        print(f"Database error retrieving Products: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error retrieving Products: {e}",
        )
    except Exception as e:  # Catch any other unexpected errors
        print(f"Unexpected error retrieving Products: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}",
        )
