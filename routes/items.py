# items.py
import logging
from fastapi import APIRouter, HTTPException, status, Depends
from pymongo.errors import PyMongoError
from ..database import get_database, serialize_mongo_document

SKU_COLLECTION = "blinkit_sku_mapping"
SALES_COLLECTION = "blinkit_sales_temp"
INVENTORY_COLLECTION = "blinkit_inventory"
REPORT_COLLECTION = "blinkit_sales_inventory_reports"
PRODUCTS_COLLECTION = "products"

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/get_all_products")
def get_all_products(database=Depends(get_database)):
    """
    Retrieves all documents from the Products collection.
    """
    try:
        products = database.get_collection(PRODUCTS_COLLECTION)
        products = list(products.find({}, {"_id": 0}))

        return serialize_mongo_document(products)

    except PyMongoError as e:  # Catch database-specific errors
        logger.info(f"Database error retrieving Products: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Database error retrieving Products: {e}",
        )
    except Exception as e:  # Catch any other unexpected errors
        logger.info(f"Unexpected error retrieving Products: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An unexpected error occurred: {e}",
        )
