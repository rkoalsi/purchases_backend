from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from ..database import get_database, serialize_mongo_document
import logging
router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("")
async def get_users():
    try:
        db = get_database()
        result = list(db["purchase_users"].find({}))
        if result:
            return JSONResponse(
                status_code=200, content=serialize_mongo_document(result)
            )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.info(f"Error uploading sales data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the file: {e}",
        )


@router.get("/permissions")
async def get_permissions():
    try:
        db = get_database()
        result = list(db["purchase_permissions"].find({}))
        if result:
            return JSONResponse(
                status_code=200, content=serialize_mongo_document(result)
            )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.info(f"Error uploading sales data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred processing the file: {e}",
        )
