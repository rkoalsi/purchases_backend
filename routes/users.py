from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from ..database import get_database, serialize_mongo_document
from bson import ObjectId
from pydantic import BaseModel
from typing import List, Optional
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


class UpdateUserRequest(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    role: Optional[str] = None
    status: Optional[str] = None


class UpdateUserPermissionsRequest(BaseModel):
    permissions: List[str]


class CreatePermissionRequest(BaseModel):
    name: str
    description: Optional[str] = ""
    is_active: Optional[bool] = True


class UpdatePermissionRequest(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    is_active: Optional[bool] = None


@router.get("")
def get_users():
    try:
        db = get_database()
        result = list(db["purchase_users"].find({}))
        return JSONResponse(
            status_code=200,
            content=serialize_mongo_document(result) if result else [],
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error fetching users: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred: {e}",
        )


@router.put("/{user_id}")
def update_user(user_id: str, request: UpdateUserRequest):
    try:
        db = get_database()
        update_data = {k: v for k, v in request.model_dump().items() if v is not None}
        if not update_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields to update",
            )
        result = db["purchase_users"].update_one(
            {"_id": ObjectId(user_id)}, {"$set": update_data}
        )
        if result.matched_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
            )
        updated = db["purchase_users"].find_one({"_id": ObjectId(user_id)})
        return JSONResponse(
            status_code=200, content=serialize_mongo_document(updated)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating user: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred: {e}",
        )


@router.put("/{user_id}/permissions")
def update_user_permissions(user_id: str, request: UpdateUserPermissionsRequest):
    try:
        db = get_database()
        result = db["purchase_users"].update_one(
            {"_id": ObjectId(user_id)},
            {"$set": {"permissions": request.permissions}},
        )
        if result.matched_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="User not found"
            )
        updated = db["purchase_users"].find_one({"_id": ObjectId(user_id)})
        return JSONResponse(
            status_code=200, content=serialize_mongo_document(updated)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating user permissions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred: {e}",
        )


@router.get("/permissions")
def get_permissions():
    try:
        db = get_database()
        result = list(db["purchase_permissions"].find({}))
        return JSONResponse(
            status_code=200,
            content=serialize_mongo_document(result) if result else [],
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error fetching permissions: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred: {e}",
        )


@router.post("/permissions")
def create_permission(request: CreatePermissionRequest):
    try:
        db = get_database()
        # Check for duplicate name
        existing = db["purchase_permissions"].find_one({"name": request.name})
        if existing:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Permission '{request.name}' already exists",
            )
        doc = request.model_dump()
        result = db["purchase_permissions"].insert_one(doc)
        created = db["purchase_permissions"].find_one({"_id": result.inserted_id})
        return JSONResponse(
            status_code=201, content=serialize_mongo_document(created)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error creating permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred: {e}",
        )


@router.put("/permissions/{permission_id}")
def update_permission(permission_id: str, request: UpdatePermissionRequest):
    try:
        db = get_database()
        update_data = {k: v for k, v in request.model_dump().items() if v is not None}
        if not update_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No fields to update",
            )
        result = db["purchase_permissions"].update_one(
            {"_id": ObjectId(permission_id)}, {"$set": update_data}
        )
        if result.matched_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Permission not found",
            )
        updated = db["purchase_permissions"].find_one(
            {"_id": ObjectId(permission_id)}
        )
        return JSONResponse(
            status_code=200, content=serialize_mongo_document(updated)
        )
    except HTTPException as e:
        raise e
    except Exception as e:
        logger.error(f"Error updating permission: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"An error occurred: {e}",
        )
