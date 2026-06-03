import asyncio
from datetime import datetime
from typing import Optional

from bson import ObjectId
from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel

from ..database import get_database, serialize_mongo_document

router = APIRouter()

NOTIFICATIONS_COLLECTION = "notifications"


@router.get("")
async def get_notifications(
    user_id: str = Query(...),
    limit: int = Query(20, ge=1, le=50),
    db=Depends(get_database),
):
    def _fetch():
        items = list(
            db[NOTIFICATIONS_COLLECTION]
            .find({"user_id": user_id})
            .sort("created_at", -1)
            .limit(limit)
        )
        unread_count = db[NOTIFICATIONS_COLLECTION].count_documents(
            {"user_id": user_id, "read": False}
        )
        return items, unread_count

    items, unread_count = await asyncio.to_thread(_fetch)
    return {
        "items": serialize_mongo_document(items),
        "unread_count": unread_count,
    }


class ReadRequest(BaseModel):
    user_id: str
    notification_id: Optional[str] = None  # omit to mark all read


@router.post("/read")
async def mark_read(request: ReadRequest, db=Depends(get_database)):
    def _mark():
        query: dict = {"user_id": request.user_id}
        if request.notification_id:
            query["_id"] = ObjectId(request.notification_id)
        db[NOTIFICATIONS_COLLECTION].update_many(query, {"$set": {"read": True}})

    await asyncio.to_thread(_mark)
    return {"ok": True}
