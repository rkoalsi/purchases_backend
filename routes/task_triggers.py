from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime, timedelta
from bson import ObjectId
from typing import List, Optional
import uuid
import asyncio
import logging

from ..database import get_database, serialize_mongo_document
from ..helpers.scheduler import send_task_assignment_notification

router = APIRouter()
logger = logging.getLogger(__name__)

COLLECTION = "task_trigger_rules"
TASKS_COLLECTION = "tasks"
USERS_COLLECTION = "purchase_users"

# Event types defined in code — adding a new one requires wiring a fire_trigger call in the relevant route
REGISTERED_EVENT_TYPES: dict = {
    "po_created": {
        "name": "PO Created",
        "description": "Fires when a new purchase order is created from draft orders",
        "available_variables": ["po_number", "vendor_name", "brand"],
    },
    "brand_order_created": {
        "name": "Brand Order Created",
        "description": "Fires when a new brand order is created",
        "available_variables": ["brand", "order_name", "po_number"],
    },
    "pil_uploaded": {
        "name": "Product Information Sheet Uploaded",
        "description": "Fires when a Product Information Sheet is uploaded to a brand order",
        "available_variables": ["brand", "order_name", "filename"],
    },
}


class SubTaskTemplate(BaseModel):
    title: str
    description: str = ""
    priority: str = "medium"
    assignee_emails: List[str] = []
    due_date_offset_days: int = 7
    tags: List[str] = []


class TaskTemplate(BaseModel):
    template_id: Optional[str] = None
    title: str
    description: str = ""
    priority: str = "medium"
    assignee_emails: List[str] = []
    due_date_offset_days: int = 7
    tags: List[str] = []
    on_complete_templates: List[SubTaskTemplate] = []


class CreateRuleRequest(BaseModel):
    event_type: str
    task_templates: List[TaskTemplate] = []


class UpdateRuleRequest(BaseModel):
    task_templates: List[TaskTemplate]
    is_active: Optional[bool] = None


def _render(template: str, context: dict) -> str:
    """Replace {{variable}} placeholders with context values."""
    for k, v in context.items():
        template = template.replace(f"{{{{{k}}}}}", str(v or ""))
    return template


def fire_trigger_sync(event_type: str, context: dict, db) -> int:
    """Find the active rule for event_type, render templates, insert tasks. Returns count created."""
    rule = db[COLLECTION].find_one({"event_type": event_type, "is_active": True})
    if not rule or not rule.get("task_templates"):
        return 0

    now = datetime.utcnow()
    task_docs = []
    notify_infos: list[tuple] = []  # (title, user_name, user_dept)

    for tmpl in rule["task_templates"]:
        # Support both legacy single assignee_email and new assignee_emails list
        emails = tmpl.get("assignee_emails") or []
        if not emails and tmpl.get("assignee_email"):
            emails = [tmpl["assignee_email"]]
        if not emails:
            logger.warning("Task trigger '%s': template has no assignees, skipping", event_type)
            continue

        users_found = list(db[USERS_COLLECTION].find(
            {"email": {"$in": emails}, "status": "active"}
        ))
        if not users_found:
            logger.warning(
                "Task trigger '%s': none of %s found or active, skipping template",
                event_type, emails,
            )
            continue

        assigned_ids = [str(u["_id"]) for u in users_found]
        assigned_names = [u.get("name", u["email"]) for u in users_found]
        assigned_depts = [u.get("department", "") for u in users_found]

        offset = int(tmpl.get("due_date_offset_days", 7))
        title = _render(tmpl["title"], context)
        description = _render(tmpl.get("description", ""), context)

        # Render on_complete_templates now so completion logic needs no context
        rendered_on_complete = []
        for sub in tmpl.get("on_complete_templates", []):
            rendered_on_complete.append({
                "title": _render(sub.get("title", ""), context),
                "description": _render(sub.get("description", ""), context),
                "priority": sub.get("priority", "medium"),
                "assignee_emails": list(sub.get("assignee_emails", [])),
                "due_date_offset_days": int(sub.get("due_date_offset_days", 7)),
                "tags": list(sub.get("tags", [])),
            })

        task_docs.append({
            "title": title,
            "description": description,
            "priority": tmpl.get("priority", "medium"),
            "status": "todo",
            "assigned_to": assigned_ids,
            "assigned_to_names": assigned_names,
            "assigned_to_departments": assigned_depts,
            "deadline": (now + timedelta(days=offset)).strftime("%Y-%m-%d"),
            "tags": list(tmpl.get("tags", [])),
            "created_by": "system",
            "created_by_name": "System",
            "creator_department": "",
            "comments": [],
            "attachments": [],
            "activity": [{
                "type": "created",
                "actor_id": "system",
                "actor_name": "System",
                "detail": f"Task auto-created by trigger: {event_type}.",
                "timestamp": now.isoformat(),
            }],
            "on_complete_templates": rendered_on_complete,
            "is_hidden": False,
            "is_deleted": False,
            "created_at": now,
            "updated_at": now,
        })
        notify_infos.append((title, assigned_names, assigned_depts))

    if not task_docs:
        return 0

    db[TASKS_COLLECTION].insert_many(task_docs)

    for title, names, depts in notify_infos:
        try:
            send_task_assignment_notification(
                task_title=title,
                created_by_name="System",
                assigned_by_name="System",
                new_assignees=[{"name": n, "department": d} for n, d in zip(names, depts)],
            )
        except Exception as _e:
            logger.warning("Task trigger Slack notification failed: %s", _e)

    return len(task_docs)


async def fire_trigger(event_type: str, context: dict, db) -> None:
    """Async wrapper — runs fire_trigger_sync in a thread, swallows all errors."""
    try:
        count = await asyncio.to_thread(fire_trigger_sync, event_type, context, db)
        if count:
            logger.info("Task trigger '%s' created %d task(s)", event_type, count)
    except Exception as _e:
        logger.warning("Task trigger '%s' failed: %s", event_type, _e)


# ── CRUD endpoints ────────────────────────────────────────────────────────────

@router.get("/event-types")
async def list_event_types():
    return JSONResponse(content=REGISTERED_EVENT_TYPES)


@router.get("/")
async def list_rules(db=Depends(get_database)):
    def _fetch():
        return list(db[COLLECTION].find({}))
    rules = await asyncio.to_thread(_fetch)
    return JSONResponse(content=[serialize_mongo_document(r) for r in rules])


@router.post("/")
async def create_rule(body: CreateRuleRequest, db=Depends(get_database)):
    if body.event_type not in REGISTERED_EVENT_TYPES:
        raise HTTPException(status_code=400, detail=f"Unknown event_type: {body.event_type}")

    def _insert():
        if db[COLLECTION].find_one({"event_type": body.event_type}):
            raise ValueError(f"A rule for event '{body.event_type}' already exists")
        templates = []
        for t in body.task_templates:
            d = t.dict()
            d["template_id"] = d.get("template_id") or str(uuid.uuid4())
            templates.append(d)
        now = datetime.utcnow()
        doc = {
            "event_type": body.event_type,
            "is_active": True,
            "task_templates": templates,
            "created_at": now,
            "updated_at": now,
        }
        result = db[COLLECTION].insert_one(doc)
        return str(result.inserted_id), doc

    try:
        rule_id, doc = await asyncio.to_thread(_insert)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))

    return JSONResponse(
        status_code=201,
        content={"_id": rule_id, **serialize_mongo_document(doc)},
    )


@router.put("/{rule_id}")
async def update_rule(rule_id: str, body: UpdateRuleRequest, db=Depends(get_database)):
    def _update():
        templates = []
        for t in body.task_templates:
            d = t.dict()
            d["template_id"] = d.get("template_id") or str(uuid.uuid4())
            templates.append(d)
        updates: dict = {"task_templates": templates, "updated_at": datetime.utcnow()}
        if body.is_active is not None:
            updates["is_active"] = body.is_active
        result = db[COLLECTION].update_one(
            {"_id": ObjectId(rule_id)}, {"$set": updates}
        )
        return result.matched_count

    matched = await asyncio.to_thread(_update)
    if not matched:
        raise HTTPException(status_code=404, detail="Rule not found")
    return JSONResponse(content={"ok": True})


@router.patch("/{rule_id}/toggle")
async def toggle_rule(rule_id: str, db=Depends(get_database)):
    def _toggle():
        rule = db[COLLECTION].find_one({"_id": ObjectId(rule_id)}, {"is_active": 1})
        if not rule:
            raise ValueError("not found")
        new_val = not rule.get("is_active", True)
        db[COLLECTION].update_one(
            {"_id": ObjectId(rule_id)},
            {"$set": {"is_active": new_val, "updated_at": datetime.utcnow()}},
        )
        return new_val

    try:
        new_val = await asyncio.to_thread(_toggle)
    except ValueError:
        raise HTTPException(status_code=404, detail="Rule not found")
    return JSONResponse(content={"is_active": new_val})


@router.delete("/{rule_id}")
async def delete_rule(rule_id: str, db=Depends(get_database)):
    def _delete():
        result = db[COLLECTION].delete_one({"_id": ObjectId(rule_id)})
        return result.deleted_count

    deleted = await asyncio.to_thread(_delete)
    if not deleted:
        raise HTTPException(status_code=404, detail="Rule not found")
    return JSONResponse(content={"ok": True})
