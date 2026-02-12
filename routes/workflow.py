from datetime import datetime, timedelta
from typing import List, Optional
from fastapi import Query, APIRouter, HTTPException, status
from ..database import get_database, serialize_mongo_document
from ..schema.workflow import (
    Task,
    TaskCreate,
    TaskCRUD,
    TaskStatus,
    TaskType,
    TaskUpdate,
    TriggerCondition,
    TriggerResult,
    TriggerType,
    TriggerValidator,
    Workflow,
    WorkflowCreate,
    WorkflowUpdate,
    WorkflowStatus,
    GetWorkflowResponse,
)
from bson import ObjectId

router = APIRouter()


@router.get("/", response_model=dict)
def get_workflows():
    try:
        db = get_database()
        workflows = list(db.workflows.find({}))
        return {
            "workflows": serialize_mongo_document(workflows),
            "status": status.HTTP_200_OK,
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Internal server error: {str(e)}",
        )


@router.post("/", response_model=dict)
def create_workflow(data: WorkflowCreate):
    try:
        db = get_database()
        workflow_dict = data.model_dump()
        result = db.workflows.insert_one(workflow_dict)
        created_workflow = db.workflows.find_one({"_id": result.inserted_id})
        if created_workflow:
            created_workflow["_id"] = str(created_workflow["_id"])
            for field in ["created_by", "updated_by", "department"]:
                if field in created_workflow and created_workflow[field]:
                    created_workflow[field] = str(created_workflow[field])
        return {
            "workflow": created_workflow,
            "status": status.HTTP_201_CREATED,  # ✅ Use 201 for creation
            "message": "Workflow created successfully",
        }

    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Failed to create workflow: {str(e)}",
        )


@router.delete("/", response_model=dict)
def delete_workflow(id: str):
    try:
        db = get_database()
        workflow = db.workflows.delete_one({"_id": ObjectId(id)})
        if workflow.deleted_count > 0:
            return {
                "response": "Workflow Successfully Deleted",
                status: status.HTTP_200_OK,
            }
        else:
            return {
                "response": "Error Deleting Workflow.",
                status: status.HTTP_400_BAD_REQUEST,
            }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Internal server error: {str(e)}",
        )
