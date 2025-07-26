from pydantic import BaseModel, Field, ConfigDict
from typing import List, Optional, Dict, Any, Annotated
from datetime import datetime
from bson import ObjectId
from enum import Enum
import re


# Custom ObjectId type for Pydantic v2
class PyObjectId(ObjectId):
    @classmethod
    def __get_pydantic_core_schema__(cls, _source_type, _handler):
        from pydantic_core import core_schema

        return core_schema.no_info_after_validator_function(
            cls.validate,
            core_schema.str_schema(),
            serialization=core_schema.to_string_ser_schema(),
        )

    @classmethod
    def validate(cls, v):
        if isinstance(v, ObjectId):
            return v
        if isinstance(v, str):
            return ObjectId(v)
        raise ValueError("Invalid ObjectId")


class TaskStatus(str, Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    CANCELLED = "cancelled"
    ON_HOLD = "on_hold"
    WAITING_FOR_TRIGGER = "waiting_for_trigger"


class WorkflowStatus(str, Enum):
    DRAFT = "draft"
    ACTIVE = "active"
    COMPLETED = "completed"
    ARCHIVED = "archived"
    CANCELLED = "cancelled"
    WAITING_FOR_TRIGGER = "waiting_for_trigger"


class Priority(int, Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    URGENT = 4
    CRITICAL = 5


class TaskType(str, Enum):
    SEQUENTIAL = "sequential"
    PARALLEL = "parallel"


class TriggerType(str, Enum):
    FILE_UPLOAD = "file_upload"
    LINK_SUBMISSION = "link_submission"
    FORM_SUBMISSION = "form_submission"
    APPROVAL = "approval"
    EXTERNAL_API = "external_api"
    TIME_BASED = "time_based"
    MANUAL = "manual"
    DEPENDENCY_COMPLETE = "dependency_complete"


class TriggerCondition(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    type: TriggerType
    description: str
    required: bool = True

    # Specific conditions based on trigger type
    min_files: Optional[int] = None
    allowed_file_types: Optional[List[str]] = None
    max_file_size: Optional[int] = None

    min_links: Optional[int] = None
    link_pattern: Optional[str] = None

    form_fields: Optional[List[str]] = None

    approver_roles: Optional[List[str]] = None
    required_approvals: Optional[int] = None

    api_endpoint: Optional[str] = None
    expected_response: Optional[Dict[str, Any]] = None

    trigger_date: Optional[datetime] = None

    custom_validation: Optional[str] = None


class TriggerResult(BaseModel):
    condition_id: str
    is_satisfied: bool
    checked_at: datetime = Field(default_factory=datetime.now)
    details: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None


class Upload(BaseModel):
    name: str
    url: str
    type: str
    size: Optional[int] = None
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    created_by: PyObjectId  # ✅ Fixed: removed alias
    updated_by: Optional[PyObjectId] = None  # ✅ Fixed: removed alias


class FormSubmission(BaseModel):
    form_id: str
    data: Dict[str, Any]
    submitted_by: PyObjectId  # ✅ Fixed: removed alias
    submitted_at: datetime = Field(default_factory=datetime.now)


class Approval(BaseModel):
    approver_id: PyObjectId  # ✅ Fixed: removed alias
    approved: bool
    comments: Optional[str] = None
    approved_at: datetime = Field(default_factory=datetime.now)


class TaskCreate(BaseModel):
    name: str
    description: str
    created_by: PyObjectId  # ✅ Fixed: removed alias
    department: PyObjectId  # ✅ Fixed: removed alias
    type: TaskType = TaskType.SEQUENTIAL
    order: int
    assigned_to: Optional[PyObjectId] = None  # ✅ Fixed: removed alias
    dependencies: List[PyObjectId] = Field(
        default_factory=list
    )  # ✅ Fixed: removed alias
    notify_on_completion: List[PyObjectId] = Field(
        default_factory=list
    )  # ✅ Fixed: removed alias
    notes: Optional[str] = None
    files: List[Upload] = Field(default_factory=list)
    links: List[Upload] = Field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    priority: Priority = Priority.LOW
    trigger_conditions: List[TriggerCondition] = Field(default_factory=list)
    auto_start: bool = True


class TaskUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    assigned_to: Optional[PyObjectId] = None  # ✅ Fixed: removed alias
    dependencies: Optional[List[PyObjectId]] = None  # ✅ Fixed: removed alias
    notify_on_completion: Optional[List[PyObjectId]] = None  # ✅ Fixed: removed alias
    notes: Optional[str] = None
    files: Optional[List[Upload]] = None
    links: Optional[List[Upload]] = None
    status: Optional[TaskStatus] = None
    priority: Optional[Priority] = None
    type: Optional[TaskType] = None
    order: Optional[int] = None
    updated_by: Optional[PyObjectId] = None  # ✅ Fixed: removed alias
    trigger_conditions: Optional[List[TriggerCondition]] = None
    auto_start: Optional[bool] = None


class Task(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={ObjectId: str, datetime: lambda dt: dt.isoformat()},
    )

    # Required fields
    name: str
    description: str
    created_by: PyObjectId  # ✅ Fixed: removed alias
    department: PyObjectId  # ✅ Fixed: removed alias
    type: TaskType = TaskType.SEQUENTIAL
    order: int

    # Optional fields with defaults
    id: Optional[Annotated[PyObjectId, Field(alias="_id")]] = (
        None  # ✅ This one keeps the alias as it's the actual document ID
    )
    assigned_to: Optional[PyObjectId] = None  # ✅ Fixed: removed alias
    dependencies: List[PyObjectId] = Field(
        default_factory=list
    )  # ✅ Fixed: removed alias
    updated_by: Optional[PyObjectId] = None  # ✅ Fixed: removed alias
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    notify_on_completion: List[PyObjectId] = Field(
        default_factory=list
    )  # ✅ Fixed: removed alias
    notes: Optional[str] = None
    files: List[Upload] = Field(default_factory=list)
    links: List[Upload] = Field(default_factory=list)
    status: TaskStatus = TaskStatus.PENDING
    priority: Priority = Priority.LOW

    # Trigger-related fields
    trigger_conditions: List[TriggerCondition] = Field(default_factory=list)
    trigger_results: List[TriggerResult] = Field(default_factory=list)
    auto_start: bool = True

    # Additional tracking fields
    form_submissions: List[FormSubmission] = Field(default_factory=list)
    approvals: List[Approval] = Field(default_factory=list)


class WorkflowCreate(BaseModel):
    name: str
    created_by: PyObjectId  # ✅ Fixed: removed alias
    description: Optional[str] = None
    status: WorkflowStatus = WorkflowStatus.DRAFT
    trigger_conditions: List[TriggerCondition] = Field(default_factory=list)
    auto_start: bool = True


class WorkflowUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    status: Optional[WorkflowStatus] = None
    updated_by: Optional[PyObjectId] = None  # ✅ Fixed: removed alias
    trigger_conditions: Optional[List[TriggerCondition]] = None
    auto_start: Optional[bool] = None


class Workflow(BaseModel):
    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        populate_by_name=True,
        json_encoders={ObjectId: str, datetime: lambda dt: dt.isoformat()},
    )

    # Required fields
    name: str
    created_by: PyObjectId  # ✅ Fixed: removed alias

    # Optional fields with defaults
    id: Optional[Annotated[PyObjectId, Field(alias="_id")]] = (
        None  # ✅ This one keeps the alias as it's the actual document ID
    )
    tasks: List[Task] = Field(default_factory=list)
    updated_by: Optional[PyObjectId] = None  # ✅ Fixed: removed alias
    created_at: datetime = Field(default_factory=datetime.now)
    updated_at: datetime = Field(default_factory=datetime.now)
    status: WorkflowStatus = WorkflowStatus.DRAFT
    description: Optional[str] = None

    # Trigger-related fields
    trigger_conditions: List[TriggerCondition] = Field(default_factory=list)
    trigger_results: List[TriggerResult] = Field(default_factory=list)
    auto_start: bool = True


# Rest of your classes remain the same...
class GetWorkflowResponse(BaseModel):
    workflows: List[Workflow]


class DeleteWorkflowResponse(BaseModel):
    response: str


class TriggerValidator:
    """Class to handle trigger condition validation"""

    @staticmethod
    def validate_file_upload(task: Task, condition: TriggerCondition) -> TriggerResult:
        """Validate file upload conditions"""
        result = TriggerResult(
            condition_id=f"file_upload_{condition.type}", is_satisfied=False
        )

        # Check minimum files
        if condition.min_files and len(task.files) < condition.min_files:
            result.error_message = (
                f"Minimum {condition.min_files} files required, got {len(task.files)}"
            )
            return result

        # Check file types
        if condition.allowed_file_types:
            invalid_files = [
                f for f in task.files if f.type not in condition.allowed_file_types
            ]
            if invalid_files:
                result.error_message = (
                    f"Invalid file types: {[f.type for f in invalid_files]}"
                )
                return result

        # Check file sizes
        if condition.max_file_size:
            oversized_files = [
                f for f in task.files if f.size and f.size > condition.max_file_size
            ]
            if oversized_files:
                result.error_message = (
                    f"Files exceed maximum size: {[f.name for f in oversized_files]}"
                )
                return result

        result.is_satisfied = True
        result.details = {"files_count": len(task.files)}
        return result

    @staticmethod
    def validate_link_submission(
        task: Task, condition: TriggerCondition
    ) -> TriggerResult:
        """Validate link submission conditions"""
        result = TriggerResult(
            condition_id=f"link_submission_{condition.type}", is_satisfied=False
        )

        # Check minimum links
        if condition.min_links and len(task.links) < condition.min_links:
            result.error_message = (
                f"Minimum {condition.min_links} links required, got {len(task.links)}"
            )
            return result

        # Validate link patterns (if specified)
        if condition.link_pattern:
            pattern = re.compile(condition.link_pattern)
            invalid_links = [l for l in task.links if not pattern.match(l.url)]
            if invalid_links:
                result.error_message = (
                    f"Invalid link patterns: {[l.url for l in invalid_links]}"
                )
                return result

        result.is_satisfied = True
        result.details = {"links_count": len(task.links)}
        return result

    @staticmethod
    def validate_form_submission(
        task: Task, condition: TriggerCondition
    ) -> TriggerResult:
        """Validate form submission conditions"""
        result = TriggerResult(
            condition_id=f"form_submission_{condition.type}", is_satisfied=False
        )

        if not task.form_submissions:
            result.error_message = "No form submissions found"
            return result

        # Check required fields
        if condition.form_fields:
            latest_submission = task.form_submissions[-1]
            missing_fields = [
                field
                for field in condition.form_fields
                if field not in latest_submission.data
            ]
            if missing_fields:
                result.error_message = f"Missing required fields: {missing_fields}"
                return result

        result.is_satisfied = True
        result.details = {"submissions_count": len(task.form_submissions)}
        return result

    @staticmethod
    def validate_approval(task: Task, condition: TriggerCondition) -> TriggerResult:
        """Validate approval conditions"""
        result = TriggerResult(
            condition_id=f"approval_{condition.type}", is_satisfied=False
        )

        approved_count = len([a for a in task.approvals if a.approved])

        if (
            condition.required_approvals
            and approved_count < condition.required_approvals
        ):
            result.error_message = f"Required {condition.required_approvals} approvals, got {approved_count}"
            return result

        result.is_satisfied = True
        result.details = {"approvals_count": approved_count}
        return result

    @staticmethod
    def validate_all_conditions(task: Task) -> List[TriggerResult]:
        """Validate all trigger conditions for a task"""
        results = []

        for condition in task.trigger_conditions:
            if condition.type == TriggerType.FILE_UPLOAD:
                results.append(TriggerValidator.validate_file_upload(task, condition))
            elif condition.type == TriggerType.LINK_SUBMISSION:
                results.append(
                    TriggerValidator.validate_link_submission(task, condition)
                )
            elif condition.type == TriggerType.FORM_SUBMISSION:
                results.append(
                    TriggerValidator.validate_form_submission(task, condition)
                )
            elif condition.type == TriggerType.APPROVAL:
                results.append(TriggerValidator.validate_approval(task, condition))

        return results

    @staticmethod
    def can_task_proceed(task: Task) -> bool:
        """Check if a task can proceed based on trigger conditions"""
        if not task.trigger_conditions:
            return True

        results = TriggerValidator.validate_all_conditions(task)

        # All required conditions must be satisfied
        required_results = [
            r
            for r in results
            if any(
                c.required
                for c in task.trigger_conditions
                if f"{c.type.value}" in r.condition_id
            )
        ]

        return all(r.is_satisfied for r in required_results)


class TaskCRUD:
    @staticmethod
    def create_task(task_data: TaskCreate) -> Task:
        """Create a new task with trigger validation"""
        task_dict = task_data.model_dump()
        task_dict["created_at"] = datetime.now()
        task_dict["updated_at"] = datetime.now()

        # Set initial status based on trigger conditions
        if task_dict["trigger_conditions"]:
            task_dict["status"] = TaskStatus.WAITING_FOR_TRIGGER

        return Task(**task_dict)

    @staticmethod
    def update_task(existing_task: Task, update_data: TaskUpdate) -> Task:
        """Update an existing task and validate triggers"""
        update_dict = update_data.model_dump(exclude_unset=True)
        update_dict["updated_at"] = datetime.now()

        # Update the existing task
        for field, value in update_dict.items():
            setattr(existing_task, field, value)

        # Validate triggers and potentially change status
        if existing_task.trigger_conditions:
            if TriggerValidator.can_task_proceed(existing_task):
                if (
                    existing_task.status == TaskStatus.WAITING_FOR_TRIGGER
                    and existing_task.auto_start
                ):
                    existing_task.status = TaskStatus.PENDING
            else:
                existing_task.status = TaskStatus.WAITING_FOR_TRIGGER

        # Update trigger results
        existing_task.trigger_results = TriggerValidator.validate_all_conditions(
            existing_task
        )

        return existing_task

    @staticmethod
    def add_file_to_task(task: Task, file: Upload) -> Task:
        """Add a file to a task and check triggers"""
        task.files.append(file)
        task.updated_at = datetime.now()

        # Re-validate triggers
        if task.trigger_conditions:
            if TriggerValidator.can_task_proceed(task):
                if task.status == TaskStatus.WAITING_FOR_TRIGGER and task.auto_start:
                    task.status = TaskStatus.PENDING

        task.trigger_results = TriggerValidator.validate_all_conditions(task)
        return task

    @staticmethod
    def submit_form_to_task(task: Task, form_submission: FormSubmission) -> Task:
        """Submit a form to a task and check triggers"""
        task.form_submissions.append(form_submission)
        task.updated_at = datetime.now()

        # Re-validate triggers
        if task.trigger_conditions:
            if TriggerValidator.can_task_proceed(task):
                if task.status == TaskStatus.WAITING_FOR_TRIGGER and task.auto_start:
                    task.status = TaskStatus.PENDING

        task.trigger_results = TriggerValidator.validate_all_conditions(task)
        return task
