from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generic, Optional, Protocol, TypeVar

from pydantic import BaseModel

from .server_models import ResourceType, Server
from ..utils.routing import DEFAULT_ROUTE_GROUP

T = TypeVar("T")  # 执行器返回类型


class TaskStatus(str, Enum):
    INIT = "INIT"
    PROCESS = "PROCESS"
    FINISH = "FINISH"
    FAIL = "FAIL"


class TaskPriority(str, Enum):
    BEST = "BEST"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

    @property
    def priority(self) -> int:
        priorities = {self.BEST: 4, self.HIGH: 3, self.MEDIUM: 2, self.LOW: 1}
        return priorities[self]

    def __lt__(self, other):
        if not isinstance(other, TaskPriority):
            return NotImplemented
        return self.priority < other.priority

    def __gt__(self, other):
        if not isinstance(other, TaskPriority):
            return NotImplemented
        return self.priority > other.priority

    def __le__(self, other):
        if not isinstance(other, TaskPriority):
            return NotImplemented
        return self.priority <= other.priority

    def __ge__(self, other):
        if not isinstance(other, TaskPriority):
            return NotImplemented
        return self.priority >= other.priority


class TaskInfo(BaseModel):
    """单个任务实例的运行信息。"""

    task_id: str
    task_type: str  # References TaskDefinition.task_type
    route_group: str = DEFAULT_ROUTE_GROUP
    status: TaskStatus
    priority: TaskPriority = TaskPriority.MEDIUM
    create_time: datetime = datetime.now()
    start_time: Optional[datetime] = None
    finish_time: Optional[datetime] = None
    progress: float = 0  # 0-100的百分比
    remaining_duration: Optional[float] = None  # 剩余时间
    wait_duration: Optional[float] = None  # 等待时间
    execution_duration: Optional[float] = None  # 执行时间
    result: Optional[Any] = None
    error: Optional[str] = None
    message: str = "Task is queued."

    def model_dump(self) -> Dict[str, Any]:
        """序列化模型，并处理 datetime 字段。"""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "route_group": self.route_group,
            "status": self.status,
            "priority": self.priority,
            "progress": self.progress,
            "create_time": self.create_time.isoformat() if self.create_time else None,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "finish_time": self.finish_time.isoformat() if self.finish_time else None,
            "message": self.message,
            "remaining_duration": self.remaining_duration,
            "wait_duration": self.wait_duration,
            "execution_duration": self.execution_duration,
            "result": self.result.model_dump() if hasattr(self.result, "model_dump") else self.result,
            "error": self.error,
        }

    @classmethod
    def from_json(cls, data: Dict[str, Any]) -> "TaskInfo":
        """从 JSON 数据创建 TaskInfo，并处理 datetime 字段。"""
        # 将 ISO 格式字符串转换为 datetime 对象
        if isinstance(data.get("create_time"), str):
            data["create_time"] = datetime.fromisoformat(data["create_time"])
        if isinstance(data.get("start_time"), str):
            data["start_time"] = datetime.fromisoformat(data["start_time"])
        if isinstance(data.get("finish_time"), str):
            data["finish_time"] = datetime.fromisoformat(data["finish_time"])
        return cls(**data)


class TaskExecutor(Protocol[T]):
    """任务执行器协议定义。"""

    async def __call__(
        self,
        server: Server | None,
        task_info: TaskInfo,
        *args: Any,
        **kwargs: Any,
    ) -> T: ...


class Task:
    def __init__(self, task_info: TaskInfo, *args, **kwargs):
        self.task_info = task_info
        self.args = args
        self.kwargs = kwargs

    def __str__(self) -> str:
        return f"Task: id: {self.task_info.task_id}, type: {self.task_info.task_type}, route_group: {self.task_info.route_group}, status: {self.task_info.status}, progress: {self.task_info.progress}, remaining_time: {self.task_info.remaining_duration}, data: {self.task_info.result}, msg: {self.task_info.message}, create_time: {self.task_info.create_time}"

    def mini_str(self) -> str:
        return f"Task: id: {self.task_info.task_id}, type: {self.task_info.task_type}, route_group: {self.task_info.route_group}"


class BaseTaskExecutor(Generic[T]):
    """任务执行器基类，实现 TaskExecutor 协议。"""

    async def __call__(
        self,
        server: Server | None,
        task_info: TaskInfo,
        *args: Any,
        **kwargs: Any,
    ) -> T:
        raise NotImplementedError


class TaskDefinition:
    """可注册任务类型的定义。"""

    def __init__(
        self,
        name: str,
        task_type: str,
        executor: TaskExecutor[Any],
        required_resource: ResourceType,
        description: str = "",
        version: str = "1.0.0",
        max_concurrency: Optional[int] = None,
        subtask_concurrency: Optional[Dict[str, int]] = None,
        subtask_key: str = "provider",
    ):
        self.name = name
        self.task_type = task_type
        self.executor = executor
        self.required_resource = required_resource
        self.max_concurrency = max_concurrency
        self.subtask_concurrency = subtask_concurrency
        self.subtask_key = subtask_key
        self.description = description
        self.version = version


class TaskQueueItem:
    """队列中任务项的数据结构。"""

    def __init__(self, priority: int, task: TaskInfo):
        self.priority = priority
        self.task = task

    def __lt__(self, other):
        if not isinstance(other, TaskQueueItem):
            return NotImplemented
        return self.priority < other.priority
