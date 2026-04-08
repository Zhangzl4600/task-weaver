from enum import Enum
from typing import Dict, List

from pydantic import BaseModel, Field, model_validator

from ..utils.routing import (
    DEFAULT_ROUTE_GROUP,
    build_dispatch_key,
    normalize_route_group,
)


# 任务需要的资源类型 | 也是服务器类型 但不严格对等
class ResourceType(str, Enum):
    CPU = "cpu"
    GPU = "gpu"
    API = "api"


class ServerStatus(str, Enum):
    stop = "stop"
    error = "error"
    idle = "idle"
    occupy = "occupy"  # 占用状态，这个时候不一定是在跑服务，但是已经被某个任务给捕获了


class TaskTypeStats(BaseModel):
    total: int
    success: int
    failed: int
    avg_duration: float


class ServerTier(int, Enum):
    LOCAL = 10000  # 本地机器
    SUPER_PRIORITY = 1000  # 超级优先级
    PREMIUM = 100  # 高性能服务器，如 A100/H100
    STANDARD = 70  # 标准服务器，如 V100/3090
    BASIC = 40  # 基础服务器，如 2080Ti/3080
    LEGACY = 10  # 旧式服务器，如 1080Ti及以下
    MINIMAL = 1  # 添加新的最低级别


class ServerOperationStats(BaseModel):
    total_count: int = 0
    avg_duration_ms: float = 0
    max_duration_ms: float = 0
    min_duration_ms: float = float("inf")

    def update_stats(self, duration_ms: float):
        self.total_count += 1
        # Update average
        self.avg_duration_ms = (
            self.avg_duration_ms * (self.total_count - 1) + duration_ms
        ) / self.total_count
        # Update max/min
        self.max_duration_ms = max(self.max_duration_ms, duration_ms)
        self.min_duration_ms = min(self.min_duration_ms, duration_ms)


class ProgramInfo(BaseModel):
    gpu_num: int
    running_gpu_num: int
    total_running_time: int
    last_shutdown_time: int
    # 运行时长
    running_time: int  # 单位：秒
    # 已完成任务数
    finished_task_num: int
    # 失败任务数
    failed_task_num: int
    first_start_time: int  # 单位：秒
    task_type_stats: Dict[str, TaskTypeStats]
    server_task_stats: Dict[str, ServerOperationStats]
    server_operation_stats: Dict[str, ServerOperationStats]


class Server(BaseModel):
    ip: str
    server_name: str
    description: str
    available_task_types: List[str] = Field(default_factory=list)  # 服务器可以跑的任务类型
    task_routes: Dict[str, List[str]] = Field(default_factory=dict)
    server_type: ResourceType  # 服务器资源类型
    status: ServerStatus = ServerStatus.stop
    tier: ServerTier = ServerTier.STANDARD  # 替换原来的 tier
    max_concurrency: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def normalize_task_routes(self) -> "Server":
        if self.task_routes:
            self.task_routes = {
                task_type: [
                    normalize_route_group(route_group) for route_group in route_groups
                ]
                for task_type, route_groups in self.task_routes.items()
            }
        else:
            self.task_routes = {
                task_type: [DEFAULT_ROUTE_GROUP]
                for task_type in self.available_task_types
            }
        self.available_task_types = list(self.task_routes.keys())
        return self

    def check_available_task_type(self, available_task_type: str):
        return available_task_type in self.task_routes

    def check_available_task_route(
        self, task_type: str, route_group: str = DEFAULT_ROUTE_GROUP
    ) -> bool:
        return normalize_route_group(route_group) in self.task_routes.get(task_type, [])

    def get_dispatch_keys(self) -> List[str]:
        dispatch_keys: List[str] = []
        for task_type, route_groups in self.task_routes.items():
            for route_group in route_groups:
                dispatch_keys.append(build_dispatch_key(task_type, route_group))
        return dispatch_keys

    # to str
    def __str__(self):
        return f"server_name:{self.server_name}"

    def description_str(self):
        return f"Server(ip={self.ip}, server_name={self.server_name}, description={self.description}, status={self.status}, tier={self.tier})"
