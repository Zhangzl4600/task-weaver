import asyncio
import traceback
import uuid
from datetime import datetime
from typing import Any, Callable, Coroutine, Dict, Optional

from ..exceptions import ProcessingError
from ..log.logger import logger
from ..models.server_models import ResourceType, Server
from ..models.task_models import Task, TaskInfo, TaskPriority, TaskStatus
from .program_info import program_manager
from .server import server_manager
from .task_catalog import task_catalog

# 任务信息变更回调类型定义
TaskInfoChangeCallback = Callable[[TaskInfo], Coroutine[Any, Any, None]]


class TaskManager:
    """分布式任务处理的核心任务管理器。

    主要职责：
    - 任务创建与入队
    - 任务执行与监控
    - 任务状态存储与查询
    - 服务器资源分配
    - 错误处理与恢复
    - 任务状态变化通知

    TaskManager 为每个任务类型维护独立队列，并结合资源状态进行调度。
    """

    def __init__(self):
        """初始化任务管理器的运行态数据结构。"""
        logger.info("Initializing TaskManager...")
        # 每个任务类型独立队列，用于并发调度
        self._queues: Dict[str, asyncio.Queue] = {}

        # 运行中的任务内存索引
        self._tasks: Dict[str, Task] = {}

        # 每个任务类型对应一个队列处理协程
        self._processors: Dict[str, asyncio.Task] = {}

        # 队列处理协程运行状态
        self._is_processor_running: Dict[str, bool] = {}

        # 启停处理协程时使用的锁
        self._task_lock = asyncio.Lock()

        # 任务信息变更监听器（key 唯一）
        self._task_info_listeners: Dict[str, TaskInfoChangeCallback] = {}
        self._task_type_limits: Dict[str, Optional[int]] = {}
        self._task_type_inflight: Dict[str, int] = {}
        self._task_slot_events: Dict[str, asyncio.Event] = {}
        self._task_slot_lock = asyncio.Lock()

        logger.info("TaskManager initialized successfully")

    def set_task_type_concurrency(
        self, task_type: str, max_concurrency: Optional[int]
    ) -> None:
        """设置任务类型的最大并发执行数。

        max_concurrency:
        - None：不限制
        - 1：串行
        - >1：并发上限
        """
        if max_concurrency is not None and max_concurrency < 1:
            raise ValueError("max_concurrency must be >= 1 or None")
        self._task_type_limits[task_type] = max_concurrency
        self._ensure_task_slot_state(task_type)
        self._task_slot_events[task_type].set()

    def _ensure_task_slot_state(self, slot_key: str) -> None:
        """确保并发槽位状态已初始化。"""
        if slot_key not in self._task_type_inflight:
            self._task_type_inflight[slot_key] = 0
        if slot_key not in self._task_slot_events:
            self._task_slot_events[slot_key] = asyncio.Event()
            self._task_slot_events[slot_key].set()

    def _get_task_type_limit(self, task_type: str) -> Optional[int]:
        """获取任务类型并发上限（优先使用运行时配置）。"""
        if task_type in self._task_type_limits:
            return self._task_type_limits[task_type]
        task_definition = task_catalog.get_task_definition(task_type)
        if not task_definition:
            return None
        return task_definition.max_concurrency

    def _get_subtask_slot_limit(self, task: Task) -> tuple[Optional[str], Optional[int]]:
        """按子任务规则计算并发槽位 key 与上限。"""
        task_type = task.task_info.task_type
        task_definition = task_catalog.get_task_definition(task_type)
        if (
            not task_definition
            or not task_definition.subtask_concurrency
            or not task_definition.subtask_key
        ):
            return None, None

        subtask_key = task_definition.subtask_key
        subtask_rules = task_definition.subtask_concurrency
        raw_value = task.kwargs.get(subtask_key)
        explicit_bucket = str(raw_value) if raw_value is not None else None

        if explicit_bucket and explicit_bucket in subtask_rules:
            bucket = explicit_bucket
            limit = subtask_rules[bucket]
            return f"{task_type}::{subtask_key}::{bucket}", limit

        if "*" in subtask_rules:
            limit = subtask_rules["*"]
            # 未命中显式规则时，共享兜底并发池
            return f"{task_type}::{subtask_key}::*", limit

        return None, None

    async def _acquire_slot(self, slot_key: str, limit: Optional[int]) -> bool:
        """申请一个并发槽位。拿不到返回False，不阻塞等待"""
        if limit is None:
            return False

        self._ensure_task_slot_state(slot_key)
        while True:
            async with self._task_slot_lock:
                if self._task_type_inflight[slot_key] < limit:
                    self._task_type_inflight[slot_key] += 1
                    if self._task_type_inflight[slot_key] >= limit:
                        self._task_slot_events[slot_key].clear()
                    return True
                self._task_slot_events[slot_key].clear()
            await self._task_slot_events[slot_key].wait()

    async def _try_acquire_slot(self, slot_key: str, limit: Optional[int]) -> bool:
        """尝试申请一个并发槽位（非阻塞）。"""
        if limit is None:
            return False

        self._ensure_task_slot_state(slot_key)
        async with self._task_slot_lock:
            if self._task_type_inflight[slot_key] < limit:
                self._task_type_inflight[slot_key] += 1
                if self._task_type_inflight[slot_key] >= limit:
                    self._task_slot_events[slot_key].clear()
                return True
            self._task_slot_events[slot_key].clear()
            return False

    async def _release_slot(self, slot_key: str) -> None:
        """释放一个并发槽位。"""
        self._ensure_task_slot_state(slot_key)
        async with self._task_slot_lock:
            self._task_type_inflight[slot_key] = max(
                self._task_type_inflight[slot_key] - 1, 0
            )
            self._task_slot_events[slot_key].set()

    async def _acquire_task_slots(self, task: Task) -> list[str]:
        """按任务规则申请所需并发槽位。"""
        task_type = task.task_info.task_type
        acquired_slots: list[str] = []
        task_type_limit = self._get_task_type_limit(task_type)
        sub_slot_key, sub_slot_limit = self._get_subtask_slot_limit(task)

        try:
            if task_type_limit is not None:
                if await self._acquire_slot(task_type, task_type_limit):
                    acquired_slots.append(task_type)

            if sub_slot_limit is not None and sub_slot_key:
                if await self._acquire_slot(sub_slot_key, sub_slot_limit):
                    acquired_slots.append(sub_slot_key)
        except Exception:
            if acquired_slots:
                await self._release_task_slots(acquired_slots)
            raise

        return acquired_slots

    async def _try_acquire_task_slots(self, task: Task) -> tuple[list[str], list[str]]:
        """尝试申请任务所需并发槽位（非阻塞）。

        返回：
        - acquired_slots: 已占用槽位
        - blocked_slots: 申请失败的槽位（用于等待/重排）
        """
        task_type = task.task_info.task_type
        acquired_slots: list[str] = []
        blocked_slots: list[str] = []
        task_type_limit = self._get_task_type_limit(task_type)
        sub_slot_key, sub_slot_limit = self._get_subtask_slot_limit(task)

        try:
            if task_type_limit is not None:
                if await self._try_acquire_slot(task_type, task_type_limit):
                    acquired_slots.append(task_type)
                else:
                    blocked_slots.append(task_type)
                    return acquired_slots, blocked_slots

            if sub_slot_limit is not None and sub_slot_key:
                if await self._try_acquire_slot(sub_slot_key, sub_slot_limit):
                    acquired_slots.append(sub_slot_key)
                else:
                    blocked_slots.append(sub_slot_key)
                    if acquired_slots:
                        await self._release_task_slots(acquired_slots)
                        acquired_slots = []
                    return acquired_slots, blocked_slots
        except Exception:
            if acquired_slots:
                await self._release_task_slots(acquired_slots)
            raise

        return acquired_slots, blocked_slots

    async def _release_task_slots(self, slot_keys: list[str]) -> None:
        """释放任务已占用的全部并发槽位。"""
        for slot_key in slot_keys:
            await self._release_slot(slot_key)

    def add_task_info_listener(
        self, key: str, callback: TaskInfoChangeCallback
    ) -> None:
        """添加任务信息变更监听器（key 唯一）"""
        if key in self._task_info_listeners:
            logger.warning(f"Listener with key {key} already exists, replacing...")
        self._task_info_listeners[key] = callback
        logger.debug(f"Added task info change listener with key {key}")

    def remove_task_info_listener(self, key: str) -> None:
        """按 key 移除任务信息监听器"""
        if key in self._task_info_listeners:
            del self._task_info_listeners[key]
            logger.debug(f"Removed task info change listener with key {key}")
        else:
            logger.warning(f"No listener found with key {key}")

    async def _notify_task_info_change(self, task_info: TaskInfo) -> None:
        """任务信息变化时通知全部监听器"""
        for key, listener in self._task_info_listeners.items():
            try:
                await listener(task_info)
            except Exception as e:
                logger.error(f"Error in task info change listener {key}: {str(e)}")

    async def update_task_status(
        self, task_info: TaskInfo, status: TaskStatus, message: str
    ) -> None:
        """更新任务状态与消息，并触发通知"""
        task_info.status = status
        task_info.message = message
        await self._notify_task_info_change(task_info)

    async def create_task(
        self, task_type: str, priority: TaskPriority, *args, **kwargs
    ) -> Task:
        """按指定参数创建任务实例。"""
        logger.info(f"Creating new task of type {task_type} with priority {priority}")
        try:
            task_definition = task_catalog.get_task_definition(task_type)
            if not task_definition:
                error_msg = f"Task type {task_type} not found in catalog"
                logger.error(error_msg)
                raise ProcessingError(error_msg)

            task_id = str(uuid.uuid4())
            logger.debug(f"Generated task ID: {task_id}")

            task = Task(
                TaskInfo(
                    task_id=task_id,
                    task_type=task_type,
                    status=TaskStatus.INIT,
                    priority=priority,
                    create_time=datetime.now(),
                    progress=0,
                    remaining_duration=None,
                    wait_duration=None,
                    execution_duration=None,
                    message="Task is created.",
                ),
                *args,
                **kwargs,
            )
            logger.info(f"Successfully created task {task_id} of type {task_type}")
            return task

        except Exception as e:
            error_msg = f"Failed to create task: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    # 为什么不直接在create_task中将task置入队列并执行
    # 因为这代表两种不同状态，一个是状态创建INIT、一个是状态预执行，中间或许业务层会有一些自定义操作而不直接入队
    async def add_task(self, task: Task) -> None:
        """将任务加入对应处理队列。"""
        self._tasks[task.task_info.task_id] = task
        await self._ensure_task_processor(task.task_info.task_type)
        await self._queues[task.task_info.task_type].put(task)
        await self.update_task_status(
            task.task_info, TaskStatus.INIT, "Task is queued."
        )
        return task

    async def _ensure_task_processor(self, task_type: str) -> None:
        """确保给定任务类型的队列处理协程已启动。"""
        if task_type not in self._queues:
            logger.info(f"Creating new queue for task type {task_type}")
            self._queues[task_type] = asyncio.Queue()
            self._is_processor_running[task_type] = False

        if not self._is_processor_running[task_type]:
            logger.info(f"Starting processor for task type {task_type}")
            async with self._task_lock:
                if not self._is_processor_running[task_type]:
                    self._is_processor_running[task_type] = True
                    self._processors[task_type] = asyncio.create_task(
                        self._process_queue(task_type)
                    )

    async def _process_queue(self, task_type: str) -> None:
        """处理指定任务类型队列中的任务。"""
        logger.info(f"Starting queue processor for task type {task_type}")
        last_warning_time = 0  # 上次告警时间
        warning_interval = 60  # 告警间隔（秒）
        try:
            while True:
                task: Task | None = None
                server: Server | None = None
                acquired_slots: list[str] = []
                task_handed_off = False
                try:
                    task_definition = task_catalog.get_task_definition(task_type)
                    if not task_definition:
                        raise ProcessingError(f"Task type {task_type} not found")

                    if self._queues[task_type].qsize() == 0:
                        break

                    task: Task = await self._queues[task_type].get()
                    acquired_slots, blocked_slots = await self._try_acquire_task_slots(
                        task
                    )
                    if blocked_slots:
                        # 当前任务槽位不足时放回队尾，避免阻塞后续可执行任务
                        await self._queues[task_type].put(task)
                        self._queues[task_type].task_done()
                        try:
                            self._ensure_task_slot_state(blocked_slots[0])
                            await asyncio.wait_for(
                                self._task_slot_events[blocked_slots[0]].wait(),
                                timeout=0.1,
                            )
                        except asyncio.TimeoutError:
                            pass
                        continue

                    if task_definition.required_resource != ResourceType.API:
                        while not server:
                            server = await server_manager.get_idle_server(
                                task_definition.task_type,
                                task_definition.required_resource,
                            )
                            if server:
                                logger.info(
                                    f"Allocated server {server.server_name} for task type {task_type}"
                                )
                                break

                            # 资源不可用时释放并发槽位，避免阻塞同类型其他可执行子任务
                            if acquired_slots:
                                await self._release_task_slots(acquired_slots)
                                acquired_slots = []

                            current_time = datetime.now().timestamp()
                            if current_time - last_warning_time >= warning_interval:
                                logger.warning(
                                    f"No available servers for task type {task_type}, waiting..."
                                )
                                last_warning_time = current_time
                            await asyncio.sleep(0.5)
                            acquired_slots = await self._acquire_task_slots(task)
                    else:
                        logger.info(f"{task_type} doesn't require server, executing...")

                    logger.info(
                        f"Processing task {task.task_info.task_id} of type {task_type}"
                    )
                    asyncio.create_task(
                        self._execute_task(task, server, release_slots=acquired_slots)
                    )
                    acquired_slots = []
                    task_handed_off = True
                except asyncio.CancelledError:
                    logger.error(f"Queue processor for {task_type} was cancelled")
                    if acquired_slots:
                        await self._release_task_slots(acquired_slots)
                    if server:
                        await server_manager.release_server(server)
                    if task and not task_handed_off:
                        self._queues[task_type].task_done()
                    break
                except Exception as e:
                    if acquired_slots:
                        await self._release_task_slots(acquired_slots)
                    if server:
                        await server_manager.release_server(server)
                    error_msg = (
                        f"Error processing task: {str(e)}\n{traceback.format_exc()}"
                    )
                    logger.error(error_msg)
                    if task and not task_handed_off:
                        await self.update_task_status(
                            task.task_info, TaskStatus.FAIL, f"Task failed: {error_msg}"
                        )
                        task.task_info.error = error_msg
                        await task_catalog.notify_task_completion(task.task_info)
                        self._queues[task_type].task_done()
        except Exception as e:
            logger.error(
                f"Fatal error in _process_queue for {task_type}: {str(e)}\n{traceback.format_exc()}"
            )
        finally:
            logger.warning(f"Queue processor for {task_type} is shutting down")
            self._is_processor_running[task_type] = False

    async def _execute_task(
        self, task: Task, server: Server | None, release_slots: Optional[list[str]] = None
    ) -> None:
        """执行单个任务并记录完整生命周期信息。"""
        logger.info(f"Starting execution of task {task.task_info.task_id}")
        try:
            task_definition = task_catalog.get_task_definition(task.task_info.task_type)
            if not task_definition or not task_definition.executor:
                error_msg = (
                    f"No executor found for task type: {task.task_info.task_type}"
                )
                logger.error(error_msg)
                raise ProcessingError(error_msg)

            await self.update_task_status(
                task.task_info, TaskStatus.PROCESS, "Task is processing"
            )
            task.task_info.start_time = datetime.now()
            task.task_info.wait_duration = (
                task.task_info.start_time - task.task_info.create_time
            ).total_seconds()
            logger.info(
                f"Task {task.task_info.task_id} waited {task.task_info.wait_duration:.2f} seconds in queue"
            )

            logger.debug(f"Executing task {task.task_info.task_id} with executor")
            await task_definition.executor(
                server, task.task_info, *task.args, **task.kwargs
            )

            await self.update_task_status(
                task.task_info, TaskStatus.FINISH, "Task completed successfully"
            )
            program_manager.update_finished_task_num(task.task_info.task_type)
            logger.info(f"Task {task.task_info.task_id} completed successfully")

        except Exception as e:
            error_msg = f"Task execution failed: {str(e)}\n{traceback.format_exc()}"
            logger.error(error_msg)
            await self.update_task_status(
                task.task_info, TaskStatus.FAIL, "Task failed"
            )
            program_manager.update_failed_task_num(task.task_info.task_type)
            task.task_info.error = error_msg
            raise
        finally:
            task.task_info.finish_time = datetime.now()
            task.task_info.execution_duration = (
                task.task_info.finish_time - task.task_info.start_time
            ).total_seconds()
            logger.info(
                f"Task {task.task_info.task_id} execution took {task.task_info.execution_duration:.2f} seconds"
            )
            logger.info(f"执行完成, 资源释放")
            if task:
                logger.debug(f"Marking task {task.task_info.task_id} as done in queue")
                await task_catalog.notify_task_completion(task.task_info)
                self._queues[task.task_info.task_type].task_done()
            if server:
                logger.debug(f"Releasing server {server.server_name}")
                await server_manager.release_server(server)
            if release_slots:
                await self._release_task_slots(release_slots)

    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        """从内存中读取任务信息。"""
        # 记录任务信息访问频次，用于降噪日志
        if not hasattr(self, "_task_access_counts"):
            self._task_access_counts = {}
            self._last_log_time = {}
        warning_interval = 10
        current_time = datetime.now().timestamp()

        # 初始化或累加访问次数
        if task_id not in self._task_access_counts:
            self._task_access_counts[task_id] = 1
            self._last_log_time[task_id] = current_time
        else:
            self._task_access_counts[task_id] += 1

        # 按间隔输出访问日志
        if current_time - self._last_log_time[task_id] >= warning_interval:
            logger.debug(
                f"Retrieving info for task {task_id} "
                f"(accessed {self._task_access_counts[task_id]} times in last {warning_interval}s)"
            )
            # 重置计数并更新时间戳
            self._task_access_counts[task_id] = 0
            self._last_log_time[task_id] = current_time

        return self._tasks.get(task_id).task_info


# 全局任务管理器实例
logger.info("Creating global TaskManager instance")
task_manager = TaskManager()
