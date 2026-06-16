import asyncio
import base64
import pickle
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Optional

from ..config import LibraryConfig
from ..exceptions import ConfigurationError
from ..models.task_models import Task, TaskInfo


def _encode_pickle(value: Any) -> str:
    return base64.b64encode(pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)).decode(
        "ascii"
    )


def _decode_pickle(value: str) -> Any:
    return pickle.loads(base64.b64decode(value.encode("ascii")))


def serialize_task(task: Task) -> Dict[str, str]:
    task_info_payload = task.task_info.model_dump()
    return {
        "task_id": task.task_info.task_id,
        "task_type": task.task_info.task_type,
        "route_group": task.task_info.route_group,
        "priority": task.task_info.priority.value,
        "task_info_payload": _encode_pickle(task_info_payload),
        "payload": _encode_pickle({"args": task.args, "kwargs": task.kwargs}),
    }


def deserialize_task(data: Dict[str, str]) -> Task:
    task_info_data = _decode_pickle(data["task_info_payload"])
    payload = _decode_pickle(data["payload"])
    task_info = TaskInfo.from_json(task_info_data)
    return Task(task_info, *payload["args"], **payload["kwargs"])


def serialize_task_info(task_info: TaskInfo) -> bytes:
    return pickle.dumps(task_info.model_dump(), protocol=pickle.HIGHEST_PROTOCOL)


def deserialize_task_info(data: bytes) -> TaskInfo:
    task_info_data = pickle.loads(data)
    return TaskInfo.from_json(task_info_data)


@dataclass
class QueueDelivery:
    dispatch_key: str
    receipt: str
    task: Task


class QueueBackend(ABC):
    keep_processors_alive: bool = False

    @abstractmethod
    async def enqueue(self, dispatch_key: str, task: Task) -> None:
        raise NotImplementedError

    @abstractmethod
    async def consume(
        self, dispatch_key: str, consumer_name: str, block_ms: int
    ) -> Optional[QueueDelivery]:
        raise NotImplementedError

    @abstractmethod
    async def reclaim(
        self,
        dispatch_key: str,
        consumer_name: str,
        min_idle_ms: int,
        excluded_receipts: Optional[set[str]] = None,
    ) -> Optional[QueueDelivery]:
        raise NotImplementedError

    @abstractmethod
    async def ack(self, delivery: QueueDelivery) -> None:
        raise NotImplementedError

    @abstractmethod
    async def requeue(self, delivery: QueueDelivery, task: Task) -> None:
        raise NotImplementedError

    @abstractmethod
    async def save_task_info(self, task_info: TaskInfo) -> None:
        raise NotImplementedError

    @abstractmethod
    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        raise NotImplementedError

    @abstractmethod
    async def list_dispatch_keys(self) -> list[str]:
        raise NotImplementedError

    @abstractmethod
    async def get_queue_size(self, dispatch_key: str) -> Optional[int]:
        raise NotImplementedError


@dataclass
class _MemoryPendingItem:
    delivery: QueueDelivery
    claimed_at: datetime
    consumer_name: str


class InMemoryQueueBackend(QueueBackend):
    keep_processors_alive = False

    def __init__(self):
        self._queues: Dict[str, asyncio.Queue[tuple[str, Dict[str, str]]]] = {}
        self._pending: Dict[str, Dict[str, _MemoryPendingItem]] = {}
        self._task_infos: Dict[str, bytes] = {}
        self._dispatch_keys: set[str] = set()

    def _ensure_dispatch_key(self, dispatch_key: str) -> None:
        if dispatch_key not in self._queues:
            self._queues[dispatch_key] = asyncio.Queue()
        if dispatch_key not in self._pending:
            self._pending[dispatch_key] = {}
        self._dispatch_keys.add(dispatch_key)

    async def enqueue(self, dispatch_key: str, task: Task) -> None:
        self._ensure_dispatch_key(dispatch_key)
        receipt = str(uuid.uuid4())
        await self._queues[dispatch_key].put((receipt, serialize_task(task)))

    async def consume(
        self, dispatch_key: str, consumer_name: str, block_ms: int
    ) -> Optional[QueueDelivery]:
        self._ensure_dispatch_key(dispatch_key)
        get_task = asyncio.create_task(self._queues[dispatch_key].get())
        try:
            receipt, payload = await asyncio.wait_for(
                get_task, timeout=block_ms / 1000
            )
        except asyncio.TimeoutError:
            get_task.cancel()
            try:
                await get_task
            except asyncio.CancelledError:
                pass
            return None

        delivery = QueueDelivery(
            dispatch_key=dispatch_key,
            receipt=receipt,
            task=deserialize_task(payload),
        )
        self._pending[dispatch_key][receipt] = _MemoryPendingItem(
            delivery=delivery,
            claimed_at=datetime.now(),
            consumer_name=consumer_name,
        )
        return delivery

    async def reclaim(
        self,
        dispatch_key: str,
        consumer_name: str,
        min_idle_ms: int,
        excluded_receipts: Optional[set[str]] = None,
    ) -> Optional[QueueDelivery]:
        self._ensure_dispatch_key(dispatch_key)
        excluded_receipts = excluded_receipts or set()
        now = datetime.now()
        for receipt, pending_item in list(self._pending[dispatch_key].items()):
            if receipt in excluded_receipts:
                continue
            idle_ms = (now - pending_item.claimed_at).total_seconds() * 1000
            if idle_ms < min_idle_ms:
                continue
            pending_item.claimed_at = now
            pending_item.consumer_name = consumer_name
            return pending_item.delivery
        return None

    async def ack(self, delivery: QueueDelivery) -> None:
        self._pending.get(delivery.dispatch_key, {}).pop(delivery.receipt, None)

    async def requeue(self, delivery: QueueDelivery, task: Task) -> None:
        await self.ack(delivery)
        await self.enqueue(delivery.dispatch_key, task)

    async def remove_queued_tasks(self) -> list[Task]:
        """从内存队列中移除全部尚未被消费者领取的任务。"""
        removed_tasks: list[Task] = []
        for queue in self._queues.values():
            while True:
                try:
                    _, payload = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break

                # 内存队列中尚未 get 出来的任务都还没有分配给处理器，可以直接取消。
                removed_tasks.append(deserialize_task(payload))
        return removed_tasks

    async def save_task_info(self, task_info: TaskInfo) -> None:
        self._task_infos[task_info.task_id] = serialize_task_info(task_info)

    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        data = self._task_infos.get(task_id)
        if not data:
            return None
        return deserialize_task_info(data)

    async def list_dispatch_keys(self) -> list[str]:
        return sorted(self._dispatch_keys)

    async def get_queue_size(self, dispatch_key: str) -> Optional[int]:
        self._ensure_dispatch_key(dispatch_key)
        return self._queues[dispatch_key].qsize() + len(self._pending[dispatch_key])


class RedisStreamQueueBackend(QueueBackend):
    keep_processors_alive = True

    def __init__(self, library_config: LibraryConfig):
        try:
            import redis
            import redis.asyncio as redis_async
        except ImportError as exc:
            raise ConfigurationError(
                "redis package is required when TASK_WEAVER_QUEUE_BACKEND=redis"
            ) from exc

        self._redis = redis_async.Redis.from_url(
            library_config.redis_url, decode_responses=True
        )
        self._redis_sync = redis.Redis.from_url(
            library_config.redis_url, decode_responses=False
        )
        self._prefix = library_config.queue_prefix
        self._group_name = library_config.queue_consumer_group
        self._task_info_key = f"{self._prefix}:task_info"
        self._dispatch_keys_key = f"{self._prefix}:dispatch_keys"

    def _stream_key(self, dispatch_key: str) -> str:
        return f"{self._prefix}:stream:{dispatch_key}"

    async def _ensure_group(self, dispatch_key: str) -> None:
        stream_key = self._stream_key(dispatch_key)
        try:
            await self._redis.xgroup_create(
                name=stream_key,
                groupname=self._group_name,
                id="0-0",
                mkstream=True,
            )
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def enqueue(self, dispatch_key: str, task: Task) -> None:
        await self._redis.sadd(self._dispatch_keys_key, dispatch_key)
        await self._redis.xadd(self._stream_key(dispatch_key), serialize_task(task))

    async def consume(
        self, dispatch_key: str, consumer_name: str, block_ms: int
    ) -> Optional[QueueDelivery]:
        await self._ensure_group(dispatch_key)
        stream_key = self._stream_key(dispatch_key)
        response = await self._redis.xreadgroup(
            groupname=self._group_name,
            consumername=consumer_name,
            streams={stream_key: ">"},
            count=1,
            block=block_ms,
        )
        return self._parse_delivery(dispatch_key, response)

    async def reclaim(
        self,
        dispatch_key: str,
        consumer_name: str,
        min_idle_ms: int,
        excluded_receipts: Optional[set[str]] = None,
    ) -> Optional[QueueDelivery]:
        await self._ensure_group(dispatch_key)
        excluded_receipts = excluded_receipts or set()
        result = await self._redis.xautoclaim(
            name=self._stream_key(dispatch_key),
            groupname=self._group_name,
            consumername=consumer_name,
            min_idle_time=min_idle_ms,
            start_id="0-0",
            count=max(10, len(excluded_receipts) + 1),
        )
        if not result:
            return None
        messages = result[1] if len(result) > 1 else []
        if not messages:
            return None
        for message_id, payload in messages:
            if message_id in excluded_receipts:
                continue
            return QueueDelivery(
                dispatch_key=dispatch_key,
                receipt=message_id,
                task=deserialize_task(payload),
            )
        return None

    async def ack(self, delivery: QueueDelivery) -> None:
        stream_key = self._stream_key(delivery.dispatch_key)
        await self._redis.xack(stream_key, self._group_name, delivery.receipt)
        await self._redis.xdel(stream_key, delivery.receipt)

    async def requeue(self, delivery: QueueDelivery, task: Task) -> None:
        await self.enqueue(delivery.dispatch_key, task)
        await self.ack(delivery)

    async def save_task_info(self, task_info: TaskInfo) -> None:
        await self._redis.hset(
            self._task_info_key,
            task_info.task_id,
            serialize_task_info(task_info),
        )

    def get_task_info(self, task_id: str) -> Optional[TaskInfo]:
        data = self._redis_sync.hget(self._task_info_key, task_id)
        if not data:
            return None
        return deserialize_task_info(data)

    async def list_dispatch_keys(self) -> list[str]:
        dispatch_keys = await self._redis.smembers(self._dispatch_keys_key)
        return sorted(dispatch_keys)

    async def get_queue_size(self, dispatch_key: str) -> Optional[int]:
        return await self._redis.xlen(self._stream_key(dispatch_key))

    def _parse_delivery(
        self, dispatch_key: str, response: Any
    ) -> Optional[QueueDelivery]:
        if not response:
            return None
        _, messages = response[0]
        if not messages:
            return None
        message_id, payload = messages[0]
        return QueueDelivery(
            dispatch_key=dispatch_key,
            receipt=message_id,
            task=deserialize_task(payload),
        )


def build_queue_backend(library_config: LibraryConfig) -> QueueBackend:
    backend_name = library_config.queue_backend.lower()
    if backend_name == "memory":
        return InMemoryQueueBackend()
    if backend_name == "redis":
        return RedisStreamQueueBackend(library_config)
    raise ConfigurationError(f"Unsupported queue backend: {library_config.queue_backend}")
