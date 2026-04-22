import asyncio
import time
from typing import Dict, List, Optional, Union
import traceback

import httpx

from ..core.program_info import program_manager
from ..log.logger import logger
from ..models.server_models import ResourceType, Server, ServerStatus, ServerTier
from ..utils.routing import DEFAULT_ROUTE_GROUP, build_dispatch_key, normalize_route_group
from ..utils.cache import CacheManager, CacheType
# TODO 这里还需要每个插件当添加server的时候因为server是由可运行的任务类型的，对应的任务类型的插件就执行runningserver的连接测试，来确保服务可用


class ServerManager:
    """服务器注册、状态维护与资源分配管理器。"""

    HEALTH_CHECK_CACHE_TTL = 10.0
    RECENT_ACTIVITY_GRACE_TTL = 2.0

    def __init__(self):
        """初始化服务器管理器与索引结构。"""
        self.cache_manager = CacheManager("server_cache", CacheType.SERVER)
        self.all_servers = self._load_servers()
        self.server_idle_event: Dict[str, asyncio.Event] = {}
        self.running_servers: List[Server] = []
        self.initialized = False
        self._lock = asyncio.Lock()
        self._init_lock = asyncio.Lock()
        self._health_check_lock = asyncio.Lock()
        self._server_active_tasks: Dict[str, int] = {}
        self._server_health_cache: Dict[str, float] = {}
        self._server_recent_activity: Dict[str, float] = {}
        self._health_check_tasks: Dict[str, asyncio.Task] = {}

        # 索引结构，加速筛选
        self.servers_by_status: Dict[ServerStatus, List[Server]] = {
            status: [] for status in ServerStatus
        }
        self.servers_by_type: Dict[str, List[Server]] = {}

    def _server_key(self, server: Server) -> str:
        """生成服务器在内部索引中的唯一键。"""
        return server.server_name or server.ip

    def _mark_server_healthy(self, server: Server):
        """记录最近一次健康检查成功时间。"""
        self._server_health_cache[self._server_key(server)] = time.monotonic()

    def _mark_server_recent_activity(self, server: Server):
        """记录服务器最近一次处于活跃或刚释放任务的时间。"""
        self._server_recent_activity[self._server_key(server)] = time.monotonic()

    def _clear_server_health_cache(self, server: Server):
        """清除服务器健康检查缓存。"""
        self._server_health_cache.pop(self._server_key(server), None)

    def _has_recent_activity(self, server: Server) -> bool:
        """判断服务器是否刚结束任务，仍处于短暂恢复窗口。"""
        last_activity_at = self._server_recent_activity.get(self._server_key(server))
        if last_activity_at is None:
            return False
        return (time.monotonic() - last_activity_at) < self.RECENT_ACTIVITY_GRACE_TTL

    def _get_recent_activity_age(self, server: Server) -> Optional[float]:
        """获取距最近一次活动的秒数。"""
        last_activity_at = self._server_recent_activity.get(self._server_key(server))
        if last_activity_at is None:
            return None
        return time.monotonic() - last_activity_at

    def _describe_server_runtime(self, server: Server) -> str:
        """构造服务器运行态诊断字符串。"""
        recent_activity_age = self._get_recent_activity_age(server)
        recent_activity_str = (
            f"{recent_activity_age:.2f}s"
            if recent_activity_age is not None
            else "n/a"
        )
        return (
            f"{server.server_name}(status={server.status.value}, "
            f"active={self._get_server_active_tasks(server)}/{server.max_concurrency}, "
            f"recent_activity_age={recent_activity_str})"
        )

    def describe_dispatch_key_servers(self, dispatch_key: str) -> str:
        """按调度键返回服务器运行态摘要。"""
        matched_servers = [
            server
            for server in self.running_servers
            if dispatch_key in self._get_server_dispatch_keys(server)
        ]
        if not matched_servers:
            return "none"
        return ", ".join(
            self._describe_server_runtime(server) for server in matched_servers
        )

    def _has_recent_health_check(self, server: Server) -> bool:
        """判断服务器是否有近期成功的健康检查结果。"""
        last_success_at = self._server_health_cache.get(self._server_key(server))
        if last_success_at is None:
            return False
        return (time.monotonic() - last_success_at) < self.HEALTH_CHECK_CACHE_TTL

    def _set_running_servers(self, servers: List[Server]):
        """按唯一键重建运行中的服务器列表。"""
        deduped_servers: Dict[str, Server] = {}
        for server in servers:
            deduped_servers[self._server_key(server)] = server
        self.running_servers = list(deduped_servers.values())

    def _add_running_server_if_missing(self, server: Server) -> bool:
        """向运行池中添加服务器，已存在则跳过。"""
        server_key = self._server_key(server)
        if any(self._server_key(item) == server_key for item in self.running_servers):
            return False
        self.running_servers.append(server)
        return True

    def _get_server_active_tasks(self, server: Server) -> int:
        """获取服务器当前占用的并发槽位数。"""
        return self._server_active_tasks.get(self._server_key(server), 0)

    def _get_server_available_slots(self, server: Server) -> int:
        """计算服务器当前可用并发槽位数。"""
        if server not in self.running_servers:
            return 0
        if server.status in {ServerStatus.stop, ServerStatus.error}:
            return 0
        active_tasks = self._get_server_active_tasks(server)
        return max(server.max_concurrency - active_tasks, 0)

    def _sync_server_runtime_status(self, server: Server):
        """根据当前运行任务数同步服务器状态。"""
        if server not in self.running_servers:
            return
        if server.status in {ServerStatus.stop, ServerStatus.error}:
            return
        if self._get_server_active_tasks(server) > 0:
            server.status = ServerStatus.occupy
        else:
            server.status = ServerStatus.idle

    def _refresh_idle_events(self, dispatch_keys: Optional[List[str]] = None):
        """按调度键刷新“有空闲资源”事件状态。"""
        if dispatch_keys is None:
            dispatch_keys = list(self.server_idle_event.keys())
        for dispatch_key in dispatch_keys:
            self._ensure_server_idle_event(dispatch_key)
            if self._check_has_idle_by_dispatch_key(dispatch_key):
                self.server_idle_event[dispatch_key].set()
            else:
                self.server_idle_event[dispatch_key].clear()

    async def ensure_initialized(self):
        """确保管理器在使用前已完成初始化。"""
        if self.initialized:
            return
        async with self._init_lock:
            if self.initialized:
                return
            await self._init_running_server()
            self.initialized = True

    def _ensure_server_idle_event(self, dispatch_key: str):
        """确保指定调度键存在可用服务器事件对象。"""
        if dispatch_key not in self.server_idle_event:
            self.server_idle_event[dispatch_key] = asyncio.Event()
            self.server_idle_event[dispatch_key].set()

    def _get_server_dispatch_keys(self, server: Server) -> List[str]:
        """获取某台服务器支持的全部调度键。"""
        return server.get_dispatch_keys()

    def _check_has_idle_by_dispatch_key(self, dispatch_key: str) -> bool:
        """检查指定调度键是否存在空闲槽位。"""
        candidate_servers = [
            server
            for server in self.running_servers
            if self._get_server_available_slots(server) > 0
        ]
        candidate_servers = [
            server
            for server in candidate_servers
            if dispatch_key in self._get_server_dispatch_keys(server)
        ]
        return bool(candidate_servers)

    async def _init_running_server(self):
        """启动时校验并恢复可运行服务器列表。"""
        start_time = time.time()
        recovered_running_servers: List[Server] = []
        self._server_active_tasks = {
            self._server_key(server): 0 for server in self.all_servers
        }
        running_num = 0
        for server in self.all_servers:
            if (
                server.status == ServerStatus.error
                or server.status == ServerStatus.occupy
            ):
                # 尝试重连，成功设为 idle，失败设为 stop
                is_connected = await self.check_server(server)
                if is_connected:
                    logger.info(f"服务器{server} error,但是重连成功")
                    server.status = ServerStatus.idle
                    self._mark_server_healthy(server)
                else:
                    logger.info(f"服务器{server} error,重连失败")
                    server.status = ServerStatus.stop
                    self._clear_server_health_cache(server)
            if server.status == ServerStatus.idle:
                if await self.check_server(server):
                    recovered_running_servers.append(server)
                    running_num += 1
                else:
                    logger.info(f"服务器{server} idle,但是连接失败")
                    server.status = ServerStatus.stop
                    self._clear_server_health_cache(server)

        self._set_running_servers(recovered_running_servers)

        program_manager.set_running_gpu_num(running_num)
        program_manager.set_gpu_num(len(self.all_servers))

        self._save_servers()
        await program_manager.record_operation_time("init_running_server", start_time)

    def _update_server_indices(self):
        """刷新服务器索引。"""
        # 清空旧索引
        for status in ServerStatus:
            self.servers_by_status[status] = []
        self.servers_by_type.clear()

        # 重建索引
        for server in self.all_servers:
            self.servers_by_status[server.status].append(server)
            for task_type in server.available_task_types:
                if task_type not in self.servers_by_type:
                    self.servers_by_type[task_type] = []
                self.servers_by_type[task_type].append(server)

    def _load_servers(self) -> List[Server]:
        """从缓存加载服务器列表。"""
        data = self.cache_manager.read_cache()
        return [Server(**server_data) for server_data in data]

    def _save_servers(self):
        """保存服务器列表到缓存。"""
        data = [server.model_dump() for server in self.all_servers]
        self.cache_manager.write_cache(data)
        self._update_server_indices()

    def get_server_by_identifier(
        self, ip: Union[str, None] = None, server_name: Union[str, None] = None
    ) -> Optional[Server]:
        """根据 IP 或名称定位服务器。"""
        for server in self.all_servers:
            if ip and server.ip == ip:
                return server
            if server_name and server.server_name == server_name:
                return server
        return None

    def check_has_idle(
        self,
        server_type: str = None,
        route_group: str = DEFAULT_ROUTE_GROUP,
    ):
        """检查是否存在可用于指定任务类型的空闲槽位。"""
        candidate_servers = [
            server
            for server in self.running_servers
            if self._get_server_available_slots(server) > 0
        ]
        if server_type:
            normalized_route_group = normalize_route_group(route_group)
            candidate_servers = [
                server
                for server in candidate_servers
                if server.check_available_task_route(server_type, normalized_route_group)
            ]
        return bool(candidate_servers)

    def check_server_running(self, server: Server):
        """判断服务器是否在运行池中。"""
        return server in self.running_servers

    async def register_server(
        self,
        ip: str,
        server_name: str,
        description,
        tier: ServerTier,
        available_task_types: List[str] = None,
        server_type: ResourceType = ResourceType.GPU,
        max_concurrency: int = 1,
        task_routes: Optional[Dict[str, List[str]]] = None,
    ):
        """注册或覆盖服务器配置。"""
        if max_concurrency < 1:
            return False, "max_concurrency must be >= 1"
        start_time = time.time()
        async with self._lock:
            old_server = self.get_server_by_identifier(ip, server_name)
            updated_server = Server(
                ip=ip,
                server_name=server_name,
                description=description,
                tier=tier,
                available_task_types=available_task_types if available_task_types else [],
                task_routes=task_routes if task_routes else {},
                server_type=server_type,
                max_concurrency=max_concurrency,
                status=old_server.status if old_server else ServerStatus.stop,
            )
            if old_server:
                logger.info(f"已经存在服务器：{old_server}")
                old_server.ip = updated_server.ip
                old_server.server_name = updated_server.server_name
                old_server.description = updated_server.description
                old_server.tier = updated_server.tier
                old_server.available_task_types = updated_server.available_task_types
                old_server.task_routes = updated_server.task_routes
                old_server.server_type = updated_server.server_type
                old_server.max_concurrency = updated_server.max_concurrency
                message = f"存在服务器：{old_server}， 已经覆盖配置"
                logger.info(message)
            else:
                self.all_servers.append(updated_server)
                self._server_active_tasks[self._server_key(updated_server)] = 0
                message = f"添加新服务器：{updated_server}"
                logger.info(message)

            self._save_servers()
            program_manager.set_gpu_num(len(self.all_servers))
            await program_manager.record_operation_time("register_server", start_time)
            return True, message

    async def get_idle_server(
        self,
        available_task_type: str = None,
        task_resource_type: ResourceType = None,
        route_group: str = DEFAULT_ROUTE_GROUP,
    ) -> Optional[Server]:
        """获取一个可用服务器，并占用其一个并发槽位。"""
        start_time = time.time()
        normalized_route_group = normalize_route_group(route_group)
        dispatch_key = (
            build_dispatch_key(available_task_type, normalized_route_group)
            if available_task_type
            else None
        )
        await self.ensure_initialized()
        async with self._lock:
            candidate_servers = list(self.running_servers)

            if available_task_type:
                candidate_servers = [
                    s
                    for s in candidate_servers
                    if s.check_available_task_route(
                        available_task_type, normalized_route_group
                    )
                ]

            if task_resource_type:
                candidate_servers = [
                    s for s in candidate_servers if s.server_type == task_resource_type
                ]

            candidate_servers = [
                s for s in candidate_servers if self._get_server_available_slots(s) > 0
            ]
            candidate_servers.sort(key=lambda x: x.tier.value, reverse=True)
            candidate_snapshot = [
                self._describe_server_runtime(server) for server in candidate_servers
            ]

        if dispatch_key:
            logger.debug(
                f"开始分配服务器 dispatch_key={dispatch_key}, "
                f"resource={task_resource_type}, candidates={candidate_snapshot or ['none']}"
            )

        for server in candidate_servers:
            if not await self.check_server(server):
                async with self._lock:
                    if not self.check_server_running(server):
                        continue
                    if (
                        self._get_server_active_tasks(server) <= 0
                        and not self._has_recent_activity(server)
                    ):
                        self.set_server_status(server, ServerStatus.error)
                        logger.error(
                            f"服务器健康检查失败并标记为 error: "
                            f"{self._describe_server_runtime(server)}, dispatch_key={dispatch_key}"
                        )
                    else:
                        # 高并发下健康检查可能因瞬时排队/限流失败，
                        # 对仍有活跃任务或刚释放任务的服务器先跳过本次分配，避免误标为 error。
                        self._sync_server_runtime_status(server)
                        logger.warning(
                            f"服务器健康检查失败，但仍处于活跃/恢复窗口，跳过本次分配: "
                            f"{self._describe_server_runtime(server)}, dispatch_key={dispatch_key}"
                        )
                continue

            async with self._lock:
                if not self.check_server_running(server):
                    continue
                if available_task_type and not server.check_available_task_route(
                    available_task_type, normalized_route_group
                ):
                    continue
                if task_resource_type and server.server_type != task_resource_type:
                    continue
                if self._get_server_available_slots(server) <= 0:
                    continue

                server_key = self._server_key(server)
                self._server_active_tasks[server_key] = (
                    self._server_active_tasks.get(server_key, 0) + 1
                )
                self._mark_server_recent_activity(server)
                self._sync_server_runtime_status(server)
                self._save_servers()
                self._refresh_idle_events(self._get_server_dispatch_keys(server))
                logger.debug(
                    f"服务器分配成功 dispatch_key={dispatch_key}, "
                    f"{self._describe_server_runtime(server)}"
                )
                if available_task_type:
                    await program_manager.record_task_time(
                        available_task_type, start_time
                    )
                return server

        if dispatch_key:
            async with self._lock:
                self._ensure_server_idle_event(dispatch_key)
                self.server_idle_event[dispatch_key].clear()
        return None

    async def get_server_list(self, server_name_list: List[str] = None) -> List[Server]:
        """获取服务器列表，可按名称过滤。"""
        try:
            await self.ensure_initialized()
            if server_name_list:
                res: List[Server] = []
                for server_name in server_name_list:
                    server = self.get_server_by_identifier(server_name=server_name)
                    if server:
                        res.append(server)
                return res
            else:
                # 获得所有的服务器
                return self.all_servers
        except Exception as e:
            logger.error(f"Get server list failed: {str(e)}\n{traceback.format_exc()}")
            return []

    async def _perform_health_check(self, server: Server) -> bool:
        """真正执行服务器连通性检测。"""
        start_time = time.time()
        backoff = 1
        try:
            for i in range(3):
                try:
                    async with httpx.AsyncClient() as client:
                        await client.get(
                            f"{server.ip}", timeout=2, follow_redirects=True
                        )
                    self._mark_server_healthy(server)
                    return True
                except (httpx.ConnectError, httpx.TimeoutException) as e:
                    if i < 2:
                        await asyncio.sleep(backoff)
                        logger.info(
                            f"check_server: 服务器({server})异常：{str(e)}，尝试重试:{backoff}秒"
                        )
                    else:
                        logger.error(f"check_server: 服务器({server})异常：{str(e)}")
                        self._clear_server_health_cache(server)
                        return False
                except Exception as e:
                    logger.error(f"check_server: 服务器({server})异常：{e}")
                    self._clear_server_health_cache(server)
                    return False
            self._clear_server_health_cache(server)
            return False
        finally:
            await program_manager.record_operation_time("check_server", start_time)

    async def check_server(self, server: Server):
        """检测服务器连通性，失败时按退避策略重试。"""
        if self._has_recent_health_check(server):
            return True

        server_key = self._server_key(server)
        async with self._health_check_lock:
            if self._has_recent_health_check(server):
                return True
            task = self._health_check_tasks.get(server_key)
            if task is None:
                task = asyncio.create_task(self._perform_health_check(server))
                self._health_check_tasks[server_key] = task

        try:
            return await task
        finally:
            async with self._health_check_lock:
                if self._health_check_tasks.get(server_key) is task and task.done():
                    self._health_check_tasks.pop(server_key, None)

    async def release_server(self, server: Server):
        """释放服务器一个并发槽位。"""
        async with self._lock:
            if server not in self.running_servers:
                return False
            if server.status == ServerStatus.error:
                return False
            server_key = self._server_key(server)
            current_tasks = self._server_active_tasks.get(server_key, 0)
            if current_tasks <= 0:
                return False
            self._server_active_tasks[server_key] = current_tasks - 1
            self._mark_server_recent_activity(server)
            self._sync_server_runtime_status(server)
            self._save_servers()
            self._refresh_idle_events(self._get_server_dispatch_keys(server))
            logger.debug(f"服务器已释放: {self._describe_server_runtime(server)}")
            return True

    def set_server_status(self, server: Server, status: ServerStatus):
        """设置服务器状态并同步缓存与事件。"""
        server.status = status
        if status in {ServerStatus.stop, ServerStatus.error, ServerStatus.idle}:
            self._server_active_tasks[self._server_key(server)] = 0

        if status == ServerStatus.idle:
            self._mark_server_healthy(server)
        else:
            self._clear_server_health_cache(server)

        if status == ServerStatus.idle:
            for dispatch_key in self._get_server_dispatch_keys(server):
                self._ensure_server_idle_event(dispatch_key)
                self.server_idle_event[dispatch_key].set()
        elif status in {ServerStatus.stop, ServerStatus.error}:
            self._refresh_idle_events(self._get_server_dispatch_keys(server))

        self._save_servers()
        return True

    async def add_running_server(
        self, ip: Union[str, None] = None, server_name: Union[str, None] = None
    ):
        """将服务器加入运行池。"""
        start_time = time.time()
        await self.ensure_initialized()
        server = None
        async with self._lock:
            server = self.get_server_by_identifier(ip, server_name)
            if not server:
                logger.error(f"Server not found - ip:{ip} server_name:{server_name}")
                return False, f"ip:{ip} server_name:{server_name} 服务器不存在"

            if self.check_server_running(server):
                logger.info(f"Server {server} is already running")
                return False, f"服务器{server}已经在运行"

        # 加入运行池前做连通性检查
        if not await self.check_server(server):
            logger.error(f"Server {server} connection check failed")
            return False, f"服务器{server}连接检查失败"

        async with self._lock:
            if self.check_server_running(server):
                logger.info(f"Server {server} is already running")
                return False, f"服务器{server}已经在运行"

            self._add_running_server_if_missing(server)
            self._server_active_tasks[self._server_key(server)] = 0
            self.set_server_status(server, ServerStatus.idle)
            self.initialized = True

            program_manager.set_running_gpu_num(len(self.running_servers))
            await program_manager.record_operation_time(
                "add_running_server", start_time
            )
            return True, f"添加服务器{server} 成功"

    async def remove_running_server(
        self, ip: Union[str, None] = None, server_name: Union[str, None] = None
    ):
        """将服务器从运行池移除（需无任务占用）。"""
        start_time = time.time()
        async with self._lock:
            server = self.get_server_by_identifier(ip, server_name)
            if not server:
                logger.error(f"Server not found - ip:{ip} server_name:{server_name}")
                return False, f"ip:{ip} server_name:{server_name} 服务器不存在"

            if server not in self.running_servers:
                return False, f"服务器{server}不在运行"

            if self._get_server_active_tasks(server) > 0:
                return False, f"服务器{server}有任务在执行，不在空闲状态，请稍后再关闭"

            self.running_servers.remove(server)
            self.set_server_status(server, ServerStatus.stop)

            program_manager.set_running_gpu_num(len(self.running_servers))
            await program_manager.record_operation_time(
                "remove_running_server", start_time
            )
            return True, f"删除服务器{server} 成功"


# 创建管理器实例
server_manager = ServerManager()
