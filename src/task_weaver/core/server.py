import asyncio
import time
from typing import Dict, List, Optional, Union
import traceback

import httpx

from ..core.program_info import program_manager
from ..log.logger import logger
from ..models.server_models import ResourceType, Server, ServerStatus, ServerTier
from ..utils.cache import CacheManager, CacheType
# TODO 这里还需要每个插件当添加server的时候因为server是由可运行的任务类型的，对应的任务类型的插件就执行runningserver的连接测试，来确保服务可用


class ServerManager:
    """服务器注册、状态维护与资源分配管理器。"""

    def __init__(self):
        """初始化服务器管理器与索引结构。"""
        self.cache_manager = CacheManager("server_cache", CacheType.SERVER)
        self.all_servers = self._load_servers()
        self.server_idle_event: Dict[str, asyncio.Event] = {}
        self.running_servers: List[Server] = []
        self.initialized = False
        self._lock = asyncio.Lock()
        self._server_active_tasks: Dict[str, int] = {}

        # 索引结构，加速筛选
        self.servers_by_status: Dict[ServerStatus, List[Server]] = {
            status: [] for status in ServerStatus
        }
        self.servers_by_type: Dict[str, List[Server]] = {}

    def _server_key(self, server: Server) -> str:
        """生成服务器在内部索引中的唯一键。"""
        return server.server_name or server.ip

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

    def _refresh_idle_events(self, task_types: Optional[List[str]] = None):
        """按任务类型刷新“有空闲资源”事件状态。"""
        if task_types is None:
            task_types = list(self.server_idle_event.keys())
        for task_type in task_types:
            self._ensure_server_idle_event(task_type)
            if self.check_has_idle(task_type):
                self.server_idle_event[task_type].set()
            else:
                self.server_idle_event[task_type].clear()

    async def ensure_initialized(self):
        """确保管理器在使用前已完成初始化。"""
        if not self.initialized:
            await self._init_running_server()
            self.initialized = True

    def _ensure_server_idle_event(self, server_type: str):
        """确保指定任务类型存在可用服务器事件对象。"""
        if server_type not in self.server_idle_event:
            self.server_idle_event[server_type] = asyncio.Event()
            self.server_idle_event[server_type].set()

    async def _init_running_server(self):
        """启动时校验并恢复可运行服务器列表。"""
        start_time = time.time()
        self.running_servers: List[Server] = []
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
                    self.set_server_status(server, ServerStatus.idle)
                else:
                    logger.info(f"服务器{server} error,重连失败")
                    self.set_server_status(server, ServerStatus.stop)
            if server.status == ServerStatus.idle:
                if await self.check_server(server):
                    self.running_servers.append(server)
                    running_num += 1
                else:
                    logger.info(f"服务器{server} idle,但是连接失败")
                    self.set_server_status(server, ServerStatus.stop)

        program_manager.set_running_gpu_num(running_num)
        program_manager.set_gpu_num(len(self.all_servers))

        # 初始化索引结构
        self._update_server_indices()
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

    def check_has_idle(self, server_type: str = None):
        """检查是否存在可用于指定任务类型的空闲槽位。"""
        candidate_servers = [
            server
            for server in self.running_servers
            if self._get_server_available_slots(server) > 0
        ]
        if server_type:
            candidate_servers = [
                server
                for server in candidate_servers
                if server.check_available_task_type(server_type)
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
    ):
        """注册或覆盖服务器配置。"""
        if max_concurrency < 1:
            return False, "max_concurrency must be >= 1"
        start_time = time.time()
        async with self._lock:
            old_server = self.get_server_by_identifier(ip, server_name)
            if old_server:
                logger.info(f"已经存在服务器：{old_server}")
                old_server.ip = ip
                old_server.server_name = server_name
                old_server.description = description
                old_server.tier = tier
                old_server.available_task_types = (
                    available_task_types if available_task_types else []
                )
                old_server.server_type = (
                    server_type if server_type else old_server.server_type
                )
                old_server.max_concurrency = max_concurrency
                message = f"存在服务器：{old_server}， 已经覆盖配置"
                logger.info(message)
            else:
                server = Server(
                    ip=ip,
                    server_name=server_name,
                    description=description,
                    tier=tier,
                    available_task_types=available_task_types
                    if available_task_types
                    else [],
                    server_type=server_type,
                    max_concurrency=max_concurrency,
                )
                self.all_servers.append(server)
                self._server_active_tasks[self._server_key(server)] = 0
                message = f"添加新服务器：{server}"
                logger.info(message)

            self._save_servers()
            program_manager.set_gpu_num(len(self.all_servers))
            await program_manager.record_operation_time("register_server", start_time)
            return True, message

    async def get_idle_server(
        self, available_task_type: str = None, task_resource_type: ResourceType = None
    ) -> Optional[Server]:
        """获取一个可用服务器，并占用其一个并发槽位。"""
        start_time = time.time()
        await self.ensure_initialized()
        async with self._lock:
            candidate_servers = list(self.running_servers)

            if available_task_type:
                candidate_servers = [
                    s
                    for s in candidate_servers
                    if s.check_available_task_type(available_task_type)
                ]

            if task_resource_type:
                candidate_servers = [
                    s for s in candidate_servers if s.server_type == task_resource_type
                ]

            candidate_servers = [
                s for s in candidate_servers if self._get_server_available_slots(s) > 0
            ]
            candidate_servers.sort(key=lambda x: x.tier.value, reverse=True)

            for server in candidate_servers:
                if await self.check_server(server):
                    server_key = self._server_key(server)
                    self._server_active_tasks[server_key] = (
                        self._server_active_tasks.get(server_key, 0) + 1
                    )
                    self._sync_server_runtime_status(server)
                    self._save_servers()
                    self._refresh_idle_events(server.available_task_types)
                    if available_task_type:
                        await program_manager.record_task_time(
                            available_task_type, start_time
                        )
                    return server
                self.set_server_status(server, ServerStatus.error)
                logger.error(f"运行服务器({server})异常，无法连接")

            if available_task_type:
                self._ensure_server_idle_event(available_task_type)
                self.server_idle_event[available_task_type].clear()
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

    async def check_server(self, server: Server):
        """检测服务器连通性，失败时按退避策略重试。"""
        start_time = time.time()
        backoff = 1
        for i in range(3):
            try:
                async with httpx.AsyncClient() as client:
                    response = await client.get(
                        f"{server.ip}", timeout=2, follow_redirects=True
                    )
                    # Any response indicates server is up
                    return True
            except (httpx.ConnectError, httpx.TimeoutException) as e:
                if i < 2:
                    await asyncio.sleep(backoff)
                    logger.info(
                        f"check_server: 服务器({server})异常：{str(e)}，尝试重试:{backoff}秒"
                    )
                else:
                    logger.error(f"check_server: 服务器({server})异常：{str(e)}")
                    return False
            except Exception as e:
                logger.error(f"check_server: 服务器({server})异常：{e}")
                return False
        await program_manager.record_operation_time("check_server", start_time)
        return True

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
            self._sync_server_runtime_status(server)
            self._save_servers()
            self._refresh_idle_events(server.available_task_types)
            return True

    def set_server_status(self, server: Server, status: ServerStatus):
        """设置服务器状态并同步缓存与事件。"""
        server.status = status
        if status in {ServerStatus.stop, ServerStatus.error, ServerStatus.idle}:
            self._server_active_tasks[self._server_key(server)] = 0

        if status == ServerStatus.idle:
            for server_type in server.available_task_types:
                self._ensure_server_idle_event(server_type)
                self.server_idle_event[server_type].set()
        elif status in {ServerStatus.stop, ServerStatus.error}:
            self._refresh_idle_events(server.available_task_types)

        self._save_servers()
        return True

    async def add_running_server(
        self, ip: Union[str, None] = None, server_name: Union[str, None] = None
    ):
        """将服务器加入运行池。"""
        start_time = time.time()
        async with self._lock:
            server = self.get_server_by_identifier(ip, server_name)
            if not server:
                logger.error(f"Server not found - ip:{ip} server_name:{server_name}")
                return False, f"ip:{ip} server_name:{server_name} 服务器不存在"

            if server in self.running_servers:
                logger.info(f"Server {server} is already running")
                return False, f"服务器{server}已经在运行"

            # 加入运行池前做连通性检查
            if not await self.check_server(server):
                logger.error(f"Server {server} connection check failed")
                return False, f"服务器{server}连接检查失败"

            self.running_servers.append(server)
            self._server_active_tasks[self._server_key(server)] = 0
            self.set_server_status(server, ServerStatus.idle)

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


# Create manager instance
server_manager = ServerManager()
