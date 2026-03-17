import traceback
from typing import Any, Callable, Coroutine, Dict, List, Optional

from ..exceptions import ConfigurationError
from ..log.logger import logger
from ..models.server_models import ResourceType
from ..models.task_models import TaskDefinition, TaskExecutor, TaskInfo

TaskCompletionCallback = Callable[[TaskInfo], Coroutine[Any, Any, None]]


class TaskCatalog:
    """任务定义与回调管理中心。"""

    def __init__(self):
        """初始化任务定义与完成回调存储。"""
        self._task_catalog: Dict[str, TaskDefinition] = {}
        self._completion_listeners: Dict[str, List[TaskCompletionCallback]] = {}

    def get_all_task_definitions(self) -> List[TaskDefinition]:
        """获取全部任务定义。"""
        return list(self._task_catalog.values())

    def add_task_definition(
        self,
        task_name: str,
        task_type: str,
        executor: TaskExecutor[Any],
        required_resource: ResourceType,
        description: str = "",
        version: str = "1.0.0",
        max_concurrency: Optional[int] = None,
        subtask_concurrency: Optional[Dict[str, int]] = None,
        subtask_key: str = "provider",
    ) -> None:
        """向目录中添加新的任务定义。"""
        if not task_name or not task_type:
            raise ConfigurationError("Task name and type cannot be empty")

        if not executor:
            raise ConfigurationError("Task executor cannot be None")

        if task_type in self._task_catalog:
            raise ConfigurationError(
                f"Task {task_type} already exists in catalog, you need to change the task_type"
            )
        if max_concurrency is not None and max_concurrency < 1:
            raise ConfigurationError("max_concurrency must be >= 1 or None")
        if subtask_concurrency is not None:
            if not isinstance(subtask_concurrency, dict):
                raise ConfigurationError("subtask_concurrency must be a dictionary")
            for key, value in subtask_concurrency.items():
                if not isinstance(key, str) or not key:
                    raise ConfigurationError(
                        "subtask_concurrency key must be a non-empty string"
                    )
                if not isinstance(value, int) or value < 1:
                    raise ConfigurationError(
                        "subtask_concurrency value must be an integer >= 1"
                    )
        if not isinstance(subtask_key, str) or not subtask_key:
            raise ConfigurationError("subtask_key must be a non-empty string")

        try:
            task_def = TaskDefinition(
                name=task_name,
                task_type=task_type,
                executor=executor,
                required_resource=required_resource,
                max_concurrency=max_concurrency,
                subtask_concurrency=subtask_concurrency,
                subtask_key=subtask_key,
                description=description,
                version=version,
            )
        except Exception as e:
            logger.error(f"Failed to create task definition: {str(e)}")
            raise ConfigurationError(f"Failed to create task definition: {str(e)}")

        self._task_catalog[task_type] = task_def
        self._completion_listeners[task_type] = []
        logger.info(f"Successfully added task definition for {task_type}")

    def remove_task_definition(self, task_type: str) -> None:
        """从目录中移除任务定义。"""
        if not task_type:
            raise ConfigurationError("Task type cannot be empty")

        if task_type not in self._task_catalog:
            raise ConfigurationError(f"Task {task_type} not found in catalog")

        try:
            del self._task_catalog[task_type]
            del self._completion_listeners[task_type]
            logger.info(f"Successfully removed task definition for {task_type}")
        except Exception as e:
            logger.error(f"Failed to remove task definition: {str(e)}")
            raise ConfigurationError(f"Failed to remove task definition: {str(e)}")

    def get_task_definition(self, task_type: str) -> Optional[TaskDefinition]:
        """按任务类型获取任务定义。"""
        if not task_type:
            raise ConfigurationError("Task type cannot be empty")
        return self._task_catalog.get(task_type)

    def add_completion_listener(
        self, task_type: str, callback: TaskCompletionCallback
    ) -> None:
        """为指定任务类型添加完成回调。"""
        if not task_type:
            raise ConfigurationError("Task type cannot be empty")

        if not callback:
            raise ConfigurationError("Callback cannot be None")

        if task_type not in self._task_catalog:
            raise ConfigurationError(f"Task type {task_type} not found in catalog")

        try:
            if task_type not in self._completion_listeners:
                self._completion_listeners[task_type] = []
            self._completion_listeners[task_type].append(callback)
            logger.debug(f"Added completion listener for task type {task_type}")
        except Exception as e:
            logger.error(f"Failed to add completion listener: {str(e)}")
            raise ConfigurationError(f"Failed to add completion listener: {str(e)}")

    def remove_completion_listener(
        self, task_type: str, callback: TaskCompletionCallback
    ) -> None:
        """移除指定任务类型的完成回调。"""
        if not task_type or not callback:
            logger.warning(
                "Attempted to remove listener with empty task type or callback"
            )
            return

        if task_type not in self._completion_listeners:
            logger.warning(f"No listeners found for task type {task_type}")
            return

        try:
            self._completion_listeners[task_type].remove(callback)
            logger.debug(f"Removed completion listener for task type {task_type}")
        except ValueError:
            logger.warning(f"Callback not found for task type {task_type}")
        except Exception as e:
            logger.error(f"Failed to remove completion listener: {str(e)}")
            raise ConfigurationError(f"Failed to remove completion listener: {str(e)}")

    async def notify_task_completion(self, task_info: TaskInfo) -> None:
        """任务完成时通知已注册回调。"""
        if not task_info:
            raise ConfigurationError("Task info cannot be None")

        task_type = task_info.task_type
        if not task_type:
            raise ConfigurationError("Task type cannot be empty")

        if task_type in self._completion_listeners:
            for callback in self._completion_listeners[task_type]:
                try:
                    await callback(task_info)
                except Exception as e:
                    logger.error(
                        f"Error in completion callback for task {task_type}: {str(e)} {traceback.format_exc()}"
                    )
                    raise e


# 全局任务目录
task_catalog = TaskCatalog()
