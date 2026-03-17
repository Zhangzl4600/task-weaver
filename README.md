# Task Weaver

Task Weaver 是一个强大的分布式任务管理库，专门用于处理GPU和API资源的任务调度和执行。它提供了灵活的任务队列管理、服务器资源分配和任务执行监控功能。

## 特性

- 支持多种任务类型（GPU任务和API任务）
- 智能的服务器资源分配
- 任务优先级管理
- 实时任务状态监控
- 可靠的错误处理和恢复机制
- 支持异步并发执行
- 服务器健康检查和自动重连

## 安装

使用 pip 安装:

```bash
pip install task-weaver
```

## 快速开始

### 基本使用

```python
from task_weaver import (
    task_manager,
    server_manager,
    task_catalog,
    TaskPriority,
    ResourceType
)

# 1. 注册服务器
server_manager.register_server(
    ip="http://192.168.1.100:8000",
    server_name="gpu-1",
    description="GPU Server 1",
    tier=1,
    available_task_types=["image_generation"],
    server_type=ResourceType.GPU,
    max_concurrency=2  # 可选：单服务器并发槽位，默认 1
)

# 2. 定义并注册任务处理器
async def process_image(server, task_info, **params):
    # 实现你的任务处理逻辑
    result = await your_processing_logic(params)
    return result

task_catalog.add_task(
    task_type="image_generation",
    executor=process_image,
    required_resources=ResourceType.GPU,
    max_concurrency=10  # 可选：任务类型最大并发，None 为不限制
)

# 3. 创建并执行任务
async def main():
    task = await task_manager.create_task(
        task_type="image_generation",
        params={
            "prompt": "A beautiful sunset",
            "steps": 30
        },
        priority=TaskPriority.HIGH
    )
    
    # 获取任务状态
    task_info = task_manager.get_task_info(task.task_info.task_id)
```

### 服务器管理

```python
# 添加服务器到运行队列
success, message = server_manager.add_running_server(ip="http://192.168.1.100:8000")

# 从运行队列中移除服务器
success, message = server_manager.remove_running_server(ip="http://192.168.1.100:8000")

# 检查服务器状态
server = server_manager.get_server_by_identifier(ip="http://192.168.1.100:8000")
```

## 高级功能

### 任务优先级

Task Weaver 支持三种任务优先级：

```python
from task_weaver import TaskPriority

# 创建不同优先级的任务
high_priority_task = await task_manager.create_task(
    task_type="image_generation",
    params=params,
    priority=TaskPriority.HIGH
)

medium_priority_task = await task_manager.create_task(
    task_type="image_generation",
    params=params,
    priority=TaskPriority.MEDIUM
)

low_priority_task = await task_manager.create_task(
    task_type="image_generation",
    params=params,
    priority=TaskPriority.LOW
)
```

### 任务状态监控

```python
from task_weaver import TaskStatus

# 获取任务信息
task_info = task_manager.get_task_info(task_id)

# 检查任务状态
if task_info.status == TaskStatus.FINISH:
    print("任务完成:", task_info.result)
elif task_info.status == TaskStatus.FAIL:
    print("任务失败:", task_info.error)
```

### 同一任务类型下按子业务限流

```python
task_catalog.add_task_definition(
    task_name="Cloud Image",
    task_type="cloud_image_v1",
    executor=cloud_image_executor,
    required_resource=ResourceType.API,
    # 任务类型总并发（可选）
    max_concurrency=20,
    # 子业务并发规则（可选）
    subtask_key="provider",
    subtask_concurrency={
        "gemini": 10,
        "baidu": 2,
        "*": 5,  # 未配置的 provider 走兜底并发
    },
)

# 提交任务时带上 provider 参数
task = await task_manager.create_task(
    task_type="cloud_image_v1",
    priority=TaskPriority.MEDIUM,
    provider="gemini",
    prompt="a cat in watercolor style",
)
await task_manager.add_task(task)
```

## 配置要求

- Python 3.10 或更高版本
- 异步支持 (asyncio)
- 网络连接（用于服务器通信）

## 许可证

本项目采用 MIT 许可证。详见 [LICENSE](LICENSE) 文件。

