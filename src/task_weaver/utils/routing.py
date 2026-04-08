from __future__ import annotations

DEFAULT_ROUTE_GROUP = "default"
DISPATCH_KEY_SEPARATOR = "::"


def normalize_route_group(route_group: str | None) -> str:
    """规范化路由组，缺省时回落到默认分组。"""
    normalized = str(route_group).strip() if route_group is not None else ""
    return normalized or DEFAULT_ROUTE_GROUP


def build_dispatch_key(task_type: str, route_group: str | None = None) -> str:
    """为队列调度和服务器路由生成稳定的调度键。"""
    if not task_type:
        raise ValueError("task_type cannot be empty")
    return f"{task_type}{DISPATCH_KEY_SEPARATOR}{normalize_route_group(route_group)}"
