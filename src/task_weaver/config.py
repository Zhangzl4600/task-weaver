import os
from dataclasses import dataclass

@dataclass
class LibraryConfig:
    """Configuration class for task_weaver"""

    debug: bool = False
    api_base_url: str = "https://api.example.com"
    api_timeout: int = 30
    queue_backend: str = "memory"
    redis_url: str = "redis://localhost:6379/0"
    queue_prefix: str = "task_weaver"
    queue_consumer_group: str = "task_weaver"
    queue_block_ms: int = 1000
    queue_reclaim_idle_ms: int = 30000
    queue_discovery_interval_ms: int = 1000

    @classmethod
    def create_default(cls) -> "LibraryConfig":
        """Create a default configuration instance"""
        return cls(
            debug=os.getenv("TASK_WEAVER_DEBUG", "false").lower() == "true",
            api_base_url=os.getenv(
                "TASK_WEAVER_API_BASE_URL", "https://api.example.com"
            ),
            api_timeout=int(os.getenv("TASK_WEAVER_API_TIMEOUT", "30")),
            queue_backend=os.getenv("TASK_WEAVER_QUEUE_BACKEND", "memory"),
            redis_url=os.getenv(
                "TASK_WEAVER_REDIS_URL", "redis://localhost:6379/0"
            ),
            queue_prefix=os.getenv("TASK_WEAVER_QUEUE_PREFIX", "task_weaver"),
            queue_consumer_group=os.getenv(
                "TASK_WEAVER_QUEUE_CONSUMER_GROUP", "task_weaver"
            ),
            queue_block_ms=int(os.getenv("TASK_WEAVER_QUEUE_BLOCK_MS", "1000")),
            queue_reclaim_idle_ms=int(
                os.getenv("TASK_WEAVER_QUEUE_RECLAIM_IDLE_MS", "30000")
            ),
            queue_discovery_interval_ms=int(
                os.getenv("TASK_WEAVER_QUEUE_DISCOVERY_INTERVAL_MS", "1000")
            ),
        )

# Global config instance
config = LibraryConfig.create_default()
