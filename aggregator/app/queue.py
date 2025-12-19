import json
from datetime import datetime
from typing import Any, Dict, Optional

import redis

from .config import settings


class DateTimeEncoder(json.JSONEncoder):
    """Custom JSON encoder that handles datetime objects."""
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)


class RedisQueue:
    def __init__(self, url: str = settings.redis_url, queue_key: str = settings.queue_key):
        self.client = redis.Redis.from_url(url, decode_responses=True)
        self.queue_key = queue_key

    def enqueue(self, event: Dict[str, Any]) -> None:
        self.client.rpush(self.queue_key, json.dumps(event, cls=DateTimeEncoder))

    def dequeue_blocking(self, timeout: int = 5) -> Optional[Dict[str, Any]]:
        data = self.client.blpop(self.queue_key, timeout=timeout)
        if data is None:
            return None
        _, payload = data
        return json.loads(payload)

    def size(self) -> int:
        return int(self.client.llen(self.queue_key))


class InMemoryQueue:
    """Simple queue for tests."""

    def __init__(self):
        self.items: list[Dict[str, Any]] = []

    def enqueue(self, event: Dict[str, Any]) -> None:
        self.items.append(event)

    def dequeue_blocking(self, timeout: int = 0) -> Optional[Dict[str, Any]]:
        if not self.items:
            return None
        return self.items.pop(0)

    def size(self) -> int:
        return len(self.items)

