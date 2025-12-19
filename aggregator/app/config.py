import os
from dataclasses import dataclass


@dataclass
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg2://user:pass@localhost:5432/uasdb",
    )
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    worker_count: int = int(os.getenv("WORKER_COUNT", "2"))
    queue_key: str = os.getenv("QUEUE_KEY", "event_queue")
    metrics_row_id: int = 1
    disable_workers: bool = os.getenv("DISABLE_WORKERS", "0") == "1"
    use_inmemory_queue: bool = os.getenv("USE_INMEMORY_QUEUE", "0") == "1"


settings = Settings()

