import datetime as dt
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, validator


class EventPayload(BaseModel):
    """Schema for incoming event data."""
    topic: str = Field(..., min_length=1, max_length=255, description="Event topic/category")
    event_id: str = Field(..., min_length=1, max_length=255, description="Unique event identifier")
    timestamp: dt.datetime = Field(..., description="Event timestamp (ISO8601)")
    source: str = Field(..., min_length=1, max_length=255, description="Event source identifier")
    payload: Dict[str, Any] = Field(..., description="Event payload data")

    @validator("timestamp", pre=True, allow_reuse=True)
    def parse_ts(cls, v: Any) -> dt.datetime:
        if isinstance(v, dt.datetime):
            return v
        return dt.datetime.fromisoformat(v.replace('Z', '+00:00'))

    class Config:
        schema_extra = {
            "example": {
                "topic": "user-events",
                "event_id": "evt-12345-uuid",
                "timestamp": "2024-01-01T12:00:00Z",
                "source": "user-service",
                "payload": {"action": "login", "user_id": 123}
            }
        }


class PublishRequest(BaseModel):
    """Request body for publishing events."""
    events: List[EventPayload] = Field(..., min_items=1, description="List of events to publish")

    @validator("events", allow_reuse=True)
    def ensure_not_empty(cls, v: List[EventPayload]) -> List[EventPayload]:
        if not v:
            raise ValueError("events must not be empty")
        return v


class PublishResponse(BaseModel):
    """Response from publish endpoint."""
    accepted: int = Field(..., description="Number of events accepted")
    queued: int = Field(..., description="Number of events queued for processing")
    processed: Optional[int] = Field(None, description="Events processed (atomic mode only)")
    duplicates: Optional[int] = Field(None, description="Duplicates detected (atomic mode only)")


class EventOut(BaseModel):
    """Schema for event output."""
    topic: str
    event_id: str
    timestamp: dt.datetime
    source: str
    payload: Dict[str, Any]
    processed_at: Optional[dt.datetime]


class StatsResponse(BaseModel):
    """Statistics response schema."""
    received: int = Field(..., description="Total events received")
    unique_processed: int = Field(..., description="Unique events processed")
    duplicate_dropped: int = Field(..., description="Duplicate events dropped")
    topics: List[str] = Field(..., description="List of active topics")
    uptime_seconds: float = Field(..., description="Service uptime in seconds")
    dedup_rate_percent: float = Field(0.0, description="Deduplication rate percentage")


class HealthResponse(BaseModel):
    """Health check response."""
    status: str = Field(..., description="Overall health status")
    database: bool = Field(..., description="Database connectivity")
    queue: bool = Field(..., description="Queue connectivity")
    uptime_seconds: float = Field(..., description="Service uptime")


class QueueStatsResponse(BaseModel):
    """Queue statistics response."""
    queue_size: int = Field(..., description="Current queue size")
    queue_type: str = Field(..., description="Queue implementation type")
    worker_count: int = Field(..., description="Number of worker threads")
    workers_enabled: bool = Field(..., description="Whether workers are running")

