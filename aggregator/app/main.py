import datetime as dt
import logging
import time
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from .config import settings
from .consumer import increment_received, start_workers, process_batch_atomic
from .db import Base, SessionLocal, engine, get_session, session_scope
from .models import Event, Metrics
from .queue import InMemoryQueue, RedisQueue
from .schemas import (
    EventOut, 
    EventPayload, 
    PublishRequest, 
    PublishResponse, 
    StatsResponse,
    HealthResponse,
    QueueStatsResponse,
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("aggregator")

queue = InMemoryQueue() if settings.use_inmemory_queue else RedisQueue()
start_time = time.time()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan event handler for startup and shutdown."""
    # Startup
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created/verified")
    
    if not settings.disable_workers:
        start_workers(queue, settings.worker_count)
        logger.info("Started %d background workers", settings.worker_count)
    else:
        logger.info("Workers disabled (test mode)")
    
    yield  # Application runs here
    
    # Shutdown (cleanup if needed)
    logger.info("Shutting down aggregator")


app = FastAPI(
    title="Pub-Sub Log Aggregator", 
    version="1.0.0",
    description="Distributed Pub-Sub Log Aggregator with Idempotent Consumer and Deduplication",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse)
def health_check(db: Session = Depends(get_session)) -> HealthResponse:
    """
    Health check endpoint for liveness/readiness probes.
    
    Checks:
    - Database connectivity
    - Queue connectivity (if using Redis)
    """
    db_healthy = True
    queue_healthy = True
    
    try:
        # Test database connection
        db.execute(select(1))
    except Exception as e:
        logger.error("Database health check failed: %s", str(e))
        db_healthy = False
        
    try:
        # Test queue connection
        queue.size()
    except Exception as e:
        logger.error("Queue health check failed: %s", str(e))
        queue_healthy = False
        
    status = "healthy" if (db_healthy and queue_healthy) else "unhealthy"
    
    return HealthResponse(
        status=status,
        database=db_healthy,
        queue=queue_healthy,
        uptime_seconds=time.time() - start_time,
    )


@app.post("/publish", response_model=PublishResponse)
def publish(
    req: PublishRequest, 
    db: Session = Depends(get_session),
    atomic: bool = Query(False, description="If true, process batch atomically (all or nothing)")
) -> PublishResponse:
    """
    Publish one or more events to the aggregator.
    
    Events are queued for processing by background workers.
    Each event must have a unique (topic, event_id) combination.
    
    Query params:
    - atomic: If true, events are processed immediately in atomic transaction
    
    Idempotency: Duplicate events (same topic+event_id) are detected and dropped.
    """
    events = [e.dict() for e in req.events]
    increment_received(db, len(events))
    
    if atomic:
        # Process immediately with atomic transaction
        with session_scope() as session:
            inserted, duplicates = process_batch_atomic(session, events)
        logger.info("Atomic batch: accepted=%d, inserted=%d, duplicates=%d", 
                   len(events), inserted, duplicates)
        return PublishResponse(
            accepted=len(events), 
            queued=0,
            processed=inserted,
            duplicates=duplicates,
        )
    else:
        # Queue for async processing
        for ev in events:
            queue.enqueue(ev)
        logger.info("Queued %d events for processing", len(events))
        return PublishResponse(accepted=len(events), queued=len(events))


@app.get("/events", response_model=List[EventOut])
def list_events(
    topic: str = Query(..., description="Topic name to filter events"),
    limit: int = Query(100, ge=1, le=1000, description="Maximum events to return"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
    db: Session = Depends(get_session),
) -> List[EventOut]:
    """
    List processed events for a specific topic.
    
    Returns events in reverse chronological order (newest first).
    Supports pagination via limit and offset.
    """
    rows = db.execute(
        select(Event)
        .where(Event.topic == topic)
        .order_by(Event.processed_at.desc())
        .limit(limit)
        .offset(offset)
    )
    return [
        EventOut(
            topic=ev.topic,
            event_id=ev.event_id,
            timestamp=ev.timestamp,
            source=ev.source,
            payload=ev.payload,
            processed_at=ev.processed_at,
        )
        for ev in rows.scalars()
    ]


@app.get("/stats", response_model=StatsResponse)
def stats(db: Session = Depends(get_session)) -> StatsResponse:
    """
    Get aggregator statistics.
    
    Returns:
    - received: Total events received
    - unique_processed: Events successfully stored (deduplicated)
    - duplicate_dropped: Duplicate events detected and dropped
    - topics: List of unique topics
    - uptime_seconds: Time since service start
    """
    metrics = db.get(Metrics, settings.metrics_row_id)
    if not metrics:
        metrics = Metrics(
            id=settings.metrics_row_id,
            received_count=0,
            unique_processed_count=0,
            duplicate_dropped_count=0,
        )
        db.add(metrics)
        db.commit()
        
    topics = [row[0] for row in db.execute(select(Event.topic).distinct())]
    uptime = time.time() - start_time
    
    # Calculate dedup rate
    total_processed = metrics.unique_processed_count + metrics.duplicate_dropped_count
    dedup_rate = (metrics.duplicate_dropped_count / total_processed * 100) if total_processed > 0 else 0.0
    
    return StatsResponse(
        received=metrics.received_count,
        unique_processed=metrics.unique_processed_count,
        duplicate_dropped=metrics.duplicate_dropped_count,
        topics=topics,
        uptime_seconds=uptime,
        dedup_rate_percent=round(dedup_rate, 2),
    )


@app.get("/queue/stats", response_model=QueueStatsResponse)
def queue_stats() -> QueueStatsResponse:
    """Get queue statistics and configuration."""
    return QueueStatsResponse(
        queue_size=queue.size(),
        queue_type="inmemory" if settings.use_inmemory_queue else "redis",
        worker_count=settings.worker_count,
        workers_enabled=not settings.disable_workers,
    )


@app.delete("/events")
def clear_events(
    topic: Optional[str] = Query(None, description="Topic to clear (all if not specified)"),
    db: Session = Depends(get_session),
) -> dict:
    """
    Clear events (for testing/demo purposes).
    
    WARNING: This is destructive! Use only for testing.
    """
    if topic:
        result = db.execute(
            Event.__table__.delete().where(Event.topic == topic)
        )
        db.commit()
        return {"deleted": result.rowcount, "topic": topic}
    else:
        result = db.execute(Event.__table__.delete())
        db.commit()
        return {"deleted": result.rowcount, "topic": "all"}


@app.post("/metrics/reset")
def reset_metrics(db: Session = Depends(get_session)) -> dict:
    """
    Reset metrics counters (for testing/demo purposes).
    
    WARNING: This resets all counters! Use only for testing.
    """
    metrics = db.get(Metrics, settings.metrics_row_id)
    if metrics:
        metrics.received_count = 0
        metrics.unique_processed_count = 0
        metrics.duplicate_dropped_count = 0
        db.commit()
    return {"status": "reset"}

