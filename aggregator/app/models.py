import datetime as dt
from typing import Any, Dict

from sqlalchemy import JSON, Column, DateTime, Integer, String, Index, UniqueConstraint, Text

from .db import Base


class Event(Base):
    """
    Processed events table with deduplication via unique constraint.
    
    The unique constraint on (topic, event_id) ensures idempotent processing:
    - INSERT with existing key fails with IntegrityError
    - This is handled by consumer to count as duplicate
    
    Indexes:
    - Primary key on id (auto)
    - Unique constraint on (topic, event_id) - for dedup
    - Index on topic for efficient filtering
    - Index on processed_at for ordering
    """
    __tablename__ = "events"
    __table_args__ = (
        UniqueConstraint("topic", "event_id", name="uq_event_topic_event_id"),
        Index("ix_events_topic", "topic"),
        Index("ix_events_processed_at", "processed_at"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String(255), nullable=False)
    event_id = Column(String(255), nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False)
    source = Column(String(255), nullable=False)
    payload = Column(JSON, nullable=False)
    processed_at = Column(DateTime(timezone=True), default=dt.datetime.utcnow)

    def __repr__(self) -> str:
        return f"<Event(topic={self.topic}, event_id={self.event_id})>"


class Metrics(Base):
    """
    Aggregator metrics table.
    
    Counter updates use atomic SQL: SET count = count + 1
    This prevents lost updates under concurrent access.
    
    Single row design (id=1) simplifies queries and ensures
    all workers update the same counters atomically.
    """
    __tablename__ = "metrics"

    id = Column(Integer, primary_key=True)
    received_count = Column(Integer, default=0, nullable=False)
    unique_processed_count = Column(Integer, default=0, nullable=False)
    duplicate_dropped_count = Column(Integer, default=0, nullable=False)
    started_at = Column(DateTime(timezone=True), default=dt.datetime.utcnow, nullable=False)

    def __repr__(self) -> str:
        return f"<Metrics(received={self.received_count}, unique={self.unique_processed_count}, dup={self.duplicate_dropped_count})>"


class AuditLog(Base):
    """
    Audit log for tracking event processing decisions.
    
    Records both successful inserts and duplicate detections
    for debugging and compliance purposes.
    """
    __tablename__ = "audit_log"
    __table_args__ = (
        Index("ix_audit_log_created_at", "created_at"),
        Index("ix_audit_log_action", "action"),
    )

    id = Column(Integer, primary_key=True, autoincrement=True)
    topic = Column(String(255), nullable=False)
    event_id = Column(String(255), nullable=False)
    action = Column(String(50), nullable=False)  # 'INSERTED', 'DUPLICATE_DROPPED'
    worker_id = Column(String(50), nullable=True)
    details = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=dt.datetime.utcnow)

    def __repr__(self) -> str:
        return f"<AuditLog(topic={self.topic}, event_id={self.event_id}, action={self.action})>"

