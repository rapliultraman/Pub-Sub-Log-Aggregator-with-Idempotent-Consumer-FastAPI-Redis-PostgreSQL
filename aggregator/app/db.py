from contextlib import contextmanager
from typing import Generator

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session, declarative_base, sessionmaker

from .config import settings

# Configure engine with connection pool settings
engine = create_engine(
    settings.database_url, 
    pool_pre_ping=True, 
    pool_size=10,
    max_overflow=20,
    future=True
)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


def get_session() -> Generator[Session, None, None]:
    """FastAPI dependency for database sessions."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Context manager for database sessions.
    
    Provides automatic commit on success and rollback on exception.
    Used by background workers for thread-safe session management.
    
    Isolation Level: Uses database default (READ COMMITTED for PostgreSQL)
    - Sufficient for our dedup pattern with unique constraints
    - Atomic counter updates prevent lost updates
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()

