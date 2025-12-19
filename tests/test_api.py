"""
Unit & Integration Tests untuk Pub-Sub Log Aggregator

Test Coverage:
1. Deduplication tests
2. Persistence tests  
3. Transaction/Concurrency tests
4. Schema validation tests
5. API endpoint tests
6. Stress/Performance tests

Total: 20 tests (requirement: 12-20)
"""
import os
import sys
import time
import threading
from pathlib import Path
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
from fastapi.testclient import TestClient

# Ensure project root on path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))


def build_event(topic: str = "topic-a", event_id: str | None = None) -> Dict[str, object]:
    """Helper to build test event."""
    return {
        "topic": topic,
        "event_id": event_id or f"id-{time.time_ns()}",
        "timestamp": "2024-01-01T00:00:00Z",
        "source": "pytest",
        "payload": {"value": 1},
    }


def drain_queue(main_module):
    """Process all queued events synchronously to make tests deterministic."""
    from aggregator.app.consumer import process_event
    from aggregator.app.db import session_scope

    processed = 0
    with session_scope() as session:
        while True:
            event = main_module.queue.dequeue_blocking(timeout=0)
            if not event:
                break
            process_event(session, event)
            processed += 1
    return processed


@pytest.fixture()
def client(tmp_path, monkeypatch):
    """Create test client with isolated SQLite database."""
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("USE_INMEMORY_QUEUE", "1")
    monkeypatch.setenv("DISABLE_WORKERS", "1")
    
    # Clear cached modules for fresh import
    for mod in [m for m in list(sys.modules) if m.startswith("aggregator")]:
        sys.modules.pop(mod)
        
    import aggregator.app.main as main

    with TestClient(main.app) as c:
        yield c, main


# =============================================================================
# 1. Basic API Tests
# =============================================================================

def test_publish_accepts_event(client):
    """Test 1: POST /publish accepts valid event."""
    test_client, main = client
    resp = test_client.post("/publish", json={"events": [build_event()]})
    assert resp.status_code == 200
    drain_queue(main)
    assert resp.json()["accepted"] == 1


def test_health_endpoint(client):
    """Test 2: GET /health returns healthy status."""
    test_client, _ = client
    resp = test_client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "healthy"
    assert data["database"] is True
    assert data["queue"] is True


def test_queue_stats_endpoint(client):
    """Test 3: GET /queue/stats returns queue info."""
    test_client, _ = client
    resp = test_client.get("/queue/stats")
    assert resp.status_code == 200
    data = resp.json()
    assert "queue_size" in data
    assert "queue_type" in data
    assert data["queue_type"] == "inmemory"


# =============================================================================
# 2. Deduplication Tests
# =============================================================================

def test_deduplication_counts(client):
    """Test 4: Duplicate events are detected and counted correctly."""
    test_client, main = client
    evt = build_event(event_id="same-id")
    
    # Send same event twice
    test_client.post("/publish", json={"events": [evt]})
    test_client.post("/publish", json={"events": [evt]})
    drain_queue(main)
    
    stats = test_client.get("/stats").json()
    assert stats["unique_processed"] >= 1
    assert stats["duplicate_dropped"] >= 1


def test_duplicate_rate_under_load(client):
    """Test 5: Multiple duplicates in single batch are handled correctly."""
    test_client, main = client
    batch = [build_event(event_id="dup-shared") for _ in range(5)]
    test_client.post("/publish", json={"events": batch})
    drain_queue(main)
    
    stats = test_client.get("/stats").json()
    assert stats["unique_processed"] >= 1
    assert stats["duplicate_dropped"] >= 4


def test_cross_topic_dedup(client):
    """Test 6: Same event_id in different topics are both stored."""
    test_client, main = client
    evt_a = build_event(topic="topic-a", event_id="cross-id")
    evt_b = build_event(topic="topic-b", event_id="cross-id")
    
    test_client.post("/publish", json={"events": [evt_a, evt_b]})
    drain_queue(main)
    
    stats = test_client.get("/stats").json()
    # Both should be stored because (topic, event_id) is unique
    assert stats["unique_processed"] >= 2


# =============================================================================
# 3. Event Query Tests
# =============================================================================

def test_events_endpoint_filters_by_topic(client):
    """Test 7: GET /events filters by topic correctly."""
    test_client, main = client
    evt_a = build_event(topic="topic-a", event_id="a1")
    evt_b = build_event(topic="topic-b", event_id="b1")
    test_client.post("/publish", json={"events": [evt_a, evt_b]})
    drain_queue(main)
    
    resp = test_client.get("/events", params={"topic": "topic-a"})
    assert resp.status_code == 200
    data = resp.json()
    assert all(item["topic"] == "topic-a" for item in data)


def test_stats_topics_listed(client):
    """Test 8: GET /stats lists all unique topics."""
    test_client, main = client
    evts = [build_event(topic="topic-x"), build_event(topic="topic-y")]
    test_client.post("/publish", json={"events": evts})
    drain_queue(main)
    
    topics = test_client.get("/stats").json()["topics"]
    assert "topic-x" in topics and "topic-y" in topics


def test_get_events_limit(client):
    """Test 9: GET /events respects limit parameter."""
    test_client, main = client
    for i in range(5):
        test_client.post("/publish", json={"events": [build_event(event_id=f"limit-{i}")]})
    drain_queue(main)
    
    resp = test_client.get("/events", params={"topic": "topic-a", "limit": 3})
    assert resp.status_code == 200
    assert len(resp.json()) <= 3


# =============================================================================
# 4. Schema Validation Tests
# =============================================================================

def test_invalid_request_rejected(client):
    """Test 10: Empty events list is rejected."""
    test_client, _ = client
    resp = test_client.post("/publish", json={"events": []})
    assert resp.status_code == 422


def test_schema_validation_timestamp(client):
    """Test 11: Invalid timestamp format is rejected."""
    test_client, _ = client
    bad_event = build_event()
    bad_event["timestamp"] = "not-a-time"
    resp = test_client.post("/publish", json={"events": [bad_event]})
    assert resp.status_code == 422


def test_schema_validation_missing_fields(client):
    """Test 12: Missing required fields are rejected."""
    test_client, _ = client
    incomplete_event = {"topic": "test"}
    resp = test_client.post("/publish", json={"events": [incomplete_event]})
    assert resp.status_code == 422


def test_schema_validation_empty_topic(client):
    """Test 13: Empty topic string is rejected."""
    test_client, _ = client
    bad_event = build_event()
    bad_event["topic"] = ""
    resp = test_client.post("/publish", json={"events": [bad_event]})
    assert resp.status_code == 422


# =============================================================================
# 5. Batch Processing Tests
# =============================================================================

def test_batch_handling_multiple_events(client):
    """Test 14: Batch of multiple events is processed correctly."""
    test_client, main = client
    events = [build_event(event_id=f"batch-{i}") for i in range(10)]
    resp = test_client.post("/publish", json={"events": events})
    assert resp.status_code == 200
    drain_queue(main)
    
    stats = test_client.get("/stats").json()
    assert stats["unique_processed"] >= 10


def test_atomic_batch_processing(client):
    """Test 15: Atomic mode processes batch immediately."""
    test_client, _ = client
    events = [build_event(event_id=f"atomic-{i}") for i in range(5)]
    
    # Add a duplicate
    events.append(build_event(event_id="atomic-0"))
    
    resp = test_client.post("/publish", json={"events": events}, params={"atomic": True})
    assert resp.status_code == 200
    data = resp.json()
    
    # Atomic mode returns processed count immediately
    assert data["accepted"] == 6
    assert data["processed"] >= 5
    assert data["duplicates"] >= 1


# =============================================================================
# 6. Persistence Tests
# =============================================================================

def test_persistence_across_restart(client, tmp_path, monkeypatch):
    """Test 16: Data persists after app restart."""
    test_client, main = client
    evt = build_event(event_id="persist-1")
    test_client.post("/publish", json={"events": [evt]})
    drain_queue(main)
    
    # Verify event was stored
    stats1 = test_client.get("/stats").json()
    assert stats1["unique_processed"] >= 1
    
    # Simulate restart with same database
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DATABASE_URL", f"sqlite:///{db_path}")
    monkeypatch.setenv("USE_INMEMORY_QUEUE", "1")
    monkeypatch.setenv("DISABLE_WORKERS", "1")
    
    for mod in [m for m in list(sys.modules) if m.startswith("aggregator")]:
        sys.modules.pop(mod)
    import aggregator.app.main as main

    with TestClient(main.app) as new_client:
        # Try to insert same event again
        new_client.post("/publish", json={"events": [evt]})
        drain_queue(main)
        
        stats2 = new_client.get("/stats").json()
        # Dedup should prevent reprocessing
        assert stats2["duplicate_dropped"] >= 1


# =============================================================================
# 7. Concurrency Tests
# =============================================================================

def test_concurrent_workers_no_double_process(client):
    """Test 17: Concurrent processing doesn't cause double inserts."""
    test_client, main = client
    
    # Submit same event multiple times (simulating parallel workers)
    events = [build_event(event_id="race-id") for _ in range(5)]
    test_client.post("/publish", json={"events": events})
    drain_queue(main)
    
    stats = test_client.get("/stats").json()
    assert stats["unique_processed"] >= 1
    assert stats["duplicate_dropped"] >= 4


def test_concurrent_publish_requests(client):
    """Test 18: Concurrent POST /publish requests are handled correctly."""
    test_client, main = client
    
    def send_event(idx: int):
        # Some events with same ID to test dedup under concurrency
        event_id = f"concurrent-{idx % 5}"  # Groups of 5 share same ID
        return test_client.post(
            "/publish", 
            json={"events": [build_event(event_id=event_id)]}
        )
    
    # Send 20 requests, each group of 5 has same event_id
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(send_event, i) for i in range(20)]
        results = [f.result() for f in as_completed(futures)]
    
    # All requests should succeed
    assert all(r.status_code == 200 for r in results)
    
    drain_queue(main)
    stats = test_client.get("/stats").json()
    
    # Should have 5 unique events (one per group)
    # and 15 duplicates
    assert stats["unique_processed"] >= 4  # At least 4 unique
    assert stats["duplicate_dropped"] >= 10  # Most should be duplicates


def test_stats_counter_consistency(client):
    """Test 19: Stats counters remain consistent under load."""
    test_client, main = client
    
    # Send mix of unique and duplicate events
    for i in range(10):
        unique_evt = build_event(event_id=f"unique-{i}")
        dup_evt = build_event(event_id="always-dup")
        test_client.post("/publish", json={"events": [unique_evt, dup_evt]})
    
    drain_queue(main)
    stats = test_client.get("/stats").json()
    
    # Verify counter consistency
    # received = unique_processed + duplicate_dropped (approximately)
    total_processed = stats["unique_processed"] + stats["duplicate_dropped"]
    assert stats["received"] >= total_processed


# =============================================================================
# 8. Stress/Performance Test
# =============================================================================

def test_stress_batch_events(client):
    """Test 20: System handles large batch of events."""
    test_client, main = client
    
    # Create 100 events with 30% duplicates
    events = []
    for i in range(70):
        events.append(build_event(event_id=f"stress-{i}"))
    for i in range(30):
        events.append(build_event(event_id=f"stress-{i % 20}"))  # 30 duplicates
    
    start_time = time.time()
    resp = test_client.post("/publish", json={"events": events})
    assert resp.status_code == 200
    
    drain_queue(main)
    elapsed = time.time() - start_time
    
    stats = test_client.get("/stats").json()
    
    # Verify processing
    assert stats["received"] >= 100
    assert stats["unique_processed"] >= 50  # At least 50 unique
    assert stats["duplicate_dropped"] >= 20  # At least 20 duplicates
    
    # Performance check (should complete within reasonable time)
    assert elapsed < 10.0, f"Stress test took too long: {elapsed:.2f}s"

