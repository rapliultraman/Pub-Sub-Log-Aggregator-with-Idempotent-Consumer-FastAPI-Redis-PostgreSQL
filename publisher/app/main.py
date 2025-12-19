"""
Publisher Service - Event Generator dengan Duplikasi Terkontrol

Fitur:
- Generate events dengan rate terkontrol
- Duplikasi terkontrol (default 35%)
- Retry eksponensial untuk fault tolerance
- Batch support untuk throughput tinggi
"""
import asyncio
import json
import os
import random
import sys
import time
import uuid
from typing import Dict, List

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configuration from environment
TARGET_URL = os.getenv("TARGET_URL", "http://localhost:8080/publish")
TOPIC = os.getenv("TOPIC", "demo-topic")
RATE_PER_SEC = float(os.getenv("RATE_PER_SEC", "50"))
DUP_RATE = float(os.getenv("DUP_RATE", "0.35"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "1"))
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "0"))  # 0 = infinite


def build_event(topic: str = None) -> Dict[str, object]:
    """Build a new event with unique ID."""
    event_id = str(uuid.uuid4())
    now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    return {
        "topic": topic or TOPIC,
        "event_id": event_id,
        "timestamp": now,
        "source": "publisher",
        "payload": {
            "value": random.randint(1, 1000),
            "generated_at": time.time(),
        },
    }


@retry(
    stop=stop_after_attempt(5), 
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((httpx.ConnectError, httpx.TimeoutException))
)
async def send_batch(events: List[Dict[str, object]], client: httpx.AsyncClient) -> dict:
    """Send batch of events with retry logic."""
    resp = await client.post(TARGET_URL, json={"events": events})
    resp.raise_for_status()
    return resp.json()


async def main() -> None:
    """Main publisher loop."""
    print(f"=" * 60)
    print(f"Publisher Started")
    print(f"=" * 60)
    print(f"Target URL: {TARGET_URL}")
    print(f"Topic: {TOPIC}")
    print(f"Rate: {RATE_PER_SEC}/s")
    print(f"Duplicate Rate: {DUP_RATE * 100}%")
    print(f"Batch Size: {BATCH_SIZE}")
    print(f"Total Events: {'infinite' if TOTAL_EVENTS == 0 else TOTAL_EVENTS}")
    print(f"=" * 60)
    
    # Pool untuk menyimpan event yang akan diduplikasi
    dup_buffer: List[Dict[str, object]] = []
    buffer_max_size = 100
    
    # Stats
    total_sent = 0
    total_unique = 0
    total_duplicates = 0
    start_time = time.time()
    
    interval = 1.0 / RATE_PER_SEC if RATE_PER_SEC > 0 else 0.1
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        # Wait for aggregator to be ready
        print("Waiting for aggregator to be ready...")
        for attempt in range(30):
            try:
                health = await client.get(TARGET_URL.replace("/publish", "/health"))
                if health.status_code == 200:
                    print("Aggregator is ready!")
                    break
            except Exception:
                pass
            await asyncio.sleep(1)
        else:
            print("Warning: Could not confirm aggregator health, proceeding anyway...")
        
        try:
            while True:
                batch: List[Dict[str, object]] = []
                
                # Build batch
                for _ in range(BATCH_SIZE):
                    # Decide whether to send duplicate or new event
                    if dup_buffer and random.random() < DUP_RATE:
                        # Send duplicate from buffer
                        event = random.choice(dup_buffer)
                        total_duplicates += 1
                    else:
                        # Generate new event
                        event = build_event()
                        total_unique += 1
                        
                        # Add to buffer for future duplication
                        if len(dup_buffer) < buffer_max_size:
                            dup_buffer.append(event)
                        else:
                            # Replace random item
                            dup_buffer[random.randint(0, buffer_max_size - 1)] = event
                    
                    batch.append(event)
                
                # Send batch
                try:
                    result = await send_batch(batch, client)
                    total_sent += len(batch)
                    
                    # Progress logging every 100 events
                    if total_sent % 100 == 0:
                        elapsed = time.time() - start_time
                        rate = total_sent / elapsed if elapsed > 0 else 0
                        print(f"Sent: {total_sent} | Unique: {total_unique} | Duplicates: {total_duplicates} | Rate: {rate:.1f}/s")
                        
                except Exception as e:
                    print(f"Error sending batch: {e}")
                
                # Check if we've hit the limit
                if TOTAL_EVENTS > 0 and total_sent >= TOTAL_EVENTS:
                    print(f"\nReached target of {TOTAL_EVENTS} events")
                    break
                
                await asyncio.sleep(interval)
                
        except asyncio.CancelledError:
            pass
        finally:
            elapsed = time.time() - start_time
            print(f"\n" + "=" * 60)
            print(f"Publisher Summary")
            print(f"=" * 60)
            print(f"Total Sent: {total_sent}")
            print(f"Unique Events: {total_unique}")
            print(f"Duplicate Events: {total_duplicates}")
            print(f"Duration: {elapsed:.2f}s")
            print(f"Average Rate: {total_sent / elapsed if elapsed > 0 else 0:.2f}/s")
            print(f"Actual Dup Rate: {total_duplicates / total_sent * 100 if total_sent > 0 else 0:.1f}%")
            print(f"=" * 60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nPublisher stopped by user")
        sys.exit(0)

