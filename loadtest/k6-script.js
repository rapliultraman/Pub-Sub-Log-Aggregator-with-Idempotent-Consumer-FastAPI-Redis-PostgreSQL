/**
 * K6 Load Test Script untuk Pub-Sub Log Aggregator
 * 
 * Menguji:
 * 1. Throughput - Kemampuan sistem menangani beban tinggi
 * 2. Deduplication - Event dengan ID yang sama hanya diproses sekali
 * 3. Latency - Response time di bawah beban
 * 4. Consistency - Stats konsisten setelah load test
 * 
 * Cara menjalankan:
 *   k6 run loadtest/k6-script.js
 * 
 * Dengan options custom:
 *   k6 run --vus 20 --duration 60s loadtest/k6-script.js
 */

import http from "k6/http";
import { check, sleep, group } from "k6";
import { Counter, Rate, Trend } from "k6/metrics";

// Custom metrics
const duplicatesSent = new Counter("duplicates_sent");
const uniqueEventsSent = new Counter("unique_events_sent");
const publishLatency = new Trend("publish_latency_ms");
const errorRate = new Rate("error_rate");

// Test configuration
export let options = {
  // Ramp-up pattern untuk stress test
  stages: [
    { duration: "10s", target: 5 },   // Ramp up ke 5 VUs
    { duration: "30s", target: 10 },  // Naik ke 10 VUs
    { duration: "20s", target: 20 },  // Puncak 20 VUs
    { duration: "10s", target: 0 },   // Ramp down
  ],
  thresholds: {
    http_req_duration: ["p(95)<500"],  // 95% request < 500ms
    error_rate: ["rate<0.05"],          // Error rate < 5%
    "publish_latency_ms": ["avg<200"],  // Average latency < 200ms
  },
};

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const DUP_RATE = parseFloat(__ENV.DUP_RATE || "0.3");  // 30% duplikasi default

// Pool event IDs untuk duplikasi
const eventPool = [];
const POOL_SIZE = 100;

// Generate unique event
function generateEvent(topic, eventId) {
  return {
    topic: topic || "k6-loadtest",
    event_id: eventId || `${__VU}-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`,
    timestamp: new Date().toISOString(),
    source: `k6-vu-${__VU}`,
    payload: { 
      iteration: __ITER,
      random_value: Math.random(),
      vu_id: __VU,
    },
  };
}

// Setup - dijalankan sekali di awal
export function setup() {
  console.log(`Starting load test against ${BASE_URL}`);
  console.log(`Duplicate rate: ${DUP_RATE * 100}%`);
  
  // Verify service is healthy
  const healthRes = http.get(`${BASE_URL}/health`);
  check(healthRes, {
    "service is healthy": (r) => r.status === 200,
  });
  
  // Get initial stats
  const statsRes = http.get(`${BASE_URL}/stats`);
  const initialStats = JSON.parse(statsRes.body);
  
  return { 
    initialStats: initialStats,
    startTime: Date.now(),
  };
}

// Main test function
export default function (data) {
  group("Publish Events", function () {
    // Determine if this should be a duplicate
    const shouldDuplicate = Math.random() < DUP_RATE && eventPool.length > 0;
    
    let event;
    if (shouldDuplicate) {
      // Pick random event from pool for duplication
      event = eventPool[Math.floor(Math.random() * eventPool.length)];
      duplicatesSent.add(1);
    } else {
      // Generate new unique event
      event = generateEvent();
      uniqueEventsSent.add(1);
      
      // Add to pool for future duplication
      if (eventPool.length < POOL_SIZE) {
        eventPool.push(event);
      } else {
        // Replace random item in pool
        eventPool[Math.floor(Math.random() * POOL_SIZE)] = event;
      }
    }
    
    const payload = JSON.stringify({ events: [event] });
    const params = {
      headers: { "Content-Type": "application/json" },
    };
    
    const startTime = Date.now();
    const res = http.post(`${BASE_URL}/publish`, payload, params);
    const latency = Date.now() - startTime;
    
    publishLatency.add(latency);
    
    const success = check(res, {
      "status is 200": (r) => r.status === 200,
      "has accepted field": (r) => JSON.parse(r.body).accepted >= 1,
    });
    
    errorRate.add(!success);
  });
  
  // Occasional stats check (10% of iterations)
  if (Math.random() < 0.1) {
    group("Check Stats", function () {
      const res = http.get(`${BASE_URL}/stats`);
      check(res, {
        "stats available": (r) => r.status === 200,
        "has unique_processed": (r) => JSON.parse(r.body).unique_processed !== undefined,
        "has duplicate_dropped": (r) => JSON.parse(r.body).duplicate_dropped !== undefined,
      });
    });
  }
  
  // Occasional events check (5% of iterations)
  if (Math.random() < 0.05) {
    group("Check Events", function () {
      const res = http.get(`${BASE_URL}/events?topic=k6-loadtest&limit=10`);
      check(res, {
        "events available": (r) => r.status === 200,
        "returns array": (r) => Array.isArray(JSON.parse(r.body)),
      });
    });
  }
  
  sleep(0.05);  // 50ms between requests
}

// Teardown - dijalankan sekali di akhir
export function teardown(data) {
  console.log("\n=== Load Test Results ===");
  
  // Wait for queue to drain
  sleep(5);
  
  // Get final stats
  const statsRes = http.get(`${BASE_URL}/stats`);
  if (statsRes.status === 200) {
    const finalStats = JSON.parse(statsRes.body);
    
    console.log(`Initial unique_processed: ${data.initialStats.unique_processed}`);
    console.log(`Final unique_processed: ${finalStats.unique_processed}`);
    console.log(`Initial duplicate_dropped: ${data.initialStats.duplicate_dropped}`);
    console.log(`Final duplicate_dropped: ${finalStats.duplicate_dropped}`);
    
    const newUnique = finalStats.unique_processed - data.initialStats.unique_processed;
    const newDuplicates = finalStats.duplicate_dropped - data.initialStats.duplicate_dropped;
    const totalProcessed = newUnique + newDuplicates;
    const actualDupRate = totalProcessed > 0 ? (newDuplicates / totalProcessed * 100).toFixed(2) : 0;
    
    console.log(`\nNew events processed: ${totalProcessed}`);
    console.log(`New unique events: ${newUnique}`);
    console.log(`New duplicates dropped: ${newDuplicates}`);
    console.log(`Actual duplicate rate: ${actualDupRate}%`);
    console.log(`Expected duplicate rate: ${DUP_RATE * 100}%`);
    
    const elapsed = (Date.now() - data.startTime) / 1000;
    console.log(`\nThroughput: ${(totalProcessed / elapsed).toFixed(2)} events/sec`);
    console.log(`Total test duration: ${elapsed.toFixed(2)}s`);
  }
}

