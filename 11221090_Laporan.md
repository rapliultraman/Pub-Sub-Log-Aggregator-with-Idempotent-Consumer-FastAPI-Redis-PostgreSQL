# LAPORAN UJIAN AKHIR SEMESTER
# SISTEM PARALEL TERDISTRIBUSI

## **Pub-Sub Log Aggregator dengan Idempotent Consumer**

---

**Nama:** Rafli Pratama Yuliandi
**NIM:** 11221090

**Dosen Pengampu:** Riska Kurniyanto Abdullah, S.T., M.Kom.

**Program Studi:** Informatika
**Fakultas:** FSTI  

---

## DAFTAR ISI

1. [Pendahuluan](#1-pendahuluan)
2. [Arsitektur Sistem](#2-arsitektur-sistem)
3. [Implementasi](#3-implementasi)
4. [Pengujian](#4-pengujian)
5. [Pembahasan Teori (T1-T10)](#5-pembahasan-teori)
6. [Kesimpulan](#6-kesimpulan)
7. [Referensi](#7-referensi)
8. [Lampiran](#8-lampiran)

---

## 1. PENDAHULUAN

### 1.1 Latar Belakang
Dalam sistem terdistribusi modern, pengelolaan log dan event dari berbagai sumber menjadi tantangan signifikan. Sistem publish-subscribe (Pub-Sub) menawarkan solusi dengan memisahkan produsen dan konsumer event, memungkinkan skalabilitas dan fleksibilitas yang tinggi (Coulouris et al., 2012).

### 1.2 Rumusan Masalah
1. Bagaimana merancang sistem log aggregator yang dapat menangani event dari berbagai sumber secara terdistribusi?
2. Bagaimana mengimplementasikan idempotent consumer untuk menjamin deduplikasi event?
3. Bagaimana menjaga konsistensi data dengan kontrol transaksi dan konkurensi yang tepat?

### 1.3 Tujuan
1. Mengimplementasikan sistem Pub-Sub Log Aggregator menggunakan FastAPI, Redis, dan PostgreSQL
2. Menerapkan idempotent consumer dengan mekanisme deduplikasi berbasis unique constraint
3. Memproses ≥20.000 event dengan ≥30% duplikasi sambil menjaga responsivitas sistem
4. Menyediakan 12-20 unit test untuk validasi fungsionalitas

### 1.4 Batasan Sistem
- Sistem berjalan dalam environment Docker Compose
- Tidak menggunakan message broker eksternal (Kafka/RabbitMQ), cukup Redis sebagai queue internal
- Fokus pada at-least-once delivery dengan idempotent consumer

---

## 2. ARSITEKTUR SISTEM

### 2.1 Diagram Arsitektur

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DOCKER COMPOSE NETWORK                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐     HTTP POST      ┌──────────────────────────────────┐    │
│  │   Client    │ ─────────────────> │         AGGREGATOR               │    │
│  │  (Publisher)│    /publish        │        (FastAPI)                 │    │
│  └─────────────┘                    │                                  │    │
│                                     │  ┌────────────┐  ┌────────────┐  │    │
│                                     │  │ API Layer  │  │  Workers   │  │    │
│                                     │  │            │  │ (N threads)│  │    │
│                                     │  └─────┬──────┘  └──────┬─────┘  │    │
│                                     └────────│────────────────│────────┘    │
│                                              │                │             │
│                          RPUSH ──────────────┘                │             │
│                                              │                │ BLPOP       │
│                                     ┌────────▼────────┐       │             │
│                                     │     REDIS       │<──────┘             │
│                                     │    (Broker)     │                     │
│                                     │  Message Queue  │                     │
│                                     └─────────────────┘                     │
│                                                                             │
│                                     ┌─────────────────┐                     │
│                                     │   POSTGRESQL    │                     │
│                                     │    (Storage)    │                     │
│                                     │                 │                     │
│                                     │ • events table  │                     │
│                                     │ • metrics table │                     │
│                                     │ • UNIQUE index  │                     │
│                                     └─────────────────┘                     │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Komponen Sistem

| Komponen | Teknologi | Port | Fungsi |
|----------|-----------|------|--------|
| **Aggregator** | FastAPI + Python 3.11 | 8080 | REST API, background workers, koordinasi |
| **Broker** | Redis 7-alpine | 6379 | Message queue dengan RPUSH/BLPOP pattern |
| **Storage** | PostgreSQL 16-alpine | 5432 | Persistent storage dengan ACID compliance |

### 2.3 Alur Data (Data Flow)

```
1. PUBLISH    : Client POST /publish → API menerima event batch
2. ENQUEUE    : API melakukan RPUSH ke Redis queue
3. DEQUEUE    : Worker melakukan BLPOP (blocking) dari Redis
4. DEDUPLICATE: Worker cek unique constraint (topic, event_id)
5. STORE      : INSERT ke PostgreSQL jika unique
6. METRICS    : UPDATE counter (received, processed, dropped)
```

### 2.4 Skema Database

```sql
-- Tabel Events (dengan unique constraint untuk deduplikasi)
CREATE TABLE events (
    id SERIAL PRIMARY KEY,
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    source VARCHAR(255) NOT NULL,
    payload JSONB NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    UNIQUE(topic, event_id)  -- Kunci deduplikasi
);

-- Index untuk query performa
CREATE INDEX idx_events_topic ON events(topic);
CREATE INDEX idx_events_timestamp ON events(timestamp DESC);

-- Tabel Metrics
CREATE TABLE metrics (
    id INTEGER PRIMARY KEY,
    received_count INTEGER DEFAULT 0,
    unique_processed_count INTEGER DEFAULT 0,
    duplicate_dropped_count INTEGER DEFAULT 0
);
```

---

## 3. IMPLEMENTASI

### 3.1 Struktur Project

```
UAS/
├── aggregator/               # Service utama
│   ├── Dockerfile
│   ├── requirements.txt
│   └── app/
│       ├── main.py          # FastAPI endpoints
│       ├── consumer.py      # Background worker & dedup logic
│       ├── models.py        # SQLAlchemy models
│       ├── schemas.py       # Pydantic schemas
│       ├── db.py            # Database connection
│       ├── queue.py         # Redis queue wrapper
│       └── config.py        # Configuration
├── publisher/               # Optional: event generator
├── tests/                   # Unit & integration tests
│   └── test_api.py          # 20 test cases
├── scripts/                 # Demo & helper scripts
├── docker-compose.yml       # Orchestration
└── REPORT.md               # Laporan ini
```

### 3.2 Implementasi Idempotent Consumer

```python
# Pseudocode: Idempotent event processing
def process_event(session, event):
    try:
        # Attempt INSERT
        new_event = Event(
            topic=event["topic"],
            event_id=event["event_id"],
            timestamp=event["timestamp"],
            source=event["source"],
            payload=event["payload"]
        )
        session.add(new_event)
        session.flush()  # Trigger constraint check
        
        # Success: increment unique_processed
        update_metric(session, "unique_processed", +1)
        session.commit()
        return True  # New event
        
    except IntegrityError:
        # Duplicate detected via UNIQUE constraint
        session.rollback()
        update_metric(session, "duplicate_dropped", +1)
        return False  # Duplicate
```

### 3.3 API Endpoints

| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| POST | `/publish` | Menerima batch events untuk diproses |
| GET | `/events?topic=X` | Mengambil events berdasarkan topic |
| GET | `/stats` | Statistik sistem (received, processed, dropped) |
| GET | `/health` | Health check untuk monitoring |
| GET | `/queue/stats` | Status antrian Redis |

### 3.4 Konfigurasi Docker Compose

```yaml
services:
  aggregator:
    build: ./aggregator
    environment:
      - DATABASE_URL=postgresql+psycopg2://user:pass@storage:5432/uasdb
      - REDIS_URL=redis://broker:6379/0
      - WORKER_COUNT=8
    depends_on:
      storage: { condition: service_healthy }
      broker: { condition: service_started }

  broker:
    image: redis:7-alpine
    volumes:
      - broker_data:/data  # Persistence

  storage:
    image: postgres:16-alpine
    volumes:
      - pg_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U user -d uasdb"]
```

---

## 4. PENGUJIAN

### 4.1 Daftar Test Cases (20 Tests)

| No | Kategori | Test Name | Deskripsi |
|----|----------|-----------|-----------|
| 1 | Basic API | `test_publish_accepts_event` | POST /publish menerima event valid |
| 2 | Basic API | `test_health_endpoint` | GET /health mengembalikan status healthy |
| 3 | Basic API | `test_queue_stats_endpoint` | GET /queue/stats mengembalikan info queue |
| 4 | Deduplication | `test_deduplication_counts` | Event duplikat terdeteksi dan dihitung |
| 5 | Deduplication | `test_duplicate_rate_under_load` | Multiple duplikat dalam batch dihandle |
| 6 | Deduplication | `test_cross_topic_dedup` | Event ID sama di topic berbeda tetap disimpan |
| 7 | Query | `test_events_endpoint_filters_by_topic` | GET /events filter by topic benar |
| 8 | Query | `test_stats_topics_listed` | GET /stats menampilkan semua topic |
| 9 | Query | `test_get_events_limit` | GET /events parameter limit berfungsi |
| 10 | Validation | `test_invalid_request_rejected` | Empty events list ditolak (422) |
| 11 | Validation | `test_schema_validation_timestamp` | Invalid timestamp ditolak |
| 12 | Validation | `test_schema_validation_missing_fields` | Missing required fields ditolak |
| 13 | Validation | `test_schema_validation_empty_topic` | Empty topic string ditolak |
| 14 | Batch | `test_batch_handling_multiple_events` | Batch events diproses benar |
| 15 | Batch | `test_atomic_batch_processing` | Atomic mode proses batch langsung |
| 16 | Persistence | `test_persistence_across_restart` | Data persisten setelah restart |
| 17 | Concurrency | `test_concurrent_workers_no_double_process` | Concurrent processing tidak double insert |
| 18 | Concurrency | `test_concurrent_publish_requests` | Concurrent POST requests dihandle |
| 19 | Concurrency | `test_stats_counter_consistency` | Stats counter konsisten under load |
| 20 | Stress | `test_stress_batch_events` | Sistem handle 100 events (30% duplikat) |

### 4.2 Menjalankan Tests

```bash
# Jalankan semua tests
pytest tests/ -v

# Dengan coverage report
pytest tests/ -v --cov=aggregator --cov-report=html
```

### 4.3 Hasil Pengujian

```
tests/test_api.py::test_publish_accepts_event PASSED
tests/test_api.py::test_health_endpoint PASSED
tests/test_api.py::test_queue_stats_endpoint PASSED
...
tests/test_api.py::test_stress_batch_events PASSED

==================== 20 passed in 7.87s ====================
```

### 4.4 Stress Test (20.000+ Events)

**Konfigurasi:**
- Total events: 20.000
- Duplikasi rate: 35%
- Batch size: 100 events/request
- Concurrent requests: 20

**Hasil:**
```
┌─────────────────────────────────────────────────┐
│ METRICS                                         │
├─────────────────────────────────────────────────┤
│ Total events dikirim    : 20000                │
│ Unique diproses         : 13000                │
│ Duplikat di-drop        : 7000                 │
│ Deduplication rate      : 35%                  │
│ Throughput              : ~150 events/s        │
└─────────────────────────────────────────────────┘

Validasi:
[OK] Total events >= 20.000 (20000)
[OK] Deduplication rate >= 30% (35%)
[OK] Sistem responsif
```

---

## 5. PEMBAHASAN TEORI

### T1 – Karakteristik Sistem Terdistribusi dan Trade-off Pub-Sub (Bab 1)

Menurut Van Steen dan Tanenbaum (2023), sistem terdistribusi adalah kumpulan elemen komputasi otonom yang tampak bagi pengguna sebagai satu sistem koheren. Karakteristik utama meliputi: **transparency** (lokasi, akses, migrasi), **openness** (interoperabilitas via standar), **scalability** (ukuran, geografis, administratif), dan **dependability** (availability, reliability, safety).

Pada Pub-Sub Log Aggregator ini, karakteristik yang diimplementasikan:
- **Distribution transparency**: Client tidak perlu tahu lokasi worker atau database
- **Scalability**: Worker dapat ditambah horizontal tanpa mengubah publisher
- **Loose coupling**: Publisher dan consumer tidak saling mengenal

**Trade-off desain yang dipilih:**

| Aspek | Pilihan | Konsekuensi |
|-------|---------|-------------|
| Delivery | At-least-once | Consumer harus idempotent |
| Ordering | Per-topic eventual | Tidak ada total order global |
| Consistency | Eventual | Ada delay antara publish dan query |
| Availability | High | Trade-off dengan strong consistency |

Sesuai CAP theorem, sistem ini memilih **AP (Availability + Partition tolerance)** dengan eventual consistency. Latency tambahan muncul akibat indirection melalui broker, namun memberikan decoupling yang diperlukan untuk skalabilitas (Van Steen & Tanenbaum, 2023, Bab 1.3).

### T2 – Kapan Memilih Arsitektur Publish-Subscribe (Bab 2)

Van Steen dan Tanenbaum (2023, Bab 2.4) menjelaskan bahwa arsitektur publish-subscribe termasuk dalam **event-based coordination** di mana komunikasi terjadi melalui propagasi event. Berbeda dengan client-server yang bersifat **request-reply synchronous**, pub-sub bersifat **asynchronous** dan **referentially decoupled**.

**Perbandingan arsitektur:**

| Kriteria | Pub-Sub | Client-Server |
|----------|---------|---------------|
| Coupling | Referentially & temporally decoupled | Tightly coupled |
| Komunikasi | Asynchronous, multicast | Synchronous, unicast |
| Skalabilitas | Horizontal (add subscribers) | Vertikal (scale server) |
| Failure impact | Isolated | Cascading |
| Use case | Event streaming, notifications | Request-response, transactions |

**Alasan teknis memilih Pub-Sub untuk Log Aggregator:**
1. **Multiple consumers**: Log perlu dikonsumsi oleh analytics, alerting, dan storage secara independen
2. **Temporal decoupling**: Publisher tidak perlu menunggu consumer memproses
3. **High throughput**: 20.000+ events memerlukan buffering yang disediakan broker
4. **Fault isolation**: Crash pada satu consumer tidak mempengaruhi lainnya

Pub-sub tidak cocok untuk operasi yang memerlukan response langsung seperti authentication atau query database. Untuk kasus tersebut, client-server lebih tepat karena latensi lebih rendah dan semantik request-reply lebih jelas (Van Steen & Tanenbaum, 2023).

### T3 – At-least-once vs Exactly-once; Peran Idempotent Consumer (Bab 3)

Van Steen dan Tanenbaum (2023, Bab 3.4) membahas **message delivery semantics** dalam konteks komunikasi antar proses. Terdapat tiga jaminan pengiriman:

| Semantics | Deskripsi | Implementasi | Kompleksitas |
|-----------|-----------|--------------|---------------|
| At-most-once | Pesan dikirim sekali, mungkin hilang | Fire-and-forget | Rendah |
| At-least-once | Pesan pasti sampai, mungkin duplikat | ACK + retry | Sedang |
| Exactly-once | Tepat sekali, tidak hilang/duplikat | 2PC atau dedup | Tinggi |

**Exactly-once** secara teoritis mustahil dalam jaringan asynchronous dengan kemungkinan failure (Van Steen & Tanenbaum, 2023). Solusi praktis adalah **at-least-once delivery** dengan **idempotent consumer**.

**Implementasi idempotent consumer pada sistem ini:**
```python
# Idempotent insert dengan unique constraint
try:
    session.add(Event(topic=t, event_id=eid, ...))  
    session.flush()  # Trigger constraint check
except IntegrityError:
    session.rollback()  # Duplicate detected, no side effect
```

**Peran idempotent consumer:**
1. Mengubah at-least-once menjadi **effectively exactly-once** pada state akhir
2. Memungkinkan **safe retry** tanpa takut data ganda
3. Worker dapat berjalan **paralel** tanpa distributed lock
4. Kompleksitas koordinasi digeser ke database yang sudah memiliki ACID

Dengan pola ini, sistem mencapai **reliable delivery** tanpa overhead two-phase commit (Van Steen & Tanenbaum, 2023, Bab 8.5).

### T4 – Skema Penamaan Topic dan Event_id (Bab 4)

Van Steen dan Tanenbaum (2023, Bab 5) menjelaskan bahwa **naming** dalam sistem terdistribusi berfungsi untuk identifikasi, lokasi, dan referensi entitas. Nama yang baik harus **unique**, **location-independent**, dan **collision-resistant**.

**Skema penamaan pada sistem ini:**

| Field | Format | Contoh | Fungsi |
|-------|--------|--------|--------|
| `topic` | kebab-case namespace | `user-events`, `order-logs` | Kategorisasi dan routing |
| `event_id` | UUID v4 atau source+timestamp | `550e8400-e29b-41d4-a716-446655440000` | Identifikasi unik per event |

**Karakteristik collision-resistant:**
- **UUID v4**: 122 bit random, probabilitas collision ~10^-37
- **Composite key**: `(topic, event_id)` memungkinkan event_id sama di topic berbeda

```sql
-- Unique constraint sebagai mekanisme dedup
CREATE TABLE events (
    topic VARCHAR(255) NOT NULL,
    event_id VARCHAR(255) NOT NULL,
    UNIQUE(topic, event_id)  -- Composite unique key
);
```

**Keuntungan skema ini:**
1. **Decentralized generation**: Publisher generate event_id sendiri tanpa koordinasi
2. **Idempotency key**: Kombinasi topic+event_id menjadi natural deduplication key
3. **Scalable**: Tidak ada single point untuk ID generation
4. **Human-readable topic**: Memudahkan debugging dan filtering

Skema ini mengikuti prinsip **flat naming** yang tidak mengandung informasi lokasi, cocok untuk sistem terdistribusi yang memerlukan location transparency (Van Steen & Tanenbaum, 2023, Bab 5.1).

### T5 – Ordering Praktis: Timestamp dan Monotonic Counter (Bab 5)

Van Steen dan Tanenbaum (2023, Bab 6.2) membahas **logical clocks** dan **physical clocks** untuk ordering events dalam sistem terdistribusi. **Total ordering** memerlukan koordinasi global yang mahal, sedangkan **partial ordering** lebih praktis untuk sistem high-throughput.

**Pendekatan ordering pada sistem ini:**

| Komponen | Jenis Clock | Fungsi |
|----------|-------------|--------|
| `timestamp` | Physical (ISO8601) | Event time dari publisher |
| `processed_at` | Physical (server time) | Audit kapan diproses |
| `id` (SERIAL) | Logical (monotonic) | Urutan insert di database |

**Batasan yang diterima:**
1. **No global total order**: Worker paralel memproses tanpa koordinasi
2. **Arrival order ≠ Event order**: Network delay dapat membalik urutan
3. **Clock skew**: Timestamp antar publisher mungkin tidak sinkron

**Dampak pada sistem:**
```
Publisher A: event(t=10:00:01) → arrives at 10:00:05
Publisher B: event(t=10:00:02) → arrives at 10:00:03
Storage order: B, A (berdasarkan arrival)
Event order: A, B (berdasarkan timestamp)
```

**Mitigasi:**
- Query dengan `ORDER BY timestamp` untuk event-time ordering
- Deduplication **tidak bergantung pada order** (berbasis unique key)
- Untuk kebutuhan strict ordering, dapat menggunakan **Lamport timestamps** atau **vector clocks**, namun menambah overhead (Van Steen & Tanenbaum, 2023, Bab 6.2).

Trade-off ini menyeimbangkan **throughput** (parallel processing) dengan **consistency** yang cukup untuk log aggregation.

### T6 – Failure Modes dan Mitigasi (Bab 6)

Van Steen dan Tanenbaum (2023, Bab 8.1-8.3) mengkategorikan failure menjadi: **crash failure**, **omission failure**, **timing failure**, **response failure**, dan **arbitrary failure**. Sistem yang **fault tolerant** harus mampu mendeteksi dan recover dari failures.

**Analisis failure modes pada sistem ini:**

| Failure Mode | Kategori | Dampak | Mitigasi |
|--------------|----------|--------|----------|
| Worker crash | Crash failure | Event tertunda | Docker restart policy (`unless-stopped`) |
| Redis crash | Crash failure | Queue hilang | Volume persistence + AOF |
| PostgreSQL crash | Crash failure | Data loss | ACID + volume persistence |
| Network timeout | Omission failure | Request gagal | Retry dengan exponential backoff |
| Duplicate delivery | Response failure | Data ganda | Unique constraint + idempotent insert |
| Message loss | Omission failure | Event hilang | At-least-once + acknowledgment |

**Strategi fault tolerance yang diimplementasikan:**

1. **Redundancy**: Data disimpan di PostgreSQL dengan volume persistent
2. **Recovery**: Docker Compose auto-restart failed containers
3. **Retry dengan backoff**: Publisher dapat retry tanpa menyebabkan duplicate
4. **Durable dedup store**: PostgreSQL menjamin dedup state survive restart

```python
# Exponential backoff pattern
for attempt in range(max_retries):
    try:
        response = requests.post(url, json=data, timeout=5)
        break
    except Timeout:
        sleep(2 ** attempt)  # 1s, 2s, 4s, 8s...
```

**Crash recovery scenario:**
```
1. Worker crash saat memproses event
2. Event tetap di Redis queue (belum di-ACK)
3. Worker restart, BLPOP mengambil event kembali
4. Insert ke DB → duplicate? → constraint reject → safe
```

Monitoring via `/stats` endpoint memantau `duplicate_dropped` untuk mendeteksi anomali delivery (Van Steen & Tanenbaum, 2023, Bab 8.5).

### T7 – Eventual Consistency pada Aggregator (Bab 7)

Van Steen dan Tanenbaum (2023, Bab 7.2) menjelaskan **consistency models** mulai dari strong consistency hingga eventual consistency. **Eventual consistency** menjamin bahwa jika tidak ada update baru, semua replika akan *eventually* konvergen ke state yang sama.

**Model konsistensi pada sistem ini:**

| Operasi | Consistency Level | Penjelasan |
|---------|-------------------|------------|
| POST /publish | Eventual | Event di-queue, belum visible |
| GET /events | Read-your-writes* | *Jika melalui queue, ada delay |
| GET /stats | Monotonic reads | Counter tidak pernah mundur |

**Timeline eventual consistency:**
```
t0: POST /publish → {"accepted": 1, "queued": 1}
    └── Event masuk Redis queue
t1: Worker BLPOP dari Redis (delay: ~10-100ms)
t2: INSERT ke PostgreSQL 
t3: GET /events → event visible
    └── Consistency window: t0 → t3
```

**Consistency window** adalah periode antara write diterima hingga read mengembalikan data terbaru. Pada sistem ini, window berkisar 10-500ms tergantung queue depth.

**Peran idempotency dalam eventual consistency:**
1. **Convergence guarantee**: Retry tidak mengubah final state
2. **Conflict resolution**: Duplicate otomatis di-resolve oleh constraint
3. **No coordination needed**: Worker independen tetap konvergen

```python
# Idempotent insert menjamin konvergensi
# Insert 1x atau 100x → hasil sama: 1 row
INSERT INTO events (topic, event_id, ...) 
VALUES ('log', 'evt-001', ...)
ON CONFLICT (topic, event_id) DO NOTHING;
```

Sistem memilih eventual consistency karena log aggregation tidak memerlukan strong consistency. Toleransi terhadap staleness beberapa ratus milidetik dapat diterima untuk use case monitoring dan analytics (Van Steen & Tanenbaum, 2023, Bab 7.2).

### T8 – Desain Transaksi: ACID dan Isolation Level (Bab 8) 

Van Steen dan Tanenbaum (2023, Bab 8.5-8.6) membahas **distributed transactions** dan properti **ACID** yang menjamin correctness. Pada sistem ini, transaksi database menjadi kunci untuk **idempotent processing** dan **consistent metrics**.

**Properti ACID pada implementasi:**

| Property | Implementasi | Contoh pada Sistem |
|----------|--------------|--------------------|
| **Atomic** | Single transaction | Insert event + update metrics |
| **Consistent** | Unique constraint | `UNIQUE(topic, event_id)` selalu terjaga |
| **Isolated** | READ COMMITTED | Concurrent workers tidak lihat uncommitted data |
| **Durable** | WAL + fsync | Data survive PostgreSQL restart |

**Pola transaksi idempotent:**

```python
# consumer.py - ACID transaction untuk event processing
def process_event(session, event_data):
    try:
        # === BEGIN TRANSACTION ===
        # 1. INSERT event (Atomic operation)
        new_event = Event(
            topic=event_data["topic"],
            event_id=event_data["event_id"],
            timestamp=event_data["timestamp"],
            payload=event_data["payload"]
        )
        session.add(new_event)
        session.flush()  # Trigger constraint check
        
        # 2. UPDATE metrics (dalam transaksi yang sama)
        session.execute(
            update(Metrics)
            .where(Metrics.id == 1)
            .values(unique_processed_count=Metrics.unique_processed_count + 1)
        )
        session.commit()  # === COMMIT ===
        return True
        
    except IntegrityError:
        # Duplicate detected - ROLLBACK untuk undo partial changes
        session.rollback()  # === ROLLBACK ===
        # Update duplicate counter (transaksi terpisah)
        increment_duplicate_counter(session)
        return False
```

**Isolation level READ COMMITTED dipilih karena:**
1. Mencegah **dirty read**: Worker tidak melihat data uncommitted
2. Memungkinkan **higher concurrency** dibanding SERIALIZABLE
3. **Lost update dicegah** oleh atomic increment (`count = count + 1`)

**Strategi menghindari lost update:**
```sql
-- SALAH: Read-modify-write (race condition)
SELECT count FROM metrics;  -- Worker A: 100
SELECT count FROM metrics;  -- Worker B: 100  
UPDATE metrics SET count = 101;  -- Worker A
UPDATE metrics SET count = 101;  -- Worker B (lost update!)

-- BENAR: Atomic increment
UPDATE metrics SET count = count + 1;  -- Atomic, no lost update
```

Dengan desain ini, sistem menjamin **exactly-once semantics pada state** meskipun delivery at-least-once (Van Steen & Tanenbaum, 2023, Bab 8.6).

### T9 – Kontrol Konkurensi: Locking dan Idempotent Write Pattern (Bab 9) 

Van Steen dan Tanenbaum (2023, Bab 6.3) membahas **mutual exclusion** dan mekanisme **concurrency control**. Pada sistem database, terdapat dua pendekatan utama: **pessimistic (locking)** dan **optimistic (validation)**.

**Perbandingan pendekatan:**

| Aspek | Pessimistic (Locking) | Optimistic (Constraint-based) |
|-------|----------------------|-------------------------------|
| Mekanisme | Acquire lock sebelum akses | Validasi saat commit |
| Overhead | Lock acquisition & release | Constraint check |
| Deadlock | Mungkin terjadi | Tidak ada |
| Throughput | Lower (sequential) | Higher (parallel) |
| Conflict rate | Cocok untuk high conflict | Cocok untuk low conflict |

**Sistem ini menggunakan Optimistic Concurrency dengan mekanisme:**

**1. Unique Constraint sebagai Distributed Lock:**
```sql
-- Constraint bertindak sebagai "lock" pada kombinasi (topic, event_id)
CREATE UNIQUE INDEX idx_event_dedup ON events(topic, event_id);

-- Concurrent inserts:
-- Worker A: INSERT (topic='log', event_id='001') → SUCCESS
-- Worker B: INSERT (topic='log', event_id='001') → IntegrityError (blocked)
```

**2. Atomic Increment untuk Metrics:**
```python
# Idempotent write pattern - no explicit locking needed
session.execute(
    update(Metrics)
    .values(unique_processed_count=Metrics.unique_processed_count + 1)
)
# PostgreSQL handles row-level locking internally
```

**3. Connection Pool per Worker:**
```python
# Setiap worker thread memiliki session sendiri
# db.py
engine = create_engine(DATABASE_URL, pool_size=10, max_overflow=20)
SessionLocal = sessionmaker(bind=engine)

# consumer.py - setiap worker mendapat session independen
def worker_thread():
    session = SessionLocal()  # Isolated session
    while True:
        event = queue.dequeue()
        process_event(session, event)  # No shared state
```

**Skenario concurrent processing:**
```
Time    Worker-1              Worker-2              Database State
────────────────────────────────────────────────────────────────────
t1      BLPOP evt-001         BLPOP evt-002         queue: [evt-001, evt-002]
t2      BEGIN                 BEGIN                 
t3      INSERT evt-001        INSERT evt-002        
t4      COMMIT ✓              COMMIT ✓              events: [evt-001, evt-002]

# Jika duplicate:
t1      BLPOP evt-001         BLPOP evt-001(retry)  
t2      BEGIN                 BEGIN                 
t3      INSERT evt-001 ✓      INSERT evt-001 ✗      IntegrityError
t4      COMMIT                ROLLBACK              events: [evt-001] (1 row)
```

**Keuntungan idempotent write pattern:**
1. **No distributed locks**: Tidak perlu Redis lock atau Zookeeper
2. **Horizontal scalable**: Tambah worker tanpa koordinasi
3. **Deadlock-free**: Constraint-based, bukan lock-based
4. **Self-healing**: Retry otomatis safe karena idempotent

Pendekatan ini mengikuti prinsip "make the operation idempotent rather than preventing duplicates" yang lebih scalable untuk sistem terdistribusi (Van Steen & Tanenbaum, 2023, Bab 6.3).

### T10 – Orkestrasi, Keamanan, Persistensi, dan Observability (Bab 10-13)

Van Steen dan Tanenbaum (2023) membahas aspek operasional sistem terdistribusi di beberapa bab: **Naming & Discovery** (Bab 5), **Coordination** (Bab 6), **Fault Tolerance** (Bab 8), dan **Security** (Bab 9).

#### A. Orkestrasi dengan Docker Compose

```yaml
# docker-compose.yml - Service orchestration
services:
  aggregator:
    build: ./aggregator
    ports: ["8080:8080"]
    depends_on:
      storage: { condition: service_healthy }
      broker: { condition: service_started }
    restart: unless-stopped
    
  broker:       # Redis - Message Queue
    image: redis:7-alpine
    volumes: [broker_data:/data]
    
  storage:      # PostgreSQL - Persistent Storage  
    image: postgres:16-alpine
    volumes: [pg_data:/var/lib/postgresql/data]
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "user"]

networks:
  default:      # Internal network (isolated)
  
volumes:
  pg_data:      # Named volume - survives container removal
  broker_data:  # Redis AOF persistence
```

**Koordinasi service startup:**
- `depends_on` dengan `condition: service_healthy` memastikan PostgreSQL ready sebelum aggregator start
- Health check mencegah connection errors saat startup

#### B. Keamanan Jaringan Lokal

Menurut Van Steen dan Tanenbaum (2023, Bab 9), keamanan mencakup **confidentiality**, **integrity**, dan **availability**.

| Aspek | Implementasi |
|-------|-------------|
| Network isolation | Docker internal network (tidak exposed ke host) |
| Port exposure | Hanya 8080 (API) yang public |
| Credentials | Environment variables, bukan hardcoded |
| Container security | Non-root user, read-only filesystem |

#### C. Persistensi dengan Named Volumes

```bash
# Data survives container removal
docker compose down     # Containers removed
docker compose up -d    # Data masih ada!

# Data HILANG jika volume dihapus
docker compose down -v  # Volumes removed = data loss
```

**Persistence layers:**
1. **PostgreSQL**: WAL (Write-Ahead Log) + fsync untuk durability
2. **Redis**: AOF (Append-Only File) untuk queue persistence
3. **Docker volumes**: Named volumes independent dari container lifecycle

#### D. Observability

| Endpoint | Fungsi | Metrics |
|----------|--------|--------|
| `GET /health` | Liveness & readiness probe | DB status, queue status |
| `GET /stats` | Business metrics | received, processed, dropped, topics |
| `GET /queue/stats` | Queue monitoring | pending count, processing rate |

```json
// GET /stats response
{
  "received": 20000,
  "unique_processed": 13067,
  "duplicate_dropped": 6933,
  "dedup_rate_percent": 34.67,
  "topics": ["user-events", "order-logs"],
  "uptime_seconds": 3600.5
}
```

**Monitoring strategy:**
- **Health checks**: Kubernetes/Docker dapat restart unhealthy containers
- **Metrics endpoint**: Dapat di-scrape oleh Prometheus
- **Structured logging**: JSON format untuk aggregasi di ELK/Loki

Kombinasi orkestrasi, keamanan, persistensi, dan observability ini memenuhi prinsip **operational excellence** untuk production-ready distributed systems (Van Steen & Tanenbaum, 2023).

---

## 6. KESIMPULAN

### 6.1 Capaian
1. Berhasil mengimplementasikan Pub-Sub Log Aggregator dengan FastAPI, Redis, dan PostgreSQL
2. Idempotent consumer terbukti efektif dengan unique constraint
3. Mampu memproses 20.000+ events dengan 35% deduplication rate
4. 20 test cases berhasil dijalankan
5. Sistem responsif dengan throughput ~150 events/detik

### 6.2 Pembelajaran
- At-least-once delivery dengan idempotent consumer adalah pilihan pragmatis
- Unique constraint di database lebih reliable daripada dedup di aplikasi
- Docker Compose mempermudah orkestrasi multi-service

### 6.3 Pengembangan Lanjutan
- Implementasi horizontal scaling dengan multiple aggregator instances
- Integrasi dengan message broker production (Kafka)
- Distributed tracing untuk observability lebih baik

---

## 7. REFERENSI

Van Steen, M., & Tanenbaum, A. S. (2023). *Distributed Systems* (4th ed., Version 01). Maarten van Steen. https://www.distributed-systems.net/

Kleppmann, M. (2017). *Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems*. O'Reilly Media.

Bernstein, P. A., & Newcomer, E. (2009). *Principles of Transaction Processing* (2nd ed.). Morgan Kaufmann.

---

## 8. LAMPIRAN

### Lampiran A: Cara Menjalankan Sistem

```bash
# 1. Clone repository
git clone <repo-url>
cd UAS

# 2. Jalankan dengan Docker Compose
docker compose up -d --build

# 3. Verifikasi sistem berjalan
curl http://localhost:8080/health

# 4. Jalankan demo
./scripts/demo.sh

# 5. Jalankan tests
pytest tests/ -v
```

### Lampiran B: API Documentation

**POST /publish**
```json
// Request
{
  "events": [
    {
      "topic": "user-events",
      "event_id": "evt-001",
      "timestamp": "2024-12-19T10:00:00Z",
      "source": "user-service",
      "payload": {"action": "login"}
    }
  ]
}

// Response
{
  "accepted": 1,
  "queued": 1
}
```

**GET /stats**
```json
{
  "received": 20000,
  "unique_processed": 13000,
  "duplicate_dropped": 7000,
  "topics": ["user-events", "order-events"],
  "uptime_seconds": 3600.5,
  "dedup_rate_percent": 35.0
}
```

### Lampiran C: Screenshot Hasil Demo

![alt text](<result_photo/Screen Shot 2025-12-19 at 10.07.30 PM.png>)

![alt text](<result_photo/Screen Shot 2025-12-19 at 10.07.39 PM.png>)

![alt text](<result_photo/Screen Shot 2025-12-19 at 10.07.43 PM.png>)

![alt text](<result_photo/Screen Shot 2025-12-19 at 10.07.46 PM.png>)

![alt text](<result_photo/Screen Shot 2025-12-19 at 10.07.53 PM.png>)

![alt text](<result_photo/Screen Shot 2025-12-19 at 10.07.58 PM.png>)

![alt text](<result_photo/Screen Shot 2025-12-19 at 10.08.06 PM.png>)


