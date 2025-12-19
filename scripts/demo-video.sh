#!/bin/bash
# =============================================================================
# Demo Script untuk Video UAS Sistem Terdistribusi
# Pub-Sub Log Aggregator dengan Idempotent Consumer
# =============================================================================
# 
# Cara menjalankan:
#   chmod +x scripts/demo-video.sh
#   ./scripts/demo-video.sh
#
# Script ini mencakup semua poin yang harus ditunjukkan dalam video demo:
# 1. Arsitektur dan build
# 2. Idempotency & Deduplication
# 3. Transaksi & Konkurensi
# 4. Persistensi (crash recovery)
# 5. Observability
# =============================================================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color
BOLD='\033[1m'

# Helper functions
print_header() {
    echo ""
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo -e "${BOLD}${BLUE}  $1${NC}"
    echo -e "${BOLD}${BLUE}================================================================${NC}"
    echo ""
}

print_step() {
    echo -e "${YELLOW}▶ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_info() {
    echo -e "${CYAN}ℹ $1${NC}"
}

wait_for_enter() {
    echo ""
    echo -e "${BOLD}Tekan ENTER untuk melanjutkan...${NC}"
    read
}

# API Base URL
BASE_URL="http://localhost:8080"

# =============================================================================
# BAGIAN 1: ARSITEKTUR DAN BUILD
# =============================================================================
demo_part1_architecture() {
    print_header "BAGIAN 1: ARSITEKTUR & BUILD IMAGE"
    
    print_step "1.1 Menampilkan struktur proyek"
    echo ""
    echo "Struktur direktori:"
    tree -L 2 --dirsfirst 2>/dev/null || find . -maxdepth 2 -type d | head -20
    
    wait_for_enter
    
    print_step "1.2 Menampilkan docker-compose.yml"
    echo ""
    echo "Komponen sistem:"
    echo "  - aggregator: FastAPI + Background Workers"
    echo "  - publisher: Event Generator"  
    echo "  - broker: Redis (Message Queue)"
    echo "  - storage: PostgreSQL (Dedup Store)"
    echo ""
    cat docker-compose.yml | head -50
    
    wait_for_enter
    
    print_step "1.3 Membersihkan environment sebelumnya"
    docker compose down -v 2>/dev/null || true
    print_success "Environment dibersihkan"
    
    wait_for_enter
    
    print_step "1.4 Build dan jalankan Docker Compose"
    echo ""
    docker compose up --build -d
    
    print_step "1.5 Menunggu services ready..."
    sleep 10
    
    # Check health
    for i in {1..30}; do
        if curl -s "$BASE_URL/health" | grep -q "healthy"; then
            print_success "Semua services ready!"
            break
        fi
        echo "Waiting... ($i/30)"
        sleep 2
    done
    
    print_step "1.6 Menampilkan container yang berjalan"
    docker compose ps
    
    wait_for_enter
}

# =============================================================================
# BAGIAN 2: IDEMPOTENCY & DEDUPLICATION
# =============================================================================
demo_part2_idempotency() {
    print_header "BAGIAN 2: IDEMPOTENCY & DEDUPLICATION"
    
    print_step "2.1 Cek stats awal (seharusnya kosong)"
    curl -s "$BASE_URL/stats" | python3 -m json.tool
    
    wait_for_enter
    
    print_step "2.2 Kirim event PERTAMA dengan event_id='demo-event-001'"
    echo ""
    curl -s -X POST "$BASE_URL/publish" \
        -H "Content-Type: application/json" \
        -d '{
            "events": [{
                "topic": "demo-topic",
                "event_id": "demo-event-001",
                "timestamp": "2024-01-01T12:00:00Z",
                "source": "demo-script",
                "payload": {"message": "First event"}
            }]
        }' | python3 -m json.tool
    
    sleep 2
    print_info "Event pertama dikirim"
    
    wait_for_enter
    
    print_step "2.3 Kirim event SAMA (duplikat) dengan event_id='demo-event-001'"
    echo ""
    curl -s -X POST "$BASE_URL/publish" \
        -H "Content-Type: application/json" \
        -d '{
            "events": [{
                "topic": "demo-topic",
                "event_id": "demo-event-001",
                "timestamp": "2024-01-01T12:00:00Z",
                "source": "demo-script",
                "payload": {"message": "First event (duplicate)"}
            }]
        }' | python3 -m json.tool
    
    sleep 2
    print_info "Event duplikat dikirim"
    
    wait_for_enter
    
    print_step "2.4 Kirim event SAMA LAGI (duplikat ke-2)"
    curl -s -X POST "$BASE_URL/publish" \
        -H "Content-Type: application/json" \
        -d '{
            "events": [{
                "topic": "demo-topic",
                "event_id": "demo-event-001",
                "timestamp": "2024-01-01T12:00:00Z",
                "source": "demo-script",
                "payload": {"message": "First event (duplicate 2)"}
            }]
        }' | python3 -m json.tool
    
    sleep 2
    
    wait_for_enter
    
    print_step "2.5 Cek stats - BUKTI DEDUPLICATION"
    echo ""
    echo -e "${BOLD}Expected: unique_processed=1, duplicate_dropped=2${NC}"
    echo ""
    curl -s "$BASE_URL/stats" | python3 -m json.tool
    
    print_success "Deduplication berhasil! Event hanya diproses SEKALI meski dikirim 3x"
    
    wait_for_enter
    
    print_step "2.6 Lihat events yang tersimpan"
    curl -s "$BASE_URL/events?topic=demo-topic" | python3 -m json.tool
    
    print_info "Hanya ada 1 event (idempotent)"
    
    wait_for_enter
}

# =============================================================================
# BAGIAN 3: TRANSAKSI & KONKURENSI
# =============================================================================
demo_part3_concurrency() {
    print_header "BAGIAN 3: TRANSAKSI & KONKURENSI"
    
    print_step "3.1 Reset metrics untuk demo konkurensi"
    curl -s -X POST "$BASE_URL/metrics/reset" | python3 -m json.tool
    
    wait_for_enter
    
    print_step "3.2 Kirim 10 event SAMA secara PARALEL (simulasi race condition)"
    echo ""
    echo "Mengirim 10 request paralel dengan event_id='concurrent-test'..."
    echo ""
    
    # Send 10 parallel requests with same event_id
    for i in {1..10}; do
        curl -s -X POST "$BASE_URL/publish" \
            -H "Content-Type: application/json" \
            -d "{
                \"events\": [{
                    \"topic\": \"concurrent-topic\",
                    \"event_id\": \"concurrent-test\",
                    \"timestamp\": \"2024-01-01T12:00:00Z\",
                    \"source\": \"concurrent-worker-$i\",
                    \"payload\": {\"worker\": $i}
                }]
            }" &
    done
    wait
    
    sleep 3
    print_success "10 request paralel selesai"
    
    wait_for_enter
    
    print_step "3.3 Cek stats - BUKTI KONKURENSI AMAN"
    echo ""
    echo -e "${BOLD}Expected: unique_processed=1, duplicate_dropped=9${NC}"
    echo "(10 request, tapi hanya 1 yang berhasil insert, 9 lainnya duplicate)"
    echo ""
    curl -s "$BASE_URL/stats" | python3 -m json.tool
    
    print_success "Tidak ada race condition! Unique constraint + atomic transaction bekerja"
    
    wait_for_enter
    
    print_step "3.4 Demo Atomic Counter Update"
    echo ""
    echo "Kirim batch 100 events dengan 30% duplikat..."
    
    # Generate batch
    events=""
    for i in {1..70}; do
        events="$events{\"topic\":\"batch-topic\",\"event_id\":\"batch-$i\",\"timestamp\":\"2024-01-01T12:00:00Z\",\"source\":\"batch\",\"payload\":{}},"
    done
    # Add duplicates
    for i in {1..30}; do
        dup_id=$((i % 20 + 1))
        events="$events{\"topic\":\"batch-topic\",\"event_id\":\"batch-$dup_id\",\"timestamp\":\"2024-01-01T12:00:00Z\",\"source\":\"batch\",\"payload\":{}},"
    done
    events="${events%,}"  # Remove trailing comma
    
    curl -s -X POST "$BASE_URL/publish?atomic=true" \
        -H "Content-Type: application/json" \
        -d "{\"events\": [$events]}" | python3 -m json.tool
    
    sleep 2
    
    wait_for_enter
    
    print_step "3.5 Cek final stats"
    curl -s "$BASE_URL/stats" | python3 -m json.tool
    
    print_info "Counter tetap konsisten meski ada banyak concurrent updates"
    
    wait_for_enter
}

# =============================================================================
# BAGIAN 4: PERSISTENSI (CRASH RECOVERY)
# =============================================================================
demo_part4_persistence() {
    print_header "BAGIAN 4: PERSISTENSI & CRASH RECOVERY"
    
    print_step "4.1 Cek stats SEBELUM crash"
    echo ""
    STATS_BEFORE=$(curl -s "$BASE_URL/stats")
    echo "$STATS_BEFORE" | python3 -m json.tool
    
    # Extract values
    UNIQUE_BEFORE=$(echo "$STATS_BEFORE" | python3 -c "import sys,json; print(json.load(sys.stdin)['unique_processed'])")
    echo ""
    echo -e "${BOLD}unique_processed sebelum crash: $UNIQUE_BEFORE${NC}"
    
    wait_for_enter
    
    print_step "4.2 Kirim event baru sebelum crash"
    curl -s -X POST "$BASE_URL/publish" \
        -H "Content-Type: application/json" \
        -d '{
            "events": [{
                "topic": "persist-topic",
                "event_id": "persist-event-before-crash",
                "timestamp": "2024-01-01T12:00:00Z",
                "source": "demo",
                "payload": {"saved": "before crash"}
            }]
        }' | python3 -m json.tool
    
    sleep 2
    
    wait_for_enter
    
    print_step "4.3 SIMULASI CRASH - Stop dan hapus container aggregator"
    echo ""
    docker compose stop aggregator
    docker compose rm -f aggregator
    
    print_info "Container aggregator dihapus!"
    echo ""
    docker compose ps
    
    wait_for_enter
    
    print_step "4.4 RESTART - Jalankan ulang aggregator"
    docker compose up -d aggregator
    
    echo "Menunggu aggregator ready..."
    sleep 10
    
    for i in {1..20}; do
        if curl -s "$BASE_URL/health" | grep -q "healthy"; then
            print_success "Aggregator ready!"
            break
        fi
        sleep 2
    done
    
    wait_for_enter
    
    print_step "4.5 Cek stats SETELAH restart"
    echo ""
    STATS_AFTER=$(curl -s "$BASE_URL/stats")
    echo "$STATS_AFTER" | python3 -m json.tool
    
    UNIQUE_AFTER=$(echo "$STATS_AFTER" | python3 -c "import sys,json; print(json.load(sys.stdin)['unique_processed'])")
    echo ""
    echo -e "${BOLD}unique_processed sebelum: $UNIQUE_BEFORE${NC}"
    echo -e "${BOLD}unique_processed sesudah: $UNIQUE_AFTER${NC}"
    
    print_success "Data PERSISTEN! Stats tetap ada setelah restart"
    
    wait_for_enter
    
    print_step "4.6 Coba kirim DUPLIKAT dari event sebelum crash"
    echo ""
    echo "Kirim event dengan event_id='persist-event-before-crash' lagi..."
    curl -s -X POST "$BASE_URL/publish" \
        -H "Content-Type: application/json" \
        -d '{
            "events": [{
                "topic": "persist-topic",
                "event_id": "persist-event-before-crash",
                "timestamp": "2024-01-01T12:00:00Z",
                "source": "demo",
                "payload": {"saved": "after crash - should be duplicate"}
            }]
        }' | python3 -m json.tool
    
    sleep 2
    
    echo ""
    curl -s "$BASE_URL/stats" | python3 -m json.tool
    
    print_success "Dedup tetap bekerja setelah restart! Event tidak diproses ulang"
    
    wait_for_enter
}

# =============================================================================
# BAGIAN 5: OBSERVABILITY
# =============================================================================
demo_part5_observability() {
    print_header "BAGIAN 5: OBSERVABILITY & MONITORING"
    
    print_step "5.1 Health Check Endpoint"
    echo ""
    curl -s "$BASE_URL/health" | python3 -m json.tool
    
    print_info "Health check untuk liveness/readiness probe"
    
    wait_for_enter
    
    print_step "5.2 Queue Stats"
    echo ""
    curl -s "$BASE_URL/queue/stats" | python3 -m json.tool
    
    print_info "Monitoring status queue dan workers"
    
    wait_for_enter
    
    print_step "5.3 Aggregator Stats (Metrics)"
    echo ""
    curl -s "$BASE_URL/stats" | python3 -m json.tool
    
    print_info "Metrics: received, unique_processed, duplicate_dropped, dedup_rate"
    
    wait_for_enter
    
    print_step "5.4 Container Logs"
    echo ""
    echo "Log aggregator (10 baris terakhir):"
    docker compose logs aggregator --tail 10
    
    wait_for_enter
    
    print_step "5.5 Cek Named Volumes (Persistensi)"
    echo ""
    docker volume ls | grep uas
    
    print_info "Volumes: pg_data, broker_data menyimpan data persisten"
    
    wait_for_enter
}

# =============================================================================
# BAGIAN 6: JARINGAN INTERNAL
# =============================================================================
demo_part6_network() {
    print_header "BAGIAN 6: KEAMANAN JARINGAN LOKAL"
    
    print_step "6.1 Lihat network Docker Compose"
    echo ""
    docker network ls | grep uas
    
    wait_for_enter
    
    print_step "6.2 Inspect network internal"
    echo ""
    docker network inspect uas_internal 2>/dev/null | head -30 || \
    docker network inspect $(docker network ls -q -f name=uas) | head -30
    
    print_info "Semua services dalam network internal, tidak ada akses eksternal"
    
    wait_for_enter
    
    print_step "6.3 Services yang di-expose"
    echo ""
    echo "Hanya port yang di-expose ke host:"
    echo "  - aggregator: 8080 (API)"
    echo "  - storage: 5432 (untuk demo/debug saja)"
    echo ""
    echo "Redis (broker) TIDAK di-expose ke luar - hanya internal"
    
    wait_for_enter
}

# =============================================================================
# MAIN DEMO
# =============================================================================
main() {
    clear
    print_header "DEMO UAS SISTEM TERDISTRIBUSI"
    echo "Pub-Sub Log Aggregator dengan Idempotent Consumer"
    echo ""
    echo "Demo ini mencakup:"
    echo "  1. Arsitektur & Build"
    echo "  2. Idempotency & Deduplication"
    echo "  3. Transaksi & Konkurensi"
    echo "  4. Persistensi (Crash Recovery)"
    echo "  5. Observability"
    echo "  6. Keamanan Jaringan"
    echo ""
    
    wait_for_enter
    
    demo_part1_architecture
    demo_part2_idempotency
    demo_part3_concurrency
    demo_part4_persistence
    demo_part5_observability
    demo_part6_network
    
    print_header "DEMO SELESAI"
    echo ""
    echo "Ringkasan fitur yang didemonstrasikan:"
    echo "  ✓ Multi-service architecture (Docker Compose)"
    echo "  ✓ Idempotent consumer dengan deduplication"
    echo "  ✓ Transaksi ACID dan konkurensi aman"
    echo "  ✓ Persistensi data dengan named volumes"
    echo "  ✓ Observability (health, stats, logs)"
    echo "  ✓ Jaringan internal yang aman"
    echo ""
    
    print_step "Cleanup (opsional)"
    echo "Untuk menghentikan sistem:"
    echo "  docker compose down"
    echo ""
    echo "Untuk menghapus data:"
    echo "  docker compose down -v"
    echo ""
}

# Run demo
main
