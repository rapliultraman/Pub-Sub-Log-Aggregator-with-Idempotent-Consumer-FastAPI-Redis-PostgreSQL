#!/bin/bash
# Script helper untuk demo sistem Pub-Sub Log Aggregator

set -e

BASE_URL="http://localhost:8080"
COLOR_GREEN='\033[0;32m'
COLOR_BLUE='\033[0;34m'
COLOR_YELLOW='\033[1;33m'
COLOR_RESET='\033[0m'

print_header() {
    echo -e "\n${COLOR_BLUE}========================================${COLOR_RESET}"
    echo -e "${COLOR_BLUE}$1${COLOR_RESET}"
    echo -e "${COLOR_BLUE}========================================${COLOR_RESET}\n"
}

print_success() {
    echo -e "${COLOR_GREEN}✓ $1${COLOR_RESET}"
}

print_info() {
    echo -e "${COLOR_YELLOW}ℹ $1${COLOR_RESET}"
}

wait_for_service() {
    print_info "Menunggu service siap..."
    for i in {1..30}; do
        if curl -s "$BASE_URL/stats" > /dev/null 2>&1; then
            print_success "Service siap!"
            return 0
        fi
        sleep 1
    done
    echo "Error: Service tidak merespons"
    exit 1
}

demo_1_single_event() {
    print_header "Demo 1: Publish Event Tunggal"
    
    print_info "Mengirim event tunggal..."
    RESPONSE=$(curl -s -X POST "$BASE_URL/publish" \
        -H "Content-Type: application/json" \
        -d '{
            "events": [{
                "topic": "demo-topic",
                "event_id": "event-001",
                "timestamp": "2024-12-12T10:00:00Z",
                "source": "demo-client",
                "payload": {"message": "Hello World"}
            }]
        }')
    
    echo "$RESPONSE" | jq '.'
    print_success "Event diterima"
    
    print_info "Menunggu processing (2 detik)..."
    sleep 2
    
    print_info "Mengambil statistik:"
    curl -s "$BASE_URL/stats" | jq '.'
    
    print_info "Mengambil event yang sudah diproses:"
    curl -s "$BASE_URL/events?topic=demo-topic" | jq '.'
}

demo_2_deduplication() {
    print_header "Demo 2: Deduplication (Idempotency)"
    
    print_info "Mengirim event duplikat 3 kali..."
    for i in {1..3}; do
        curl -s -X POST "$BASE_URL/publish" \
            -H "Content-Type: application/json" \
            -d '{
                "events": [{
                    "topic": "demo-topic",
                    "event_id": "duplicate-test-001",
                    "timestamp": "2024-12-12T10:00:00Z",
                    "source": "demo-client",
                    "payload": {"test": "duplicate"}
                }]
            }' > /dev/null
        echo "  Duplikat #$i dikirim"
    done
    
    print_info "Menunggu processing (3 detik)..."
    sleep 3
    
    print_info "Statistik setelah deduplication:"
    STATS=$(curl -s "$BASE_URL/stats")
    echo "$STATS" | jq '.'
    
    RECEIVED=$(echo "$STATS" | jq -r '.received')
    UNIQUE=$(echo "$STATS" | jq -r '.unique_processed')
    DROPPED=$(echo "$STATS" | jq -r '.duplicate_dropped')
    
    print_info "Verifikasi:"
    echo "  - Received: $RECEIVED (harus 3)"
    echo "  - Unique Processed: $UNIQUE (harus 1)"
    echo "  - Duplicate Dropped: $DROPPED (harus 2)"
    
    print_info "Jumlah event di database:"
    EVENTS=$(curl -s "$BASE_URL/events?topic=demo-topic")
    COUNT=$(echo "$EVENTS" | jq 'length')
    echo "  Total events: $COUNT (harus 1)"
}

demo_3_batch() {
    print_header "Demo 3: Batch Processing"
    
    print_info "Mengirim batch dengan 4 event (1 duplikat)..."
    RESPONSE=$(curl -s -X POST "$BASE_URL/publish" \
        -H "Content-Type: application/json" \
        -d '{
            "events": [
                {"topic": "batch-topic", "event_id": "batch-001", "timestamp": "2024-12-12T10:00:00Z", "source": "batch", "payload": {}},
                {"topic": "batch-topic", "event_id": "batch-002", "timestamp": "2024-12-12T10:00:01Z", "source": "batch", "payload": {}},
                {"topic": "batch-topic", "event_id": "batch-003", "timestamp": "2024-12-12T10:00:02Z", "source": "batch", "payload": {}},
                {"topic": "batch-topic", "event_id": "batch-001", "timestamp": "2024-12-12T10:00:03Z", "source": "batch", "payload": {}}
            ]
        }')
    
    echo "$RESPONSE" | jq '.'
    print_success "Batch diterima"
    
    print_info "Menunggu processing (3 detik)..."
    sleep 3
    
    print_info "Statistik:"
    curl -s "$BASE_URL/stats" | jq '.'
    
    print_info "Event batch yang diproses:"
    curl -s "$BASE_URL/events?topic=batch-topic" | jq '.'
}

demo_4_queue_stats() {
    print_header "Demo 4: Queue Statistics"
    
    print_info "Status queue:"
    curl -s "$BASE_URL/queue/stats" | jq '.'
    
    print_info "Mengirim 10 event sekaligus untuk melihat queue..."
    for i in {1..10}; do
        curl -s -X POST "$BASE_URL/publish" \
            -H "Content-Type: application/json" \
            -d "{
                \"events\": [{
                    \"topic\": \"queue-test\",
                    \"event_id\": \"queue-$i\",
                    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
                    \"source\": \"queue-demo\",
                    \"payload\": {}
                }]
            }" > /dev/null &
    done
    wait
    
    print_info "Queue size setelah pengiriman:"
    curl -s "$BASE_URL/queue/stats" | jq '.queue_size'
    
    print_info "Menunggu processing (3 detik)..."
    sleep 3
    
    print_info "Queue size setelah processing:"
    curl -s "$BASE_URL/queue/stats" | jq '.queue_size'
}

demo_5_load_test() {
    print_header "Demo 5: Load Test (50 events)"
    
    print_info "Mengirim 50 event dengan 30% duplikasi..."
    TOTAL=50
    DUP_COUNT=15
    
    # Generate unique events
    for i in $(seq 1 $((TOTAL - DUP_COUNT))); do
        curl -s -X POST "$BASE_URL/publish" \
            -H "Content-Type: application/json" \
            -d "{
                \"events\": [{
                    \"topic\": \"load-test\",
                    \"event_id\": \"load-unique-$i\",
                    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
                    \"source\": \"load-demo\",
                    \"payload\": {\"iteration\": $i}
                }]
            }" > /dev/null &
    done
    
    # Generate duplicates
    for i in $(seq 1 $DUP_COUNT); do
        curl -s -X POST "$BASE_URL/publish" \
            -H "Content-Type: application/json" \
            -d "{
                \"events\": [{
                    \"topic\": \"load-test\",
                    \"event_id\": \"load-unique-$((i % 5 + 1))\",
                    \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",
                    \"source\": \"load-demo\",
                    \"payload\": {\"duplicate\": true}
                }]
            }" > /dev/null &
    done
    
    wait
    
    print_info "Menunggu processing (5 detik)..."
    sleep 5
    
    print_info "Hasil load test:"
    STATS=$(curl -s "$BASE_URL/stats")
    echo "$STATS" | jq '.'
    
    RECEIVED=$(echo "$STATS" | jq -r '.received')
    UNIQUE=$(echo "$STATS" | jq -r '.unique_processed')
    DROPPED=$(echo "$STATS" | jq -r '.duplicate_dropped')
    
    print_info "Analisis:"
    echo "  - Total diterima: $RECEIVED"
    echo "  - Unique diproses: $UNIQUE"
    echo "  - Duplikat di-drop: $DROPPED"
    echo "  - Duplikat rate: $(echo "scale=2; $DROPPED * 100 / $RECEIVED" | bc)%"
}

demo_6_stress_test() {
    print_header "Demo 6: Stress Test (20.000+ events, 30% duplikasi)"
    
    TOTAL_EVENTS=20000
    DUPLICATE_PERCENT=35
    UNIQUE_EVENTS=$((TOTAL_EVENTS * (100 - DUPLICATE_PERCENT) / 100))  # 13000 unique
    DUPLICATE_EVENTS=$((TOTAL_EVENTS - UNIQUE_EVENTS))                  # 7000 duplicates
    BATCH_SIZE=100
    CONCURRENT_REQUESTS=20
    
    print_info "Konfigurasi:"
    echo "  - Total events: $TOTAL_EVENTS"
    echo "  - Unique events: $UNIQUE_EVENTS"
    echo "  - Duplicate events: $DUPLICATE_EVENTS (~${DUPLICATE_PERCENT}%)"
    echo "  - Batch size: $BATCH_SIZE events per request"
    echo "  - Concurrent requests: $CONCURRENT_REQUESTS"
    
    # Get initial stats
    INITIAL_STATS=$(curl -s "$BASE_URL/stats")
    INITIAL_RECEIVED=$(echo "$INITIAL_STATS" | jq -r '.received')
    INITIAL_UNIQUE=$(echo "$INITIAL_STATS" | jq -r '.unique_processed')
    INITIAL_DROPPED=$(echo "$INITIAL_STATS" | jq -r '.duplicate_dropped')
    
    print_info "Statistik awal: received=$INITIAL_RECEIVED, unique=$INITIAL_UNIQUE, dropped=$INITIAL_DROPPED"
    print_info "Memulai stress test..."
    
    START_TIME=$(python3 -c 'import time; print(time.time())')
    
    # Send unique events first using atomic mode so they're immediately in DB
    UNIQUE_BATCHES=$((UNIQUE_EVENTS / BATCH_SIZE))
    print_info "Mengirim $UNIQUE_EVENTS unique events ($UNIQUE_BATCHES batches) dengan atomic mode..."
    
    for batch in $(seq 0 $((UNIQUE_BATCHES - 1))); do
        # Build batch JSON
        events="["
        for j in $(seq 1 $BATCH_SIZE); do
            event_num=$((batch * BATCH_SIZE + j))
            if [ $j -gt 1 ]; then
                events+=","
            fi
            events+="{\"topic\":\"stress-test\",\"event_id\":\"stress-event-$event_num\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"source\":\"stress-demo\",\"payload\":{\"batch\":$batch,\"seq\":$j}}"
        done
        events+="]"
        
        # Send with atomic mode for immediate DB insert
        curl -s -X POST "$BASE_URL/publish?atomic=true" \
            -H "Content-Type: application/json" \
            -d "{\"events\":$events}" > /dev/null &
        
        # Control concurrency
        if [ $(( (batch + 1) % CONCURRENT_REQUESTS )) -eq 0 ]; then
            wait
            # Show progress every 40 batches
            if [ $(( (batch + 1) % 40 )) -eq 0 ]; then
                PROGRESS=$(( (batch + 1) * BATCH_SIZE ))
                echo "    Progress: $PROGRESS / $UNIQUE_EVENTS unique events sent"
            fi
        fi
    done
    wait
    
    print_success "Unique events terkirim dan diproses!"
    
    # Send duplicate events (queued mode, will be detected as duplicates)
    DUPLICATE_BATCHES=$((DUPLICATE_EVENTS / BATCH_SIZE))
    print_info "Mengirim $DUPLICATE_EVENTS duplicate events ($DUPLICATE_BATCHES batches)..."
    
    for batch in $(seq 0 $((DUPLICATE_BATCHES - 1))); do
        # Build batch JSON with duplicate IDs from unique events
        events="["
        for j in $(seq 1 $BATCH_SIZE); do
            event_num=$((batch * BATCH_SIZE + j))
            # Reference existing unique event IDs
            dup_id=$((event_num % UNIQUE_EVENTS + 1))
            if [ $j -gt 1 ]; then
                events+=","
            fi
            events+="{\"topic\":\"stress-test\",\"event_id\":\"stress-event-$dup_id\",\"timestamp\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"source\":\"stress-demo-dup\",\"payload\":{\"batch\":$batch,\"dup\":true}}"
        done
        events+="]"
        
        curl -s -X POST "$BASE_URL/publish" \
            -H "Content-Type: application/json" \
            -d "{\"events\":$events}" > /dev/null &
        
        # Control concurrency
        if [ $(( (batch + 1) % CONCURRENT_REQUESTS )) -eq 0 ]; then
            wait
            if [ $(( (batch + 1) % 40 )) -eq 0 ]; then
                PROGRESS=$(( (batch + 1) * BATCH_SIZE ))
                echo "    Progress: $PROGRESS / $DUPLICATE_EVENTS duplicate events sent"
            fi
        fi
    done
    wait
    
    SEND_END_TIME=$(python3 -c 'import time; print(time.time())')
    SEND_DURATION=$(python3 -c "print(round($SEND_END_TIME - $START_TIME, 2))")
    
    print_success "Semua events terkirim!"
    SEND_THROUGHPUT=$(python3 -c "print(int($TOTAL_EVENTS / $SEND_DURATION))")
    print_info "Waktu pengiriman: ${SEND_DURATION} detik"
    print_info "Throughput pengiriman: $SEND_THROUGHPUT events/detik"
    
    # Wait for processing and monitor progress
    print_info "Menunggu processing selesai..."
    
    LAST_DROPPED=0
    STABLE_COUNT=0
    MAX_WAIT=180  # Maximum wait time in seconds
    WAIT_COUNT=0
    
    while [ $STABLE_COUNT -lt 3 ] && [ $WAIT_COUNT -lt $MAX_WAIT ]; do
        sleep 2
        WAIT_COUNT=$((WAIT_COUNT + 2))
        
        CURRENT_STATS=$(curl -s "$BASE_URL/stats")
        CURRENT_PROCESSED=$(echo "$CURRENT_STATS" | jq -r '.unique_processed')
        CURRENT_DROPPED=$(echo "$CURRENT_STATS" | jq -r '.duplicate_dropped')
        QUEUE_SIZE=$(curl -s "$BASE_URL/queue/stats" | jq -r '.queue_size')
        
        echo "    [${WAIT_COUNT}s] Processed: $CURRENT_PROCESSED | Dropped: $CURRENT_DROPPED | Queue: $QUEUE_SIZE"
        
        if [ "$CURRENT_DROPPED" == "$LAST_DROPPED" ] && [ "$QUEUE_SIZE" == "0" ]; then
            STABLE_COUNT=$((STABLE_COUNT + 1))
        else
            STABLE_COUNT=0
        fi
        LAST_DROPPED=$CURRENT_DROPPED
    done
    
    END_TIME=$(python3 -c 'import time; print(time.time())')
    TOTAL_DURATION=$(python3 -c "print(round($END_TIME - $START_TIME, 2))")
    
    print_header "Hasil Stress Test"
    
    # Final stats
    FINAL_STATS=$(curl -s "$BASE_URL/stats")
    echo "$FINAL_STATS" | jq '.'
    
    FINAL_RECEIVED=$(echo "$FINAL_STATS" | jq -r '.received')
    FINAL_UNIQUE=$(echo "$FINAL_STATS" | jq -r '.unique_processed')
    FINAL_DROPPED=$(echo "$FINAL_STATS" | jq -r '.duplicate_dropped')
    
    # Calculate test-specific metrics
    TEST_RECEIVED=$((FINAL_RECEIVED - INITIAL_RECEIVED))
    TEST_UNIQUE=$((FINAL_UNIQUE - INITIAL_UNIQUE))
    TEST_DROPPED=$((FINAL_DROPPED - INITIAL_DROPPED))
    
    # Calculate throughput
    TOTAL_THROUGHPUT=$(python3 -c "print(int($TEST_RECEIVED / $TOTAL_DURATION))")
    SEND_THROUGHPUT=$(python3 -c "print(int($TEST_RECEIVED / $SEND_DURATION))")
    
    print_info "Analisis Stress Test:"
    echo "  ┌─────────────────────────────────────────────────┐"
    echo "  │ METRICS                                         │"
    echo "  ├─────────────────────────────────────────────────┤"
    printf "  │ Total events dikirim    : %-20s │\n" "$TEST_RECEIVED"
    printf "  │ Target events           : %-20s │\n" "$TOTAL_EVENTS"
    printf "  │ Unique diproses         : %-20s │\n" "$TEST_UNIQUE"
    printf "  │ Duplikat di-drop        : %-20s │\n" "$TEST_DROPPED"
    echo "  ├─────────────────────────────────────────────────┤"
    printf "  │ Waktu pengiriman        : %-17ss │\n" "$SEND_DURATION"
    printf "  │ Total waktu             : %-17ss │\n" "$TOTAL_DURATION"
    printf "  │ Throughput (send)       : %-14s evt/s │\n" "$SEND_THROUGHPUT"
    printf "  │ Throughput (total)      : %-14s evt/s │\n" "$TOTAL_THROUGHPUT"
    echo "  ├─────────────────────────────────────────────────┤"
    
    # Calculate actual duplication rate
    if [ "$TEST_RECEIVED" -gt 0 ]; then
        ACTUAL_DUP_RATE=$(python3 -c "print(round($TEST_DROPPED * 100 / $TEST_RECEIVED, 2))")
        printf "  │ Deduplication rate      : %-17s%% │\n" "$ACTUAL_DUP_RATE"
    fi
    echo "  └─────────────────────────────────────────────────┘"
    
    # Validation
    print_info "Validasi:"
    if [ "$TEST_RECEIVED" -ge 20000 ]; then
        print_success "✓ Total events >= 20.000 ($TEST_RECEIVED)"
    else
        echo -e "${COLOR_YELLOW}⚠ Total events < 20.000 ($TEST_RECEIVED)${COLOR_RESET}"
    fi
    
    DUP_CHECK=$(python3 -c "print(1 if $ACTUAL_DUP_RATE >= 30 else 0)")
    if [ "$DUP_CHECK" -eq 1 ]; then
        print_success "✓ Deduplication rate >= 30% ($ACTUAL_DUP_RATE%)"
    else
        echo -e "${COLOR_YELLOW}⚠ Deduplication rate < 30% ($ACTUAL_DUP_RATE%)${COLOR_RESET}"
    fi
    
    if [ "$TOTAL_THROUGHPUT" -ge 100 ]; then
        print_success "✓ Sistem responsif (throughput: $TOTAL_THROUGHPUT events/s)"
    else
        echo -e "${COLOR_YELLOW}⚠ Throughput rendah: $TOTAL_THROUGHPUT events/s${COLOR_RESET}"
    fi
}

show_menu() {
    echo -e "\n${COLOR_BLUE}Pilih demo yang ingin dijalankan:${COLOR_RESET}"
    echo "1) Demo 1: Publish Event Tunggal"
    echo "2) Demo 2: Deduplication (Idempotency)"
    echo "3) Demo 3: Batch Processing"
    echo "4) Demo 4: Queue Statistics"
    echo "5) Demo 5: Load Test (50 events)"
    echo "6) Demo 6: Stress Test (20.000+ events, 30% duplikasi)"
    echo "7) Jalankan semua demo"
    echo "8) Tampilkan stats saja"
    echo "0) Keluar"
    echo -n "Pilihan: "
}

main() {
    print_header "Pub-Sub Log Aggregator - Demo Script"
    
    wait_for_service
    
    if [ "$1" == "all" ]; then
        demo_1_single_event
        demo_2_deduplication
        demo_3_batch
        demo_4_queue_stats
        demo_5_load_test
        demo_6_stress_test
        print_header "Semua demo selesai!"
        curl -s "$BASE_URL/stats" | jq '.'
    elif [ "$1" == "stats" ]; then
        print_header "Statistik Sistem"
        curl -s "$BASE_URL/stats" | jq '.'
        curl -s "$BASE_URL/queue/stats" | jq '.'
    elif [ -n "$1" ]; then
        case "$1" in
            1) demo_1_single_event ;;
            2) demo_2_deduplication ;;
            3) demo_3_batch ;;
            4) demo_4_queue_stats ;;
            5) demo_5_load_test ;;
            6) demo_6_stress_test ;;
            *) echo "Pilihan tidak valid" ;;
        esac
    else
        while true; do
            show_menu
            read choice
            case $choice in
                1) demo_1_single_event ;;
                2) demo_2_deduplication ;;
                3) demo_3_batch ;;
                4) demo_4_queue_stats ;;
                5) demo_5_load_test ;;
                6) demo_6_stress_test ;;
                7) 
                    demo_1_single_event
                    demo_2_deduplication
                    demo_3_batch
                    demo_4_queue_stats
                    demo_5_load_test
                    demo_6_stress_test
                    print_header "Semua demo selesai!"
                    curl -s "$BASE_URL/stats" | jq '.'
                    ;;
                8) 
                    print_header "Statistik Sistem"
                    curl -s "$BASE_URL/stats" | jq '.'
                    curl -s "$BASE_URL/queue/stats" | jq '.'
                    ;;
                0) exit 0 ;;
                *) echo "Pilihan tidak valid" ;;
            esac
        done
    fi
}

main "$@"

