#!/bin/bash
# Script untuk menjalankan tests di Docker Compose

set -e

COLOR_GREEN='\033[0;32m'
COLOR_BLUE='\033[0;34m'
COLOR_YELLOW='\033[1;33m'
COLOR_RED='\033[0;31m'
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

print_error() {
    echo -e "${COLOR_RED}✗ $1${COLOR_RESET}"
}

# Method 1: Test dengan test service
test_with_service() {
    print_header "Method 1: Test dengan Test Service"
    
    print_info "Membangun test image..."
    docker compose -f docker-compose.yml -f docker-compose.test.yml build test
    
    print_info "Menjalankan tests..."
    docker compose -f docker-compose.yml -f docker-compose.test.yml \
        --profile test run --rm test
    
    print_success "Tests selesai!"
}

# Method 2: Test di aggregator container yang sudah running
test_in_running_container() {
    print_header "Method 2: Test di Container yang Sudah Running"
    
    print_info "Memastikan container aggregator berjalan..."
    if ! docker compose ps aggregator | grep -q "Up"; then
        print_error "Container aggregator tidak berjalan. Jalankan: docker compose up -d"
        exit 1
    fi
    
    print_info "Menginstall dependencies test di container..."
    docker compose exec aggregator bash -c "
        pip install pytest pytest-cov httpx || true
    "
    
    print_info "Menyalin test files ke container..."
    docker compose cp tests aggregator:/tmp/tests
    
    print_info "Menjalankan tests..."
    docker compose exec aggregator bash -c "
        cd /tmp && \
        PYTHONPATH=/app pytest tests/test_api.py -v --tb=short
    "
    
    print_success "Tests selesai!"
}

# Method 3: Test dengan exec langsung (jika code sudah ada di container)
test_with_exec() {
    print_header "Method 3: Test dengan Exec (Code sudah di Container)"
    
    print_info "Memastikan container aggregator berjalan..."
    if ! docker compose ps aggregator | grep -q "Up"; then
        print_error "Container aggregator tidak berjalan. Jalankan: docker compose up -d"
        exit 1
    fi
    
    print_info "Menjalankan tests di container..."
    docker compose exec aggregator bash -c "
        if [ -d /app/tests ]; then
            cd /app && \
            pip install pytest pytest-cov httpx > /dev/null 2>&1 && \
            pytest tests/test_api.py -v --tb=short
        else
            echo 'Tests directory tidak ditemukan. Gunakan Method 1 atau 2.'
            exit 1
        fi
    "
}

# Method 4: Integration test terhadap service yang running
test_integration() {
    print_header "Method 4: Integration Test terhadap Service Running"
    
    print_info "Memastikan semua service berjalan..."
    docker compose up -d
    
    print_info "Menunggu service ready (15 detik)..."
    sleep 15
    
    print_info "Menjalankan integration tests..."
    docker compose run --rm --entrypoint "" test bash -c "
        pip install pytest pytest-cov httpx > /dev/null 2>&1 && \
        export AGGREGATOR_URL=http://aggregator:8080 && \
        export DATABASE_URL=postgresql+psycopg2://user:pass@storage:5432/uasdb && \
        pytest tests/test_api.py::test_publish_accepts_event -v
    " || true
    
    print_info "Untuk test lengkap, gunakan Method 1"
}

show_menu() {
    echo -e "\n${COLOR_BLUE}Pilih metode testing:${COLOR_RESET}"
    echo "1) Test dengan Test Service (Recommended)"
    echo "2) Test di Container yang Sudah Running"
    echo "3) Test dengan Exec (jika code sudah di container)"
    echo "4) Integration Test terhadap Service Running"
    echo "5) Jalankan semua methods"
    echo "0) Keluar"
    echo -n "Pilihan: "
}

main() {
    print_header "Docker Compose Testing Script"
    
    if [ "$1" == "1" ] || [ "$1" == "service" ]; then
        test_with_service
    elif [ "$1" == "2" ] || [ "$1" == "exec" ]; then
        test_in_running_container
    elif [ "$1" == "3" ] || [ "$1" == "direct" ]; then
        test_with_exec
    elif [ "$1" == "4" ] || [ "$1" == "integration" ]; then
        test_integration
    elif [ "$1" == "5" ] || [ "$1" == "all" ]; then
        test_with_service
        echo ""
        test_in_running_container
    elif [ -n "$1" ]; then
        print_error "Pilihan tidak valid: $1"
        echo "Gunakan: 1, 2, 3, 4, 5, atau kosongkan untuk menu interaktif"
        exit 1
    else
        while true; do
            show_menu
            read choice
            case $choice in
                1) test_with_service ;;
                2) test_in_running_container ;;
                3) test_with_exec ;;
                4) test_integration ;;
                5) 
                    test_with_service
                    echo ""
                    test_in_running_container
                    ;;
                0) exit 0 ;;
                *) print_error "Pilihan tidak valid" ;;
            esac
        done
    fi
}

main "$@"

