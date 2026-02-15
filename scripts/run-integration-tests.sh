#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
COMPOSE_FILE="$PROJECT_DIR/docker-compose.test.yml"

# Functions
print_header() {
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
}

print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
    echo -e "${BLUE}ℹ${NC} $1"
}

# Check if Docker is running
check_docker() {
    print_header "Checking Docker"

    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed"
        echo "Please install Docker Desktop: https://www.docker.com/products/docker-desktop"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        print_error "Docker daemon is not running"
        echo "Please start Docker Desktop and try again"
        exit 1
    fi

    print_success "Docker is running"
    docker --version
}

# Start containers
start_containers() {
    print_header "Starting MySQL Test Containers"

    cd "$PROJECT_DIR"

    # Check if containers are already running
    if docker-compose -f docker-compose.test.yml ps | grep -q "Up"; then
        print_warning "Containers already running. Stopping and restarting..."
        docker-compose -f docker-compose.test.yml down -v
    fi

    print_info "Starting 7 MySQL containers (this may take a minute)..."
    docker-compose -f docker-compose.test.yml up -d

    print_success "Containers started"
}

# Wait for containers to be healthy
wait_for_healthy() {
    print_header "Waiting for Containers to be Healthy"

    local max_wait=120  # 2 minutes
    local elapsed=0
    local check_interval=5

    while [ $elapsed -lt $max_wait ]; do
        # Get container status
        local status_output=$(docker-compose -f docker-compose.test.yml ps 2>&1)

        # Count total containers and healthy containers
        local total=$(echo "$status_output" | grep -c "Up" || true)
        local healthy=$(echo "$status_output" | grep -c "healthy" || true)

        if [ "$total" -gt 0 ] && [ "$healthy" -eq "$total" ]; then
            echo ""
            print_success "All $healthy containers are healthy!"
            return 0
        fi

        printf "\r${YELLOW}⏳${NC} Waiting for containers... ($elapsed/${max_wait}s) - $healthy/$total healthy"
        sleep $check_interval
        elapsed=$((elapsed + check_interval))
    done

    echo ""
    print_error "Timeout waiting for containers to become healthy"
    print_info "Container status:"
    docker-compose -f docker-compose.test.yml ps
    exit 1
}

# Show container status
show_status() {
    print_header "Container Status"
    docker-compose -f docker-compose.test.yml ps
}

# Run integration tests
run_tests() {
    print_header "Running Integration Tests"

    cd "$PROJECT_DIR"

    print_info "Running tests with -v flag for verbose output..."
    echo ""

    if go test -tags=integration ./test -v; then
        echo ""
        print_success "All integration tests passed!"
        return 0
    else
        echo ""
        print_error "Some integration tests failed"
        return 1
    fi
}

# Run integration benchmarks
run_benchmarks() {
    print_header "Running Integration Benchmarks"

    cd "$PROJECT_DIR"

    print_info "Running benchmarks..."
    echo ""

    go test -tags=integration ./test -bench=. -benchmem
}

# Cleanup containers
cleanup() {
    print_header "Cleaning Up"

    cd "$PROJECT_DIR"

    print_info "Stopping and removing containers..."
    docker-compose -f docker-compose.test.yml down -v

    print_success "Cleanup complete"
}

# Show help
show_help() {
    cat << EOF
Usage: ./scripts/run-integration-tests.sh [OPTIONS]

Run dbsafe integration tests with Docker containers.

Options:
    -h, --help          Show this help message
    -s, --start-only    Only start containers (don't run tests)
    -t, --test-only     Only run tests (assume containers are running)
    -b, --benchmark     Run benchmarks after tests
    -k, --keep          Keep containers running after tests
    -c, --cleanup       Stop and remove all test containers
    --status            Show container status and exit

Examples:
    # Run full test suite (start, test, cleanup)
    ./scripts/run-integration-tests.sh

    # Start containers and keep them running
    ./scripts/run-integration-tests.sh -s -k

    # Run tests against already-running containers
    ./scripts/run-integration-tests.sh -t

    # Run tests and benchmarks, keep containers running
    ./scripts/run-integration-tests.sh -b -k

    # Just cleanup
    ./scripts/run-integration-tests.sh -c

EOF
}

# Parse arguments
START_ONLY=false
TEST_ONLY=false
RUN_BENCHMARK=false
KEEP_RUNNING=false
CLEANUP_ONLY=false
SHOW_STATUS=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -s|--start-only)
            START_ONLY=true
            shift
            ;;
        -t|--test-only)
            TEST_ONLY=true
            shift
            ;;
        -b|--benchmark)
            RUN_BENCHMARK=true
            shift
            ;;
        -k|--keep)
            KEEP_RUNNING=true
            shift
            ;;
        -c|--cleanup)
            CLEANUP_ONLY=true
            shift
            ;;
        --status)
            SHOW_STATUS=true
            shift
            ;;
        *)
            print_error "Unknown option: $1"
            show_help
            exit 1
            ;;
    esac
done

# Main execution
main() {
    echo ""
    print_header "dbsafe Integration Test Runner"
    echo ""

    # Handle special modes
    if [ "$CLEANUP_ONLY" = true ]; then
        cleanup
        exit 0
    fi

    if [ "$SHOW_STATUS" = true ]; then
        show_status
        exit 0
    fi

    # Check Docker
    check_docker
    echo ""

    # Start containers (unless test-only mode)
    if [ "$TEST_ONLY" = false ]; then
        start_containers
        echo ""
        wait_for_healthy
        echo ""
        show_status
        echo ""
    fi

    # Run tests (unless start-only mode)
    if [ "$START_ONLY" = false ]; then
        if run_tests; then
            TEST_SUCCESS=true
        else
            TEST_SUCCESS=false
        fi
        echo ""

        # Run benchmarks if requested
        if [ "$RUN_BENCHMARK" = true ] && [ "$TEST_SUCCESS" = true ]; then
            run_benchmarks
            echo ""
        fi
    fi

    # Cleanup (unless keep flag is set)
    if [ "$KEEP_RUNNING" = false ] && [ "$START_ONLY" = false ]; then
        cleanup
        echo ""
    else
        print_info "Containers are still running. Use './scripts/run-integration-tests.sh -c' to cleanup"
        echo ""
    fi

    # Final status
    if [ "$START_ONLY" = true ]; then
        print_success "Containers started and ready for testing!"
        print_info "Run tests with: go test -tags=integration ./test -v"
        print_info "Cleanup with: ./scripts/run-integration-tests.sh -c"
    elif [ "$TEST_ONLY" = false ]; then
        if [ "$TEST_SUCCESS" = true ]; then
            print_header "✓ Integration Tests Complete - All Passed!"
        else
            print_header "✗ Integration Tests Complete - Some Failed"
            exit 1
        fi
    fi

    echo ""
}

# Run main function
main
