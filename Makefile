BINARY_NAME=dbsafe
VERSION=$(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT=$(shell git rev-parse --short HEAD 2>/dev/null || echo "none")
BUILD_DATE=$(shell date -u '+%Y-%m-%dT%H:%M:%SZ')
LDFLAGS=-ldflags "-X github.com/nethalo/dbsafe/cmd.Version=$(VERSION) -X github.com/nethalo/dbsafe/cmd.CommitSHA=$(COMMIT) -X github.com/nethalo/dbsafe/cmd.BuildDate=$(BUILD_DATE)"

.PHONY: all build clean test lint install demo-up demo-down

all: build

build:
	go build $(LDFLAGS) -o $(BINARY_NAME) .

install:
	go install $(LDFLAGS) .

clean:
	rm -f $(BINARY_NAME)
	go clean

test:
	go test ./... -v

lint:
	golangci-lint run ./...

# Cross-compilation
build-all:
	GOOS=linux   GOARCH=amd64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-amd64 .
	GOOS=linux   GOARCH=arm64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-linux-arm64 .
	GOOS=darwin  GOARCH=amd64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-amd64 .
	GOOS=darwin  GOARCH=arm64 go build $(LDFLAGS) -o dist/$(BINARY_NAME)-darwin-arm64 .

# Demo environment: MySQL 8.0 pre-loaded with ~1.3 GB of e-commerce data
# Showcases DANGEROUS risk levels, gh-ost/pt-osc commands, chunked DML, triggers, and FK display.
demo-up:
	@echo "Starting dbsafe demo environment..."
	@docker compose -f docker-compose.demo.yml up -d
	@echo "Seeding ~2.56M rows of demo data (3-5 min on first run)..."
	@until docker compose -f docker-compose.demo.yml exec -T mysql-demo \
		mysql -u dbsafe -pdbsafe_demo demo -e "SELECT 1" > /dev/null 2>&1; do \
		printf "."; sleep 5; \
	done
	@echo ""
	@echo "Demo environment ready!"
	@echo ""
	@echo "  export DBSAFE_PASSWORD=dbsafe_demo"
	@echo "  CONN=\"-H 127.0.0.1 -P 23306 -u dbsafe -d demo\""
	@echo ""
	@echo "Example commands:"
	@echo "  # DANGEROUS — COPY algorithm on 1.3 GB table, generates gh-ost + pt-osc commands"
	@echo "  DBSAFE_PASSWORD=dbsafe_demo ./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo \\"
	@echo "    \"ALTER TABLE orders MODIFY COLUMN total_amount DECIMAL(14,4)\""
	@echo ""
	@echo "  # DANGEROUS — charset conversion (full table rebuild)"
	@echo "  DBSAFE_PASSWORD=dbsafe_demo ./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo \\"
	@echo "    \"ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4\""
	@echo ""
	@echo "  # SAFE — INSTANT add column on the same 1.3 GB table (contrast!)"
	@echo "  DBSAFE_PASSWORD=dbsafe_demo ./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo \\"
	@echo "    \"ALTER TABLE orders ADD COLUMN loyalty_points INT\""
	@echo ""
	@echo "  # DANGEROUS — DML chunking (>100K rows affected)"
	@echo "  DBSAFE_PASSWORD=dbsafe_demo ./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo \\"
	@echo "    \"DELETE FROM audit_log WHERE created_at < '2025-06-01'\""
	@echo ""
	@echo "  # Trigger fire warning — orders table has 2 AFTER triggers"
	@echo "  DBSAFE_PASSWORD=dbsafe_demo ./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo \\"
	@echo "    \"UPDATE orders SET status = 'cancelled' WHERE status = 'pending'\""
	@echo ""
	@echo "  # FK-rich display (order_items has 2 foreign keys)"
	@echo "  DBSAFE_PASSWORD=dbsafe_demo ./dbsafe plan -H 127.0.0.1 -P 23306 -u dbsafe -d demo \\"
	@echo "    \"ALTER TABLE order_items MODIFY COLUMN unit_price DECIMAL(12,4)\""

demo-down:
	@echo "Stopping demo environment..."
	@docker compose -f docker-compose.demo.yml down -v

tidy:
	go mod tidy

deps:
	go mod download
