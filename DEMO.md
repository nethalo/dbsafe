# dbsafe Demo Environment

A MySQL instance pre-loaded with ~2.4M rows of realistic e-commerce data, designed to showcase every major dbsafe output: DANGEROUS risk levels, gh-ost/pt-osc commands, chunked DML scripts, trigger fire warnings, and FK displays.

## Start

```bash
make demo-up                    # MySQL 8.0 (default)
make demo-up MYSQL_VERSION=8.4  # MySQL 8.4 LTS
```

### MySQL 8.0 (default)

Uses `tmpfs` for fast I/O and runs via Rosetta 2 on Apple Silicon. First run seeds ~2.56M rows and takes **~10–12 minutes on Apple Silicon, ~3–5 minutes on x86**. Subsequent `make demo-up` calls after a `demo-down` reseed from scratch (tmpfs is ephemeral).

### MySQL 8.4 LTS

Stores data on overlay2 instead of tmpfs — seeding takes **~15–20 minutes**. No `tmpfs` or `command:` override is used (both crash MySQL 8.4 under macOS Docker Desktop). Auth uses `mysql_native_password=ON` via a mounted config file.

> **Note**: `mysql:8.4` publishes an ARM64 manifest, but Docker may use a cached amd64 layer if you've previously pulled it. Run `docker pull --platform linux/arm64 mysql:8.4` first to get the native image on Apple Silicon.

You'll see dots while it waits for seeding to finish, then the ready message.

## Run dbsafe commands

Password goes via env var (the `-p` flag isn't wired to viper internally):

```bash
# Shorthand to avoid repeating flags
export DBSAFE_PASSWORD=dbsafe_demo
CONN="-H 127.0.0.1 -P 23306 -u dbsafe -d demo"

# DANGEROUS — COPY algorithm on 1.2 GB table → gh-ost + pt-osc commands generated
./dbsafe plan $CONN "ALTER TABLE orders MODIFY COLUMN total_amount DECIMAL(14,4)"

# DANGEROUS — charset conversion forces full table rebuild
./dbsafe plan $CONN "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4"

# SAFE — INSTANT algorithm, same 1.2 GB table (striking contrast to above)
./dbsafe plan $CONN "ALTER TABLE orders ADD COLUMN loyalty_points INT"

# INPLACE — add index, shows disk space estimate
./dbsafe plan $CONN "ALTER TABLE orders ADD INDEX idx_payment (payment_method, created_at)"

# DANGEROUS — DML affects >100K rows → chunked script written to disk
./dbsafe plan $CONN "DELETE FROM audit_log WHERE created_at < '2025-06-01'"

# DANGEROUS — no WHERE clause
./dbsafe plan $CONN "DELETE FROM audit_log"

# DANGEROUS DML + trigger fire warning (orders has 2 AFTER triggers)
./dbsafe plan $CONN "UPDATE orders SET status = 'cancelled' WHERE status = 'pending'"

# FK-rich display — order_items references both orders and products
./dbsafe plan $CONN "ALTER TABLE order_items MODIFY COLUMN unit_price DECIMAL(12,4)"

# JSON output (for CI/CD pipelines)
./dbsafe plan $CONN --format json "ALTER TABLE orders MODIFY COLUMN total_amount DECIMAL(14,4)"

# Idempotent SP wrapper — outputs a stored procedure safe to re-run
./dbsafe plan $CONN --idempotent "ALTER TABLE customers ADD COLUMN notes TEXT"

# Topology info
./dbsafe connect $CONN
```

## Access MySQL directly

```bash
# MySQL 8.0 (default)
docker compose -f docker-compose.demo.yml exec mysql-demo \
  mysql -u dbsafe -pdbsafe_demo demo

# MySQL 8.4
docker compose -f docker-compose.demo-84.yml exec mysql-demo \
  mysql -u dbsafe -pdbsafe_demo demo

# As root (replace compose file as needed)
docker compose -f docker-compose.demo.yml exec mysql-demo \
  mysql -u root -proot demo
```

Useful queries once inside:

```sql
-- Check table sizes
SELECT TABLE_NAME,
       FORMAT(TABLE_ROWS, 0) AS rows,
       CONCAT(ROUND((DATA_LENGTH+INDEX_LENGTH)/1024/1024/1024, 2), ' GB') AS size
FROM information_schema.TABLES
WHERE TABLE_SCHEMA = 'demo'
ORDER BY DATA_LENGTH+INDEX_LENGTH DESC;

-- Check triggers
SHOW TRIGGERS;

-- Check foreign keys
SELECT CONSTRAINT_NAME, TABLE_NAME, REFERENCED_TABLE_NAME
FROM information_schema.KEY_COLUMN_USAGE
WHERE TABLE_SCHEMA = 'demo' AND REFERENCED_TABLE_NAME IS NOT NULL;
```

## Stop

```bash
make demo-down                    # Stop MySQL 8.0
make demo-down MYSQL_VERSION=8.4  # Stop MySQL 8.4
```

Stops the container and removes volumes. Everything is gone. Next `make demo-up` starts completely fresh.

## Connection details

| What      | Value                                       |
|-----------|---------------------------------------------|
| Host      | `127.0.0.1`                                 |
| Port      | `23306`                                     |
| User      | `dbsafe`                                    |
| Password  | `dbsafe_demo` (via `DBSAFE_PASSWORD=...`)   |
| Database  | `demo`                                      |

## What's in the database

| Table         | Rows     | Size    | Purpose                                   |
|---------------|----------|---------|-------------------------------------------|
| `orders`      | ~2.4M    | ~1.2 GB | Star table — triggers DANGEROUS DDL risk  |
| `audit_log`   | ~250K    | ~77 MB  | DML demo — triggers CHUNKED at >100K rows |
| `order_items` | ~500K    | ~30 MB  | 2 FK constraints (orders + products)      |
| `customers`   | ~10K     | ~3 MB   | FK target for orders                      |
| `products`    | 1K       | <1 MB   | FK target for order_items                 |

`orders` uses `utf8mb3` charset deliberately — `CONVERT TO CHARACTER SET utf8mb4` forces a full COPY rebuild on a 1.2 GB table, which is the most visually impressive dbsafe output.

Two triggers on `orders` (`AFTER UPDATE`, `AFTER DELETE`) appear as warnings on any DML against that table.
