# dbsafe Demo Environment

A MySQL 8.0 instance pre-loaded with ~2.4M rows of realistic e-commerce data, designed to showcase every major dbsafe output: DANGEROUS risk levels, gh-ost/pt-osc commands, chunked DML scripts, trigger fire warnings, and FK displays.

## Start

```bash
make demo-up
```

First run seeds ~2.56M rows and takes **3–5 minutes**. Subsequent `make demo-up` calls after a `demo-down` also reseed from scratch (tmpfs is ephemeral). You'll see dots while it waits, then the ready message.

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

# Topology info
./dbsafe connect $CONN
```

## Access MySQL directly

```bash
# As the dbsafe read-only user
docker compose -f docker-compose.demo.yml exec mysql-demo \
  mysql -u dbsafe -pdbsafe_demo demo

# As root (full access)
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
make demo-down
```

Stops the container and deletes the tmpfs volume. Everything is gone. Next `make demo-up` starts completely fresh.

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
| `order_items` | ~300K    | ~30 MB  | 2 FK constraints (orders + products)      |
| `customers`   | ~10K     | ~3 MB   | FK target for orders                      |
| `products`    | 1K       | <1 MB   | FK target for order_items                 |

`orders` uses `utf8mb3` charset deliberately — `CONVERT TO CHARACTER SET utf8mb4` forces a full COPY rebuild on a 1.2 GB table, which is the most visually impressive dbsafe output.

Two triggers on `orders` (`AFTER UPDATE`, `AFTER DELETE`) appear as warnings on any DML against that table.
