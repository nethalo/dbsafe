# dbsafe Online DDL Operations — Full Test Spec

## Objective

Verify that `dbsafe` correctly classifies the DDL algorithm (INSTANT, INPLACE, or COPY), lock level, and table-rebuild behavior for **every online DDL operation** documented in the MySQL reference manual, for both MySQL 8.0 and MySQL 8.4. This spec exists to prevent regressions like [issue #18](https://github.com/nethalo/dbsafe/issues/18).

## Reference Documentation

- **MySQL 8.0** — Tables 17.16 through 17.22 + Partitioning:
  https://dev.mysql.com/doc/refman/8.0/en/innodb-online-ddl-operations.html
- **MySQL 8.4** — Tables 17.15 through 17.21 + Partitioning:
  https://dev.mysql.com/doc/refman/8.4/en/innodb-online-ddl-operations.html

## Context: Bug #18 (the motivation for this spec)

`dbsafe plan` was run against:
```sql
ALTER TABLE orders CHANGE COLUMN total_amount amount DECIMAL(14,4)
```
`dbsafe` reported `Algorithm: INSTANT`, `Lock: NONE`, `Rebuilds table: false`, and recommended "Safe to run directly."

**Correct answer:** The column `total_amount` was `DECIMAL(10,2)` (different from the declared `DECIMAL(14,4)`), making this a **data type change**. Per MySQL docs: "Changing the column data type is only supported with `ALGORITHM=COPY`." The output should have been `Algorithm: COPY`, `Rebuilds table: true`, concurrent DML = no, with a recommendation to use pt-osc or gh-ost.

**Root cause:** `dbsafe` did not compare the declared type against the actual column type from the live schema, and it did not properly classify `CHANGE COLUMN` operations that combine a rename with a type change.

---

## Test Environment

Use the demo environment from [DEMO.md](https://github.com/nethalo/dbsafe/blob/main/DEMO.md).

### Start

```bash
make demo-up MYSQL_VERSION=8.4
```

First run seeds ~2.56M rows (~10–12 min on Apple Silicon, ~3–5 min on x86).

### Connection

```bash
export DBSAFE_PASSWORD=dbsafe_demo
CONN="-H 127.0.0.1 -P 23306 -u dbsafe -d demo"
```

| Property | Value |
|----------|-------|
| Host | `127.0.0.1` |
| Port | `23306` |
| User | `dbsafe` |
| Password | `dbsafe_demo` (via `DBSAFE_PASSWORD`) |
| Database | `demo` |

### Existing Demo Tables

| Table | Rows | Size | Notes |
|-------|------|------|-------|
| `orders` | ~2.4M | ~1.2 GB | 2 AFTER triggers, utf8mb3 charset, FK to customers |
| `audit_log` | ~250K | ~77 MB | DML demo table |
| `order_items` | ~500K | ~30 MB | 2 FK constraints (orders + products) |
| `customers` | ~10K | ~3 MB | FK target for orders |
| `products` | 1K | <1 MB | FK target for order_items |

### Direct MySQL Access (for setup/verification)

```bash
# As root
docker compose -f docker-compose.demo.yml exec mysql-demo \
  mysql -u root -proot demo
```

### Additional Test Tables

Some operations require table structures not present in the demo. Create these **as root** before running the tests:

```sql
USE demo;

-- For generated column tests
CREATE TABLE IF NOT EXISTS gen_col_test (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    price DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    quantity INT NOT NULL DEFAULT 1,
    total_stored DECIMAL(12,2) AS (price * quantity) STORED,
    total_virtual DECIMAL(12,2) AS (price * quantity) VIRTUAL,
    label VARCHAR(100) DEFAULT 'item'
) ENGINE=InnoDB;

INSERT INTO gen_col_test (price, quantity, label)
SELECT ROUND(RAND()*100,2), FLOOR(RAND()*10)+1, CONCAT('item_', seq)
FROM (SELECT @r:=@r+1 AS seq FROM information_schema.tables t1,
      (SELECT @r:=0) r LIMIT 1000) s;

-- For tablespace tests
CREATE TABLE IF NOT EXISTS tablespace_test (
    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    data VARCHAR(200) DEFAULT 'test'
) ENGINE=InnoDB;

INSERT INTO tablespace_test (data) SELECT REPEAT('x',100)
FROM (SELECT @r2:=@r2+1 FROM information_schema.tables t1,
      (SELECT @r2:=0) r LIMIT 1000) s;

-- For partitioning tests
CREATE TABLE IF NOT EXISTS partition_test (
    id INT NOT NULL AUTO_INCREMENT,
    created_at DATE NOT NULL,
    data VARCHAR(100),
    PRIMARY KEY (id, created_at)
) ENGINE=InnoDB
PARTITION BY RANGE (YEAR(created_at)) (
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p2025 VALUES LESS THAN (2026),
    PARTITION pmax VALUES LESS THAN MAXVALUE
);

INSERT INTO partition_test (created_at, data)
SELECT DATE_ADD('2023-01-01', INTERVAL FLOOR(RAND()*1095) DAY), REPEAT('y',50)
FROM (SELECT @r3:=@r3+1 FROM information_schema.tables t1,
      (SELECT @r3:=0) r LIMIT 5000) s;
```

### Running a Test

```bash
./dbsafe plan $CONN "<SQL>"
```

For JSON output (for CI/CD validation):
```bash
./dbsafe plan $CONN --format json "<SQL>"
```

### Reset Between Tests

Some tests modify schema. Reset the demo between destructive tests:
```bash
make demo-down && make demo-up
```

Or, for faster iteration, re-run only the CREATE TABLE statements for the specific test table being modified via `mysql -u root -proot`.

---

## Test Matrix: How to Read Each Section

For every operation below, `dbsafe` output must be validated against these fields:

1. **Algorithm** — INSTANT, INPLACE, or COPY
2. **Lock** — NONE, SHARED, or EXCLUSIVE
3. **Rebuilds table** — true/false
4. **Concurrent DML** — whether reads/writes are allowed during the operation
5. **Recommendation** — must NOT say "Safe to run directly" for COPY on large tables

Record results as:

| # | Operation | Test SQL | Expected Algo | Actual Algo | Expected Rebuild | Actual Rebuild | PASS/FAIL |
|---|-----------|----------|---------------|-------------|------------------|----------------|-----------|

---

## SECTION 1: Index Operations

**MySQL 8.0 Table 17.16 / MySQL 8.4 Table 17.15**

### 1.1 Creating or Adding a Secondary Index

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE orders ADD INDEX idx_payment (payment_method, created_at)
```

**Verify:** INPLACE, no rebuild, concurrent DML allowed. On the 1.2 GB `orders` table, `dbsafe` should show disk space estimate for the new index.

### 1.2 Dropping an Index

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
-- First add an index, then test dropping it
ALTER TABLE orders DROP INDEX idx_payment
```

**Verify:** INPLACE, metadata-only, concurrent DML allowed.

### 1.3 Renaming an Index

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE orders RENAME INDEX idx_customer_id TO idx_cust
```

**Verify:** INPLACE, metadata-only.

### 1.4 Adding a FULLTEXT Index

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes* |
| Rebuilds Table | No* (rebuilds if first FT index and no FTS_DOC_ID) |
| Concurrent DML | No |
| Metadata Only | No |

```sql
-- Use a table with a text column
ALTER TABLE audit_log ADD FULLTEXT INDEX ft_action (action)
```

**Verify:** INPLACE, concurrent DML = **No** (LOCK=SHARED or higher). First FULLTEXT index may rebuild the table. `dbsafe` should warn about DML blocking.

### 1.5 Adding a SPATIAL Index

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | No |
| Metadata Only | No |

> **Note:** Demo tables don't have GEOMETRY columns. If `dbsafe` parses the SQL without connecting, test with a synthetic statement. Otherwise, create a test table with a GEOMETRY column.

```sql
-- Setup:
CREATE TABLE geo_test (id INT PRIMARY KEY, g GEOMETRY NOT NULL SRID 0);
ALTER TABLE geo_test ADD SPATIAL INDEX idx_geo (g)
```

**Verify:** INPLACE, LOCK=SHARED, concurrent DML = No.

### 1.6 Changing the Index Type

| Property | Expected |
|----------|----------|
| Instant | Yes |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE orders DROP INDEX idx_status, ADD INDEX idx_status (status) USING BTREE
```

**Verify:** INSTANT, metadata-only.

---

## SECTION 2: Primary Key Operations

**MySQL 8.0 Table 17.17 / MySQL 8.4 Table 17.16**

### 2.1 Adding a Primary Key

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes* |
| Rebuilds Table | Yes* |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
-- Create test table without PK
CREATE TABLE no_pk_test (a INT NOT NULL, b VARCHAR(50));
INSERT INTO no_pk_test SELECT FLOOR(RAND()*100000), 'test' FROM
  (SELECT @r4:=@r4+1 FROM information_schema.tables t1, (SELECT @r4:=0) r LIMIT 1000) s;
ALTER TABLE no_pk_test ADD PRIMARY KEY (a)
```

**Verify:** INPLACE, rebuilds table = yes (expensive), concurrent DML allowed. `dbsafe` should flag this as a table rebuild.

### 2.2 Dropping a Primary Key

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | No |
| Rebuilds Table | Yes |
| Concurrent DML | No |
| Metadata Only | No |

```sql
ALTER TABLE no_pk_test DROP PRIMARY KEY
```

**Verify:** COPY only. Concurrent DML = No. `dbsafe` must report COPY algorithm.

### 2.3 Dropping a Primary Key and Adding Another

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | Yes |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE no_pk_test DROP PRIMARY KEY, ADD PRIMARY KEY (b, a)
```

**Verify:** INPLACE, rebuilds table, concurrent DML allowed. Expensive operation.

---

## SECTION 3: Column Operations

**MySQL 8.0 Table 17.18 / MySQL 8.4 Table 17.17**

### 3.1 Adding a Column

| Property | Expected |
|----------|----------|
| Instant | Yes* |
| In Place | Yes |
| Rebuilds Table | No* |
| Concurrent DML | Yes* |
| Metadata Only | Yes |

```sql
ALTER TABLE orders ADD COLUMN loyalty_points INT
```

**Verify:** INSTANT, no rebuild, safe to run directly. (This is already in the demo.)

**Edge case — auto-increment column:**
```sql
ALTER TABLE orders ADD COLUMN seq_id INT NOT NULL AUTO_INCREMENT, ADD UNIQUE KEY (seq_id)
```
**Verify:** INPLACE (not INSTANT), LOCK=SHARED minimum, concurrent DML NOT permitted. Expensive.

### 3.2 Dropping a Column

| Property | Expected |
|----------|----------|
| Instant | Yes* (≥8.0.29) |
| In Place | Yes |
| Rebuilds Table | Yes (INPLACE), No (INSTANT) |
| Concurrent DML | Yes |
| Metadata Only | Yes (INSTANT) |

```sql
-- First add a column, then drop it
ALTER TABLE orders DROP COLUMN loyalty_points
```

**Verify:** INSTANT (≥8.0.29), no rebuild.

### 3.3 Renaming a Column (same data type — pure rename)

| Property | Expected |
|----------|----------|
| Instant | Yes* (≥8.0.28) |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes* |
| Metadata Only | Yes |

```sql
-- Keep exact same type as the existing column
ALTER TABLE orders CHANGE COLUMN status order_status ENUM('pending','processing','shipped','delivered','cancelled') NOT NULL DEFAULT 'pending'
```

**Verify:** INSTANT (≥8.0.28) or INPLACE. No rebuild. `dbsafe` must detect the type is unchanged.

### 3.4 CHANGE COLUMN with Data Type Change ⚠️ BUG #18 REGRESSION TEST

| Property | Expected |
|----------|----------|
| Instant | **No** |
| In Place | **No** |
| Rebuilds Table | **Yes** |
| Concurrent DML | **No** |
| Metadata Only | **No** |

```sql
ALTER TABLE orders CHANGE COLUMN total_amount amount DECIMAL(14,4)
```

**Verify:** `dbsafe` MUST report **COPY**. `total_amount` is `DECIMAL(10,2)`, changing to `DECIMAL(14,4)` is a data type change. This is the exact bug #18 scenario.

**Additional sub-cases:**
```sql
-- MODIFY (no rename, just type change) — must also be COPY
ALTER TABLE orders MODIFY COLUMN total_amount DECIMAL(14,4)

-- CHANGE with same type — must NOT be COPY (should be INSTANT/INPLACE)
ALTER TABLE orders CHANGE COLUMN total_amount order_total DECIMAL(10,2) NOT NULL DEFAULT 0.00
```

### 3.5 Reordering Columns

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | Yes |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE orders MODIFY COLUMN payment_method VARCHAR(20) AFTER id
```

**Verify:** INPLACE, rebuilds table (expensive), concurrent DML allowed.

### 3.6 Setting a Column Default Value

| Property | Expected |
|----------|----------|
| Instant | Yes |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'processing'
```

**Verify:** INSTANT, metadata-only.

### 3.7 Changing the Column Data Type (MODIFY)

| Property | Expected |
|----------|----------|
| Instant | **No** |
| In Place | **No** |
| Rebuilds Table | **Yes** |
| Concurrent DML | **No** |
| Metadata Only | **No** |

```sql
ALTER TABLE orders MODIFY COLUMN total_amount DECIMAL(14,4)
```

**Verify:** **COPY**. This is already in the DEMO.md as the first "DANGEROUS" example. Full table copy, blocks all DML. On the 1.2 GB `orders` table, `dbsafe` must generate gh-ost/pt-osc commands.

### 3.8 Extending VARCHAR Column Size

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
-- Within same length-byte boundary (stays in 0–255 range)
ALTER TABLE orders MODIFY COLUMN payment_method VARCHAR(50)
```

**Verify:** INPLACE, no rebuild, metadata-only.

**Edge case — crossing the 255-byte boundary:**
```sql
ALTER TABLE orders MODIFY COLUMN payment_method VARCHAR(256)
```
**Verify:** **COPY** (length bytes change from 1 to 2). Critical edge case.

### 3.9 Dropping the Column Default Value

| Property | Expected |
|----------|----------|
| Instant | Yes |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE orders ALTER COLUMN status DROP DEFAULT
```

**Verify:** INSTANT, metadata-only.

### 3.10 Changing the Auto-Increment Value

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | No* |

```sql
ALTER TABLE orders AUTO_INCREMENT = 99999999
```

**Verify:** INPLACE, no rebuild.

### 3.11 Making a Column NULL

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | Yes* |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE customers MODIFY COLUMN name VARCHAR(100) NULL
```

**Verify:** INPLACE, rebuilds table, concurrent DML allowed.

### 3.12 Making a Column NOT NULL

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes* |
| Rebuilds Table | Yes* |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE customers MODIFY COLUMN email VARCHAR(200) NOT NULL
```

**Verify:** INPLACE, rebuilds table. Requires STRICT mode; fails if existing NULLs. `dbsafe` should ideally warn about NULL values.

### 3.13 Modifying ENUM/SET Definition

| Property | Expected |
|----------|----------|
| Instant | Yes |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
-- Adding members at the END of the list
ALTER TABLE orders MODIFY COLUMN status ENUM('pending','processing','shipped','delivered','cancelled','refunded')
```

**Verify:** INSTANT, metadata-only.

**Edge case — adding member NOT at the end (reorder/insert in middle):**
```sql
ALTER TABLE orders MODIFY COLUMN status ENUM('new','pending','processing','shipped','delivered','cancelled')
```
**Verify:** This changes storage representation → requires COPY. `dbsafe` should NOT report INSTANT.

---

## SECTION 4: Generated Column Operations

**MySQL 8.0 Table 17.19 / MySQL 8.4 Table 17.18**

Use the `gen_col_test` table created in setup.

### 4.1 Adding a STORED Generated Column

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | No |
| Rebuilds Table | Yes |
| Concurrent DML | No |
| Metadata Only | No |

```sql
ALTER TABLE gen_col_test ADD COLUMN discount_price DECIMAL(10,2) AS (price * 0.9) STORED
```

**Verify:** COPY (not INPLACE — stored columns require expression evaluation). Concurrent DML = No.

### 4.2 Adding a VIRTUAL Generated Column

| Property | Expected |
|----------|----------|
| Instant | Yes* |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE gen_col_test ADD COLUMN markup DECIMAL(10,2) AS (price * 1.1) VIRTUAL
```

**Verify:** INSTANT or INPLACE for non-partitioned tables. No rebuild.

### 4.3 Dropping a STORED Generated Column

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | Yes |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE gen_col_test DROP COLUMN total_stored
```

**Verify:** INPLACE, rebuilds table.

### 4.4 Dropping a VIRTUAL Generated Column

| Property | Expected |
|----------|----------|
| Instant | Yes* |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE gen_col_test DROP COLUMN total_virtual
```

**Verify:** INSTANT or INPLACE for non-partitioned tables. No rebuild.

### 4.5 Modifying a STORED Generated Column Order

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | No |
| Rebuilds Table | Yes |
| Concurrent DML | No |
| Metadata Only | No |

```sql
ALTER TABLE gen_col_test MODIFY COLUMN total_stored DECIMAL(12,2) AS (price * quantity) STORED FIRST
```

**Verify:** COPY, rebuilds table.

### 4.6 Modifying a VIRTUAL Generated Column Order

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE gen_col_test MODIFY COLUMN total_virtual DECIMAL(12,2) AS (price * quantity) VIRTUAL AFTER id
```

**Verify:** INPLACE, no rebuild.

---

## SECTION 5: Foreign Key Operations

**MySQL 8.0 Table 17.20 / MySQL 8.4 Table 17.19**

Use the `order_items` table (has FKs to `orders` and `products`).

### 5.1 Adding a Foreign Key Constraint

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes* |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
-- INPLACE is supported when foreign_key_checks is disabled
-- With foreign_key_checks enabled, only COPY is supported
ALTER TABLE order_items ADD CONSTRAINT fk_test FOREIGN KEY (order_id) REFERENCES orders(id)
```

**Verify:** `dbsafe` should check the state of `foreign_key_checks` and report accordingly. If enabled → COPY; if disabled → INPLACE.

### 5.2 Dropping a Foreign Key Constraint

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE order_items DROP FOREIGN KEY fk_order_items_order
```

**Verify:** INPLACE, metadata-only. Works with `foreign_key_checks` enabled or disabled.

---

## SECTION 6: Table Operations

**MySQL 8.0 Table 17.21 / MySQL 8.4 Table 17.20**

### 6.1 Changing ROW_FORMAT

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | Yes |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE customers ROW_FORMAT=COMPRESSED
```

**Verify:** INPLACE, rebuilds table. Expensive.

### 6.2 Changing KEY_BLOCK_SIZE

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | Yes |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE customers KEY_BLOCK_SIZE=8
```

**Verify:** INPLACE, rebuilds table.

### 6.3 Setting Persistent Statistics Options

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE orders STATS_PERSISTENT=0, STATS_SAMPLE_PAGES=20, STATS_AUTO_RECALC=1
```

**Verify:** INPLACE, metadata-only.

### 6.4 Converting Character Set

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes* |
| Rebuilds Table | Yes (if encoding differs) |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
-- orders uses utf8mb3 deliberately — this forces a full rebuild
ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4
```

**Verify:** INPLACE, rebuilds table (encoding differs). This is already in the DEMO.md as the second "DANGEROUS" example. On the 1.2 GB `orders` table, `dbsafe` must flag this as DANGEROUS and generate pt-osc/gh-ost commands.

**Note:** INPLACE is NOT supported for tables with FULLTEXT indexes; those require COPY.

### 6.5 Specifying a Character Set (column level)

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes* |
| Rebuilds Table | Yes (if encoding differs) |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE orders MODIFY COLUMN payment_method VARCHAR(20) CHARACTER SET utf8mb4
```

**Verify:** INPLACE, rebuilds table if charset is actually different.

### 6.6 Rebuilding a Table (FORCE / null ALTER)

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | Yes |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE orders FORCE
```

```sql
ALTER TABLE orders ENGINE=InnoDB
```

**Verify:** INPLACE, rebuilds table. This reclaims space and resets TOTAL_ROW_VERSIONS.

### 6.7 OPTIMIZE TABLE (null rebuild)

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | Yes |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
OPTIMIZE TABLE orders
```

**Verify:** Mapped to `ALTER TABLE ... FORCE` internally. INPLACE, rebuild.

> **Note:** `dbsafe` may or may not support OPTIMIZE TABLE syntax. If it doesn't, skip this and document as out of scope.

### 6.8 Renaming a Table

| Property | Expected |
|----------|----------|
| Instant | Yes |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
ALTER TABLE products RENAME TO product_catalog
```

**Verify:** INSTANT, metadata-only. No data movement.

---

## SECTION 7: Tablespace Operations

**MySQL 8.0 Table 17.22 / MySQL 8.4 Table 17.21**

### 7.1 Renaming a General Tablespace

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | Yes |
| Rebuilds Table | No |
| Concurrent DML | Yes |
| Metadata Only | Yes |

```sql
-- Requires a general tablespace to exist
ALTER TABLESPACE ts1 RENAME TO ts2
```

**Verify:** INPLACE. Note: does not support the ALGORITHM clause explicitly.

> **Note:** Demo environment may not have general tablespaces. This test may require additional setup or can be tested via SQL parsing only.

### 7.2 Enabling File-Per-Table Tablespace Encryption

| Property | Expected |
|----------|----------|
| Instant | No |
| In Place | No |
| Rebuilds Table | Yes |
| Concurrent DML | Yes |
| Metadata Only | No |

```sql
ALTER TABLE tablespace_test ENCRYPTION='Y'
```

**Verify:** COPY algorithm. Requires keyring plugin to be active.

> **Note:** Demo environment may not have encryption configured. Document as conditional test.

---

## SECTION 8: Partitioning Operations

**MySQL 8.0 & 8.4 — Partitioning section (follows the same rules as regular InnoDB tables with some exceptions)**

Use the `partition_test` table created in setup.

### 8.1 Adding a Partition

```sql
ALTER TABLE partition_test ADD PARTITION (PARTITION p2026 VALUES LESS THAN (2027))
```

**Verify:** Fast metadata operation when adding to RANGE/LIST partitions. No data movement for existing partitions.

### 8.2 Dropping a Partition

```sql
ALTER TABLE partition_test DROP PARTITION p2023
```

**Verify:** Drops data and partition definition. Very fast, no rebuild of remaining partitions. `dbsafe` should warn about data loss.

### 8.3 Reorganizing Partitions

```sql
ALTER TABLE partition_test REORGANIZE PARTITION pmax INTO (
    PARTITION p2026 VALUES LESS THAN (2027),
    PARTITION pmax VALUES LESS THAN MAXVALUE
)
```

**Verify:** Only rebuilds the affected partition, not the entire table. INPLACE.

### 8.4 Rebuilding Partitions

```sql
ALTER TABLE partition_test REBUILD PARTITION p2024
```

**Verify:** Rebuilds only the specified partition. INPLACE.

### 8.5 COALESCE / EXCHANGE / TRUNCATE Partitions

```sql
ALTER TABLE partition_test TRUNCATE PARTITION p2023
```

**Verify:** Drops all rows in the partition without rebuilding. `dbsafe` should warn about data loss.

> **Note:** Some partition operations (COALESCE, EXCHANGE) require HASH/KEY partitioning or specific conditions. If the demo doesn't support them, document as out of scope for this test run.

---

## Execution Checklist

### Per-operation validation:

For **each** test above, verify:

- [ ] `Algorithm` field matches expected
- [ ] `Lock` field matches expected
- [ ] `Rebuilds table` field matches expected
- [ ] `Recommendation` does NOT say "Safe to run directly" for COPY on large tables
- [ ] `Method` recommends pt-osc/gh-ost for COPY operations on large tables
- [ ] `Rollback` section provides a valid reverse operation
- [ ] JSON output (`--format json`) contains the same correct values

### Cross-version validation:

Run every test against **both** MySQL 8.0 and 8.4 (if a second instance is available on port 23307). The tables are the same between versions, but table numbers and some behaviors differ:

| Category | MySQL 8.0 Table | MySQL 8.4 Table |
|----------|-----------------|-----------------|
| Index Operations | 17.16 | 17.15 |
| Primary Key Operations | 17.17 | 17.16 |
| Column Operations | 17.18 | 17.17 |
| Generated Column Operations | 17.19 | 17.18 |
| Foreign Key Operations | 17.20 | 17.19 |
| Table Operations | 17.21 | 17.20 |
| Tablespace Operations | 17.22 | 17.21 |

---

## Results Template

```
| Section | # | Operation | Test SQL (abbreviated) | MySQL Ver | Expected | Actual | PASS/FAIL |
|---------|---|-----------|------------------------|-----------|----------|--------|-----------|
| Index | 1.1 | Add secondary index | ADD INDEX idx_payment... | 8.0 | INPLACE | | |
| Index | 1.1 | Add secondary index | ADD INDEX idx_payment... | 8.4 | INPLACE | | |
| ... | ... | ... | ... | ... | ... | ... | ... |
| Column | 3.4 | CHANGE + type change | CHANGE total_amount... | 8.0 | COPY | | |
| Column | 3.4 | CHANGE + type change | CHANGE total_amount... | 8.4 | COPY | | |
```

---

## Success Criteria

- **All operations across all 8 sections × 2 MySQL versions** must have correct algorithm classification
- **Zero regressions** on bug #18: `CHANGE COLUMN` with a data type change must always report COPY
- **Edge cases** must pass: VARCHAR 255→256 boundary, ENUM member reorder, auto-increment column add, foreign key with/without `foreign_key_checks`, charset conversion on FULLTEXT tables
- **JSON output** must be consistent with text output
- Any operation `dbsafe` does not currently support must be documented as a known gap (not a silent wrong answer)

## Teardown

```bash
make demo-down
```
