# dbsafe

Pre-execution safety analysis for MySQL DDL/DML operations.

Know exactly what your `ALTER TABLE` or `DELETE` will do before you run it. No guesses.

## What it does

dbsafe analyzes MySQL DDL and DML statements and tells you:

- **Algorithm & locking behavior** — INSTANT, INPLACE, or COPY? Will it lock writes?
- **Replication impact** — How does this affect Galera/PXC, Group Replication, or async replicas?
- **Risk assessment** — Safe, caution, or dangerous?
- **Execution method** — Direct, gh-ost, pt-online-schema-change, or chunked?
- **Rollback plan** — Pre-generated reverse SQL and recovery options
- **Chunked scripts** — Auto-generated batched scripts for large DML

## Supported MySQL versions

- MySQL 8.0.x (including Percona Server 8.0)
- MySQL 8.4 LTS (including Percona Server 8.4)
- Percona XtraDB Cluster 8.0 / 8.4
- MySQL Group Replication 8.0 / 8.4

MySQL 5.7 is not supported (EOL October 2023).

## Quick start

```bash
# Build
make build

# Initialize config
./dbsafe config init

# Test connection
./dbsafe connect -H 10.0.1.50 -u dbsafe -p

# Analyze a DDL
./dbsafe plan "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT ''"

# Analyze a DML
./dbsafe plan "DELETE FROM logs WHERE created_at < '2023-01-01'"

# Read SQL from file
./dbsafe plan --file migration.sql

# JSON output for CI/CD
./dbsafe plan --format json "ALTER TABLE events ADD INDEX idx_created (created_at)"

# Markdown for tickets
./dbsafe plan --format markdown "ALTER TABLE users DROP COLUMN legacy_field" > ticket.md
```

## MySQL user setup

dbsafe requires **read-only** access. It never modifies data.

```sql
CREATE USER 'dbsafe'@'%' IDENTIFIED BY '<password>';
GRANT SELECT ON *.* TO 'dbsafe'@'%';
GRANT PROCESS ON *.* TO 'dbsafe'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'dbsafe'@'%';
-- No write permissions. Ever.
```

## Configuration

```bash
# Create interactively
dbsafe config init

# Or manually at ~/.dbsafe/config.yaml
```

```yaml
connections:
  default:
    host: 127.0.0.1
    port: 3306
    user: dbsafe
    database: myapp

defaults:
  chunk_size: 10000
  chunk_sleep: 0.5
  format: text        # text | plain | json | markdown
```

## Output formats

| Format | Flag | Use case |
|--------|------|----------|
| `text` | `--format text` | Default. Styled terminal output with colors |
| `plain` | `--format plain` | No colors/boxes. Safe for piping, logs, Slack |
| `json` | `--format json` | Machine-readable. CI/CD pipelines, scripting |
| `markdown` | `--format markdown` | Documentation, PR descriptions, tickets |

## How it works

1. **Parses** the SQL statement (vitess/sqlparser)
2. **Connects** to MySQL with read-only credentials
3. **Detects topology** — standalone, async replica, Galera/PXC, Group Replication
4. **Collects metadata** — table size, row count, indexes, FKs, triggers
5. **Classifies** DDL against the operation matrix (MySQL version × operation → algorithm/lock)
6. **Estimates** DML impact via EXPLAIN and write-set size calculation
7. **Generates** recommendation, warnings, rollback plan, and chunked scripts

## Topology awareness

dbsafe auto-detects your MySQL topology and adjusts recommendations:

- **Galera/PXC**: Warns about TOI vs RSU, wsrep_max_ws_size limits, flow control. Blocks gh-ost recommendations (incompatible).
- **Group Replication**: Checks transaction_size_limit, single-primary vs multi-primary mode.
- **Async/Semi-sync**: Monitors replication lag, warns about large operations increasing lag.

## License

Apache 2.0 - see [LICENSE](LICENSE) file for details.

Free for commercial and personal use with attribution.
