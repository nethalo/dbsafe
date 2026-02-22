# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build and Development Commands

```bash
# Build the binary
make build          # Outputs: ./dbsafe

# Run tests
make test           # Runs all tests with verbose output
go test ./...       # Run all tests (quiet)
go test -run TestName ./internal/parser  # Run specific test in package

# Linting
make lint           # Requires golangci-lint (not available in sandbox — TLS blocked)
go vet ./...        # Available everywhere; catches most critical issues
# golangci-lint is not installable in the Claude Code sandbox (TLS cert error).
# Use go vet + manual errcheck review instead.

# Clean build artifacts
make clean

# Install to $GOPATH/bin
make install

# Dependencies
make deps           # Download dependencies
make tidy           # Tidy go.mod/go.sum
```

## Demo Environment

A MySQL 8.0 instance pre-loaded with ~2.4M rows of realistic e-commerce data. See `DEMO.md` for the full usage guide.

```bash
make demo-up       # Start MySQL container + seed data (~10-12 min on Apple Silicon, ~3-5 min on x86)
make demo-down     # Stop and remove container + data
```

**Connection**: `127.0.0.1:23306`, user `dbsafe`, database `demo`, password via `DBSAFE_PASSWORD=dbsafe_demo`

**Key tables**: `orders` (~2.4M rows, 1.2 GB, utf8mb3, 2 triggers), `audit_log` (~250K rows), `order_items` (2 FKs), `customers`, `products`

**What it showcases**: DANGEROUS risk levels (COPY on 1.2 GB), pt-osc-only recommendation for `orders` (has 2 triggers — gh-ost is excluded), chunked DML scripts (>100K rows), trigger fire warnings, FK displays, INSTANT vs COPY contrast.

**Files**: `docker-compose.demo.yml`, `scripts/demo-seed.sql`, `DEMO.md`

## Architecture Overview

### Processing Flow

The core flow for analyzing SQL statements:

1. **Parse** (`internal/parser`) - Vitess sqlparser extracts DDL/DML operations, table names, column info
2. **Connect** (`internal/mysql`) - Establish read-only MySQL connection
3. **Detect** (`internal/topology`) - Auto-detect topology (standalone, Galera/PXC, Group Replication, async replica)
4. **Collect** (`internal/mysql`) - Gather table metadata (size, indexes, FKs, triggers, engine)
5. **Analyze** (`internal/analyzer`) - Match operation against DDL matrix, estimate impact
6. **Render** (`internal/output`) - Format output (text, plain, json, markdown)

### Module Breakdown

**`cmd/`** - Cobra CLI commands
- `root.go`: Global flags, viper config initialization, **important**: maps nested config (`connections.default.*`) to flat keys
- `connect.go`: Test connection and show topology
- `plan.go`: Main analysis command for DDL/DML
- `config.go`: Interactive config setup

**`internal/parser/`** - SQL parsing via Vitess
- Extracts: database, table, operation type, column details, WHERE clauses
- Returns `ParsedSQL` struct with classified operation (DDL vs DML, specific operation type)

**`internal/mysql/`** - Database interaction
- `connection.go`: DSN building, connection pooling
- `metadata.go`: Query INFORMATION_SCHEMA for table metadata
- `variables.go`: Version parsing, flavor detection (mysql, percona, percona-xtradb-cluster), variable queries

**`internal/topology/`** - Cluster topology detection
- Detection order: Galera/PXC → Group Replication → Async Replication → Standalone
- **Galera/PXC**: Checks `wsrep_on` variable + `wsrep_cluster_size` status (status is more reliable)
- **Group Replication**: Checks `group_replication_group_name` variable
- Returns `Info` struct with topology type and cluster-specific metadata

**`internal/analyzer/`** - Core analysis engine
- `ddl_matrix.go`: **The DDL classification matrix** - maps (MySQL version × DDL operation) → (algorithm, lock level, table rebuild)
  - Version ranges: V8_0_Early, V8_0_Instant (8.0.12+), V8_0_Full (8.0.29+), V8_4_LTS
  - Critical for determining INSTANT vs INPLACE vs COPY operations
  - **When modifying this file**, use the `ddl-matrix-reviewer` agent to validate changes (`.claude/agents/ddl-matrix-reviewer.md`)
- `analyzer.go`: Risk assessment, execution method recommendations (native, gh-ost, pt-osc, chunked)

**`internal/output/`** - Multi-format rendering
- Supports: text (styled terminal), plain (no colors), json, markdown
- Uses charmbracelet/lipgloss for terminal styling

### Config File Architecture

**Critical**: Config uses nested YAML structure but viper expects flat keys:

```yaml
connections:
  default:
    host: 127.0.0.1
    port: 3306
    user: dbsafe
    database: myapp
defaults:
  chunk_size: 10000
  format: text
```

**Mapping in `cmd/root.go`**: `initConfig()` explicitly maps `connections.default.host` → `host`, etc. after reading config. This happens **only if flags aren't explicitly set** (respects CLI flag precedence).

### Flag Handling

- Password flag `-p` uses `NoOptDefVal = ""` to support optional values (allows `-p` as last parameter without value)
- Viper binds flags to config keys, but not all flags (e.g., password is intentionally not bound)
- Database validation: `plan` command requires database via `-d` flag or qualified table name in SQL

## Key Patterns and Conventions

### Error Handling
- Return descriptive errors with context: `fmt.Errorf("metadata collection failed: %w", err)`
- User-facing errors should suggest fixes (e.g., "use -d flag or specify database in SQL")

### Version Detection
- `ParseVersion()` extracts `major.minor.patch` and detects flavor from version string
- Flavor strings: "mysql", "percona", "percona-xtradb-cluster", "mariadb"
- Version-specific feature checks: `SupportsInstantAddColumn()`, `SupportsInstantAnyPosition()`

### Topology Detection Robustness
- Check enabling variables first (e.g., `wsrep_on`) before cluster state
- Prefer status variables over regular variables when available (more reliable)
- Galera/PXC: Read `wsrep_cluster_size` from **status** first, fallback to variable

### Testing

See [TESTING.md](TESTING.md) for the full testing guide (coverage reports, integration test setup, benchmarks, fuzz tests, CI/CD examples).

**Test files**: `*_test.go` in same package

**Test types**:
- **Unit tests**: Parser, analyzer, renderer tests
- **Integration tests**: `test/integration_test.go` with real MySQL containers (build tag: `// +build integration`)
- **Benchmarks**: Performance tracking with allocation profiling
- **Fuzz tests**: Edge case discovery with seed corpus

**Running tests**:
```bash
# Unit tests only
go test ./...

# Integration tests (requires Docker)
./scripts/run-integration-tests.sh

# With coverage
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

#### Critical Testing Lessons

**1. Mock Structure - ALWAYS Isolate Mocks Per Subtest**

❌ **WRONG** - Shared mock causes expectation conflicts:
```go
func TestSomething(t *testing.T) {
    db, mock, _ := sqlmock.New()  // Created once
    defer db.Close()

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            tt.setupMock()  // All subtests add to same mock
            // Expectations pile up and execute in wrong order!
        })
    }
}
```

✅ **CORRECT** - Each subtest gets its own mock:
```go
func TestSomething(t *testing.T) {
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            db, mock, _ := sqlmock.New()  // Fresh mock per subtest
            defer db.Close()

            tt.setupMock(mock)  // Isolated expectations
        })
    }
}
```

**2. sqlmock Regex Escaping - The 4-Backslash Rule**

MySQL LIKE patterns with underscores require **4 backslashes** in sqlmock expectations:

```go
// Actual SQL query sent to MySQL:
"SHOW GLOBAL VARIABLES LIKE 'wsrep\_on'"  // 1 backslash in SQL

// sqlmock regex pattern needs to match that backslash:
// Regex: \\ matches one literal backslash
// Go string: \\\\ produces \\ in regex
mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'")  // 4 backslashes in source
```

**The escaping chain**:
1. Go source code: `\\\\_` (4 backslashes)
2. Go string literal: `\\_` (2 backslashes after Go interprets it)
3. Regex pattern: `\_` (matches 1 literal backslash + underscore in SQL)
4. MySQL query: `\_` (1 backslash in actual SQL)

**Common patterns requiring 4 backslashes**:
```go
'read\\\\_only'
'super\\\\_read\\\\_only'
'wsrep\\\\_on'
'wsrep\\\\_cluster\\\\_size'
'group\\\\_replication\\\\_group\\\\_name'
```

**3. MySQL SHOW Commands Don't Support Prepared Statements**

❌ **WRONG** - Causes syntax error:
```go
db.QueryRow("SHOW VARIABLES LIKE ?", name)
// Error: You have an error in your SQL syntax near '?' at line 1
```

✅ **CORRECT** - Use direct string formatting with escaping:
```go
escapedName := strings.ReplaceAll(name, "_", "\\_")
escapedName = strings.ReplaceAll(escapedName, "%", "\\%")
query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", escapedName)
db.QueryRow(query)
```

**Security note**: Variable names are system-defined constants (not user input), so direct formatting is safe.

**4. Two-Query Fallback Pattern for Variables**

Some MySQL variables (like `wsrep_on`) aren't available via `SHOW GLOBAL VARIABLES`:

```go
func GetVariable(db *sql.DB, name string) (string, error) {
    // Try GLOBAL first
    query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", escapedName)
    err := db.QueryRow(query).Scan(&varName, &value)
    if err == sql.ErrNoRows {
        // Fallback to non-GLOBAL
        query = fmt.Sprintf("SHOW VARIABLES LIKE '%s'", escapedName)
        err = db.QueryRow(query).Scan(&varName, &value)
    }
    return value, err
}
```

**Mock both queries**:
```go
// First attempt with GLOBAL returns no rows
mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
    WillReturnError(sql.ErrNoRows)

// Second attempt without GLOBAL succeeds
mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
    WillReturnRows(rows)
```

**5. Integration Test Docker Setup**

**Apple Silicon compatibility** - Add platform specification:
```yaml
services:
  mysql-standalone:
    image: mysql:8.0
    platform: linux/amd64  # Required for ARM64 Macs (uses Rosetta 2)
```

**Test execution** - Keep containers running for fast iteration:
```bash
./scripts/run-integration-tests.sh -s -k  # Start containers, keep running
./scripts/run-integration-tests.sh -t     # Run tests only (~3 seconds)
./scripts/run-integration-tests.sh -c     # Cleanup when done
```

**7. tmpfs and MySQL on macOS Docker Desktop**

Not all MySQL images work with `tmpfs` on macOS Docker Desktop. The Lima VM that Docker Desktop uses doesn't support certain kernel ioctls that some MySQL startup code relies on:

- **MySQL 8.0**: Works fine with `tmpfs`
- **MySQL 8.4**: Crashes with `MY-011065 Unable to determine if daemon is running: Inappropriate ioctl for device` — remove `tmpfs`, let it use overlay2
- **PXC 8.0**: Crashes with `Permission denied` on the tmpfs mount point — remove `tmpfs`, let it use overlay2

```yaml
# Works on macOS Docker Desktop:
mysql-standalone:
  tmpfs:
    - /var/lib/mysql:size=1G  # OK for mysql:8.0

# Broken on macOS Docker Desktop — remove tmpfs entirely:
mysql-lts:
  # no tmpfs: section — uses overlay2 instead
pxc-node1:
  # no tmpfs: section — uses overlay2 instead
```

**8. MySQL 8.4 auth: use config file mount, not `command:` override**

MySQL 8.4 defaults to `caching_sha2_password`. To re-enable `mysql_native_password` for plaintext TCP connections in Docker:

✅ **CORRECT** — mount a config file (doesn't change entrypoint behavior):
```yaml
volumes:
  - ./test/mysql84.cnf:/etc/mysql/conf.d/mysql84.cnf:ro
```
```ini
# test/mysql84.cnf
[mysqld]
mysql_native_password=ON
```

❌ **WRONG** — `command:` override changes how the Docker entrypoint starts the temporary init server, triggering the ioctl crash on macOS:
```yaml
command: --mysql-native-password=ON  # breaks MySQL 8.4 on macOS
```

**9. PXC env vars required for single-node bootstrap**

PXC requires the same env vars as vanilla MySQL images (`MYSQL_DATABASE`, `MYSQL_USER`, `MYSQL_PASSWORD`) plus PXC-specific ones. The bootstrap SQL file (`/docker-entrypoint-initdb.d/`) is NOT needed when env vars are set — adding it can interfere with the PXC wsrep bootstrap phase and cause the timezone-load step to fail.

```yaml
pxc-node1:
  environment:
    MYSQL_ROOT_PASSWORD: test_root_password
    MYSQL_DATABASE: testdb
    MYSQL_USER: dbsafe
    MYSQL_PASSWORD: test_password
    CLUSTER_NAME: test_cluster
    XTRABACKUP_PASSWORD: test_xtrabackup_password
  # No volumes: with initdb SQL — env vars handle DB/user creation
```

**ARM64 / Apple Silicon limitations**: `percona:8.0` has no ARM64 image (crashes immediately). `pxc-node1` bootstraps successfully on x86-64 but Galera's `asio` SSL network layer hangs non-deterministically under Rosetta 2. The current integration tests (`TestIntegration_StandaloneMySQL`, `TestIntegration_MySQLLTS`, `TestIntegration_DDLClassification`) only target `mysql-standalone` and `mysql-lts` and pass on Apple Silicon.

**10. Deferred Index Creation for Bulk Loads**

When bulk-loading into a large InnoDB table on tmpfs, secondary indexes during load cause two problems: (1) each inserted row must also update every index B-tree, slowing inserts dramatically; (2) with a large buffer pool, all dirty pages stay in memory and the checkpoint LSN never advances — the redo log fills up and MySQL crashes.

**Fix**: create the table without secondary indexes, load all data in batched transactions, then add indexes afterward:

```sql
-- During seed: table created without secondary indexes
CREATE TABLE orders (
  id BIGINT UNSIGNED PRIMARY KEY AUTO_INCREMENT,
  ...
);

-- Load data in 100K-row batches via stored procedure
-- (each batch COMMITs, which advances the checkpoint LSN)

-- After load: add secondary indexes
ALTER TABLE orders
  ADD INDEX idx_customer (customer_id),
  ADD INDEX idx_status (status, created_at),
  ...;
```

**Why batched COMMITs matter**: a single giant transaction holds all dirty pages until commit, blocking checkpointing. Committing every 100K rows lets InnoDB flush and reclaim redo log space.

**Don't use `--innodb-redo-log-capacity`**: increasing the log size just delays the crash — the root cause is the checkpoint not advancing. Fix the load pattern instead.

**6. Fixing Escaping Issues - Use Byte-Level Operations**

When shell/sed/perl escaping becomes confusing, use Python byte mode:
```python
with open('file.go', 'rb') as f:
    content = f.read()

# Match and replace actual bytes
old = b"'wsrep\\\\_on'"     # 2 backslashes in file
new = b"'wsrep\\\\\\\\_on'"  # 4 backslashes in file
content = content.replace(old, new)

with open('file.go', 'wb') as f:
    f.write(content)
```

This bypasses all shell/string interpretation layers.

## MySQL Interaction Constraints

- **Read-only**: dbsafe never modifies data
- Required grants: SELECT, PROCESS, REPLICATION CLIENT
- Default connection pool: MaxOpenConns=2, MaxIdleConns=1 (CLI tool, not server)
- When database is empty in config, defaults to `information_schema` for connection (DSN building)

## GoReleaser Integration

The project uses GoReleaser for releases (`.goreleaser.yaml` exists). Build versioning uses git tags via ldflags in Makefile.
