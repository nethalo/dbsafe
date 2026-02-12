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
make lint           # Requires golangci-lint

# Clean build artifacts
make clean

# Install to $GOPATH/bin
make install

# Dependencies
make deps           # Download dependencies
make tidy           # Tidy go.mod/go.sum
```

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
- Test files: `*_test.go` in same package
- Parser tests validate SQL extraction accuracy
- Analyzer tests verify DDL matrix classifications
- Renderer tests check output format correctness

## MySQL Interaction Constraints

- **Read-only**: dbsafe never modifies data
- Required grants: SELECT, PROCESS, REPLICATION CLIENT
- Default connection pool: MaxOpenConns=2, MaxIdleConns=1 (CLI tool, not server)
- When database is empty in config, defaults to `information_schema` for connection (DSN building)

## GoReleaser Integration

The project uses GoReleaser for releases (`.goreleaser.yaml` exists). Build versioning uses git tags via ldflags in Makefile.
