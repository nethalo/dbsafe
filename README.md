<h1 align="center">
	<br>
	<img src="assets/dbsafe-logo-v4.svg" alt="dbsafe" width="400">
	<br>
</h1>

<h4 align="center">🛡️ Know exactly what your MySQL DDL/DML will do <em>before</em> you run it</h4>

<p align="center">
	<a href="https://github.com/nethalo/dbsafe/releases"><img src="https://img.shields.io/github/v/release/nethalo/dbsafe?style=flat-square" alt="Latest Release"></a>
	<a href="https://github.com/nethalo/dbsafe/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=flat-square" alt="License"></a>
</p>

<p align="center">
	<a href="#-highlights">Highlights</a> •
	<a href="#-installation">Installation</a> •
	<a href="#-quick-start">Quick Start</a> •
	<a href="#-what-it-analyzes">What It Analyzes</a> •
	<a href="#-examples">Examples</a> •
	<a href="#-supported-versions">Supported Versions</a>
</p>

<br>

> **No more surprises.** Pre-execution safety analysis for MySQL DDL/DML operations. Stop guessing, start knowing.

---

## ✨ Highlights

- **🔍 Deep Analysis** — Algorithm (INSTANT/INPLACE/COPY), locking behavior, table rebuild detection
- **🎯 Risk Assessment** — Automatic classification: Safe, Caution, or Dangerous
- **🌐 Topology Aware** — Auto-detects Galera/PXC, Group Replication, async replicas and adjusts warnings
- **📊 Impact Estimation** — Table size, row count, replication lag, write-set size calculations
- **🔄 Rollback Plans** — Pre-generated reverse SQL and recovery options
- **📝 Chunked Scripts** — Auto-generated batched DELETE/UPDATE scripts for large operations
- **🎨 Multiple Formats** — Text, Plain, JSON, Markdown - perfect for CI/CD and documentation
- **⚡ Read-Only** — Never modifies your data. Ever.
- **✅ Production-Ready** — 85-97% test coverage, integration tests with real MySQL, security hardened

---

## 📦 Installation

### Download Pre-built Binary (Recommended)

**Option 1: One-liner install** (auto-detects OS/arch, verifies checksum, installs latest):

```bash
curl -sSfL https://raw.githubusercontent.com/nethalo/dbsafe/main/install.sh | sh -s -- -b /usr/local/bin
```

Install a specific version or to a custom directory:

```bash
# Specific version
curl -sSfL https://raw.githubusercontent.com/nethalo/dbsafe/main/install.sh | sh -s -- -b /usr/local/bin v0.2.1

# Current directory (./bin/dbsafe)
curl -sSfL https://raw.githubusercontent.com/nethalo/dbsafe/main/install.sh | sh
```

**Option 2: Download from [Releases page](https://github.com/nethalo/dbsafe/releases/latest)** (manual)

**Option 3: Using curl** (specific version):

```bash
# Set version
VERSION=0.2.1

# macOS (Apple Silicon)
curl -L https://github.com/nethalo/dbsafe/releases/download/v${VERSION}/dbsafe_${VERSION}_darwin_arm64.tar.gz | tar xz
sudo mv dbsafe /usr/local/bin/

# macOS (Intel)
curl -L https://github.com/nethalo/dbsafe/releases/download/v${VERSION}/dbsafe_${VERSION}_darwin_amd64.tar.gz | tar xz
sudo mv dbsafe /usr/local/bin/

# Linux (x86_64)
curl -L https://github.com/nethalo/dbsafe/releases/download/v${VERSION}/dbsafe_${VERSION}_linux_amd64.tar.gz | tar xz
sudo mv dbsafe /usr/local/bin/

# Linux (ARM64)
curl -L https://github.com/nethalo/dbsafe/releases/download/v${VERSION}/dbsafe_${VERSION}_linux_arm64.tar.gz | tar xz
sudo mv dbsafe /usr/local/bin/
```

### Build from Source

Requires Go 1.23+

```bash
git clone https://github.com/nethalo/dbsafe.git
cd dbsafe
make build
sudo mv dbsafe /usr/local/bin/
```

---

## 🚀 Quick Start

### 1️⃣ Set up MySQL user (read-only)

```sql
CREATE USER 'dbsafe'@'%' IDENTIFIED BY '<password>';
GRANT SELECT ON *.* TO 'dbsafe'@'%';
GRANT PROCESS ON *.* TO 'dbsafe'@'%';
GRANT REPLICATION CLIENT ON *.* TO 'dbsafe'@'%';
-- ⚠️ No write permissions. Ever.
```

### 2️⃣ Initialize config

```bash
dbsafe config init
```

This creates `~/.dbsafe/config.yaml` interactively.

### 3️⃣ Test connection

```bash
dbsafe connect
```

### 4️⃣ Analyze your first DDL

```bash
dbsafe plan "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
```

🎉 **That's it!** You'll get a detailed analysis of what this DDL will do.

---

## 🔬 What It Analyzes

<details open>
<summary><strong>📋 DDL Operations</strong></summary>

<br>

- ✅ `ADD COLUMN` - Detects INSTANT vs INPLACE, position-specific behavior
- ✅ `DROP COLUMN` - Table rebuild warnings
- ✅ `MODIFY COLUMN` - Data type changes, null/default modifications
- ✅ `CHANGE COLUMN` - Column renames with type changes
- ✅ `ADD INDEX` - Algorithm detection, size warnings
- ✅ `DROP INDEX` - Safety checks
- ✅ `ADD/DROP FOREIGN KEY` - Locking behavior
- ✅ `CHANGE CHARSET` - Full table rebuild warnings
- ✅ `RENAME TABLE` - Metadata-only confirmation

**MySQL Version Matrix:**
- MySQL 8.0.0-8.0.11 (Early)
- MySQL 8.0.12-8.0.28 (INSTANT for trailing columns)
- MySQL 8.0.29+ (INSTANT for any position)
- MySQL 8.4 LTS (Full INSTANT support)

</details>

<details open>
<summary><strong>💥 DML Operations</strong></summary>

<br>

- ✅ `DELETE` - Row estimates via EXPLAIN, chunk calculations
- ✅ `UPDATE` - WHERE clause analysis, affected row estimation
- ✅ `INSERT` - Basic analysis

**Features:**
- 🎯 Automatic chunking recommendations for large operations
- 📊 Affected row percentage calculations
- ⚠️ Write-set size warnings for Galera/PXC
- 🔄 Generated chunked scripts with configurable batch sizes

</details>

<details open>
<summary><strong>🌐 Topology Detection</strong></summary>

<br>

**Auto-detected topologies:**

- 🔷 **Galera/Percona XtraDB Cluster**
  - TOI vs RSU warnings
  - Flow control detection
  - `wsrep_max_ws_size` limit checks
  - Blocks gh-ost recommendations (incompatible)

- 🔶 **MySQL Group Replication**
  - Single-primary vs multi-primary mode
  - `transaction_size_limit` warnings
  - Member count and role detection

- 🔵 **Async/Semi-sync Replication**
  - Replication lag monitoring
  - Primary vs replica detection
  - Large operation lag warnings

- ⚪ **Standalone**
  - Standard MySQL server

</details>

---

## 💡 Examples

### Analyze a DDL

```bash
dbsafe plan "ALTER TABLE orders ADD INDEX idx_created (created_at)"
```

**Output:**
```
╭────────────────────────────────────────────────────────────╮
│ dbsafe — DDL Analysis                                      │
│ Table:             shop.orders                             │
│ Table size:        45.2 GB                                 │
│ Row count:         ~127,456,891                            │
│ Engine:            InnoDB                                  │
╰────────────────────────────────────────────────────────────╯
╭────────────────────────────────────────────────────────────╮
│ Operation                                                  │
│ Type:              ADD_INDEX                               │
│ Algorithm:         INPLACE                                 │
│ Lock:              NONE                                    │
│ Rebuilds table:    false                                   │
╰────────────────────────────────────────────────────────────╯
╭────────────────────────────────────────────────────────────╮
│ Recommendation                                             │
│ ✅ Proceed with caution.                                   │
│                                                            │
│ Large table (45.2 GB). INPLACE allows concurrent DML but  │
│ requires online index build. Estimated time: 2-4 hours.   │
│                                                            │
│ Method: GH-OST (for zero downtime)                        │
╰────────────────────────────────────────────────────────────╯
```

### Analyze DML with chunking

```bash
dbsafe plan "DELETE FROM logs WHERE created_at < '2023-01-01'"
```

Automatically generates:
- 📊 Estimated affected rows
- ⚙️ Recommended chunk size
- 📝 **Executable chunked script** saved to `/tmp/dbsafe-chunked-*.sql`

### JSON output for CI/CD

```bash
dbsafe plan --format json "ALTER TABLE users DROP COLUMN legacy_field" | jq .
```

Perfect for:
- GitHub Actions workflows
- GitLab CI pipelines
- Migration approval gates
- Automated safety checks

### From a file

```bash
dbsafe plan --file migration.sql
```

---

## 🎯 Output Formats

| Format | Flag | Use Case |
|--------|------|----------|
| 🎨 **Text** | `--format text` | Default. Styled terminal with colors & boxes |
| 📄 **Plain** | `--format plain` | No colors. Safe for logs, Slack, piping |
| 📊 **JSON** | `--format json` | CI/CD pipelines, scripting, automation |
| 📝 **Markdown** | `--format markdown` | PR descriptions, tickets, documentation |

---

## 🐬 Supported Versions

| MySQL Version | Support | Notes |
|--------------|---------|-------|
| **MySQL 8.0.x** | ✅ Full | Including Percona Server 8.0 |
| **MySQL 8.4 LTS** | ✅ Full | Including Percona Server 8.4 |
| **Percona XtraDB Cluster 8.x** | ✅ Full | Galera-aware analysis |
| **Group Replication 8.x** | ✅ Full | Topology detection |
| **MySQL 5.7** | ❌ No | EOL October 2023 |
| **MariaDB** | ❌ No | Different DDL behavior |

---

## ⚙️ Configuration

<details>
<summary><strong>Config file structure</strong></summary>

<br>

Location: `~/.dbsafe/config.yaml`

```yaml
connections:
  default:
    host: 127.0.0.1
    port: 3306
    user: dbsafe
    database: myapp
    # password: never store in config, use -p flag

defaults:
  chunk_size: 10000      # Rows per chunk for DML
  chunk_sleep: 0.5       # Seconds between chunks
  format: text           # text | plain | json | markdown
```

**Commands:**
```bash
dbsafe config init     # Create config interactively
dbsafe config show     # Display current config
```

</details>

---

## 🏗️ How It Works

```
1. 📝 Parse SQL       → Vitess sqlparser extracts operation details
2. 🔌 Connect         → Read-only MySQL connection
3. 🔍 Detect Topology → Auto-detect cluster type
4. 📊 Collect Metadata → Table size, indexes, FKs, triggers, engine
5. 🧮 Analyze         → Match against DDL matrix (version × operation)
6. 🎯 Estimate Impact → EXPLAIN for DML, write-set calculations
7. 📋 Generate Report → Recommendations, warnings, rollback, scripts
```

---

## 🧪 Testing & Quality

**dbsafe is production-ready with comprehensive test coverage.**

### Test Coverage

| Package | Coverage | Status |
|---------|----------|--------|
| **Parser** | 96.6% | ✅ Nearly complete |
| **Analyzer** | 94.9% | ✅ Comprehensive |
| **Topology** | 93.0% | ✅ Excellent |
| **MySQL** | 85.5% | ✅ Production-ready |
| **Security** | 100% | ✅ All functions covered |

### Test Suite

**100+ Unit Tests** - Fast, focused validation of individual components

**28 Benchmarks** - Performance tracking with allocation profiling:
```bash
go test -bench=. -benchmem ./internal/...
```

**7 Fuzz Tests** - Automated edge case discovery with 50+ seed cases:
```bash
go test -fuzz=FuzzParse -fuzztime=30s ./internal/parser
```

**Integration Tests** - End-to-end validation with real MySQL 8.0:
```bash
./scripts/run-integration-tests.sh
```

Validates:
- ✅ Topology detection accuracy
- ✅ Version parsing correctness
- ✅ Metadata collection from INFORMATION_SCHEMA
- ✅ DDL algorithm classification (INSTANT/INPLACE/COPY)
- ✅ DML analysis with real EXPLAIN queries
- ✅ Security features (SQL injection prevention)

**Platform Support:**
- ✅ Apple Silicon (M1/M2/M3) - via Rosetta 2 emulation
- ✅ Intel/AMD (x86_64) - native execution
- ✅ Linux - native execution

### Security Testing

**100% coverage** of security-critical functions:
- SQL injection prevention (`validateSafeForExplain`)
- Identifier escaping (`escapeIdentifier`)
- Path traversal prevention (`validateSQLFilePath`)
- Fuzz testing with injection attack patterns

### Running Tests

```bash
# Unit tests (fast, ~2 seconds)
go test ./...

# With coverage report
go test -cover ./...
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Integration tests (~30 seconds)
./scripts/run-integration-tests.sh

# Benchmarks
go test -bench=. -benchmem ./internal/...
```

See [TESTING.md](TESTING.md) for the complete testing guide.

---

## 🤝 Contributing

Contributions welcome! Please:

1. 🍴 Fork the repo
2. 🌿 Create a feature branch
3. ✅ Add tests
4. 📝 Update docs if needed
5. 🚀 Submit a PR

---

## 📄 License

Apache 2.0 - see [LICENSE](LICENSE) file for details.

**Free for commercial and personal use with attribution.**

---

## 🙏 Acknowledgments

- Built with [Vitess sqlparser](https://github.com/vitessio/vitess) for MySQL SQL parsing
- Inspired by the need for safer database operations at scale
- Thanks to the MySQL and Percona communities for comprehensive DDL documentation

---

<p align="center">
	<br>
	<em>Made with ☕ and ❤️ for safer database operations</em>
	<br><br>
	<a href="https://github.com/nethalo/dbsafe">⭐ Star on GitHub</a> •
	<a href="https://github.com/nethalo/dbsafe/issues">🐛 Report Bug</a> •
	<a href="https://github.com/nethalo/dbsafe/issues">💡 Request Feature</a>
</p>
