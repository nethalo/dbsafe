<h1 align="center">
	<br>
	<img src="assets/dbsafe-logo-v4.svg" alt="dbsafe" width="200">
	<br>
	dbsafe
	<br>
</h1>

<h4 align="center">🛡️ Know exactly what your MySQL DDL/DML will do <em>before</em> you run it</h4>

<p align="center">
	<a href="https://github.com/nethalo/dbsafe/releases"><img src="https://img.shields.io/github/v/release/nethalo/dbsafe?style=flat-square" alt="Latest Release"></a>
	<a href="https://github.com/nethalo/dbsafe/blob/main/LICENSE"><img src="https://img.shields.io/badge/license-Apache%202.0-blue.svg?style=flat-square" alt="License"></a>
	<a href="https://github.com/nethalo/dbsafe/actions"><img src="https://img.shields.io/github/actions/workflow/status/nethalo/dbsafe/release.yml?style=flat-square" alt="Build Status"></a>
	<a href="https://goreportcard.com/report/github.com/nethalo/dbsafe"><img src="https://goreportcard.com/badge/github.com/nethalo/dbsafe?style=flat-square" alt="Go Report Card"></a>
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

---

## 📦 Installation

### Download Pre-built Binary (Recommended)

Grab the latest binary for your platform from [**Releases**](https://github.com/nethalo/dbsafe/releases):

```bash
# macOS (Apple Silicon)
curl -L https://github.com/nethalo/dbsafe/releases/latest/download/dbsafe_Darwin_arm64.tar.gz | tar xz
sudo mv dbsafe /usr/local/bin/

# macOS (Intel)
curl -L https://github.com/nethalo/dbsafe/releases/latest/download/dbsafe_Darwin_x86_64.tar.gz | tar xz
sudo mv dbsafe /usr/local/bin/

# Linux (x86_64)
curl -L https://github.com/nethalo/dbsafe/releases/latest/download/dbsafe_Linux_x86_64.tar.gz | tar xz
sudo mv dbsafe /usr/local/bin/

# Linux (ARM64)
curl -L https://github.com/nethalo/dbsafe/releases/latest/download/dbsafe_Linux_arm64.tar.gz | tar xz
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

<details>
<summary><strong>📋 DDL Operations (click to expand)</strong></summary>

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

<details>
<summary><strong>💥 DML Operations (click to expand)</strong></summary>

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

<details>
<summary><strong>🌐 Topology Detection (click to expand)</strong></summary>

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
