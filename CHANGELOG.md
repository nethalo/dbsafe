# Changelog

All notable changes to dbsafe are documented here.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/) and the project adheres to [Semantic Versioning](https://semver.org/).

---

## [Unreleased]

## [0.3.1] - 2026-02-23

### Fixed
- `CHANGE COLUMN` was incorrectly classified as `INSTANT` for MySQL 8.0.29+ and 8.4 LTS — `INSTANT` is never valid for this operation (#18)
- Rename-only `CHANGE COLUMN` (same data type) now correctly reports `INPLACE`; type-changing `CHANGE COLUMN` now correctly reports `COPY` with `SHARED` lock and table rebuild
- Added runtime type-comparison logic: dbsafe compares the existing column type (from `INFORMATION_SCHEMA`) against the new type in the SQL and upgrades to `COPY` when they differ, with a warning showing the detected change (e.g. `decimal(12,2) → decimal(14,4)`)

## [0.3.0] - 2026-02-22

### Added
- Cloud MySQL support: TLS (`--tls`, `--tls-ca` flags), Aurora MySQL auto-detection (topology `AuroraWriter`/`AuroraReader`, gh-ost replaced with pt-osc automatically), Amazon RDS detection via `basedir`
- Full-pipeline regression test suite: 26 end-to-end cases covering DDL/DML across all supported MySQL version ranges and topology types
- Parser tests for detail fields and previously uncovered branches (ADD/DROP PARTITION, ENGINE, ROW_FORMAT, ALTER COLUMN SET/DROP DEFAULT)

### Fixed
- Integration test containers on Apple Silicon:
  - `mysql-lts` (8.4): removed `tmpfs` (ioctl incompatibility with macOS Docker Desktop), added `test/mysql84.cnf` config mount to enable `mysql_native_password` for plain TCP auth
  - `pxc-node1`: removed `tmpfs` (permission denied on PXC init), added required `MYSQL_DATABASE`/`MYSQL_USER`/`MYSQL_PASSWORD` env vars and healthcheck, removed redundant bootstrap SQL volume
  - `waitForMySQL`: fixed connection leak — `defer db.Close()` inside a retry loop held up to 30 connections open until the function returned; now closes explicitly per iteration

### Changed
- `test/pxc-bootstrap.sql` replaced: was an empty directory (crashed PXC entrypoint), now a valid SQL file
- TESTING.md documents Apple Silicon container limitations (Percona no ARM64 image, PXC Galera flaky under Rosetta 2)

## [0.2.8] - 2026-02-21

### Fixed
- gh-ost is no longer suggested for tables with existing triggers; pt-online-schema-change (with `--preserve-triggers`) is recommended instead (#17)

## [0.2.7] - 2026-02-21

### Added
- Claude Code automations: `release` skill, `ddl-matrix-reviewer` agent, `go vet` PostToolUse hook

## [0.2.6] - 2026-02-21

### Added
- Demo environment: MySQL 8.0 pre-loaded with ~2.4M rows of e-commerce data (`docker-compose.demo.yml`, `scripts/demo-seed.sql`, `DEMO.md`)
- `make demo-up` / `make demo-down` targets for one-command demo setup
- `.gitignore` pattern for generated chunked SQL plan output files (`dbsafe-plan-*.sql`)

## [0.2.5] - 2026-02-21

### Fixed
- Unchecked error returns from `bufio.ReadString` in `config init` interactive prompts (`cmd/config.go`)
- Unchecked `rows.Scan` error in Group Replication member role detection (`internal/topology/detector.go`)
- Unchecked `rows.Scan` error in async replication lag detection (`internal/topology/detector.go`)

### Changed
- `interface{}` replaced with `any` in replication status scan slice (Go 1.18+ idiom)
- Removed unused `strings` import and `_ = strings.Join` workaround from `plain.go`
- Struct field alignment normalised across multiple files (gofmt)

---

## [0.2.4] - 2026-02-21

### Fixed
- `DROP PRIMARY KEY` was misclassified as `DROP INDEX` — now correctly identified as `DropPrimaryKey`
- `DROP FOREIGN KEY` was misclassified as `DROP INDEX` — now correctly identified as `DropForeignKey`
- `ADD PRIMARY KEY` was misclassified as `ADD INDEX` — now correctly identified as `AddPrimaryKey`

### Added
- DDL matrix entries for `ADD/DROP PRIMARY KEY`, `CHANGE ROW FORMAT`, `ADD/DROP PARTITION` — these operations now produce full recommendations instead of the worst-case fallback
- Parser classification for `ALTER COLUMN SET/DROP DEFAULT`, `ENGINE =`, `ROW_FORMAT =`, `ADD PARTITION`, `DROP PARTITION` (all previously fell through to `OtherDDL`)
- 14 new tests (9 parser + 5 analyzer) covering all newly classified operations

### Changed
- Singleton Vitess parser via `sync.Once` instead of per-call allocation
- README documents all supported DDL operations

---

## [0.2.3] - 2025-12-01

### Added
- `--version` flag to display the current binary version
- Output includes both gh-ost and pt-osc tool options for large DDL operations

---

## [0.2.2] - 2025-11-15

### Added
- `install.sh` one-liner install script with OS/arch auto-detection and checksum verification
- Disk space requirements included in DDL recommendations

---

## [0.2.1] - 2025-10-28

### Fixed
- `wsrep_cluster_size` STATUS query escaping in Galera/PXC detection
- `wsrep_on` variable lookup: falls back to `SHOW VARIABLES` when `SHOW GLOBAL VARIABLES` returns no rows
- `SHOW VARIABLES` prepared statement issue — now uses direct string formatting with escaping
- Integration test docker-compose compatibility for Apple Silicon (ARM64)

### Changed
- Comprehensive test suite with 85–97% coverage across all packages
- Integration test runner script (`scripts/run-integration-tests.sh`)

---

## [0.2.0] - 2025-10-01

### Fixed
- Affected rows calculation for DML statements with WHERE clause

### Added
- Comprehensive test coverage for production readiness
- Security hardening: SQL injection prevention, file permission checks, path traversal prevention

---

## [0.1.5] - 2025-09-15

### Fixed
- PXC detection: `wsrep_on` requires `SHOW VARIABLES` (not `SHOW GLOBAL VARIABLES`)

---

## [0.1.4] - 2025-09-10

### Fixed
- `--verbose` flag was not passed through to topology detection

### Changed
- "What It Analyzes" sections expanded by default in README

---

## [0.1.3] - 2025-09-05

### Fixed
- Operation/recommendation/rollback sections no longer shown for unparsable DDL
- Usage text suppressed on command errors

### Added
- Executable gh-ost and pt-osc command generation in recommendations
- Verbose debug logging for PXC detection

---

## [0.1.2] - 2025-08-20

### Added
- dbsafe logo SVG
- Apache 2.0 license
- Column validation to detect DDL failures before execution
- Syntax validation warning for unparsable DDL operations

### Fixed
- PXC cluster detection error handling
- Skip `CREATE USER` recommendation when connecting as root

---

## [0.1.1] - 2025-08-10

### Fixed
- Password flag (`-p`) handling
- Config file loading
- PXC topology detection

---

## [0.1.0] - 2025-08-01

### Added
- Initial release
- DDL analysis: INSTANT/INPLACE/COPY classification across MySQL 8.0 and 8.4 LTS
- DML analysis: DELETE/UPDATE/INSERT with WHERE clause detection and chunked script generation
- Topology detection: Galera/PXC, Group Replication, async replication, standalone
- Output formats: text, plain, JSON, markdown
- GoReleaser config and GitHub Actions release workflow

[0.3.1]: https://github.com/nethalo/dbsafe/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/nethalo/dbsafe/compare/v0.2.8...v0.3.0
[0.2.8]: https://github.com/nethalo/dbsafe/compare/v0.2.7...v0.2.8
[0.2.7]: https://github.com/nethalo/dbsafe/compare/v0.2.6...v0.2.7
[0.2.6]: https://github.com/nethalo/dbsafe/compare/v0.2.5...v0.2.6
[0.2.5]: https://github.com/nethalo/dbsafe/compare/v0.2.4...v0.2.5
[0.2.4]: https://github.com/nethalo/dbsafe/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/nethalo/dbsafe/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/nethalo/dbsafe/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/nethalo/dbsafe/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/nethalo/dbsafe/compare/v0.1.5...v0.2.0
[0.1.5]: https://github.com/nethalo/dbsafe/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/nethalo/dbsafe/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/nethalo/dbsafe/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/nethalo/dbsafe/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/nethalo/dbsafe/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/nethalo/dbsafe/releases/tag/v0.1.0
