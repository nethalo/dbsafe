# Testing Guide

Complete guide to testing dbsafe - from quick unit tests to comprehensive integration testing.

## ✅ Status: All Tests Passing

**Last verified**: 2026-02-15

- ✅ **Unit Tests**: All passing (96.6% parser, 94.9% analyzer, 93.0% topology, 85.5% mysql)
- ✅ **Integration Tests**: Verified working with real MySQL 8.0 containers
- ✅ **Benchmarks**: 28 benchmarks tracking performance
- ✅ **Fuzz Tests**: 7 fuzz functions with 50+ seed cases

**Integration test results**:
```
✓ TestIntegration_StandaloneMySQL (0.25s)
  - Topology detection: PASS
  - Version parsing: PASS
  - Metadata collection: PASS
  - DDL/DML analysis: PASS

✓ TestIntegration_DDLClassification (0.01s)
  - INSTANT algorithm detection: PASS
  - INPLACE algorithm detection: PASS
  - COPY algorithm detection: PASS
```

---

## Quick Reference

```bash
# Unit tests (fast, ~2 seconds)
go test ./...

# Unit tests with coverage
go test -cover ./...

# Integration tests (full suite, ~60 seconds)
./scripts/run-integration-tests.sh

# Benchmarks
go test -bench=. -benchmem ./internal/...

# Fuzz tests (run for 30 seconds)
go test -fuzz=FuzzParse -fuzztime=30s ./internal/parser
```

---

## Test Categories

### 1. Unit Tests ✅

**What they test**: Individual functions and modules in isolation

**How to run**:
```bash
# All unit tests
go test ./...

# Specific package
go test ./internal/analyzer
go test ./internal/parser

# Specific test
go test ./internal/analyzer -run TestAnalyze_AddColumn

# With coverage
go test -cover ./...

# Coverage report (HTML)
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

**Speed**: Fast (~2 seconds for all packages)

**When to run**:
- Before every commit
- During development (continuous)
- In pre-commit hooks

### 2. Integration Tests 🐳

**What they test**: End-to-end functionality with real MySQL instances

**How to run**:

**Using the helper script** (recommended):
```bash
# Full test cycle
./scripts/run-integration-tests.sh

# Keep containers running for faster iteration
./scripts/run-integration-tests.sh -s -k  # Start once
./scripts/run-integration-tests.sh -t     # Run tests (fast)
./scripts/run-integration-tests.sh -c     # Cleanup when done
```

**Manual approach**:
```bash
# Start containers
docker-compose -f docker-compose.test.yml up -d

# Wait for healthy (check status)
docker-compose -f docker-compose.test.yml ps

# Run tests
go test -tags=integration ./test -v

# Cleanup
docker-compose -f docker-compose.test.yml down -v
```

**Speed**: Moderate (~60 seconds including container startup)

**When to run**:
- Before merging PRs
- After major changes
- Before releases
- In CI/CD pipelines

**What's tested**:
- ✅ Topology detection (Standalone, Galera, Group Replication, etc.)
- ✅ Version detection (including LTS versions)
- ✅ Metadata collection from real tables
- ✅ DDL algorithm classification (INSTANT/INPLACE/COPY)
- ✅ End-to-end analysis workflows

**MySQL topologies covered**:
1. MySQL 8.0 Standalone
2. MySQL 8.4 LTS
3. Percona Server 8.0
4. Percona XtraDB Cluster (Galera)
5. Group Replication
6. Async Replication (Primary + Replica)

### 3. Benchmarks ⚡

**What they test**: Performance characteristics and resource usage

**How to run**:
```bash
# All benchmarks
go test -bench=. -benchmem ./internal/...

# Specific package
go test -bench=. -benchmem ./internal/parser
go test -bench=. -benchmem ./internal/analyzer

# Specific benchmark
go test -bench=BenchmarkParse_SimpleSelect -benchmem ./internal/parser

# Run multiple times for accuracy
go test -bench=. -benchmem -benchtime=10s ./internal/...

# Compare results
go test -bench=. -benchmem ./internal/parser | tee old.txt
# (make changes)
go test -bench=. -benchmem ./internal/parser | tee new.txt
benchstat old.txt new.txt
```

**Speed**: Moderate (varies by benchmark)

**When to run**:
- Before performance-critical changes
- To establish baselines
- To detect regressions
- During optimization work

**Available benchmarks**:
- Parser: SQL parsing performance (5 benchmarks)
- Analyzer: DDL/DML analysis (12 benchmarks)
- Output: Rendering performance (11 benchmarks)
- Integration: Metadata collection (1 benchmark)

### 4. Fuzz Tests 🔍

**What they test**: Edge cases, crashes, and security with random inputs

**How to run**:
```bash
# Parser fuzzing (30 seconds)
go test -fuzz=FuzzParse -fuzztime=30s ./internal/parser

# Security validation fuzzing
go test -fuzz=FuzzValidateSafeForExplain -fuzztime=30s ./internal/mysql

# Identifier escaping fuzzing
go test -fuzz=FuzzEscapeIdentifier -fuzztime=30s ./internal/mysql

# Run all fuzz tests (5 seconds each, for quick validation)
go test -fuzz=. -fuzztime=5s ./internal/parser
go test -fuzz=. -fuzztime=5s ./internal/mysql

# Extended fuzzing (overnight)
go test -fuzz=FuzzParse -fuzztime=8h ./internal/parser
```

**Speed**: Variable (user-controlled)

**When to run**:
- Before security-sensitive changes
- Periodically (automated overnight runs)
- After parser modifications
- Before major releases

**What's tested**:
- ✅ Parser robustness (handles malformed SQL)
- ✅ SQL injection prevention
- ✅ Identifier escaping correctness
- ✅ No panics on any input

**Seed cases include**:
- Valid SQL statements
- SQL injection attempts (`'; DROP TABLE users; --`)
- Edge cases (empty strings, unicode, null bytes)
- Malformed syntax

---

## Coverage Reports

### View Current Coverage

```bash
# Quick overview
go test -cover ./...

# Detailed by package
go test -cover ./internal/analyzer
go test -cover ./internal/parser
go test -cover ./internal/mysql
go test -cover ./internal/output
go test -cover ./internal/topology

# Generate HTML report
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out
```

### Current Coverage (as of 2026-02-15)

| Package | Coverage | Status |
|---------|----------|--------|
| internal/analyzer | 94.9% | ✅ Excellent |
| internal/parser | 96.6% | ✅ Excellent |
| internal/topology | 93.0% | ✅ Excellent |
| internal/mysql | 85.5% | ✅ Good |
| internal/output | 79.3% | ⚠️ Good |

---

## CI/CD Integration

### GitHub Actions

```yaml
name: Tests

on: [push, pull_request]

jobs:
  unit-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      - name: Run unit tests
        run: go test -v -cover ./...

  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      - name: Run integration tests
        run: ./scripts/run-integration-tests.sh

  benchmarks:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-go@v4
        with:
          go-version: '1.21'
      - name: Run benchmarks
        run: go test -bench=. -benchmem ./internal/... | tee benchmark.txt
      - name: Upload results
        uses: actions/upload-artifact@v3
        with:
          name: benchmark-results
          path: benchmark.txt
```

### Pre-commit Hook

Create `.git/hooks/pre-commit`:

```bash
#!/bin/bash
set -e

echo "Running unit tests..."
go test ./...

echo "Checking test coverage..."
go test -cover ./... | grep -v "no test files"

echo "All checks passed!"
```

Make it executable:
```bash
chmod +x .git/hooks/pre-commit
```

---

## Troubleshooting

### Unit Tests

**Problem**: Tests fail with import errors
```
package X is not in GOROOT
```
**Solution**: Run `go mod download` and `go mod tidy`

**Problem**: Tests hang or timeout
```
panic: test timed out after 10m0s
```
**Solution**: Check for deadlocks, increase timeout with `-timeout 30m`

### Integration Tests

**Problem**: Docker not running
```
Cannot connect to the Docker daemon
```
**Solution**: Start Docker Desktop

**Problem**: Containers fail to start
```
Error: port 13306 is already allocated
```
**Solution**: Stop conflicting containers or change ports in `docker-compose.test.yml`

**Problem**: Containers not healthy
```
Timeout waiting for containers to become healthy
```
**Solution**:
- Check Docker resource limits
- Pull images manually: `docker pull mysql:8.0 mysql:8.4`
- Check logs: `docker-compose -f docker-compose.test.yml logs`

**Problem**: Connection refused in tests
```
dial tcp 127.0.0.1:13306: connect: connection refused
```
**Solution**: Wait longer for containers to be ready, or increase wait time in test

**Problem**: `percona` container exits immediately (code 1) on Apple Silicon
```
percona  exited (1)
```
**Cause**: Percona Server 8.0 does not publish ARM64 images. The `linux/amd64` image crashes under Rosetta 2 emulation on Apple Silicon Macs.
**Solution**: The `percona` container and its integration tests are expected to be unavailable on ARM64 hosts. Tests that target it will skip automatically. This container works correctly on x86-64 (amd64) Linux hosts and in CI.

### Fuzz Tests

**Problem**: Crashes not reproducible
```
Found crash but can't reproduce
```
**Solution**: Check `testdata/fuzz/` directory for crash corpus, use `-run` with specific input

**Problem**: Slow fuzzing
```
Only 10 iterations per second
```
**Solution**: This is normal for complex functions; use `-parallel` flag to increase throughput

---

## Best Practices

### Development Workflow

1. **Write failing test first** (TDD)
   ```bash
   go test ./internal/analyzer -run TestNewFeature  # Fails
   # Implement feature
   go test ./internal/analyzer -run TestNewFeature  # Passes
   ```

2. **Run unit tests continuously** during development
   ```bash
   # Use watch tool
   go install github.com/cespare/reflex@latest
   reflex -r '\.go$' go test ./...
   ```

3. **Run integration tests before commits**
   ```bash
   ./scripts/run-integration-tests.sh
   ```

4. **Check coverage** for new code
   ```bash
   go test -cover ./internal/analyzer
   ```

5. **Run benchmarks** for performance-sensitive changes
   ```bash
   go test -bench=. -benchmem ./internal/parser
   ```

### Writing Good Tests

**Unit tests**:
- ✅ Use table-driven tests for multiple cases
- ✅ Test edge cases (empty, nil, max values)
- ✅ Test error conditions
- ✅ Keep tests focused (one thing per test)
- ✅ Use descriptive test names

**Integration tests**:
- ✅ Use realistic data
- ✅ Test end-to-end workflows
- ✅ Clean up after tests
- ✅ Make tests independent (can run in any order)

**Benchmarks**:
- ✅ Use `b.ReportAllocs()` to track allocations
- ✅ Use `b.ResetTimer()` after setup
- ✅ Run with `-benchmem` for memory stats

**Fuzz tests**:
- ✅ Provide good seed corpus
- ✅ Check for panics (never panic on any input)
- ✅ Verify invariants hold for all inputs
- ✅ Use fuzz tests to complement unit tests, not replace them

---

## Additional Resources

- **Go Testing Documentation**: https://go.dev/doc/tutorial/fuzz
- **Table-Driven Tests**: https://go.dev/wiki/TableDrivenTests
- **Benchmarking**: https://pkg.go.dev/testing#hdr-Benchmarks
- **Coverage Tools**: https://go.dev/blog/cover

---

For more details on the integration test script, see `scripts/README.md`.
