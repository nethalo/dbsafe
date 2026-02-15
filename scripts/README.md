# dbsafe Scripts

Helper scripts for development and testing.

## Integration Test Runner

**Location**: `./scripts/run-integration-tests.sh`

Comprehensive script to manage Docker containers and run integration tests.

### Quick Start

```bash
# Run full test suite (start containers, run tests, cleanup)
./scripts/run-integration-tests.sh
```

### Usage

```bash
./scripts/run-integration-tests.sh [OPTIONS]
```

### Options

| Option | Description |
|--------|-------------|
| `-h, --help` | Show help message |
| `-s, --start-only` | Only start containers (don't run tests) |
| `-t, --test-only` | Only run tests (assume containers are running) |
| `-b, --benchmark` | Run benchmarks after tests |
| `-k, --keep` | Keep containers running after tests |
| `-c, --cleanup` | Stop and remove all test containers |
| `--status` | Show container status and exit |

### Examples

**Full test cycle** (recommended for CI/local testing):
```bash
./scripts/run-integration-tests.sh
```

**Start containers for development** (keep them running):
```bash
./scripts/run-integration-tests.sh -s -k
```

**Run tests against running containers** (fast iteration):
```bash
./scripts/run-integration-tests.sh -t
```

**Run tests and benchmarks** (performance validation):
```bash
./scripts/run-integration-tests.sh -b
```

**Check container status**:
```bash
./scripts/run-integration-tests.sh --status
```

**Cleanup when done**:
```bash
./scripts/run-integration-tests.sh -c
```

### What It Does

1. **Checks Docker** - Verifies Docker is installed and running
2. **Starts Containers** - Launches 7 MySQL instances:
   - MySQL 8.0 Standalone (port 13306)
   - MySQL 8.4 LTS (port 13307)
   - Percona Server 8.0 (port 13308)
   - Percona XtraDB Cluster (port 13309)
   - Group Replication Primary (port 13310)
   - Async Replication Primary (port 13311)
   - Async Replication Replica (port 13312)
3. **Waits for Health** - Polls containers until all are healthy (max 2 minutes)
4. **Runs Tests** - Executes `go test -tags=integration ./test -v`
5. **Runs Benchmarks** - (Optional) Performance benchmarking
6. **Cleanup** - Stops and removes containers (unless `-k` flag)

### Development Workflow

**Option 1: Full test each time** (slowest, most reliable)
```bash
./scripts/run-integration-tests.sh
```

**Option 2: Keep containers running** (faster iteration)
```bash
# Start once
./scripts/run-integration-tests.sh -s -k

# Run tests multiple times (fast)
./scripts/run-integration-tests.sh -t
./scripts/run-integration-tests.sh -t
./scripts/run-integration-tests.sh -t

# Cleanup when done
./scripts/run-integration-tests.sh -c
```

**Option 3: Manual control** (maximum flexibility)
```bash
# Start containers
docker-compose -f docker-compose.test.yml up -d

# Run specific tests
go test -tags=integration ./test -v -run TestIntegration_StandaloneMySQL
go test -tags=integration ./test -v -run TestIntegration_MySQLLTS

# Run benchmarks
go test -tags=integration ./test -bench=. -benchmem

# Cleanup
docker-compose -f docker-compose.test.yml down -v
```

### Troubleshooting

**Docker not running:**
```
✗ Docker daemon is not running
Please start Docker Desktop and try again
```
→ Start Docker Desktop application

**Containers timeout:**
```
✗ Timeout waiting for containers to become healthy
```
→ Check Docker resource limits (Docker Desktop → Settings → Resources)
→ Try pulling images manually: `docker pull mysql:8.0 mysql:8.4 percona:8.0`

**Port conflicts:**
```
Error: port 13306 is already allocated
```
→ Stop conflicting containers or change ports in `docker-compose.test.yml`

**Test failures:**
```
✗ Some integration tests failed
```
→ Check container logs: `docker-compose -f docker-compose.test.yml logs`
→ Verify connectivity: `mysql -h 127.0.0.1 -P 13306 -u dbsafe -ptest_password`

### CI/CD Integration

**GitHub Actions example:**
```yaml
- name: Run Integration Tests
  run: |
    chmod +x ./scripts/run-integration-tests.sh
    ./scripts/run-integration-tests.sh
```

**GitLab CI example:**
```yaml
integration-tests:
  services:
    - docker:dind
  script:
    - ./scripts/run-integration-tests.sh
```

### Performance

**Container startup time**: ~30-60 seconds (all 7 containers)
**Test execution time**: ~3-5 seconds
**Cleanup time**: ~5-10 seconds

**Total time (full cycle)**: ~45-75 seconds

Using `-k` (keep running) and `-t` (test-only) reduces iteration time to ~3-5 seconds.

---

## Future Scripts

Additional helper scripts may be added:

- `run-benchmarks.sh` - Run all benchmarks across packages
- `run-fuzz-tests.sh` - Run fuzz tests for extended periods
- `generate-coverage.sh` - Generate comprehensive coverage reports
- `check-security.sh` - Run security-specific tests and checks
