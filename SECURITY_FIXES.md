# Security Fixes Applied

This document summarizes the critical security fixes applied to dbsafe.

## Date: 2026-02-15

### 1. SQL Injection in EXPLAIN Query (CRITICAL)

**File:** `internal/mysql/variables.go`
**Issue:** User-provided SQL was concatenated directly to "EXPLAIN " without validation, allowing potential SQL injection.

**Fix:**
- Added `validateSafeForExplain()` function that validates SQL before executing EXPLAIN
- Only allows SELECT, UPDATE, DELETE statements
- Blocks statement chaining (semicolons)
- Rejects dangerous statements (DROP, INSERT, CREATE, ALTER, GRANT, etc.)

**Test Coverage:** `internal/mysql/security_test.go`
- `TestValidateSafeForExplain` - validates allowed/blocked statements
- `TestValidateSafeForExplain_InjectionAttempts` - tests SQL injection attempts

---

### 2. SQL Injection in SHOW CREATE TABLE (CRITICAL)

**File:** `internal/mysql/metadata.go`
**Issue:** Database and table names were directly interpolated into SQL using fmt.Sprintf without proper escaping.

**Fix:**
- Added `escapeIdentifier()` function that properly escapes MySQL identifiers
- Escapes backticks by doubling them (`` ` `` becomes `` `` ``)
- Wraps identifiers in backticks to prevent keyword conflicts

**Example:**
```go
// Before (vulnerable):
fmt.Sprintf("SHOW CREATE TABLE `%s`.`%s`", database, table)

// After (secure):
fmt.Sprintf("SHOW CREATE TABLE %s.%s", escapeIdentifier(database), escapeIdentifier(table))
```

**Test Coverage:** `internal/mysql/security_test.go`
- `TestEscapeIdentifier` - validates proper escaping of various inputs

---

### 3. Insecure File Permissions on Generated Scripts (MEDIUM)

**File:** `cmd/plan.go`
**Issue:** Generated SQL scripts were created with 0644 permissions (world-readable), potentially exposing sensitive database information.

**Fix:**
- Changed file permissions from `0644` to `0600` (owner read/write only)
- Added user feedback when script is written with secure permissions

**Before:**
```go
os.WriteFile(scriptPath, []byte(result.GeneratedScript), 0644)
```

**After:**
```go
os.WriteFile(scriptPath, []byte(result.GeneratedScript), 0600)
```

---

### 4. Path Traversal in --file Parameter (MEDIUM)

**File:** `cmd/plan.go`
**Issue:** The `--file` flag accepted any path without validation, allowing users to read arbitrary files like `/etc/passwd`.

**Fix:**
- Added `validateSQLFilePath()` function that:
  - Cleans paths to resolve `..` and `.` components
  - Verifies file exists and is a regular file (not directory, device, symlink)
  - Blocks files larger than 10MB (likely not SQL files)
  - Warns when reading from sensitive system paths (`/etc/`, `/sys/`, `/proc/`, `/dev/`)

**Test Coverage:** `cmd/security_test.go`
- `TestValidateSQLFilePath` - validates file path validation logic
- `TestValidateSQLFilePath_PathTraversal` - tests path traversal attempts
- `TestValidateSQLFilePath_CleanPath` - validates path cleaning

---

## Defense in Depth Strategy

These fixes implement defense-in-depth:

1. **Input Validation:** All user input (SQL, file paths, identifiers) is validated before use
2. **Escaping:** Identifiers are properly escaped before interpolation
3. **Least Privilege:** Generated files use restrictive permissions
4. **Whitelisting:** Only safe SQL statement types are allowed for EXPLAIN

## Testing

All security fixes include comprehensive test coverage:

```bash
# Run security tests
go test ./internal/mysql -run Test.*Security -v
go test ./internal/mysql -run TestEscape -v
go test ./internal/mysql -run TestValidateSafe -v
go test ./cmd -run TestValidateSQLFile -v
```

## Security Best Practices Going Forward

1. **Never concatenate user input into SQL** - Always use parameterized queries or proper escaping
2. **Validate all file paths** - Don't trust user-provided paths
3. **Use restrictive file permissions** - Default to 0600 for sensitive files
4. **Test injection vectors** - Always include security tests for input validation
5. **Defense in depth** - Implement multiple layers of security (parser + validation)

## Related Issues

- See CLAUDE.md for architecture notes on security
- All security tests are in `*_test.go` files alongside the fixed code
- Run full test suite with `make test`
