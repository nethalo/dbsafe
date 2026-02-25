package mysql

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// ServerVersion represents a parsed MySQL version.
type ServerVersion struct {
	Raw           string // e.g. "8.0.35-27-Percona XtraDB Cluster"
	Major         int    // 8
	Minor         int    // 0
	Patch         int    // 35 (0 for Aurora)
	Flavor        string // "mysql", "percona", "percona-xtradb-cluster", "aurora-mysql"
	IsLTS         bool   // true for 8.4.x
	AuroraVersion string // e.g., "3.04.0" (empty for non-Aurora)
}

// String returns a human-readable version string.
func (v ServerVersion) String() string {
	if v.AuroraVersion != "" {
		return fmt.Sprintf("%d.%d (aurora-mysql %s)", v.Major, v.Minor, v.AuroraVersion)
	}
	return fmt.Sprintf("%d.%d.%d (%s)", v.Major, v.Minor, v.Patch, v.Flavor)
}

// IsAurora returns true if this is an Aurora MySQL instance.
func (v ServerVersion) IsAurora() bool {
	return v.Flavor == "aurora-mysql"
}

// EffectivePatch returns the MySQL-compatible patch version for DDL matrix lookups.
// Aurora 3.x (MySQL 8.0 compat) is based on MySQL 8.0.23, so we use 23 as the
// effective patch level for algorithm classification.
func (v ServerVersion) EffectivePatch() int {
	if v.IsAurora() && v.Major == 8 && v.Minor == 0 {
		return 23
	}
	return v.Patch
}

// AtLeast returns true if the server version is >= the given version.
func (v ServerVersion) AtLeast(major, minor, patch int) bool {
	if v.Major != major {
		return v.Major > major
	}
	if v.Minor != minor {
		return v.Minor > minor
	}
	return v.Patch >= patch
}

// SupportsInstantAddColumn returns true if INSTANT ADD COLUMN (trailing) is supported.
// MySQL 8.0.12+
func (v ServerVersion) SupportsInstantAddColumn() bool {
	return v.AtLeast(8, 0, 12)
}

// SupportsInstantAnyPosition returns true if INSTANT for ADD/DROP COLUMN in any position.
// MySQL 8.0.29+
func (v ServerVersion) SupportsInstantAnyPosition() bool {
	return v.AtLeast(8, 0, 29)
}

// SupportsInstantDropColumn returns true if INSTANT DROP COLUMN is supported.
// MySQL 8.0.29+
func (v ServerVersion) SupportsInstantDropColumn() bool {
	return v.AtLeast(8, 0, 29)
}

// GetServerVersion queries and parses the MySQL server version.
func GetServerVersion(db *sql.DB) (ServerVersion, error) {
	var raw string
	err := db.QueryRow("SELECT VERSION()").Scan(&raw)
	if err != nil {
		return ServerVersion{}, fmt.Errorf("querying version: %w", err)
	}
	return ParseVersion(raw)
}

// ParseVersion parses a MySQL version string.
func ParseVersion(raw string) (ServerVersion, error) {
	v := ServerVersion{Raw: raw}

	// Check for Aurora MySQL first (e.g., "8.0.mysql_aurora.3.04.0")
	// Must be checked before the general regex since Aurora versions don't have a numeric patch.
	auroraRe := regexp.MustCompile(`^(\d+)\.(\d+)\.mysql_aurora\.(\d+\.\d+\.\d+)`)
	if m := auroraRe.FindStringSubmatch(raw); len(m) >= 4 {
		v.Major, _ = strconv.Atoi(m[1])
		v.Minor, _ = strconv.Atoi(m[2])
		v.Patch = 0 // Aurora doesn't map to MySQL patch versions
		v.Flavor = "aurora-mysql"
		v.AuroraVersion = m[3]
		v.IsLTS = false // Aurora doesn't follow MySQL LTS schedule
		return v, nil
	}

	// Extract major.minor.patch from the beginning
	re := regexp.MustCompile(`^(\d+)\.(\d+)\.(\d+)`)
	matches := re.FindStringSubmatch(raw)
	if len(matches) < 4 {
		return v, fmt.Errorf("could not parse version: %s", raw)
	}

	v.Major, _ = strconv.Atoi(matches[1])
	v.Minor, _ = strconv.Atoi(matches[2])
	v.Patch, _ = strconv.Atoi(matches[3])

	// Detect flavor
	lower := strings.ToLower(raw)
	switch {
	case strings.Contains(lower, "percona xtradb cluster"):
		v.Flavor = "percona-xtradb-cluster"
	case strings.Contains(lower, "percona"):
		v.Flavor = "percona"
	case strings.Contains(lower, "mariadb"):
		v.Flavor = "mariadb" // not supported, but detect it
	default:
		v.Flavor = "mysql"
	}

	// 8.4.x is LTS
	v.IsLTS = v.Major == 8 && v.Minor == 4

	return v, nil
}

// GetVariable reads a single MySQL variable.
// Returns the value, or empty string if variable doesn't exist.
// Note: Some variables (like wsrep_on) require SHOW VARIABLES without GLOBAL.
func GetVariable(db *sql.DB, name string) (string, error) {
	var varName, value sql.NullString

	// Escape the variable name for LIKE clause (prevent SQL injection)
	escapedName := strings.ReplaceAll(name, "_", "\\_")
	escapedName = strings.ReplaceAll(escapedName, "%", "\\%")

	// Try with GLOBAL first (most variables)
	// Note: SHOW commands don't support prepared statements in all MySQL drivers
	query := fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", escapedName)
	err := db.QueryRow(query).Scan(&varName, &value)
	if err == nil && value.Valid && value.String != "" {
		return value.String, nil
	}

	// If GLOBAL didn't work, try without GLOBAL (needed for some wsrep variables)
	query = fmt.Sprintf("SHOW VARIABLES LIKE '%s'", escapedName)
	err = db.QueryRow(query).Scan(&varName, &value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil // variable doesn't exist
		}
		return "", fmt.Errorf("query failed: %w", err)
	}

	// Check if value is NULL
	if !value.Valid {
		return "", nil
	}

	return value.String, nil
}

// GetStatus reads a single MySQL global status variable.
func GetStatus(db *sql.DB, name string) (string, error) {
	var varName, value string

	// Escape the variable name for LIKE clause
	escapedName := strings.ReplaceAll(name, "_", "\\_")
	escapedName = strings.ReplaceAll(escapedName, "%", "\\%")

	// Note: SHOW commands don't support prepared statements in all MySQL drivers
	query := fmt.Sprintf("SHOW GLOBAL STATUS LIKE '%s'", escapedName)
	err := db.QueryRow(query).Scan(&varName, &value)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}
	return value, nil
}

// GetVariableInt reads a MySQL variable and returns it as int64.
func GetVariableInt(db *sql.DB, name string) (int64, error) {
	val, err := GetVariable(db, name)
	if err != nil || val == "" {
		return 0, err
	}
	return strconv.ParseInt(val, 10, 64)
}

// validateSafeForExplain checks if SQL is safe to use with EXPLAIN.
// This prevents SQL injection by ensuring only SELECT/UPDATE/DELETE statements are explained.
func validateSafeForExplain(sqlText string) error {
	sqlText = strings.TrimSpace(sqlText)
	upper := strings.ToUpper(sqlText)

	// Only allow SELECT, UPDATE, DELETE statements
	// Reject: DROP, INSERT, CREATE, ALTER, GRANT, etc.
	allowed := false
	for _, prefix := range []string{"SELECT ", "UPDATE ", "DELETE ", "(SELECT "} {
		if strings.HasPrefix(upper, prefix) {
			allowed = true
			break
		}
	}

	if !allowed {
		return fmt.Errorf("SQL statement not safe for EXPLAIN: must be SELECT, UPDATE, or DELETE")
	}

	// Additional check: ensure no semicolons (prevents statement chaining)
	if strings.Contains(sqlText, ";") {
		return fmt.Errorf("SQL statement contains semicolon: statement chaining not allowed")
	}

	return nil
}

// EstimateRowsAffected runs EXPLAIN on a DML statement to get row estimate.
// Note: This function validates the SQL is a safe DML statement before executing EXPLAIN.
func EstimateRowsAffected(db *sql.DB, sqlText string) (int64, error) {
	// Security: Validate that this is a safe SQL statement before using EXPLAIN
	// Even though the parser has already validated this, we add defense-in-depth
	// to prevent SQL injection if this function is ever called with untrusted input.
	if err := validateSafeForExplain(sqlText); err != nil {
		return 0, err
	}

	rows, err := db.Query("EXPLAIN " + sqlText)
	if err != nil {
		return 0, fmt.Errorf("EXPLAIN failed: %w", err)
	}
	defer rows.Close()

	// EXPLAIN output has variable columns depending on format
	// We need the 'rows' column (position 9 in traditional EXPLAIN)
	cols, _ := rows.Columns()
	var maxRows int64

	for rows.Next() {
		values := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			continue
		}

		// Find the "rows" column
		for i, col := range cols {
			if strings.ToLower(col) == "rows" && values[i].Valid {
				n, _ := strconv.ParseInt(values[i].String, 10, 64)
				if n > maxRows {
					maxRows = n
				}
			}
		}
	}

	return maxRows, nil
}
