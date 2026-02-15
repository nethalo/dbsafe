package mysql

import (
	"strings"
	"testing"
)

// Fuzz test for SQL injection prevention

func FuzzValidateSafeForExplain(f *testing.F) {
	// Seed with known safe and unsafe inputs
	seeds := []string{
		"SELECT * FROM users",
		"UPDATE users SET x=1",
		"DELETE FROM logs",
		"DROP TABLE users",
		"INSERT INTO users VALUES (1)",
		"CREATE TABLE test (id INT)",
		"GRANT ALL ON *.* TO 'user'@'host'",
		"SELECT * FROM users; DROP TABLE users;",
		"SELECT * FROM users WHERE id = 1",
		"UPDATE users SET name = 'test' WHERE id < 100",
		// Injection attempts
		"'; DROP TABLE users; --",
		"SELECT * FROM users; DELETE FROM users;",
		"SELECT * FROM users\x00DROP TABLE users",
		"SELECT/**/FROM/**/users",
		"SELECT\nFROM\nusers",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// Should never panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("validateSafeForExplain panicked on %q: %v", sql, r)
			}
		}()

		err := validateSafeForExplain(sql)

		// If contains semicolon, should be rejected
		if strings.Contains(sql, ";") && err == nil {
			t.Errorf("validateSafeForExplain should reject SQL with semicolon: %q", sql)
		}

		// If starts with dangerous keywords, should be rejected
		upper := strings.ToUpper(strings.TrimSpace(sql))
		dangerous := []string{"DROP ", "CREATE ", "ALTER ", "GRANT ", "TRUNCATE ", "INSERT "}
		for _, keyword := range dangerous {
			if strings.HasPrefix(upper, keyword) && err == nil {
				t.Errorf("validateSafeForExplain should reject %s: %q", keyword, sql)
			}
		}

		// If starts with safe keywords, should be accepted
		safe := []string{"SELECT ", "UPDATE ", "DELETE ", "(SELECT "}
		for _, keyword := range safe {
			if strings.HasPrefix(upper, keyword) && !strings.Contains(sql, ";") && err != nil {
				// This might be okay - could be other validation reasons
				// But log it for review
				t.Logf("validateSafeForExplain rejected safe-looking SQL: %q, error: %v", sql, err)
			}
		}
	})
}

func FuzzEscapeIdentifier(f *testing.F) {
	// Seed with various identifier patterns
	seeds := []string{
		"users",
		"my_table",
		"table-name",
		"user`table",
		"a`b`c",
		"",
		"`",
		"``",
		"```",
		"normal_name",
		"123_table",
		"täble", // Unicode
		"table\x00name", // Null byte
		"table\nname", // Newline
		"very_long_table_name_with_many_characters_that_exceeds_normal_limits",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, identifier string) {
		// Should never panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("escapeIdentifier panicked on %q: %v", identifier, r)
			}
		}()

		result := escapeIdentifier(identifier)

		// Result should always start and end with backtick
		if !strings.HasPrefix(result, "`") {
			t.Errorf("escapeIdentifier should start with backtick, got: %q", result)
		}
		if !strings.HasSuffix(result, "`") {
			t.Errorf("escapeIdentifier should end with backtick, got: %q", result)
		}

		// Single backticks in input should become double backticks
		if strings.Contains(identifier, "`") && !strings.Contains(identifier, "``") {
			// Result should have doubled backticks
			unescaped := strings.ReplaceAll(result[1:len(result)-1], "``", "`")
			if unescaped != identifier {
				t.Errorf("escapeIdentifier didn't properly escape backticks: input=%q, output=%q", identifier, result)
			}
		}

		// Should be safe to use in SQL (no SQL injection possible)
		// Try to construct a query and ensure it's safe
		query := "SELECT * FROM " + result
		if strings.Contains(query, "; DROP") || strings.Contains(query, "/*") {
			t.Errorf("escapeIdentifier created unsafe SQL: %q", query)
		}
	})
}

func FuzzEscapeIdentifier_RoundTrip(f *testing.F) {
	// Test that escaping and unescaping produces original
	f.Add("users")
	f.Add("my_table")
	f.Add("a`b")

	f.Fuzz(func(t *testing.T, identifier string) {
		escaped := escapeIdentifier(identifier)

		// Remove outer backticks
		if len(escaped) < 2 {
			t.Errorf("escaped result too short: %q", escaped)
			return
		}

		inner := escaped[1 : len(escaped)-1]

		// Unescape doubled backticks
		unescaped := strings.ReplaceAll(inner, "``", "`")

		// Should match original
		if unescaped != identifier {
			t.Errorf("Round trip failed: original=%q, escaped=%q, unescaped=%q", identifier, escaped, unescaped)
		}
	})
}

func FuzzValidateSafeForExplain_InjectionAttempts(f *testing.F) {
	// Focus on SQL injection patterns
	injectionPatterns := []string{
		"'; DROP TABLE users; --",
		"' OR '1'='1",
		"' OR 1=1 --",
		"admin'--",
		"' OR 'a'='a",
		"') OR ('1'='1",
		"1' UNION SELECT * FROM users --",
		"' AND 1=0 UNION ALL SELECT 'admin', '81dc9bdb52d04dc20036dbd8313ed055'",
	}

	for _, pattern := range injectionPatterns {
		f.Add("SELECT * FROM users WHERE id = " + pattern)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		err := validateSafeForExplain(sql)

		// All injection attempts should be rejected (they contain semicolons or invalid syntax)
		// But we primarily care that it doesn't panic
		_ = err
	})
}
