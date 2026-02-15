package mysql

import (
	"strings"
	"testing"
)

func TestEscapeIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "users",
			expected: "`users`",
		},
		{
			name:     "identifier with backtick",
			input:    "user`s",
			expected: "`user``s`",
		},
		{
			name:     "identifier with multiple backticks",
			input:    "a`b`c",
			expected: "`a``b``c`",
		},
		{
			name:     "empty identifier",
			input:    "",
			expected: "``",
		},
		{
			name:     "SQL injection attempt",
			input:    "users`; DROP TABLE users; --",
			expected: "`users``; DROP TABLE users; --`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := escapeIdentifier(tt.input)
			if result != tt.expected {
				t.Errorf("escapeIdentifier(%q) = %q, want %q", tt.input, result, tt.expected)
			}
		})
	}
}

func TestValidateSafeForExplain(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantError bool
	}{
		{
			name:      "valid SELECT",
			sql:       "SELECT * FROM users WHERE id = 1",
			wantError: false,
		},
		{
			name:      "valid UPDATE",
			sql:       "UPDATE users SET name = 'John' WHERE id = 1",
			wantError: false,
		},
		{
			name:      "valid DELETE",
			sql:       "DELETE FROM users WHERE id = 1",
			wantError: false,
		},
		{
			name:      "valid SELECT with subquery",
			sql:       "(SELECT * FROM users)",
			wantError: false,
		},
		{
			name:      "invalid - DROP TABLE",
			sql:       "DROP TABLE users",
			wantError: true,
		},
		{
			name:      "invalid - INSERT",
			sql:       "INSERT INTO users VALUES (1, 'John')",
			wantError: true,
		},
		{
			name:      "invalid - CREATE TABLE",
			sql:       "CREATE TABLE users (id INT)",
			wantError: true,
		},
		{
			name:      "invalid - ALTER TABLE",
			sql:       "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			wantError: true,
		},
		{
			name:      "invalid - semicolon (statement chaining)",
			sql:       "SELECT * FROM users; DROP TABLE users;",
			wantError: true,
		},
		{
			name:      "invalid - GRANT",
			sql:       "GRANT ALL ON *.* TO 'user'@'host'",
			wantError: true,
		},
		{
			name:      "case insensitive - select lowercase",
			sql:       "select * from users",
			wantError: false,
		},
		{
			name:      "with leading whitespace",
			sql:       "  SELECT * FROM users",
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSafeForExplain(tt.sql)
			if tt.wantError && err == nil {
				t.Errorf("validateSafeForExplain(%q) expected error, got nil", tt.sql)
			}
			if !tt.wantError && err != nil {
				t.Errorf("validateSafeForExplain(%q) unexpected error: %v", tt.sql, err)
			}
		})
	}
}

func TestValidateSafeForExplain_InjectionAttempts(t *testing.T) {
	injectionAttempts := []string{
		"SELECT * FROM users; DROP TABLE users; --",
		"SELECT * FROM users WHERE id = 1; DELETE FROM users; --",
		"UPDATE users SET admin = 1; GRANT ALL ON *.* TO 'hacker'@'%'; --",
	}

	for _, sql := range injectionAttempts {
		t.Run("injection_"+sql[:20]+"...", func(t *testing.T) {
			err := validateSafeForExplain(sql)
			if err == nil {
				t.Errorf("validateSafeForExplain should reject injection attempt: %s", sql)
			}
			if !strings.Contains(err.Error(), "semicolon") {
				t.Errorf("expected semicolon error, got: %v", err)
			}
		})
	}
}
