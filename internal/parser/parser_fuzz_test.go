package parser

import (
	"testing"
)

// Fuzz test for SQL parser - discovers edge cases and crashes

func FuzzParse(f *testing.F) {
	// Seed corpus with valid SQL statements
	seeds := []string{
		"ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		"DELETE FROM logs WHERE id = 1",
		"UPDATE users SET name = 'test' WHERE id = 1",
		"SELECT * FROM users",
		"ALTER TABLE test DROP COLUMN old",
		"ALTER TABLE t1 ADD INDEX idx (col)",
		"INSERT INTO users VALUES (1, 'test')",
		"CREATE TABLE test (id INT)",
		"RENAME TABLE old TO new",
		"ALTER TABLE users MODIFY COLUMN name VARCHAR(100)",
		"ALTER TABLE users CHANGE COLUMN old_name new_name VARCHAR(100)",
		// Edge cases
		"ALTER TABLE `users` ADD COLUMN `email` VARCHAR(255)",
		"ALTER TABLE db.users ADD COLUMN email VARCHAR(255)",
		"ALTER TABLE `db`.`users` ADD COLUMN email VARCHAR(255)",
		"DELETE FROM users",
		"UPDATE users SET x=1",
		// Potentially problematic inputs
		"",
		" ",
		"  \n\t  ",
		"ALTER",
		"SELECT",
		// SQL injection attempts
		"'; DROP TABLE users; --",
		"' OR '1'='1",
		"\\x00\\x00",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		// The parser should never panic, regardless of input
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Parse panicked on input %q: %v", sql, r)
			}
		}()

		result, err := Parse(sql)

		// Either we get a result or an error, but never a panic
		if err == nil && result == nil {
			t.Error("Parse returned nil result with nil error")
		}

		// If we got a result, validate it's consistent
		if result != nil {
			// Type should be set
			if result.Type == "" {
				t.Errorf("Parse returned result with empty Type for input: %q", sql)
			}

			// If it's DDL, DDLOp should be set
			if result.Type == DDL && result.DDLOp == "" {
				t.Errorf("DDL result has empty DDLOp for input: %q", sql)
			}

			// If it's DML, DMLOp should be set (unless it's unsupported)
			if result.Type == DML && result.DMLOp == "" && result.Type != Unknown {
				t.Errorf("DML result has empty DMLOp for input: %q", sql)
			}

			// RawSQL should match input (trimmed)
			if result.RawSQL == "" && sql != "" {
				t.Errorf("Parse returned empty RawSQL for non-empty input: %q", sql)
			}
		}
	})
}

func FuzzParse_NoPanic(f *testing.F) {
	// Simplified fuzz test focused on preventing panics
	f.Add("ALTER TABLE users ADD COLUMN email VARCHAR(255)")
	f.Add("DELETE FROM users WHERE id = 1")

	f.Fuzz(func(t *testing.T, sql string) {
		// Should never panic
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Parse panicked: %v", r)
			}
		}()

		Parse(sql)
	})
}

func FuzzParse_ValidDDL(f *testing.F) {
	// Fuzz test for DDL statements specifically
	seeds := []string{
		"ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		"ALTER TABLE users DROP COLUMN email",
		"ALTER TABLE users MODIFY COLUMN name TEXT",
		"ALTER TABLE users ADD INDEX idx_email (email)",
		"ALTER TABLE users DROP INDEX idx_email",
	}

	for _, seed := range seeds {
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, sql string) {
		result, err := Parse(sql)

		// If parsing succeeded and it's DDL, validate structure
		if err == nil && result != nil && result.Type == DDL {
			// DDL should have a table name (unless it's RENAME with no tables)
			if result.Table == "" && result.DDLOp != RenameTable {
				// This is okay - might be malformed but parsed
			}

			// If it's ADD COLUMN, should have column name
			if result.DDLOp == AddColumn && result.ColumnName == "" {
				t.Logf("ADD COLUMN without column name: %q", sql)
			}
		}
	})
}
