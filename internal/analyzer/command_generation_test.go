package analyzer

import (
	"strings"
	"testing"

	"github.com/nethalo/dbsafe/internal/parser"
)

func TestExtractAlterSpec(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple ADD COLUMN",
			sql:      "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			expected: "ADD COLUMN email VARCHAR(255)",
		},
		{
			name:     "qualified table name",
			sql:      "ALTER TABLE mydb.users ADD COLUMN email VARCHAR(255)",
			expected: "ADD COLUMN email VARCHAR(255)",
		},
		{
			name:     "backtick quoted table",
			sql:      "ALTER TABLE `users` DROP COLUMN temp",
			expected: "DROP COLUMN temp",
		},
		{
			name:     "backtick quoted database and table",
			sql:      "ALTER TABLE `mydb`.`users` MODIFY COLUMN name VARCHAR(100)",
			expected: "MODIFY COLUMN name VARCHAR(100)",
		},
		{
			name:     "with extra whitespace",
			sql:      "  ALTER TABLE   users   ADD INDEX idx_email (email)  ",
			expected: "ADD INDEX idx_email (email)",
		},
		{
			name:     "not ALTER TABLE",
			sql:      "CREATE TABLE users (id INT)",
			expected: "CREATE TABLE users (id INT)",
		},
		{
			name:     "empty string",
			sql:      "",
			expected: "",
		},
		{
			name:     "lowercase alter table",
			sql:      "alter table users add column email varchar(255)",
			expected: "add column email varchar(255)",
		},
		{
			name:     "complex ALTER with multiple operations",
			sql:      "ALTER TABLE users ADD COLUMN email VARCHAR(255), ADD INDEX idx_email (email)",
			expected: "ADD COLUMN email VARCHAR(255), ADD INDEX idx_email (email)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractAlterSpec(tt.sql)
			if result != tt.expected {
				t.Errorf("extractAlterSpec(%q) = %q, want %q", tt.sql, result, tt.expected)
			}
		})
	}
}

func TestGenerateGhostCommand(t *testing.T) {
	tests := []struct {
		name          string
		input         Input
		wantContains  []string
		wantNotEmpty  bool
	}{
		{
			name: "basic ALTER with TCP connection",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.AddColumn,
					RawSQL:   "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
					Database: "mydb",
					Table:    "users",
				},
				Connection: &ConnectionInfo{
					Host: "localhost",
					Port: 3306,
					User: "dbuser",
				},
			},
			wantContains: []string{
				"gh-ost",
				"--user=\"dbuser\"",
				"--host=\"localhost\"",
				"--port=3306",
				"--database=\"mydb\"",
				"--table=\"users\"",
				"--alter=\"ADD COLUMN email VARCHAR(255)\"",
				"--assume-rbr",
				"--execute",
			},
			wantNotEmpty: true,
		},
		{
			name: "ALTER with socket connection",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.DropColumn,
					RawSQL:   "ALTER TABLE test DROP COLUMN old_col",
					Database: "testdb",
					Table:    "test",
				},
				Connection: &ConnectionInfo{
					User:   "testuser",
					Socket: "/var/run/mysqld/mysqld.sock",
				},
			},
			wantContains: []string{
				"gh-ost",
				"--socket=\"/var/run/mysqld/mysqld.sock\"",
				"--user=\"testuser\"",
				"--database=\"testdb\"",
				"--table=\"test\"",
				"--alter=\"DROP COLUMN old_col\"",
			},
			wantNotEmpty: true,
		},
		{
			name: "no connection info",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:   parser.DDL,
					DDLOp:  parser.AddIndex,
					RawSQL: "ALTER TABLE users ADD INDEX idx_email (email)",
				},
				Connection: nil,
			},
			wantNotEmpty: false,
		},
		{
			name: "ALTER with backticked table",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.AddColumn,
					RawSQL:   "ALTER TABLE `my-table` ADD COLUMN status INT",
					Database: "mydb",
					Table:    "my-table",
				},
				Connection: &ConnectionInfo{
					Host: "db.example.com",
					Port: 3307,
					User: "admin",
				},
			},
			wantContains: []string{
				"--host=\"db.example.com\"",
				"--port=3307",
				"--database=\"mydb\"",
				"--table=\"my-table\"",
				"--alter=\"ADD COLUMN status INT\"",
			},
			wantNotEmpty: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generateGhostCommand(tt.input)

			if tt.wantNotEmpty && result == "" {
				t.Error("expected non-empty result, got empty string")
			}

			if !tt.wantNotEmpty && result != "" {
				t.Errorf("expected empty result, got: %s", result)
			}

			for _, want := range tt.wantContains {
				if !strings.Contains(result, want) {
					t.Errorf("result should contain %q, got:\n%s", want, result)
				}
			}

			// Verify it's a valid shell command structure
			if tt.wantNotEmpty {
				if !strings.HasPrefix(result, "gh-ost") {
					t.Errorf("command should start with 'gh-ost', got: %s", result)
				}
				if !strings.Contains(result, "--execute") {
					t.Error("command should contain --execute flag")
				}
			}
		})
	}
}

func TestGeneratePtOSCCommand(t *testing.T) {
	tests := []struct {
		name         string
		input        Input
		isGalera     bool
		wantContains []string
		wantNotEmpty bool
	}{
		{
			name: "basic ALTER with TCP connection",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.AddColumn,
					RawSQL:   "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
					Database: "mydb",
					Table:    "users",
				},
				Connection: &ConnectionInfo{
					Host: "localhost",
					Port: 3306,
					User: "dbuser",
				},
			},
			isGalera: false,
			wantContains: []string{
				"pt-online-schema-change",
				"h=localhost,P=3306",
				"u=dbuser",
				"D=mydb,t=users",
				"--alter \"ADD COLUMN email VARCHAR(255)\"",
				"--execute",
				"--chunk-size=1000",
				"--preserve-triggers",
			},
			wantNotEmpty: true,
		},
		{
			name: "ALTER with socket connection",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.DropIndex,
					RawSQL:   "ALTER TABLE test DROP INDEX old_idx",
					Database: "testdb",
					Table:    "test",
				},
				Connection: &ConnectionInfo{
					User:   "testuser",
					Socket: "/tmp/mysql.sock",
				},
			},
			isGalera: false,
			wantContains: []string{
				"pt-online-schema-change",
				"S=/tmp/mysql.sock",
				"u=testuser",
				"D=testdb,t=test",
				"--alter \"DROP INDEX old_idx\"",
			},
			wantNotEmpty: true,
		},
		{
			name: "Galera cluster with special flags",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.AddColumn,
					RawSQL:   "ALTER TABLE users ADD COLUMN status INT",
					Database: "prod",
					Table:    "users",
				},
				Connection: &ConnectionInfo{
					Host: "galera-node-1",
					Port: 3306,
					User: "admin",
				},
			},
			isGalera: true,
			wantContains: []string{
				"pt-online-schema-change",
				"--max-flow-ctl=0.5",
				"--check-plan",
			},
			wantNotEmpty: true,
		},
		{
			name: "no connection info",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:   parser.DDL,
					DDLOp:  parser.AddIndex,
					RawSQL: "ALTER TABLE users ADD INDEX idx_email (email)",
				},
				Connection: nil,
			},
			isGalera:     false,
			wantNotEmpty: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := generatePtOSCCommand(tt.input, tt.isGalera)

			if tt.wantNotEmpty && result == "" {
				t.Error("expected non-empty result, got empty string")
			}

			if !tt.wantNotEmpty && result != "" {
				t.Errorf("expected empty result, got: %s", result)
			}

			for _, want := range tt.wantContains {
				if !strings.Contains(result, want) {
					t.Errorf("result should contain %q, got:\n%s", want, result)
				}
			}

			// Verify Galera-specific flags
			if tt.isGalera {
				if !strings.Contains(result, "--max-flow-ctl") {
					t.Error("Galera command should contain --max-flow-ctl")
				}
				if !strings.Contains(result, "--check-plan") {
					t.Error("Galera command should contain --check-plan")
				}
			}

			// Verify command structure
			if tt.wantNotEmpty {
				if !strings.HasPrefix(result, "pt-online-schema-change") {
					t.Errorf("command should start with 'pt-online-schema-change', got: %s", result)
				}
			}
		})
	}
}

func TestGenerateGhostCommand_EdgeCases(t *testing.T) {
	// Test with special characters in ALTER spec
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:     parser.DDL,
			DDLOp:    parser.AddColumn,
			RawSQL:   "ALTER TABLE users ADD COLUMN `data-field` VARCHAR(255) DEFAULT 'test'",
			Database: "mydb",
			Table:    "users",
		},
		Connection: &ConnectionInfo{
			Host: "localhost",
			Port: 3306,
			User: "user",
		},
	}

	result := generateGhostCommand(input)

	if !strings.Contains(result, "--alter=") {
		t.Error("should contain --alter flag")
	}

	// Verify special characters are preserved
	if !strings.Contains(result, "data-field") {
		t.Error("should preserve column name with special characters")
	}
}

func TestGeneratePtOSCCommand_DSNFormat(t *testing.T) {
	// Test DSN format is correct
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:     parser.DDL,
			DDLOp:    parser.AddColumn,
			RawSQL:   "ALTER TABLE test ADD COLUMN col INT",
			Database: "db",
			Table:    "test",
		},
		Connection: &ConnectionInfo{
			Host: "host",
			Port: 3307,
			User: "user",
		},
	}

	result := generatePtOSCCommand(input, false)

	// DSN should be in format: h=host,P=port,u=user,D=db,t=table
	expectedParts := []string{"h=host", "P=3307", "u=user", "D=db", "t=test"}
	for _, part := range expectedParts {
		if !strings.Contains(result, part) {
			t.Errorf("DSN should contain %q, got: %s", part, result)
		}
	}
}
