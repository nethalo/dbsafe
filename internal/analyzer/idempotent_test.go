package analyzer

import (
	"strings"
	"testing"

	"github.com/nethalo/dbsafe/internal/parser"
)

func TestGenerateIdempotentSP(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		database    string
		table       string
		wantSP      bool                          // whether we expect a non-empty SP
		wantWarning bool                          // whether we expect a non-empty warning
		checkSP     func(t *testing.T, sp string) // optional extra checks on the SP
	}{
		{
			name:     "ADD COLUMN generates IF NOT EXISTS check on COLUMNS",
			sql:      "ALTER TABLE orders ADD COLUMN email VARCHAR(255)",
			database: "myapp",
			table:    "orders",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "IF NOT EXISTS")
				assertContains(t, sp, "INFORMATION_SCHEMA.COLUMNS")
				assertContains(t, sp, "COLUMN_NAME = 'email'")
				assertContains(t, sp, "TABLE_SCHEMA = 'myapp'")
				assertContains(t, sp, "TABLE_NAME = 'orders'")
				assertContains(t, sp, "DELIMITER //")
				assertContains(t, sp, "CALL `dbsafe_idempotent_myapp_orders`()")
				assertContains(t, sp, "DROP PROCEDURE IF EXISTS `dbsafe_idempotent_myapp_orders`")
			},
		},
		{
			name:     "DROP COLUMN generates IF EXISTS check on COLUMNS",
			sql:      "ALTER TABLE orders DROP COLUMN legacy_field",
			database: "myapp",
			table:    "orders",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "IF EXISTS")
				assertContains(t, sp, "INFORMATION_SCHEMA.COLUMNS")
				assertContains(t, sp, "COLUMN_NAME = 'legacy_field'")
			},
		},
		{
			name:     "MODIFY COLUMN generates IF EXISTS check on COLUMNS",
			sql:      "ALTER TABLE users MODIFY COLUMN name VARCHAR(500)",
			database: "myapp",
			table:    "users",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "INFORMATION_SCHEMA.COLUMNS")
				assertContains(t, sp, "COLUMN_NAME = 'name'")
			},
		},
		{
			name:     "CHANGE COLUMN uses old column name",
			sql:      "ALTER TABLE users CHANGE COLUMN name full_name VARCHAR(500)",
			database: "myapp",
			table:    "users",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "COLUMN_NAME = 'name'")
				assertNotContains(t, sp, "COLUMN_NAME = 'full_name'")
			},
		},
		{
			name:     "ADD INDEX generates IF NOT EXISTS check on STATISTICS",
			sql:      "ALTER TABLE orders ADD INDEX idx_status (status)",
			database: "myapp",
			table:    "orders",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "IF NOT EXISTS")
				assertContains(t, sp, "INFORMATION_SCHEMA.STATISTICS")
				assertContains(t, sp, "INDEX_NAME = 'idx_status'")
			},
		},
		{
			name:     "DROP INDEX generates IF EXISTS check on STATISTICS",
			sql:      "ALTER TABLE orders DROP INDEX idx_status",
			database: "myapp",
			table:    "orders",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "IF EXISTS")
				assertContains(t, sp, "INFORMATION_SCHEMA.STATISTICS")
				assertContains(t, sp, "INDEX_NAME = 'idx_status'")
			},
		},
		{
			name:     "ADD PRIMARY KEY generates IF NOT EXISTS for PRIMARY",
			sql:      "ALTER TABLE orders ADD PRIMARY KEY (id)",
			database: "myapp",
			table:    "orders",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "INDEX_NAME = 'PRIMARY'")
				assertContains(t, sp, "IF NOT EXISTS")
			},
		},
		{
			name:     "DROP PRIMARY KEY generates IF EXISTS for PRIMARY",
			sql:      "ALTER TABLE orders DROP PRIMARY KEY",
			database: "myapp",
			table:    "orders",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "INDEX_NAME = 'PRIMARY'")
				assertContains(t, sp, "IF EXISTS")
			},
		},
		{
			name:     "RENAME INDEX uses old index name",
			sql:      "ALTER TABLE users RENAME INDEX idx_old TO idx_new",
			database: "myapp",
			table:    "users",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "INDEX_NAME = 'idx_old'")
				assertContains(t, sp, "IF EXISTS")
			},
		},
		{
			name:     "ADD FOREIGN KEY generates IF NOT EXISTS on TABLE_CONSTRAINTS",
			sql:      "ALTER TABLE order_items ADD CONSTRAINT fk_order FOREIGN KEY (order_id) REFERENCES orders(id)",
			database: "myapp",
			table:    "order_items",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "INFORMATION_SCHEMA.TABLE_CONSTRAINTS")
				assertContains(t, sp, "CONSTRAINT_NAME = 'fk_order'")
				assertContains(t, sp, "CONSTRAINT_TYPE = 'FOREIGN KEY'")
				assertContains(t, sp, "IF NOT EXISTS")
			},
		},
		{
			name:     "DROP FOREIGN KEY generates IF EXISTS on TABLE_CONSTRAINTS",
			sql:      "ALTER TABLE order_items DROP FOREIGN KEY fk_order",
			database: "myapp",
			table:    "order_items",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "INFORMATION_SCHEMA.TABLE_CONSTRAINTS")
				assertContains(t, sp, "CONSTRAINT_NAME = 'fk_order'")
				assertContains(t, sp, "IF EXISTS")
			},
		},
		{
			name:     "ENGINE change generates IF NOT EXISTS engine check",
			sql:      "ALTER TABLE orders ENGINE=InnoDB",
			database: "myapp",
			table:    "orders",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "INFORMATION_SCHEMA.TABLES")
				assertContains(t, sp, "UPPER(ENGINE) = UPPER('innodb')")
				assertContains(t, sp, "IF NOT EXISTS")
			},
		},
		{
			name:     "RENAME TABLE generates IF EXISTS table check",
			sql:      "ALTER TABLE orders RENAME TO orders_archive",
			database: "myapp",
			table:    "orders",
			wantSP:   true,
			checkSP: func(t *testing.T, sp string) {
				assertContains(t, sp, "INFORMATION_SCHEMA.TABLES")
				assertContains(t, sp, "TABLE_NAME = 'orders'")
				assertContains(t, sp, "IF EXISTS")
			},
		},
		// Unsupported operations
		{
			name:        "MULTIPLE_OPS returns warning",
			sql:         "ALTER TABLE orders ADD COLUMN a INT, ADD COLUMN b INT",
			database:    "myapp",
			table:       "orders",
			wantSP:      false,
			wantWarning: true,
		},
		{
			name:        "CONVERT_CHARSET returns warning",
			sql:         "ALTER TABLE orders CONVERT TO CHARACTER SET utf8mb4",
			database:    "myapp",
			table:       "orders",
			wantSP:      false,
			wantWarning: true,
		},
		{
			name:        "SET_DEFAULT returns warning (metadata-only)",
			sql:         "ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'pending'",
			database:    "myapp",
			table:       "orders",
			wantSP:      false,
			wantWarning: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			sp, warn := GenerateIdempotentSP(parsed, tt.database, tt.table)

			if tt.wantSP && sp == "" {
				t.Errorf("expected non-empty SP, got empty (warning: %q)", warn)
			}
			if !tt.wantSP && sp != "" {
				t.Errorf("expected empty SP, got: %q", sp)
			}
			if tt.wantWarning && warn == "" {
				t.Errorf("expected a warning, got empty")
			}
			if tt.checkSP != nil && sp != "" {
				tt.checkSP(t, sp)
			}
		})
	}
}

func TestGenerateIdempotentSP_ProcName(t *testing.T) {
	parsed, _ := parser.Parse("ALTER TABLE orders ADD COLUMN email VARCHAR(255)")
	sp, _ := GenerateIdempotentSP(parsed, "my-app", "my.table")
	// Hyphens and dots should be replaced with underscores in the proc name
	assertContains(t, sp, "`dbsafe_idempotent_my_app_my_table`")
}

func TestGenerateIdempotentSP_FullStructure(t *testing.T) {
	parsed, _ := parser.Parse("ALTER TABLE orders ADD COLUMN email VARCHAR(255)")
	sp, warn := GenerateIdempotentSP(parsed, "myapp", "orders")
	if warn != "" {
		t.Fatalf("unexpected warning: %q", warn)
	}

	// Verify the SP has all structural elements in order
	elements := []string{
		"DELIMITER //",
		"DROP PROCEDURE IF EXISTS `dbsafe_idempotent_myapp_orders`//",
		"CREATE PROCEDURE `dbsafe_idempotent_myapp_orders`()",
		"BEGIN",
		"IF NOT EXISTS",
		"SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS",
		"COLUMN_NAME = 'email'",
		"THEN",
		"ALTER TABLE orders ADD COLUMN email VARCHAR(255);",
		"END IF;",
		"END//",
		"DELIMITER ;",
		"CALL `dbsafe_idempotent_myapp_orders`();",
		"DROP PROCEDURE IF EXISTS `dbsafe_idempotent_myapp_orders`;",
	}

	for _, el := range elements {
		if !strings.Contains(sp, el) {
			t.Errorf("SP missing expected element: %q\n\nFull SP:\n%s", el, sp)
		}
	}
}

func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("expected to contain %q\n\nActual:\n%s", substr, s)
	}
}

func assertNotContains(t *testing.T, s, substr string) {
	t.Helper()
	if strings.Contains(s, substr) {
		t.Errorf("expected NOT to contain %q\n\nActual:\n%s", substr, s)
	}
}
