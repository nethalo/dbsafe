package parser

import (
	"testing"
)

func TestParse_AlterTableAddColumn(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		table      string
		database   string
		ddlOp      DDLOperation
		columnName string
		hasNotNull bool
		hasDefault bool
		firstAfter bool
	}{
		{
			name:       "simple add column",
			sql:        "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			table:      "users",
			ddlOp:      AddColumn,
			columnName: "email",
		},
		{
			name:       "add column not null with default",
			sql:        "ALTER TABLE users ADD COLUMN status INT NOT NULL DEFAULT 0",
			table:      "users",
			ddlOp:      AddColumn,
			columnName: "status",
			hasNotNull: true,
			hasDefault: true,
		},
		{
			name:       "add column with AFTER",
			sql:        "ALTER TABLE users ADD COLUMN middle_name VARCHAR(100) AFTER first_name",
			table:      "users",
			ddlOp:      AddColumn,
			columnName: "middle_name",
			firstAfter: true,
		},
		{
			name:       "add column with FIRST",
			sql:        "ALTER TABLE users ADD COLUMN id BIGINT FIRST",
			table:      "users",
			ddlOp:      AddColumn,
			columnName: "id",
			firstAfter: true,
		},
		{
			name:       "qualified table name",
			sql:        "ALTER TABLE mydb.users ADD COLUMN email VARCHAR(255)",
			table:      "users",
			database:   "mydb",
			ddlOp:      AddColumn,
			columnName: "email",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Type != DDL {
				t.Errorf("Type = %q, want DDL", result.Type)
			}
			if result.Table != tt.table {
				t.Errorf("Table = %q, want %q", result.Table, tt.table)
			}
			if result.Database != tt.database {
				t.Errorf("Database = %q, want %q", result.Database, tt.database)
			}
			if result.DDLOp != tt.ddlOp {
				t.Errorf("DDLOp = %q, want %q", result.DDLOp, tt.ddlOp)
			}
			if result.ColumnName != tt.columnName {
				t.Errorf("ColumnName = %q, want %q", result.ColumnName, tt.columnName)
			}
			if result.HasNotNull != tt.hasNotNull {
				t.Errorf("HasNotNull = %v, want %v", result.HasNotNull, tt.hasNotNull)
			}
			if result.HasDefault != tt.hasDefault {
				t.Errorf("HasDefault = %v, want %v", result.HasDefault, tt.hasDefault)
			}
			if result.IsFirstAfter != tt.firstAfter {
				t.Errorf("IsFirstAfter = %v, want %v", result.IsFirstAfter, tt.firstAfter)
			}
		})
	}
}

func TestParse_AlterTableDropColumn(t *testing.T) {
	result, err := Parse("ALTER TABLE orders DROP COLUMN legacy_field")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Type != DDL {
		t.Errorf("Type = %q, want DDL", result.Type)
	}
	if result.Table != "orders" {
		t.Errorf("Table = %q, want %q", result.Table, "orders")
	}
	if result.DDLOp != DropColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, DropColumn)
	}
	if result.ColumnName != "legacy_field" {
		t.Errorf("ColumnName = %q, want %q", result.ColumnName, "legacy_field")
	}
}

func TestParse_AlterTableModifyColumn(t *testing.T) {
	result, err := Parse("ALTER TABLE users MODIFY COLUMN name VARCHAR(500)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ModifyColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ModifyColumn)
	}
	if result.Table != "users" {
		t.Errorf("Table = %q, want %q", result.Table, "users")
	}
}

func TestParse_AlterTableChangeColumn(t *testing.T) {
	result, err := Parse("ALTER TABLE users CHANGE COLUMN name full_name VARCHAR(500)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ChangeColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ChangeColumn)
	}
}

func TestParse_AlterTableAddIndex(t *testing.T) {
	result, err := Parse("ALTER TABLE events ADD INDEX idx_created (created_at)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Type != DDL {
		t.Errorf("Type = %q, want DDL", result.Type)
	}
	if result.DDLOp != AddIndex {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddIndex)
	}
	if result.IndexName != "idx_created" {
		t.Errorf("IndexName = %q, want %q", result.IndexName, "idx_created")
	}
}

func TestParse_AlterTableDropIndex(t *testing.T) {
	result, err := Parse("ALTER TABLE events DROP INDEX idx_old")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != DropIndex {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, DropIndex)
	}
	if result.IndexName != "idx_old" {
		t.Errorf("IndexName = %q, want %q", result.IndexName, "idx_old")
	}
}

func TestParse_AlterTableAddForeignKey(t *testing.T) {
	result, err := Parse("ALTER TABLE orders ADD CONSTRAINT fk_user FOREIGN KEY (user_id) REFERENCES users(id)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddForeignKey {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddForeignKey)
	}
}

func TestParse_AlterTableChangeCharset(t *testing.T) {
	result, err := Parse("ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ChangeCharset {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ChangeCharset)
	}
}

func TestParse_AlterTableMultipleOps(t *testing.T) {
	result, err := Parse("ALTER TABLE users ADD COLUMN age INT, DROP COLUMN legacy")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != MultipleOps {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, MultipleOps)
	}
	if len(result.DDLOperations) != 2 {
		t.Fatalf("DDLOperations length = %d, want 2", len(result.DDLOperations))
	}
	if result.DDLOperations[0] != AddColumn {
		t.Errorf("DDLOperations[0] = %q, want %q", result.DDLOperations[0], AddColumn)
	}
	if result.DDLOperations[1] != DropColumn {
		t.Errorf("DDLOperations[1] = %q, want %q", result.DDLOperations[1], DropColumn)
	}
}

func TestParse_RenameTable(t *testing.T) {
	result, err := Parse("RENAME TABLE users TO users_backup")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Type != DDL {
		t.Errorf("Type = %q, want DDL", result.Type)
	}
	if result.DDLOp != RenameTable {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, RenameTable)
	}
	if result.Table != "users" {
		t.Errorf("Table = %q, want %q", result.Table, "users")
	}
}

func TestParse_Delete(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		table    string
		hasWhere bool
	}{
		{
			name:     "delete with where",
			sql:      "DELETE FROM logs WHERE created_at < '2023-01-01'",
			table:    "logs",
			hasWhere: true,
		},
		{
			name:     "delete without where",
			sql:      "DELETE FROM temp_data",
			table:    "temp_data",
			hasWhere: false,
		},
		{
			name:     "delete with qualified table",
			sql:      "DELETE FROM mydb.logs WHERE id > 100",
			table:    "logs",
			hasWhere: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Type != DML {
				t.Errorf("Type = %q, want DML", result.Type)
			}
			if result.DMLOp != Delete {
				t.Errorf("DMLOp = %q, want %q", result.DMLOp, Delete)
			}
			if result.Table != tt.table {
				t.Errorf("Table = %q, want %q", result.Table, tt.table)
			}
			if result.HasWhere != tt.hasWhere {
				t.Errorf("HasWhere = %v, want %v", result.HasWhere, tt.hasWhere)
			}
			if tt.hasWhere && result.WhereClause == "" {
				t.Error("WhereClause is empty, expected non-empty")
			}
		})
	}
}

func TestParse_Update(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		table    string
		hasWhere bool
	}{
		{
			name:     "update with where",
			sql:      "UPDATE users SET status = 'inactive' WHERE last_login < '2023-01-01'",
			table:    "users",
			hasWhere: true,
		},
		{
			name:     "update without where",
			sql:      "UPDATE counters SET value = 0",
			table:    "counters",
			hasWhere: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Type != DML {
				t.Errorf("Type = %q, want DML", result.Type)
			}
			if result.DMLOp != Update {
				t.Errorf("DMLOp = %q, want %q", result.DMLOp, Update)
			}
			if result.Table != tt.table {
				t.Errorf("Table = %q, want %q", result.Table, tt.table)
			}
			if result.HasWhere != tt.hasWhere {
				t.Errorf("HasWhere = %v, want %v", result.HasWhere, tt.hasWhere)
			}
		})
	}
}

func TestParse_Insert(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		table    string
		database string
	}{
		{
			name:  "simple insert",
			sql:   "INSERT INTO users (name, email) VALUES ('John', 'john@example.com')",
			table: "users",
		},
		{
			name:     "insert with qualified table",
			sql:      "INSERT INTO mydb.users (name) VALUES ('John')",
			table:    "users",
			database: "mydb",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Type != DML {
				t.Errorf("Type = %q, want DML", result.Type)
			}
			if result.DMLOp != Insert {
				t.Errorf("DMLOp = %q, want %q", result.DMLOp, Insert)
			}
			if result.Table != tt.table {
				t.Errorf("Table = %q, want %q", result.Table, tt.table)
			}
			if result.Database != tt.database {
				t.Errorf("Database = %q, want %q", result.Database, tt.database)
			}
		})
	}
}

func TestParse_UnknownStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{name: "select", sql: "SELECT * FROM users"},
		{name: "show", sql: "SHOW TABLES"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.Type != Unknown {
				t.Errorf("Type = %q, want UNKNOWN", result.Type)
			}
		})
	}
}

func TestParse_InvalidSQL(t *testing.T) {
	_, err := Parse("THIS IS NOT SQL AT ALL")
	if err == nil {
		t.Error("expected error for invalid SQL, got nil")
	}
}

func TestParse_TrailingSemicolon(t *testing.T) {
	result, err := Parse("ALTER TABLE users ADD COLUMN email VARCHAR(255);")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Type != DDL {
		t.Errorf("Type = %q, want DDL", result.Type)
	}
	if result.Table != "users" {
		t.Errorf("Table = %q, want %q", result.Table, "users")
	}
}

func TestParse_WhitespaceHandling(t *testing.T) {
	result, err := Parse("  ALTER TABLE users ADD COLUMN email VARCHAR(255)  ")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Type != DDL {
		t.Errorf("Type = %q, want DDL", result.Type)
	}
}
