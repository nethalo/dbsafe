package parser

import (
	"strings"
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

func TestParse_AlterTableConvertCharset(t *testing.T) {
	result, err := Parse("ALTER TABLE users CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ConvertCharset {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ConvertCharset)
	}
}

func TestParse_AlterTableChangeCharset(t *testing.T) {
	tests := []string{
		"ALTER TABLE users CHARACTER SET = utf8mb4",
		"ALTER TABLE users CHARSET = utf8mb4",
		"ALTER TABLE users DEFAULT CHARACTER SET utf8mb4",
	}
	for _, sql := range tests {
		result, err := Parse(sql)
		if err != nil {
			t.Fatalf("unexpected error parsing %q: %v", sql, err)
		}
		if result.DDLOp != ChangeCharset {
			t.Errorf("SQL %q: DDLOp = %q, want %q", sql, result.DDLOp, ChangeCharset)
		}
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
		{
			name:  "insert select",
			sql:   "INSERT INTO users SELECT * FROM old_users",
			table: "users",
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

func TestParse_LoadData(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "simple load data",
			sql:  "LOAD DATA INFILE '/tmp/data.csv' INTO TABLE users",
		},
		{
			name: "load data local",
			sql:  "LOAD DATA LOCAL INFILE '/tmp/data.csv' INTO TABLE mydb.orders",
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
			if result.DMLOp != LoadData {
				t.Errorf("DMLOp = %q, want %q", result.DMLOp, LoadData)
			}
			// Note: Vitess doesn't parse LOAD DATA details, so table name won't be extracted
		})
	}
}

func TestParse_CreateTable(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		table    string
		database string
	}{
		{
			name:  "simple create table",
			sql:   "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100))",
			table: "users",
		},
		{
			name:  "create table as select",
			sql:   "CREATE TABLE new_users AS SELECT * FROM old_users",
			table: "new_users",
		},
		{
			name:  "create table select (without AS)",
			sql:   "CREATE TABLE new_users SELECT * FROM old_users",
			table: "new_users",
		},
		{
			name:     "create table with qualified name",
			sql:      "CREATE TABLE mydb.users (id INT)",
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
			if result.Type != DDL {
				t.Errorf("Type = %q, want DDL", result.Type)
			}
			if result.DDLOp != CreateTable {
				t.Errorf("DDLOp = %q, want %q", result.DDLOp, CreateTable)
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

func TestParse_InvalidDataType(t *testing.T) {
	// Vitess cannot classify ADD COLUMN with an unknown type (e.g. VRCHAR);
	// the AlterTable AST node is returned but the AlterOptions produce OtherDDL.
	sql := "ALTER TABLE users ADD COLUMN email VRCHAR(255) NOT NULL DEFAULT ''"
	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Type != DDL {
		t.Errorf("Type = %q, want DDL", result.Type)
	}
	if result.DDLOp != OtherDDL {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, OtherDDL)
	}
}

func TestParse_AlterTableDropPrimaryKey(t *testing.T) {
	result, err := Parse("ALTER TABLE t DROP PRIMARY KEY")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != DropPrimaryKey {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, DropPrimaryKey)
	}
}

func TestParse_AlterTableDropForeignKey(t *testing.T) {
	result, err := Parse("ALTER TABLE t DROP FOREIGN KEY fk_user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != DropForeignKey {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, DropForeignKey)
	}
}

func TestParse_AlterTableAddPrimaryKey(t *testing.T) {
	result, err := Parse("ALTER TABLE t ADD PRIMARY KEY (id)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddPrimaryKey {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddPrimaryKey)
	}
	if len(result.IndexColumns) != 1 || result.IndexColumns[0] != "id" {
		t.Errorf("IndexColumns = %v, want [id]", result.IndexColumns)
	}
}

func TestParse_AlterTableAddPrimaryKey_MultiCol(t *testing.T) {
	result, err := Parse("ALTER TABLE t ADD PRIMARY KEY (a, b)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddPrimaryKey {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddPrimaryKey)
	}
	if len(result.IndexColumns) != 2 || result.IndexColumns[0] != "a" || result.IndexColumns[1] != "b" {
		t.Errorf("IndexColumns = %v, want [a b]", result.IndexColumns)
	}
}

func TestParse_AlterTableChangeEngine(t *testing.T) {
	result, err := Parse("ALTER TABLE t ENGINE = InnoDB")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ChangeEngine {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ChangeEngine)
	}
}

func TestParse_AlterTableChangeRowFormat(t *testing.T) {
	result, err := Parse("ALTER TABLE t ROW_FORMAT = DYNAMIC")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ChangeRowFormat {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ChangeRowFormat)
	}
}

func TestParse_AlterTableSetDefault(t *testing.T) {
	result, err := Parse("ALTER TABLE t ALTER COLUMN c SET DEFAULT 42")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != SetDefault {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, SetDefault)
	}
}

func TestParse_AlterTableDropDefault(t *testing.T) {
	result, err := Parse("ALTER TABLE t ALTER COLUMN c DROP DEFAULT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != DropDefault {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, DropDefault)
	}
}

func TestParse_AlterTableAddPartition(t *testing.T) {
	result, err := Parse("ALTER TABLE t ADD PARTITION (PARTITION p1 VALUES LESS THAN (100))")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddPartition {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddPartition)
	}
}

func TestParse_AlterTableDropPartition(t *testing.T) {
	result, err := Parse("ALTER TABLE t DROP PARTITION p1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != DropPartition {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, DropPartition)
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

// TestParse_RawSQL verifies the RawSQL field stores the cleaned input (trimmed,
// semicolon stripped) so callers can reproduce the exact statement that was analyzed.
func TestParse_RawSQL(t *testing.T) {
	tests := []struct {
		input   string
		wantRaw string
	}{
		{
			input:   "ALTER TABLE users ADD COLUMN email VARCHAR(255);",
			wantRaw: "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		},
		{
			input:   "  DELETE FROM logs WHERE id = 1  ",
			wantRaw: "DELETE FROM logs WHERE id = 1",
		},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result, err := Parse(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.RawSQL != tt.wantRaw {
				t.Errorf("RawSQL = %q, want %q", result.RawSQL, tt.wantRaw)
			}
		})
	}
}

// TestParse_AlterTableChangeColumn_Fields checks that OldColumnName, NewColumnName,
// and ColumnDef are all populated for CHANGE COLUMN — these are used by the analyzer
// but were not asserted in the existing test.
func TestParse_AlterTableChangeColumn_Fields(t *testing.T) {
	result, err := Parse("ALTER TABLE users CHANGE COLUMN old_name new_name VARCHAR(500)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.OldColumnName != "old_name" {
		t.Errorf("OldColumnName = %q, want %q", result.OldColumnName, "old_name")
	}
	if result.NewColumnName != "new_name" {
		t.Errorf("NewColumnName = %q, want %q", result.NewColumnName, "new_name")
	}
	if result.ColumnDef == "" {
		t.Error("ColumnDef is empty, expected non-empty")
	}
}

// TestParse_AlterTableModifyColumn_ColumnName checks that ColumnName and ColumnDef
// are extracted for MODIFY COLUMN, which the existing test omits.
func TestParse_AlterTableModifyColumn_ColumnName(t *testing.T) {
	result, err := Parse("ALTER TABLE users MODIFY COLUMN email VARCHAR(500) NOT NULL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ColumnName != "email" {
		t.Errorf("ColumnName = %q, want %q", result.ColumnName, "email")
	}
	if result.ColumnDef == "" {
		t.Error("ColumnDef is empty, expected non-empty")
	}
}

// TestParse_ModifyColumn_ExtractsNewColumnType verifies that MODIFY COLUMN now populates
// NewColumnType, enabling the analyzer to detect the new type for INPLACE eligibility.
func TestParse_ModifyColumn_ExtractsNewColumnType(t *testing.T) {
	result, err := Parse("ALTER TABLE orders MODIFY COLUMN order_number VARCHAR(50)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ModifyColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ModifyColumn)
	}
	if result.ColumnName != "order_number" {
		t.Errorf("ColumnName = %q, want %q", result.ColumnName, "order_number")
	}
	if result.NewColumnType == "" {
		t.Error("NewColumnType is empty; MODIFY COLUMN should populate it")
	}
	if result.NewColumnType != "varchar(50)" {
		t.Errorf("NewColumnType = %q, want %q", result.NewColumnType, "varchar(50)")
	}
}

// TestParse_WhereClauseContent verifies the WhereClause string contains the actual
// condition, not just that it's non-empty.
func TestParse_WhereClauseContent(t *testing.T) {
	result, err := Parse("DELETE FROM logs WHERE id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !result.HasWhere {
		t.Error("HasWhere = false, want true")
	}
	if !strings.Contains(result.WhereClause, "id") {
		t.Errorf("WhereClause = %q, expected to contain column name 'id'", result.WhereClause)
	}
}

// TestParse_AddColumn_ColumnDef verifies that ColumnDef is populated for ADD COLUMN.
func TestParse_AddColumn_ColumnDef(t *testing.T) {
	result, err := Parse("ALTER TABLE users ADD COLUMN score DECIMAL(10,2) NOT NULL DEFAULT 0.0")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.ColumnDef == "" {
		t.Error("ColumnDef is empty, expected non-empty")
	}
}

// TestParse_DropForeignKey_IndexName checks that IndexName is captured for
// DROP FOREIGN KEY, which the existing test omits.
func TestParse_DropForeignKey_IndexName(t *testing.T) {
	result, err := Parse("ALTER TABLE orders DROP FOREIGN KEY fk_user")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.IndexName != "fk_user" {
		t.Errorf("IndexName = %q, want %q", result.IndexName, "fk_user")
	}
}

// TestParse_UpdateQualifiedTable checks that the database name is extracted from a
// qualified table reference in UPDATE, mirroring the equivalent DELETE test.
func TestParse_UpdateQualifiedTable(t *testing.T) {
	result, err := Parse("UPDATE mydb.users SET status = 'active' WHERE id = 1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Database != "mydb" {
		t.Errorf("Database = %q, want %q", result.Database, "mydb")
	}
	if result.Table != "users" {
		t.Errorf("Table = %q, want %q", result.Table, "users")
	}
	if !result.HasWhere {
		t.Error("HasWhere = false, want true")
	}
}

// TestParse_RenameTableQualified verifies that the source database and table are
// extracted correctly when RENAME TABLE uses a qualified name.
func TestParse_RenameTableQualified(t *testing.T) {
	result, err := Parse("RENAME TABLE mydb.users TO mydb.users_backup")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Database != "mydb" {
		t.Errorf("Database = %q, want %q", result.Database, "mydb")
	}
	if result.Table != "users" {
		t.Errorf("Table = %q, want %q", result.Table, "users")
	}
}

// TestParse_OtherDDLTableOption exercises the TableOptions branch of
// classifySingleAlterOp for a table option that maps to OtherDDL (unrecognized option).
func TestParse_OtherDDLTableOption(t *testing.T) {
	// An unrecognized table option falls through to OtherDDL.
	result, err := Parse("ALTER TABLE t COMMENT='hello'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != OtherDDL {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, OtherDDL)
	}
}

// TestParse_KeyBlockSize verifies that KEY_BLOCK_SIZE is classified correctly.
func TestParse_KeyBlockSize(t *testing.T) {
	result, err := Parse("ALTER TABLE t KEY_BLOCK_SIZE = 8")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != KeyBlockSize {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, KeyBlockSize)
	}
}

// TestParse_StatsOption verifies that InnoDB statistics table options are classified correctly.
func TestParse_StatsOption(t *testing.T) {
	tests := []string{
		"ALTER TABLE t STATS_PERSISTENT=1",
		"ALTER TABLE t STATS_SAMPLE_PAGES=25",
		"ALTER TABLE t STATS_AUTO_RECALC=1",
	}
	for _, sql := range tests {
		result, err := Parse(sql)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", sql, err)
		}
		if result.DDLOp != StatsOption {
			t.Errorf("%q: DDLOp = %q, want %q", sql, result.DDLOp, StatsOption)
		}
	}
}

// TestParse_TableEncryption verifies that ENCRYPTION table option is classified correctly.
func TestParse_TableEncryption(t *testing.T) {
	result, err := Parse("ALTER TABLE t ENCRYPTION='Y'")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != TableEncryption {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, TableEncryption)
	}
}

// TestParse_AddColumnAutoIncrement verifies that AUTO_INCREMENT is detected in ADD COLUMN.
func TestParse_AddColumnAutoIncrement(t *testing.T) {
	result, err := Parse("ALTER TABLE t ADD COLUMN id BIGINT AUTO_INCREMENT PRIMARY KEY")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddColumn)
	}
	if !result.HasAutoIncrement {
		t.Errorf("HasAutoIncrement = false, want true")
	}
}

// TestParse_MultipleOps_AutoIncrementPropagated verifies that HasAutoIncrement is set
// when ADD COLUMN AUTO_INCREMENT appears as part of a multi-op ALTER TABLE.
func TestParse_MultipleOps_AutoIncrementPropagated(t *testing.T) {
	result, err := Parse("ALTER TABLE t ADD COLUMN seq_id INT NOT NULL AUTO_INCREMENT, ADD UNIQUE KEY (seq_id)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != MultipleOps {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, MultipleOps)
	}
	if !result.HasAutoIncrement {
		t.Errorf("HasAutoIncrement = false, want true (should be propagated from ADD COLUMN)")
	}
	if len(result.DDLOperations) != 2 {
		t.Errorf("DDLOperations len = %d, want 2", len(result.DDLOperations))
	}
}

// TestParse_AddStoredGeneratedColumn verifies that ADD COLUMN ... AS (...) STORED
// sets IsGeneratedStored=true.
func TestParse_AddStoredGeneratedColumn(t *testing.T) {
	result, err := Parse("ALTER TABLE gen_col_test ADD COLUMN discount_price DECIMAL(10,2) AS (price * 0.9) STORED")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddColumn)
	}
	if !result.IsGeneratedStored {
		t.Errorf("IsGeneratedStored = false, want true for STORED generated column")
	}
	if result.ColumnName != "discount_price" {
		t.Errorf("ColumnName = %q, want %q", result.ColumnName, "discount_price")
	}
}

// TestParse_AddVirtualGeneratedColumn verifies that ADD COLUMN ... AS (...) VIRTUAL
// does NOT set IsGeneratedStored.
func TestParse_AddVirtualGeneratedColumn(t *testing.T) {
	result, err := Parse("ALTER TABLE gen_col_test ADD COLUMN discount_virtual DECIMAL(10,2) AS (price * 0.9) VIRTUAL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddColumn)
	}
	if result.IsGeneratedStored {
		t.Errorf("IsGeneratedStored = true, want false for VIRTUAL generated column")
	}
}

// TestParse_ModifyStoredGeneratedColumn verifies that MODIFY COLUMN ... AS (...) STORED
// sets both IsGeneratedColumn=true and IsGeneratedStored=true.
func TestParse_ModifyStoredGeneratedColumn(t *testing.T) {
	result, err := Parse("ALTER TABLE t MODIFY COLUMN total_stored DECIMAL(12,2) AS (price * quantity) STORED FIRST")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ModifyColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ModifyColumn)
	}
	if !result.IsGeneratedColumn {
		t.Error("IsGeneratedColumn = false, want true for STORED generated column")
	}
	if !result.IsGeneratedStored {
		t.Error("IsGeneratedStored = false, want true for STORED generated column")
	}
	if !result.IsFirstAfter {
		t.Error("IsFirstAfter = false, want true (FIRST clause present)")
	}
}

// TestParse_ModifyVirtualGeneratedColumn verifies that MODIFY COLUMN ... AS (...) VIRTUAL
// sets IsGeneratedColumn=true but NOT IsGeneratedStored.
func TestParse_ModifyVirtualGeneratedColumn(t *testing.T) {
	result, err := Parse("ALTER TABLE t MODIFY COLUMN total_virtual DECIMAL(12,2) AS (price * quantity) VIRTUAL AFTER id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ModifyColumn {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ModifyColumn)
	}
	if !result.IsGeneratedColumn {
		t.Error("IsGeneratedColumn = false, want true for VIRTUAL generated column")
	}
	if result.IsGeneratedStored {
		t.Error("IsGeneratedStored = true, want false for VIRTUAL generated column")
	}
	if !result.IsFirstAfter {
		t.Error("IsFirstAfter = false, want true (AFTER clause present)")
	}
}

// TestParse_ChangeIndexType verifies that DROP INDEX + ADD INDEX on the same name
// is detected as ChangeIndexType.
func TestParse_ChangeIndexType(t *testing.T) {
	result, err := Parse("ALTER TABLE t DROP INDEX idx_email, ADD INDEX idx_email (email)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ChangeIndexType {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ChangeIndexType)
	}
	if result.IndexName != "idx_email" {
		t.Errorf("IndexName = %q, want %q", result.IndexName, "idx_email")
	}
}

// TestParse_ReplacePrimaryKey verifies that DROP PRIMARY KEY + ADD PRIMARY KEY
// is detected as ReplacePrimaryKey.
func TestParse_ReplacePrimaryKey(t *testing.T) {
	result, err := Parse("ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY (col1, col2)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ReplacePrimaryKey {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ReplacePrimaryKey)
	}
}

// TestParse_ChangeAutoIncrement verifies that AUTO_INCREMENT = N is recognized.
func TestParse_ChangeAutoIncrement(t *testing.T) {
	result, err := Parse("ALTER TABLE orders AUTO_INCREMENT = 99999999")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ChangeAutoIncrement {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ChangeAutoIncrement)
	}
	if result.Table != "orders" {
		t.Errorf("Table = %q, want %q", result.Table, "orders")
	}
}

// TestParse_BacktickIdentifiers is a unit test (vs benchmark) confirming that
// backtick-quoted database, table, and column names are correctly unquoted.
func TestParse_BacktickIdentifiers(t *testing.T) {
	result, err := Parse("ALTER TABLE `my_db`.`my_table` ADD COLUMN `new_col` INT")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Database != "my_db" {
		t.Errorf("Database = %q, want %q", result.Database, "my_db")
	}
	if result.Table != "my_table" {
		t.Errorf("Table = %q, want %q", result.Table, "my_table")
	}
	if result.ColumnName != "new_col" {
		t.Errorf("ColumnName = %q, want %q", result.ColumnName, "new_col")
	}
}

func TestParse_ChangeColumn_ExtractsNewColumnType(t *testing.T) {
	tests := []struct {
		name          string
		sql           string
		oldColumnName string
		newColumnName string
		newColumnType string
	}{
		{
			name:          "rename with type change",
			sql:           "ALTER TABLE orders CHANGE COLUMN total_amount amount DECIMAL(14,4)",
			oldColumnName: "total_amount",
			newColumnName: "amount",
			newColumnType: "decimal(14,4)",
		},
		{
			name:          "rename only same type",
			sql:           "ALTER TABLE users CHANGE COLUMN fname first_name VARCHAR(100)",
			oldColumnName: "fname",
			newColumnName: "first_name",
			newColumnType: "varchar(100)",
		},
		{
			name:          "same name type change",
			sql:           "ALTER TABLE t CHANGE COLUMN col col BIGINT",
			oldColumnName: "col",
			newColumnName: "col",
			newColumnType: "bigint",
		},
		{
			// Regression: NOT NULL DEFAULT must NOT be included in NewColumnType.
			// The analyzer compares against INFORMATION_SCHEMA.COLUMN_TYPE which has
			// only the base type (e.g. "varchar(20)"), so options must be stripped.
			name:          "rename only with NOT NULL DEFAULT options",
			sql:           "ALTER TABLE orders CHANGE COLUMN status order_status VARCHAR(20) NOT NULL DEFAULT 'pending'",
			oldColumnName: "status",
			newColumnName: "order_status",
			newColumnType: "varchar(20)",
		},
		{
			name:          "rename only with UNSIGNED NOT NULL",
			sql:           "ALTER TABLE t CHANGE COLUMN qty amount INT UNSIGNED NOT NULL DEFAULT 0",
			oldColumnName: "qty",
			newColumnName: "amount",
			newColumnType: "int unsigned",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.DDLOp != ChangeColumn {
				t.Errorf("DDLOp = %q, want %q", result.DDLOp, ChangeColumn)
			}
			if result.OldColumnName != tt.oldColumnName {
				t.Errorf("OldColumnName = %q, want %q", result.OldColumnName, tt.oldColumnName)
			}
			if result.NewColumnName != tt.newColumnName {
				t.Errorf("NewColumnName = %q, want %q", result.NewColumnName, tt.newColumnName)
			}
			if result.NewColumnType != tt.newColumnType {
				t.Errorf("NewColumnType = %q, want %q", result.NewColumnType, tt.newColumnType)
			}
		})
	}
}

// =============================================================
// New operation parser tests (Phase 2)
// =============================================================

func TestParse_RenameIndex(t *testing.T) {
	result, err := Parse("ALTER TABLE orders RENAME INDEX idx_customer_id TO idx_cust")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != RenameIndex {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, RenameIndex)
	}
	if result.Table != "orders" {
		t.Errorf("Table = %q, want %q", result.Table, "orders")
	}
}

func TestParse_AddFulltextIndex(t *testing.T) {
	result, err := Parse("ALTER TABLE audit_log ADD FULLTEXT INDEX ft_action (action)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddFulltextIndex {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddFulltextIndex)
	}
}

func TestParse_AddSpatialIndex(t *testing.T) {
	result, err := Parse("ALTER TABLE geo_test ADD SPATIAL INDEX idx_geo (g)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != AddSpatialIndex {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, AddSpatialIndex)
	}
}

func TestParse_ForceRebuild(t *testing.T) {
	result, err := Parse("ALTER TABLE orders FORCE")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ForceRebuild {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ForceRebuild)
	}
	if result.Table != "orders" {
		t.Errorf("Table = %q, want %q", result.Table, "orders")
	}
}

func TestParse_ReorganizePartition(t *testing.T) {
	sql := `ALTER TABLE partition_test REORGANIZE PARTITION pmax INTO (
		PARTITION p2026 VALUES LESS THAN (2027),
		PARTITION pmax VALUES LESS THAN MAXVALUE
	)`
	result, err := Parse(sql)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ReorganizePartition {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, ReorganizePartition)
	}
}

func TestParse_RebuildPartition(t *testing.T) {
	result, err := Parse("ALTER TABLE partition_test REBUILD PARTITION p2024")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != RebuildPartition {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, RebuildPartition)
	}
}

func TestParse_TruncatePartition(t *testing.T) {
	result, err := Parse("ALTER TABLE partition_test TRUNCATE PARTITION p2023")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != TruncatePartition {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, TruncatePartition)
	}
}

func TestParse_ModifyColumn_IsFirstAfter(t *testing.T) {
	// MODIFY COLUMN with AFTER should set IsFirstAfter=true
	result, err := Parse("ALTER TABLE t MODIFY COLUMN name VARCHAR(100) AFTER id")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ModifyColumn {
		t.Errorf("DDLOp = %q, want ModifyColumn", result.DDLOp)
	}
	if !result.IsFirstAfter {
		t.Error("IsFirstAfter = false, want true for MODIFY COLUMN ... AFTER")
	}
	if result.NewColumnType != "varchar(100)" {
		t.Errorf("NewColumnType = %q, want %q", result.NewColumnType, "varchar(100)")
	}
}

func TestParse_ModifyColumn_NotNull_SetsNullable(t *testing.T) {
	result, err := Parse("ALTER TABLE t MODIFY COLUMN name VARCHAR(100) NOT NULL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ModifyColumn {
		t.Errorf("DDLOp = %q, want ModifyColumn", result.DDLOp)
	}
	if result.NewColumnNullable == nil {
		t.Fatal("NewColumnNullable = nil, want *false (NOT NULL specified)")
	}
	if *result.NewColumnNullable != false {
		t.Errorf("*NewColumnNullable = %v, want false (NOT NULL)", *result.NewColumnNullable)
	}
}

func TestParse_ModifyColumn_Null_SetsNullable(t *testing.T) {
	result, err := Parse("ALTER TABLE t MODIFY COLUMN name VARCHAR(100) NULL")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NewColumnNullable == nil {
		t.Fatal("NewColumnNullable = nil, want *true (NULL specified)")
	}
	if *result.NewColumnNullable != true {
		t.Errorf("*NewColumnNullable = %v, want true (NULL)", *result.NewColumnNullable)
	}
}

func TestParse_ModifyColumn_NoNullSpec_NilNullable(t *testing.T) {
	// No NULL/NOT NULL → NewColumnNullable should be nil
	result, err := Parse("ALTER TABLE t MODIFY COLUMN name VARCHAR(100)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.NewColumnNullable != nil {
		t.Errorf("NewColumnNullable = %v, want nil (nullability not specified)", result.NewColumnNullable)
	}
}

func TestParse_ModifyColumn_Charset(t *testing.T) {
	tests := []struct {
		name              string
		sql               string
		wantCharset       string
		wantColumnName    string
	}{
		{
			name:           "explicit charset",
			sql:            "ALTER TABLE t MODIFY COLUMN name VARCHAR(100) CHARACTER SET utf8mb4",
			wantCharset:    "utf8mb4",
			wantColumnName: "name",
		},
		{
			name:           "explicit charset with collate",
			sql:            "ALTER TABLE t MODIFY COLUMN name VARCHAR(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci",
			wantCharset:    "utf8mb4",
			wantColumnName: "name",
		},
		{
			name:           "no charset clause",
			sql:            "ALTER TABLE t MODIFY COLUMN name VARCHAR(100)",
			wantCharset:    "",
			wantColumnName: "name",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := Parse(tt.sql)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if result.DDLOp != ModifyColumn {
				t.Errorf("DDLOp = %q, want ModifyColumn", result.DDLOp)
			}
			if result.ColumnName != tt.wantColumnName {
				t.Errorf("ColumnName = %q, want %q", result.ColumnName, tt.wantColumnName)
			}
			if result.NewColumnCharset != tt.wantCharset {
				t.Errorf("NewColumnCharset = %q, want %q", result.NewColumnCharset, tt.wantCharset)
			}
		})
	}
}

func TestParse_OptimizeTable(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantDB   string
		wantTbl  string
	}{
		{
			name:    "simple",
			sql:     "OPTIMIZE TABLE orders",
			wantTbl: "orders",
		},
		{
			name:    "qualified name",
			sql:     "OPTIMIZE TABLE mydb.orders",
			wantDB:  "mydb",
			wantTbl: "orders",
		},
		{
			name:    "NO_WRITE_TO_BINLOG variant",
			sql:     "OPTIMIZE NO_WRITE_TO_BINLOG TABLE orders",
			wantTbl: "orders",
		},
		{
			name:    "LOCAL variant",
			sql:     "OPTIMIZE LOCAL TABLE orders",
			wantTbl: "orders",
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
			if result.DDLOp != OptimizeTable {
				t.Errorf("DDLOp = %q, want OptimizeTable", result.DDLOp)
			}
			if result.Table != tt.wantTbl {
				t.Errorf("Table = %q, want %q", result.Table, tt.wantTbl)
			}
			if result.Database != tt.wantDB {
				t.Errorf("Database = %q, want %q", result.Database, tt.wantDB)
			}
		})
	}
}

// Regression #37: ALTER TABLE ... ENGINE=InnoDB must extract NewEngine.
func TestParse_ChangeEngine_ExtractsEngineName(t *testing.T) {
	result, err := Parse("ALTER TABLE orders ENGINE=InnoDB")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != ChangeEngine {
		t.Errorf("DDLOp = %v, want CHANGE_ENGINE", result.DDLOp)
	}
	if result.NewEngine != "innodb" {
		t.Errorf("NewEngine = %q, want \"innodb\"", result.NewEngine)
	}
}

// Regression #38: ALTER TABLE ... RENAME TO must parse as RENAME_TABLE (not OTHER).
func TestParse_AlterTableRenameTO_IsRenameTable(t *testing.T) {
	for _, sql := range []string{
		"ALTER TABLE products RENAME TO product_catalog",
		"ALTER TABLE products RENAME product_catalog",
	} {
		result, err := Parse(sql)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", sql, err)
		}
		if result.DDLOp != RenameTable {
			t.Errorf("%q: DDLOp = %v, want RENAME_TABLE", sql, result.DDLOp)
		}
	}
}

func TestParse_AlterTablespace(t *testing.T) {
	result, err := Parse("ALTER TABLESPACE ts1 RENAME TO ts2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Type != DDL {
		t.Errorf("Type = %q, want DDL", result.Type)
	}
	if result.DDLOp != AlterTablespace {
		t.Errorf("DDLOp = %q, want AlterTablespace", result.DDLOp)
	}
	if result.TablespaceName != "ts1" {
		t.Errorf("TablespaceName = %q, want ts1", result.TablespaceName)
	}
	if result.NewTablespaceName != "ts2" {
		t.Errorf("NewTablespaceName = %q, want ts2", result.NewTablespaceName)
	}
	// Tablespace operations have no table name
	if result.Table != "" {
		t.Errorf("Table = %q, want empty", result.Table)
	}
}
