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
// classifySingleAlterOp for a table option that is neither ENGINE nor ROW_FORMAT.
func TestParse_OtherDDLTableOption(t *testing.T) {
	result, err := Parse("ALTER TABLE t AUTO_INCREMENT = 1000")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.DDLOp != OtherDDL {
		t.Errorf("DDLOp = %q, want %q", result.DDLOp, OtherDDL)
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
