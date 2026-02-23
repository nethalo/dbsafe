package mysql

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestTableMetadata_TotalSize(t *testing.T) {
	meta := &TableMetadata{
		DataLength:  1024 * 1024 * 100, // 100 MB
		IndexLength: 1024 * 1024 * 50,  // 50 MB
	}

	want := int64(1024 * 1024 * 150) // 150 MB
	if got := meta.TotalSize(); got != want {
		t.Errorf("TotalSize() = %d, want %d", got, want)
	}
}

func TestTableMetadata_TotalSizeHuman(t *testing.T) {
	tests := []struct {
		name        string
		dataLength  int64
		indexLength int64
		want        string
	}{
		{
			name:        "bytes",
			dataLength:  500,
			indexLength: 500,
			want:        "1000 B",
		},
		{
			name:        "kilobytes",
			dataLength:  1024 * 5,
			indexLength: 1024 * 3,
			want:        "8.0 KB",
		},
		{
			name:        "megabytes",
			dataLength:  1024 * 1024 * 100,
			indexLength: 1024 * 1024 * 50,
			want:        "150.0 MB",
		},
		{
			name:        "gigabytes",
			dataLength:  1024 * 1024 * 1024 * 2,
			indexLength: 1024 * 1024 * 1024 * 1,
			want:        "3.0 GB",
		},
		{
			name:        "terabytes",
			dataLength:  1024 * 1024 * 1024 * 1024 * 2,
			indexLength: 1024 * 1024 * 1024 * 1024 * 1,
			want:        "3.0 TB",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meta := &TableMetadata{
				DataLength:  tt.dataLength,
				IndexLength: tt.indexLength,
			}
			if got := meta.TotalSizeHuman(); got != tt.want {
				t.Errorf("TotalSizeHuman() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		bytes int64
		want  string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{1023, "1023 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{1024 * 1024 * 1024 * 1024, "1.0 TB"},
		{1536 * 1024 * 1024 * 1024, "1.5 TB"},
	}

	for _, tt := range tests {
		t.Run(tt.want, func(t *testing.T) {
			if got := humanBytes(tt.bytes); got != tt.want {
				t.Errorf("humanBytes(%d) = %q, want %q", tt.bytes, got, tt.want)
			}
		})
	}
}

func TestGetTableMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	t.Run("success - complete table metadata", func(t *testing.T) {
		// Mock TABLES query
		tableRows := sqlmock.NewRows([]string{
			"ENGINE", "TABLE_ROWS", "DATA_LENGTH", "INDEX_LENGTH",
			"AVG_ROW_LENGTH", "AUTO_INCREMENT", "ROW_FORMAT",
		}).AddRow("InnoDB", 1000, 102400, 51200, 102, 1001, "Dynamic")

		mock.ExpectQuery("SELECT.*FROM information_schema.TABLES").
			WithArgs("testdb", "users").
			WillReturnRows(tableRows)

		// Mock SHOW CREATE TABLE
		createRows := sqlmock.NewRows([]string{"Table", "Create Table"}).
			AddRow("users", "CREATE TABLE `users` (\n  `id` int NOT NULL,\n  `name` varchar(100)\n)")
		mock.ExpectQuery("SHOW CREATE TABLE").
			WillReturnRows(createRows)

		// Mock COLUMNS query
		colRows := sqlmock.NewRows([]string{
			"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT",
			"ORDINAL_POSITION", "CHARACTER_SET_NAME", "COLLATION_NAME", "EXTRA",
		}).
			AddRow("id", "int", "NO", nil, 1, nil, nil, "auto_increment").
			AddRow("name", "varchar(100)", "YES", "''", 2, "utf8mb4", "utf8mb4_unicode_ci", "")

		mock.ExpectQuery("SELECT.*FROM information_schema.COLUMNS").
			WithArgs("testdb", "users").
			WillReturnRows(colRows)

		// Mock STATISTICS query (indexes)
		idxRows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "NON_UNIQUE", "INDEX_TYPE"}).
			AddRow("PRIMARY", "id", false, "BTREE").
			AddRow("idx_name", "name", true, "BTREE")

		mock.ExpectQuery("SELECT.*FROM information_schema.STATISTICS").
			WithArgs("testdb", "users").
			WillReturnRows(idxRows)

		// Mock KEY_COLUMN_USAGE query (foreign keys)
		fkRows := sqlmock.NewRows([]string{
			"CONSTRAINT_NAME", "COLUMN_NAME", "REFERENCED_TABLE_SCHEMA",
			"REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
		}) // No foreign keys

		mock.ExpectQuery("SELECT.*FROM information_schema.KEY_COLUMN_USAGE").
			WithArgs("testdb", "users").
			WillReturnRows(fkRows)

		// Mock TRIGGERS query
		triggerRows := sqlmock.NewRows([]string{
			"TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING", "ACTION_STATEMENT",
		}) // No triggers

		mock.ExpectQuery("SELECT.*FROM information_schema.TRIGGERS").
			WithArgs("testdb", "users").
			WillReturnRows(triggerRows)

		meta, err := GetTableMetadata(db, "testdb", "users")
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if meta.Database != "testdb" {
			t.Errorf("Database = %q, want %q", meta.Database, "testdb")
		}
		if meta.Table != "users" {
			t.Errorf("Table = %q, want %q", meta.Table, "users")
		}
		if meta.Engine != "InnoDB" {
			t.Errorf("Engine = %q, want %q", meta.Engine, "InnoDB")
		}
		if meta.RowCount != 1000 {
			t.Errorf("RowCount = %d, want %d", meta.RowCount, 1000)
		}
		if meta.DataLength != 102400 {
			t.Errorf("DataLength = %d, want %d", meta.DataLength, 102400)
		}
		if meta.IndexLength != 51200 {
			t.Errorf("IndexLength = %d, want %d", meta.IndexLength, 51200)
		}
		if meta.AvgRowLength != 102 {
			t.Errorf("AvgRowLength = %d, want %d", meta.AvgRowLength, 102)
		}
		if meta.AutoIncrement != 1001 {
			t.Errorf("AutoIncrement = %d, want %d", meta.AutoIncrement, 1001)
		}
		if meta.RowFormat != "Dynamic" {
			t.Errorf("RowFormat = %q, want %q", meta.RowFormat, "Dynamic")
		}
		if len(meta.Columns) != 2 {
			t.Errorf("len(Columns) = %d, want 2", len(meta.Columns))
		}
		if len(meta.Indexes) != 2 {
			t.Errorf("len(Indexes) = %d, want 2", len(meta.Indexes))
		}
	})

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetTableMetadata_TableNotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT.*FROM information_schema.TABLES").
		WithArgs("testdb", "nonexistent").
		WillReturnError(sql.ErrNoRows)

	_, err = GetTableMetadata(db, "testdb", "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent table, got nil")
	}
	if !findSubstring(err.Error(), "not found") {
		t.Errorf("error should mention 'not found', got: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetColumns(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"COLUMN_NAME", "COLUMN_TYPE", "IS_NULLABLE", "COLUMN_DEFAULT",
		"ORDINAL_POSITION", "CHARACTER_SET_NAME", "COLLATION_NAME", "EXTRA",
	}).
		AddRow("id", "int", "NO", nil, 1, nil, nil, "").
		AddRow("name", "varchar(100)", "YES", "John", 2, "utf8mb4", "utf8mb4_unicode_ci", "").
		AddRow("created_at", "timestamp", "NO", "CURRENT_TIMESTAMP", 3, nil, nil, "")

	mock.ExpectQuery("SELECT.*FROM information_schema.COLUMNS").
		WithArgs("testdb", "users").
		WillReturnRows(rows)

	cols, err := getColumns(db, "testdb", "users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(cols) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(cols))
	}

	// Check first column (id)
	if cols[0].Name != "id" {
		t.Errorf("cols[0].Name = %q, want %q", cols[0].Name, "id")
	}
	if cols[0].Nullable {
		t.Error("cols[0].Nullable = true, want false")
	}
	if cols[0].Default != nil {
		t.Errorf("cols[0].Default = %v, want nil", cols[0].Default)
	}

	// Check second column (name)
	if cols[1].Name != "name" {
		t.Errorf("cols[1].Name = %q, want %q", cols[1].Name, "name")
	}
	if !cols[1].Nullable {
		t.Error("cols[1].Nullable = false, want true")
	}
	if cols[1].Default == nil || *cols[1].Default != "John" {
		t.Errorf("cols[1].Default = %v, want 'John'", cols[1].Default)
	}
	if cols[1].CharacterSet == nil || *cols[1].CharacterSet != "utf8mb4" {
		t.Errorf("cols[1].CharacterSet = %v, want 'utf8mb4'", cols[1].CharacterSet)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetIndexes(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{"INDEX_NAME", "COLUMN_NAME", "NON_UNIQUE", "INDEX_TYPE"}).
		AddRow("PRIMARY", "id", false, "BTREE").
		AddRow("idx_email", "email", true, "BTREE").
		AddRow("idx_name_created", "name", true, "BTREE").
		AddRow("idx_name_created", "created_at", true, "BTREE")

	mock.ExpectQuery("SELECT.*FROM information_schema.STATISTICS").
		WithArgs("testdb", "users").
		WillReturnRows(rows)

	indexes, err := getIndexes(db, "testdb", "users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(indexes) != 3 {
		t.Fatalf("expected 3 indexes, got %d", len(indexes))
	}

	// Check PRIMARY key
	if indexes[0].Name != "PRIMARY" {
		t.Errorf("indexes[0].Name = %q, want %q", indexes[0].Name, "PRIMARY")
	}
	if indexes[0].NonUnique {
		t.Error("indexes[0].NonUnique = true, want false")
	}
	if len(indexes[0].Columns) != 1 || indexes[0].Columns[0] != "id" {
		t.Errorf("indexes[0].Columns = %v, want ['id']", indexes[0].Columns)
	}

	// Check composite index
	if indexes[2].Name != "idx_name_created" {
		t.Errorf("indexes[2].Name = %q, want %q", indexes[2].Name, "idx_name_created")
	}
	if len(indexes[2].Columns) != 2 {
		t.Fatalf("expected 2 columns in composite index, got %d", len(indexes[2].Columns))
	}
	if indexes[2].Columns[0] != "name" || indexes[2].Columns[1] != "created_at" {
		t.Errorf("indexes[2].Columns = %v, want ['name', 'created_at']", indexes[2].Columns)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetForeignKeys(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"CONSTRAINT_NAME", "COLUMN_NAME", "REFERENCED_TABLE_SCHEMA",
		"REFERENCED_TABLE_NAME", "REFERENCED_COLUMN_NAME",
	}).
		AddRow("fk_user", "user_id", "testdb", "users", "id").
		AddRow("fk_composite", "col1", "otherdb", "other_table", "ref1").
		AddRow("fk_composite", "col2", "otherdb", "other_table", "ref2")

	mock.ExpectQuery("SELECT.*FROM information_schema.KEY_COLUMN_USAGE").
		WithArgs("testdb", "orders").
		WillReturnRows(rows)

	fks, err := getForeignKeys(db, "testdb", "orders")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(fks) != 2 {
		t.Fatalf("expected 2 foreign keys, got %d", len(fks))
	}

	// Check single column FK
	if fks[0].Name != "fk_user" {
		t.Errorf("fks[0].Name = %q, want %q", fks[0].Name, "fk_user")
	}
	if fks[0].ReferencedTable != "users" {
		t.Errorf("fks[0].ReferencedTable = %q, want %q", fks[0].ReferencedTable, "users")
	}
	if len(fks[0].Columns) != 1 || fks[0].Columns[0] != "user_id" {
		t.Errorf("fks[0].Columns = %v, want ['user_id']", fks[0].Columns)
	}

	// Check composite FK
	if fks[1].Name != "fk_composite" {
		t.Errorf("fks[1].Name = %q, want %q", fks[1].Name, "fk_composite")
	}
	if len(fks[1].Columns) != 2 {
		t.Fatalf("expected 2 columns in composite FK, got %d", len(fks[1].Columns))
	}
	if fks[1].Columns[0] != "col1" || fks[1].Columns[1] != "col2" {
		t.Errorf("fks[1].Columns = %v, want ['col1', 'col2']", fks[1].Columns)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetTriggers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	rows := sqlmock.NewRows([]string{
		"TRIGGER_NAME", "EVENT_MANIPULATION", "ACTION_TIMING", "ACTION_STATEMENT",
	}).
		AddRow("before_insert_check", "INSERT", "BEFORE", "BEGIN ... END").
		AddRow("after_update_log", "UPDATE", "AFTER", "INSERT INTO audit_log ...")

	mock.ExpectQuery("SELECT.*FROM information_schema.TRIGGERS").
		WithArgs("testdb", "users").
		WillReturnRows(rows)

	triggers, err := getTriggers(db, "testdb", "users")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(triggers) != 2 {
		t.Fatalf("expected 2 triggers, got %d", len(triggers))
	}

	if triggers[0].Name != "before_insert_check" {
		t.Errorf("triggers[0].Name = %q, want %q", triggers[0].Name, "before_insert_check")
	}
	if triggers[0].Event != "INSERT" {
		t.Errorf("triggers[0].Event = %q, want %q", triggers[0].Event, "INSERT")
	}
	if triggers[0].Timing != "BEFORE" {
		t.Errorf("triggers[0].Timing = %q, want %q", triggers[0].Timing, "BEFORE")
	}

	if triggers[1].Name != "after_update_log" {
		t.Errorf("triggers[1].Name = %q, want %q", triggers[1].Name, "after_update_log")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
