package mysql

import (
	"database/sql"
	"fmt"
	"strings"
)

// TableMetadata holds all metadata about a table needed for analysis.
type TableMetadata struct {
	Database      string
	Table         string
	Engine        string
	RowCount      int64
	DataLength    int64  // bytes
	IndexLength   int64  // bytes
	AvgRowLength  int64  // bytes
	AutoIncrement int64
	RowFormat     string
	CreateTable   string // full CREATE TABLE statement
	Columns       []ColumnInfo
	Indexes       []IndexInfo
	ForeignKeys   []ForeignKeyInfo
	Triggers      []TriggerInfo
}

// TotalSize returns data + index size in bytes.
func (m *TableMetadata) TotalSize() int64 {
	return m.DataLength + m.IndexLength
}

// TotalSizeHuman returns a human-readable size string.
func (m *TableMetadata) TotalSizeHuman() string {
	return humanBytes(m.TotalSize())
}

// IndexInfo describes a single index on a table.
type IndexInfo struct {
	Name      string
	Columns   []string
	NonUnique bool
	Type      string // BTREE, HASH, FULLTEXT, SPATIAL
}

// ForeignKeyInfo describes a foreign key relationship.
type ForeignKeyInfo struct {
	Name             string
	Columns          []string
	ReferencedTable  string
	ReferencedSchema string
	ReferencedCols   []string
}

// TriggerInfo describes a trigger on a table.
type TriggerInfo struct {
	Name      string
	Event     string // INSERT, UPDATE, DELETE
	Timing    string // BEFORE, AFTER
	Statement string
}

// ColumnInfo describes a single column in a table.
type ColumnInfo struct {
	Name         string
	Type         string
	Nullable     bool
	Default      *string
	Position     int
	CharacterSet *string
	Collation    *string
}

// escapeIdentifier safely escapes a MySQL identifier (database, table, column name)
// by wrapping it in backticks and escaping any backticks within the identifier.
// This prevents SQL injection when building dynamic queries with identifier names.
func escapeIdentifier(identifier string) string {
	// Replace backticks with escaped backticks
	escaped := strings.ReplaceAll(identifier, "`", "``")
	// Wrap in backticks
	return "`" + escaped + "`"
}

// GetTableMetadata collects comprehensive metadata about a table.
func GetTableMetadata(db *sql.DB, database, table string) (*TableMetadata, error) {
	meta := &TableMetadata{
		Database: database,
		Table:    table,
	}

	// Basic table info from information_schema.TABLES
	err := db.QueryRow(`
		SELECT 
			ENGINE,
			IFNULL(TABLE_ROWS, 0),
			IFNULL(DATA_LENGTH, 0),
			IFNULL(INDEX_LENGTH, 0),
			IFNULL(AVG_ROW_LENGTH, 0),
			IFNULL(AUTO_INCREMENT, 0),
			IFNULL(ROW_FORMAT, '')
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
	`, database, table).Scan(
		&meta.Engine,
		&meta.RowCount,
		&meta.DataLength,
		&meta.IndexLength,
		&meta.AvgRowLength,
		&meta.AutoIncrement,
		&meta.RowFormat,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("table %s.%s not found", database, table)
		}
		return nil, fmt.Errorf("querying table info: %w", err)
	}

	// SHOW CREATE TABLE for full definition
	var tblName, createStmt string
	// Security: Use escapeIdentifier to prevent SQL injection via database/table names
	query := fmt.Sprintf("SHOW CREATE TABLE %s.%s", escapeIdentifier(database), escapeIdentifier(table))
	err = db.QueryRow(query).Scan(&tblName, &createStmt)
	if err == nil {
		meta.CreateTable = createStmt
	}

	// Columns
	meta.Columns, err = getColumns(db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}

	// Indexes
	meta.Indexes, err = getIndexes(db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying indexes: %w", err)
	}

	// Foreign keys (referencing FROM this table)
	meta.ForeignKeys, err = getForeignKeys(db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys: %w", err)
	}

	// Triggers
	meta.Triggers, err = getTriggers(db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying triggers: %w", err)
	}

	return meta, nil
}

func getIndexes(db *sql.DB, database, table string) ([]IndexInfo, error) {
	rows, err := db.Query(`
		SELECT 
			INDEX_NAME,
			COLUMN_NAME,
			NON_UNIQUE,
			IFNULL(INDEX_TYPE, 'BTREE')
		FROM information_schema.STATISTICS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY INDEX_NAME, SEQ_IN_INDEX
	`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexMap := make(map[string]*IndexInfo)
	var order []string

	for rows.Next() {
		var name, col, idxType string
		var nonUnique bool
		if err := rows.Scan(&name, &col, &nonUnique, &idxType); err != nil {
			return nil, err
		}

		if _, exists := indexMap[name]; !exists {
			indexMap[name] = &IndexInfo{
				Name:      name,
				NonUnique: nonUnique,
				Type:      idxType,
			}
			order = append(order, name)
		}
		indexMap[name].Columns = append(indexMap[name].Columns, col)
	}

	var result []IndexInfo
	for _, name := range order {
		result = append(result, *indexMap[name])
	}
	return result, nil
}

func getForeignKeys(db *sql.DB, database, table string) ([]ForeignKeyInfo, error) {
	rows, err := db.Query(`
		SELECT 
			CONSTRAINT_NAME,
			COLUMN_NAME,
			REFERENCED_TABLE_SCHEMA,
			REFERENCED_TABLE_NAME,
			REFERENCED_COLUMN_NAME
		FROM information_schema.KEY_COLUMN_USAGE
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
			AND REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY CONSTRAINT_NAME, ORDINAL_POSITION
	`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fkMap := make(map[string]*ForeignKeyInfo)
	var order []string

	for rows.Next() {
		var name, col, refSchema, refTable, refCol string
		if err := rows.Scan(&name, &col, &refSchema, &refTable, &refCol); err != nil {
			return nil, err
		}

		if _, exists := fkMap[name]; !exists {
			fkMap[name] = &ForeignKeyInfo{
				Name:             name,
				ReferencedSchema: refSchema,
				ReferencedTable:  refTable,
			}
			order = append(order, name)
		}
		fkMap[name].Columns = append(fkMap[name].Columns, col)
		fkMap[name].ReferencedCols = append(fkMap[name].ReferencedCols, refCol)
	}

	var result []ForeignKeyInfo
	for _, name := range order {
		result = append(result, *fkMap[name])
	}
	return result, nil
}

func getTriggers(db *sql.DB, database, table string) ([]TriggerInfo, error) {
	rows, err := db.Query(`
		SELECT 
			TRIGGER_NAME,
			EVENT_MANIPULATION,
			ACTION_TIMING,
			ACTION_STATEMENT
		FROM information_schema.TRIGGERS
		WHERE EVENT_OBJECT_SCHEMA = ? AND EVENT_OBJECT_TABLE = ?
	`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []TriggerInfo
	for rows.Next() {
		var t TriggerInfo
		if err := rows.Scan(&t.Name, &t.Event, &t.Timing, &t.Statement); err != nil {
			return nil, err
		}
		result = append(result, t)
	}
	return result, nil
}

func getColumns(db *sql.DB, database, table string) ([]ColumnInfo, error) {
	rows, err := db.Query(`
		SELECT
			COLUMN_NAME,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			ORDINAL_POSITION,
			CHARACTER_SET_NAME,
			COLLATION_NAME
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []ColumnInfo
	for rows.Next() {
		var c ColumnInfo
		var nullable string
		var defaultVal, charSet, collation sql.NullString

		if err := rows.Scan(&c.Name, &c.Type, &nullable, &defaultVal, &c.Position, &charSet, &collation); err != nil {
			return nil, err
		}

		c.Nullable = (nullable == "YES")
		if defaultVal.Valid {
			c.Default = &defaultVal.String
		}
		if charSet.Valid {
			c.CharacterSet = &charSet.String
		}
		if collation.Valid {
			c.Collation = &collation.String
		}

		result = append(result, c)
	}
	return result, nil
}

func humanBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
		TB = GB * 1024
	)
	switch {
	case b >= TB:
		return fmt.Sprintf("%.1f TB", float64(b)/float64(TB))
	case b >= GB:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
