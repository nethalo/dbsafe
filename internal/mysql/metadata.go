package mysql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// TableMetadata holds all metadata about a table needed for analysis.
type TableMetadata struct {
	Database           string
	Table              string
	Engine             string
	RowCount           int64
	DataLength         int64 // bytes
	IndexLength        int64 // bytes
	AvgRowLength       int64 // bytes
	AutoIncrement      int64
	RowFormat          string
	CreateTable        string // full CREATE TABLE statement
	Columns            []ColumnInfo
	Indexes            []IndexInfo
	ForeignKeys        []ForeignKeyInfo
	InboundForeignKeys []ForeignKeyInfo
	Triggers           []TriggerInfo
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
	DeleteRule       string // CASCADE, RESTRICT, SET NULL, NO ACTION, SET DEFAULT
	UpdateRule       string
	ChildTable       string // populated only for inbound FKs (the table that owns the constraint)
	ChildSchema      string // populated only for inbound FKs
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
	Name              string
	Type              string
	Nullable          bool
	Default           *string
	Position          int
	CharacterSet      *string
	Collation         *string
	IsStoredGenerated bool // true when EXTRA contains "STORED GENERATED"
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
	ctx := context.Background()
	meta := &TableMetadata{
		Database: database,
		Table:    table,
	}

	// Basic table info from information_schema.TABLES
	err := db.QueryRowContext(ctx, `
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
	err = db.QueryRowContext(ctx, query).Scan(&tblName, &createStmt)
	if err == nil {
		meta.CreateTable = createStmt
	}

	// Columns
	meta.Columns, err = getColumns(ctx, db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying columns: %w", err)
	}

	// Indexes
	meta.Indexes, err = getIndexes(ctx, db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying indexes: %w", err)
	}

	// Foreign keys (referencing FROM this table)
	meta.ForeignKeys, err = getForeignKeys(ctx, db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying foreign keys: %w", err)
	}

	// Inbound foreign keys (other tables referencing THIS table)
	meta.InboundForeignKeys, err = getInboundForeignKeys(ctx, db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying inbound foreign keys: %w", err)
	}

	// Triggers
	meta.Triggers, err = getTriggers(ctx, db, database, table)
	if err != nil {
		return nil, fmt.Errorf("querying triggers: %w", err)
	}

	return meta, nil
}

func getIndexes(ctx context.Context, db *sql.DB, database, table string) ([]IndexInfo, error) {
	rows, err := db.QueryContext(ctx, `
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

func getForeignKeys(ctx context.Context, db *sql.DB, database, table string) ([]ForeignKeyInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			k.CONSTRAINT_NAME,
			k.COLUMN_NAME,
			k.REFERENCED_TABLE_SCHEMA,
			k.REFERENCED_TABLE_NAME,
			k.REFERENCED_COLUMN_NAME,
			r.DELETE_RULE,
			r.UPDATE_RULE
		FROM information_schema.KEY_COLUMN_USAGE k
		JOIN information_schema.REFERENTIAL_CONSTRAINTS r
			ON r.CONSTRAINT_SCHEMA = k.TABLE_SCHEMA
			AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
			AND r.TABLE_NAME = k.TABLE_NAME
		WHERE k.TABLE_SCHEMA = ? AND k.TABLE_NAME = ?
			AND k.REFERENCED_TABLE_NAME IS NOT NULL
		ORDER BY k.CONSTRAINT_NAME, k.ORDINAL_POSITION
	`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fkMap := make(map[string]*ForeignKeyInfo)
	var order []string

	for rows.Next() {
		var name, col, refSchema, refTable, refCol, deleteRule, updateRule string
		if err := rows.Scan(&name, &col, &refSchema, &refTable, &refCol, &deleteRule, &updateRule); err != nil {
			return nil, err
		}

		if _, exists := fkMap[name]; !exists {
			fkMap[name] = &ForeignKeyInfo{
				Name:             name,
				ReferencedSchema: refSchema,
				ReferencedTable:  refTable,
				DeleteRule:       deleteRule,
				UpdateRule:       updateRule,
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

func getInboundForeignKeys(ctx context.Context, db *sql.DB, database, table string) ([]ForeignKeyInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			k.CONSTRAINT_NAME,
			k.TABLE_SCHEMA,
			k.TABLE_NAME,
			k.COLUMN_NAME,
			k.REFERENCED_COLUMN_NAME,
			r.DELETE_RULE,
			r.UPDATE_RULE
		FROM information_schema.KEY_COLUMN_USAGE k
		JOIN information_schema.REFERENTIAL_CONSTRAINTS r
			ON r.CONSTRAINT_SCHEMA = k.TABLE_SCHEMA
			AND r.CONSTRAINT_NAME = k.CONSTRAINT_NAME
			AND r.TABLE_NAME = k.TABLE_NAME
		WHERE k.REFERENCED_TABLE_SCHEMA = ? AND k.REFERENCED_TABLE_NAME = ?
		ORDER BY k.TABLE_NAME, k.CONSTRAINT_NAME, k.ORDINAL_POSITION
	`, database, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fkMap := make(map[string]*ForeignKeyInfo)
	var order []string

	for rows.Next() {
		var name, childSchema, childTable, col, refCol, deleteRule, updateRule string
		if err := rows.Scan(&name, &childSchema, &childTable, &col, &refCol, &deleteRule, &updateRule); err != nil {
			return nil, err
		}

		key := childSchema + "." + childTable + "." + name
		if _, exists := fkMap[key]; !exists {
			fkMap[key] = &ForeignKeyInfo{
				Name:             name,
				ChildSchema:      childSchema,
				ChildTable:       childTable,
				ReferencedTable:  table,
				ReferencedSchema: database,
				DeleteRule:       deleteRule,
				UpdateRule:       updateRule,
			}
			order = append(order, key)
		}
		fkMap[key].Columns = append(fkMap[key].Columns, col)
		fkMap[key].ReferencedCols = append(fkMap[key].ReferencedCols, refCol)
	}

	var result []ForeignKeyInfo
	for _, key := range order {
		result = append(result, *fkMap[key])
	}
	return result, nil
}

func getTriggers(ctx context.Context, db *sql.DB, database, table string) ([]TriggerInfo, error) {
	rows, err := db.QueryContext(ctx, `
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

func getColumns(ctx context.Context, db *sql.DB, database, table string) ([]ColumnInfo, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT
			COLUMN_NAME,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			ORDINAL_POSITION,
			CHARACTER_SET_NAME,
			COLLATION_NAME,
			EXTRA
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
		var defaultVal, charSet, collation, extra sql.NullString

		if err := rows.Scan(&c.Name, &c.Type, &nullable, &defaultVal, &c.Position, &charSet, &collation, &extra); err != nil {
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
		if extra.Valid && strings.Contains(strings.ToUpper(extra.String), "STORED GENERATED") {
			c.IsStoredGenerated = true
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
