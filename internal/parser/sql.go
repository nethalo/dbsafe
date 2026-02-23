package parser

import (
	"fmt"
	"regexp"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/sqlparser"
)

// Pre-pass regexes for statements Vitess can't parse or loses info from.
var (
	// OPTIMIZE TABLE [NO_WRITE_TO_BINLOG|LOCAL] <tbl>
	reOptimizeTable = regexp.MustCompile(`(?i)^OPTIMIZE\s+(?:NO_WRITE_TO_BINLOG\s+|LOCAL\s+)?TABLE\s+(\S+)`)
	// ALTER TABLESPACE <name> RENAME TO <new_name>
	reAlterTablespace = regexp.MustCompile(`(?i)^ALTER\s+TABLESPACE\s+(\S+)\s+RENAME\s+TO\s+(\S+)`)
)

// StatementType classifies the SQL statement.
type StatementType string

const (
	DDL     StatementType = "DDL"
	DML     StatementType = "DML"
	Unknown StatementType = "UNKNOWN"
)

// DDLOperation enumerates specific DDL operations we analyze.
type DDLOperation string

const (
	AddColumn       DDLOperation = "ADD_COLUMN"
	DropColumn      DDLOperation = "DROP_COLUMN"
	ModifyColumn    DDLOperation = "MODIFY_COLUMN"
	ChangeColumn    DDLOperation = "CHANGE_COLUMN"
	AddIndex        DDLOperation = "ADD_INDEX"
	DropIndex       DDLOperation = "DROP_INDEX"
	AddForeignKey   DDLOperation = "ADD_FOREIGN_KEY"
	DropForeignKey  DDLOperation = "DROP_FOREIGN_KEY"
	AddPrimaryKey   DDLOperation = "ADD_PRIMARY_KEY"
	DropPrimaryKey  DDLOperation = "DROP_PRIMARY_KEY"
	RenameTable     DDLOperation = "RENAME_TABLE"
	ChangeEngine    DDLOperation = "CHANGE_ENGINE"
	ChangeCharset   DDLOperation = "CHANGE_CHARSET"   // ALTER TABLE ... CHARACTER SET = ... (table default only)
	ConvertCharset  DDLOperation = "CONVERT_CHARSET"  // ALTER TABLE ... CONVERT TO CHARACTER SET ... (rewrites all columns)
	ChangeRowFormat DDLOperation = "CHANGE_ROW_FORMAT"
	AddPartition        DDLOperation = "ADD_PARTITION"
	DropPartition       DDLOperation = "DROP_PARTITION"
	ReorganizePartition DDLOperation = "REORGANIZE_PARTITION"
	RebuildPartition    DDLOperation = "REBUILD_PARTITION"
	TruncatePartition   DDLOperation = "TRUNCATE_PARTITION"
	SetDefault          DDLOperation = "SET_DEFAULT"
	DropDefault         DDLOperation = "DROP_DEFAULT"
	RenameIndex         DDLOperation = "RENAME_INDEX"
	AddFulltextIndex    DDLOperation = "ADD_FULLTEXT_INDEX"
	AddSpatialIndex     DDLOperation = "ADD_SPATIAL_INDEX"
	ChangeAutoIncrement DDLOperation = "CHANGE_AUTO_INCREMENT"
	ForceRebuild        DDLOperation = "FORCE_REBUILD"
	MultipleOps         DDLOperation = "MULTIPLE_OPS"
	CreateTable         DDLOperation = "CREATE_TABLE"
	OtherDDL            DDLOperation = "OTHER"

	// Table option operations (metadata-only, INPLACE LOCK=NONE)
	KeyBlockSize    DDLOperation = "KEY_BLOCK_SIZE"
	StatsOption     DDLOperation = "STATS_OPTION"
	TableEncryption DDLOperation = "TABLE_ENCRYPTION"

	// Multi-op combined patterns
	ChangeIndexType    DDLOperation = "CHANGE_INDEX_TYPE"    // DROP INDEX + ADD INDEX (same name)
	ReplacePrimaryKey  DDLOperation = "REPLACE_PRIMARY_KEY"  // DROP PRIMARY KEY + ADD PRIMARY KEY

	// Statement-level DDL operations (not ALTER TABLE sub-operations)
	OptimizeTable   DDLOperation = "OPTIMIZE_TABLE"   // OPTIMIZE TABLE <tbl>
	AlterTablespace DDLOperation = "ALTER_TABLESPACE" // ALTER TABLESPACE <name> RENAME TO <new>
)

// DMLOperation enumerates DML operations.
type DMLOperation string

const (
	Delete   DMLOperation = "DELETE"
	Update   DMLOperation = "UPDATE"
	Insert   DMLOperation = "INSERT"
	LoadData DMLOperation = "LOAD_DATA"
)

// ParsedSQL holds the result of parsing a SQL statement.
type ParsedSQL struct {
	Type          StatementType
	RawSQL        string
	Database      string // extracted from qualified table name if present
	Table         string
	DDLOp         DDLOperation
	DMLOp         DMLOperation
	WhereClause   string // for DML: the WHERE as string
	HasWhere      bool
	ColumnName    string         // for ADD/DROP/MODIFY COLUMN
	OldColumnName string         // for CHANGE COLUMN
	NewColumnName string         // for CHANGE COLUMN
	NewColumnType     string         // for CHANGE/MODIFY COLUMN: the new column type (e.g. "decimal(14,4)")
	NewColumnCharset  string         // for MODIFY COLUMN: explicit CHARACTER SET clause if present (lowercase)
	NewColumnNullable *bool          // for MODIFY COLUMN: nil=unspecified, *true=NULL, *false=NOT NULL
	ColumnDef         string         // full column definition for ADD COLUMN
	IsFirstAfter      bool           // ADD COLUMN/MODIFY COLUMN ... FIRST or AFTER
	IndexName         string         // for ADD/DROP INDEX
	HasNotNull        bool           // ADD COLUMN ... NOT NULL
	HasDefault        bool           // ADD COLUMN ... DEFAULT
	HasAutoIncrement  bool           // ADD COLUMN ... AUTO_INCREMENT
	IsGeneratedStored  bool           // ADD/MODIFY COLUMN ... AS (...) STORED
	IsGeneratedColumn  bool           // ADD/MODIFY COLUMN has an AS (...) expression (STORED or VIRTUAL)
	DDLOperations      []DDLOperation // for multi-op ALTER TABLE
	TablespaceName    string         // for ALTER TABLESPACE
	NewTablespaceName string         // for ALTER TABLESPACE ... RENAME TO
	IndexColumns      []string       // for ADD PRIMARY KEY / ADD INDEX: the indexed column names
	NewEngine         string         // for ENGINE=<name>: the target engine (lowercased)
}

var (
	parserOnce      sync.Once
	globalParser    *sqlparser.Parser
	globalParserErr error
)

func getParser() (*sqlparser.Parser, error) {
	parserOnce.Do(func() {
		globalParser, globalParserErr = sqlparser.New(sqlparser.Options{})
	})
	return globalParser, globalParserErr
}

// splitQualified splits a possibly-qualified name (db.table or table) into (db, name).
func splitQualified(name string) (string, string) {
	name = strings.Trim(name, "`")
	if idx := strings.IndexByte(name, '.'); idx >= 0 {
		return strings.Trim(name[:idx], "`"), strings.Trim(name[idx+1:], "`")
	}
	return "", name
}

// Parse parses a SQL statement and extracts information needed for analysis.
func Parse(sql string) (*ParsedSQL, error) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimRight(sql, ";")

	// Pre-pass: OPTIMIZE TABLE — Vitess parses this as OtherAdmin without preserving the table name.
	if m := reOptimizeTable.FindStringSubmatch(sql); m != nil {
		db, table := splitQualified(m[1])
		return &ParsedSQL{
			Type:     DDL,
			RawSQL:   sql,
			DDLOp:    OptimizeTable,
			Database: db,
			Table:    table,
		}, nil
	}

	// Pre-pass: ALTER TABLESPACE ... RENAME TO — Vitess returns a parse error for this statement.
	if m := reAlterTablespace.FindStringSubmatch(sql); m != nil {
		return &ParsedSQL{
			Type:              DDL,
			RawSQL:            sql,
			DDLOp:             AlterTablespace,
			TablespaceName:    strings.Trim(m[1], "`"),
			NewTablespaceName: strings.Trim(m[2], "`"),
		}, nil
	}

	p, err := getParser()
	if err != nil {
		return nil, fmt.Errorf("creating parser: %w", err)
	}

	stmt, err := p.Parse(sql)
	if err != nil {
		return nil, fmt.Errorf("parsing SQL: %w", err)
	}

	result := &ParsedSQL{
		RawSQL: sql,
	}

	switch s := stmt.(type) {
	case *sqlparser.AlterTable:
		result.Type = DDL
		result.Database, result.Table = extractTableName(s.Table)
		classifyAlterTable(s, result)

	case *sqlparser.RenameTable:
		result.Type = DDL
		result.DDLOp = RenameTable
		if len(s.TablePairs) > 0 {
			result.Database, result.Table = extractTableName(s.TablePairs[0].FromTable)
		}

	case *sqlparser.CreateTable:
		result.Type = DDL
		result.DDLOp = CreateTable
		result.Database, result.Table = extractTableName(s.Table)

	case *sqlparser.Delete:
		result.Type = DML
		result.DMLOp = Delete
		if len(s.TableExprs) > 0 {
			result.Database, result.Table = extractFromTableExprs(s.TableExprs)
		}
		extractWhere(s.Where, result)

	case *sqlparser.Update:
		result.Type = DML
		result.DMLOp = Update
		if len(s.TableExprs) > 0 {
			result.Database, result.Table = extractFromTableExprs(s.TableExprs)
		}
		extractWhere(s.Where, result)

	case *sqlparser.Insert:
		result.Type = DML
		result.DMLOp = Insert
		if s.Table != nil {
			if tn, ok := s.Table.Expr.(sqlparser.TableName); ok {
				result.Database, result.Table = extractTableName(tn)
			}
		}

	case *sqlparser.Load:
		result.Type = DML
		result.DMLOp = LoadData
		// Note: Vitess doesn't parse LOAD DATA details, so we can't extract table name

	default:
		result.Type = Unknown
	}

	return result, nil
}

func extractTableName(tn sqlparser.TableName) (string, string) {
	db := tn.Qualifier.String()
	table := tn.Name.String()
	return db, table
}

func extractFromTableExprs(exprs sqlparser.TableExprs) (string, string) {
	for _, expr := range exprs {
		switch t := expr.(type) {
		case *sqlparser.AliasedTableExpr:
			if tn, ok := t.Expr.(sqlparser.TableName); ok {
				return extractTableName(tn)
			}
		}
	}
	return "", ""
}

func extractWhere(where *sqlparser.Where, result *ParsedSQL) {
	if where != nil {
		result.WhereClause = sqlparser.String(where.Expr)
		result.HasWhere = true
	}
}

func classifyAlterTable(alter *sqlparser.AlterTable, result *ParsedSQL) {
	// Partition operations live in PartitionSpec, not AlterOptions.
	if alter.PartitionSpec != nil {
		switch alter.PartitionSpec.Action {
		case sqlparser.AddAction:
			result.DDLOp = AddPartition
			return
		case sqlparser.DropAction:
			result.DDLOp = DropPartition
			return
		case sqlparser.ReorganizeAction:
			result.DDLOp = ReorganizePartition
			return
		case sqlparser.RebuildAction:
			result.DDLOp = RebuildPartition
			return
		case sqlparser.TruncateAction:
			result.DDLOp = TruncatePartition
			return
		}
	}

	if len(alter.AlterOptions) == 0 {
		result.DDLOp = OtherDDL
		return
	}

	// If multiple operations, check for well-known two-op patterns before falling back.
	if len(alter.AlterOptions) > 1 {
		// Pattern: exactly DROP INDEX + ADD INDEX on the same index name → index type change.
		if len(alter.AlterOptions) == 2 {
			if indexName, ok := detectDropAddIndexPattern(alter.AlterOptions); ok {
				result.DDLOp = ChangeIndexType
				result.IndexName = indexName
				return
			}
			// Pattern: exactly DROP PRIMARY KEY + ADD PRIMARY KEY → primary key replacement.
			if detectDropAddPKPattern(alter.AlterOptions) {
				result.DDLOp = ReplacePrimaryKey
				return
			}
		}

		result.DDLOp = MultipleOps
		for _, opt := range alter.AlterOptions {
			result.DDLOperations = append(result.DDLOperations, classifySingleAlterOp(opt))
			// Propagate AUTO_INCREMENT flag so the analyzer can apply the correct
			// INPLACE+SHARED classification even inside a multi-op ALTER.
			if addCols, ok := opt.(*sqlparser.AddColumns); ok {
				if len(addCols.Columns) > 0 && addCols.Columns[0].Type.Options != nil &&
					addCols.Columns[0].Type.Options.Autoincrement {
					result.HasAutoIncrement = true
				}
			}
		}
		return
	}

	// Single operation
	result.DDLOp = classifySingleAlterOp(alter.AlterOptions[0])

	// Extract details for single operations
	switch opt := alter.AlterOptions[0].(type) {
	case *sqlparser.AddColumns:
		if len(opt.Columns) > 0 {
			col := opt.Columns[0]
			result.ColumnName = col.Name.String()
			result.ColumnDef = sqlparser.String(col)

			// Check for NOT NULL
			if col.Type.Options != nil && col.Type.Options.Null != nil && !*col.Type.Options.Null {
				result.HasNotNull = true
			}

			// Check for DEFAULT
			if col.Type.Options != nil && col.Type.Options.Default != nil {
				result.HasDefault = true
			}

			// Check for FIRST/AFTER
			if opt.First || opt.After != nil {
				result.IsFirstAfter = true
			}

			// Check for AUTO_INCREMENT
			if col.Type.Options != nil && col.Type.Options.Autoincrement {
				result.HasAutoIncrement = true
			}

			// Check for generated column (STORED or VIRTUAL).
			if col.Type.Options != nil && col.Type.Options.As != nil {
				result.IsGeneratedColumn = true
				if col.Type.Options.Storage == sqlparser.StoredStorage {
					result.IsGeneratedStored = true
				}
			}
		}

	case *sqlparser.DropColumn:
		result.ColumnName = opt.Name.Name.String()

	case *sqlparser.ModifyColumn:
		result.ColumnName = opt.NewColDefinition.Name.String()
		result.ColumnDef = sqlparser.String(opt.NewColDefinition)
		if opt.NewColDefinition.Type != nil {
			typeBuf := sqlparser.NewTrackedBuffer(nil)
			opt.NewColDefinition.Type.Format(typeBuf)
			result.NewColumnType = strings.ToLower(typeBuf.String())
			// Capture explicit CHARACTER SET clause (if any).
			if opt.NewColDefinition.Type.Charset.Name != "" {
				result.NewColumnCharset = strings.ToLower(opt.NewColDefinition.Type.Charset.Name)
			}
			// Detect explicit NULL/NOT NULL specification for nullability-change detection
			if opt.NewColDefinition.Type.Options != nil {
				result.NewColumnNullable = opt.NewColDefinition.Type.Options.Null
			}
		}
		// Detect FIRST/AFTER for column reordering
		if opt.First || opt.After != nil {
			result.IsFirstAfter = true
		}
		// Detect STORED/VIRTUAL generated column in new definition
		if opt.NewColDefinition.Type != nil && opt.NewColDefinition.Type.Options != nil &&
			opt.NewColDefinition.Type.Options.As != nil {
			result.IsGeneratedColumn = true
			if opt.NewColDefinition.Type.Options.Storage == sqlparser.StoredStorage {
				result.IsGeneratedStored = true
			}
		}

	case *sqlparser.ChangeColumn:
		result.OldColumnName = opt.OldColumn.Name.String()
		result.NewColumnName = opt.NewColDefinition.Name.String()
		result.ColumnDef = sqlparser.String(opt.NewColDefinition)
		if opt.NewColDefinition.Type != nil {
			typeBuf := sqlparser.NewTrackedBuffer(nil)
			opt.NewColDefinition.Type.Format(typeBuf)
			result.NewColumnType = strings.ToLower(typeBuf.String())
		}

	case *sqlparser.AddIndexDefinition:
		result.IndexName = opt.IndexDefinition.Info.Name.String()
		// Extract column names so the analyzer can inspect their nullability (needed for ADD PRIMARY KEY).
		for _, col := range opt.IndexDefinition.Columns {
			if !col.Column.IsEmpty() {
				result.IndexColumns = append(result.IndexColumns, col.Column.String())
			}
		}

	case *sqlparser.DropKey:
		result.IndexName = opt.Name.String()

	case sqlparser.TableOptions:
		// Extract the target engine name for ENGINE= changes (used to detect same-engine rebuilds).
		for _, tableOpt := range opt {
			if strings.ToUpper(tableOpt.Name) == "ENGINE" && tableOpt.String != "" {
				result.NewEngine = strings.ToLower(tableOpt.String)
				break
			}
		}
	}
}

func classifySingleAlterOp(opt sqlparser.AlterOption) DDLOperation {
	switch opt := opt.(type) {
	case *sqlparser.AddColumns:
		return AddColumn
	case *sqlparser.DropColumn:
		return DropColumn
	case *sqlparser.ModifyColumn:
		return ModifyColumn
	case *sqlparser.ChangeColumn:
		return ChangeColumn
	case *sqlparser.AddIndexDefinition:
		switch opt.IndexDefinition.Info.Type {
		case sqlparser.IndexTypePrimary:
			return AddPrimaryKey
		case sqlparser.IndexTypeFullText:
			return AddFulltextIndex
		case sqlparser.IndexTypeSpatial:
			return AddSpatialIndex
		}
		return AddIndex
	case *sqlparser.DropKey:
		switch opt.Type {
		case sqlparser.PrimaryKeyType:
			return DropPrimaryKey
		case sqlparser.ForeignKeyType:
			return DropForeignKey
		default:
			return DropIndex
		}
	case *sqlparser.RenameIndex:
		return RenameIndex
	case *sqlparser.RenameTableName:
		return RenameTable
	case *sqlparser.Force:
		return ForceRebuild
	case *sqlparser.AddConstraintDefinition:
		return AddForeignKey
	case *sqlparser.AlterCharset:
		return ConvertCharset
	case *sqlparser.AlterColumn:
		if opt.DropDefault {
			return DropDefault
		}
		if opt.DefaultVal != nil {
			return SetDefault
		}
		return OtherDDL
	case sqlparser.TableOptions:
		for _, tableOpt := range opt {
			switch strings.ToUpper(tableOpt.Name) {
			case "ENGINE":
				return ChangeEngine
			case "ROW_FORMAT":
				return ChangeRowFormat
			case "CHARSET", "CHARACTER SET":
				return ChangeCharset
			case "AUTO_INCREMENT":
				return ChangeAutoIncrement
			case "KEY_BLOCK_SIZE":
				return KeyBlockSize
			case "STATS_PERSISTENT", "STATS_SAMPLE_PAGES", "STATS_AUTO_RECALC":
				return StatsOption
			case "ENCRYPTION":
				return TableEncryption
			}
		}
		return OtherDDL
	default:
		return OtherDDL
	}
}

// detectDropAddIndexPattern checks whether two alter options form a DROP INDEX + ADD INDEX
// on the same index name (an index type change). Returns the index name and true on match.
func detectDropAddIndexPattern(opts []sqlparser.AlterOption) (string, bool) {
	var dropName, addName string

	for _, opt := range opts {
		switch o := opt.(type) {
		case *sqlparser.DropKey:
			if o.Type != sqlparser.PrimaryKeyType && o.Type != sqlparser.ForeignKeyType {
				dropName = o.Name.String()
			}
		case *sqlparser.AddIndexDefinition:
			info := o.IndexDefinition.Info
			if info.Type != sqlparser.IndexTypePrimary {
				addName = info.Name.String()
			}
		}
	}

	if dropName != "" && addName != "" && strings.EqualFold(dropName, addName) {
		return dropName, true
	}
	return "", false
}

// detectDropAddPKPattern checks whether two alter options form a DROP PRIMARY KEY + ADD PRIMARY KEY.
func detectDropAddPKPattern(opts []sqlparser.AlterOption) bool {
	hasDrop, hasAdd := false, false
	for _, opt := range opts {
		switch o := opt.(type) {
		case *sqlparser.DropKey:
			if o.Type == sqlparser.PrimaryKeyType {
				hasDrop = true
			}
		case *sqlparser.AddIndexDefinition:
			if o.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary {
				hasAdd = true
			}
		}
	}
	return hasDrop && hasAdd
}
