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
	AddColumn           DDLOperation = "ADD_COLUMN"
	DropColumn          DDLOperation = "DROP_COLUMN"
	ModifyColumn        DDLOperation = "MODIFY_COLUMN"
	ChangeColumn        DDLOperation = "CHANGE_COLUMN"
	AddIndex            DDLOperation = "ADD_INDEX"
	DropIndex           DDLOperation = "DROP_INDEX"
	AddForeignKey       DDLOperation = "ADD_FOREIGN_KEY"
	DropForeignKey      DDLOperation = "DROP_FOREIGN_KEY"
	AddPrimaryKey       DDLOperation = "ADD_PRIMARY_KEY"
	DropPrimaryKey      DDLOperation = "DROP_PRIMARY_KEY"
	RenameTable         DDLOperation = "RENAME_TABLE"
	ChangeEngine        DDLOperation = "CHANGE_ENGINE"
	ChangeCharset       DDLOperation = "CHANGE_CHARSET"  // ALTER TABLE ... CHARACTER SET = ... (table default only)
	ConvertCharset      DDLOperation = "CONVERT_CHARSET" // ALTER TABLE ... CONVERT TO CHARACTER SET ... (rewrites all columns)
	ChangeRowFormat     DDLOperation = "CHANGE_ROW_FORMAT"
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
	AddCheckConstraint  DDLOperation = "ADD_CHECK_CONSTRAINT"
	OtherDDL            DDLOperation = "OTHER"

	// Table option operations (metadata-only, INPLACE LOCK=NONE)
	KeyBlockSize    DDLOperation = "KEY_BLOCK_SIZE"
	StatsOption     DDLOperation = "STATS_OPTION"
	TableEncryption DDLOperation = "TABLE_ENCRYPTION"

	// Multi-op combined patterns
	ChangeIndexType   DDLOperation = "CHANGE_INDEX_TYPE"   // DROP INDEX + ADD INDEX (same name)
	ReplacePrimaryKey DDLOperation = "REPLACE_PRIMARY_KEY" // DROP PRIMARY KEY + ADD PRIMARY KEY

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

// SubOperation holds per-sub-operation details for a multi-op ALTER TABLE.
// Each entry in SubOperations corresponds to one clause in the compound ALTER.
type SubOperation struct {
	Op                DDLOperation
	ColumnName        string   // ADD/DROP/MODIFY/CHANGE COLUMN (new name for CHANGE)
	OldColumnName     string   // CHANGE COLUMN original name
	NewColumnType     string   // CHANGE/MODIFY COLUMN base type
	NewColumnCharset  string   // MODIFY COLUMN explicit CHARACTER SET
	NewColumnNullable *bool    // MODIFY COLUMN NULL/NOT NULL
	IsFirstAfter      bool     // ADD/MODIFY COLUMN ... FIRST|AFTER
	IndexName         string   // ADD/DROP INDEX, ADD FK, RENAME INDEX
	IndexColumns      []string // ADD PRIMARY KEY / ADD INDEX columns
	IsUniqueIndex     bool     // ADD UNIQUE KEY/INDEX
	HasAutoIncrement  bool     // ADD COLUMN ... AUTO_INCREMENT
	HasNotNull        bool     // ADD COLUMN ... NOT NULL
	IsGeneratedStored bool     // ADD/MODIFY ... AS (...) STORED
	IsGeneratedColumn bool     // ADD/MODIFY ... AS (...) expression
	NewEngine         string   // ENGINE=<name>
	CheckExpr         string   // ADD CONSTRAINT CHECK (expr)
}

// ParsedSQL holds the result of parsing a SQL statement.
type ParsedSQL struct {
	Type              StatementType
	RawSQL            string
	Database          string // extracted from qualified table name if present
	Table             string
	DDLOp             DDLOperation
	DMLOp             DMLOperation
	WhereClause       string // for DML: the WHERE as string
	HasWhere          bool
	ColumnName        string         // for ADD/DROP/MODIFY COLUMN
	OldColumnName     string         // for CHANGE COLUMN
	NewColumnName     string         // for CHANGE COLUMN
	NewColumnType     string         // for CHANGE/MODIFY COLUMN: the new column type (e.g. "decimal(14,4)")
	NewColumnCharset  string         // for MODIFY COLUMN: explicit CHARACTER SET clause if present (lowercase)
	NewColumnNullable *bool          // for MODIFY COLUMN: nil=unspecified, *true=NULL, *false=NOT NULL
	ColumnDef         string         // full column definition for ADD COLUMN
	IsFirstAfter      bool           // ADD COLUMN/MODIFY COLUMN ... FIRST or AFTER
	IndexName         string         // for ADD/DROP INDEX
	HasNotNull        bool           // ADD COLUMN ... NOT NULL
	HasDefault        bool           // ADD COLUMN ... DEFAULT
	HasAutoIncrement  bool           // ADD COLUMN ... AUTO_INCREMENT
	IsGeneratedStored bool           // ADD/MODIFY COLUMN ... AS (...) STORED
	IsGeneratedColumn bool           // ADD/MODIFY COLUMN has an AS (...) expression (STORED or VIRTUAL)
	SubOperations     []SubOperation // for multi-op ALTER TABLE: per-sub-op details
	TablespaceName    string         // for ALTER TABLESPACE
	NewTablespaceName string         // for ALTER TABLESPACE ... RENAME TO
	IndexColumns      []string       // for ADD PRIMARY KEY / ADD INDEX: the indexed column names
	IsUniqueIndex     bool           // true when ADD UNIQUE KEY/INDEX
	NewEngine         string         // for ENGINE=<name>: the target engine (lowercased)
	CheckExpr         string         // for ADD CONSTRAINT ... CHECK: the check expression
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
	if before, after, ok := strings.Cut(name, "."); ok {
		return strings.Trim(before, "`"), strings.Trim(after, "`")
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
		if t, ok := expr.(*sqlparser.AliasedTableExpr); ok {
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
			subOp := extractAlterOpDetails(opt)
			result.SubOperations = append(result.SubOperations, subOp)
			// Propagate AUTO_INCREMENT flag so the analyzer can apply the correct
			// INPLACE+SHARED classification even inside a multi-op ALTER.
			if subOp.HasAutoIncrement {
				result.HasAutoIncrement = true
			}
		}
		return
	}

	// Single operation
	result.DDLOp = classifySingleAlterOp(alter.AlterOptions[0])

	// Extract details via the shared helper and populate SubOperations[0].
	subOp := extractAlterOpDetails(alter.AlterOptions[0])
	result.SubOperations = []SubOperation{subOp}

	// Copy common fields to top-level ParsedSQL for backward compatibility with
	// all existing single-op analyzer paths.
	result.ColumnName = subOp.ColumnName
	result.OldColumnName = subOp.OldColumnName
	result.NewColumnType = subOp.NewColumnType
	result.NewColumnCharset = subOp.NewColumnCharset
	result.NewColumnNullable = subOp.NewColumnNullable
	result.IsFirstAfter = subOp.IsFirstAfter
	result.IndexName = subOp.IndexName
	result.IndexColumns = subOp.IndexColumns
	result.IsUniqueIndex = subOp.IsUniqueIndex
	result.HasAutoIncrement = subOp.HasAutoIncrement
	result.HasNotNull = subOp.HasNotNull
	result.IsGeneratedStored = subOp.IsGeneratedStored
	result.IsGeneratedColumn = subOp.IsGeneratedColumn
	result.NewEngine = subOp.NewEngine
	result.CheckExpr = subOp.CheckExpr

	// Handle fields not in SubOperation (single-op only).
	switch opt := alter.AlterOptions[0].(type) {
	case *sqlparser.AddColumns:
		if len(opt.Columns) > 0 {
			col := opt.Columns[0]
			result.ColumnDef = sqlparser.String(col)
			if col.Type.Options != nil && col.Type.Options.Default != nil {
				result.HasDefault = true
			}
		}
	case *sqlparser.ModifyColumn:
		result.ColumnDef = sqlparser.String(opt.NewColDefinition)
	case *sqlparser.ChangeColumn:
		result.NewColumnName = opt.NewColDefinition.Name.String()
		result.ColumnDef = sqlparser.String(opt.NewColDefinition)
	}
}

// extractAlterOpDetails classifies a single ALTER TABLE option and extracts all
// per-op metadata into a SubOperation. Used for both multi-op and single-op paths.
func extractAlterOpDetails(opt sqlparser.AlterOption) SubOperation {
	subOp := SubOperation{Op: classifySingleAlterOp(opt)}

	switch o := opt.(type) {
	case *sqlparser.AddColumns:
		if len(o.Columns) > 0 {
			col := o.Columns[0]
			subOp.ColumnName = col.Name.String()
			if col.Type.Options != nil {
				if col.Type.Options.Null != nil && !*col.Type.Options.Null {
					subOp.HasNotNull = true
				}
				if col.Type.Options.Autoincrement {
					subOp.HasAutoIncrement = true
				}
				if col.Type.Options.As != nil {
					subOp.IsGeneratedColumn = true
					if col.Type.Options.Storage == sqlparser.StoredStorage {
						subOp.IsGeneratedStored = true
					}
				}
			}
			if o.First || o.After != nil {
				subOp.IsFirstAfter = true
			}
		}

	case *sqlparser.DropColumn:
		subOp.ColumnName = o.Name.Name.String()

	case *sqlparser.ModifyColumn:
		subOp.ColumnName = o.NewColDefinition.Name.String()
		if o.NewColDefinition.Type != nil {
			subOp.NewColumnType = baseColumnTypeString(o.NewColDefinition.Type)
			if o.NewColDefinition.Type.Charset.Name != "" {
				subOp.NewColumnCharset = strings.ToLower(o.NewColDefinition.Type.Charset.Name)
			}
			if o.NewColDefinition.Type.Options != nil {
				subOp.NewColumnNullable = o.NewColDefinition.Type.Options.Null
				if o.NewColDefinition.Type.Options.As != nil {
					subOp.IsGeneratedColumn = true
					if o.NewColDefinition.Type.Options.Storage == sqlparser.StoredStorage {
						subOp.IsGeneratedStored = true
					}
				}
			}
		}
		if o.First || o.After != nil {
			subOp.IsFirstAfter = true
		}

	case *sqlparser.ChangeColumn:
		subOp.OldColumnName = o.OldColumn.Name.String()
		subOp.ColumnName = o.NewColDefinition.Name.String() // new column name
		if o.NewColDefinition.Type != nil {
			subOp.NewColumnType = baseColumnTypeString(o.NewColDefinition.Type)
		}

	case *sqlparser.AddIndexDefinition:
		subOp.IndexName = o.IndexDefinition.Info.Name.String()
		subOp.IsUniqueIndex = o.IndexDefinition.Info.Type == sqlparser.IndexTypeUnique
		for _, col := range o.IndexDefinition.Columns {
			if !col.Column.IsEmpty() {
				subOp.IndexColumns = append(subOp.IndexColumns, col.Column.String())
			}
		}

	case *sqlparser.DropKey:
		subOp.IndexName = o.Name.String()

	case *sqlparser.AddConstraintDefinition:
		if chk, ok := o.ConstraintDefinition.Details.(*sqlparser.CheckConstraintDefinition); ok {
			subOp.CheckExpr = sqlparser.String(chk.Expr)
		} else {
			subOp.IndexName = o.ConstraintDefinition.Name.String()
		}

	case *sqlparser.RenameIndex:
		subOp.IndexName = o.OldName.String()

	case sqlparser.TableOptions:
		for _, tableOpt := range o {
			if strings.ToUpper(tableOpt.Name) == "ENGINE" && tableOpt.String != "" {
				subOp.NewEngine = strings.ToLower(tableOpt.String)
				break
			}
		}
	}

	return subOp
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
		if _, ok := opt.ConstraintDefinition.Details.(*sqlparser.CheckConstraintDefinition); ok {
			return AddCheckConstraint
		}
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

// baseColumnTypeString returns only the data type portion of a Vitess ColumnType —
// type keyword + length/scale + UNSIGNED/ZEROFILL + enum values — without column-level
// options (NULL / NOT NULL, DEFAULT, AUTO_INCREMENT, COLLATE, etc.).
//
// This matches the format of INFORMATION_SCHEMA.COLUMNS.COLUMN_TYPE, enabling accurate
// type comparisons in the analyzer (e.g. detecting a true type change vs a rename-only).
func baseColumnTypeString(ct *sqlparser.ColumnType) string {
	if ct == nil {
		return ""
	}
	baseCT := &sqlparser.ColumnType{
		Type:       ct.Type,
		Length:     ct.Length,
		Scale:      ct.Scale,
		Unsigned:   ct.Unsigned,
		Zerofill:   ct.Zerofill,
		EnumValues: ct.EnumValues,
		// Charset omitted: captured separately in NewColumnCharset.
		// Options omitted: NULL/NOT NULL, DEFAULT, AUTO_INCREMENT, etc.
	}
	typeBuf := sqlparser.NewTrackedBuffer(nil)
	baseCT.Format(typeBuf)
	return strings.ToLower(typeBuf.String())
}
