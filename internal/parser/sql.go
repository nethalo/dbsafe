package parser

import (
	"fmt"
	"strings"
	"sync"

	"vitess.io/vitess/go/vt/sqlparser"
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
	ChangeCharset   DDLOperation = "CHANGE_CHARSET"
	ChangeRowFormat DDLOperation = "CHANGE_ROW_FORMAT"
	AddPartition    DDLOperation = "ADD_PARTITION"
	DropPartition   DDLOperation = "DROP_PARTITION"
	SetDefault      DDLOperation = "SET_DEFAULT"
	DropDefault     DDLOperation = "DROP_DEFAULT"
	MultipleOps     DDLOperation = "MULTIPLE_OPS"
	CreateTable     DDLOperation = "CREATE_TABLE"
	OtherDDL        DDLOperation = "OTHER"
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
	ColumnName    string // for ADD/DROP/MODIFY COLUMN
	OldColumnName string // for CHANGE COLUMN
	NewColumnName string // for CHANGE COLUMN
	ColumnDef     string // full column definition for ADD COLUMN
	IsFirstAfter  bool   // ADD COLUMN ... FIRST or AFTER
	IndexName     string // for ADD/DROP INDEX
	HasNotNull    bool   // ADD COLUMN ... NOT NULL
	HasDefault    bool   // ADD COLUMN ... DEFAULT
	DDLOperations []DDLOperation // for multi-op ALTER TABLE
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

// Parse parses a SQL statement and extracts information needed for analysis.
func Parse(sql string) (*ParsedSQL, error) {
	sql = strings.TrimSpace(sql)
	sql = strings.TrimRight(sql, ";")

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
		}
	}

	if len(alter.AlterOptions) == 0 {
		result.DDLOp = OtherDDL
		return
	}

	// If multiple operations, mark as such
	if len(alter.AlterOptions) > 1 {
		result.DDLOp = MultipleOps
		for _, opt := range alter.AlterOptions {
			result.DDLOperations = append(result.DDLOperations, classifySingleAlterOp(opt))
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
		}

	case *sqlparser.DropColumn:
		result.ColumnName = opt.Name.Name.String()

	case *sqlparser.ModifyColumn:
		result.ColumnName = opt.NewColDefinition.Name.String()
		result.ColumnDef = sqlparser.String(opt.NewColDefinition)

	case *sqlparser.ChangeColumn:
		result.OldColumnName = opt.OldColumn.Name.String()
		result.NewColumnName = opt.NewColDefinition.Name.String()
		result.ColumnDef = sqlparser.String(opt.NewColDefinition)

	case *sqlparser.AddIndexDefinition:
		result.IndexName = opt.IndexDefinition.Info.Name.String()

	case *sqlparser.DropKey:
		result.IndexName = opt.Name.String()
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
		if opt.IndexDefinition.Info.Type == sqlparser.IndexTypePrimary {
			return AddPrimaryKey
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
	case *sqlparser.AddConstraintDefinition:
		return AddForeignKey
	case *sqlparser.AlterCharset:
		return ChangeCharset
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
			switch tableOpt.Name {
			case "ENGINE":
				return ChangeEngine
			case "ROW_FORMAT":
				return ChangeRowFormat
			}
		}
		return OtherDDL
	default:
		return OtherDDL
	}
}
