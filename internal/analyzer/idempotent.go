package analyzer

import (
	"fmt"
	"strings"

	"github.com/nethalo/dbsafe/internal/parser"
)

// GenerateIdempotentSP generates a stored procedure that wraps the DDL in an
// existence check, making it safe to re-run. Returns (sp, warning): if the
// operation is unsupported, sp is empty and warning explains why.
func GenerateIdempotentSP(parsed *parser.ParsedSQL, database, table string) (sp string, warning string) {
	procName := fmt.Sprintf("dbsafe_idempotent_%s_%s", sanitizeIdent(database), sanitizeIdent(table))
	ddl := parsed.RawSQL

	switch parsed.DDLOp {
	// ── Column operations ────────────────────────────────────────────────────
	case parser.AddColumn:
		if parsed.ColumnName == "" {
			return "", "Cannot generate idempotent SP: column name not detected."
		}
		return buildSP(procName, "IF NOT", columnExistsCondition(database, table, parsed.ColumnName), ddl), ""

	case parser.DropColumn:
		if parsed.ColumnName == "" {
			return "", "Cannot generate idempotent SP: column name not detected."
		}
		return buildSP(procName, "IF", columnExistsCondition(database, table, parsed.ColumnName), ddl), ""

	case parser.ModifyColumn:
		if parsed.ColumnName == "" {
			return "", "Cannot generate idempotent SP: column name not detected."
		}
		return buildSP(procName, "IF", columnExistsCondition(database, table, parsed.ColumnName), ddl), ""

	case parser.ChangeColumn:
		colName := parsed.OldColumnName
		if colName == "" {
			return "", "Cannot generate idempotent SP: old column name not detected."
		}
		return buildSP(procName, "IF", columnExistsCondition(database, table, colName), ddl), ""

	// ── Index operations ─────────────────────────────────────────────────────
	case parser.AddIndex, parser.AddFulltextIndex, parser.AddSpatialIndex:
		if parsed.IndexName == "" {
			return "", "Cannot generate idempotent SP: index name not detected."
		}
		return buildSP(procName, "IF NOT", indexExistsCondition(database, table, parsed.IndexName), ddl), ""

	case parser.DropIndex:
		if parsed.IndexName == "" {
			return "", "Cannot generate idempotent SP: index name not detected."
		}
		return buildSP(procName, "IF", indexExistsCondition(database, table, parsed.IndexName), ddl), ""

	case parser.AddPrimaryKey:
		return buildSP(procName, "IF NOT", indexExistsCondition(database, table, "PRIMARY"), ddl), ""

	case parser.DropPrimaryKey:
		return buildSP(procName, "IF", indexExistsCondition(database, table, "PRIMARY"), ddl), ""

	case parser.RenameIndex:
		if parsed.IndexName == "" {
			return "", "Cannot generate idempotent SP: old index name not detected."
		}
		return buildSP(procName, "IF", indexExistsCondition(database, table, parsed.IndexName), ddl), ""

	// ── FK operations ────────────────────────────────────────────────────────
	case parser.AddForeignKey:
		if parsed.IndexName == "" {
			return "", "Cannot generate idempotent SP: FK constraint name not detected."
		}
		return buildSP(procName, "IF NOT", fkExistsCondition(database, table, parsed.IndexName), ddl), ""

	case parser.DropForeignKey:
		if parsed.IndexName == "" {
			return "", "Cannot generate idempotent SP: FK constraint name not detected."
		}
		return buildSP(procName, "IF", fkExistsCondition(database, table, parsed.IndexName), ddl), ""

	// ── Table-level operations ────────────────────────────────────────────────
	case parser.ChangeEngine:
		if parsed.NewEngine == "" {
			return "", "Cannot generate idempotent SP: target engine not detected."
		}
		return buildSP(procName, "IF NOT", engineIsCondition(database, table, parsed.NewEngine), ddl), ""

	case parser.RenameTable:
		return buildSP(procName, "IF", tableExistsCondition(database, table), ddl), ""

	// ── Unsupported ───────────────────────────────────────────────────────────
	case parser.MultipleOps:
		return "", "Cannot generate idempotent SP for compound ALTER TABLE (multiple operations). Split into separate statements."

	case parser.ConvertCharset, parser.ChangeCharset:
		return "", "Cannot generate idempotent SP for CHARACTER SET changes: the check would require inspecting every column's collation."

	case parser.AddPartition, parser.DropPartition, parser.ReorganizePartition, parser.RebuildPartition, parser.TruncatePartition:
		return "", "Cannot generate idempotent SP for partition operations (not supported in v1)."

	case parser.SetDefault, parser.DropDefault, parser.ChangeAutoIncrement,
		parser.KeyBlockSize, parser.StatsOption, parser.TableEncryption, parser.ChangeRowFormat:
		return "", "Idempotent SP not generated: metadata-only operations are already safe to re-run."

	default:
		return "", fmt.Sprintf("Cannot generate idempotent SP for operation %q.", parsed.DDLOp)
	}
}

// buildSP assembles the idempotent stored procedure SQL.
// ifKeyword is "IF" or "IF NOT"; cond is an EXISTS(...) block.
func buildSP(procName, ifKeyword, cond, ddl string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "DELIMITER //\n")
	fmt.Fprintf(&b, "DROP PROCEDURE IF EXISTS `%s`//\n", procName)
	fmt.Fprintf(&b, "CREATE PROCEDURE `%s`()\n", procName)
	fmt.Fprintf(&b, "BEGIN\n")
	fmt.Fprintf(&b, "    %s %s THEN\n", ifKeyword, cond)
	fmt.Fprintf(&b, "        %s;\n", ddl)
	fmt.Fprintf(&b, "    END IF;\n")
	fmt.Fprintf(&b, "END//\n")
	fmt.Fprintf(&b, "DELIMITER ;\n")
	fmt.Fprintf(&b, "CALL `%s`();\n", procName)
	fmt.Fprintf(&b, "DROP PROCEDURE IF EXISTS `%s`;", procName)
	return b.String()
}

// columnExistsCondition returns an EXISTS(...) block for INFORMATION_SCHEMA.COLUMNS.
func columnExistsCondition(database, table, column string) string {
	return fmt.Sprintf(
		"EXISTS (\n        SELECT 1 FROM INFORMATION_SCHEMA.COLUMNS\n        WHERE TABLE_SCHEMA = '%s'\n        AND TABLE_NAME = '%s'\n        AND COLUMN_NAME = '%s'\n    )",
		escapeSQL(database), escapeSQL(table), escapeSQL(column),
	)
}

// indexExistsCondition returns an EXISTS(...) block for INFORMATION_SCHEMA.STATISTICS.
func indexExistsCondition(database, table, indexName string) string {
	return fmt.Sprintf(
		"EXISTS (\n        SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS\n        WHERE TABLE_SCHEMA = '%s'\n        AND TABLE_NAME = '%s'\n        AND INDEX_NAME = '%s'\n    )",
		escapeSQL(database), escapeSQL(table), escapeSQL(indexName),
	)
}

// fkExistsCondition returns an EXISTS(...) block for INFORMATION_SCHEMA.TABLE_CONSTRAINTS.
func fkExistsCondition(database, table, constraintName string) string {
	return fmt.Sprintf(
		"EXISTS (\n        SELECT 1 FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS\n        WHERE TABLE_SCHEMA = '%s'\n        AND TABLE_NAME = '%s'\n        AND CONSTRAINT_NAME = '%s'\n        AND CONSTRAINT_TYPE = 'FOREIGN KEY'\n    )",
		escapeSQL(database), escapeSQL(table), escapeSQL(constraintName),
	)
}

// engineIsCondition returns an EXISTS(...) block checking the table's current engine.
func engineIsCondition(database, table, engine string) string {
	return fmt.Sprintf(
		"EXISTS (\n        SELECT 1 FROM INFORMATION_SCHEMA.TABLES\n        WHERE TABLE_SCHEMA = '%s'\n        AND TABLE_NAME = '%s'\n        AND UPPER(ENGINE) = UPPER('%s')\n    )",
		escapeSQL(database), escapeSQL(table), escapeSQL(engine),
	)
}

// tableExistsCondition returns an EXISTS(...) block checking table existence.
func tableExistsCondition(database, table string) string {
	return fmt.Sprintf(
		"EXISTS (\n        SELECT 1 FROM INFORMATION_SCHEMA.TABLES\n        WHERE TABLE_SCHEMA = '%s'\n        AND TABLE_NAME = '%s'\n    )",
		escapeSQL(database), escapeSQL(table),
	)
}

// sanitizeIdent makes a string safe for use as part of a MySQL identifier.
func sanitizeIdent(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// escapeSQL escapes single quotes in SQL string literals.
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}
