package analyzer

import (
	"fmt"
	"strings"
	"time"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// RiskLevel classifies the overall risk of an operation.
type RiskLevel string

const (
	RiskSafe      RiskLevel = "SAFE"
	RiskCaution   RiskLevel = "CAUTION"
	RiskDangerous RiskLevel = "DANGEROUS"
)

// ExecutionMethod is what dbsafe recommends.
type ExecutionMethod string

const (
	ExecDirect     ExecutionMethod = "DIRECT"
	ExecGhost      ExecutionMethod = "GH-OST"
	ExecPtOSC      ExecutionMethod = "PT-ONLINE-SCHEMA-CHANGE"
	ExecChunked    ExecutionMethod = "CHUNKED"
	ExecRSU        ExecutionMethod = "RSU" // Rolling Schema Upgrade (Galera)
)

// Input holds everything the analyzer needs.
type Input struct {
	Parsed    *parser.ParsedSQL
	Meta      *mysql.TableMetadata
	Topo      *topology.Info
	Version   mysql.ServerVersion
	ChunkSize int
}

// Result holds the complete analysis output.
type Result struct {
	// Metadata
	Statement     string
	StatementType parser.StatementType
	Database      string
	Table         string
	TableMeta     *mysql.TableMetadata
	Topology      *topology.Info
	Version       mysql.ServerVersion
	AnalyzedAt    time.Time

	// DDL-specific
	DDLOp          parser.DDLOperation
	Classification DDLClassification

	// DML-specific
	DMLOp         parser.DMLOperation
	AffectedRows  int64
	AffectedPct   float64
	HasWhere      bool
	WriteSetSize  int64 // estimated bytes for write-set

	// Recommendation
	Risk            RiskLevel
	Method          ExecutionMethod
	Recommendation  string
	Warnings        []string
	ClusterWarnings []string

	// Rollback
	RollbackSQL     string
	RollbackNotes   string
	RollbackOptions []RollbackOption

	// DML script generation
	GeneratedScript string
	ScriptPath      string
	ChunkSize       int
	ChunkCount      int64
}

// RollbackOption describes one way to undo the operation.
type RollbackOption struct {
	Label       string
	SQL         string
	Description string
}

// Analyze runs the full analysis pipeline.
func Analyze(input Input) *Result {
	result := &Result{
		Statement:     input.Parsed.RawSQL,
		StatementType: input.Parsed.Type,
		Database:      input.Parsed.Database,
		Table:         input.Parsed.Table,
		TableMeta:     input.Meta,
		Topology:      input.Topo,
		Version:       input.Version,
		AnalyzedAt:    time.Now(),
		ChunkSize:     input.ChunkSize,
	}

	if result.Database == "" {
		result.Database = input.Meta.Database
	}

	switch input.Parsed.Type {
	case parser.DDL:
		analyzeDDL(input, result)
	case parser.DML:
		analyzeDML(input, result)
	}

	// Apply topology-specific warnings
	applyTopologyWarnings(input, result)

	return result
}

func analyzeDDL(input Input, result *Result) {
	result.DDLOp = input.Parsed.DDLOp

	// Warn if operation couldn't be fully parsed
	if input.Parsed.DDLOp == parser.OtherDDL {
		result.Warnings = append(result.Warnings,
			"⚠️  DDL operation could not be fully parsed. This may indicate a syntax error or unsupported operation.",
			"Please verify the SQL syntax manually before execution.",
		)
		result.Risk = RiskDangerous
	}

	// Validate column existence before proceeding
	validateColumnOperation(input, result)

	// Classify using the DDL matrix
	v := input.Version
	result.Classification = ClassifyDDLWithContext(input.Parsed, v.Major, v.Minor, v.Patch)

	// Determine risk and method based on algorithm
	// Note: Column validation may have already set Risk to RiskDangerous, which we preserve
	switch result.Classification.Algorithm {
	case AlgoInstant:
		if result.Risk != RiskDangerous {
			result.Risk = RiskSafe
		}
		result.Method = ExecDirect
		if result.Risk != RiskDangerous {
			result.Recommendation = fmt.Sprintf(
				"This operation uses INSTANT algorithm in MySQL %s. No table rebuild, no lock, no replication impact. Execute directly.",
				v.String(),
			)
		}

	case AlgoInplace:
		if result.Classification.Lock == LockNone {
			if input.Meta.TotalSize() > 10*1024*1024*1024 { // > 10 GB
				if result.Risk != RiskDangerous {
					result.Risk = RiskCaution
					result.Recommendation = "INPLACE with no lock, but table is large. I/O impact during index build. Consider scheduling during low-traffic window."
				}
			} else {
				if result.Risk != RiskDangerous {
					result.Risk = RiskSafe
					result.Recommendation = "INPLACE with no lock. Concurrent DML allowed. Safe to run directly."
				}
			}
			result.Method = ExecDirect
		} else {
			// INPLACE with lock (SHARED or EXCLUSIVE)
			// Both pt-osc and gh-ost can handle this, so we set pt-osc as method
			// but recommend both as options. The Galera override (if applicable)
			// will enforce pt-osc later in applyTopologyWarnings().
			if input.Meta.TotalSize() > 1*1024*1024*1024 { // > 1 GB
				if result.Risk != RiskDangerous {
					result.Risk = RiskDangerous
				}
				result.Method = ExecPtOSC
				if result.Risk == RiskDangerous && result.Recommendation == "" {
					result.Recommendation = "INPLACE with SHARED lock on a large table. Use pt-online-schema-change or gh-ost to avoid blocking writes."
				}
			} else {
				if result.Risk != RiskDangerous {
					result.Risk = RiskCaution
					result.Recommendation = "INPLACE with SHARED lock. Reads allowed but writes blocked. Table is small enough for direct execution during low-traffic window."
				}
				result.Method = ExecDirect
			}
		}

	case AlgoCopy:
		if input.Meta.TotalSize() > 1*1024*1024*1024 { // > 1 GB
			if result.Risk != RiskDangerous {
				result.Risk = RiskDangerous
			}
			// gh-ost vs pt-online-schema-change decision:
			//
			// gh-ost is preferred for non-Galera topologies because:
			// - No triggers: Uses binlog streaming instead of triggers, avoiding overhead on production table
			// - Pausable/resumable: Can pause and resume without starting over
			// - Better throttling: Sub-second lag detection, more granular control
			// - Testable cutover: Can test the table swap before actually doing it
			//
			// However, gh-ost is incompatible with Galera/PXC because it relies on binlog
			// streaming which conflicts with Galera's writeset replication. In Galera clusters,
			// we must use pt-online-schema-change (which uses triggers that replicate correctly).
			if input.Topo.Type == topology.Galera {
				result.Method = ExecPtOSC
				if result.Recommendation == "" {
					result.Recommendation = "COPY algorithm on a large table in Galera/PXC. Use pt-online-schema-change with --max-flow-ctl. Do NOT use gh-ost (incompatible with Galera writeset replication)."
				}
			} else {
				result.Method = ExecGhost
				if result.Recommendation == "" {
					result.Recommendation = "COPY algorithm on a large table. Use gh-ost (preferred) or pt-online-schema-change to avoid blocking writes."
				}
			}
		} else {
			if result.Risk != RiskDangerous {
				result.Risk = RiskCaution
				result.Recommendation = "COPY algorithm rebuilds the table. Table is small enough for direct execution during low-traffic window."
			}
			result.Method = ExecDirect
		}
	}

	// Generate rollback SQL
	generateDDLRollback(input, result)
}

func analyzeDML(input Input, result *Result) {
	result.DMLOp = input.Parsed.DMLOp
	result.HasWhere = input.Parsed.HasWhere
	result.AffectedRows = estimateAffectedRows(input)

	if input.Meta.RowCount > 0 {
		result.AffectedPct = float64(result.AffectedRows) / float64(input.Meta.RowCount) * 100
	}

	// Estimate write-set size
	result.WriteSetSize = result.AffectedRows * input.Meta.AvgRowLength

	// Check for missing WHERE clause
	if !result.HasWhere && (result.DMLOp == parser.Delete || result.DMLOp == parser.Update) {
		result.Risk = RiskDangerous
		result.Warnings = append(result.Warnings, "No WHERE clause! This will affect ALL rows in the table.")
	}

	// Determine chunking need
	const chunkThreshold int64 = 100000 // 100K rows
	if result.AffectedRows > chunkThreshold {
		result.Risk = RiskDangerous
		result.Method = ExecChunked
		result.ChunkCount = (result.AffectedRows + int64(input.ChunkSize) - 1) / int64(input.ChunkSize)
		result.Recommendation = fmt.Sprintf(
			"Affecting ~%s rows (%.1f%%). Chunk into batches of %d rows with sleep between chunks to avoid lock contention and replication lag.",
			formatNumber(result.AffectedRows), result.AffectedPct, input.ChunkSize,
		)
	} else if result.AffectedRows > 10000 {
		result.Risk = RiskCaution
		result.Method = ExecDirect
		result.Recommendation = fmt.Sprintf(
			"Affecting ~%s rows (%.1f%%). Moderate impact. Direct execution OK during low-traffic window, but consider chunking if you want to be safe.",
			formatNumber(result.AffectedRows), result.AffectedPct,
		)
	} else {
		if result.Risk == "" {
			result.Risk = RiskSafe
		}
		result.Method = ExecDirect
		result.Recommendation = fmt.Sprintf(
			"Affecting ~%s rows (%.1f%%). Small operation. Safe to run directly.",
			formatNumber(result.AffectedRows), result.AffectedPct,
		)
	}

	// Check triggers
	for _, trigger := range input.Meta.Triggers {
		event := strings.ToUpper(trigger.Event)
		dmlOp := strings.ToUpper(string(result.DMLOp))
		if event == dmlOp {
			result.Warnings = append(result.Warnings, fmt.Sprintf(
				"Trigger %s (%s %s) will fire for each affected row. Verify target table can handle the write volume.",
				trigger.Name, trigger.Timing, trigger.Event,
			))
		}
	}

	// Generate rollback plan
	generateDMLRollback(input, result)

	// Generate chunked script if needed
	if result.Method == ExecChunked {
		generateChunkedScript(input, result)
	}
}

func estimateAffectedRows(input Input) int64 {
	// The EXPLAIN-based estimate should have been collected in plan.go
	// For now, use the table row count as a rough fallback
	// In practice, this will come from EXPLAIN on the actual DML
	if !input.Parsed.HasWhere {
		return input.Meta.RowCount
	}
	// Conservative estimate: assume EXPLAIN will be called by the caller
	// Return 0 to indicate "needs EXPLAIN"
	return 0
}

func validateColumnOperation(input Input, result *Result) {
	p := input.Parsed

	// Helper to check if column exists
	columnExists := func(colName string) bool {
		for _, col := range input.Meta.Columns {
			if col.Name == colName {
				return true
			}
		}
		return false
	}

	switch p.DDLOp {
	case parser.AddColumn:
		if columnExists(p.ColumnName) {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Column '%s' already exists! This ADD COLUMN operation will fail.", p.ColumnName))
			result.Risk = RiskDangerous
		}

	case parser.DropColumn:
		if !columnExists(p.ColumnName) {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Column '%s' does not exist! This DROP COLUMN operation will fail.", p.ColumnName))
			result.Risk = RiskDangerous
		}

	case parser.ModifyColumn:
		if !columnExists(p.ColumnName) {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Column '%s' does not exist! This MODIFY COLUMN operation will fail.", p.ColumnName))
			result.Risk = RiskDangerous
		}

	case parser.ChangeColumn:
		oldExists := columnExists(p.OldColumnName)
		newExists := columnExists(p.NewColumnName)

		if !oldExists {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Source column '%s' does not exist! This CHANGE COLUMN operation will fail.", p.OldColumnName))
			result.Risk = RiskDangerous
		}

		// Only warn about new name if it's different from old name and already exists
		if p.OldColumnName != p.NewColumnName && newExists {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("Target column name '%s' already exists! This CHANGE COLUMN operation will fail.", p.NewColumnName))
			result.Risk = RiskDangerous
		}
	}
}

func applyTopologyWarnings(input Input, result *Result) {
	switch input.Topo.Type {
	case topology.Galera:
		applyGaleraWarnings(input, result)
	case topology.GroupRepl:
		applyGRWarnings(input, result)
	case topology.AsyncReplica, topology.SemiSyncReplica:
		applyReplicationWarnings(input, result)
	}
}

func applyGaleraWarnings(input Input, result *Result) {
	// DDL: warn about TOI impact
	if result.StatementType == parser.DDL && input.Topo.GaleraOSUMethod == "TOI" {
		if result.Classification.Algorithm != AlgoInstant {
			result.ClusterWarnings = append(result.ClusterWarnings, fmt.Sprintf(
				"TOI will execute this DDL on ALL %d nodes simultaneously. Consider RSU for large operations: SET wsrep_OSU_method=RSU; then run ALTER on each node individually.",
				input.Topo.GaleraClusterSize,
			))
		}
	}

	// DML: warn about write-set size limit
	if result.StatementType == parser.DML && input.Topo.WsrepMaxWsSize > 0 {
		if result.WriteSetSize > input.Topo.WsrepMaxWsSize {
			result.ClusterWarnings = append(result.ClusterWarnings, fmt.Sprintf(
				"Estimated write-set (~%s) EXCEEDS wsrep_max_ws_size (%s). Transaction WILL be rejected by Galera. Chunking is MANDATORY.",
				humanBytes(result.WriteSetSize), humanBytes(input.Topo.WsrepMaxWsSize),
			))
			result.Risk = RiskDangerous
			result.Method = ExecChunked
		}
	}

	// Flow control warning
	if input.Topo.FlowControlPaused > 0.01 {
		result.ClusterWarnings = append(result.ClusterWarnings, fmt.Sprintf(
			"Flow control paused at %s. Cluster is already under write pressure. Consider waiting or reducing chunk size.",
			input.Topo.FlowControlPausedPct,
		))
	}

	// gh-ost incompatibility
	if result.Method == ExecGhost {
		result.ClusterWarnings = append(result.ClusterWarnings,
			"gh-ost is NOT compatible with Galera/PXC. It relies on binlog streaming which conflicts with Galera writeset replication. Use pt-online-schema-change instead.",
		)
		result.Method = ExecPtOSC
	}
}

func applyGRWarnings(input Input, result *Result) {
	// Transaction size limit
	if result.StatementType == parser.DML && input.Topo.GRTransactionLimit > 0 {
		if result.WriteSetSize > input.Topo.GRTransactionLimit {
			result.ClusterWarnings = append(result.ClusterWarnings, fmt.Sprintf(
				"Estimated write-set (~%s) EXCEEDS group_replication_transaction_size_limit (%s). Transaction will be rejected. Chunking is MANDATORY.",
				humanBytes(result.WriteSetSize), humanBytes(input.Topo.GRTransactionLimit),
			))
			result.Risk = RiskDangerous
			result.Method = ExecChunked
		}
	}

	// Multi-primary warning for DDL
	if result.StatementType == parser.DDL && input.Topo.GRMode == "MULTI-PRIMARY" {
		result.ClusterWarnings = append(result.ClusterWarnings,
			"Running DDL in multi-primary Group Replication mode. Ensure no conflicting DDL is running on other primaries.",
		)
	}
}

func applyReplicationWarnings(input Input, result *Result) {
	if input.Topo.ReplicaLagSecs != nil && *input.Topo.ReplicaLagSecs > 30 {
		result.ClusterWarnings = append(result.ClusterWarnings, fmt.Sprintf(
			"Replication lag detected: %d seconds. Large operations will increase lag further. Consider chunking with sleep.",
			*input.Topo.ReplicaLagSecs,
		))
	}
}

func generateDDLRollback(input Input, result *Result) {
	db := result.Database
	table := result.Table
	p := input.Parsed

	switch p.DDLOp {
	case parser.AddColumn:
		result.RollbackSQL = fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP COLUMN `%s`;", db, table, p.ColumnName)
		if input.Version.SupportsInstantDropColumn() {
			result.RollbackNotes = "DROP COLUMN is INSTANT in your MySQL version."
		} else {
			result.RollbackNotes = "DROP COLUMN uses INPLACE with table rebuild in your MySQL version."
		}

	case parser.DropColumn:
		result.RollbackNotes = "Cannot automatically reverse DROP COLUMN. Restore from backup or recreate the column with original definition from SHOW CREATE TABLE."

	case parser.AddIndex:
		result.RollbackSQL = fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP INDEX `%s`;", db, table, p.IndexName)
		result.RollbackNotes = "DROP INDEX is INPLACE with no lock. Very fast."

	case parser.DropIndex:
		result.RollbackNotes = "Recreate the index using the original definition from SHOW CREATE TABLE."

	case parser.RenameTable:
		result.RollbackNotes = "Reverse the RENAME TABLE with the original and new names swapped."

	default:
		result.RollbackNotes = "Review SHOW CREATE TABLE output to reconstruct the original state."
	}
}

func generateDMLRollback(input Input, result *Result) {
	db := result.Database
	table := result.Table
	ts := time.Now().Format("20060102")

	// Option A: Pre-backup
	backupTable := fmt.Sprintf("%s_backup_%s", table, ts)
	backupSQL := fmt.Sprintf("CREATE TABLE `%s`.`%s` AS\nSELECT * FROM `%s`.`%s`", db, backupTable, db, table)
	if input.Parsed.HasWhere {
		backupSQL += fmt.Sprintf("\nWHERE %s", input.Parsed.WhereClause)
	}
	backupSQL += ";"

	restoreSQL := fmt.Sprintf("INSERT INTO `%s`.`%s`\nSELECT * FROM `%s`.`%s`;", db, table, db, backupTable)

	backupSize := result.AffectedRows * input.Meta.AvgRowLength
	backupDesc := fmt.Sprintf("Create backup table before execution (~%s). Run the backup SQL first, then execute the DML.", humanBytes(backupSize))

	result.RollbackOptions = append(result.RollbackOptions, RollbackOption{
		Label:       "Pre-backup (RECOMMENDED)",
		SQL:         backupSQL + "\n\n-- Restore command:\n" + restoreSQL,
		Description: backupDesc,
	})

	// Option B: Binlog-based
	result.RollbackOptions = append(result.RollbackOptions, RollbackOption{
		Label:       "Point-in-time recovery",
		SQL:         "",
		Description: "Requires binlog_format=ROW and binlog_row_image=FULL. Use mysqlbinlog or my2sql to generate reverse SQL from binary logs.",
	})
}

func generateChunkedScript(input Input, result *Result) {
	// This is a simplified chunked script generator
	// Real implementation would detect the best column to chunk on (usually PK)
	db := result.Database
	table := result.Table
	ts := time.Now().Format("20060102_150405")

	var script strings.Builder
	script.WriteString(fmt.Sprintf("-- dbsafe generated chunked script\n"))
	script.WriteString(fmt.Sprintf("-- Table: %s.%s\n", db, table))
	script.WriteString(fmt.Sprintf("-- Estimated rows: %d\n", result.AffectedRows))
	script.WriteString(fmt.Sprintf("-- Chunk size: %d\n", input.ChunkSize))
	script.WriteString(fmt.Sprintf("-- Generated: %s\n\n", time.Now().Format(time.RFC3339)))

	script.WriteString("SET @batch_size = " + fmt.Sprintf("%d", input.ChunkSize) + ";\n")
	script.WriteString("SET @sleep_time = 0.5;\n\n")

	script.WriteString("-- Loop: execute in batches\n")
	script.WriteString("-- Adjust @batch_size and @sleep_time as needed\n")

	switch input.Parsed.DMLOp {
	case parser.Delete:
		script.WriteString(fmt.Sprintf(`
SET @affected = 1;
WHILE @affected > 0 DO
    DELETE FROM %s.%s
    WHERE %s
    LIMIT @batch_size;
    
    SET @affected = ROW_COUNT();
    SELECT CONCAT('Deleted ', @affected, ' rows') AS progress;
    
    DO SLEEP(@sleep_time);
END WHILE;
`, "`"+db+"`", "`"+table+"`", input.Parsed.WhereClause))

	case parser.Update:
		script.WriteString(fmt.Sprintf("-- UPDATE chunking requires a primary key column.\n"))
		script.WriteString(fmt.Sprintf("-- Use the PK to iterate in ranges.\n"))
		script.WriteString(fmt.Sprintf("-- Example pattern (adjust for your PK column):\n\n"))
		script.WriteString(fmt.Sprintf(`
SET @min_id = (SELECT MIN(id) FROM %s.%s WHERE %s);
SET @max_id = (SELECT MAX(id) FROM %s.%s WHERE %s);
SET @current = @min_id;

WHILE @current <= @max_id DO
    -- Replace this with your actual UPDATE statement
    -- %s
    -- AND id BETWEEN @current AND @current + @batch_size - 1;
    
    SET @current = @current + @batch_size;
    DO SLEEP(@sleep_time);
END WHILE;
`, "`"+db+"`", "`"+table+"`", input.Parsed.WhereClause,
			"`"+db+"`", "`"+table+"`", input.Parsed.WhereClause,
			input.Parsed.RawSQL))
	}

	result.GeneratedScript = script.String()
	result.ScriptPath = fmt.Sprintf("./dbsafe-plan-%s-%s-%s.sql", table, strings.ToLower(string(input.Parsed.DMLOp)), ts)
}

func humanBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
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

func formatNumber(n int64) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}
