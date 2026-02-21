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

// ghostPreferredRationale explains why gh-ost is the primary recommendation
// when both tools are viable (non-Galera topologies).
const ghostPreferredRationale = "gh-ost (recommended): copies via binlog streaming — no triggers on the production " +
	"table, supports pause/resume without restarting, provides sub-second lag throttling, and allows a " +
	"testable cutover before committing.\n" +
	"pt-online-schema-change: proven trigger-based alternative; simpler setup, no binlog requirements."

// ptOSCOnlyRationale explains why gh-ost cannot be used on Galera/PXC.
const ptOSCOnlyRationale = "gh-ost is NOT compatible with Galera/PXC: it relies on binlog streaming which " +
	"conflicts with Galera's writeset replication and will corrupt the cluster. " +
	"pt-online-schema-change uses triggers that replicate correctly across all Galera nodes."

// ptOSCTriggerRationale explains why gh-ost cannot be used when the table has existing triggers.
const ptOSCTriggerRationale = "gh-ost cannot operate on tables with existing triggers: it creates a shadow " +
	"table and uses binlog streaming to replay changes, but triggers on the original table would also fire " +
	"during the shadow table population, causing data corruption or errors. " +
	"pt-online-schema-change supports --preserve-triggers to safely migrate tables that have triggers."

// ExecutionMethod is what dbsafe recommends.
type ExecutionMethod string

const (
	ExecDirect  ExecutionMethod = "DIRECT"
	ExecGhost   ExecutionMethod = "GH-OST"
	ExecPtOSC   ExecutionMethod = "PT-ONLINE-SCHEMA-CHANGE"
	ExecChunked ExecutionMethod = "CHUNKED"
	ExecRSU     ExecutionMethod = "RSU" // Rolling Schema Upgrade (Galera)
)

// ConnectionInfo holds non-sensitive connection details for command generation.
type ConnectionInfo struct {
	Host   string
	Port   int
	User   string
	Socket string
}

// Input holds everything the analyzer needs.
type Input struct {
	Parsed        *parser.ParsedSQL
	Meta          *mysql.TableMetadata
	Topo          *topology.Info
	Version       mysql.ServerVersion
	ChunkSize     int
	Connection    *ConnectionInfo // Optional: for generating executable commands
	EstimatedRows int64           // EXPLAIN-based row estimate for DML
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
	DMLOp        parser.DMLOperation
	AffectedRows int64
	AffectedPct  float64
	HasWhere     bool
	WriteSetSize int64 // estimated bytes for write-set

	// Recommendation
	Risk                        RiskLevel
	Method                      ExecutionMethod
	AlternativeMethod           ExecutionMethod // set when both gh-ost and pt-osc are viable
	Recommendation              string
	ExecutionCommand            string // Generated command for primary method
	AlternativeExecutionCommand string // Generated command for alternative method
	MethodRationale             string // Explains why primary is preferred (or why alternative is excluded)
	Warnings                    []string
	ClusterWarnings             []string
	DiskEstimate                *DiskSpaceEstimate

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

// DiskSpaceEstimate describes how much additional disk space an operation will need.
type DiskSpaceEstimate struct {
	RequiredBytes int64
	RequiredHuman string
	Reason        string
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

	// Compute disk space estimate after method is finalized (topology may override ExecGhost → ExecPtOSC)
	if result.StatementType == parser.DDL {
		result.DiskEstimate = estimateDiskSpace(input, result)
	}

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
			// Both gh-ost and pt-osc can avoid the lock by copying the table online.
			// gh-ost is preferred for non-Galera; applyGaleraWarnings() will override
			// to pt-osc (and clear the alternative) if the topology is Galera.
			if input.Meta.TotalSize() > 1*1024*1024*1024 { // > 1 GB
				if result.Risk != RiskDangerous {
					result.Risk = RiskDangerous
				}
				result.Method = ExecGhost
				result.AlternativeMethod = ExecPtOSC
				result.MethodRationale = ghostPreferredRationale
				if result.Risk == RiskDangerous && result.Recommendation == "" {
					result.Recommendation = "INPLACE with SHARED lock on a large table. Use an online schema change tool to avoid blocking writes."
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
				result.MethodRationale = ptOSCOnlyRationale
				if result.Recommendation == "" {
					result.Recommendation = "COPY algorithm on a large table in Galera/PXC. Use pt-online-schema-change with --max-flow-ctl."
				}
			} else {
				result.Method = ExecGhost
				result.AlternativeMethod = ExecPtOSC
				result.MethodRationale = ghostPreferredRationale
				if result.Recommendation == "" {
					result.Recommendation = "COPY algorithm on a large table. Use an online schema change tool to avoid blocking writes."
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

	// gh-ost cannot operate on tables with triggers: its shadow table approach causes triggers
	// on the original table to fire during population, leading to data corruption or errors.
	// Override to pt-osc (with --preserve-triggers) when triggers are present.
	if result.Method == ExecGhost && len(input.Meta.Triggers) > 0 {
		result.Method = ExecPtOSC
		result.AlternativeMethod = ""
		result.MethodRationale = ptOSCTriggerRationale
	}

	// Generate executable command for the primary method, and alternative when both are viable.
	if result.Method == ExecGhost {
		result.ExecutionCommand = generateGhostCommand(input)
		if result.AlternativeMethod == ExecPtOSC {
			result.AlternativeExecutionCommand = generatePtOSCCommand(input, false)
		}
	} else if result.Method == ExecPtOSC {
		result.ExecutionCommand = generatePtOSCCommand(input, input.Topo.Type == topology.Galera)
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
	// If EXPLAIN-based estimate was provided, use it
	if input.EstimatedRows > 0 {
		return input.EstimatedRows
	}

	// Fallback: if no WHERE clause, entire table is affected
	if !input.Parsed.HasWhere {
		return input.Meta.RowCount
	}

	// If no estimate provided and has WHERE, return 0
	// (caller should provide EXPLAIN estimate for accurate results)
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

	// gh-ost incompatibility: override to pt-osc and remove the now-invalid alternative.
	if result.Method == ExecGhost {
		result.ClusterWarnings = append(result.ClusterWarnings,
			"gh-ost is NOT compatible with Galera/PXC. It relies on binlog streaming which conflicts with Galera writeset replication. Use pt-online-schema-change instead.",
		)
		result.Method = ExecPtOSC
		result.AlternativeMethod = ""
		result.AlternativeExecutionCommand = ""
		result.MethodRationale = ptOSCOnlyRationale
		result.ExecutionCommand = generatePtOSCCommand(input, true)
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

// estimateDiskSpace returns the additional disk space needed for a DDL operation,
// or nil if no significant extra space is required (INSTANT algorithm or table < 100 MB).
// Must be called after applyTopologyWarnings so that the final Method is reflected.
func estimateDiskSpace(input Input, result *Result) *DiskSpaceEstimate {
	const threshold = 100 * 1024 * 1024 // 100 MB

	// INSTANT algorithm needs no extra disk space
	if result.Classification.Algorithm == AlgoInstant {
		return nil
	}

	// gh-ost and pt-osc both create a full shadow table during migration
	if result.Method == ExecGhost {
		total := input.Meta.TotalSize()
		if total < threshold {
			return nil
		}
		return &DiskSpaceEstimate{
			RequiredBytes: total,
			RequiredHuman: humanBytes(total),
			Reason:        "gh-ost creates a full shadow copy of the table during migration",
		}
	}
	if result.Method == ExecPtOSC {
		total := input.Meta.TotalSize()
		if total < threshold {
			return nil
		}
		return &DiskSpaceEstimate{
			RequiredBytes: total,
			RequiredHuman: humanBytes(total),
			Reason:        "pt-online-schema-change creates a full shadow copy of the table during migration",
		}
	}

	// COPY algorithm executed directly: MySQL builds a temp table copy
	if result.Classification.Algorithm == AlgoCopy {
		total := input.Meta.TotalSize()
		if total < threshold {
			return nil
		}
		return &DiskSpaceEstimate{
			RequiredBytes: total,
			RequiredHuman: humanBytes(total),
			Reason:        "COPY algorithm creates a temporary full table copy during the operation",
		}
	}

	// INPLACE with table rebuild: temporary copy is created and swapped in
	if result.Classification.RebuildsTable {
		total := input.Meta.TotalSize()
		if total < threshold {
			return nil
		}
		return &DiskSpaceEstimate{
			RequiredBytes: total,
			RequiredHuman: humanBytes(total),
			Reason:        "INPLACE with table rebuild creates a temporary copy of the table",
		}
	}

	// INPLACE without table rebuild (e.g. ADD INDEX): temp sort files for the new index
	indexLen := input.Meta.IndexLength
	if indexLen < threshold {
		return nil
	}
	return &DiskSpaceEstimate{
		RequiredBytes: indexLen,
		RequiredHuman: humanBytes(indexLen),
		Reason:        "INPLACE index build requires temporary space proportional to the new index size",
	}
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

// extractAlterSpec extracts the ALTER specification from a DDL statement.
// For "ALTER TABLE users ADD COLUMN email VARCHAR(255)", returns "ADD COLUMN email VARCHAR(255)".
func extractAlterSpec(sql string) string {
	// Remove leading/trailing whitespace
	sql = strings.TrimSpace(sql)

	// Find "ALTER TABLE" (case-insensitive)
	alterIdx := strings.Index(strings.ToUpper(sql), "ALTER TABLE")
	if alterIdx == -1 {
		return sql // Return original if not an ALTER TABLE
	}

	// Skip past "ALTER TABLE"
	remaining := sql[alterIdx+11:] // len("ALTER TABLE") = 11
	remaining = strings.TrimSpace(remaining)

	// Find the table name (could be quoted or qualified)
	// Simple approach: find the first space after skipping the table identifier
	// This handles: tablename, `tablename`, db.table, `db`.`table`
	inBacktick := false
	tableEnd := 0
	for i, ch := range remaining {
		if ch == '`' {
			inBacktick = !inBacktick
		} else if !inBacktick && (ch == ' ' || ch == '\t' || ch == '\n') {
			tableEnd = i
			break
		}
	}

	if tableEnd == 0 {
		return "" // Couldn't find table name
	}

	// Return everything after the table name
	alterSpec := strings.TrimSpace(remaining[tableEnd:])
	return alterSpec
}

// generateGhostCommand generates a gh-ost command for the given DDL.
func generateGhostCommand(input Input) string {
	if input.Connection == nil {
		return "" // Can't generate without connection info
	}

	alterSpec := extractAlterSpec(input.Parsed.RawSQL)
	if alterSpec == "" {
		return ""
	}

	var cmd strings.Builder
	cmd.WriteString("gh-ost \\\n")
	cmd.WriteString(fmt.Sprintf("  --user=\"%s\" \\\n", input.Connection.User))

	if input.Connection.Socket != "" {
		cmd.WriteString(fmt.Sprintf("  --socket=\"%s\" \\\n", input.Connection.Socket))
	} else {
		cmd.WriteString(fmt.Sprintf("  --host=\"%s\" \\\n", input.Connection.Host))
		cmd.WriteString(fmt.Sprintf("  --port=%d \\\n", input.Connection.Port))
	}

	cmd.WriteString(fmt.Sprintf("  --database=\"%s\" \\\n", input.Parsed.Database))
	cmd.WriteString(fmt.Sprintf("  --table=\"%s\" \\\n", input.Parsed.Table))
	cmd.WriteString(fmt.Sprintf("  --alter=\"%s\" \\\n", alterSpec))
	cmd.WriteString("  --assume-rbr \\\n")
	cmd.WriteString("  --cut-over=default \\\n")
	cmd.WriteString("  --exact-rowcount \\\n")
	cmd.WriteString("  --concurrent-rowcount \\\n")
	cmd.WriteString("  --default-retries=120 \\\n")
	cmd.WriteString("  --panic-flag-file=/tmp/ghost.panic.flag \\\n")
	cmd.WriteString("  --postpone-cut-over-flag-file=/tmp/ghost.postpone.flag \\\n")
	cmd.WriteString("  --execute")

	return cmd.String()
}

// generatePtOSCCommand generates a pt-online-schema-change command for the given DDL.
func generatePtOSCCommand(input Input, isGalera bool) string {
	if input.Connection == nil {
		return "" // Can't generate without connection info
	}

	alterSpec := extractAlterSpec(input.Parsed.RawSQL)
	if alterSpec == "" {
		return ""
	}

	var cmd strings.Builder
	cmd.WriteString("pt-online-schema-change \\\n")

	// Build DSN
	var dsn string
	if input.Connection.Socket != "" {
		dsn = fmt.Sprintf("S=%s", input.Connection.Socket)
	} else {
		dsn = fmt.Sprintf("h=%s,P=%d", input.Connection.Host, input.Connection.Port)
	}
	dsn += fmt.Sprintf(",u=%s", input.Connection.User)
	dsn += fmt.Sprintf(",D=%s,t=%s", input.Parsed.Database, input.Parsed.Table)

	cmd.WriteString(fmt.Sprintf("  %s \\\n", dsn))
	cmd.WriteString(fmt.Sprintf("  --alter \"%s\" \\\n", alterSpec))
	cmd.WriteString("  --execute \\\n")
	cmd.WriteString("  --chunk-size=1000 \\\n")
	cmd.WriteString("  --chunk-time=0.5 \\\n")
	cmd.WriteString("  --max-load=Threads_running=25 \\\n")
	cmd.WriteString("  --critical-load=Threads_running=50 \\\n")

	// Galera-specific flags
	if isGalera {
		cmd.WriteString("  --max-flow-ctl=0.5 \\\n")
		cmd.WriteString("  --check-plan \\\n")
	}

	cmd.WriteString("  --alter-foreign-keys-method=auto \\\n")
	cmd.WriteString("  --preserve-triggers")

	return cmd.String()
}
