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

// auroraGhostRationale explains why gh-ost cannot be used on Aurora MySQL.
const auroraGhostRationale = "gh-ost is NOT compatible with Aurora MySQL: Aurora uses storage-layer " +
	"replication instead of MySQL binary log replication. gh-ost relies on reading the binary log stream " +
	"which is not accessible on Aurora. Use pt-online-schema-change instead."

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
	Host     string
	Port     int
	User     string
	Socket   string
	Database string
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

	// ForeignKeyChecksDisabled reflects the server's foreign_key_checks variable at analysis
	// time. Zero value (false) means checks are ON — the safe default that requires COPY for
	// ADD FOREIGN KEY. Set to true only when the server reports foreign_key_checks=OFF.
	ForeignKeyChecksDisabled bool
}

// SubOpResult holds the per-sub-operation classification for a multi-op ALTER TABLE.
type SubOpResult struct {
	Op             parser.DDLOperation
	Classification DDLClassification
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
	SubOpResults   []SubOpResult // per-sub-op classification breakdown (multi-op only)

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

	// Idempotent stored procedure (when --idempotent is set)
	IdempotentSP string

	// OptimizedDDL is the original ALTER TABLE with explicit ALGORITHM and LOCK hints appended,
	// ready to copy-paste. Only set for ALTER TABLE with INSTANT or INPLACE algorithm.
	OptimizedDDL string
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
	// Use EffectivePatch() so Aurora 8.0 is treated as MySQL 8.0.23 for algorithm selection.
	v := input.Version
	result.Classification = ClassifyDDLWithContext(input.Parsed, v.Major, v.Minor, v.EffectivePatch())

	// For CONVERT TO CHARACTER SET: refine the COPY baseline from the matrix using live
	// table metadata. Per WL#11605, COPY is required when any indexed string column exists;
	// INPLACE is sufficient otherwise — but SHARED lock always applies regardless.
	if input.Parsed.DDLOp == parser.ConvertCharset {
		applyConvertCharsetClassification(input, result)
	}

	// For CHANGE COLUMN: check if the data type is actually changing.
	// The matrix baseline is INSTANT (≥8.0.29) or INPLACE (older) for rename-only.
	// If the type changes, COPY is required regardless of version.
	if input.Parsed.DDLOp == parser.ChangeColumn && input.Parsed.NewColumnType != "" {
		if oldType := findColumnType(input.Meta.Columns, input.Parsed.OldColumnName); oldType != "" {
			if !strings.EqualFold(strings.ReplaceAll(oldType, " ", ""), strings.ReplaceAll(input.Parsed.NewColumnType, " ", "")) {
				result.Classification = DDLClassification{
					Algorithm:     AlgoCopy,
					Lock:          LockShared,
					RebuildsTable: true,
					Notes:         "Data type change requires COPY algorithm with SHARED lock. Reads allowed, writes blocked during rebuild.",
				}
				result.Warnings = append(result.Warnings, fmt.Sprintf(
					"Column '%s' type change detected: %s → %s. COPY algorithm required.",
					input.Parsed.OldColumnName, oldType, input.Parsed.NewColumnType,
				))
			}
		}
	}

	// For MODIFY COLUMN: apply sub-type classification overrides in priority order.
	// These overrides refine the COPY baseline from the matrix using live schema metadata.
	if input.Parsed.DDLOp == parser.ModifyColumn && input.Parsed.NewColumnType != "" {
		oldType := findColumnType(input.Meta.Columns, input.Parsed.ColumnName)
		if oldType != "" {
			charset := findColumnCharset(input.Meta.Columns, input.Parsed.ColumnName)

			// Check for an explicit charset change: always requires COPY.
			// (The COPY baseline from the matrix already applies; we add a specific warning.)
			if input.Parsed.NewColumnCharset != "" && charset != "" &&
				!strings.EqualFold(charset, input.Parsed.NewColumnCharset) {
				result.Warnings = append(result.Warnings, fmt.Sprintf(
					"Column '%s' charset change detected: %s → %s. COPY with SHARED lock required.",
					input.Parsed.ColumnName, charset, input.Parsed.NewColumnCharset,
				))
			} else {
				// No charset change (or charset is the same / not specified): try INPLACE optimizations.
				// NewColumnType is already the base data type (no NULL/DEFAULT options).

				// Priority 1: ENUM/SET append-at-end → INSTANT (metadata-only).
				if cls, ok := classifyModifyColumnEnum(oldType, input.Parsed.NewColumnType); ok {
					result.Classification = cls
				} else if cls, ok := classifyModifyColumnVarchar(oldType, input.Parsed.NewColumnType, charset); ok {
					// Priority 2: VARCHAR extension within same length-prefix tier → INPLACE, no rebuild.
					result.Classification = cls
				}
			}
		}
	}

	// For ALTER TABLESPACE RENAME: warn if the server version is too old (introduced in 8.0.21).
	if input.Parsed.DDLOp == parser.AlterTablespace {
		vr := classifyVersion(v.Major, v.Minor, v.EffectivePatch())
		if vr == V8_0_Early || (vr == V8_0_Instant && v.EffectivePatch() < 21) {
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("ALTER TABLESPACE ... RENAME TO requires MySQL 8.0.21+. Your version (%s) will reject this statement with a syntax error.", v.String()),
			)
			result.Risk = RiskDangerous
		}
	}

	// For TABLE ENCRYPTION: warn that keyring plugin must be configured.
	// dbsafe cannot verify plugin presence from a read-only connection, so this is informational.
	if input.Parsed.DDLOp == parser.TableEncryption {
		result.Warnings = append(result.Warnings,
			"Requires keyring plugin (keyring_file, keyring_vault, or component_keyring_*). Operation will fail if keyring is not configured.",
		)
	}

	// For ENGINE= same-engine (e.g. ENGINE=InnoDB on an InnoDB table): MySQL treats this as a
	// null ALTER TABLE operation — identical to ALTER TABLE ... FORCE. The table is rebuilt
	// INPLACE to reclaim fragmentation and reset TOTAL_ROW_VERSIONS. The matrix baseline for
	// ChangeEngine is COPY (cross-engine conversion); override to INPLACE+rebuild when the
	// target engine matches the current engine.
	if input.Parsed.DDLOp == parser.ChangeEngine &&
		input.Parsed.NewEngine != "" &&
		input.Meta != nil &&
		strings.EqualFold(input.Parsed.NewEngine, input.Meta.Engine) {
		result.Classification = DDLClassification{
			Algorithm:     AlgoInplace,
			Lock:          LockNone,
			RebuildsTable: true,
			Notes:         "ENGINE=<same engine>: equivalent to ALTER TABLE ... FORCE. INPLACE rebuild to reclaim fragmentation. Concurrent DML allowed.",
		}
	}

	// For ADD FOREIGN KEY: the matrix baseline (INPLACE+NONE) applies only when
	// foreign_key_checks=OFF. With the default foreign_key_checks=ON, MySQL must validate all
	// existing rows against the new constraint, which requires COPY+SHARED (no concurrent writes).
	// Zero value of ForeignKeyChecksDisabled is false, so the safe COPY path is the default.
	if input.Parsed.DDLOp == parser.AddForeignKey && !input.ForeignKeyChecksDisabled {
		result.Classification = DDLClassification{
			Algorithm:     AlgoCopy,
			Lock:          LockShared,
			RebuildsTable: false,
			Notes:         "ADD FOREIGN KEY with foreign_key_checks=ON requires COPY algorithm. MySQL must validate all existing rows against the constraint. Set foreign_key_checks=OFF before the ALTER to use INPLACE.",
		}
		result.Warnings = append(result.Warnings,
			"foreign_key_checks=ON: COPY algorithm required for ADD FOREIGN KEY. All existing rows will be validated against the new constraint, blocking concurrent writes.",
		)
	}

	// For ADD PRIMARY KEY: the matrix baseline is INPLACE+rebuild, but if any PK column is
	// nullable in the live schema, MySQL must convert it to NOT NULL first, requiring COPY.
	if input.Parsed.DDLOp == parser.AddPrimaryKey && len(input.Parsed.IndexColumns) > 0 {
		for _, colName := range input.Parsed.IndexColumns {
			for _, col := range input.Meta.Columns {
				if strings.EqualFold(col.Name, colName) && col.Nullable {
					result.Classification = DDLClassification{
						Algorithm:     AlgoCopy,
						Lock:          LockShared,
						RebuildsTable: true,
						Notes:         "Nullable PK column requires COPY: MySQL must implicitly convert the column from NULL to NOT NULL during the rebuild.",
					}
					result.Warnings = append(result.Warnings, fmt.Sprintf(
						"Column '%s' is nullable: ADD PRIMARY KEY on a nullable column requires COPY algorithm (MySQL must enforce NOT NULL).",
						colName,
					))
					break
				}
			}
			if result.Classification.Algorithm == AlgoCopy {
				break
			}
		}
	}

	// For ADD UNIQUE KEY or ADD PRIMARY KEY: suggest a pre-flight duplicate-check query.
	// If duplicates exist, the ALTER will fail with "Duplicate entry". Running the SELECT
	// lets the user discover and resolve duplicates before attempting the ALTER.
	if (input.Parsed.DDLOp == parser.AddPrimaryKey || (input.Parsed.DDLOp == parser.AddIndex && input.Parsed.IsUniqueIndex)) &&
		len(input.Parsed.IndexColumns) > 0 {
		cols := strings.Join(input.Parsed.IndexColumns, ", ")
		result.Warnings = append(result.Warnings, fmt.Sprintf(
			"This ALTER will fail if duplicates exist. Verify with:\n  SELECT %s, COUNT(*) cnt FROM %s GROUP BY %s HAVING cnt > 1 LIMIT 5;",
			cols, input.Parsed.Table, cols,
		))
	}

	// For ADD CONSTRAINT ... CHECK: suggest a pre-flight validation query.
	// If any existing row violates the check expression, the ALTER will fail.
	if input.Parsed.DDLOp == parser.AddCheckConstraint && input.Parsed.CheckExpr != "" {
		result.Warnings = append(result.Warnings, fmt.Sprintf(
			"This ALTER will fail if any row violates the check constraint. Verify with:\n  SELECT * FROM %s WHERE NOT (%s) LIMIT 5;",
			input.Parsed.Table, input.Parsed.CheckExpr,
		))
	}

	// For ADD COLUMN with AUTO_INCREMENT: requires INPLACE with SHARED lock minimum and
	// full table rebuild. Concurrent DML is not permitted (MySQL 8.0 Table 17.18).
	if input.Parsed.DDLOp == parser.AddColumn && input.Parsed.HasAutoIncrement {
		result.Classification = DDLClassification{
			Algorithm:     AlgoInplace,
			Lock:          LockShared,
			RebuildsTable: true,
			Notes:         "ADD COLUMN with AUTO_INCREMENT: INPLACE with SHARED lock minimum. Concurrent DML not permitted. Full table rebuild required.",
		}
		result.Warnings = append(result.Warnings,
			"AUTO_INCREMENT column: INPLACE with LOCK=SHARED required. Concurrent DML (writes) are blocked during the rebuild.",
		)
	}

	// For ADD STORED generated column: always requires COPY with SHARED lock.
	// MySQL must rewrite all rows to compute and store the generated values.
	// ADD VIRTUAL generated column is already INSTANT from the matrix.
	if input.Parsed.DDLOp == parser.AddColumn && input.Parsed.IsGeneratedStored {
		result.Classification = DDLClassification{
			Algorithm:     AlgoCopy,
			Lock:          LockShared,
			RebuildsTable: true,
			Notes:         "ADD STORED generated column requires COPY algorithm. MySQL must rewrite all rows to compute and store the generated values. Concurrent writes blocked.",
		}
		result.Warnings = append(result.Warnings,
			"STORED generated column: COPY with LOCK=SHARED required. All rows must be rewritten to compute stored values. Concurrent writes are blocked.",
		)
	}

	// For DROP STORED generated column: always INPLACE with table rebuild.
	// MySQL must rewrite all rows to remove the stored values, but allows concurrent DML.
	// DROP VIRTUAL generated column uses the matrix baseline (INSTANT on 8.0.29+).
	if input.Parsed.DDLOp == parser.DropColumn {
		for _, col := range input.Meta.Columns {
			if strings.EqualFold(col.Name, input.Parsed.ColumnName) && col.IsStoredGenerated {
				result.Classification = DDLClassification{
					Algorithm:     AlgoInplace,
					Lock:          LockNone,
					RebuildsTable: true,
					Notes:         "DROP STORED generated column: INPLACE with table rebuild. MySQL rewrites all rows to remove the stored values. Concurrent DML allowed.",
				}
				break
			}
		}

		// On 8.0.29+ the matrix baseline is INSTANT for DROP COLUMN, but MySQL cannot
		// use INSTANT when the column participates in any index — it falls back to INPLACE
		// with a full table rebuild. Check all indexes for the dropped column name.
		if result.Classification.Algorithm == AlgoInstant {
			found := false
			for _, idx := range input.Meta.Indexes {
				if found {
					break
				}
				for _, idxCol := range idx.Columns {
					if strings.EqualFold(idxCol, input.Parsed.ColumnName) {
						result.Classification = DDLClassification{
							Algorithm:     AlgoInplace,
							Lock:          LockNone,
							RebuildsTable: true,
							Notes: fmt.Sprintf(
								"DROP COLUMN on indexed column: INPLACE with table rebuild. Column '%s' is part of index '%s'; MySQL cannot use INSTANT for indexed columns. Concurrent DML allowed.",
								input.Parsed.ColumnName, idx.Name),
						}
						result.Warnings = append(result.Warnings, fmt.Sprintf(
							"Column '%s' is part of index '%s'. Consider dropping the index first if you want a faster operation.",
							input.Parsed.ColumnName, idx.Name))
						found = true
						break
					}
				}
			}
		}
	}

	// For MULTIPLE_OPS: classify each sub-operation individually with live-metadata
	// refinements and return the most restrictive combined result.
	if input.Parsed.DDLOp == parser.MultipleOps && len(input.Parsed.SubOperations) > 0 {
		var subOpWarnings []string
		result.Classification, result.SubOpResults, subOpWarnings = aggregateMultipleOps(
			input.Parsed.SubOperations, input.Meta, input.ForeignKeyChecksDisabled, v,
		)
		result.Warnings = append(result.Warnings, subOpWarnings...)
	}

	// For MODIFY COLUMN with FIRST/AFTER: column reorder behavior depends on column type.
	// Generated columns have different rules than regular columns.
	if input.Parsed.DDLOp == parser.ModifyColumn && input.Parsed.IsFirstAfter {
		oldType := findColumnType(input.Meta.Columns, input.Parsed.ColumnName)
		if oldType != "" {
			newBase := strings.ToLower(strings.TrimSpace(input.Parsed.NewColumnType))
			if strings.EqualFold(strings.ReplaceAll(oldType, " ", ""), strings.ReplaceAll(newBase, " ", "")) {
				switch {
				case input.Parsed.IsGeneratedStored:
					// STORED generated column reorder requires COPY: all rows must be rewritten
					// to move the physical column data.
					result.Classification = DDLClassification{
						Algorithm:     AlgoCopy,
						Lock:          LockShared,
						RebuildsTable: true,
						Notes:         "STORED generated column reorder (FIRST/AFTER): COPY required. MySQL must rewrite all rows to relocate the stored values.",
					}
				case input.Parsed.IsGeneratedColumn:
					// VIRTUAL generated column reorder: INPLACE, no rebuild. There is no
					// stored data to move — only metadata changes.
					result.Classification = DDLClassification{
						Algorithm:     AlgoInplace,
						Lock:          LockNone,
						RebuildsTable: false,
						Notes:         "VIRTUAL generated column reorder (FIRST/AFTER): INPLACE, no rebuild. No stored values to move.",
					}
				default:
					// Regular column reorder — INPLACE with table rebuild, concurrent DML allowed.
					result.Classification = DDLClassification{
						Algorithm:     AlgoInplace,
						Lock:          LockNone,
						RebuildsTable: true,
						Notes:         "Column reorder (FIRST/AFTER) with same type: INPLACE with table rebuild, concurrent DML allowed.",
					}
				}
			}
			// If types differ, the existing classification (COPY) already covers the type-change case.
		}
	}

	// For MODIFY COLUMN: nullability change (NULL ↔ NOT NULL) with same base type → INPLACE rebuild.
	// Checked after reorder so both can co-apply; the result is the same (INPLACE + rebuild).
	if input.Parsed.DDLOp == parser.ModifyColumn && input.Parsed.NewColumnNullable != nil && input.Parsed.NewColumnType != "" {
		oldType := findColumnType(input.Meta.Columns, input.Parsed.ColumnName)
		if oldType != "" {
			newBase := strings.ToLower(strings.TrimSpace(input.Parsed.NewColumnType))
			sameBaseType := strings.EqualFold(strings.ReplaceAll(oldType, " ", ""), strings.ReplaceAll(newBase, " ", ""))
			if sameBaseType {
				for _, col := range input.Meta.Columns {
					if strings.EqualFold(col.Name, input.Parsed.ColumnName) {
						newNullable := *input.Parsed.NewColumnNullable
						if newNullable != col.Nullable {
							result.Classification = DDLClassification{
								Algorithm:     AlgoInplace,
								Lock:          LockNone,
								RebuildsTable: true,
								Notes:         "Nullability change (NULL ↔ NOT NULL) with same base type: INPLACE with table rebuild, concurrent DML allowed.",
							}
						}
						break
					}
				}
			}
		}
	}

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
	switch result.Method {
	case ExecGhost:
		result.ExecutionCommand = generateGhostCommand(input)
		if result.AlternativeMethod == ExecPtOSC {
			result.AlternativeExecutionCommand = generatePtOSCCommand(input, false)
		}
	case ExecPtOSC:
		result.ExecutionCommand = generatePtOSCCommand(input, input.Topo.Type == topology.Galera)
	}

	// Build an optimized copy-paste DDL for ALTER TABLE with INSTANT/INPLACE algorithm.
	if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(input.Parsed.RawSQL)), "ALTER TABLE") {
		result.OptimizedDDL = buildOptimizedDDL(input.Parsed.RawSQL, result.Classification)
	}

	// Generate rollback SQL
	generateDDLRollback(input, result)
}

// buildOptimizedDDL appends ALGORITHM and LOCK hints to an ALTER TABLE statement so the user
// can copy-paste it directly. Returns empty string for COPY or DEPENDS (no improvement possible).
func buildOptimizedDDL(rawSQL string, c DDLClassification) string {
	if c.Algorithm != AlgoInstant && c.Algorithm != AlgoInplace {
		return ""
	}
	sql := strings.TrimRight(strings.TrimSpace(rawSQL), ";")
	return fmt.Sprintf("%s, ALGORITHM=%s, LOCK=%s;", sql, c.Algorithm, c.Lock)
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
	switch {
	case result.AffectedRows > chunkThreshold:
		result.Risk = RiskDangerous
		result.Method = ExecChunked
		result.ChunkCount = (result.AffectedRows + int64(input.ChunkSize) - 1) / int64(input.ChunkSize)
		result.Recommendation = fmt.Sprintf(
			"Affecting ~%s rows (%.1f%%). Chunk into batches of %d rows with sleep between chunks to avoid lock contention and replication lag.",
			formatNumber(result.AffectedRows), result.AffectedPct, input.ChunkSize,
		)
	case result.AffectedRows > 10000:
		result.Risk = RiskCaution
		result.Method = ExecDirect
		result.Recommendation = fmt.Sprintf(
			"Affecting ~%s rows (%.1f%%). Moderate impact. Direct execution OK during low-traffic window, but consider chunking if you want to be safe.",
			formatNumber(result.AffectedRows), result.AffectedPct,
		)
	default:
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

// applyConvertCharsetClassification refines the DDL matrix baseline for CONVERT TO CHARACTER SET.
// Per WL#11605: if any indexed string column exists the algorithm must be COPY; otherwise INPLACE
// is permitted. In both cases MySQL always acquires a SHARED lock — concurrent DML is never allowed.
func applyConvertCharsetClassification(input Input, result *Result) {
	// Build set of indexed column names (case-insensitive).
	indexedCols := make(map[string]bool)
	for _, idx := range input.Meta.Indexes {
		for _, col := range idx.Columns {
			indexedCols[strings.ToLower(col)] = true
		}
	}

	// Find which indexed columns are string types.
	var indexedStringCols []string
	for _, col := range input.Meta.Columns {
		if indexedCols[strings.ToLower(col.Name)] && isStringType(col.Type) {
			indexedStringCols = append(indexedStringCols, col.Name)
		}
	}

	if len(indexedStringCols) > 0 {
		result.Classification = DDLClassification{
			Algorithm:     AlgoCopy,
			Lock:          LockShared,
			RebuildsTable: true,
			Notes: fmt.Sprintf(
				"COPY algorithm required: indexed string column(s) (%s) cannot have their collation changed INPLACE (WL#11605). Reads allowed, writes blocked during full table rebuild.",
				strings.Join(indexedStringCols, ", "),
			),
		}
	} else {
		result.Classification = DDLClassification{
			Algorithm:     AlgoInplace,
			Lock:          LockShared,
			RebuildsTable: true,
			Notes:         "INPLACE possible (no indexed string columns), but CONVERT TO CHARACTER SET always acquires SHARED lock — writes are blocked for the entire rebuild regardless of algorithm.",
		}
		result.Warnings = append(result.Warnings,
			"No indexed string columns: INPLACE algorithm is used, but CONVERT TO CHARACTER SET always holds a SHARED lock. Writes are blocked during the entire table rebuild.",
		)
	}
}

// isStringType reports whether a MySQL column type is a character string type
// (varchar, char, text family, enum, set) that participates in character set encoding.
func isStringType(colType string) bool {
	lower := strings.ToLower(colType)
	for _, prefix := range []string{"varchar", "char", "tinytext", "mediumtext", "longtext", "text", "enum", "set"} {
		if strings.HasPrefix(lower, prefix) {
			return true
		}
	}
	return false
}

// classifySubOp returns the DDL classification and any warnings for a single sub-operation
// within a multi-op ALTER TABLE, applying the same live-metadata refinements as analyzeDDL.
func classifySubOp(subOp parser.SubOperation, meta *mysql.TableMetadata, fkChecksDisabled bool, v mysql.ServerVersion) (DDLClassification, []string) {
	var warnings []string

	// Matrix baseline — with the AUTO_INCREMENT override handled up front.
	var cls DDLClassification
	if subOp.Op == parser.AddColumn && subOp.HasAutoIncrement {
		cls = DDLClassification{Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: true,
			Notes: "ADD COLUMN with AUTO_INCREMENT: INPLACE with SHARED lock minimum. Full table rebuild required."}
	} else {
		cls = ClassifyDDL(subOp.Op, v.Major, v.Minor, v.EffectivePatch())
	}

	switch subOp.Op {
	case parser.AddColumn:
		// STORED generated column requires COPY.
		if subOp.IsGeneratedStored {
			cls = DDLClassification{Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true,
				Notes: "ADD STORED generated column requires COPY algorithm."}
			warnings = append(warnings, "STORED generated column in compound ALTER: COPY with LOCK=SHARED required.")
		}

	case parser.DropColumn:
		if meta != nil {
			// STORED generated column drop: INPLACE + rebuild.
			for _, col := range meta.Columns {
				if strings.EqualFold(col.Name, subOp.ColumnName) && col.IsStoredGenerated {
					cls = DDLClassification{Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true,
						Notes: "DROP STORED generated column: INPLACE with table rebuild."}
					break
				}
			}
			// Indexed column: downgrade INSTANT to INPLACE + rebuild.
			if cls.Algorithm == AlgoInstant {
				for _, idx := range meta.Indexes {
					for _, idxCol := range idx.Columns {
						if strings.EqualFold(idxCol, subOp.ColumnName) {
							cls = DDLClassification{Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true,
								Notes: fmt.Sprintf("DROP COLUMN on indexed column '%s': INPLACE with rebuild.", subOp.ColumnName)}
							warnings = append(warnings, fmt.Sprintf(
								"Column '%s' is part of index '%s'. Consider dropping the index first.",
								subOp.ColumnName, idx.Name))
							break
						}
					}
					if cls.Algorithm == AlgoInplace {
						break
					}
				}
			}
		}

	case parser.ChangeColumn:
		if subOp.OldColumnName != "" && subOp.NewColumnType != "" && meta != nil {
			oldType := findColumnType(meta.Columns, subOp.OldColumnName)
			if oldType != "" && !strings.EqualFold(
				strings.ReplaceAll(oldType, " ", ""),
				strings.ReplaceAll(subOp.NewColumnType, " ", ""),
			) {
				cls = DDLClassification{Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true,
					Notes: "Data type change requires COPY algorithm."}
				warnings = append(warnings, fmt.Sprintf(
					"Column '%s' type change detected: %s → %s. COPY algorithm required.",
					subOp.OldColumnName, oldType, subOp.NewColumnType,
				))
			}
		}

	case parser.ModifyColumn:
		if subOp.NewColumnType != "" && meta != nil {
			oldType := findColumnType(meta.Columns, subOp.ColumnName)
			if oldType != "" {
				if subOp.NewColumnCharset == "" {
					// No charset change: try INSTANT/INPLACE optimizations.
					if c, ok := classifyModifyColumnEnum(oldType, subOp.NewColumnType); ok {
						cls = c
					} else {
						charset := findColumnCharset(meta.Columns, subOp.ColumnName)
						if c, ok := classifyModifyColumnVarchar(oldType, subOp.NewColumnType, charset); ok {
							cls = c
						}
					}
				}
			}
		}

	case parser.AddPrimaryKey:
		if len(subOp.IndexColumns) > 0 && meta != nil {
			for _, colName := range subOp.IndexColumns {
				for _, col := range meta.Columns {
					if strings.EqualFold(col.Name, colName) && col.Nullable {
						cls = DDLClassification{Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true,
							Notes: "Nullable PK column requires COPY."}
						warnings = append(warnings, fmt.Sprintf(
							"Column '%s' is nullable: ADD PRIMARY KEY on a nullable column requires COPY algorithm.",
							colName,
						))
						break
					}
				}
				if cls.Algorithm == AlgoCopy {
					break
				}
			}
		}

	case parser.AddForeignKey:
		if !fkChecksDisabled {
			cls = DDLClassification{Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: false,
				Notes: "ADD FOREIGN KEY with foreign_key_checks=ON requires COPY."}
			warnings = append(warnings, "foreign_key_checks=ON: COPY algorithm required for ADD FOREIGN KEY.")
		}

	case parser.ChangeEngine:
		if subOp.NewEngine != "" && meta != nil && strings.EqualFold(subOp.NewEngine, meta.Engine) {
			cls = DDLClassification{Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true,
				Notes: "ENGINE=<same engine>: equivalent to ALTER TABLE ... FORCE. INPLACE rebuild."}
		}
	}

	return cls, warnings
}

// aggregateMultipleOps classifies a MULTIPLE_OPS ALTER TABLE by applying live-metadata
// refinements to each sub-operation and returning the most restrictive combined result,
// per-sub-op results, and any per-sub-op warnings.
//
// Algorithm precedence (most to least restrictive): COPY > INPLACE > INSTANT
// Lock precedence (most to least restrictive): EXCLUSIVE > SHARED > NONE
func aggregateMultipleOps(subOps []parser.SubOperation, meta *mysql.TableMetadata, fkChecksDisabled bool, v mysql.ServerVersion) (DDLClassification, []SubOpResult, []string) {
	algoPriority := map[Algorithm]int{AlgoInstant: 0, AlgoInplace: 1, AlgoCopy: 2}
	lockPriority := map[LockLevel]int{LockNone: 0, LockShared: 1, LockExclusive: 2}

	combined := DDLClassification{
		Algorithm: AlgoInstant,
		Lock:      LockNone,
	}

	var subOpResults []SubOpResult
	var allWarnings []string

	for _, subOp := range subOps {
		cls, warnings := classifySubOp(subOp, meta, fkChecksDisabled, v)
		subOpResults = append(subOpResults, SubOpResult{Op: subOp.Op, Classification: cls})
		allWarnings = append(allWarnings, warnings...)

		if algoPriority[cls.Algorithm] > algoPriority[combined.Algorithm] {
			combined.Algorithm = cls.Algorithm
		}
		if lockPriority[cls.Lock] > lockPriority[combined.Lock] {
			combined.Lock = cls.Lock
		}
		if cls.RebuildsTable {
			combined.RebuildsTable = true
		}
	}

	combined.Notes = "Combined algorithm and lock derived from the most restrictive sub-operation."
	return combined, subOpResults, allWarnings
}

// findColumnType returns the type string for a column by name, or empty if not found.
func findColumnType(columns []mysql.ColumnInfo, name string) string {
	for _, col := range columns {
		if strings.EqualFold(col.Name, name) {
			return strings.ToLower(col.Type)
		}
	}
	return ""
}

// findColumnCharset returns the effective character set for a column, or empty if not found/not a string type.
func findColumnCharset(columns []mysql.ColumnInfo, name string) string {
	for _, col := range columns {
		if strings.EqualFold(col.Name, name) {
			if col.CharacterSet != nil {
				return *col.CharacterSet
			}
			return ""
		}
	}
	return ""
}

// maxBytesPerChar returns the maximum bytes per character for a MySQL charset.
// Returns 4 (utf8mb4) as a safe default for unknown charsets.
func maxBytesPerChar(charset string) int {
	switch strings.ToLower(charset) {
	case "ascii", "latin1", "latin2", "latin5", "latin7", "armscii8",
		"cp850", "cp852", "cp866", "cp1250", "cp1251", "cp1256", "cp1257",
		"dec8", "greek", "hebrew", "hp8", "keybcs2", "koi8r", "koi8u",
		"macce", "macroman", "swe7", "tis620", "binary":
		return 1
	case "gbk", "gb2312", "big5", "sjis", "cp932", "euckr", "ucs2", "utf16le":
		return 2
	case "ujis", "eucjpms", "utf8", "utf8mb3":
		return 3
	case "utf8mb4", "utf32", "utf16", "gb18030":
		return 4
	default:
		return 4 // conservative default
	}
}

// varcharLengthPrefixBytes returns the number of bytes used for the VARCHAR length prefix
// (1 if maxBytes ≤ 255, else 2).
func varcharLengthPrefixBytes(n int, charset string) int {
	if n*maxBytesPerChar(charset) <= 255 {
		return 1
	}
	return 2
}

// extractVarcharLength parses the length N from a type string like "varchar(50)".
// Returns (n, true) on success, (0, false) otherwise.
func extractVarcharLength(typeStr string) (int, bool) {
	s := strings.TrimSpace(strings.ToLower(typeStr))
	if !strings.HasPrefix(s, "varchar(") || !strings.HasSuffix(s, ")") {
		return 0, false
	}
	inner := s[len("varchar(") : len(s)-1]
	var n int
	if _, err := fmt.Sscanf(inner, "%d", &n); err != nil {
		return 0, false
	}
	return n, true
}

// classifyModifyColumnEnum checks if a MODIFY COLUMN ENUM/SET change is a pure append-at-end.
// MySQL stores ENUM/SET values as integer indexes; appending new members at the end doesn't
// change the mapping for existing rows, so it qualifies for INSTANT (metadata-only).
// Inserting, reordering, or removing members changes the integer mapping → COPY required.
// Returns (classification, true) if INSTANT is safe, (zero, false) otherwise.
func classifyModifyColumnEnum(oldType, newType string) (DDLClassification, bool) {
	oldMembers, oldOK := parseEnumMembers(oldType)
	newMembers, newOK := parseEnumMembers(newType)
	if !oldOK || !newOK {
		return DDLClassification{}, false
	}

	// New list must have more members than the old list.
	if len(newMembers) <= len(oldMembers) {
		return DDLClassification{}, false
	}

	// The old members must appear in the same order at the start of the new list.
	for i, m := range oldMembers {
		if !strings.EqualFold(m, newMembers[i]) {
			return DDLClassification{}, false
		}
	}

	// Check whether the append crosses a storage-size boundary.
	// ENUM: 1 byte for ≤255 members, 2 bytes for >255. Crossing requires a COPY.
	// SET: 1 byte per 8 members (bitmask). (oldCount+7)/8 bytes → if byte count changes, COPY.
	s := strings.TrimSpace(strings.ToLower(oldType))
	oldCount, newCount := len(oldMembers), len(newMembers)
	if strings.HasPrefix(s, "enum(") {
		if oldCount <= 255 && newCount > 255 {
			return DDLClassification{}, false
		}
	} else {
		// SET: storage in bytes = ceil(count/8)
		if (oldCount+7)/8 != (newCount+7)/8 {
			return DDLClassification{}, false
		}
	}

	return DDLClassification{
		Algorithm:     AlgoInstant,
		Lock:          LockNone,
		RebuildsTable: false,
		Notes: fmt.Sprintf(
			"ENUM/SET values appended at the end (%d → %d members): INSTANT, metadata-only. "+
				"Existing rows retain their stored integer representation.",
			len(oldMembers), len(newMembers),
		),
	}, true
}

// parseEnumMembers extracts the member list from a MySQL ENUM or SET type string.
// Handles types like: enum('a','b','c') or set('x','y').
// Returns the member list and true on success, nil and false otherwise.
func parseEnumMembers(typeStr string) ([]string, bool) {
	s := strings.TrimSpace(strings.ToLower(typeStr))
	if (!strings.HasPrefix(s, "enum(") && !strings.HasPrefix(s, "set(")) || !strings.HasSuffix(s, ")") {
		return nil, false
	}

	// Find the opening paren.
	parenIdx := strings.IndexByte(s, '(')
	if parenIdx < 0 {
		return nil, false
	}
	inner := s[parenIdx+1 : len(s)-1]

	var members []string
	var current strings.Builder
	inQuote := false
	for _, ch := range inner {
		switch {
		case ch == '\'' && !inQuote:
			inQuote = true
		case ch == '\'' && inQuote:
			members = append(members, current.String())
			current.Reset()
			inQuote = false
		case inQuote:
			current.WriteRune(ch)
		}
	}

	if len(members) == 0 {
		return nil, false
	}
	return members, true
}

// classifyModifyColumnVarchar checks if a MODIFY COLUMN VARCHAR change qualifies for INPLACE.
// Returns the classification and true if INPLACE is possible, (zero, false) otherwise.
// newType must be the base data type only (no NULL/DEFAULT options) — see baseColumnTypeString.
func classifyModifyColumnVarchar(oldType, newType, charset string) (DDLClassification, bool) {
	oldN, oldOK := extractVarcharLength(oldType)
	newN, newOK := extractVarcharLength(newType)
	if !oldOK || !newOK {
		return DDLClassification{}, false
	}

	// Only expansions are INPLACE; shrinking always requires COPY.
	if newN < oldN {
		return DDLClassification{}, false
	}

	// Use utf8mb4 (most restrictive) if charset is unknown.
	if charset == "" {
		charset = "utf8mb4"
	}

	oldPrefix := varcharLengthPrefixBytes(oldN, charset)
	newPrefix := varcharLengthPrefixBytes(newN, charset)

	if oldPrefix != newPrefix {
		// Length prefix tier changed — MySQL must rewrite all rows (COPY).
		return DDLClassification{}, false
	}

	return DDLClassification{
		Algorithm:     AlgoInplace,
		Lock:          LockNone,
		RebuildsTable: false,
		Notes: fmt.Sprintf(
			"VARCHAR extension from %d to %d chars stays within the %d-byte length prefix tier (%s charset, max %d bytes/char). INPLACE, no row rewrite, no lock.",
			oldN, newN, oldPrefix, charset, maxBytesPerChar(charset),
		),
	}, true
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
	case topology.AuroraWriter, topology.AuroraReader:
		applyAuroraWarnings(input, result)
	}

	// RDS-specific advisory: gh-ost needs extra flags on RDS managed MySQL.
	if input.Topo.IsCloudManaged && input.Topo.CloudProvider == "aws-rds" && result.Method == ExecGhost {
		result.ClusterWarnings = append(result.ClusterWarnings,
			"AWS RDS: gh-ost requires --allow-on-master and --assume-rbr flags. Ensure binary logging is enabled and the IAM/DB user has REPLICATION SLAVE privilege.",
		)
	}
}

func applyAuroraWarnings(input Input, result *Result) {
	// Warn if connected to an Aurora read replica — DDL/DML must run on writer.
	if input.Topo.Type == topology.AuroraReader {
		result.ClusterWarnings = append(result.ClusterWarnings,
			"Connected to an Aurora READ REPLICA. DDL and DML must be executed on the writer instance.",
		)
	}

	// gh-ost is incompatible with Aurora: Aurora uses storage-layer replication, not binlog streaming.
	// Override to pt-osc and clear the now-invalid alternative.
	if result.Method == ExecGhost {
		result.ClusterWarnings = append(result.ClusterWarnings,
			"gh-ost is NOT compatible with Aurora MySQL. Aurora uses storage-layer replication instead of MySQL binary log replication. Use pt-online-schema-change instead.",
		)
		result.Method = ExecPtOSC
		result.AlternativeMethod = ""
		result.AlternativeExecutionCommand = ""
		result.MethodRationale = auroraGhostRationale
		result.ExecutionCommand = generatePtOSCCommand(input, false)
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
	script.WriteString("-- dbsafe generated chunked script\n")
	fmt.Fprintf(&script, "-- Table: %s.%s\n", db, table)
	fmt.Fprintf(&script, "-- Estimated rows: %d\n", result.AffectedRows)
	fmt.Fprintf(&script, "-- Chunk size: %d\n", input.ChunkSize)
	fmt.Fprintf(&script, "-- Generated: %s\n\n", time.Now().Format(time.RFC3339))

	fmt.Fprintf(&script, "SET @batch_size = %d;\n", input.ChunkSize)
	script.WriteString("SET @sleep_time = 0.5;\n\n")

	script.WriteString("-- Loop: execute in batches\n")
	script.WriteString("-- Adjust @batch_size and @sleep_time as needed\n")

	switch input.Parsed.DMLOp {
	case parser.Delete:
		fmt.Fprintf(&script, `
SET @affected = 1;
WHILE @affected > 0 DO
    DELETE FROM %s.%s
    WHERE %s
    LIMIT @batch_size;
    
    SET @affected = ROW_COUNT();
    SELECT CONCAT('Deleted ', @affected, ' rows') AS progress;
    
    DO SLEEP(@sleep_time);
END WHILE;
`, "`"+db+"`", "`"+table+"`", input.Parsed.WhereClause)

	case parser.Update:
		script.WriteString("-- UPDATE chunking requires a primary key column.\n")
		script.WriteString("-- Use the PK to iterate in ranges.\n")
		script.WriteString("-- Example pattern (adjust for your PK column):\n\n")
		fmt.Fprintf(&script, `
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
			input.Parsed.RawSQL)
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
	fmt.Fprintf(&cmd, "  --user=\"%s\" \\\n", input.Connection.User)

	if input.Connection.Socket != "" {
		fmt.Fprintf(&cmd, "  --socket=\"%s\" \\\n", input.Connection.Socket)
	} else {
		fmt.Fprintf(&cmd, "  --host=\"%s\" \\\n", input.Connection.Host)
		fmt.Fprintf(&cmd, "  --port=%d \\\n", input.Connection.Port)
	}

	fmt.Fprintf(&cmd, "  --database=\"%s\" \\\n", input.Parsed.Database)
	fmt.Fprintf(&cmd, "  --table=\"%s\" \\\n", input.Parsed.Table)
	fmt.Fprintf(&cmd, "  --alter=\"%s\" \\\n", alterSpec)
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
	database := input.Connection.Database
	if database == "" {
		database = input.Parsed.Database
	}
	dsn += fmt.Sprintf(",D=%s,t=%s", database, input.Parsed.Table)

	fmt.Fprintf(&cmd, "  %s \\\n", dsn)
	fmt.Fprintf(&cmd, "  --alter \"%s\" \\\n", alterSpec)
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
