package analyzer

import (
	"testing"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// regressionCase drives TestRegression_FullPipeline.
// sql is parsed by parser.Parse() and fed through the full Analyze() pipeline.
type regressionCase struct {
	name string
	sql  string

	// Environment
	version        mysql.ServerVersion
	topoType       topology.Type
	topoSetup      func(*topology.Info)       // optional: set cluster/cloud fields after Type
	metaSetup      func(*mysql.TableMetadata) // optional: add triggers, adjust columns, etc.
	tableSizeBytes int64
	rowCount       int64
	avgRowLen      int64
	chunkSize      int
	estimatedRows  int64 // injected as Input.EstimatedRows (simulates EXPLAIN output)
	connInfo       *ConnectionInfo

	// Expectations — zero value means "don't check"
	wantRisk            RiskLevel
	wantMethod          ExecutionMethod
	wantAlternative     ExecutionMethod     // positive: must equal this
	wantNoAlternative   bool               // true: AlternativeMethod must be ""
	wantAlgo            Algorithm          // checks result.Classification.Algorithm
	wantDDLOp           parser.DDLOperation // checks result.DDLOp
	wantWarningSubstr   []string            // each substr must appear in result.Warnings
	wantClusterSubstr   []string            // each substr must appear in result.ClusterWarnings
	wantNoWarningSubstr []string            // each substr must NOT appear in result.Warnings
	wantNoClusterSubstr []string            // each substr must NOT appear in result.ClusterWarnings
}

// buildRegressionInput parses tc.sql with the real parser, builds TableMetadata and
// topology.Info from the test-case fields, and returns a wired Input ready for Analyze().
func buildRegressionInput(t *testing.T, tc regressionCase) Input {
	t.Helper()

	parsed, err := parser.Parse(tc.sql)
	if err != nil {
		t.Fatalf("parser.Parse(%q) failed: %v", tc.sql, err)
	}

	// Use "testdb" when the SQL doesn't qualify the table name.
	if parsed.Database == "" {
		parsed.Database = "testdb"
	}
	table := parsed.Table
	if table == "" {
		table = "test_table"
	}

	avgRowLen := tc.avgRowLen
	if avgRowLen == 0 {
		avgRowLen = 100
	}
	rowCount := tc.rowCount
	if rowCount == 0 {
		rowCount = 1000
	}
	chunkSize := tc.chunkSize
	if chunkSize == 0 {
		chunkSize = 10000
	}

	half := tc.tableSizeBytes / 2
	meta := &mysql.TableMetadata{
		Database:     parsed.Database,
		Table:        table,
		DataLength:   half,
		IndexLength:  tc.tableSizeBytes - half,
		RowCount:     rowCount,
		AvgRowLength: avgRowLen,
		// Standard columns present in every case:
		//   "existing_col" → satisfies DROP/MODIFY COLUMN validation
		//   "notes"        → intentionally absent → ADD COLUMN notes passes validation
		//   "existing_col" → ADD COLUMN existing_col triggers "already exists" (case 26)
		Columns: []mysql.ColumnInfo{
			{Name: "id", Type: "int unsigned", Position: 1},
			{Name: "existing_col", Type: "varchar(100)", Position: 2},
		},
	}
	if tc.metaSetup != nil {
		tc.metaSetup(meta)
	}

	topo := &topology.Info{Type: tc.topoType}
	if tc.topoSetup != nil {
		tc.topoSetup(topo)
	}

	return Input{
		Parsed:        parsed,
		Meta:          meta,
		Topo:          topo,
		Version:       tc.version,
		ChunkSize:     chunkSize,
		Connection:    tc.connInfo,
		EstimatedRows: tc.estimatedRows,
	}
}

// TestRegression_FullPipeline runs real SQL through parser.Parse() → Analyze() and
// verifies the complete result. It covers all major code paths:
// DDL algorithms, topology overrides, cloud features (Aurora/RDS), triggers, and DML.
func TestRegression_FullPipeline(t *testing.T) {
	const (
		small = 100 * 1024 * 1024      // 100 MB — below all "large" thresholds
		large = 2 * 1024 * 1024 * 1024 // 2 GB — triggers online-schema-change tools
	)

	v8_0_5  := mysql.ServerVersion{Major: 8, Minor: 0, Patch: 5, Flavor: "mysql"}
	v8_0_35 := mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35, Flavor: "mysql"}
	vAurora := mysql.ServerVersion{
		Major: 8, Minor: 0, Patch: 0,
		Flavor:        "aurora-mysql",
		AuroraVersion: "3.04.0",
	}

	cases := []regressionCase{

		// ─────────────────────────────────────────────────────────────────
		// Group 1 — DDL basics (existing behavior, parser → analyzer path)
		// ─────────────────────────────────────────────────────────────────

		{
			name:           "1. ADD COLUMN INSTANT 8.0.35 standalone small",
			sql:            "ALTER TABLE orders ADD COLUMN notes TEXT",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInstant,
		},
		{
			name:           "2. ADD INDEX INPLACE 8.0.35 standalone small",
			sql:            "ALTER TABLE orders ADD INDEX idx_existing (existing_col)",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInplace,
		},
		{
			name:            "3. MODIFY COLUMN COPY 8.0.35 standalone 2GB → GH-OST + PT-OSC alt",
			sql:             "ALTER TABLE orders MODIFY COLUMN existing_col TEXT",
			version:         v8_0_35,
			topoType:        topology.Standalone,
			tableSizeBytes:  large,
			wantRisk:        RiskDangerous,
			wantMethod:      ExecGhost,
			wantAlternative: ExecPtOSC,
			wantAlgo:        AlgoCopy,
		},
		{
			name:           "4. ADD COLUMN pre-INSTANT 8.0.5 → INPLACE (not INSTANT)",
			sql:            "ALTER TABLE orders ADD COLUMN notes TEXT",
			version:        v8_0_5,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInplace, // V8_0_Early: no INSTANT yet
		},
		{
			name:           "5. DROP COLUMN INSTANT 8.0.35 small",
			sql:            "ALTER TABLE orders DROP COLUMN existing_col",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInstant, // 8.0.29+: INSTANT DROP COLUMN
		},

		// ─────────────────────────────────────────────────────────────────
		// Group 2 — Galera topology
		// ─────────────────────────────────────────────────────────────────

		{
			name:    "6. COPY Galera 2GB → PT-OSC only, no gh-ost",
			sql:     "ALTER TABLE orders MODIFY COLUMN existing_col TEXT",
			version: v8_0_35,
			topoType: topology.Galera,
			topoSetup: func(info *topology.Info) {
				// TOI with cluster size → TOI cluster warning fires for non-INSTANT DDL
				info.GaleraOSUMethod = "TOI"
				info.GaleraClusterSize = 3
			},
			tableSizeBytes:  large,
			wantRisk:        RiskDangerous,
			wantMethod:      ExecPtOSC,
			wantNoAlternative: true, // gh-ost must NOT be offered on Galera
			wantClusterSubstr: []string{"TOI"},
		},
		{
			name:           "7. INSTANT ADD COLUMN Galera small → SAFE DIRECT",
			sql:            "ALTER TABLE orders ADD COLUMN notes TEXT",
			version:        v8_0_35,
			topoType:       topology.Galera,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
		},
		{
			// WriteSetSize = 500K rows × 5000 bytes = 2.5 GB > wsrep_max_ws_size (1 GB)
			name:    "8. DELETE Galera WriteSet exceeds wsrep_max_ws_size → DANGEROUS CHUNKED",
			sql:     "DELETE FROM orders WHERE id > 0",
			version: v8_0_35,
			topoType: topology.Galera,
			topoSetup: func(info *topology.Info) {
				info.WsrepMaxWsSize = 1 * 1024 * 1024 * 1024 // 1 GB limit
			},
			tableSizeBytes: small,
			rowCount:       500000,
			avgRowLen:      5000,
			estimatedRows:  500000,
			wantRisk:       RiskDangerous,
			wantMethod:     ExecChunked,
			wantClusterSubstr: []string{"wsrep_max_ws_size"},
		},

		// ─────────────────────────────────────────────────────────────────
		// Group 3 — Aurora topology (cloud features added in recent release)
		// ─────────────────────────────────────────────────────────────────

		{
			// gh-ost is initially selected for non-Galera COPY+large,
			// then applyAuroraWarnings overrides it to pt-osc.
			name:              "9. Aurora writer COPY 2GB → PT-OSC (gh-ost overridden), Aurora warning",
			sql:               "ALTER TABLE orders MODIFY COLUMN existing_col TEXT",
			version:           vAurora,
			topoType:          topology.AuroraWriter,
			tableSizeBytes:    large,
			wantRisk:          RiskDangerous,
			wantMethod:        ExecPtOSC,
			wantNoAlternative: true,
			wantClusterSubstr: []string{"Aurora"},
		},
		{
			// EffectivePatch() = 23 for Aurora 8.0 → classifyVersion(8,0,23) = V8_0_Instant
			// → ADD COLUMN uses INSTANT algorithm, just like MySQL 8.0.20+
			name:           "10. Aurora writer ADD COLUMN small → INSTANT (EffectivePatch=23)",
			sql:            "ALTER TABLE orders ADD COLUMN notes TEXT",
			version:        vAurora,
			topoType:       topology.AuroraWriter,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInstant,
		},
		{
			name:           "11. Aurora reader + DDL → read-replica cluster warning",
			sql:            "ALTER TABLE orders ADD INDEX idx_existing (existing_col)",
			version:        vAurora,
			topoType:       topology.AuroraReader,
			tableSizeBytes: small,
			wantClusterSubstr: []string{"READ REPLICA"},
		},
		{
			// Explicit check: Patch=0 in ServerVersion, EffectivePatch() returns 23,
			// which places Aurora in V8_0_Instant → INSTANT for ADD COLUMN.
			name:           "12. Aurora EffectivePatch=23 classifies ADD COLUMN as INSTANT",
			sql:            "ALTER TABLE orders ADD COLUMN notes TEXT",
			version:        mysql.ServerVersion{Major: 8, Minor: 0, Patch: 0, Flavor: "aurora-mysql", AuroraVersion: "3.04.0"},
			topoType:       topology.AuroraWriter,
			tableSizeBytes: small,
			wantAlgo:       AlgoInstant,
		},

		// ─────────────────────────────────────────────────────────────────
		// Group 4 — RDS (cloud-managed but not Aurora)
		// ─────────────────────────────────────────────────────────────────

		{
			// gh-ost is recommended (non-Galera, non-Aurora COPY large),
			// and the RDS advisory adds the --allow-on-master cluster warning.
			name:    "13. RDS standalone COPY 2GB → GH-OST + --allow-on-master warning",
			sql:     "ALTER TABLE orders MODIFY COLUMN existing_col TEXT",
			version: v8_0_35,
			topoType: topology.Standalone,
			topoSetup: func(info *topology.Info) {
				info.IsCloudManaged = true
				info.CloudProvider = "aws-rds"
			},
			connInfo:          &ConnectionInfo{Host: "mydb.rds.amazonaws.com", Port: 3306, User: "admin"},
			tableSizeBytes:    large,
			wantRisk:          RiskDangerous,
			wantMethod:        ExecGhost,
			wantClusterSubstr: []string{"--allow-on-master"},
		},
		{
			// INSTANT uses DIRECT → gh-ost is never selected → no RDS warning.
			name:    "14. RDS standalone INSTANT small → SAFE DIRECT, no RDS advisory",
			sql:     "ALTER TABLE orders ADD COLUMN notes TEXT",
			version: v8_0_35,
			topoType: topology.Standalone,
			topoSetup: func(info *topology.Info) {
				info.IsCloudManaged = true
				info.CloudProvider = "aws-rds"
			},
			tableSizeBytes:      small,
			wantRisk:            RiskSafe,
			wantMethod:          ExecDirect,
			wantNoClusterSubstr: []string{"--allow-on-master"},
		},

		// ─────────────────────────────────────────────────────────────────
		// Group 5 — Trigger interaction
		// ─────────────────────────────────────────────────────────────────

		{
			// gh-ost is initially selected (COPY+large+standalone),
			// then the trigger override switches it to pt-osc.
			name:    "15. Large table with trigger → PT-OSC only, gh-ost blocked",
			sql:     "ALTER TABLE orders MODIFY COLUMN existing_col TEXT",
			version: v8_0_35,
			topoType: topology.Standalone,
			metaSetup: func(m *mysql.TableMetadata) {
				m.Triggers = []mysql.TriggerInfo{
					{Name: "trg_audit", Event: "UPDATE", Timing: "AFTER"},
				}
			},
			tableSizeBytes:  large,
			wantRisk:        RiskDangerous,
			wantMethod:      ExecPtOSC,
			wantNoAlternative: true,
		},
		{
			// Both Aurora and the trigger independently force pt-osc.
			// Trigger override fires first (in analyzeDDL), Aurora warning confirms result.
			name:    "16. Aurora writer + trigger + COPY 2GB → PT-OSC",
			sql:     "ALTER TABLE orders MODIFY COLUMN existing_col TEXT",
			version: vAurora,
			topoType: topology.AuroraWriter,
			metaSetup: func(m *mysql.TableMetadata) {
				m.Triggers = []mysql.TriggerInfo{
					{Name: "trg_orders_upd", Event: "UPDATE", Timing: "AFTER"},
				}
			},
			tableSizeBytes:  large,
			wantRisk:        RiskDangerous,
			wantMethod:      ExecPtOSC,
			wantNoAlternative: true,
		},

		// ─────────────────────────────────────────────────────────────────
		// Group 6 — DML
		// ─────────────────────────────────────────────────────────────────

		{
			// HasWhere=true + EstimatedRows=50 → AffectedRows=50 < 10K → SAFE DIRECT.
			name:          "17. DELETE WHERE small (EXPLAIN=50 rows) → SAFE DIRECT",
			sql:           "DELETE FROM orders WHERE id > 1000",
			version:       v8_0_35,
			topoType:      topology.Standalone,
			tableSizeBytes: small,
			rowCount:      100000,
			estimatedRows: 50,
			wantRisk:      RiskSafe,
			wantMethod:    ExecDirect,
		},
		{
			// HasWhere=true + EstimatedRows=500K → AffectedRows=500K > 100K → DANGEROUS CHUNKED.
			name:          "18. DELETE WHERE large (EXPLAIN=500K rows) → DANGEROUS CHUNKED",
			sql:           "DELETE FROM orders WHERE id > 0",
			version:       v8_0_35,
			topoType:      topology.Standalone,
			tableSizeBytes: small,
			rowCount:      1000000,
			estimatedRows: 500000,
			wantRisk:      RiskDangerous,
			wantMethod:    ExecChunked,
		},
		{
			// HasWhere=false → AffectedRows = all rows (1000) → DANGEROUS + no-WHERE warning.
			// Table is small (1K rows) → method stays DIRECT (< 10K row threshold).
			name:           "19. UPDATE without WHERE → DANGEROUS, all-rows warning",
			sql:            "UPDATE orders SET existing_col = 'archived'",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			rowCount:       1000,
			wantRisk:       RiskDangerous,
			wantWarningSubstr: []string{"No WHERE clause"},
		},
		{
			// Galera: HasWhere=true + EstimatedRows=500K → WriteSetSize=2.5GB > 1GB limit.
			// Both the row-count threshold and the write-set check push to CHUNKED.
			name:    "20. DELETE Galera write-set limit exceeded → DANGEROUS CHUNKED",
			sql:     "DELETE FROM orders WHERE id > 0",
			version: v8_0_35,
			topoType: topology.Galera,
			topoSetup: func(info *topology.Info) {
				info.WsrepMaxWsSize = 1 * 1024 * 1024 * 1024 // 1 GB
			},
			tableSizeBytes: small,
			rowCount:       500000,
			avgRowLen:      5000,
			estimatedRows:  500000,
			wantRisk:       RiskDangerous,
			wantMethod:     ExecChunked,
			wantClusterSubstr: []string{"wsrep_max_ws_size"},
		},

		// ─────────────────────────────────────────────────────────────────
		// Group 7 — Group Replication
		// ─────────────────────────────────────────────────────────────────

		{
			name:    "21. GR multi-primary + DDL → conflicting-DDL cluster warning",
			sql:     "ALTER TABLE orders ADD INDEX idx_existing (existing_col)",
			version: v8_0_35,
			topoType: topology.GroupRepl,
			topoSetup: func(info *topology.Info) {
				info.GRMode = "MULTI-PRIMARY"
			},
			tableSizeBytes:    small,
			wantClusterSubstr: []string{"multi-primary"},
		},
		{
			// GR: WriteSetSize=2.5GB > GRTransactionLimit(1GB) → CHUNKED + cluster warning.
			name:    "22. GR large DML exceeds transaction limit → DANGEROUS CHUNKED",
			sql:     "DELETE FROM orders WHERE id > 0",
			version: v8_0_35,
			topoType: topology.GroupRepl,
			topoSetup: func(info *topology.Info) {
				info.GRTransactionLimit = 1 * 1024 * 1024 * 1024 // 1 GB
			},
			tableSizeBytes: small,
			rowCount:       500000,
			avgRowLen:      5000,
			estimatedRows:  500000,
			wantRisk:       RiskDangerous,
			wantMethod:     ExecChunked,
			wantClusterSubstr: []string{"group_replication_transaction_size_limit"},
		},

		// ─────────────────────────────────────────────────────────────────
		// Group 8 — Edge cases
		// ─────────────────────────────────────────────────────────────────

		{
			// RENAME TABLE is an atomic metadata operation in InnoDB → INSTANT, no lock.
			name:           "23. RENAME TABLE → INSTANT SAFE DIRECT",
			sql:            "RENAME TABLE orders TO orders_old",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInstant,
			wantDDLOp:      parser.RenameTable,
		},
		{
			// DROP INDEX is metadata-only (INPLACE, no table rebuild) in all MySQL 8.0+ versions.
			name:           "24. DROP INDEX → INPLACE SAFE DIRECT",
			sql:            "ALTER TABLE orders DROP INDEX idx_existing",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInplace,
			wantDDLOp:      parser.DropIndex,
		},
		{
			// Multi-op ALTER → parser sets DDLOp=MultipleOps (not OtherDDL).
			// Matrix has no MultipleOps entry → falls back to COPY+SHARED (safe default).
			// The "could not be fully parsed" warning is ONLY for OtherDDL, not MultipleOps.
			name:           "25. Multiple ALTERs → MultipleOps, COPY fallback, no parse-error warning",
			sql:            "ALTER TABLE orders ADD COLUMN notes TEXT, ADD INDEX idx_notes (notes)",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantDDLOp:      parser.MultipleOps,
			wantAlgo:       AlgoCopy, // matrix default for unknown operations
			wantNoWarningSubstr: []string{"could not be fully parsed"},
		},
		{
			// Column validation: existing_col is already in meta.Columns →
			// ADD COLUMN existing_col triggers "already exists" warning and DANGEROUS risk.
			name:           "26. ADD existing column → DANGEROUS + already-exists warning",
			sql:            "ALTER TABLE orders ADD COLUMN existing_col TEXT",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantRisk:       RiskDangerous,
			wantWarningSubstr: []string{"already exists"},
		},
		{
			// OPTIMIZE TABLE is a statement-level DDL that reorganizes the table and updates
			// index statistics. The matrix classifies it as INPLACE with a table rebuild.
			name:           "27. OPTIMIZE TABLE → INPLACE with rebuild, SAFE DIRECT",
			sql:            "OPTIMIZE TABLE orders",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInplace,
			wantDDLOp:      parser.OptimizeTable,
		},
		{
			// ALTER TABLESPACE RENAME is a metadata-only operation (INPLACE, no table rebuild).
			// v8_0_35 (> 8.0.21) → no version-too-old warning fires.
			name:           "28. ALTER TABLESPACE RENAME → INPLACE SAFE DIRECT",
			sql:            "ALTER TABLESPACE ts1 RENAME TO ts2",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: 0, // no table involved
			wantRisk:       RiskSafe,
			wantMethod:     ExecDirect,
			wantAlgo:       AlgoInplace,
			wantDDLOp:      parser.AlterTablespace,
		},
		{
			// MODIFY COLUMN with an explicit charset change (utf8mb3 → utf8mb4):
			// the charset guard fires and keeps the COPY baseline, adding a specific warning.
			name: "29. MODIFY COLUMN charset change utf8mb3→utf8mb4 → COPY + charset warning",
			sql:  "ALTER TABLE orders MODIFY COLUMN existing_col VARCHAR(100) CHARACTER SET utf8mb4",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			metaSetup: func(m *mysql.TableMetadata) {
				cs := "utf8mb3"
				m.Columns[1].CharacterSet = &cs // existing_col gets old charset
			},
			wantAlgo:          AlgoCopy,
			wantDDLOp:         parser.ModifyColumn,
			wantWarningSubstr: []string{"charset change"},
		},
		{
			// MODIFY COLUMN with same charset (utf8mb4) and VARCHAR tier unchanged:
			// 100×4=400 bytes and 200×4=800 bytes are both in the 2-byte prefix tier (>255),
			// so classifyModifyColumnVarchar returns INPLACE.
			name: "30. MODIFY COLUMN same charset VARCHAR expansion → INPLACE",
			sql:  "ALTER TABLE orders MODIFY COLUMN existing_col VARCHAR(200) CHARACTER SET utf8mb4",
			version:        v8_0_35,
			topoType:       topology.Standalone,
			tableSizeBytes: small,
			metaSetup: func(m *mysql.TableMetadata) {
				cs := "utf8mb4"
				m.Columns[1].CharacterSet = &cs
			},
			wantAlgo:  AlgoInplace,
			wantDDLOp: parser.ModifyColumn,
		},
	}

	for _, tc := range cases {
		tc := tc // capture loop variable
		t.Run(tc.name, func(t *testing.T) {
			input := buildRegressionInput(t, tc)
			result := Analyze(input)

			// Risk level
			if tc.wantRisk != "" && result.Risk != tc.wantRisk {
				t.Errorf("Risk = %q, want %q", result.Risk, tc.wantRisk)
			}

			// Primary execution method
			if tc.wantMethod != "" && result.Method != tc.wantMethod {
				t.Errorf("Method = %q, want %q", result.Method, tc.wantMethod)
			}

			// Alternative method — positive check
			if tc.wantAlternative != "" && result.AlternativeMethod != tc.wantAlternative {
				t.Errorf("AlternativeMethod = %q, want %q", result.AlternativeMethod, tc.wantAlternative)
			}

			// Alternative method — negative check (must be empty)
			if tc.wantNoAlternative && result.AlternativeMethod != "" {
				t.Errorf("AlternativeMethod = %q, want empty (topology/trigger must suppress alternatives)", result.AlternativeMethod)
			}

			// DDL classification algorithm
			if tc.wantAlgo != "" && result.Classification.Algorithm != tc.wantAlgo {
				t.Errorf("Classification.Algorithm = %q, want %q", result.Classification.Algorithm, tc.wantAlgo)
			}

			// DDL operation extracted by parser
			if tc.wantDDLOp != "" && result.DDLOp != tc.wantDDLOp {
				t.Errorf("DDLOp = %q, want %q", result.DDLOp, tc.wantDDLOp)
			}

			// Warnings — must contain
			for _, substr := range tc.wantWarningSubstr {
				if !containsWarning(result.Warnings, substr) {
					t.Errorf("Warnings missing %q\n  got: %v", substr, result.Warnings)
				}
			}

			// Cluster warnings — must contain
			for _, substr := range tc.wantClusterSubstr {
				if !containsWarning(result.ClusterWarnings, substr) {
					t.Errorf("ClusterWarnings missing %q\n  got: %v", substr, result.ClusterWarnings)
				}
			}

			// Warnings — must NOT contain
			for _, substr := range tc.wantNoWarningSubstr {
				if containsWarning(result.Warnings, substr) {
					t.Errorf("Warnings should NOT contain %q\n  got: %v", substr, result.Warnings)
				}
			}

			// Cluster warnings — must NOT contain
			for _, substr := range tc.wantNoClusterSubstr {
				if containsWarning(result.ClusterWarnings, substr) {
					t.Errorf("ClusterWarnings should NOT contain %q\n  got: %v", substr, result.ClusterWarnings)
				}
			}
		})
	}
}
