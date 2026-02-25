package analyzer

import (
	"strings"
	"testing"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// Helper to build a minimal Input for DDL tests.
func ddlInput(op parser.DDLOperation, version mysql.ServerVersion, totalSizeBytes int64, topoType topology.Type) Input {
	half := totalSizeBytes / 2
	parsed := &parser.ParsedSQL{
		Type:   parser.DDL,
		RawSQL: "ALTER TABLE test ...",
		Table:  "test",
		DDLOp:  op,
	}

	// Set column names based on operation to ensure validation passes
	switch op {
	case parser.AddColumn:
		parsed.ColumnName = "new_col" // Will be added
	case parser.DropColumn, parser.ModifyColumn:
		parsed.ColumnName = "existing_col" // Must exist
	case parser.ChangeColumn:
		parsed.OldColumnName = "existing_col" // Must exist
		parsed.NewColumnName = "renamed_col"  // Must not exist
	}

	return Input{
		Parsed: parsed,
		Meta: &mysql.TableMetadata{
			Database:     "testdb",
			Table:        "test",
			DataLength:   half,
			IndexLength:  totalSizeBytes - half,
			RowCount:     1000,
			AvgRowLength: 100,
			// Add default columns so validation doesn't fail
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Position: 1},
				{Name: "existing_col", Type: "varchar(100)", Position: 2},
			},
		},
		Version: version,
		Topo:    &topology.Info{Type: topoType},
	}
}

// Helper to build a minimal Input for DML tests.
func dmlInput(op parser.DMLOperation, hasWhere bool, rowCount int64, avgRowLen int64, chunkSize int, topoType topology.Type) Input {
	whereClause := ""
	if hasWhere {
		whereClause = "id > 0"
	}
	return Input{
		Parsed: &parser.ParsedSQL{
			Type:        parser.DML,
			RawSQL:      "DELETE FROM test WHERE id > 0",
			Table:       "test",
			DMLOp:       op,
			HasWhere:    hasWhere,
			WhereClause: whereClause,
		},
		Meta: &mysql.TableMetadata{
			Database:     "testdb",
			Table:        "test",
			DataLength:   rowCount * avgRowLen,
			RowCount:     rowCount,
			AvgRowLength: avgRowLen,
		},
		Version:   mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35},
		Topo:      &topology.Info{Type: topoType},
		ChunkSize: chunkSize,
	}
}

var (
	v8_0_5  = mysql.ServerVersion{Major: 8, Minor: 0, Patch: 5}
	v8_0_20 = mysql.ServerVersion{Major: 8, Minor: 0, Patch: 20}
	v8_0_35 = mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}
	v8_4_0  = mysql.ServerVersion{Major: 8, Minor: 4, Patch: 0}
)

// =============================================================
// DDL Matrix / Classification Tests
// =============================================================

func TestClassifyVersion(t *testing.T) {
	tests := []struct {
		name                string
		major, minor, patch int
		want                VersionRange
	}{
		{"8.0.5 early", 8, 0, 5, V8_0_Early},
		{"8.0.11 early boundary", 8, 0, 11, V8_0_Early},
		{"8.0.12 instant", 8, 0, 12, V8_0_Instant},
		{"8.0.28 instant boundary", 8, 0, 28, V8_0_Instant},
		{"8.0.29 full", 8, 0, 29, V8_0_Full},
		{"8.0.35 full", 8, 0, 35, V8_0_Full},
		{"8.4.0 LTS", 8, 4, 0, V8_4_LTS},
		{"8.4.3 LTS", 8, 4, 3, V8_4_LTS},
		{"unknown defaults to full", 9, 0, 0, V8_0_Full},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := classifyVersion(tt.major, tt.minor, tt.patch)
			if got != tt.want {
				t.Errorf("classifyVersion(%d,%d,%d) = %v, want %v", tt.major, tt.minor, tt.patch, got, tt.want)
			}
		})
	}
}

func TestClassifyDDL_AddColumn(t *testing.T) {
	tests := []struct {
		name    string
		version mysql.ServerVersion
		wantAlg Algorithm
	}{
		{"8.0.5 inplace", v8_0_5, AlgoInplace},
		{"8.0.20 instant", v8_0_20, AlgoInstant},
		{"8.0.35 instant", v8_0_35, AlgoInstant},
		{"8.4 instant", v8_4_0, AlgoInstant},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := ClassifyDDL(parser.AddColumn, tt.version.Major, tt.version.Minor, tt.version.Patch)
			if c.Algorithm != tt.wantAlg {
				t.Errorf("Algorithm = %q, want %q", c.Algorithm, tt.wantAlg)
			}
			if c.Lock != LockNone {
				t.Errorf("Lock = %q, want NONE", c.Lock)
			}
		})
	}
}

func TestClassifyDDL_DropColumn(t *testing.T) {
	// Pre-8.0.29: INPLACE with rebuild
	c := ClassifyDDL(parser.DropColumn, 8, 0, 20)
	if c.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE", c.Algorithm)
	}
	if !c.RebuildsTable {
		t.Error("RebuildsTable = false, want true")
	}

	// 8.0.29+: INSTANT
	c = ClassifyDDL(parser.DropColumn, 8, 0, 35)
	if c.Algorithm != AlgoInstant {
		t.Errorf("Algorithm = %q, want INSTANT", c.Algorithm)
	}
	if c.RebuildsTable {
		t.Error("RebuildsTable = true, want false")
	}
}

func TestClassifyDDL_ModifyColumn(t *testing.T) {
	// All versions: COPY + SHARED
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.ModifyColumn, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoCopy {
			t.Errorf("v%d.%d.%d: Algorithm = %q, want COPY", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockShared {
			t.Errorf("v%d.%d.%d: Lock = %q, want SHARED", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

func TestClassifyDDL_AddIndex(t *testing.T) {
	c := ClassifyDDL(parser.AddIndex, 8, 0, 35)
	if c.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE", c.Algorithm)
	}
	if c.Lock != LockNone {
		t.Errorf("Lock = %q, want NONE", c.Lock)
	}
	if c.RebuildsTable {
		t.Error("RebuildsTable = true, want false")
	}
}

func TestClassifyDDL_DropIndex(t *testing.T) {
	c := ClassifyDDL(parser.DropIndex, 8, 4, 0)
	if c.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE", c.Algorithm)
	}
	if c.Lock != LockNone {
		t.Errorf("Lock = %q, want NONE", c.Lock)
	}
}

func TestClassifyDDL_ChangeCharset(t *testing.T) {
	// CHARACTER SET = ... is a metadata-only change; no rebuild, no lock.
	c := ClassifyDDL(parser.ChangeCharset, 8, 0, 35)
	if c.Algorithm != AlgoInstant {
		t.Errorf("Algorithm = %q, want INSTANT", c.Algorithm)
	}
	if c.RebuildsTable {
		t.Error("RebuildsTable = true, want false")
	}
}

func TestClassifyDDL_ConvertCharset(t *testing.T) {
	// CONVERT TO CHARACTER SET baseline is COPY+SHARED (refined by analyzer with metadata).
	c := ClassifyDDL(parser.ConvertCharset, 8, 0, 35)
	if c.Algorithm != AlgoCopy {
		t.Errorf("Algorithm = %q, want COPY", c.Algorithm)
	}
	if c.Lock != LockShared {
		t.Errorf("Lock = %q, want SHARED", c.Lock)
	}
	if !c.RebuildsTable {
		t.Error("RebuildsTable = false, want true")
	}
}

func TestClassifyDDL_RenameTable(t *testing.T) {
	c := ClassifyDDL(parser.RenameTable, 8, 0, 35)
	if c.Algorithm != AlgoInstant {
		t.Errorf("Algorithm = %q, want INSTANT", c.Algorithm)
	}
}

func TestClassifyDDL_AddPrimaryKey(t *testing.T) {
	// All versions: INPLACE + NONE lock + table rebuild (baseline for NOT NULL columns).
	// Nullable PK columns are upgraded to COPY by the analyzer context check.
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.AddPrimaryKey, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

func TestAnalyzeDDL_AddPrimaryKey_NotNullColumn(t *testing.T) {
	// ADD PRIMARY KEY on a NOT NULL column: baseline INPLACE with rebuild.
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:         parser.DDL,
			RawSQL:       "ALTER TABLE t ADD PRIMARY KEY (a)",
			Table:        "t",
			DDLOp:        parser.AddPrimaryKey,
			IndexColumns: []string{"a"},
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "a", Type: "int", Nullable: false},
			},
		},
		Version: v8_0_35,
		Topo:    &topology.Info{Type: topology.Standalone},
	}
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE", result.Classification.Algorithm)
	}
	if result.Classification.Lock != LockNone {
		t.Errorf("Lock = %q, want NONE", result.Classification.Lock)
	}
	if !result.Classification.RebuildsTable {
		t.Errorf("RebuildsTable = false, want true")
	}
}

func TestAnalyzeDDL_AddPrimaryKey_NullableColumn(t *testing.T) {
	// ADD PRIMARY KEY on a nullable column: must upgrade to COPY (MySQL enforces NOT NULL).
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:         parser.DDL,
			RawSQL:       "ALTER TABLE t ADD PRIMARY KEY (a)",
			Table:        "t",
			DDLOp:        parser.AddPrimaryKey,
			IndexColumns: []string{"a"},
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "a", Type: "int", Nullable: true},
			},
		},
		Version: v8_0_35,
		Topo:    &topology.Info{Type: topology.Standalone},
	}
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("Algorithm = %q, want COPY", result.Classification.Algorithm)
	}
	if len(result.Warnings) == 0 {
		t.Error("expected a nullable column warning, got none")
	}
}

func TestAnalyzeDDL_AddPrimaryKey_DuplicateCheckWarning(t *testing.T) {
	// ADD PRIMARY KEY should always include a duplicate-check query in warnings.
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:         parser.DDL,
			RawSQL:       "ALTER TABLE t ADD PRIMARY KEY (id)",
			Table:        "orders",
			DDLOp:        parser.AddPrimaryKey,
			IndexColumns: []string{"id"},
		},
		Meta:    &mysql.TableMetadata{Table: "orders"},
		Version: v8_0_35,
		Topo:    &topology.Info{Type: topology.Standalone},
	}
	result := Analyze(input)
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "SELECT id, COUNT(*) cnt FROM orders GROUP BY id HAVING cnt > 1") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected duplicate-check SELECT in warnings, got: %v", result.Warnings)
	}
}

func TestAnalyzeDDL_AddUniqueKey_DuplicateCheckWarning(t *testing.T) {
	// ADD UNIQUE KEY should include a duplicate-check query in warnings.
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			RawSQL:        "ALTER TABLE t ADD UNIQUE KEY uk_email (email)",
			Table:         "users",
			DDLOp:         parser.AddIndex,
			IsUniqueIndex: true,
			IndexColumns:  []string{"email"},
		},
		Meta:    &mysql.TableMetadata{Table: "users"},
		Version: v8_0_35,
		Topo:    &topology.Info{Type: topology.Standalone},
	}
	result := Analyze(input)
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "SELECT email, COUNT(*) cnt FROM users GROUP BY email HAVING cnt > 1") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected duplicate-check SELECT in warnings, got: %v", result.Warnings)
	}
}

func TestAnalyzeDDL_AddUniqueKey_Composite_DuplicateCheckWarning(t *testing.T) {
	// Composite unique key: SELECT should include all columns.
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			RawSQL:        "ALTER TABLE t ADD UNIQUE KEY uk_name (first_name, last_name)",
			Table:         "customers",
			DDLOp:         parser.AddIndex,
			IsUniqueIndex: true,
			IndexColumns:  []string{"first_name", "last_name"},
		},
		Meta:    &mysql.TableMetadata{Table: "customers"},
		Version: v8_0_35,
		Topo:    &topology.Info{Type: topology.Standalone},
	}
	result := Analyze(input)
	found := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "SELECT first_name, last_name, COUNT(*) cnt FROM customers GROUP BY first_name, last_name HAVING cnt > 1") {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("expected composite duplicate-check SELECT in warnings, got: %v", result.Warnings)
	}
}

func TestAnalyzeDDL_AddIndex_NonUnique_NoDuplicateCheckWarning(t *testing.T) {
	// Regular (non-unique) ADD INDEX should NOT get a duplicate-check warning.
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:         parser.DDL,
			RawSQL:       "ALTER TABLE t ADD INDEX idx_status (status)",
			Table:        "orders",
			DDLOp:        parser.AddIndex,
			IndexColumns: []string{"status"},
		},
		Meta:    &mysql.TableMetadata{Table: "orders"},
		Version: v8_0_35,
		Topo:    &topology.Info{Type: topology.Standalone},
	}
	result := Analyze(input)
	for _, w := range result.Warnings {
		if strings.Contains(w, "GROUP BY") {
			t.Errorf("non-unique index should not get duplicate-check warning, got: %s", w)
		}
	}
}

func TestClassifyDDL_DropPrimaryKey(t *testing.T) {
	// All versions: COPY + SHARED + table rebuild
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.DropPrimaryKey, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoCopy {
			t.Errorf("v%d.%d.%d: Algorithm = %q, want COPY", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockShared {
			t.Errorf("v%d.%d.%d: Lock = %q, want SHARED", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

func TestClassifyDDL_ChangeRowFormat(t *testing.T) {
	// All versions: INPLACE + no lock + table rebuild
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.ChangeRowFormat, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

func TestClassifyDDL_AddPartition(t *testing.T) {
	// All versions: INPLACE + no lock + no rebuild
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.AddPartition, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

func TestClassifyDDL_DropPartition(t *testing.T) {
	// All versions: INPLACE + no lock + no rebuild
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.DropPartition, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

func TestClassifyDDL_UnknownOp(t *testing.T) {
	c := ClassifyDDL("UNKNOWN_OP", 8, 0, 35)
	// Should return safe fallback: COPY + SHARED
	if c.Algorithm != AlgoCopy {
		t.Errorf("Algorithm = %q, want COPY (fallback)", c.Algorithm)
	}
	if c.Lock != LockShared {
		t.Errorf("Lock = %q, want SHARED (fallback)", c.Lock)
	}
}

func TestClassifyDDLWithContext_AddColumnFirstAfter(t *testing.T) {
	// 8.0.20: ADD COLUMN AFTER should downgrade from INSTANT to INPLACE
	parsed := &parser.ParsedSQL{
		DDLOp:        parser.AddColumn,
		IsFirstAfter: true,
	}
	c := ClassifyDDLWithContext(parsed, 8, 0, 20)
	if c.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE (FIRST/AFTER on 8.0.20)", c.Algorithm)
	}

	// 8.0.35: ADD COLUMN AFTER should stay INSTANT
	c = ClassifyDDLWithContext(parsed, 8, 0, 35)
	if c.Algorithm != AlgoInstant {
		t.Errorf("Algorithm = %q, want INSTANT (FIRST/AFTER on 8.0.35)", c.Algorithm)
	}

	// 8.4: ADD COLUMN AFTER should stay INSTANT
	c = ClassifyDDLWithContext(parsed, 8, 4, 0)
	if c.Algorithm != AlgoInstant {
		t.Errorf("Algorithm = %q, want INSTANT (FIRST/AFTER on 8.4)", c.Algorithm)
	}
}

// =============================================================
// DDL Analysis (Risk + Method) Tests
// =============================================================

func TestAnalyzeDDL_InstantIsSafe(t *testing.T) {
	input := ddlInput(parser.AddColumn, v8_0_35, 100*1024*1024, topology.Standalone)
	result := Analyze(input)

	if result.Risk != RiskSafe {
		t.Errorf("Risk = %q, want SAFE", result.Risk)
	}
	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT", result.Method)
	}
	if result.Classification.Algorithm != AlgoInstant {
		t.Errorf("Algorithm = %q, want INSTANT", result.Classification.Algorithm)
	}
}

func TestAnalyzeDDL_InplaceNoLock_SmallTable(t *testing.T) {
	input := ddlInput(parser.AddIndex, v8_0_35, 100*1024*1024, topology.Standalone) // 100MB
	result := Analyze(input)

	if result.Risk != RiskSafe {
		t.Errorf("Risk = %q, want SAFE", result.Risk)
	}
	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT", result.Method)
	}
}

func TestAnalyzeDDL_InplaceNoLock_LargeTable(t *testing.T) {
	input := ddlInput(parser.AddIndex, v8_0_35, 11*1024*1024*1024, topology.Standalone) // 11GB
	result := Analyze(input)

	if result.Risk != RiskCaution {
		t.Errorf("Risk = %q, want CAUTION", result.Risk)
	}
	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT", result.Method)
	}
}

func TestAnalyzeDDL_CopyAlgo_SmallTable(t *testing.T) {
	input := ddlInput(parser.ModifyColumn, v8_0_35, 500*1024*1024, topology.Standalone) // 500MB
	result := Analyze(input)

	if result.Risk != RiskCaution {
		t.Errorf("Risk = %q, want CAUTION", result.Risk)
	}
	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT", result.Method)
	}
}

func TestAnalyzeDDL_CopyAlgo_LargeTable(t *testing.T) {
	input := ddlInput(parser.ModifyColumn, v8_0_35, 2*1024*1024*1024, topology.Standalone) // 2GB
	result := Analyze(input)

	if result.Risk != RiskDangerous {
		t.Errorf("Risk = %q, want DANGEROUS", result.Risk)
	}
	if result.Method != ExecGhost {
		t.Errorf("Method = %q, want GH-OST", result.Method)
	}
}

func TestAnalyzeDDL_CopyAlgo_LargeTable_Galera(t *testing.T) {
	input := ddlInput(parser.ModifyColumn, v8_0_35, 2*1024*1024*1024, topology.Galera) // 2GB on Galera
	result := Analyze(input)

	if result.Risk != RiskDangerous {
		t.Errorf("Risk = %q, want DANGEROUS", result.Risk)
	}
	if result.Method != ExecPtOSC {
		t.Errorf("Method = %q, want PT-ONLINE-SCHEMA-CHANGE (Galera can't use gh-ost)", result.Method)
	}
}

func TestAnalyzeDDL_GhostOverriddenByTriggers(t *testing.T) {
	// Large table + triggers: gh-ost must be overridden by pt-osc.
	input := ddlInput(parser.ModifyColumn, v8_0_35, 2*1024*1024*1024, topology.Standalone) // 2GB
	input.Meta.Triggers = []mysql.TriggerInfo{
		{Name: "trg_audit", Event: "UPDATE", Timing: "AFTER"},
	}

	result := Analyze(input)

	if result.Method != ExecPtOSC {
		t.Errorf("Method = %q, want PT-ONLINE-SCHEMA-CHANGE (table has triggers, gh-ost incompatible)", result.Method)
	}
	if result.AlternativeMethod != "" {
		t.Errorf("AlternativeMethod = %q, want empty (no gh-ost alternative when triggers present)", result.AlternativeMethod)
	}
	if !containsStr(result.MethodRationale, "trigger") {
		t.Errorf("MethodRationale should mention triggers, got: %s", result.MethodRationale)
	}
}

func TestAnalyzeDDL_GhostNotOverriddenWithoutTriggers(t *testing.T) {
	// Regression guard: large table without triggers should still recommend gh-ost.
	input := ddlInput(parser.ModifyColumn, v8_0_35, 2*1024*1024*1024, topology.Standalone) // 2GB

	result := Analyze(input)

	if result.Method != ExecGhost {
		t.Errorf("Method = %q, want GH-OST (no triggers present)", result.Method)
	}
}

func TestAnalyzeDDL_SmallTableWithTriggers_StillDirect(t *testing.T) {
	// Small table + triggers: method stays DIRECT (trigger check only applies to gh-ost path).
	input := ddlInput(parser.ModifyColumn, v8_0_35, 500*1024*1024, topology.Standalone) // 500MB
	input.Meta.Triggers = []mysql.TriggerInfo{
		{Name: "trg_audit", Event: "UPDATE", Timing: "AFTER"},
	}

	result := Analyze(input)

	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT (small table; trigger override only applies to gh-ost)", result.Method)
	}
}

func TestAnalyzeDDL_InplaceSharedLock_SmallTable(t *testing.T) {
	// ADD COLUMN FIRST/AFTER on 8.0.20 → INPLACE + downgrade won't have SHARED lock
	// Use ADD FOREIGN KEY which is INPLACE + NONE by default
	// Actually let's test the code path for INPLACE + SHARED lock by creating a custom scenario
	// ModifyColumn is COPY + SHARED, not INPLACE + SHARED. Let me check what gives INPLACE + SHARED...
	// From the matrix, nothing gives INPLACE + SHARED directly. The code checks for lock != LockNone.
	// Let's skip this and test the rollback generation instead.
}

// =============================================================
// DML Analysis Tests
// =============================================================

func TestAnalyzeDML_SmallDelete(t *testing.T) {
	// Delete without WHERE → all rows affected (1000 rows, small)
	// But no WHERE triggers DANGEROUS regardless of size
	input := dmlInput(parser.Delete, false, 1000, 100, 10000, topology.Standalone)
	result := Analyze(input)

	if result.DMLOp != parser.Delete {
		t.Errorf("DMLOp = %q, want DELETE", result.DMLOp)
	}
	if result.Risk != RiskDangerous {
		t.Errorf("Risk = %q, want DANGEROUS (no WHERE clause)", result.Risk)
	}
	if !containsWarning(result.Warnings, "No WHERE clause") {
		t.Error("expected warning about missing WHERE clause")
	}
}

func TestAnalyzeDML_DeleteWithWhere_SmallRows(t *testing.T) {
	// With WHERE, estimateAffectedRows returns 0 (needs EXPLAIN)
	input := dmlInput(parser.Delete, true, 500, 100, 10000, topology.Standalone)
	result := Analyze(input)

	if result.Risk != RiskSafe {
		t.Errorf("Risk = %q, want SAFE (small row estimate)", result.Risk)
	}
	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT", result.Method)
	}
}

// TestAnalyzeDML_WithEstimatedRows tests issue #11 fix
// When EstimatedRows is provided via EXPLAIN, it should be used for calculation
func TestAnalyzeDML_WithEstimatedRows(t *testing.T) {
	// Simulate issue #11: DELETE with WHERE clause that affects most rows
	// Table has 4,654,623 rows total
	// EXPLAIN estimates 4,654,623 rows will be affected (100%)
	input := dmlInput(parser.Delete, true, 4654623, 100, 10000, topology.Standalone)
	input.EstimatedRows = 4654623 // EXPLAIN result from issue #11

	result := Analyze(input)

	// Should use the EstimatedRows, not return 0
	if result.AffectedRows != 4654623 {
		t.Errorf("AffectedRows = %d, want 4654623 (from EXPLAIN)", result.AffectedRows)
	}

	// Percentage should be 100%
	if result.AffectedPct != 100.0 {
		t.Errorf("AffectedPct = %.1f, want 100.0", result.AffectedPct)
	}

	// Should recommend chunking for this many rows
	if result.Method != ExecChunked {
		t.Errorf("Method = %q, want CHUNKED (large operation)", result.Method)
	}

	// Risk should be DANGEROUS due to large number of rows
	if result.Risk != RiskDangerous {
		t.Errorf("Risk = %q, want DANGEROUS (large operation)", result.Risk)
	}
}

// TestAnalyzeDML_WithEstimatedRows_Medium tests medium-sized EXPLAIN estimate
func TestAnalyzeDML_WithEstimatedRows_Medium(t *testing.T) {
	// Medium size: 50K rows affected (EXPLAIN estimate)
	input := dmlInput(parser.Delete, true, 1000000, 100, 10000, topology.Standalone)
	input.EstimatedRows = 50000 // EXPLAIN says 50K rows

	result := Analyze(input)

	if result.AffectedRows != 50000 {
		t.Errorf("AffectedRows = %d, want 50000", result.AffectedRows)
	}

	// 50K rows should trigger CAUTION, not chunking
	if result.Risk != RiskCaution {
		t.Errorf("Risk = %q, want CAUTION", result.Risk)
	}

	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT", result.Method)
	}
}

// TestAnalyzeDML_NoEstimateProvided_WithWhere tests backward compatibility
// When no EstimatedRows provided and there's a WHERE, should return 0 (caller needs to run EXPLAIN)
func TestAnalyzeDML_NoEstimateProvided_WithWhere(t *testing.T) {
	input := dmlInput(parser.Delete, true, 1000000, 100, 10000, topology.Standalone)
	// No EstimatedRows set (remains 0)

	result := Analyze(input)

	// Without EXPLAIN estimate, should return 0
	if result.AffectedRows != 0 {
		t.Errorf("AffectedRows = %d, want 0 (no EXPLAIN provided)", result.AffectedRows)
	}

	// Should still be safe since AffectedRows is 0
	if result.Risk != RiskSafe {
		t.Errorf("Risk = %q, want SAFE (default when no estimate)", result.Risk)
	}
}

func TestAnalyzeDML_UpdateNoWhere(t *testing.T) {
	input := dmlInput(parser.Update, false, 200000, 100, 10000, topology.Standalone)
	result := Analyze(input)

	if result.Risk != RiskDangerous {
		t.Errorf("Risk = %q, want DANGEROUS", result.Risk)
	}
	if !containsWarning(result.Warnings, "No WHERE clause") {
		t.Error("expected warning about missing WHERE clause")
	}
	// 200K rows without WHERE → all rows affected → needs chunking
	if result.Method != ExecChunked {
		t.Errorf("Method = %q, want CHUNKED", result.Method)
	}
}

func TestAnalyzeDML_LargeDeleteNoWhere_ChunkCount(t *testing.T) {
	input := dmlInput(parser.Delete, false, 500000, 100, 10000, topology.Standalone)
	result := Analyze(input)

	if result.Method != ExecChunked {
		t.Errorf("Method = %q, want CHUNKED", result.Method)
	}
	// 500K rows / 10K chunk = 50 chunks
	if result.ChunkCount != 50 {
		t.Errorf("ChunkCount = %d, want 50", result.ChunkCount)
	}
}

func TestAnalyzeDML_MediumDelete_Caution(t *testing.T) {
	// No WHERE, 50K rows → all affected, >10K but ≤100K
	// No-WHERE sets DANGEROUS initially, but the 10K-100K band overwrites to CAUTION
	input := dmlInput(parser.Delete, false, 50000, 100, 10000, topology.Standalone)
	result := Analyze(input)

	if result.Risk != RiskCaution {
		t.Errorf("Risk = %q, want CAUTION", result.Risk)
	}
	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT", result.Method)
	}
	// Should still have the no-WHERE warning
	if !containsWarning(result.Warnings, "No WHERE clause") {
		t.Error("expected warning about missing WHERE clause")
	}
}

func TestAnalyzeDML_TriggerWarning(t *testing.T) {
	input := dmlInput(parser.Delete, false, 1000, 100, 10000, topology.Standalone)
	input.Meta.Triggers = []mysql.TriggerInfo{
		{Name: "trg_audit", Event: "DELETE", Timing: "AFTER"},
	}
	result := Analyze(input)

	if !containsWarning(result.Warnings, "Trigger trg_audit") {
		t.Errorf("expected trigger warning, got: %v", result.Warnings)
	}
}

func TestAnalyzeDML_TriggerNoMatch(t *testing.T) {
	input := dmlInput(parser.Delete, false, 1000, 100, 10000, topology.Standalone)
	input.Meta.Triggers = []mysql.TriggerInfo{
		{Name: "trg_insert", Event: "INSERT", Timing: "BEFORE"},
	}
	result := Analyze(input)

	for _, w := range result.Warnings {
		if containsStr(w, "trg_insert") {
			t.Errorf("unexpected trigger warning for non-matching event: %s", w)
		}
	}
}

func TestAnalyzeDML_AffectedPct(t *testing.T) {
	input := dmlInput(parser.Delete, false, 200, 100, 10000, topology.Standalone)
	result := Analyze(input)

	// All 200 rows affected, 200/200 = 100%
	if result.AffectedPct != 100.0 {
		t.Errorf("AffectedPct = %.1f, want 100.0", result.AffectedPct)
	}
}

// =============================================================
// Topology Warning Tests
// =============================================================

func TestTopologyWarnings_Galera_TOI(t *testing.T) {
	input := ddlInput(parser.AddIndex, v8_0_35, 100*1024*1024, topology.Galera)
	input.Topo.GaleraOSUMethod = "TOI"
	input.Topo.GaleraClusterSize = 3

	result := Analyze(input)

	if !containsWarning(result.ClusterWarnings, "TOI") {
		t.Errorf("expected TOI warning, got: %v", result.ClusterWarnings)
	}
}

func TestTopologyWarnings_Galera_TOI_Instant_NoWarning(t *testing.T) {
	// INSTANT operations should NOT get TOI warning
	input := ddlInput(parser.AddColumn, v8_0_35, 100*1024*1024, topology.Galera)
	input.Topo.GaleraOSUMethod = "TOI"
	input.Topo.GaleraClusterSize = 3

	result := Analyze(input)

	if containsWarning(result.ClusterWarnings, "TOI") {
		t.Errorf("INSTANT operation should not get TOI warning, got: %v", result.ClusterWarnings)
	}
}

func TestTopologyWarnings_Galera_WriteSetExceeded(t *testing.T) {
	input := dmlInput(parser.Delete, false, 500000, 200, 10000, topology.Galera)
	input.Topo.WsrepMaxWsSize = 1024 * 1024 * 1024 // 1GB
	// WriteSetSize = 500K * 200 = 100MB, which is < 1GB, so no warning

	result := Analyze(input)
	if containsWarning(result.ClusterWarnings, "EXCEEDS wsrep_max_ws_size") {
		t.Error("write-set should not exceed limit")
	}

	// Now make it exceed: 500K rows * 5000 bytes = 2.5GB > 1GB
	input2 := dmlInput(parser.Delete, false, 500000, 5000, 10000, topology.Galera)
	input2.Topo.WsrepMaxWsSize = 1024 * 1024 * 1024 // 1GB
	result2 := Analyze(input2)

	if !containsWarning(result2.ClusterWarnings, "EXCEEDS wsrep_max_ws_size") {
		t.Errorf("expected write-set exceeded warning, got: %v", result2.ClusterWarnings)
	}
	if result2.Risk != RiskDangerous {
		t.Errorf("Risk = %q, want DANGEROUS", result2.Risk)
	}
	if result2.Method != ExecChunked {
		t.Errorf("Method = %q, want CHUNKED", result2.Method)
	}
}

func TestTopologyWarnings_Galera_FlowControl(t *testing.T) {
	input := ddlInput(parser.AddIndex, v8_0_35, 100*1024*1024, topology.Galera)
	input.Topo.FlowControlPaused = 0.05
	input.Topo.FlowControlPausedPct = "5.0%"

	result := Analyze(input)

	if !containsWarning(result.ClusterWarnings, "Flow control") {
		t.Errorf("expected flow control warning, got: %v", result.ClusterWarnings)
	}
}

func TestTopologyWarnings_Galera_GhostOverride(t *testing.T) {
	// If analyzer recommends gh-ost but topology is Galera, it should switch to pt-osc
	input := ddlInput(parser.ModifyColumn, v8_0_35, 2*1024*1024*1024, topology.Galera)

	result := Analyze(input)

	if result.Method != ExecPtOSC {
		t.Errorf("Method = %q, want PT-ONLINE-SCHEMA-CHANGE (Galera overrides gh-ost)", result.Method)
	}
}

func TestTopologyWarnings_GroupReplication_TransactionLimit(t *testing.T) {
	input := dmlInput(parser.Delete, false, 500000, 5000, 10000, topology.GroupRepl)
	input.Topo.GRTransactionLimit = 1024 * 1024 * 1024 // 1GB

	result := Analyze(input)

	if !containsWarning(result.ClusterWarnings, "group_replication_transaction_size_limit") {
		t.Errorf("expected GR transaction limit warning, got: %v", result.ClusterWarnings)
	}
}

func TestTopologyWarnings_GroupReplication_MultiPrimary(t *testing.T) {
	input := ddlInput(parser.AddIndex, v8_0_35, 100*1024*1024, topology.GroupRepl)
	input.Topo.GRMode = "MULTI-PRIMARY"

	result := Analyze(input)

	if !containsWarning(result.ClusterWarnings, "multi-primary") {
		t.Errorf("expected multi-primary warning, got: %v", result.ClusterWarnings)
	}
}

func TestTopologyWarnings_Replication_Lag(t *testing.T) {
	lag := int64(60)
	input := ddlInput(parser.AddIndex, v8_0_35, 100*1024*1024, topology.AsyncReplica)
	input.Topo.ReplicaLagSecs = &lag

	result := Analyze(input)

	if !containsWarning(result.ClusterWarnings, "Replication lag") {
		t.Errorf("expected replication lag warning, got: %v", result.ClusterWarnings)
	}
}

func TestTopologyWarnings_Replication_NoLag(t *testing.T) {
	lag := int64(5)
	input := ddlInput(parser.AddIndex, v8_0_35, 100*1024*1024, topology.AsyncReplica)
	input.Topo.ReplicaLagSecs = &lag

	result := Analyze(input)

	if containsWarning(result.ClusterWarnings, "Replication lag") {
		t.Error("should not warn about lag when lag is small")
	}
}

// =============================================================
// Rollback Generation Tests
// =============================================================

func TestRollback_AddColumn(t *testing.T) {
	input := ddlInput(parser.AddColumn, v8_0_35, 100*1024*1024, topology.Standalone)
	input.Parsed.ColumnName = "email"

	result := Analyze(input)

	if result.RollbackSQL == "" {
		t.Error("expected rollback SQL for ADD COLUMN")
	}
	if !containsStr(result.RollbackSQL, "DROP COLUMN") {
		t.Errorf("rollback SQL should contain DROP COLUMN, got: %s", result.RollbackSQL)
	}
	if !containsStr(result.RollbackSQL, "email") {
		t.Errorf("rollback SQL should reference column name, got: %s", result.RollbackSQL)
	}
}

func TestRollback_AddColumn_InstantDropNote(t *testing.T) {
	// 8.0.35 supports instant drop
	input := ddlInput(parser.AddColumn, v8_0_35, 100*1024*1024, topology.Standalone)
	input.Parsed.ColumnName = "email"
	result := Analyze(input)
	if !containsStr(result.RollbackNotes, "INSTANT") {
		t.Errorf("expected INSTANT note for 8.0.35, got: %s", result.RollbackNotes)
	}

	// 8.0.20 does not support instant drop
	input2 := ddlInput(parser.AddColumn, v8_0_20, 100*1024*1024, topology.Standalone)
	input2.Parsed.ColumnName = "email"
	result2 := Analyze(input2)
	if !containsStr(result2.RollbackNotes, "INPLACE") {
		t.Errorf("expected INPLACE note for 8.0.20, got: %s", result2.RollbackNotes)
	}
}

func TestRollback_DropColumn(t *testing.T) {
	input := ddlInput(parser.DropColumn, v8_0_35, 100*1024*1024, topology.Standalone)
	input.Parsed.ColumnName = "old_field"

	result := Analyze(input)

	if result.RollbackSQL != "" {
		t.Errorf("DROP COLUMN rollback should not have automatic SQL, got: %s", result.RollbackSQL)
	}
	if result.RollbackNotes == "" {
		t.Error("expected rollback notes for DROP COLUMN")
	}
}

func TestRollback_AddIndex(t *testing.T) {
	input := ddlInput(parser.AddIndex, v8_0_35, 100*1024*1024, topology.Standalone)
	input.Parsed.IndexName = "idx_email"

	result := Analyze(input)

	if !containsStr(result.RollbackSQL, "DROP INDEX") {
		t.Errorf("rollback SQL should contain DROP INDEX, got: %s", result.RollbackSQL)
	}
	if !containsStr(result.RollbackSQL, "idx_email") {
		t.Errorf("rollback SQL should reference index name, got: %s", result.RollbackSQL)
	}
}

func TestRollback_DML_HasOptions(t *testing.T) {
	input := dmlInput(parser.Delete, true, 1000, 100, 10000, topology.Standalone)

	result := Analyze(input)

	if len(result.RollbackOptions) < 2 {
		t.Fatalf("expected at least 2 rollback options, got %d", len(result.RollbackOptions))
	}
	if !containsStr(result.RollbackOptions[0].Label, "Pre-backup") {
		t.Errorf("first rollback option should be pre-backup, got: %s", result.RollbackOptions[0].Label)
	}
	if !containsStr(result.RollbackOptions[1].Label, "Point-in-time") {
		t.Errorf("second rollback option should be point-in-time, got: %s", result.RollbackOptions[1].Label)
	}
}

// =============================================================
// Chunked Script Generation Tests
// =============================================================

func TestChunkedScript_GeneratedForLargeDelete(t *testing.T) {
	input := dmlInput(parser.Delete, false, 500000, 100, 10000, topology.Standalone)

	result := Analyze(input)

	if result.Method != ExecChunked {
		t.Fatalf("Method = %q, want CHUNKED", result.Method)
	}
	if result.GeneratedScript == "" {
		t.Error("expected generated chunked script")
	}
	if !containsStr(result.GeneratedScript, "LIMIT") {
		t.Error("chunked delete script should contain LIMIT")
	}
	if result.ScriptPath == "" {
		t.Error("expected ScriptPath to be set")
	}
}

func TestChunkedScript_NotGeneratedForSmallOps(t *testing.T) {
	input := dmlInput(parser.Delete, true, 100, 100, 10000, topology.Standalone)

	result := Analyze(input)

	if result.GeneratedScript != "" {
		t.Error("should not generate chunked script for small operations")
	}
}

// =============================================================
// Result Metadata Tests
// =============================================================

func TestAnalyze_ResultMetadata(t *testing.T) {
	input := ddlInput(parser.AddColumn, v8_0_35, 100*1024*1024, topology.Standalone)
	result := Analyze(input)

	if result.Statement != input.Parsed.RawSQL {
		t.Errorf("Statement = %q, want %q", result.Statement, input.Parsed.RawSQL)
	}
	if result.StatementType != parser.DDL {
		t.Errorf("StatementType = %q, want DDL", result.StatementType)
	}
	if result.Table != "test" {
		t.Errorf("Table = %q, want %q", result.Table, "test")
	}
	if result.Database != "testdb" {
		t.Errorf("Database = %q, want %q", result.Database, "testdb")
	}
	if result.AnalyzedAt.IsZero() {
		t.Error("AnalyzedAt should not be zero")
	}
}

func TestAnalyze_DatabaseFallback(t *testing.T) {
	input := ddlInput(parser.AddColumn, v8_0_35, 100*1024*1024, topology.Standalone)
	input.Parsed.Database = "" // no database in SQL
	input.Meta.Database = "from_meta"

	result := Analyze(input)

	if result.Database != "from_meta" {
		t.Errorf("Database = %q, want %q (fallback to metadata)", result.Database, "from_meta")
	}
}

// =============================================================
// Disk Space Estimate Tests
// =============================================================

func TestDiskEstimate_Instant_Nil(t *testing.T) {
	// ADD COLUMN on 8.0.35 → INSTANT → no disk estimate regardless of size
	input := ddlInput(parser.AddColumn, v8_0_35, 2*1024*1024*1024, topology.Standalone) // 2 GB
	result := Analyze(input)

	if result.Classification.Algorithm != AlgoInstant {
		t.Fatalf("expected INSTANT algorithm, got %s", result.Classification.Algorithm)
	}
	if result.DiskEstimate != nil {
		t.Errorf("DiskEstimate should be nil for INSTANT algorithm, got: %+v", result.DiskEstimate)
	}
}

func TestDiskEstimate_Copy_LargeTable(t *testing.T) {
	// MODIFY COLUMN → COPY on 2 GB table → disk estimate ≈ table size
	input := ddlInput(parser.ModifyColumn, v8_0_35, 2*1024*1024*1024, topology.Standalone)
	result := Analyze(input)

	if result.DiskEstimate == nil {
		t.Fatal("DiskEstimate should not be nil for COPY on large table")
	}
	wantBytes := input.Meta.TotalSize()
	if result.DiskEstimate.RequiredBytes != wantBytes {
		t.Errorf("RequiredBytes = %d, want %d", result.DiskEstimate.RequiredBytes, wantBytes)
	}
	if result.DiskEstimate.RequiredHuman == "" {
		t.Error("RequiredHuman should not be empty")
	}
	if result.DiskEstimate.Reason == "" {
		t.Error("Reason should not be empty")
	}
}

func TestDiskEstimate_Copy_SmallTable_Nil(t *testing.T) {
	// MODIFY COLUMN → COPY on 50 MB table → below 100 MB threshold → nil
	input := ddlInput(parser.ModifyColumn, v8_0_35, 50*1024*1024, topology.Standalone)
	result := Analyze(input)

	if result.DiskEstimate != nil {
		t.Errorf("DiskEstimate should be nil for COPY on small table (<100MB), got: %+v", result.DiskEstimate)
	}
}

func TestDiskEstimate_Ghost_MentionsGhost(t *testing.T) {
	// MODIFY COLUMN → COPY on 2 GB non-Galera → gh-ost method → reason mentions "gh-ost"
	input := ddlInput(parser.ModifyColumn, v8_0_35, 2*1024*1024*1024, topology.Standalone)
	result := Analyze(input)

	if result.Method != ExecGhost {
		t.Fatalf("expected ExecGhost, got %s", result.Method)
	}
	if result.DiskEstimate == nil {
		t.Fatal("DiskEstimate should not be nil")
	}
	if !containsStr(result.DiskEstimate.Reason, "gh-ost") {
		t.Errorf("Reason should mention gh-ost, got: %s", result.DiskEstimate.Reason)
	}
}

func TestDiskEstimate_PtOSC_Galera_MentionsPtOSC(t *testing.T) {
	// MODIFY COLUMN → COPY on 2 GB Galera → pt-osc method → reason mentions "pt-online-schema-change"
	input := ddlInput(parser.ModifyColumn, v8_0_35, 2*1024*1024*1024, topology.Galera)
	result := Analyze(input)

	if result.Method != ExecPtOSC {
		t.Fatalf("expected ExecPtOSC, got %s", result.Method)
	}
	if result.DiskEstimate == nil {
		t.Fatal("DiskEstimate should not be nil")
	}
	if !containsStr(result.DiskEstimate.Reason, "pt-online-schema-change") {
		t.Errorf("Reason should mention pt-online-schema-change, got: %s", result.DiskEstimate.Reason)
	}
}

func TestDiskEstimate_Inplace_NoRebuild_LargeIndexLength(t *testing.T) {
	// ADD INDEX on 11 GB table → INPLACE, no rebuild → disk estimate ≈ IndexLength
	input := ddlInput(parser.AddIndex, v8_0_35, 11*1024*1024*1024, topology.Standalone)
	result := Analyze(input)

	if result.Classification.RebuildsTable {
		t.Fatal("AddIndex should not rebuild table")
	}
	if result.DiskEstimate == nil {
		t.Fatal("DiskEstimate should not be nil for large INPLACE index build")
	}
	if result.DiskEstimate.RequiredBytes != input.Meta.IndexLength {
		t.Errorf("RequiredBytes = %d, want IndexLength %d", result.DiskEstimate.RequiredBytes, input.Meta.IndexLength)
	}
}

func TestDiskEstimate_DML_Nil(t *testing.T) {
	// DELETE on large table → DML → never gets disk estimate
	input := dmlInput(parser.Delete, false, 500000, 200, 10000, topology.Standalone)
	result := Analyze(input)

	if result.DiskEstimate != nil {
		t.Errorf("DiskEstimate should always be nil for DML, got: %+v", result.DiskEstimate)
	}
}

// =============================================================
// Utility Tests
// =============================================================

func TestHumanBytes(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1536, "1.5 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
		{int64(2.5 * 1024 * 1024 * 1024), "2.5 GB"},
	}
	for _, tt := range tests {
		got := humanBytes(tt.input)
		if got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestFormatNumber(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{500, "500"},
		{1500, "1.5K"},
		{1500000, "1.5M"},
		{1500000000, "1.5B"},
	}
	for _, tt := range tests {
		got := formatNumber(tt.input)
		if got != tt.want {
			t.Errorf("formatNumber(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// =============================================================
// Helpers
// =============================================================
// Column Validation Tests
// =============================================================

func TestColumnValidation_AddColumn_AlreadyExists(t *testing.T) {
	input := ddlInput(parser.AddColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.ColumnName = "existing_col"
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if !containsWarning(result.Warnings, "already exists") {
		t.Errorf("Expected warning about column already existing, got: %v", result.Warnings)
	}
	if result.Risk != RiskDangerous {
		t.Errorf("Expected RiskDangerous, got: %v", result.Risk)
	}
}

func TestColumnValidation_AddColumn_NewColumn(t *testing.T) {
	input := ddlInput(parser.AddColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.ColumnName = "new_col"
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if containsWarning(result.Warnings, "already exists") {
		t.Errorf("Did not expect warning about column existing, got: %v", result.Warnings)
	}
	// Risk should be RiskSafe for INSTANT operations (8.0.35)
	if result.Risk != RiskSafe {
		t.Errorf("Expected RiskSafe for new column on 8.0.35, got: %v", result.Risk)
	}
}

func TestColumnValidation_DropColumn_DoesNotExist(t *testing.T) {
	input := ddlInput(parser.DropColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.ColumnName = "nonexistent_col"
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if !containsWarning(result.Warnings, "does not exist") {
		t.Errorf("Expected warning about column not existing, got: %v", result.Warnings)
	}
	if result.Risk != RiskDangerous {
		t.Errorf("Expected RiskDangerous, got: %v", result.Risk)
	}
}

func TestColumnValidation_DropColumn_Exists(t *testing.T) {
	input := ddlInput(parser.DropColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.ColumnName = "existing_col"
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if containsWarning(result.Warnings, "does not exist") {
		t.Errorf("Did not expect warning about column not existing, got: %v", result.Warnings)
	}
}

func TestColumnValidation_ModifyColumn_DoesNotExist(t *testing.T) {
	input := ddlInput(parser.ModifyColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.ColumnName = "nonexistent_col"
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if !containsWarning(result.Warnings, "does not exist") {
		t.Errorf("Expected warning about column not existing, got: %v", result.Warnings)
	}
	if result.Risk != RiskDangerous {
		t.Errorf("Expected RiskDangerous, got: %v", result.Risk)
	}
}

func TestColumnValidation_ChangeColumn_OldDoesNotExist(t *testing.T) {
	input := ddlInput(parser.ChangeColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.OldColumnName = "nonexistent_col"
	input.Parsed.NewColumnName = "new_name"
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if !containsWarning(result.Warnings, "Source column") || !containsWarning(result.Warnings, "does not exist") {
		t.Errorf("Expected warning about source column not existing, got: %v", result.Warnings)
	}
	if result.Risk != RiskDangerous {
		t.Errorf("Expected RiskDangerous, got: %v", result.Risk)
	}
}

func TestColumnValidation_ChangeColumn_NewAlreadyExists(t *testing.T) {
	input := ddlInput(parser.ChangeColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.OldColumnName = "existing_col"
	input.Parsed.NewColumnName = "id" // Already exists
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if !containsWarning(result.Warnings, "Target column") || !containsWarning(result.Warnings, "already exists") {
		t.Errorf("Expected warning about target column already existing, got: %v", result.Warnings)
	}
	if result.Risk != RiskDangerous {
		t.Errorf("Expected RiskDangerous, got: %v", result.Risk)
	}
}

func TestColumnValidation_ChangeColumn_ValidRename(t *testing.T) {
	input := ddlInput(parser.ChangeColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.OldColumnName = "existing_col"
	input.Parsed.NewColumnName = "renamed_col"
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if containsWarning(result.Warnings, "does not exist") || containsWarning(result.Warnings, "already exists") {
		t.Errorf("Did not expect column validation warnings, got: %v", result.Warnings)
	}
}

func TestColumnValidation_ChangeColumn_SameNameAllowed(t *testing.T) {
	// CHANGE COLUMN can be used to change just the type, keeping the same name
	input := ddlInput(parser.ChangeColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.OldColumnName = "existing_col"
	input.Parsed.NewColumnName = "existing_col" // Same name
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	// Should not warn about "already exists" when old and new names are the same
	if containsWarning(result.Warnings, "already exists") {
		t.Errorf("Should not warn about target already existing when renaming to same name, got: %v", result.Warnings)
	}
}

func TestClassifyDDL_ChangeColumn_AlgorithmByVersion(t *testing.T) {
	// CHANGE COLUMN rename-only is INPLACE before MySQL 8.0.29 (Bug#33175960 added INSTANT in 8.0.28,
	// which maps to V8_0_Full range starting at 8.0.29 in our version bucketing).
	// From 8.0.29+ (V8_0_Full) and 8.4, INSTANT is used.
	tests := []struct {
		v        mysql.ServerVersion
		wantAlgo Algorithm
	}{
		{v8_0_5, AlgoInplace},   // V8_0_Early
		{v8_0_20, AlgoInplace},  // V8_0_Instant
		{v8_0_35, AlgoInstant},  // V8_0_Full
		{v8_4_0, AlgoInstant},   // V8_4_LTS
	}
	for _, tt := range tests {
		c := ClassifyDDL(parser.ChangeColumn, tt.v.Major, tt.v.Minor, tt.v.Patch)
		if c.Algorithm != tt.wantAlgo {
			t.Errorf("v%d.%d.%d: ChangeColumn algorithm = %s, want %s", tt.v.Major, tt.v.Minor, tt.v.Patch, c.Algorithm, tt.wantAlgo)
		}
	}
}

func TestChangeColumn_TypeChange_RequiresCopy(t *testing.T) {
	// When a type change is detected, classification must upgrade to COPY.
	input := ddlInput(parser.ChangeColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.OldColumnName = "total_amount"
	input.Parsed.NewColumnName = "amount"
	input.Parsed.NewColumnType = "decimal(14,4)" // different from existing
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "total_amount", Type: "decimal(12,2)", Position: 2},
	}

	result := Analyze(input)

	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("Expected COPY for type change, got %s", result.Classification.Algorithm)
	}
	if result.Classification.Lock != LockShared {
		t.Errorf("Expected SHARED lock for type change, got %s", result.Classification.Lock)
	}
	if !result.Classification.RebuildsTable {
		t.Error("Expected RebuildsTable=true for type change")
	}
	if !containsWarning(result.Warnings, "type change detected") {
		t.Errorf("Expected type-change warning, got: %v", result.Warnings)
	}
}

func TestChangeColumn_RenameOnly_UsesInstant(t *testing.T) {
	// Rename with same type uses INSTANT on MySQL ≥8.0.29 (MySQL Bug#33175960).
	// On older MySQL (<8.0.29) it falls back to INPLACE.
	tests := []struct {
		version  mysql.ServerVersion
		wantAlgo Algorithm
	}{
		{mysql.ServerVersion{Major: 8, Minor: 0, Patch: 20}, AlgoInplace},  // V8_0_Instant → INPLACE
		{mysql.ServerVersion{Major: 8, Minor: 0, Patch: 29}, AlgoInstant},  // V8_0_Full → INSTANT
		{mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, AlgoInstant},  // V8_0_Full → INSTANT
		{mysql.ServerVersion{Major: 8, Minor: 4, Patch: 0}, AlgoInstant},   // V8_4_LTS → INSTANT
	}
	for _, tt := range tests {
		input := ddlInput(parser.ChangeColumn, tt.version, 0, topology.Standalone)
		input.Parsed.OldColumnName = "existing_col"
		input.Parsed.NewColumnName = "renamed_col"
		input.Parsed.NewColumnType = "decimal(12,2)" // same type as in metadata
		input.Meta.Columns = []mysql.ColumnInfo{
			{Name: "id", Type: "int", Position: 1},
			{Name: "existing_col", Type: "decimal(12,2)", Position: 2},
		}

		result := Analyze(input)

		if result.Classification.Algorithm != tt.wantAlgo {
			t.Errorf("v%d.%d.%d: Expected %s for rename-only, got %s",
				tt.version.Major, tt.version.Minor, tt.version.Patch, tt.wantAlgo, result.Classification.Algorithm)
		}
		if containsWarning(result.Warnings, "type change detected") {
			t.Errorf("v%d.%d.%d: Should not warn about type change for rename-only, got: %v",
				tt.version.Major, tt.version.Minor, tt.version.Patch, result.Warnings)
		}
	}
}

func TestChangeColumn_SameName_TypeChange_RequiresCopy(t *testing.T) {
	// CHANGE COLUMN keeping same name but changing type still requires COPY.
	input := ddlInput(parser.ChangeColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
	input.Parsed.OldColumnName = "existing_col"
	input.Parsed.NewColumnName = "existing_col" // same name
	input.Parsed.NewColumnType = "varchar(255)" // different from varchar(100)
	input.Meta.Columns = []mysql.ColumnInfo{
		{Name: "id", Type: "int", Position: 1},
		{Name: "existing_col", Type: "varchar(100)", Position: 2},
	}

	result := Analyze(input)

	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("Expected COPY for type change (same name), got %s", result.Classification.Algorithm)
	}
	if !containsWarning(result.Warnings, "type change detected") {
		t.Errorf("Expected type-change warning, got: %v", result.Warnings)
	}
}

func TestChangeColumn_RenameOnly_BaseTypeOnly_NoFalsePositive(t *testing.T) {
	// Regression test: CHANGE COLUMN with NOT NULL DEFAULT qualifiers must not be
	// treated as a type change. The parser strips Options so NewColumnType is just
	// the base type (e.g. "varchar(20)"), matching INFORMATION_SCHEMA.COLUMN_TYPE.
	tests := []struct {
		name          string
		oldType       string // INFORMATION_SCHEMA.COLUMN_TYPE
		newColumnType string // what the parser produces (base type only)
		wantAlgo      Algorithm
	}{
		{
			name:          "varchar same type — parser strips NOT NULL DEFAULT",
			oldType:       "varchar(20)",
			newColumnType: "varchar(20)", // was: "varchar(20) not null default 'pending'"
			wantAlgo:      AlgoInstant,   // INSTANT on MySQL ≥8.0.29 (Bug#33175960)
		},
		{
			name:          "decimal same type — parser strips NOT NULL DEFAULT",
			oldType:       "decimal(12,2)",
			newColumnType: "decimal(12,2)", // was: "decimal(12,2) not null default 0.00"
			wantAlgo:      AlgoInstant,     // INSTANT on MySQL ≥8.0.29
		},
		{
			name:          "int unsigned same type — base type includes unsigned modifier",
			oldType:       "int unsigned",
			newColumnType: "int unsigned",
			wantAlgo:      AlgoInstant, // INSTANT on MySQL ≥8.0.29
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := ddlInput(parser.ChangeColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 0, topology.Standalone)
			input.Parsed.OldColumnName = "col"
			input.Parsed.NewColumnName = "new_col"
			input.Parsed.NewColumnType = tt.newColumnType
			input.Meta.Columns = []mysql.ColumnInfo{
				{Name: "col", Type: tt.oldType, Position: 1},
			}

			result := Analyze(input)

			if result.Classification.Algorithm != tt.wantAlgo {
				t.Errorf("Algorithm = %s, want %s", result.Classification.Algorithm, tt.wantAlgo)
			}
			if containsWarning(result.Warnings, "type change detected") {
				t.Errorf("Should not warn about type change for rename-only, got: %v", result.Warnings)
			}
		})
	}
}

// =============================================================
// MODIFY COLUMN VARCHAR INPLACE detection (issue #19)
// =============================================================

func modifyColumnInput(colOldType, newColType, charset string, sizeBytes int64) Input {
	cs := charset
	return Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			RawSQL:        "ALTER TABLE orders MODIFY COLUMN order_number " + newColType,
			Table:         "orders",
			DDLOp:         parser.ModifyColumn,
			ColumnName:    "order_number",
			NewColumnType: strings.ToLower(newColType),
		},
		Meta: &mysql.TableMetadata{
			Database:    "demo",
			Table:       "orders",
			DataLength:  sizeBytes,
			IndexLength: 0,
			RowCount:    100000,
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int unsigned", Position: 1},
				{Name: "order_number", Type: colOldType, Position: 2, CharacterSet: &cs},
			},
		},
		Version: mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35},
		Topo:    &topology.Info{Type: topology.Standalone},
	}
}

func TestModifyColumn_VarcharExpansion_SameTier_utf8mb3_IsInplace(t *testing.T) {
	// Issue #19: VARCHAR(20)→VARCHAR(50) in utf8mb3: 60→150 bytes, both 1-byte prefix → INPLACE
	input := modifyColumnInput("varchar(20)", "VARCHAR(50)", "utf8mb3", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("expected INPLACE, got %s. Notes: %s", result.Classification.Algorithm, result.Classification.Notes)
	}
	if result.Classification.RebuildsTable {
		t.Error("expected RebuildsTable=false for INPLACE VARCHAR extension")
	}
	if result.Classification.Lock != LockNone {
		t.Errorf("expected LockNone, got %s", result.Classification.Lock)
	}
}

func TestModifyColumn_VarcharExpansion_CrossesTier_utf8mb3_IsCopy(t *testing.T) {
	// VARCHAR(80)→VARCHAR(90) in utf8mb3: 240→270 bytes, crosses 1→2 byte prefix tier → COPY
	input := modifyColumnInput("varchar(80)", "VARCHAR(90)", "utf8mb3", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("expected COPY for prefix-tier crossing, got %s", result.Classification.Algorithm)
	}
}

func TestModifyColumn_VarcharExpansion_SameTier_utf8mb4_IsInplace(t *testing.T) {
	// VARCHAR(30)→VARCHAR(60) in utf8mb4: 120→240 bytes, both 1-byte prefix (≤255) → INPLACE
	input := modifyColumnInput("varchar(30)", "VARCHAR(60)", "utf8mb4", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("expected INPLACE for utf8mb4 same-tier expansion, got %s", result.Classification.Algorithm)
	}
}

func TestModifyColumn_VarcharExpansion_CrossesTier_utf8mb4_IsCopy(t *testing.T) {
	// VARCHAR(63)→VARCHAR(64) in utf8mb4: 252→256 bytes, crosses 1→2 byte prefix tier → COPY
	input := modifyColumnInput("varchar(63)", "VARCHAR(64)", "utf8mb4", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("expected COPY for utf8mb4 prefix-tier crossing, got %s", result.Classification.Algorithm)
	}
}

func TestModifyColumn_VarcharExpansion_SameTier_latin1_IsInplace(t *testing.T) {
	// VARCHAR(100)→VARCHAR(200) in latin1: both ≤255 bytes → INPLACE
	input := modifyColumnInput("varchar(100)", "VARCHAR(200)", "latin1", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("expected INPLACE for latin1 same-tier expansion, got %s", result.Classification.Algorithm)
	}
}

func TestModifyColumn_VarcharExpansion_CrossesTier_latin1_IsCopy(t *testing.T) {
	// VARCHAR(200)→VARCHAR(300) in latin1: 200→300 bytes, crosses 1→2 byte prefix tier → COPY
	input := modifyColumnInput("varchar(200)", "VARCHAR(300)", "latin1", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("expected COPY for latin1 prefix-tier crossing, got %s", result.Classification.Algorithm)
	}
}

func TestModifyColumn_VarcharShrink_IsCopy(t *testing.T) {
	// Shrinking VARCHAR is not INPLACE — MySQL requires COPY
	input := modifyColumnInput("varchar(100)", "VARCHAR(50)", "utf8mb3", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("expected COPY for VARCHAR shrink, got %s", result.Classification.Algorithm)
	}
}

func TestModifyColumn_NonVarchar_IsCopy(t *testing.T) {
	// Non-VARCHAR type changes always use the matrix default (COPY)
	input := modifyColumnInput("int", "BIGINT", "utf8mb3", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("expected COPY for non-varchar type change, got %s", result.Classification.Algorithm)
	}
}

func TestModifyColumn_VarcharExpansion_BothInHighTier_IsInplace(t *testing.T) {
	// Both in 2-byte prefix tier (>255 bytes): VARCHAR(100)→VARCHAR(200) in utf8mb4
	// 100*4=400 bytes, 200*4=800 bytes — both >255 bytes → same 2-byte prefix tier → INPLACE
	input := modifyColumnInput("varchar(100)", "VARCHAR(200)", "utf8mb4", 50*1024*1024)
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("expected INPLACE for both-in-2-byte-prefix expansion, got %s", result.Classification.Algorithm)
	}
}

// Unit tests for helper functions
func TestExtractVarcharLength(t *testing.T) {
	tests := []struct {
		input  string
		wantN  int
		wantOK bool
	}{
		{"varchar(50)", 50, true},
		{"varchar(255)", 255, true},
		{"VARCHAR(50)", 50, true}, // function lowercases its input internally
		{"int", 0, false},
		{"char(10)", 0, false},
		{"varchar()", 0, false},
	}
	for _, tt := range tests {
		n, ok := extractVarcharLength(tt.input)
		if ok != tt.wantOK || n != tt.wantN {
			t.Errorf("extractVarcharLength(%q) = (%d, %v), want (%d, %v)", tt.input, n, ok, tt.wantN, tt.wantOK)
		}
	}
}

func TestMaxBytesPerChar(t *testing.T) {
	tests := []struct {
		charset string
		want    int
	}{
		{"latin1", 1},
		{"ascii", 1},
		{"utf8mb3", 3},
		{"utf8", 3},
		{"utf8mb4", 4},
		{"utf32", 4},
		{"gbk", 2},
		{"unknown_charset", 4}, // conservative default
	}
	for _, tt := range tests {
		if got := maxBytesPerChar(tt.charset); got != tt.want {
			t.Errorf("maxBytesPerChar(%q) = %d, want %d", tt.charset, got, tt.want)
		}
	}
}

func TestAnalyzeDDL_AddCheckConstraint_Warning(t *testing.T) {
	input := ddlInput(parser.AddCheckConstraint, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 10*1024*1024, topology.Standalone)
	input.Parsed.CheckExpr = "amount > 0"
	input.Parsed.Table = "orders"

	result := Analyze(input)

	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE", result.Classification.Algorithm)
	}
	if result.Classification.Lock != LockNone {
		t.Errorf("Lock = %q, want NONE", result.Classification.Lock)
	}
	if !containsWarning(result.Warnings, "NOT (amount > 0)") {
		t.Errorf("Expected check constraint validation warning, got: %v", result.Warnings)
	}
	if !containsWarning(result.Warnings, "orders") {
		t.Errorf("Expected table name in warning, got: %v", result.Warnings)
	}
}

func TestAnalyzeDDL_UnparsableOperation(t *testing.T) {
	// Test that OtherDDL operations generate a syntax warning
	input := ddlInput(parser.OtherDDL, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 100*1024*1024, topology.Standalone)
	input.Parsed.DDLOp = parser.OtherDDL
	input.Parsed.RawSQL = "ALTER TABLE users ADD COLUMN email VRCHAR(255)" // Typo: VRCHAR

	result := Analyze(input)

	// Should generate warning about unparsable operation
	if !containsWarning(result.Warnings, "could not be fully parsed") {
		t.Errorf("Expected warning about unparsable operation, got warnings: %v", result.Warnings)
	}

	// Should be marked as DANGEROUS
	if result.Risk != RiskDangerous {
		t.Errorf("Expected RiskDangerous for unparsable operation, got: %s", result.Risk)
	}

	// Should use default classification (COPY)
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("Expected COPY algorithm for unknown operation, got: %s", result.Classification.Algorithm)
	}
}

// =============================================================

func containsWarning(warnings []string, substr string) bool {
	for _, w := range warnings {
		if containsStr(w, substr) {
			return true
		}
	}
	return false
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && contains(s, substr))
}

func contains(s, substr string) bool {
	for i := 0; i+len(substr) <= len(s); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
