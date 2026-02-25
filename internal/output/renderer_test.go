package output

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/nethalo/dbsafe/internal/analyzer"
	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// =============================================================
// Test Fixtures
// =============================================================

func ddlResult() *analyzer.Result {
	return &analyzer.Result{
		Statement:     "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
		StatementType: parser.DDL,
		Database:      "testdb",
		Table:         "users",
		TableMeta: &mysql.TableMetadata{
			Database:     "testdb",
			Table:        "users",
			Engine:       "InnoDB",
			RowCount:     50000,
			DataLength:   10 * 1024 * 1024,
			IndexLength:  2 * 1024 * 1024,
			AvgRowLength: 200,
			Indexes: []mysql.IndexInfo{
				{Name: "PRIMARY", Columns: []string{"id"}, Type: "BTREE"},
				{Name: "idx_email", Columns: []string{"email"}, Type: "BTREE"},
			},
		},
		Topology:   &topology.Info{Type: topology.Standalone},
		Version:    mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35, Flavor: "mysql"},
		AnalyzedAt: time.Now(),

		DDLOp: parser.AddColumn,
		Classification: analyzer.DDLClassification{
			Algorithm:     analyzer.AlgoInstant,
			Lock:          analyzer.LockNone,
			RebuildsTable: false,
			Notes:         "INSTANT for any column position.",
		},

		Risk:           analyzer.RiskSafe,
		Method:         analyzer.ExecDirect,
		Recommendation: "This operation uses INSTANT algorithm. Safe to execute directly.",

		RollbackSQL:   "ALTER TABLE `testdb`.`users` DROP COLUMN `email`;",
		RollbackNotes: "DROP COLUMN is INSTANT in your MySQL version.",
	}
}

func dmlResult() *analyzer.Result {
	return &analyzer.Result{
		Statement:     "DELETE FROM logs WHERE created_at < '2023-01-01'",
		StatementType: parser.DML,
		Database:      "testdb",
		Table:         "logs",
		TableMeta: &mysql.TableMetadata{
			Database:     "testdb",
			Table:        "logs",
			Engine:       "InnoDB",
			RowCount:     500000,
			DataLength:   100 * 1024 * 1024,
			IndexLength:  20 * 1024 * 1024,
			AvgRowLength: 200,
		},
		Topology:   &topology.Info{Type: topology.Standalone},
		Version:    mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35, Flavor: "mysql"},
		AnalyzedAt: time.Now(),

		DMLOp:        parser.Delete,
		AffectedRows: 200000,
		AffectedPct:  40.0,
		HasWhere:     true,
		WriteSetSize: 200000 * 200,

		Risk:           analyzer.RiskDangerous,
		Method:         analyzer.ExecChunked,
		Recommendation: "Affecting ~200K rows. Chunk into batches.",
		ChunkSize:      10000,
		ChunkCount:     20,

		RollbackOptions: []analyzer.RollbackOption{
			{Label: "Pre-backup (RECOMMENDED)", SQL: "CREATE TABLE backup AS SELECT ...", Description: "Create backup table before execution."},
			{Label: "Point-in-time recovery", Description: "Use mysqlbinlog to generate reverse SQL."},
		},
	}
}

func dmlResultWithWarnings() *analyzer.Result {
	r := dmlResult()
	r.Warnings = []string{"No WHERE clause! This will affect ALL rows."}
	r.ClusterWarnings = []string{"Flow control paused at 5.0%."}
	return r
}

func ddlResultWithDiskEstimate() *analyzer.Result {
	r := ddlResult()
	// Simulate a large-table COPY operation that needs disk space
	r.Classification.Algorithm = analyzer.AlgoCopy
	r.Classification.RebuildsTable = true
	r.Risk = analyzer.RiskDangerous
	r.Method = analyzer.ExecGhost
	r.Recommendation = "COPY algorithm on a large table. Use gh-ost."
	r.DiskEstimate = &analyzer.DiskSpaceEstimate{
		RequiredBytes: 2 * 1024 * 1024 * 1024,
		RequiredHuman: "2.0 GB",
		Reason:        "gh-ost creates a full shadow copy of the table during migration",
	}
	return r
}

func galeraResult() *analyzer.Result {
	r := ddlResult()
	r.Topology = &topology.Info{
		Type:                 topology.Galera,
		GaleraClusterSize:    3,
		GaleraOSUMethod:      "TOI",
		GaleraNodeState:      "Synced",
		FlowControlPausedPct: "0.0%",
	}
	r.Classification.Algorithm = analyzer.AlgoInplace
	r.ClusterWarnings = []string{"TOI will execute this DDL on ALL 3 nodes simultaneously."}
	return r
}

func sampleConn() mysql.ConnectionConfig {
	return mysql.ConnectionConfig{
		Host: "10.0.1.50",
		Port: 3306,
		User: "dbsafe",
	}
}

func sampleTopo() *topology.Info {
	return &topology.Info{
		Type:     topology.Standalone,
		Version:  mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35, Flavor: "mysql"},
		ReadOnly: false,
	}
}

// =============================================================
// NewRenderer Factory Tests
// =============================================================

func TestNewRenderer(t *testing.T) {
	var buf bytes.Buffer

	tests := []struct {
		format   string
		wantType string
	}{
		{"json", "*output.JSONRenderer"},
		{"markdown", "*output.MarkdownRenderer"},
		{"plain", "*output.PlainRenderer"},
		{"text", "*output.TextRenderer"},
		{"", "*output.TextRenderer"},        // default
		{"unknown", "*output.TextRenderer"}, // fallback
	}

	for _, tt := range tests {
		r := NewRenderer(tt.format, &buf)
		got := typeString(r)
		if got != tt.wantType {
			t.Errorf("NewRenderer(%q) type = %s, want %s", tt.format, got, tt.wantType)
		}
	}
}

func typeString(r Renderer) string {
	switch r.(type) {
	case *JSONRenderer:
		return "*output.JSONRenderer"
	case *MarkdownRenderer:
		return "*output.MarkdownRenderer"
	case *PlainRenderer:
		return "*output.PlainRenderer"
	case *TextRenderer:
		return "*output.TextRenderer"
	default:
		return "unknown"
	}
}

// =============================================================
// Plain Renderer Tests
// =============================================================

func TestPlainRenderer_RenderPlan_DDL(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderPlan(ddlResult())
	out := buf.String()

	expects := []string{
		"DDL Analysis",
		"testdb.users",
		"ADD_COLUMN",
		"INSTANT",
		"NONE",
		"SAFE",
		"DIRECT",
		"DROP COLUMN",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("plain DDL output missing %q", e)
		}
	}
}

func TestPlainRenderer_RenderPlan_DML(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderPlan(dmlResult())
	out := buf.String()

	expects := []string{
		"DML Analysis",
		"testdb.logs",
		"DELETE",
		"DANGEROUS",
		"CHUNKED",
		"Pre-backup",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("plain DML output missing %q", e)
		}
	}
}

func TestPlainRenderer_RenderPlan_Warnings(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderPlan(dmlResultWithWarnings())
	out := buf.String()

	if !strings.Contains(out, "WARNING: No WHERE clause") {
		t.Error("plain output missing WARNING prefix")
	}
	if !strings.Contains(out, "CLUSTER WARNING: Flow control") {
		t.Error("plain output missing CLUSTER WARNING prefix")
	}
}

func TestPlainRenderer_RenderPlan_TopologyShown(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderPlan(galeraResult())
	out := buf.String()

	if !strings.Contains(out, "Topology") {
		t.Error("plain output should show topology for non-standalone")
	}
	if !strings.Contains(out, "Percona XtraDB Cluster") {
		t.Error("plain output should show Galera type")
	}
}

func TestPlainRenderer_RenderPlan_StandaloneNoTopology(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderPlan(ddlResult()) // standalone
	out := buf.String()

	if strings.Contains(out, "--- Topology ---") {
		t.Error("plain output should NOT show topology section for standalone")
	}
}

func TestPlainRenderer_RenderPlan_ScriptPath(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	result := dmlResult()
	result.GeneratedScript = "-- chunked script"
	result.ScriptPath = "./dbsafe-plan-logs-delete.sql"
	r.RenderPlan(result)
	out := buf.String()

	if !strings.Contains(out, "Script written to: ./dbsafe-plan-logs-delete.sql") {
		t.Error("plain output missing script path")
	}
}

func TestPlainRenderer_RenderPlan_DiskEstimate_Shown(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderPlan(ddlResultWithDiskEstimate())
	out := buf.String()

	if !strings.Contains(out, "Disk required:") {
		t.Error("plain output missing 'Disk required:' label")
	}
	if !strings.Contains(out, "2.0 GB") {
		t.Error("plain output missing disk size '2.0 GB'")
	}
	if !strings.Contains(out, "gh-ost") {
		t.Error("plain output missing disk reason mentioning gh-ost")
	}
}

func TestPlainRenderer_RenderPlan_DiskEstimate_Absent_ForInstant(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderPlan(ddlResult()) // INSTANT, DiskEstimate is nil
	out := buf.String()

	if strings.Contains(out, "Disk required:") {
		t.Error("plain output should NOT show disk estimate for INSTANT algorithm")
	}
}

func TestPlainRenderer_RenderTopology(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderTopology(sampleConn(), sampleTopo())
	out := buf.String()

	expects := []string{
		"Connection Info",
		"10.0.1.50:3306",
		"8.0.35",
		"Standalone",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("plain topology output missing %q", e)
		}
	}
}

func TestPlainRenderer_RenderTopology_Socket(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	conn := sampleConn()
	conn.Socket = "/var/run/mysqld/mysqld.sock"
	r.RenderTopology(conn, sampleTopo())
	out := buf.String()

	if !strings.Contains(out, "/var/run/mysqld/mysqld.sock") {
		t.Error("plain topology should show socket path when set")
	}
	if strings.Contains(out, "10.0.1.50:3306") {
		t.Error("plain topology should NOT show host:port when socket is set")
	}
}

func TestPlainRenderer_RenderTopology_Galera(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	topo := &topology.Info{
		Type:                 topology.Galera,
		Version:              mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35, Flavor: "percona-xtradb-cluster"},
		GaleraClusterSize:    3,
		GaleraNodeState:      "Synced",
		GaleraOSUMethod:      "TOI",
		FlowControlPausedPct: "0.0%",
	}
	r.RenderTopology(sampleConn(), topo)
	out := buf.String()

	for _, e := range []string{"3 nodes", "Synced", "TOI", "0.0%"} {
		if !strings.Contains(out, e) {
			t.Errorf("plain Galera topology output missing %q", e)
		}
	}
}

func TestPlainRenderer_RenderTopology_GroupReplication(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	topo := &topology.Info{
		Type:          topology.GroupRepl,
		Version:       mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35, Flavor: "mysql"},
		GRMode:        "SINGLE-PRIMARY",
		GRMemberCount: 3,
	}
	r.RenderTopology(sampleConn(), topo)
	out := buf.String()

	for _, e := range []string{"SINGLE-PRIMARY", "3"} {
		if !strings.Contains(out, e) {
			t.Errorf("plain GR topology output missing %q", e)
		}
	}
}

// =============================================================
// JSON Renderer Tests
// =============================================================

func TestJSONRenderer_RenderPlan_DDL(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderPlan(ddlResult())

	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if out["type"] != "DDL" {
		t.Errorf("type = %v, want DDL", out["type"])
	}
	if out["database"] != "testdb" {
		t.Errorf("database = %v, want testdb", out["database"])
	}
	if out["table"] != "users" {
		t.Errorf("table = %v, want users", out["table"])
	}
	if out["risk"] != "SAFE" {
		t.Errorf("risk = %v, want SAFE", out["risk"])
	}
	if out["recommended_method"] != "DIRECT" {
		t.Errorf("recommended_method = %v, want DIRECT", out["recommended_method"])
	}

	op := out["operation"].(map[string]any)
	if op["algorithm"] != "INSTANT" {
		t.Errorf("operation.algorithm = %v, want INSTANT", op["algorithm"])
	}
	if op["lock"] != "NONE" {
		t.Errorf("operation.lock = %v, want NONE", op["lock"])
	}
	if op["rebuilds_table"] != false {
		t.Errorf("operation.rebuilds_table = %v, want false", op["rebuilds_table"])
	}

	meta := out["table_metadata"].(map[string]any)
	if meta["engine"] != "InnoDB" {
		t.Errorf("table_metadata.engine = %v, want InnoDB", meta["engine"])
	}
	if meta["index_count"] != float64(2) {
		t.Errorf("table_metadata.index_count = %v, want 2", meta["index_count"])
	}

	rollback := out["rollback"].(map[string]any)
	if rollback["sql"] == nil || rollback["sql"] == "" {
		t.Error("rollback.sql should not be empty")
	}
}

func TestJSONRenderer_RenderPlan_DML(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderPlan(dmlResult())

	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if out["risk"] != "DANGEROUS" {
		t.Errorf("risk = %v, want DANGEROUS", out["risk"])
	}
	if out["recommended_method"] != "CHUNKED" {
		t.Errorf("recommended_method = %v, want CHUNKED", out["recommended_method"])
	}

	op := out["operation"].(map[string]any)
	if op["dml_operation"] != "DELETE" {
		t.Errorf("operation.dml_operation = %v, want DELETE", op["dml_operation"])
	}
	if op["affected_rows"] != float64(200000) {
		t.Errorf("operation.affected_rows = %v, want 200000", op["affected_rows"])
	}
	if op["chunk_count"] != float64(20) {
		t.Errorf("operation.chunk_count = %v, want 20", op["chunk_count"])
	}

	rollback := out["rollback"].(map[string]any)
	options := rollback["options"].([]any)
	if len(options) != 2 {
		t.Errorf("rollback.options length = %d, want 2", len(options))
	}
}

func TestJSONRenderer_RenderPlan_Warnings(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderPlan(dmlResultWithWarnings())

	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	warnings := out["warnings"].([]any)
	if len(warnings) != 1 {
		t.Errorf("warnings length = %d, want 1", len(warnings))
	}
	clusterWarnings := out["cluster_warnings"].([]any)
	if len(clusterWarnings) != 1 {
		t.Errorf("cluster_warnings length = %d, want 1", len(clusterWarnings))
	}
}

func TestJSONRenderer_RenderPlan_NoWarningsOmitted(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderPlan(ddlResult()) // no warnings

	var out map[string]any
	json.Unmarshal(buf.Bytes(), &out)

	// warnings and cluster_warnings should be omitted (omitempty)
	if _, ok := out["warnings"]; ok {
		t.Error("warnings should be omitted when empty")
	}
	if _, ok := out["cluster_warnings"]; ok {
		t.Error("cluster_warnings should be omitted when empty")
	}
}

func TestJSONRenderer_RenderPlan_ScriptPath(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	result := dmlResult()
	result.GeneratedScript = "-- chunked script"
	result.ScriptPath = "./dbsafe-plan-logs-delete.sql"
	r.RenderPlan(result)

	var out map[string]any
	json.Unmarshal(buf.Bytes(), &out)

	script := out["generated_script"].(map[string]any)
	if script["path"] != "./dbsafe-plan-logs-delete.sql" {
		t.Errorf("script.path = %v, want ./dbsafe-plan-logs-delete.sql", script["path"])
	}
}

func TestJSONRenderer_RenderPlan_NoScriptOmitted(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	result := ddlResult()
	r.RenderPlan(result)

	var out map[string]any
	json.Unmarshal(buf.Bytes(), &out)

	if _, ok := out["generated_script"]; ok {
		t.Error("generated_script should be omitted when no script")
	}
}

func TestJSONRenderer_RenderPlan_GaleraTopology(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderPlan(galeraResult())

	var out map[string]any
	json.Unmarshal(buf.Bytes(), &out)

	topo := out["topology"].(map[string]any)
	if topo["type"] != "galera" {
		t.Errorf("topology.type = %v, want galera", topo["type"])
	}
	if topo["cluster_size"] != float64(3) {
		t.Errorf("topology.cluster_size = %v, want 3", topo["cluster_size"])
	}
	if topo["osu_method"] != "TOI" {
		t.Errorf("topology.osu_method = %v, want TOI", topo["osu_method"])
	}
}

func TestJSONRenderer_RenderPlan_DiskEstimate_Shown(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderPlan(ddlResultWithDiskEstimate())

	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	disk, ok := out["disk_space_estimate"].(map[string]any)
	if !ok {
		t.Fatal("disk_space_estimate key missing or wrong type in JSON output")
	}
	if disk["required_human"] != "2.0 GB" {
		t.Errorf("required_human = %v, want '2.0 GB'", disk["required_human"])
	}
	if disk["required_bytes"] != float64(2*1024*1024*1024) {
		t.Errorf("required_bytes = %v, want %d", disk["required_bytes"], 2*1024*1024*1024)
	}
	if !strings.Contains(disk["reason"].(string), "gh-ost") {
		t.Errorf("reason should mention gh-ost, got: %v", disk["reason"])
	}
}

func TestJSONRenderer_RenderPlan_DiskEstimate_OmittedWhenNil(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderPlan(ddlResult()) // INSTANT, DiskEstimate is nil

	var out map[string]any
	json.Unmarshal(buf.Bytes(), &out)

	if _, ok := out["disk_space_estimate"]; ok {
		t.Error("disk_space_estimate should be omitted when DiskEstimate is nil")
	}
}

func TestJSONRenderer_RenderPlan_ValidJSON(t *testing.T) {
	// Ensure all result types produce valid JSON
	results := []*analyzer.Result{ddlResult(), dmlResult(), dmlResultWithWarnings(), galeraResult()}
	for i, result := range results {
		var buf bytes.Buffer
		r := &JSONRenderer{w: &buf}
		r.RenderPlan(result)
		if !json.Valid(buf.Bytes()) {
			t.Errorf("result[%d] produced invalid JSON", i)
		}
	}
}

func TestJSONRenderer_RenderTopology(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderTopology(sampleConn(), sampleTopo())

	var out map[string]any
	if err := json.Unmarshal(buf.Bytes(), &out); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}

	if out["host"] != "10.0.1.50" {
		t.Errorf("host = %v, want 10.0.1.50", out["host"])
	}
	if out["port"] != float64(3306) {
		t.Errorf("port = %v, want 3306", out["port"])
	}
	if out["topology"] != "standalone" {
		t.Errorf("topology = %v, want standalone", out["topology"])
	}
}

func TestJSONRenderer_RenderTopology_Galera(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	topo := &topology.Info{
		Type:                 topology.Galera,
		Version:              mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35, Flavor: "percona-xtradb-cluster"},
		GaleraClusterSize:    3,
		GaleraNodeState:      "Synced",
		GaleraOSUMethod:      "TOI",
		WsrepMaxWsSize:       2147483647,
		FlowControlPausedPct: "0.0%",
	}
	r.RenderTopology(sampleConn(), topo)

	var out map[string]any
	json.Unmarshal(buf.Bytes(), &out)

	if out["cluster_size"] != float64(3) {
		t.Errorf("cluster_size = %v, want 3", out["cluster_size"])
	}
	if out["osu_method"] != "TOI" {
		t.Errorf("osu_method = %v, want TOI", out["osu_method"])
	}
}

// =============================================================
// Markdown Renderer Tests
// =============================================================

func TestMarkdownRenderer_RenderPlan_DDL(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(ddlResult())
	out := buf.String()

	expects := []string{
		"# dbsafe — DDL Analysis",
		"**Statement:** `ALTER TABLE users ADD COLUMN email VARCHAR(255)`",
		"## Table Metadata",
		"| Table | `testdb.users` |",
		"| Engine | InnoDB |",
		"## Operation",
		"| Algorithm | **INSTANT** |",
		"| Lock | NONE |",
		"## Rollback",
		"```sql",
		"DROP COLUMN",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("markdown DDL output missing %q", e)
		}
	}
}

func TestMarkdownRenderer_RenderPlan_DML(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(dmlResult())
	out := buf.String()

	expects := []string{
		"# dbsafe — DML Analysis",
		"| Type | DELETE |",
		"| Affected rows |",
		"Recommendation",
		"**Method:** CHUNKED",
		"### Pre-backup",
		"### Point-in-time recovery",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("markdown DML output missing %q", e)
		}
	}
}

func TestMarkdownRenderer_RenderPlan_Warnings(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(dmlResultWithWarnings())
	out := buf.String()

	if !strings.Contains(out, "## ⚠ Warnings") {
		t.Error("markdown should have warnings section")
	}
	if !strings.Contains(out, "**Warning:** No WHERE clause") {
		t.Error("markdown should contain warning text")
	}
	if !strings.Contains(out, "**Cluster:** Flow control") {
		t.Error("markdown should contain cluster warning text")
	}
}

func TestMarkdownRenderer_RenderPlan_NoWarningsSection(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(ddlResult()) // no warnings
	out := buf.String()

	if strings.Contains(out, "## ⚠ Warnings") {
		t.Error("markdown should NOT show warnings section when no warnings")
	}
}

func TestMarkdownRenderer_RenderPlan_TopologyForGalera(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(galeraResult())
	out := buf.String()

	if !strings.Contains(out, "## Topology") {
		t.Error("markdown should show topology section for Galera")
	}
	if !strings.Contains(out, "TOI") {
		t.Error("markdown Galera topology should contain OSU method")
	}
}

func TestMarkdownRenderer_RenderPlan_NoTopologyForStandalone(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(ddlResult())
	out := buf.String()

	if strings.Contains(out, "## Topology") {
		t.Error("markdown should NOT show topology section for standalone")
	}
}

func TestMarkdownRenderer_RenderPlan_DiskEstimate_Shown(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(ddlResultWithDiskEstimate())
	out := buf.String()

	if !strings.Contains(out, "**Disk space required:**") {
		t.Error("markdown output missing '**Disk space required:**' label")
	}
	if !strings.Contains(out, "2.0 GB") {
		t.Error("markdown output missing disk size '2.0 GB'")
	}
	if !strings.Contains(out, "gh-ost") {
		t.Error("markdown output missing disk reason mentioning gh-ost")
	}
}

func TestMarkdownRenderer_RenderPlan_DiskEstimate_Absent_ForInstant(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(ddlResult()) // INSTANT, DiskEstimate is nil
	out := buf.String()

	if strings.Contains(out, "Disk space required") {
		t.Error("markdown output should NOT show disk estimate for INSTANT algorithm")
	}
}

func TestMarkdownRenderer_RenderPlan_RiskEmoji(t *testing.T) {
	tests := []struct {
		risk  analyzer.RiskLevel
		emoji string
	}{
		{analyzer.RiskSafe, "✅"},
		{analyzer.RiskCaution, "⚠️"},
		{analyzer.RiskDangerous, "❌"},
	}
	for _, tt := range tests {
		var buf bytes.Buffer
		r := &MarkdownRenderer{w: &buf}
		result := ddlResult()
		result.Risk = tt.risk
		r.RenderPlan(result)
		out := buf.String()

		if !strings.Contains(out, tt.emoji) {
			t.Errorf("markdown with risk %s should contain emoji %s", tt.risk, tt.emoji)
		}
	}
}

func TestMarkdownRenderer_RenderPlan_ScriptPath(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	result := dmlResult()
	result.GeneratedScript = "-- script"
	result.ScriptPath = "./dbsafe-plan-logs-delete.sql"
	r.RenderPlan(result)
	out := buf.String()

	if !strings.Contains(out, "`./dbsafe-plan-logs-delete.sql`") {
		t.Error("markdown should show script path in backticks")
	}
}

func TestMarkdownRenderer_RenderTopology(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderTopology(sampleConn(), sampleTopo())
	out := buf.String()

	expects := []string{
		"# dbsafe — Connection Info",
		"| Host | `10.0.1.50:3306` |",
		"| Version | 8.0.35",
		"| Topology | Standalone |",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("markdown topology output missing %q", e)
		}
	}
}

// =============================================================
// Text Renderer Tests
// =============================================================

func TestTextRenderer_RenderPlan_DDL(t *testing.T) {
	var buf bytes.Buffer
	r := &TextRenderer{w: &buf}
	r.RenderPlan(ddlResult())
	out := buf.String()

	// Text renderer uses lipgloss styling, so exact formatting varies,
	// but key content should still be present
	expects := []string{
		"testdb.users",
		"ADD_COLUMN",
		"INSTANT",
		"NONE",
		"DIRECT",
		"DROP COLUMN",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("text DDL output missing %q", e)
		}
	}
}

func TestTextRenderer_RenderPlan_DML(t *testing.T) {
	var buf bytes.Buffer
	r := &TextRenderer{w: &buf}
	r.RenderPlan(dmlResult())
	out := buf.String()

	expects := []string{
		"testdb.logs",
		"DELETE",
		"CHUNKED",
		"Pre-backup",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("text DML output missing %q", e)
		}
	}
}

func TestTextRenderer_RenderPlan_DiskEstimate_Shown(t *testing.T) {
	var buf bytes.Buffer
	r := &TextRenderer{w: &buf}
	r.RenderPlan(ddlResultWithDiskEstimate())
	out := buf.String()

	if !strings.Contains(out, "Disk required:") {
		t.Error("text output missing 'Disk required:' label")
	}
	if !strings.Contains(out, "2.0 GB") {
		t.Error("text output missing disk size '2.0 GB'")
	}
}

func TestTextRenderer_RenderPlan_DiskEstimate_Absent_ForInstant(t *testing.T) {
	var buf bytes.Buffer
	r := &TextRenderer{w: &buf}
	r.RenderPlan(ddlResult()) // INSTANT, DiskEstimate is nil
	out := buf.String()

	if strings.Contains(out, "Disk required:") {
		t.Error("text output should NOT show disk estimate for INSTANT algorithm")
	}
}

func TestTextRenderer_RenderPlan_Warnings(t *testing.T) {
	var buf bytes.Buffer
	r := &TextRenderer{w: &buf}
	r.RenderPlan(dmlResultWithWarnings())
	out := buf.String()

	if !strings.Contains(out, "No WHERE clause") {
		t.Error("text output missing warning content")
	}
	if !strings.Contains(out, "Flow control") {
		t.Error("text output missing cluster warning content")
	}
}

func TestTextRenderer_RenderTopology(t *testing.T) {
	var buf bytes.Buffer
	r := &TextRenderer{w: &buf}
	r.RenderTopology(sampleConn(), sampleTopo())
	out := buf.String()

	expects := []string{
		"10.0.1.50:3306",
		"8.0.35",
		"Standalone",
	}
	for _, e := range expects {
		if !strings.Contains(out, e) {
			t.Errorf("text topology output missing %q", e)
		}
	}
}

func TestTextRenderer_RenderTopology_Socket(t *testing.T) {
	var buf bytes.Buffer
	r := &TextRenderer{w: &buf}
	conn := sampleConn()
	conn.Socket = "/tmp/mysql.sock"
	r.RenderTopology(conn, sampleTopo())
	out := buf.String()

	if !strings.Contains(out, "/tmp/mysql.sock") {
		t.Error("text topology should show socket path")
	}
}

// =============================================================
// Helper function tests
// =============================================================

func TestFormatTopoType(t *testing.T) {
	tests := []struct {
		topo *topology.Info
		want string
	}{
		{&topology.Info{Type: topology.Standalone}, "Standalone"},
		{&topology.Info{Type: topology.AsyncReplica}, "Async Replication"},
		{&topology.Info{Type: topology.SemiSyncReplica}, "Semi-sync Replication"},
		{&topology.Info{Type: topology.Galera, GaleraClusterSize: 3}, "Percona XtraDB Cluster (3 nodes)"},
		{&topology.Info{Type: topology.GroupRepl, GRMode: "SINGLE-PRIMARY", GRMemberCount: 3}, "Group Replication (SINGLE-PRIMARY, 3 members)"},
	}
	for _, tt := range tests {
		got := formatTopoType(tt.topo)
		if got != tt.want {
			t.Errorf("formatTopoType(%s) = %q, want %q", tt.topo.Type, got, tt.want)
		}
	}
}

func TestFormatFKRefs(t *testing.T) {
	if got := formatFKRefs(nil); got != "None" {
		t.Errorf("formatFKRefs(nil) = %q, want None", got)
	}

	fks := []mysql.ForeignKeyInfo{
		{Name: "fk_user", Columns: []string{"user_id"}, ReferencedTable: "users", ReferencedCols: []string{"id"}},
	}
	got := formatFKRefs(fks)
	if !strings.Contains(got, "1") || !strings.Contains(got, "users.id") {
		t.Errorf("formatFKRefs = %q, want count and reference", got)
	}
}

func TestFormatTriggers(t *testing.T) {
	if got := formatTriggers(nil); got != "None" {
		t.Errorf("formatTriggers(nil) = %q, want None", got)
	}

	triggers := []mysql.TriggerInfo{
		{Name: "trg_audit", Event: "DELETE", Timing: "AFTER"},
	}
	got := formatTriggers(triggers)
	if !strings.Contains(got, "1") || !strings.Contains(got, "AFTER DELETE") || !strings.Contains(got, "trg_audit") {
		t.Errorf("formatTriggers = %q, want count and trigger info", got)
	}
}

func TestFormatNumber_Output(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{0, "0"},
		{999, "999"},
		{1000, "1,000"},
		{50000, "50,000"},
		{1000000, "1,000,000"},
	}
	for _, tt := range tests {
		got := formatNumber(tt.input)
		if got != tt.want {
			t.Errorf("formatNumber(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

func TestHumanBytes_Output(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{500, "500 B"},
		{1024, "1.0 KB"},
		{1024 * 1024, "1.0 MB"},
		{1024 * 1024 * 1024, "1.0 GB"},
	}
	for _, tt := range tests {
		got := humanBytes(tt.input)
		if got != tt.want {
			t.Errorf("humanBytes(%d) = %q, want %q", tt.input, got, tt.want)
		}
	}
}

// multiOpResult returns a Result fixture with SubOpResults for multi-op rendering tests.
func multiOpResult() *analyzer.Result {
	r := ddlResult()
	r.Statement = "ALTER TABLE users ADD COLUMN nickname VARCHAR(50), ADD INDEX idx_nickname (nickname)"
	r.DDLOp = parser.MultipleOps
	r.Classification = analyzer.DDLClassification{
		Algorithm:     analyzer.AlgoInplace,
		Lock:          analyzer.LockNone,
		RebuildsTable: true,
		Notes:         "Combined algorithm and lock derived from the most restrictive sub-operation.",
	}
	r.SubOpResults = []analyzer.SubOpResult{
		{
			Op:             parser.AddColumn,
			Classification: analyzer.DDLClassification{Algorithm: analyzer.AlgoInstant, Lock: analyzer.LockNone},
		},
		{
			Op:             parser.AddIndex,
			Classification: analyzer.DDLClassification{Algorithm: analyzer.AlgoInplace, Lock: analyzer.LockNone, RebuildsTable: true},
		},
	}
	r.Risk = analyzer.RiskSafe
	r.Method = analyzer.ExecDirect
	return r
}

func TestTextRenderer_MultiOp_SubOpsLine(t *testing.T) {
	var buf bytes.Buffer
	r := &TextRenderer{w: &buf}
	r.RenderPlan(multiOpResult())
	out := buf.String()

	if !strings.Contains(out, "MULTIPLE_OPS") {
		t.Error("text output missing MULTIPLE_OPS type")
	}
	if !strings.Contains(out, "Sub-ops:") {
		t.Error("text output missing Sub-ops: line")
	}
	if !strings.Contains(out, "ADD_COLUMN") {
		t.Error("text output missing ADD_COLUMN in sub-ops")
	}
	if !strings.Contains(out, "ADD_INDEX") {
		t.Error("text output missing ADD_INDEX in sub-ops")
	}
}

func TestPlainRenderer_MultiOp_SubOpsLine(t *testing.T) {
	var buf bytes.Buffer
	r := &PlainRenderer{w: &buf}
	r.RenderPlan(multiOpResult())
	out := buf.String()

	if !strings.Contains(out, "Sub-ops:") {
		t.Error("plain output missing Sub-ops: line")
	}
	if !strings.Contains(out, "ADD_COLUMN") {
		t.Error("plain output missing ADD_COLUMN in sub-ops")
	}
}

func TestMarkdownRenderer_MultiOp_SubOpsRows(t *testing.T) {
	var buf bytes.Buffer
	r := &MarkdownRenderer{w: &buf}
	r.RenderPlan(multiOpResult())
	out := buf.String()

	if !strings.Contains(out, "Sub-op: ADD_COLUMN") {
		t.Error("markdown output missing sub-op ADD_COLUMN row")
	}
	if !strings.Contains(out, "Sub-op: ADD_INDEX") {
		t.Error("markdown output missing sub-op ADD_INDEX row")
	}
}

func TestJSONRenderer_MultiOp_SubOperations(t *testing.T) {
	var buf bytes.Buffer
	r := &JSONRenderer{w: &buf}
	r.RenderPlan(multiOpResult())
	out := buf.String()

	if !strings.Contains(out, `"sub_operations"`) {
		t.Error("JSON output missing sub_operations field")
	}
	if !strings.Contains(out, `"ADD_COLUMN"`) {
		t.Error("JSON sub_operations missing ADD_COLUMN entry")
	}
	if !strings.Contains(out, `"ADD_INDEX"`) {
		t.Error("JSON sub_operations missing ADD_INDEX entry")
	}

	// Verify valid JSON structure
	var parsed map[string]any
	if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
		t.Errorf("JSON output is not valid JSON: %v", err)
	}
	op, ok := parsed["operation"].(map[string]any)
	if !ok {
		t.Fatal("JSON operation field missing or wrong type")
	}
	subOps, ok := op["sub_operations"].([]any)
	if !ok || len(subOps) != 2 {
		t.Errorf("JSON sub_operations: got %v, want 2-element array", op["sub_operations"])
	}
}
