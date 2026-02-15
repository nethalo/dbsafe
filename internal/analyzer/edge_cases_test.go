package analyzer

import (
	"strings"
	"testing"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

func TestAnalyze_OtherDDL(t *testing.T) {
	// Test handling of unrecognized DDL operations
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:     parser.DDL,
			DDLOp:    parser.OtherDDL,
			Database: "testdb",
			Table:    "test",
			RawSQL:   "ALTER TABLE test SOME_UNKNOWN_OPERATION",
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "test",
		},
		Version: mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30},
		Topo:    &topology.Info{Type: topology.Standalone},
	}

	result := Analyze(input)

	// Should be marked as DANGEROUS
	if result.Risk != RiskDangerous {
		t.Errorf("OtherDDL should be RiskDangerous, got %s", result.Risk)
	}

	// Should have warnings
	if len(result.Warnings) == 0 {
		t.Error("OtherDDL should have warnings")
	}

	foundWarning := false
	for _, warning := range result.Warnings {
		if strings.Contains(warning, "could not be fully parsed") {
			foundWarning = true
			break
		}
	}
	if !foundWarning {
		t.Error("should warn about parsing failure")
	}
}

func TestAnalyze_ColumnValidation(t *testing.T) {
	tests := []struct {
		name          string
		operation     parser.DDLOperation
		columnName    string
		oldColumnName string
		newColumnName string
		existingCols  []mysql.ColumnInfo
		wantRisk      RiskLevel
		wantWarning   string
	}{
		{
			name:       "ADD COLUMN that already exists",
			operation:  parser.AddColumn,
			columnName: "email",
			existingCols: []mysql.ColumnInfo{
				{Name: "id"},
				{Name: "email"}, // Already exists!
			},
			wantRisk:    RiskDangerous,
			wantWarning: "already exists",
		},
		{
			name:       "DROP COLUMN that doesn't exist",
			operation:  parser.DropColumn,
			columnName: "nonexistent",
			existingCols: []mysql.ColumnInfo{
				{Name: "id"},
				{Name: "name"},
			},
			wantRisk:    RiskDangerous,
			wantWarning: "does not exist",
		},
		{
			name:       "MODIFY COLUMN that doesn't exist",
			operation:  parser.ModifyColumn,
			columnName: "missing",
			existingCols: []mysql.ColumnInfo{
				{Name: "id"},
			},
			wantRisk:    RiskDangerous,
			wantWarning: "does not exist",
		},
		{
			name:          "CHANGE COLUMN - old doesn't exist",
			operation:     parser.ChangeColumn,
			oldColumnName: "old_name",
			newColumnName: "new_name",
			existingCols: []mysql.ColumnInfo{
				{Name: "id"},
			},
			wantRisk:    RiskDangerous,
			wantWarning: "does not exist",
		},
		{
			name:          "CHANGE COLUMN - new already exists",
			operation:     parser.ChangeColumn,
			oldColumnName: "status",
			newColumnName: "email", // Already exists
			existingCols: []mysql.ColumnInfo{
				{Name: "id"},
				{Name: "status"},
				{Name: "email"},
			},
			wantRisk:    RiskDangerous,
			wantWarning: "already exists",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := Input{
				Parsed: &parser.ParsedSQL{
					Type:          parser.DDL,
					DDLOp:         tt.operation,
					Database:      "db",
					Table:         "test",
					ColumnName:    tt.columnName,
					OldColumnName: tt.oldColumnName,
					NewColumnName: tt.newColumnName,
				},
				Meta: &mysql.TableMetadata{
					Database: "db",
					Table:    "test",
					Columns:  tt.existingCols,
				},
				Version: mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30},
				Topo:    &topology.Info{Type: topology.Standalone},
			}

			result := Analyze(input)

			if result.Risk != tt.wantRisk {
				t.Errorf("Risk = %s, want %s", result.Risk, tt.wantRisk)
			}

			foundWarning := false
			for _, w := range result.Warnings {
				if strings.Contains(strings.ToLower(w), strings.ToLower(tt.wantWarning)) {
					foundWarning = true
					break
				}
			}
			if !foundWarning {
				t.Errorf("should have warning containing %q, got warnings: %v", tt.wantWarning, result.Warnings)
			}
		})
	}
}

func TestAnalyze_ChangeColumn_SameNameAllowed(t *testing.T) {
	// CHANGE COLUMN can rename to the same name (just changes type/attributes)
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			DDLOp:         parser.ChangeColumn,
			Database:      "db",
			Table:         "test",
			OldColumnName: "status",
			NewColumnName: "status", // Same name
		},
		Meta: &mysql.TableMetadata{
			Database: "db",
			Table:    "test",
			Columns: []mysql.ColumnInfo{
				{Name: "id"},
				{Name: "status"},
			},
		},
		Version: mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30},
		Topo:    &topology.Info{Type: topology.Standalone},
	}

	result := Analyze(input)

	// Should NOT be dangerous - same name is allowed
	if result.Risk == RiskDangerous {
		t.Error("CHANGE COLUMN to same name should not be dangerous")
	}

	// Should not have "already exists" warning
	for _, w := range result.Warnings {
		if strings.Contains(w, "already exists") {
			t.Errorf("should not warn about column already existing when renaming to itself: %s", w)
		}
	}
}

func TestGenerateChunkedScript_Delete(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:        parser.DML,
			DMLOp:       parser.Delete,
			RawSQL:      "DELETE FROM logs WHERE created_at < '2020-01-01'",
			Database:    "mydb",
			Table:       "logs",
			WhereClause: "created_at < '2020-01-01'",
		},
		Meta: &mysql.TableMetadata{
			Database: "mydb",
			Table:    "logs",
		},
		ChunkSize: 5000,
	}

	result := &Result{
		Database:     "mydb",
		Table:        "logs",
		AffectedRows: 100000,
		ChunkSize:    5000,
	}

	generateChunkedScript(input, result)

	if result.GeneratedScript == "" {
		t.Fatal("should generate chunked script")
	}

	script := result.GeneratedScript

	// Should contain batch size variable
	if !strings.Contains(script, "@batch_size") {
		t.Error("script should define @batch_size variable")
	}

	// Should contain sleep time
	if !strings.Contains(script, "@sleep_time") {
		t.Error("script should define @sleep_time variable")
	}

	// Should contain DELETE statement
	if !strings.Contains(script, "DELETE FROM") {
		t.Error("script should contain DELETE statement")
	}

	// Should contain WHERE clause
	if !strings.Contains(script, "created_at < '2020-01-01'") {
		t.Error("script should contain WHERE clause")
	}

	// Should contain LIMIT
	if !strings.Contains(script, "LIMIT") {
		t.Error("script should use LIMIT for chunking")
	}

	// Should have while loop
	if !strings.Contains(script, "WHILE") {
		t.Error("script should use WHILE loop")
	}

	// Script path should be set
	if result.ScriptPath == "" {
		t.Error("should set ScriptPath")
	}

	if !strings.Contains(result.ScriptPath, "delete") {
		t.Error("script path should contain operation type")
	}
}

func TestGenerateChunkedScript_Update(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:        parser.DML,
			DMLOp:       parser.Update,
			RawSQL:      "UPDATE users SET active = 0 WHERE last_login < '2020-01-01'",
			Database:    "prod",
			Table:       "users",
			WhereClause: "last_login < '2020-01-01'",
		},
		Meta: &mysql.TableMetadata{
			Database: "prod",
			Table:    "users",
		},
		ChunkSize: 10000,
	}

	result := &Result{
		Database:     "prod",
		Table:        "users",
		AffectedRows: 500000,
		ChunkSize:    10000,
	}

	generateChunkedScript(input, result)

	if result.GeneratedScript == "" {
		t.Fatal("should generate chunked script")
	}

	script := result.GeneratedScript

	// UPDATE script should mention primary key requirement
	if !strings.Contains(script, "primary key") || !strings.Contains(script, "PK") {
		t.Error("UPDATE script should mention primary key requirement")
	}

	// Should contain example pattern
	if !strings.Contains(script, "Example pattern") {
		t.Error("UPDATE script should provide example pattern")
	}

	// Should reference the original WHERE clause
	if !strings.Contains(script, "last_login < '2020-01-01'") {
		t.Error("script should reference original WHERE clause")
	}
}

func TestGenerateChunkedScript_ChunkCount(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:        parser.DML,
			DMLOp:       parser.Delete,
			Database:    "db",
			Table:       "test",
			WhereClause: "1=1",
		},
		Meta: &mysql.TableMetadata{
			Database: "db",
			Table:    "test",
		},
		ChunkSize: 1000,
	}

	result := &Result{
		Database:     "db",
		Table:        "test",
		AffectedRows: 5000,
		ChunkSize:    1000,
	}

	generateChunkedScript(input, result)

	// Should show estimated rows in comments
	if !strings.Contains(result.GeneratedScript, "5000") {
		t.Error("script should mention estimated row count")
	}

	if !strings.Contains(result.GeneratedScript, "1000") {
		t.Error("script should mention chunk size")
	}
}

func TestApplyTopologyWarnings_GaleraWriteSetExceeds(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:  parser.DML,
			DMLOp: parser.Update,
		},
		Topo: &topology.Info{
			Type:            topology.Galera,
			WsrepMaxWsSize:  10 * 1024 * 1024, // 10 MB limit
			GaleraClusterSize: 3,
		},
	}

	result := &Result{
		StatementType: parser.DML,
		WriteSetSize:  20 * 1024 * 1024, // 20 MB - EXCEEDS limit
	}

	applyTopologyWarnings(input, result)

	// Should warn about exceeding write-set size
	foundWarning := false
	for _, w := range result.ClusterWarnings {
		if strings.Contains(w, "EXCEEDS wsrep_max_ws_size") {
			foundWarning = true
			break
		}
	}
	if !foundWarning {
		t.Error("should warn when write-set exceeds Galera limit")
	}

	// Should force chunked execution
	if result.Method != ExecChunked {
		t.Errorf("should force ExecChunked when exceeding write-set, got %s", result.Method)
	}

	if result.Risk != RiskDangerous {
		t.Errorf("should be RiskDangerous when exceeding write-set, got %s", result.Risk)
	}
}

func TestApplyTopologyWarnings_GaleraFlowControl(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type: parser.DDL,
		},
		Topo: &topology.Info{
			Type:                 topology.Galera,
			FlowControlPaused:    0.15, // 15% paused
			FlowControlPausedPct: "15.00%",
		},
	}

	result := &Result{
		StatementType: parser.DDL,
	}

	applyTopologyWarnings(input, result)

	// Should warn about flow control pressure
	foundWarning := false
	for _, w := range result.ClusterWarnings {
		if strings.Contains(w, "Flow control") && strings.Contains(w, "15") {
			foundWarning = true
			break
		}
	}
	if !foundWarning {
		t.Error("should warn about flow control when paused > 1%")
	}
}

func TestApplyTopologyWarnings_GroupReplicationLimit(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:  parser.DML,
			DMLOp: parser.Delete,
		},
		Topo: &topology.Info{
			Type:                topology.GroupRepl,
			GRTransactionLimit:  50 * 1024 * 1024, // 50 MB
		},
	}

	result := &Result{
		StatementType: parser.DML,
		WriteSetSize:  100 * 1024 * 1024, // 100 MB - EXCEEDS
	}

	applyTopologyWarnings(input, result)

	// Should warn about exceeding GR transaction size limit
	foundWarning := false
	for _, w := range result.ClusterWarnings {
		if strings.Contains(w, "EXCEEDS group_replication_transaction_size_limit") {
			foundWarning = true
			break
		}
	}
	if !foundWarning {
		t.Error("should warn when exceeding GR transaction limit")
	}
}

func TestApplyTopologyWarnings_ReplicationLag(t *testing.T) {
	lagSecs := int64(60) // 60 seconds lag
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type: parser.DML,
		},
		Topo: &topology.Info{
			Type:           topology.AsyncReplica,
			ReplicaLagSecs: &lagSecs,
		},
	}

	result := &Result{
		StatementType: parser.DML,
	}

	applyTopologyWarnings(input, result)

	// Should warn about replication lag
	foundWarning := false
	for _, w := range result.ClusterWarnings {
		if strings.Contains(w, "lag") && strings.Contains(w, "60") {
			foundWarning = true
			break
		}
	}
	if !foundWarning {
		t.Error("should warn about replication lag > 30 seconds")
	}
}
