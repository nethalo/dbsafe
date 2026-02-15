package analyzer

import (
	"testing"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// Benchmark DDL analysis performance

func BenchmarkAnalyze_DDL_Instant(b *testing.B) {
	input := ddlInput(parser.AddColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30}, 1*1024*1024*1024, topology.Standalone)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Analyze(input)
	}
}

func BenchmarkAnalyze_DDL_Inplace(b *testing.B) {
	input := ddlInput(parser.AddIndex, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30}, 10*1024*1024*1024, topology.Standalone)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Analyze(input)
	}
}

func BenchmarkAnalyze_DDL_Copy(b *testing.B) {
	input := ddlInput(parser.ChangeEngine, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 11}, 10*1024*1024*1024, topology.Standalone)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Analyze(input)
	}
}

func BenchmarkAnalyze_DML_Small(b *testing.B) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:        parser.DML,
			DMLOp:       parser.Delete,
			Database:    "testdb",
			Table:       "logs",
			HasWhere:    true,
			WhereClause: "id < 1000",
		},
		Meta: &mysql.TableMetadata{
			Database:     "testdb",
			Table:        "logs",
			RowCount:     10000,
			AvgRowLength: 100,
		},
		Version:       mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30},
		Topo:          &topology.Info{Type: topology.Standalone},
		ChunkSize:     10000,
		EstimatedRows: 500, // Small operation
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Analyze(input)
	}
}

func BenchmarkAnalyze_DML_Large(b *testing.B) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:        parser.DML,
			DMLOp:       parser.Update,
			Database:    "prod",
			Table:       "users",
			HasWhere:    true,
			WhereClause: "last_login < '2020-01-01'",
		},
		Meta: &mysql.TableMetadata{
			Database:     "prod",
			Table:        "users",
			RowCount:     10000000,
			AvgRowLength: 500,
		},
		Version:       mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30},
		Topo:          &topology.Info{Type: topology.Standalone},
		ChunkSize:     10000,
		EstimatedRows: 500000, // Large operation - will trigger chunking
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Analyze(input)
	}
}

func BenchmarkAnalyze_Galera(b *testing.B) {
	input := ddlInput(parser.AddColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30}, 5*1024*1024*1024, topology.Galera)
	input.Topo = &topology.Info{
		Type:                 topology.Galera,
		GaleraClusterSize:    3,
		GaleraOSUMethod:      "TOI",
		WsrepMaxWsSize:       2147483647,
		FlowControlPaused:    0.0,
		FlowControlPausedPct: "0.0%",
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Analyze(input)
	}
}

func BenchmarkAnalyze_GroupReplication(b *testing.B) {
	input := ddlInput(parser.ModifyColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30}, 2*1024*1024*1024, topology.GroupRepl)
	input.Topo = &topology.Info{
		Type:               topology.GroupRepl,
		GRMode:             "SINGLE-PRIMARY",
		GRMemberCount:      3,
		GRTransactionLimit: 150 * 1024 * 1024,
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = Analyze(input)
	}
}

// Benchmark command generation

func BenchmarkGenerateGhostCommand(b *testing.B) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:     parser.DDL,
			DDLOp:    parser.AddColumn,
			RawSQL:   "ALTER TABLE users ADD COLUMN email VARCHAR(255)",
			Database: "mydb",
			Table:    "users",
		},
		Connection: &ConnectionInfo{
			Host: "localhost",
			Port: 3306,
			User: "dbuser",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = generateGhostCommand(input)
	}
}

func BenchmarkGeneratePtOSCCommand(b *testing.B) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:     parser.DDL,
			DDLOp:    parser.AddIndex,
			RawSQL:   "ALTER TABLE products ADD INDEX idx_price (price)",
			Database: "shop",
			Table:    "products",
		},
		Connection: &ConnectionInfo{
			Host: "db.example.com",
			Port: 3306,
			User: "admin",
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = generatePtOSCCommand(input, false)
	}
}

func BenchmarkGenerateChunkedScript(b *testing.B) {
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
		ChunkSize: 10000,
	}

	result := &Result{
		Database:     "mydb",
		Table:        "logs",
		AffectedRows: 500000,
		ChunkSize:    10000,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		generateChunkedScript(input, result)
		// Reset for next iteration
		result.GeneratedScript = ""
		result.ScriptPath = ""
	}
}

// Benchmark DDL classification

func BenchmarkClassifyDDLWithContext_AddColumn(b *testing.B) {
	parsed := &parser.ParsedSQL{
		Type:   parser.DDL,
		DDLOp:  parser.AddColumn,
		Table:  "users",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ClassifyDDLWithContext(parsed, 8, 0, 30)
	}
}

func BenchmarkClassifyDDLWithContext_ComplexDDL(b *testing.B) {
	parsed := &parser.ParsedSQL{
		Type:          parser.DDL,
		DDLOp:         parser.MultipleOps,
		Table:         "products",
		DDLOperations: []parser.DDLOperation{parser.AddColumn, parser.AddIndex, parser.ModifyColumn},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = ClassifyDDLWithContext(parsed, 8, 0, 30)
	}
}

// Benchmark concurrent analysis

func BenchmarkAnalyze_Concurrent(b *testing.B) {
	input := ddlInput(parser.AddColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30}, 1*1024*1024*1024, topology.Standalone)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = Analyze(input)
		}
	})
}
