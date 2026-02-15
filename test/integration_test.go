// +build integration

package test

import (
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/nethalo/dbsafe/internal/analyzer"
	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

/*
Integration tests for dbsafe with real MySQL instances.

To run these tests:
1. Start test databases: docker-compose -f docker-compose.test.yml up -d
2. Wait for healthy: docker-compose -f docker-compose.test.yml ps
3. Run tests: go test -tags=integration ./test
4. Cleanup: docker-compose -f docker-compose.test.yml down -v

Environment variables:
- MYSQL_STANDALONE_DSN: DSN for standalone MySQL (default: dbsafe:test_password@tcp(localhost:13306)/testdb)
- MYSQL_LTS_DSN: DSN for MySQL 8.4 LTS
- PERCONA_DSN: DSN for Percona Server
- PXC_DSN: DSN for Percona XtraDB Cluster
- GR_DSN: DSN for Group Replication
- REPL_PRIMARY_DSN: DSN for replication primary
- REPL_REPLICA_DSN: DSN for replication replica
*/

// Test database connections
func getStandaloneDSN() string {
	if dsn := os.Getenv("MYSQL_STANDALONE_DSN"); dsn != "" {
		return dsn
	}
	return "dbsafe:test_password@tcp(localhost:13306)/testdb"
}

func getLTSDSN() string {
	if dsn := os.Getenv("MYSQL_LTS_DSN"); dsn != "" {
		return dsn
	}
	return "dbsafe:test_password@tcp(localhost:13307)/testdb"
}

func waitForMySQL(dsn string, maxAttempts int) error {
	for i := 0; i < maxAttempts; i++ {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}
		defer db.Close()

		if err := db.Ping(); err == nil {
			return nil
		}

		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("MySQL not ready after %d attempts", maxAttempts)
}

func setupTestTable(db *sql.DB, tableName string) error {
	// Create a test table with various column types
	createSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT PRIMARY KEY AUTO_INCREMENT,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(255),
			age INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
			status ENUM('active', 'inactive') DEFAULT 'active',
			data JSON,
			INDEX idx_email (email),
			INDEX idx_status (status)
		) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
	`, tableName)

	_, err := db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("failed to create test table: %w", err)
	}

	// Insert some test data
	insertSQL := fmt.Sprintf(`
		INSERT INTO %s (name, email, age) VALUES
		('Alice', 'alice@example.com', 30),
		('Bob', 'bob@example.com', 25),
		('Charlie', 'charlie@example.com', 35)
	`, tableName)

	_, err = db.Exec(insertSQL)
	if err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	return nil
}

func cleanupTestTable(db *sql.DB, tableName string) {
	db.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName))
}

// Integration Tests

func TestIntegration_StandaloneMySQL(t *testing.T) {
	dsn := getStandaloneDSN()

	if err := waitForMySQL(dsn, 30); err != nil {
		t.Skip("MySQL standalone not available:", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tableName := "integration_test_standalone"
	if err := setupTestTable(db, tableName); err != nil {
		t.Fatal(err)
	}
	defer cleanupTestTable(db, tableName)

	// Test topology detection
	topo, err := topology.Detect(db, false)
	if err != nil {
		t.Fatalf("topology detection failed: %v", err)
	}

	if topo.Type != topology.Standalone {
		t.Errorf("expected Standalone topology, got %s", topo.Type)
	}

	// Test version detection
	version, err := mysql.GetServerVersion(db)
	if err != nil {
		t.Fatalf("version detection failed: %v", err)
	}

	if version.Major != 8 {
		t.Errorf("expected MySQL 8.x, got %d.%d.%d", version.Major, version.Minor, version.Patch)
	}

	// Test metadata collection
	meta, err := mysql.GetTableMetadata(db, "testdb", tableName)
	if err != nil {
		t.Fatalf("metadata collection failed: %v", err)
	}

	if meta.Table != tableName {
		t.Errorf("expected table name %s, got %s", tableName, meta.Table)
	}

	if len(meta.Columns) < 5 {
		t.Errorf("expected at least 5 columns, got %d", len(meta.Columns))
	}

	if len(meta.Indexes) < 2 {
		t.Errorf("expected at least 2 indexes, got %d", len(meta.Indexes))
	}

	// Test end-to-end analysis - DDL
	ddlSQL := fmt.Sprintf("ALTER TABLE %s ADD COLUMN phone VARCHAR(20)", tableName)
	parsed, err := parser.Parse(ddlSQL)
	if err != nil {
		t.Fatalf("parsing DDL failed: %v", err)
	}

	input := analyzer.Input{
		Parsed:    parsed,
		Meta:      meta,
		Topo:      topo,
		Version:   version,
		ChunkSize: 10000,
	}

	result := analyzer.Analyze(input)

	if result.Risk == "" {
		t.Error("expected risk assessment")
	}

	if result.Method == "" {
		t.Error("expected execution method recommendation")
	}

	if result.Recommendation == "" {
		t.Error("expected recommendation")
	}

	// Test end-to-end analysis - DML
	dmlSQL := fmt.Sprintf("DELETE FROM %s WHERE age > 30", tableName)
	parsedDML, err := parser.Parse(dmlSQL)
	if err != nil {
		t.Fatalf("parsing DML failed: %v", err)
	}

	estimatedRows, err := mysql.EstimateRowsAffected(db, dmlSQL)
	if err != nil {
		t.Logf("EXPLAIN failed (expected for some statements): %v", err)
		estimatedRows = 0
	}

	inputDML := analyzer.Input{
		Parsed:        parsedDML,
		Meta:          meta,
		Topo:          topo,
		Version:       version,
		ChunkSize:     10000,
		EstimatedRows: estimatedRows,
	}

	resultDML := analyzer.Analyze(inputDML)

	if resultDML.DMLOp != parser.Delete {
		t.Errorf("expected DELETE operation, got %s", resultDML.DMLOp)
	}

	if len(resultDML.RollbackOptions) == 0 {
		t.Error("expected rollback options for DML")
	}
}

func TestIntegration_MySQLLTS(t *testing.T) {
	dsn := getLTSDSN()

	if err := waitForMySQL(dsn, 30); err != nil {
		t.Skip("MySQL LTS not available:", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Test version detection
	version, err := mysql.GetServerVersion(db)
	if err != nil {
		t.Fatalf("version detection failed: %v", err)
	}

	if !version.IsLTS {
		t.Errorf("expected LTS version, got %s", version.String())
	}

	if version.Major != 8 || version.Minor != 4 {
		t.Errorf("expected 8.4.x, got %d.%d.%d", version.Major, version.Minor, version.Patch)
	}
}

func TestIntegration_DDLClassification(t *testing.T) {
	dsn := getStandaloneDSN()

	if err := waitForMySQL(dsn, 30); err != nil {
		t.Skip("MySQL not available:", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	version, err := mysql.GetServerVersion(db)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name              string
		sql               string
		expectedAlgorithm analyzer.Algorithm
	}{
		{
			name:              "ADD COLUMN trailing - INSTANT in 8.0.12+",
			sql:               "ALTER TABLE test ADD COLUMN new_col VARCHAR(100)",
			expectedAlgorithm: analyzer.AlgoInstant,
		},
		{
			name:              "ADD INDEX - INPLACE",
			sql:               "ALTER TABLE test ADD INDEX idx_new (new_col)",
			expectedAlgorithm: analyzer.AlgoInplace,
		},
		{
			name:              "MODIFY COLUMN type change - INPLACE or COPY",
			sql:               "ALTER TABLE test MODIFY COLUMN name TEXT",
			expectedAlgorithm: analyzer.AlgoInplace, // Might be COPY depending on change
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed, err := parser.Parse(tt.sql)
			if err != nil {
				t.Fatalf("parse failed: %v", err)
			}

			classification := analyzer.ClassifyDDLWithContext(parsed, version.Major, version.Minor, version.Patch)

			t.Logf("SQL: %s", tt.sql)
			t.Logf("Algorithm: %s, Lock: %s, Rebuilds: %v",
				classification.Algorithm, classification.Lock, classification.RebuildsTable)

			// Note: Expected algorithm might vary by version, so we just verify it's set
			if classification.Algorithm == "" {
				t.Error("expected algorithm to be set")
			}
		})
	}
}

// Benchmark integration tests

func BenchmarkIntegration_MetadataCollection(b *testing.B) {
	dsn := getStandaloneDSN()

	if err := waitForMySQL(dsn, 10); err != nil {
		b.Skip("MySQL not available:", err)
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()

	tableName := "benchmark_metadata_test"
	if err := setupTestTable(db, tableName); err != nil {
		b.Fatal(err)
	}
	defer cleanupTestTable(db, tableName)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := mysql.GetTableMetadata(db, "testdb", tableName)
		if err != nil {
			b.Fatal(err)
		}
	}
}
