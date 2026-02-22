package topology

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/nethalo/dbsafe/internal/mysql"
)

func TestDetectGalera(t *testing.T) {
	tests := []struct {
		name              string
		wsrepOn           string
		wsrepOnErr        error
		clusterSizeStatus string
		clusterStatusErr  error
		clusterSizeVar    string
		clusterVarErr     error
		expectedDetected  bool
		expectedSize      int
		expectedError     bool
	}{
		{
			name:              "PXC cluster with 3 nodes (status)",
			wsrepOn:           "ON",
			clusterSizeStatus: "3",
			expectedDetected:  true,
			expectedSize:      3,
		},
		{
			name:             "PXC cluster with 3 nodes (fallback to variable)",
			wsrepOn:          "ON",
			clusterStatusErr: sql.ErrNoRows,
			clusterSizeVar:   "3",
			expectedDetected: true,
			expectedSize:     3,
		},
		{
			name:             "wsrep_on is OFF",
			wsrepOn:          "OFF",
			expectedDetected: false,
		},
		{
			name:             "wsrep_on not found",
			wsrepOnErr:       sql.ErrNoRows,
			expectedDetected: false,
		},
		{
			name:              "cluster size is 0",
			wsrepOn:           "ON",
			clusterSizeStatus: "0",
			expectedDetected:  false,
		},
		{
			name:              "cluster size is empty",
			wsrepOn:           "ON",
			clusterSizeStatus: "",
			clusterSizeVar:    "",
			expectedDetected:  false,
		},
		{
			name:             "cluster size status and variable both fail",
			wsrepOn:          "ON",
			clusterStatusErr: sql.ErrNoRows,
			clusterVarErr:    sql.ErrNoRows,
			expectedDetected: false,
		},
		{
			name:          "wsrep_on query fails with real error",
			wsrepOnErr:    fmt.Errorf("connection lost"),
			expectedError: true,
		},
		{
			name:             "cluster size status fails with real error",
			wsrepOn:          "ON",
			clusterStatusErr: fmt.Errorf("permission denied"),
			clusterVarErr:    sql.ErrNoRows,
			expectedError:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock: %v", err)
			}
			defer db.Close()

			// Mock wsrep_on variable query
			// Note: wsrep_on requires SHOW VARIABLES (not GLOBAL)
			if tt.wsrepOnErr != nil {
				// First GLOBAL attempt fails
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
					WillReturnError(sql.ErrNoRows)
				// Then non-GLOBAL attempt also fails
				mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
					WillReturnError(tt.wsrepOnErr)
			} else {
				if tt.wsrepOn != "" {
					// GLOBAL returns no rows (wsrep_on not available via GLOBAL)
					mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
						WillReturnError(sql.ErrNoRows)
					// Fallback to SHOW VARIABLES returns value
					rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_on", tt.wsrepOn)
					mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
						WillReturnRows(rows)
				} else {
					// Variable doesn't exist - both queries return no rows
					mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
						WillReturnError(sql.ErrNoRows)
					mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
						WillReturnError(sql.ErrNoRows)
				}
			}

			// Only expect cluster size queries if wsrep_on is ON and we didn't error yet
			if tt.wsrepOn == "ON" && !tt.expectedError {
				// Mock wsrep_cluster_size status query
				if tt.clusterStatusErr != nil {
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_cluster\\\\_size'").
						WillReturnError(tt.clusterStatusErr)

					// Fallback to variable query
					if tt.clusterVarErr != nil {
						// Both GLOBAL and non-GLOBAL attempts
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_cluster\\\\_size'").
							WillReturnError(sql.ErrNoRows)
						mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_cluster\\\\_size'").
							WillReturnError(tt.clusterVarErr)
					} else if tt.clusterSizeVar != "" {
						rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
							AddRow("wsrep_cluster_size", tt.clusterSizeVar)
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_cluster\\\\_size'").
							WillReturnRows(rows)
					} else {
						// Variable doesn't exist - both queries fail
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_cluster\\\\_size'").
							WillReturnError(sql.ErrNoRows)
						mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_cluster\\\\_size'").
							WillReturnError(sql.ErrNoRows)
					}
				} else if tt.clusterSizeStatus != "" {
					rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_cluster_size", tt.clusterSizeStatus)
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_cluster\\\\_size'").
						WillReturnRows(rows)
				} else {
					// Status doesn't exist, should fallback to variable
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_cluster\\\\_size'").
						WillReturnError(sql.ErrNoRows)

					if tt.clusterSizeVar != "" {
						rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
							AddRow("wsrep_cluster_size", tt.clusterSizeVar)
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_cluster\\\\_size'").
							WillReturnRows(rows)
					} else {
						// Both GLOBAL and non-GLOBAL queries return no rows
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_cluster\\\\_size'").
							WillReturnError(sql.ErrNoRows)
						mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_cluster\\\\_size'").
							WillReturnError(sql.ErrNoRows)
					}
				}

				// If detected, mock additional Galera info queries
				if tt.expectedDetected {
					// wsrep_local_state_comment status
					stateRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_local_state_comment", "Synced")
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_local\\\\_state\\\\_comment'").
						WillReturnRows(stateRows)

					// wsrep_OSU_method variable
					osuRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_OSU_method", "TOI")
					mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_OSU\\\\_method'").
						WillReturnRows(osuRows)

					// wsrep_max_ws_size variable
					maxWsRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_max_ws_size", "2147483647")
					mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_max\\\\_ws\\\\_size'").
						WillReturnRows(maxWsRows)

					// wsrep_flow_control_paused status
					fcRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_flow_control_paused", "0.0")
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_flow\\\\_control\\\\_paused'").
						WillReturnRows(fcRows)
				}
			}

			info := &Info{
				Version: mysql.ServerVersion{Major: 8, Minor: 0, Patch: 43},
			}

			detected, err := detectGalera(db, info, false)

			if tt.expectedError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("detectGalera returned unexpected error: %v", err)
			}

			if detected != tt.expectedDetected {
				t.Errorf("expected detected=%v, got %v", tt.expectedDetected, detected)
			}

			if tt.expectedDetected {
				if info.Type != Galera {
					t.Errorf("expected Type=Galera, got %s", info.Type)
				}
				if info.GaleraClusterSize != tt.expectedSize {
					t.Errorf("expected GaleraClusterSize=%d, got %d", tt.expectedSize, info.GaleraClusterSize)
				}
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestDetect_PXCCluster(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION() query
	versionRows := sqlmock.NewRows([]string{"VERSION()"}).
		AddRow("8.0.43-34.1-Percona XtraDB Cluster (GPL), Release rel34, Revision 0682ba7, WSREP version 26.1.4.3")
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(versionRows)

	// Mock read_only
	readOnlyRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("read_only", "OFF")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(readOnlyRows)

	// Mock super_read_only
	superReadOnlyRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("super_read_only", "OFF")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(superReadOnlyRows)

	// Mock wsrep_on (Galera detection)
	// wsrep_on requires SHOW VARIABLES (not GLOBAL)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	wsrepOnRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_on", "ON")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnRows(wsrepOnRows)

	// Mock wsrep_cluster_size status
	clusterSizeRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_cluster_size", "3")
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_cluster\\\\_size'").
		WillReturnRows(clusterSizeRows)

	// Mock additional Galera info
	stateRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_local_state_comment", "Synced")
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_local\\\\_state\\\\_comment'").
		WillReturnRows(stateRows)

	osuRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_OSU_method", "TOI")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_OSU\\\\_method'").
		WillReturnRows(osuRows)

	maxWsRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_max_ws_size", "2147483647")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_max\\\\_ws\\\\_size'").
		WillReturnRows(maxWsRows)

	fcRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_flow_control_paused", "0.0")
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_flow\\\\_control\\\\_paused'").
		WillReturnRows(fcRows)

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != Galera {
		t.Errorf("expected Type=Galera, got %s", info.Type)
	}

	if info.GaleraClusterSize != 3 {
		t.Errorf("expected GaleraClusterSize=3, got %d", info.GaleraClusterSize)
	}

	if info.Version.Flavor != "percona-xtradb-cluster" {
		t.Errorf("expected Flavor=percona-xtradb-cluster, got %s", info.Version.Flavor)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_VerboseLogging(t *testing.T) {
	// Capture log output
	var logBuf strings.Builder
	log.SetOutput(&logBuf)
	defer log.SetOutput(os.Stderr)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock version_comment query (new debug query)
	versionCommentRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("version_comment", "Percona XtraDB Cluster (GPL), Release rel34, Revision 0682ba7, WSREP version 26.1.4.3")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'version\\\\_comment'").
		WillReturnRows(versionCommentRows)

	// Mock wsrep_on query
	// wsrep_on requires SHOW VARIABLES (not GLOBAL)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	wsrepOnRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_on", "ON")
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnRows(wsrepOnRows)

	// Mock wsrep_cluster_size status
	clusterSizeRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_cluster_size", "3")
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_cluster\\\\_size'").
		WillReturnRows(clusterSizeRows)

	// Mock additional Galera info
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_local\\\\_state\\\\_comment'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("wsrep_local_state_comment", "Synced"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_OSU\\\\_method'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("wsrep_OSU_method", "TOI"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_max\\\\_ws\\\\_size'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("wsrep_max_ws_size", "2147483647"))
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'wsrep\\\\_flow\\\\_control\\\\_paused'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("wsrep_flow_control_paused", "0.0"))

	info := &Info{
		Version: mysql.ServerVersion{Major: 8, Minor: 0, Patch: 43},
	}

	// Call with verbose=true
	detected, err := detectGalera(db, info, true)
	if err != nil {
		t.Fatalf("detectGalera returned error: %v", err)
	}

	if !detected {
		t.Errorf("expected detected=true, got false")
	}

	// Verify debug output was generated
	logOutput := logBuf.String()
	if !strings.Contains(logOutput, "[DEBUG]") {
		t.Errorf("expected verbose debug output, but got none. Output: %s", logOutput)
	}

	if !strings.Contains(logOutput, "version_comment") {
		t.Errorf("expected debug output to mention version_comment, but it doesn't. Output: %s", logOutput)
	}

	if !strings.Contains(logOutput, "wsrep_on") {
		t.Errorf("expected debug output to mention wsrep_on, but it doesn't. Output: %s", logOutput)
	}

	if !strings.Contains(logOutput, "Galera/PXC detected") {
		t.Errorf("expected debug output to mention Galera detection, but it doesn't. Output: %s", logOutput)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

// TestGetVariable_ActualQuery tests GetVariable with real query to ensure proper scanning
func TestGetVariable_ActualQuery(t *testing.T) {
	tests := []struct {
		name          string
		varName       string
		mockValue     string
		expectedValue string
		globalWorks   bool // true if SHOW GLOBAL VARIABLES returns value
	}{
		{
			name:          "wsrep_on from SHOW VARIABLES (not GLOBAL)",
			varName:       "wsrep_on",
			mockValue:     "ON",
			expectedValue: "ON",
			globalWorks:   false, // wsrep_on requires non-GLOBAL query
		},
		{
			name:          "version_comment from GLOBAL",
			varName:       "version_comment",
			mockValue:     "Percona XtraDB Cluster (GPL), Release rel34",
			expectedValue: "Percona XtraDB Cluster (GPL), Release rel34",
			globalWorks:   true,
		},
		{
			name:          "numeric value from GLOBAL",
			varName:       "max_connections",
			mockValue:     "151",
			expectedValue: "151",
			globalWorks:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock: %v", err)
			}
			defer db.Close()

			// Escape variable name for regex matching (4 backslashes per underscore)
			escapedName := strings.ReplaceAll(tt.varName, "_", "\\\\_")

			if tt.globalWorks {
				// SHOW GLOBAL VARIABLES returns the value
				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow(tt.varName, tt.mockValue)
				mock.ExpectQuery(fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", escapedName)).
					WillReturnRows(rows)
			} else {
				// SHOW GLOBAL VARIABLES returns no rows, fallback to SHOW VARIABLES
				mock.ExpectQuery(fmt.Sprintf("SHOW GLOBAL VARIABLES LIKE '%s'", escapedName)).
					WillReturnError(sql.ErrNoRows)

				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow(tt.varName, tt.mockValue)
				mock.ExpectQuery(fmt.Sprintf("SHOW VARIABLES LIKE '%s'", escapedName)).
					WillReturnRows(rows)
			}

			value, err := mysql.GetVariable(db, tt.varName)
			if err != nil {
				t.Fatalf("GetVariable returned error: %v", err)
			}

			if value != tt.expectedValue {
				t.Errorf("expected value %q, got %q", tt.expectedValue, value)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestDetect_Standalone(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION()
	versionRows := sqlmock.NewRows([]string{"VERSION()"}).
		AddRow("8.0.43")
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(versionRows)

	// Mock read_only
	readOnlyRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("read_only", "OFF")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(readOnlyRows)

	// Mock super_read_only
	superReadOnlyRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("super_read_only", "OFF")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(superReadOnlyRows)

	// Mock wsrep_on - doesn't exist on standalone
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)

	// Mock group_replication_group_name - doesn't exist
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_group\\\\_name'").
		WillReturnError(sql.ErrNoRows)

	// Mock SHOW REPLICA STATUS - not a replica
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnError(fmt.Errorf("no replica status"))

	// Mock SHOW SLAVE STATUS (fallback) - not a replica
	mock.ExpectQuery("SHOW SLAVE STATUS").
		WillReturnError(fmt.Errorf("no slave status"))

	// Mock processlist for primary detection
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema\\.PROCESSLIST").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	// Mock basedir (RDS detection)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'basedir'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("basedir", "/usr/"))

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != Standalone {
		t.Errorf("expected Type=Standalone, got %s", info.Type)
	}
	if info.IsCloudManaged {
		t.Error("expected IsCloudManaged=false for local MySQL")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_AuroraWriter(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION() — Aurora format
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.mysql_aurora.3.04.0"))

	// Mock read_only = OFF (writer)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "OFF"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "OFF"))

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != AuroraWriter {
		t.Errorf("expected Type=AuroraWriter, got %s", info.Type)
	}
	if !info.IsCloudManaged {
		t.Error("expected IsCloudManaged=true")
	}
	if info.CloudProvider != "aws-aurora" {
		t.Errorf("expected CloudProvider=aws-aurora, got %s", info.CloudProvider)
	}
	if info.Version.AuroraVersion != "3.04.0" {
		t.Errorf("expected AuroraVersion=3.04.0, got %s", info.Version.AuroraVersion)
	}
	if info.ReadOnly {
		t.Error("expected ReadOnly=false for writer")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_AuroraReader(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION() — Aurora format
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.mysql_aurora.3.07.1"))

	// Mock read_only = ON (reader/replica)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "ON"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "ON"))

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != AuroraReader {
		t.Errorf("expected Type=AuroraReader, got %s", info.Type)
	}
	if !info.IsCloudManaged {
		t.Error("expected IsCloudManaged=true")
	}
	if info.CloudProvider != "aws-aurora" {
		t.Errorf("expected CloudProvider=aws-aurora, got %s", info.CloudProvider)
	}
	if info.Version.AuroraVersion != "3.07.1" {
		t.Errorf("expected AuroraVersion=3.07.1, got %s", info.Version.AuroraVersion)
	}
	if !info.ReadOnly {
		t.Error("expected ReadOnly=true for Aurora reader")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_RDS_Standalone(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION() — standard MySQL (not Aurora)
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.35"))

	// Mock read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "OFF"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "OFF"))

	// Mock wsrep_on - not Galera
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)

	// Mock group_replication - not GR
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_group\\\\_name'").
		WillReturnError(sql.ErrNoRows)

	// Mock SHOW REPLICA STATUS - not a replica
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnError(fmt.Errorf("not a replica"))
	mock.ExpectQuery("SHOW SLAVE STATUS").
		WillReturnError(fmt.Errorf("not a replica"))

	// Mock processlist - no binlog dump threads
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.PROCESSLIST").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	// Mock basedir — RDS marker
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'basedir'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("basedir", "/rdsdbbin/mysql/"))

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != Standalone {
		t.Errorf("expected Type=Standalone, got %s", info.Type)
	}
	if !info.IsCloudManaged {
		t.Error("expected IsCloudManaged=true for RDS")
	}
	if info.CloudProvider != "aws-rds" {
		t.Errorf("expected CloudProvider=aws-rds, got %s", info.CloudProvider)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_GroupReplication_SinglePrimary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION()
	versionRows := sqlmock.NewRows([]string{"VERSION()"}).
		AddRow("8.0.35")
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(versionRows)

	// Mock read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "ON"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "ON"))

	// Mock wsrep_on - not Galera
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)

	// Mock Group Replication detection
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_group\\\\_name'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("group_replication_group_name", "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"))

	// Mock single primary mode
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_single\\\\_primary\\\\_mode'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("group_replication_single_primary_mode", "ON"))

	// Mock transaction size limit
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_transaction\\\\_size\\\\_limit'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("group_replication_transaction_size_limit", "150000000"))

	// Mock member role query
	memberRows := sqlmock.NewRows([]string{"MEMBER_ROLE", "MEMBER_STATE"}).
		AddRow("SECONDARY", "ONLINE")
	mock.ExpectQuery("SELECT MEMBER_ROLE, MEMBER_STATE FROM performance_schema.replication_group_members").
		WillReturnRows(memberRows)

	// Mock member count query
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM performance_schema.replication_group_members").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(3))

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != GroupRepl {
		t.Errorf("expected Type=GroupRepl, got %s", info.Type)
	}
	if info.GRMode != "SINGLE-PRIMARY" {
		t.Errorf("expected GRMode=SINGLE-PRIMARY, got %s", info.GRMode)
	}
	if info.GRMemberRole != "SECONDARY" {
		t.Errorf("expected GRMemberRole=SECONDARY, got %s", info.GRMemberRole)
	}
	if info.GRMemberCount != 3 {
		t.Errorf("expected GRMemberCount=3, got %d", info.GRMemberCount)
	}
	if info.GRTransactionLimit != 150000000 {
		t.Errorf("expected GRTransactionLimit=150000000, got %d", info.GRTransactionLimit)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_GroupReplication_MultiPrimary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION()
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.35"))

	// Mock read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "OFF"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "OFF"))

	// Mock wsrep_on - not Galera
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)

	// Mock Group Replication - multi-primary
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_group\\\\_name'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("group_replication_group_name", "test-group"))

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_single\\\\_primary\\\\_mode'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("group_replication_single_primary_mode", "OFF"))

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_transaction\\\\_size\\\\_limit'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("group_replication_transaction_size_limit", "150000000"))

	// Mock member role - PRIMARY in multi-primary
	mock.ExpectQuery("SELECT MEMBER_ROLE, MEMBER_STATE FROM performance_schema.replication_group_members").
		WillReturnRows(sqlmock.NewRows([]string{"MEMBER_ROLE", "MEMBER_STATE"}).
			AddRow("PRIMARY", "ONLINE"))

	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM performance_schema.replication_group_members").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(5))

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != GroupRepl {
		t.Errorf("expected Type=GroupRepl, got %s", info.Type)
	}
	if info.GRMode != "MULTI-PRIMARY" {
		t.Errorf("expected GRMode=MULTI-PRIMARY, got %s", info.GRMode)
	}
	if info.GRMemberRole != "PRIMARY" {
		t.Errorf("expected GRMemberRole=PRIMARY, got %s", info.GRMemberRole)
	}
	if info.GRMemberCount != 5 {
		t.Errorf("expected GRMemberCount=5, got %d", info.GRMemberCount)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_AsyncReplication_Replica(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION()
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.35"))

	// Mock read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "ON"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "ON"))

	// Mock wsrep_on - not Galera
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)

	// Mock group_replication_group_name - not Group Replication
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_group\\\\_name'").
		WillReturnError(sql.ErrNoRows)

	// Mock SHOW REPLICA STATUS - is a replica
	replicaRows := sqlmock.NewRows([]string{
		"Replica_IO_State", "Source_Host", "Source_User", "Source_Port",
		"Connect_Retry", "Source_Log_File", "Read_Source_Log_Pos",
		"Relay_Log_File", "Relay_Log_Pos", "Relay_Source_Log_File",
		"Replica_IO_Running", "Replica_SQL_Running", "Seconds_Behind_Source",
	}).AddRow(
		"Waiting for source to send event", "primary.example.com", "repl", 3306,
		60, "mysql-bin.000001", 12345,
		"relay-bin.000001", 6789, "mysql-bin.000001",
		"Yes", "Yes", "5",
	)
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(replicaRows)

	// Mock processlist check (no binlog dump threads)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.PROCESSLIST").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	// Mock semi-sync check
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_source\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_source\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_master\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_master\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != AsyncReplica {
		t.Errorf("expected Type=AsyncReplica, got %s", info.Type)
	}
	if !info.IsReplica {
		t.Error("expected IsReplica=true")
	}
	if info.IsPrimary {
		t.Error("expected IsPrimary=false")
	}
	if info.ReplicaLagSecs == nil {
		t.Error("expected ReplicaLagSecs to be set")
	} else if *info.ReplicaLagSecs != 5 {
		t.Errorf("expected ReplicaLagSecs=5, got %d", *info.ReplicaLagSecs)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_AsyncReplication_Primary(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION()
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.35"))

	// Mock read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "OFF"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "OFF"))

	// Mock wsrep_on - not Galera
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)

	// Mock group_replication_group_name - not GR
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_group\\\\_name'").
		WillReturnError(sql.ErrNoRows)

	// Mock SHOW REPLICA STATUS - not a replica (returns no rows)
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnError(fmt.Errorf("not a replica"))

	// Mock SHOW SLAVE STATUS fallback - also not a replica
	mock.ExpectQuery("SHOW SLAVE STATUS").
		WillReturnError(fmt.Errorf("not a replica"))

	// Mock processlist check - has binlog dump threads (is a primary)
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.PROCESSLIST").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(2))

	// Mock semi-sync check - not enabled
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_source\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_source\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_master\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_master\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != AsyncReplica {
		t.Errorf("expected Type=AsyncReplica, got %s", info.Type)
	}
	if info.IsReplica {
		t.Error("expected IsReplica=false")
	}
	if !info.IsPrimary {
		t.Error("expected IsPrimary=true")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_SemiSyncReplication(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION()
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("8.0.35"))

	// Mock read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "ON"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "ON"))

	// Mock wsrep_on - not Galera
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)

	// Mock group_replication - not GR
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_group\\\\_name'").
		WillReturnError(sql.ErrNoRows)

	// Mock SHOW REPLICA STATUS - is a replica (MySQL 8.0.22+)
	replicaRows := sqlmock.NewRows([]string{
		"Replica_IO_Running", "Replica_SQL_Running", "Seconds_Behind_Source",
	}).AddRow("Yes", "Yes", "0")
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnRows(replicaRows)

	// Mock processlist
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.PROCESSLIST").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	// Mock semi-sync - source enabled
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_source\\\\_enabled'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("rpl_semi_sync_source_enabled", "ON"))

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != SemiSyncReplica {
		t.Errorf("expected Type=SemiSyncReplica, got %s", info.Type)
	}
	if !info.IsReplica {
		t.Error("expected IsReplica=true")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestDetect_SemiSync_OldSyntax(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	// Mock VERSION()
	mock.ExpectQuery("SELECT VERSION\\(\\)").
		WillReturnRows(sqlmock.NewRows([]string{"VERSION()"}).AddRow("5.7.40"))

	// Mock read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("read_only", "ON"))

	// Mock super_read_only
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'super\\\\_read\\\\_only'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("super_read_only", "ON"))

	// Mock wsrep_on - not Galera
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
		WillReturnError(sql.ErrNoRows)

	// Mock group_replication - not GR
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'group\\\\_replication\\\\_group\\\\_name'").
		WillReturnError(sql.ErrNoRows)

	// Mock SHOW REPLICA STATUS - not supported in MySQL 5.7
	mock.ExpectQuery("SHOW REPLICA STATUS").
		WillReturnError(fmt.Errorf("unknown command"))

	// Mock SHOW SLAVE STATUS fallback - is a replica
	slaveRows := sqlmock.NewRows([]string{
		"Slave_IO_Running", "Slave_SQL_Running", "Seconds_Behind_Master",
	}).AddRow("Yes", "Yes", "2")
	mock.ExpectQuery("SHOW SLAVE STATUS").
		WillReturnRows(slaveRows)

	// Mock processlist
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.PROCESSLIST").
		WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	// Mock semi-sync - source not available, use master
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_source\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_source\\\\_enabled'").
		WillReturnError(sql.ErrNoRows)

	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'rpl\\\\_semi\\\\_sync\\\\_master\\\\_enabled'").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).
			AddRow("rpl_semi_sync_master_enabled", "ON"))

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != SemiSyncReplica {
		t.Errorf("expected Type=SemiSyncReplica, got %s", info.Type)
	}
	if !info.IsReplica {
		t.Error("expected IsReplica=true")
	}
	if info.ReplicaLagSecs == nil {
		t.Error("expected ReplicaLagSecs to be set")
	} else if *info.ReplicaLagSecs != 2 {
		t.Errorf("expected ReplicaLagSecs=2, got %d", *info.ReplicaLagSecs)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
