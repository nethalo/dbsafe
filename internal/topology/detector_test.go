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
			name:              "PXC cluster with 3 nodes (fallback to variable)",
			wsrepOn:           "ON",
			clusterStatusErr:  sql.ErrNoRows,
			clusterSizeVar:    "3",
			expectedDetected:  true,
			expectedSize:      3,
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
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
					WithArgs("wsrep_on").
					WillReturnError(sql.ErrNoRows)
				// Then non-GLOBAL attempt also fails
				mock.ExpectQuery("SHOW VARIABLES LIKE ?").
					WithArgs("wsrep_on").
					WillReturnError(tt.wsrepOnErr)
			} else {
				if tt.wsrepOn != "" {
					// GLOBAL returns no rows (wsrep_on not available via GLOBAL)
					mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
						WithArgs("wsrep_on").
						WillReturnError(sql.ErrNoRows)
					// Fallback to SHOW VARIABLES returns value
					rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_on", tt.wsrepOn)
					mock.ExpectQuery("SHOW VARIABLES LIKE ?").
						WithArgs("wsrep_on").
						WillReturnRows(rows)
				} else {
					// Variable doesn't exist - both queries return no rows
					mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
						WithArgs("wsrep_on").
						WillReturnError(sql.ErrNoRows)
					mock.ExpectQuery("SHOW VARIABLES LIKE ?").
						WithArgs("wsrep_on").
						WillReturnError(sql.ErrNoRows)
				}
			}

			// Only expect cluster size queries if wsrep_on is ON and we didn't error yet
			if tt.wsrepOn == "ON" && !tt.expectedError {
				// Mock wsrep_cluster_size status query
				if tt.clusterStatusErr != nil {
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
						WithArgs("wsrep_cluster_size").
						WillReturnError(tt.clusterStatusErr)

					// Fallback to variable query
					if tt.clusterVarErr != nil {
						// Both GLOBAL and non-GLOBAL attempts
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
							WithArgs("wsrep_cluster_size").
							WillReturnError(sql.ErrNoRows)
						mock.ExpectQuery("SHOW VARIABLES LIKE ?").
							WithArgs("wsrep_cluster_size").
							WillReturnError(tt.clusterVarErr)
					} else if tt.clusterSizeVar != "" {
						rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
							AddRow("wsrep_cluster_size", tt.clusterSizeVar)
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
							WithArgs("wsrep_cluster_size").
							WillReturnRows(rows)
					} else {
						// Variable doesn't exist - both queries fail
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
							WithArgs("wsrep_cluster_size").
							WillReturnError(sql.ErrNoRows)
						mock.ExpectQuery("SHOW VARIABLES LIKE ?").
							WithArgs("wsrep_cluster_size").
							WillReturnError(sql.ErrNoRows)
					}
				} else if tt.clusterSizeStatus != "" {
					rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_cluster_size", tt.clusterSizeStatus)
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
						WithArgs("wsrep_cluster_size").
						WillReturnRows(rows)
				} else {
					// Status doesn't exist, should fallback to variable
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
						WithArgs("wsrep_cluster_size").
						WillReturnError(sql.ErrNoRows)

					if tt.clusterSizeVar != "" {
						rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
							AddRow("wsrep_cluster_size", tt.clusterSizeVar)
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
							WithArgs("wsrep_cluster_size").
							WillReturnRows(rows)
					} else {
						// Both GLOBAL and non-GLOBAL queries return no rows
						mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
							WithArgs("wsrep_cluster_size").
							WillReturnError(sql.ErrNoRows)
						mock.ExpectQuery("SHOW VARIABLES LIKE ?").
							WithArgs("wsrep_cluster_size").
							WillReturnError(sql.ErrNoRows)
					}
				}

				// If detected, mock additional Galera info queries
				if tt.expectedDetected {
					// wsrep_local_state_comment status
					stateRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_local_state_comment", "Synced")
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
						WithArgs("wsrep_local_state_comment").
						WillReturnRows(stateRows)

					// wsrep_OSU_method variable
					osuRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_OSU_method", "TOI")
					mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
						WithArgs("wsrep_OSU_method").
						WillReturnRows(osuRows)

					// wsrep_max_ws_size variable
					maxWsRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_max_ws_size", "2147483647")
					mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
						WithArgs("wsrep_max_ws_size").
						WillReturnRows(maxWsRows)

					// wsrep_flow_control_paused status
					fcRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
						AddRow("wsrep_flow_control_paused", "0.0")
					mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
						WithArgs("wsrep_flow_control_paused").
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
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("read_only").
		WillReturnRows(readOnlyRows)

	// Mock super_read_only
	superReadOnlyRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("super_read_only", "OFF")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("super_read_only").
		WillReturnRows(superReadOnlyRows)

	// Mock wsrep_on (Galera detection)
	// wsrep_on requires SHOW VARIABLES (not GLOBAL)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnError(sql.ErrNoRows)
	wsrepOnRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_on", "ON")
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnRows(wsrepOnRows)

	// Mock wsrep_cluster_size status
	clusterSizeRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_cluster_size", "3")
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
		WithArgs("wsrep_cluster_size").
		WillReturnRows(clusterSizeRows)

	// Mock additional Galera info
	stateRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_local_state_comment", "Synced")
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
		WithArgs("wsrep_local_state_comment").
		WillReturnRows(stateRows)

	osuRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_OSU_method", "TOI")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("wsrep_OSU_method").
		WillReturnRows(osuRows)

	maxWsRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_max_ws_size", "2147483647")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("wsrep_max_ws_size").
		WillReturnRows(maxWsRows)

	fcRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_flow_control_paused", "0.0")
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
		WithArgs("wsrep_flow_control_paused").
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
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("version_comment").
		WillReturnRows(versionCommentRows)

	// Mock wsrep_on query
	// wsrep_on requires SHOW VARIABLES (not GLOBAL)
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnError(sql.ErrNoRows)
	wsrepOnRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_on", "ON")
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnRows(wsrepOnRows)

	// Mock wsrep_cluster_size status
	clusterSizeRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("wsrep_cluster_size", "3")
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
		WithArgs("wsrep_cluster_size").
		WillReturnRows(clusterSizeRows)

	// Mock additional Galera info
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
		WithArgs("wsrep_local_state_comment").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("wsrep_local_state_comment", "Synced"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("wsrep_OSU_method").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("wsrep_OSU_method", "TOI"))
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("wsrep_max_ws_size").
		WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}).AddRow("wsrep_max_ws_size", "2147483647"))
	mock.ExpectQuery("SHOW GLOBAL STATUS LIKE ?").
		WithArgs("wsrep_flow_control_paused").
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

			if tt.globalWorks {
				// SHOW GLOBAL VARIABLES returns the value
				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow(tt.varName, tt.mockValue)
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
					WithArgs(tt.varName).
					WillReturnRows(rows)
			} else {
				// SHOW GLOBAL VARIABLES returns no rows, fallback to SHOW VARIABLES
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
					WithArgs(tt.varName).
					WillReturnError(sql.ErrNoRows)

				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow(tt.varName, tt.mockValue)
				mock.ExpectQuery("SHOW VARIABLES LIKE ?").
					WithArgs(tt.varName).
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
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("read_only").
		WillReturnRows(readOnlyRows)

	// Mock super_read_only
	superReadOnlyRows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
		AddRow("super_read_only", "OFF")
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("super_read_only").
		WillReturnRows(superReadOnlyRows)

	// Mock wsrep_on - doesn't exist on standalone
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectQuery("SHOW VARIABLES LIKE ?").
		WithArgs("wsrep_on").
		WillReturnError(sql.ErrNoRows)

	// Mock group_replication_group_name - doesn't exist
	mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE ?").
		WithArgs("group_replication_group_name").
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

	info, err := Detect(db, false)
	if err != nil {
		t.Fatalf("Detect returned error: %v", err)
	}

	if info.Type != Standalone {
		t.Errorf("expected Type=Standalone, got %s", info.Type)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
