package mysql

import (
	"database/sql"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestParseVersion(t *testing.T) {
	tests := []struct {
		name       string
		raw        string
		wantMajor  int
		wantMinor  int
		wantPatch  int
		wantFlavor string
		wantIsLTS  bool
		wantErr    bool
	}{
		{
			name:       "MySQL 8.0.35",
			raw:        "8.0.35",
			wantMajor:  8,
			wantMinor:  0,
			wantPatch:  35,
			wantFlavor: "mysql",
			wantIsLTS:  false,
		},
		{
			name:       "MySQL 8.4.0 LTS",
			raw:        "8.4.0",
			wantMajor:  8,
			wantMinor:  4,
			wantPatch:  0,
			wantFlavor: "mysql",
			wantIsLTS:  true,
		},
		{
			name:       "Percona Server",
			raw:        "8.0.28-19-Percona Server",
			wantMajor:  8,
			wantMinor:  0,
			wantPatch:  28,
			wantFlavor: "percona",
			wantIsLTS:  false,
		},
		{
			name:       "Percona XtraDB Cluster",
			raw:        "8.0.35-27-Percona XtraDB Cluster",
			wantMajor:  8,
			wantMinor:  0,
			wantPatch:  35,
			wantFlavor: "percona-xtradb-cluster",
			wantIsLTS:  false,
		},
		{
			name:       "MariaDB",
			raw:        "10.11.6-MariaDB",
			wantMajor:  10,
			wantMinor:  11,
			wantPatch:  6,
			wantFlavor: "mariadb",
			wantIsLTS:  false,
		},
		{
			name:    "invalid version",
			raw:     "not-a-version",
			wantErr: true,
		},
		{
			name:    "incomplete version",
			raw:     "8.0",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v, err := ParseVersion(tt.raw)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if v.Major != tt.wantMajor {
				t.Errorf("Major = %d, want %d", v.Major, tt.wantMajor)
			}
			if v.Minor != tt.wantMinor {
				t.Errorf("Minor = %d, want %d", v.Minor, tt.wantMinor)
			}
			if v.Patch != tt.wantPatch {
				t.Errorf("Patch = %d, want %d", v.Patch, tt.wantPatch)
			}
			if v.Flavor != tt.wantFlavor {
				t.Errorf("Flavor = %q, want %q", v.Flavor, tt.wantFlavor)
			}
			if v.IsLTS != tt.wantIsLTS {
				t.Errorf("IsLTS = %v, want %v", v.IsLTS, tt.wantIsLTS)
			}
			if v.Raw != tt.raw {
				t.Errorf("Raw = %q, want %q", v.Raw, tt.raw)
			}
		})
	}
}

func TestServerVersion_AtLeast(t *testing.T) {
	v := ServerVersion{Major: 8, Minor: 0, Patch: 35}

	tests := []struct {
		name        string
		major       int
		minor       int
		patch       int
		wantAtLeast bool
	}{
		{"exact match", 8, 0, 35, true},
		{"lower patch", 8, 0, 30, true},
		{"higher patch", 8, 0, 40, false},
		{"lower minor", 8, 0, 0, true},
		{"higher minor", 8, 1, 0, false},
		{"lower major", 7, 9, 99, true},
		{"higher major", 9, 0, 0, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := v.AtLeast(tt.major, tt.minor, tt.patch)
			if got != tt.wantAtLeast {
				t.Errorf("AtLeast(%d, %d, %d) = %v, want %v",
					tt.major, tt.minor, tt.patch, got, tt.wantAtLeast)
			}
		})
	}
}

func TestServerVersion_FeatureSupport(t *testing.T) {
	tests := []struct {
		name              string
		version           ServerVersion
		wantInstantAdd    bool
		wantInstantAnyPos bool
		wantInstantDrop   bool
	}{
		{
			name:              "8.0.5 - old version",
			version:           ServerVersion{Major: 8, Minor: 0, Patch: 5},
			wantInstantAdd:    false,
			wantInstantAnyPos: false,
			wantInstantDrop:   false,
		},
		{
			name:              "8.0.12 - instant add only",
			version:           ServerVersion{Major: 8, Minor: 0, Patch: 12},
			wantInstantAdd:    true,
			wantInstantAnyPos: false,
			wantInstantDrop:   false,
		},
		{
			name:              "8.0.28 - just before any position",
			version:           ServerVersion{Major: 8, Minor: 0, Patch: 28},
			wantInstantAdd:    true,
			wantInstantAnyPos: false,
			wantInstantDrop:   false,
		},
		{
			name:              "8.0.29 - all instant features",
			version:           ServerVersion{Major: 8, Minor: 0, Patch: 29},
			wantInstantAdd:    true,
			wantInstantAnyPos: true,
			wantInstantDrop:   true,
		},
		{
			name:              "8.4.0 - latest LTS",
			version:           ServerVersion{Major: 8, Minor: 4, Patch: 0},
			wantInstantAdd:    true,
			wantInstantAnyPos: true,
			wantInstantDrop:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.version.SupportsInstantAddColumn(); got != tt.wantInstantAdd {
				t.Errorf("SupportsInstantAddColumn() = %v, want %v", got, tt.wantInstantAdd)
			}
			if got := tt.version.SupportsInstantAnyPosition(); got != tt.wantInstantAnyPos {
				t.Errorf("SupportsInstantAnyPosition() = %v, want %v", got, tt.wantInstantAnyPos)
			}
			if got := tt.version.SupportsInstantDropColumn(); got != tt.wantInstantDrop {
				t.Errorf("SupportsInstantDropColumn() = %v, want %v", got, tt.wantInstantDrop)
			}
		})
	}
}

func TestServerVersion_String(t *testing.T) {
	v := ServerVersion{Major: 8, Minor: 0, Patch: 35, Flavor: "percona-xtradb-cluster"}
	want := "8.0.35 (percona-xtradb-cluster)"
	if got := v.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

func TestGetServerVersion(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name       string
		setupMock  func()
		wantMajor  int
		wantMinor  int
		wantPatch  int
		wantFlavor string
		wantErr    bool
	}{
		{
			name: "success - MySQL",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"VERSION()"}).
					AddRow("8.0.35")
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(rows)
			},
			wantMajor:  8,
			wantMinor:  0,
			wantPatch:  35,
			wantFlavor: "mysql",
		},
		{
			name: "success - Percona XtraDB Cluster",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"VERSION()"}).
					AddRow("8.0.35-27-Percona XtraDB Cluster")
				mock.ExpectQuery("SELECT VERSION()").WillReturnRows(rows)
			},
			wantMajor:  8,
			wantMinor:  0,
			wantPatch:  35,
			wantFlavor: "percona-xtradb-cluster",
		},
		{
			name: "query error",
			setupMock: func() {
				mock.ExpectQuery("SELECT VERSION()").WillReturnError(sql.ErrConnDone)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			v, err := GetServerVersion(db)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if v.Major != tt.wantMajor {
				t.Errorf("Major = %d, want %d", v.Major, tt.wantMajor)
			}
			if v.Minor != tt.wantMinor {
				t.Errorf("Minor = %d, want %d", v.Minor, tt.wantMinor)
			}
			if v.Patch != tt.wantPatch {
				t.Errorf("Patch = %d, want %d", v.Patch, tt.wantPatch)
			}
			if v.Flavor != tt.wantFlavor {
				t.Errorf("Flavor = %q, want %q", v.Flavor, tt.wantFlavor)
			}
		})
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}

func TestGetVariable(t *testing.T) {
	tests := []struct {
		name      string
		varName   string
		setupMock func(mock sqlmock.Sqlmock)
		wantValue string
		wantErr   bool
	}{
		{
			name:    "found with GLOBAL",
			varName: "max_connections",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow("max_connections", "151")
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max\\\\_connections'").
					WillReturnRows(rows)
			},
			wantValue: "151",
		},
		{
			name:    "found without GLOBAL (wsrep_on case)",
			varName: "wsrep_on",
			setupMock: func(mock sqlmock.Sqlmock) {
				// First try with GLOBAL returns no rows
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'wsrep\\\\_on'").
					WillReturnRows(sqlmock.NewRows([]string{"Variable_name", "Value"}))
				// Second try without GLOBAL succeeds
				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow("wsrep_on", "ON")
				mock.ExpectQuery("SHOW VARIABLES LIKE 'wsrep\\\\_on'").
					WillReturnRows(rows)
			},
			wantValue: "ON",
		},
		{
			name:    "variable not found",
			varName: "nonexistent_var",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'nonexistent\\\\_var'").
					WillReturnError(sql.ErrNoRows)
				mock.ExpectQuery("SHOW VARIABLES LIKE 'nonexistent\\\\_var'").
					WillReturnError(sql.ErrNoRows)
			},
			wantValue: "",
		},
		{
			name:    "NULL value from GLOBAL, empty from non-GLOBAL",
			varName: "some_var",
			setupMock: func(mock sqlmock.Sqlmock) {
				// GLOBAL returns NULL
				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow("some_var", nil)
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'some\\\\_var'").
					WillReturnRows(rows)
				// Non-GLOBAL also returns empty/NULL
				rows2 := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow("some_var", nil)
				mock.ExpectQuery("SHOW VARIABLES LIKE 'some\\\\_var'").
					WillReturnRows(rows2)
			},
			wantValue: "",
		},
		{
			name:    "query error",
			varName: "error_var",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'error\\\\_var'").
					WillReturnError(sql.ErrConnDone)
				mock.ExpectQuery("SHOW VARIABLES LIKE 'error\\\\_var'").
					WillReturnError(sql.ErrConnDone)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			got, err := GetVariable(db, tt.varName)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tt.wantValue {
				t.Errorf("GetVariable() = %q, want %q", got, tt.wantValue)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestGetStatus(t *testing.T) {
	tests := []struct {
		name      string
		varName   string
		setupMock func(mock sqlmock.Sqlmock)
		wantValue string
		wantErr   bool
	}{
		{
			name:    "found",
			varName: "Threads_connected",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow("Threads_connected", "42")
				mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'Threads\\\\_connected'").
					WillReturnRows(rows)
			},
			wantValue: "42",
		},
		{
			name:    "not found",
			varName: "nonexistent",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'nonexistent'").
					WillReturnError(sql.ErrNoRows)
			},
			wantValue: "",
		},
		{
			name:    "query error",
			varName: "error_status",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW GLOBAL STATUS LIKE 'error\\\\_status'").
					WillReturnError(sql.ErrConnDone)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			got, err := GetStatus(db, tt.varName)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tt.wantValue {
				t.Errorf("GetStatus() = %q, want %q", got, tt.wantValue)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestGetVariableInt(t *testing.T) {
	tests := []struct {
		name      string
		varName   string
		setupMock func(mock sqlmock.Sqlmock)
		want      int64
		wantErr   bool
	}{
		{
			name:    "valid integer",
			varName: "max_connections",
			setupMock: func(mock sqlmock.Sqlmock) {
				rows := sqlmock.NewRows([]string{"Variable_name", "Value"}).
					AddRow("max_connections", "151")
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'max\\\\_connections'").
					WillReturnRows(rows)
			},
			want: 151,
		},
		{
			name:    "variable not found returns 0",
			varName: "nonexistent",
			setupMock: func(mock sqlmock.Sqlmock) {
				mock.ExpectQuery("SHOW GLOBAL VARIABLES LIKE 'nonexistent'").
					WillReturnError(sql.ErrNoRows)
				mock.ExpectQuery("SHOW VARIABLES LIKE 'nonexistent'").
					WillReturnError(sql.ErrNoRows)
			},
			want: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("failed to create mock: %v", err)
			}
			defer db.Close()

			tt.setupMock(mock)

			got, err := GetVariableInt(db, tt.varName)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tt.want {
				t.Errorf("GetVariableInt() = %d, want %d", got, tt.want)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Errorf("unfulfilled expectations: %v", err)
			}
		})
	}
}

func TestEstimateRowsAffected(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create mock: %v", err)
	}
	defer db.Close()

	tests := []struct {
		name      string
		sql       string
		setupMock func()
		want      int64
		wantErr   bool
	}{
		{
			name: "single row result",
			sql:  "DELETE FROM users WHERE id = 1",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"id", "select_type", "table", "type", "possible_keys", "key", "key_len", "ref", "rows", "Extra"}).
					AddRow(1, "SIMPLE", "users", "range", "PRIMARY", "PRIMARY", "4", nil, 100, "Using where")
				mock.ExpectQuery("EXPLAIN DELETE FROM users WHERE id = 1").
					WillReturnRows(rows)
			},
			want: 100,
		},
		{
			name: "multiple rows - use max",
			sql:  "DELETE FROM orders WHERE user_id IN (SELECT id FROM users WHERE status = 'inactive')",
			setupMock: func() {
				rows := sqlmock.NewRows([]string{"id", "select_type", "table", "rows"}).
					AddRow(1, "PRIMARY", "orders", 500).
					AddRow(2, "SUBQUERY", "users", 1000)
				mock.ExpectQuery("EXPLAIN DELETE FROM orders").
					WillReturnRows(rows)
			},
			want: 1000,
		},
		{
			name: "query error",
			sql:  "DELETE FROM invalid",
			setupMock: func() {
				mock.ExpectQuery("EXPLAIN DELETE FROM invalid").
					WillReturnError(sql.ErrConnDone)
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.setupMock()

			got, err := EstimateRowsAffected(db, tt.sql)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if got != tt.want {
				t.Errorf("EstimateRowsAffected() = %d, want %d", got, tt.want)
			}
		})
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unfulfilled expectations: %v", err)
	}
}
