package mysql

import (
	"testing"
)

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name string
		cfg  ConnectionConfig
		want string
	}{
		{
			name: "TCP connection with all fields",
			cfg: ConnectionConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "root",
				Password: "secret",
				Database: "mydb",
			},
			want: "root:secret@tcp(localhost:3306)/mydb?parseTime=true&interpolateParams=true",
		},
		{
			name: "TCP connection without database",
			cfg: ConnectionConfig{
				Host:     "192.168.1.100",
				Port:     3307,
				User:     "dbsafe",
				Password: "pass123",
			},
			want: "dbsafe:pass123@tcp(192.168.1.100:3307)/information_schema?parseTime=true&interpolateParams=true",
		},
		{
			name: "Unix socket connection",
			cfg: ConnectionConfig{
				Socket:   "/var/run/mysqld/mysqld.sock",
				User:     "app",
				Password: "apppass",
				Database: "production",
			},
			want: "app:apppass@unix(/var/run/mysqld/mysqld.sock)/production?parseTime=true&interpolateParams=true",
		},
		{
			name: "Empty password",
			cfg: ConnectionConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "readonly",
				Password: "",
				Database: "test",
			},
			want: "readonly:@tcp(localhost:3306)/test?parseTime=true&interpolateParams=true",
		},
		{
			name: "Special characters in password",
			cfg: ConnectionConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "user",
				Password: "p@ss:w0rd!",
				Database: "db",
			},
			want: "user:p@ss:w0rd!@tcp(localhost:3306)/db?parseTime=true&interpolateParams=true",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildDSN(tt.cfg)
			if got != tt.want {
				t.Errorf("buildDSN() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestConnectionConfig_DSNDefaults(t *testing.T) {
	// Test that empty database defaults to information_schema
	cfg := ConnectionConfig{
		Host:     "localhost",
		Port:     3306,
		User:     "user",
		Password: "pass",
		Database: "",
	}

	dsn := buildDSN(cfg)
	if !contains(dsn, "information_schema") {
		t.Errorf("DSN with empty database should default to information_schema, got: %s", dsn)
	}

	// Test socket takes precedence over host
	cfg2 := ConnectionConfig{
		Host:     "localhost",
		Port:     3306,
		Socket:   "/tmp/mysql.sock",
		User:     "user",
		Password: "pass",
		Database: "db",
	}

	dsn2 := buildDSN(cfg2)
	if !contains(dsn2, "unix(/tmp/mysql.sock)") {
		t.Errorf("DSN with socket should use unix protocol, got: %s", dsn2)
	}
	if contains(dsn2, "tcp") {
		t.Errorf("DSN with socket should not contain tcp, got: %s", dsn2)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(substr) == 0 || (len(s) > 0 && len(substr) > 0 && findSubstring(s, substr)))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// Note: We cannot test Connect() without a real MySQL server or complex mocking
// of the sql.Open and db.Ping calls. The buildDSN function is the core logic
// we can unit test. Integration tests would cover Connect().
