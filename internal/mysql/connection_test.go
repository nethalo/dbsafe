package mysql

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"
)

func TestBuildDSN(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ConnectionConfig
		want    string
		wantErr bool
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
		{
			name: "TLS preferred",
			cfg: ConnectionConfig{
				Host:     "rds.example.com",
				Port:     3306,
				User:     "admin",
				Password: "pass",
				Database: "prod",
				TLSMode:  "preferred",
			},
			want: "admin:pass@tcp(rds.example.com:3306)/prod?parseTime=true&interpolateParams=true&tls=preferred",
		},
		{
			name: "TLS required",
			cfg: ConnectionConfig{
				Host:     "rds.example.com",
				Port:     3306,
				User:     "admin",
				Password: "pass",
				Database: "prod",
				TLSMode:  "required",
			},
			want: "admin:pass@tcp(rds.example.com:3306)/prod?parseTime=true&interpolateParams=true&tls=true",
		},
		{
			name: "TLS skip-verify",
			cfg: ConnectionConfig{
				Host:     "10.0.0.1",
				Port:     3306,
				User:     "user",
				Password: "pass",
				Database: "db",
				TLSMode:  "skip-verify",
			},
			want: "user:pass@tcp(10.0.0.1:3306)/db?parseTime=true&interpolateParams=true&tls=skip-verify",
		},
		{
			name: "TLS custom",
			cfg: ConnectionConfig{
				Host:     "aurora.example.com",
				Port:     3306,
				User:     "user",
				Password: "pass",
				Database: "db",
				TLSMode:  "custom",
				TLSCA:    "/path/to/ca.pem",
			},
			want: "user:pass@tcp(aurora.example.com:3306)/db?parseTime=true&interpolateParams=true&tls=dbsafe-custom",
		},
		{
			name: "TLS disabled (explicit)",
			cfg: ConnectionConfig{
				Host:     "localhost",
				Port:     3306,
				User:     "user",
				Password: "pass",
				Database: "db",
				TLSMode:  "disabled",
			},
			want: "user:pass@tcp(localhost:3306)/db?parseTime=true&interpolateParams=true",
		},
		{
			name: "invalid TLS mode",
			cfg: ConnectionConfig{
				Host:    "localhost",
				Port:    3306,
				TLSMode: "bogus",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildDSN(tt.cfg)
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

	dsn, err := buildDSN(cfg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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

	dsn2, err := buildDSN(cfg2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !contains(dsn2, "unix(/tmp/mysql.sock)") {
		t.Errorf("DSN with socket should use unix protocol, got: %s", dsn2)
	}
	if contains(dsn2, "tcp") {
		t.Errorf("DSN with socket should not contain tcp, got: %s", dsn2)
	}
}

func TestRegisterCustomTLS(t *testing.T) {
	t.Run("valid CA certificate", func(t *testing.T) {
		caPath := writeTempCA(t)
		err := registerCustomTLS(caPath)
		if err != nil {
			t.Errorf("registerCustomTLS() unexpected error: %v", err)
		}
	})

	t.Run("nonexistent file", func(t *testing.T) {
		err := registerCustomTLS("/nonexistent/path/ca.pem")
		if err == nil {
			t.Error("expected error for nonexistent file, got nil")
		}
	})

	t.Run("invalid PEM content", func(t *testing.T) {
		f, err := os.CreateTemp(t.TempDir(), "bad-ca-*.pem")
		if err != nil {
			t.Fatal(err)
		}
		f.WriteString("this is not a valid PEM file")
		f.Close()

		err = registerCustomTLS(f.Name())
		if err == nil {
			t.Error("expected error for invalid PEM content, got nil")
		}
	})
}

// writeTempCA generates a self-signed CA cert and writes it to a temp file.
func writeTempCA(t *testing.T) string {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test-ca"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}

	f, err := os.CreateTemp(t.TempDir(), "ca-*.pem")
	if err != nil {
		t.Fatal(err)
	}

	if err := pem.Encode(f, &pem.Block{Type: "CERTIFICATE", Bytes: certDER}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	return f.Name()
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
