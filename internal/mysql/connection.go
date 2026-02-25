package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"syscall"

	mysqldriver "github.com/go-sql-driver/mysql"
	"golang.org/x/term"
)

// ConnectionConfig holds MySQL connection parameters.
type ConnectionConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	Database string
	Socket   string
	TLSMode  string // "", "disabled", "preferred", "required", "skip-verify", "custom"
	TLSCA    string // path to CA certificate file (required when TLSMode == "custom")
}

// Connect establishes a MySQL connection.
func Connect(cfg ConnectionConfig) (*sql.DB, error) {
	// Register custom TLS config before building DSN
	if cfg.TLSMode == "custom" {
		if cfg.TLSCA == "" {
			return nil, fmt.Errorf("--tls-ca is required when --tls=custom")
		}
		if err := registerCustomTLS(cfg.TLSCA); err != nil {
			return nil, fmt.Errorf("TLS setup failed: %w", err)
		}
	}

	dsn, err := buildDSN(cfg)
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Verify the connection actually works
	if err := db.PingContext(context.Background()); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	// Conservative connection pool for a CLI tool
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	return db, nil
}

// registerCustomTLS reads a CA certificate PEM file and registers it as a named TLS config.
func registerCustomTLS(caPath string) error {
	pem, err := os.ReadFile(caPath)
	if err != nil {
		return fmt.Errorf("reading CA certificate %q: %w", caPath, err)
	}

	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(pem) {
		return fmt.Errorf("no valid certificates found in %q", caPath)
	}

	return mysqldriver.RegisterTLSConfig("dbsafe-custom", &tls.Config{
		RootCAs: rootCAs,
	})
}

func buildDSN(cfg ConnectionConfig) (string, error) {
	// Validate TLS mode
	switch cfg.TLSMode {
	case "", "disabled", "preferred", "required", "skip-verify", "custom":
		// valid
	default:
		return "", fmt.Errorf("invalid TLS mode %q: valid values are disabled, preferred, required, skip-verify, custom", cfg.TLSMode)
	}

	// Format: user:password@protocol(address)/dbname?params
	var addr string
	if cfg.Socket != "" {
		addr = fmt.Sprintf("unix(%s)", cfg.Socket)
	} else {
		addr = fmt.Sprintf("tcp(%s:%d)", cfg.Host, cfg.Port)
	}

	db := cfg.Database
	if db == "" {
		db = "information_schema"
	}

	dsn := fmt.Sprintf("%s:%s@%s/%s?parseTime=true&interpolateParams=true",
		cfg.User, cfg.Password, addr, db)

	// Append TLS parameter
	switch cfg.TLSMode {
	case "preferred":
		dsn += "&tls=preferred"
	case "required":
		dsn += "&tls=true"
	case "skip-verify":
		dsn += "&tls=skip-verify"
	case "custom":
		dsn += "&tls=dbsafe-custom"
		// "" and "disabled" → no TLS param (current behavior)
	}

	return dsn, nil
}

// PromptPassword reads a password from the terminal without echoing.
func PromptPassword() string {
	fmt.Print("Enter password: ")
	password, err := term.ReadPassword(syscall.Stdin)
	fmt.Println() // newline after hidden input
	if err != nil {
		return ""
	}
	return string(password)
}
