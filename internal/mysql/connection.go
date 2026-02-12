package mysql

import (
	"database/sql"
	"fmt"
	"syscall"

	_ "github.com/go-sql-driver/mysql"
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
}

// Connect establishes a MySQL connection.
func Connect(cfg ConnectionConfig) (*sql.DB, error) {
	dsn := buildDSN(cfg)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open connection: %w", err)
	}

	// Verify the connection actually works
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping: %w", err)
	}

	// Conservative connection pool for a CLI tool
	db.SetMaxOpenConns(2)
	db.SetMaxIdleConns(1)

	return db, nil
}

func buildDSN(cfg ConnectionConfig) string {
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

	return fmt.Sprintf("%s:%s@%s/%s?parseTime=true&interpolateParams=true",
		cfg.User, cfg.Password, addr, db)
}

// PromptPassword reads a password from the terminal without echoing.
func PromptPassword() string {
	fmt.Print("Enter password: ")
	password, err := term.ReadPassword(int(syscall.Stdin))
	fmt.Println() // newline after hidden input
	if err != nil {
		return ""
	}
	return string(password)
}
