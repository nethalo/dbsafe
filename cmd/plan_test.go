package cmd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestGetSQLInput_FromArgs(t *testing.T) {
	// Create a temporary command for testing
	cmd := planCmd
	args := []string{"SELECT * FROM users"}

	sql, err := getSQLInput(cmd, args)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "SELECT * FROM users"
	if sql != expected {
		t.Errorf("getSQLInput() = %q, want %q", sql, expected)
	}
}

func TestGetSQLInput_FromFile(t *testing.T) {
	// Create a temporary SQL file
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "test.sql")
	content := "ALTER TABLE users ADD COLUMN email VARCHAR(255);\n"

	if err := os.WriteFile(sqlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	// Create command and set file flag
	cmd := planCmd
	cmd.Flags().Set("file", sqlFile)
	defer cmd.Flags().Set("file", "") // reset

	sql, err := getSQLInput(cmd, []string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should trim whitespace (trailing newline)
	// Note: Parser trims semicolons, not getSQLInput
	expected := "ALTER TABLE users ADD COLUMN email VARCHAR(255);"
	if sql != expected {
		t.Errorf("getSQLInput() = %q, want %q", sql, expected)
	}
}

func TestGetSQLInput_FileNotFound(t *testing.T) {
	cmd := planCmd
	cmd.Flags().Set("file", "/nonexistent/file.sql")
	defer cmd.Flags().Set("file", "")

	_, err := getSQLInput(cmd, []string{})
	if err == nil {
		t.Error("expected error for nonexistent file, got nil")
	}
}

func TestGetSQLInput_NoInput(t *testing.T) {
	cmd := planCmd
	cmd.Flags().Set("file", "")
	defer cmd.Flags().Set("file", "")

	_, err := getSQLInput(cmd, []string{})
	if err == nil {
		t.Error("expected error when no SQL provided, got nil")
	}
}

func TestGetSQLInput_FileTakesPrecedence(t *testing.T) {
	// Create a temporary SQL file
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "test.sql")
	fileContent := "DELETE FROM logs"

	if err := os.WriteFile(sqlFile, []byte(fileContent), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	cmd := planCmd
	cmd.Flags().Set("file", sqlFile)
	defer cmd.Flags().Set("file", "")

	// Provide args too, but file should take precedence
	sql, err := getSQLInput(cmd, []string{"SELECT * FROM users"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if sql != "DELETE FROM logs" {
		t.Errorf("getSQLInput() = %q, want %q (file should take precedence)", sql, "DELETE FROM logs")
	}
}
