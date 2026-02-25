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

func TestPlanCmd_Structure(t *testing.T) {
	if planCmd == nil {
		t.Fatal("planCmd should not be nil")
	}

	if planCmd.Use != "plan [SQL statement]" {
		t.Errorf("planCmd.Use = %q, want %q", planCmd.Use, "plan [SQL statement]")
	}

	if planCmd.Short == "" {
		t.Error("planCmd.Short should not be empty")
	}

	if planCmd.Long == "" {
		t.Error("planCmd.Long should not be empty")
	}

	// Verify command is registered with root
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Name() == "plan" {
			found = true
			break
		}
	}
	if !found {
		t.Error("plan command should be registered with root command")
	}

	// Verify SilenceUsage is set
	if !planCmd.SilenceUsage {
		t.Error("planCmd should set SilenceUsage to true")
	}
}

func TestPlanCmd_Flags(t *testing.T) {
	// Test that expected flags exist
	fileFlag := planCmd.Flags().Lookup("file")
	if fileFlag == nil {
		t.Error("plan command should have --file flag")
	}

	chunkSizeFlag := planCmd.Flags().Lookup("chunk-size")
	if chunkSizeFlag == nil {
		t.Error("plan command should have --chunk-size flag")
		return
	}

	// Verify default chunk size
	if chunkSizeFlag.DefValue != "10000" {
		t.Errorf("chunk-size default = %s, want 10000", chunkSizeFlag.DefValue)
	}
}

func TestPlanCmd_MaxArgs(t *testing.T) {
	// Plan command should accept at most 1 argument (the SQL statement)
	// This is enforced by cobra.MaximumNArgs(1)
	if planCmd.Args == nil {
		t.Error("planCmd should have Args validator")
	}
}

func TestGetSQLInput_WhitespaceHandling(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "leading whitespace",
			input:    "  SELECT * FROM users",
			expected: "SELECT * FROM users",
		},
		{
			name:     "trailing whitespace",
			input:    "SELECT * FROM users  ",
			expected: "SELECT * FROM users",
		},
		{
			name:     "leading and trailing whitespace",
			input:    "  SELECT * FROM users  ",
			expected: "SELECT * FROM users",
		},
		{
			name:     "newlines",
			input:    "\nSELECT * FROM users\n",
			expected: "SELECT * FROM users",
		},
		{
			name:     "tabs",
			input:    "\tSELECT * FROM users\t",
			expected: "SELECT * FROM users",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := planCmd
			sql, err := getSQLInput(cmd, []string{tc.input})
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if sql != tc.expected {
				t.Errorf("getSQLInput(%q) = %q, want %q", tc.input, sql, tc.expected)
			}
		})
	}
}

func TestGetSQLInput_FileWithWhitespace(t *testing.T) {
	tmpDir := t.TempDir()
	sqlFile := filepath.Join(tmpDir, "whitespace.sql")

	// Create file with lots of whitespace
	content := "\n\n  UPDATE users SET active = 1  \n\n"
	if err := os.WriteFile(sqlFile, []byte(content), 0644); err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}

	cmd := planCmd
	cmd.Flags().Set("file", sqlFile)
	defer cmd.Flags().Set("file", "")

	sql, err := getSQLInput(cmd, []string{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := "UPDATE users SET active = 1"
	if sql != expected {
		t.Errorf("getSQLInput() = %q, want %q", sql, expected)
	}
}
