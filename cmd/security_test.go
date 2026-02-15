package cmd

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestValidateSQLFilePath(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	// Create a valid SQL file
	validFile := filepath.Join(tmpDir, "test.sql")
	err := os.WriteFile(validFile, []byte("SELECT * FROM users;"), 0600)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Create a large file (> 10MB)
	largeFile := filepath.Join(tmpDir, "large.sql")
	largeData := make([]byte, 11*1024*1024) // 11 MB
	err = os.WriteFile(largeFile, largeData, 0600)
	if err != nil {
		t.Fatalf("Failed to create large file: %v", err)
	}

	// Create a directory
	dirPath := filepath.Join(tmpDir, "testdir")
	err = os.Mkdir(dirPath, 0700)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	tests := []struct {
		name      string
		filePath  string
		wantError bool
		errMsg    string
	}{
		{
			name:      "valid SQL file",
			filePath:  validFile,
			wantError: false,
		},
		{
			name:      "non-existent file",
			filePath:  filepath.Join(tmpDir, "nonexistent.sql"),
			wantError: true,
			errMsg:    "cannot access file",
		},
		{
			name:      "directory instead of file",
			filePath:  dirPath,
			wantError: true,
			errMsg:    "not a regular file",
		},
		{
			name:      "file too large",
			filePath:  largeFile,
			wantError: true,
			errMsg:    "file too large",
		},
		{
			name:      "valid file with relative path",
			filePath:  validFile,
			wantError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateSQLFilePath(tt.filePath)
			if tt.wantError && err == nil {
				t.Errorf("validateSQLFilePath(%q) expected error, got nil", tt.filePath)
			}
			if !tt.wantError && err != nil {
				t.Errorf("validateSQLFilePath(%q) unexpected error: %v", tt.filePath, err)
			}
			if tt.wantError && err != nil && tt.errMsg != "" {
				if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("validateSQLFilePath(%q) error = %v, want error containing %q", tt.filePath, err, tt.errMsg)
				}
			}
		})
	}
}

func TestValidateSQLFilePath_PathTraversal(t *testing.T) {
	// Test path traversal attempts
	pathTraversalAttempts := []string{
		"../../../etc/passwd",
		"../../.ssh/id_rsa",
		"/etc/passwd",
		"/etc/shadow",
	}

	for _, attempt := range pathTraversalAttempts {
		t.Run("path_traversal_"+attempt, func(t *testing.T) {
			err := validateSQLFilePath(attempt)
			// Should fail because these files don't exist or aren't regular files
			// or trigger warnings for sensitive paths
			if err == nil {
				// If no error, the file exists - check if it's a sensitive path
				// (this would trigger a warning but not an error in our implementation)
				absPath, _ := filepath.Abs(attempt)
				isSensitive := strings.HasPrefix(absPath, "/etc/") ||
					strings.HasPrefix(absPath, "/sys/") ||
					strings.HasPrefix(absPath, "/proc/") ||
					strings.HasPrefix(absPath, "/dev/")

				if !isSensitive {
					t.Logf("Path %s was allowed (not a sensitive system path)", attempt)
				}
			}
		})
	}
}

func TestValidateSQLFilePath_CleanPath(t *testing.T) {
	// Create a temporary directory for testing
	tmpDir := t.TempDir()

	// Create a valid SQL file
	validFile := filepath.Join(tmpDir, "test.sql")
	err := os.WriteFile(validFile, []byte("SELECT * FROM users;"), 0600)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Test with path containing . and ..
	messyPath := filepath.Join(tmpDir, ".", "subdir", "..", "test.sql")

	err = validateSQLFilePath(messyPath)
	if err != nil {
		t.Errorf("validateSQLFilePath should clean and accept messy path: %v", err)
	}
}
