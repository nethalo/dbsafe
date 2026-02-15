package cmd

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestConfigInitCmd_NewConfig(t *testing.T) {
	// Create a temporary directory
	tmpDir := t.TempDir()

	// Override HOME to point to temp dir
	origHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	// Prepare input simulation (auto-accept defaults)
	input := "127.0.0.1\n3306\ndbsafe\n\ntext\n"

	// Save original stdin
	oldStdin := os.Stdin
	defer func() { os.Stdin = oldStdin }()

	// Create a temporary file for input
	tmpInput, err := os.CreateTemp(tmpDir, "input")
	if err != nil {
		t.Fatalf("failed to create temp input file: %v", err)
	}
	defer tmpInput.Close()
	tmpInput.WriteString(input)
	tmpInput.Seek(0, 0)
	os.Stdin = tmpInput

	// Capture output
	output := &bytes.Buffer{}
	configInitCmd.SetOut(output)
	configInitCmd.SetErr(output)

	// Execute command
	err = configInitCmd.RunE(configInitCmd, []string{})
	if err != nil {
		t.Fatalf("config init should succeed: %v", err)
	}

	// Verify config file was created
	configPath := filepath.Join(tmpDir, ".dbsafe", "config.yaml")
	if _, err := os.Stat(configPath); os.IsNotExist(err) {
		t.Errorf("config file should be created at %s", configPath)
	}

	// Read and verify config content
	content, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("failed to read config file: %v", err)
	}

	contentStr := string(content)

	// Verify expected content
	expectedStrings := []string{
		"connections:",
		"default:",
		"host: 127.0.0.1",
		"port: 3306",
		"user: dbsafe",
		"defaults:",
		"chunk_size: 10000",
		"format: text",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(contentStr, expected) {
			t.Errorf("config should contain %q, content:\n%s", expected, contentStr)
		}
	}

	// Verify file permissions (should be 0600)
	fileInfo, err := os.Stat(configPath)
	if err != nil {
		t.Fatalf("failed to stat config file: %v", err)
	}

	perm := fileInfo.Mode().Perm()
	if perm != 0600 {
		t.Errorf("config file permissions = %o, want 0600", perm)
	}
}

func TestConfigInitCmd_AlreadyExists_Abort(t *testing.T) {
	tmpDir := t.TempDir()

	origHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	// Create .dbsafe directory and existing config
	configDir := filepath.Join(tmpDir, ".dbsafe")
	os.MkdirAll(configDir, 0700)
	configPath := filepath.Join(configDir, "config.yaml")
	os.WriteFile(configPath, []byte("existing: config"), 0600)

	// Simulate user saying "no" to overwrite
	input := "n\n"
	tmpInput, err := os.CreateTemp(tmpDir, "input")
	if err != nil {
		t.Fatalf("failed to create temp input: %v", err)
	}
	defer tmpInput.Close()
	tmpInput.WriteString(input)
	tmpInput.Seek(0, 0)

	oldStdin := os.Stdin
	os.Stdin = tmpInput
	defer func() { os.Stdin = oldStdin }()

	output := &bytes.Buffer{}
	configInitCmd.SetOut(output)
	configInitCmd.SetErr(output)

	err = configInitCmd.RunE(configInitCmd, []string{})
	if err != nil {
		t.Fatalf("config init should handle abort gracefully: %v", err)
	}

	// Verify original config wasn't changed
	content, _ := os.ReadFile(configPath)
	if string(content) != "existing: config" {
		t.Error("config should not be overwritten when user aborts")
	}

	result := output.String()
	if !strings.Contains(result, "Aborted") {
		t.Errorf("output should indicate abort, got: %s", result)
	}
}

func TestConfigInitCmd_AlreadyExists_Overwrite(t *testing.T) {
	tmpDir := t.TempDir()

	origHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	// Create existing config
	configDir := filepath.Join(tmpDir, ".dbsafe")
	os.MkdirAll(configDir, 0700)
	configPath := filepath.Join(configDir, "config.yaml")
	os.WriteFile(configPath, []byte("old: config"), 0600)

	// Simulate user saying "yes" to overwrite, then provide inputs
	input := "y\nlocalhost\n3307\ntestuser\ntestdb\njson\n"
	tmpInput, err := os.CreateTemp(tmpDir, "input")
	if err != nil {
		t.Fatalf("failed to create temp input: %v", err)
	}
	defer tmpInput.Close()
	tmpInput.WriteString(input)
	tmpInput.Seek(0, 0)

	oldStdin := os.Stdin
	os.Stdin = tmpInput
	defer func() { os.Stdin = oldStdin }()

	output := &bytes.Buffer{}
	configInitCmd.SetOut(output)
	configInitCmd.SetErr(output)

	err = configInitCmd.RunE(configInitCmd, []string{})
	if err != nil {
		t.Fatalf("config init should succeed: %v", err)
	}

	// Verify config was updated
	content, _ := os.ReadFile(configPath)
	contentStr := string(content)

	if !strings.Contains(contentStr, "host: localhost") {
		t.Error("config should contain new host")
	}
	if !strings.Contains(contentStr, "port: 3307") {
		t.Error("config should contain new port")
	}
	if strings.Contains(contentStr, "old: config") {
		t.Error("config should not contain old content")
	}
}

func TestConfigShowCmd_NoConfig(t *testing.T) {
	tmpDir := t.TempDir()

	origHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	viper.Reset()
	cfgFile = ""

	output := &bytes.Buffer{}
	configShowCmd.SetOut(output)
	configShowCmd.SetErr(output)

	err := configShowCmd.RunE(configShowCmd, []string{})
	if err != nil {
		t.Fatalf("config show should handle missing config: %v", err)
	}

	result := output.String()
	if !strings.Contains(result, "No config file found") {
		t.Errorf("should indicate no config found, got: %s", result)
	}
	if !strings.Contains(result, "config init") {
		t.Errorf("should suggest running 'config init', got: %s", result)
	}
}

func TestConfigShowCmd_WithConfig(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "test-config.yaml")

	configContent := `connections:
  default:
    host: testhost
    port: 3307
    user: testuser
defaults:
  format: json
`
	err := os.WriteFile(configPath, []byte(configContent), 0600)
	if err != nil {
		t.Fatalf("failed to create test config: %v", err)
	}

	// Set viper to use this config
	viper.Reset()
	viper.SetConfigFile(configPath)
	viper.ReadInConfig()

	output := &bytes.Buffer{}
	configShowCmd.SetOut(output)
	configShowCmd.SetErr(output)

	err = configShowCmd.RunE(configShowCmd, []string{})
	if err != nil {
		t.Fatalf("config show should succeed: %v", err)
	}

	result := output.String()

	// Should show config file path
	if !strings.Contains(result, configPath) {
		t.Errorf("should show config file path, got: %s", result)
	}

	// Should show config content
	if !strings.Contains(result, "testhost") {
		t.Errorf("should show config content, got: %s", result)
	}
}

func TestConfigCmd_Structure(t *testing.T) {
	if configCmd == nil {
		t.Fatal("configCmd should not be nil")
	}

	if configCmd.Use != "config" {
		t.Errorf("configCmd.Use = %q, want %q", configCmd.Use, "config")
	}

	// Verify subcommands exist
	subCommands := configCmd.Commands()
	if len(subCommands) < 2 {
		t.Errorf("configCmd should have at least 2 subcommands (init, show), got %d", len(subCommands))
	}

	// Verify init subcommand
	var foundInit, foundShow bool
	for _, cmd := range subCommands {
		if cmd.Use == "init" {
			foundInit = true
		}
		if cmd.Use == "show" {
			foundShow = true
		}
	}

	if !foundInit {
		t.Error("configCmd should have 'init' subcommand")
	}
	if !foundShow {
		t.Error("configCmd should have 'show' subcommand")
	}
}

func TestConfigInitCmd_DirectoryCreation(t *testing.T) {
	tmpDir := t.TempDir()

	origHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	// Ensure .dbsafe directory doesn't exist
	configDir := filepath.Join(tmpDir, ".dbsafe")
	if _, err := os.Stat(configDir); !os.IsNotExist(err) {
		t.Fatal("test setup error: .dbsafe should not exist")
	}

	// Simulate user input
	input := "\n\n\n\n\n" // Accept all defaults
	tmpInput, _ := os.CreateTemp(tmpDir, "input")
	defer tmpInput.Close()
	tmpInput.WriteString(input)
	tmpInput.Seek(0, 0)

	oldStdin := os.Stdin
	os.Stdin = tmpInput
	defer func() { os.Stdin = oldStdin }()

	output := &bytes.Buffer{}
	configInitCmd.SetOut(output)
	configInitCmd.SetErr(output)

	err := configInitCmd.RunE(configInitCmd, []string{})
	if err != nil {
		t.Fatalf("config init should create directory: %v", err)
	}

	// Verify directory was created with correct permissions
	dirInfo, err := os.Stat(configDir)
	if err != nil {
		t.Fatalf(".dbsafe directory should be created: %v", err)
	}

	if !dirInfo.IsDir() {
		t.Error(".dbsafe should be a directory")
	}

	perm := dirInfo.Mode().Perm()
	if perm != 0700 {
		t.Errorf(".dbsafe directory permissions = %o, want 0700", perm)
	}
}

func TestConfigInitCmd_Recommendations(t *testing.T) {
	tmpDir := t.TempDir()

	origHome := os.Getenv("HOME")
	os.Setenv("HOME", tmpDir)
	defer os.Setenv("HOME", origHome)

	// Test with non-root user
	input := "\n\ncustomuser\n\n\n"
	tmpInput, _ := os.CreateTemp(tmpDir, "input")
	defer tmpInput.Close()
	tmpInput.WriteString(input)
	tmpInput.Seek(0, 0)

	oldStdin := os.Stdin
	os.Stdin = tmpInput
	defer func() { os.Stdin = oldStdin }()

	output := &bytes.Buffer{}
	configInitCmd.SetOut(output)
	configInitCmd.SetErr(output)

	configInitCmd.RunE(configInitCmd, []string{})

	result := output.String()

	// Should show SQL recommendations for non-root user
	if !strings.Contains(result, "CREATE USER") {
		t.Error("should show CREATE USER recommendation for non-root user")
	}
	if !strings.Contains(result, "GRANT SELECT") {
		t.Error("should show GRANT recommendations")
	}
}
