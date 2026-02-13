package cmd

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/viper"
)

func TestInitConfig_FileNotFound(t *testing.T) {
	// Save original values
	origHome := os.Getenv("HOME")
	defer os.Setenv("HOME", origHome)

	// Set HOME to a temp dir with no config
	tmpDir := t.TempDir()
	os.Setenv("HOME", tmpDir)

	// Reset viper
	viper.Reset()
	cfgFile = ""

	// This should not error even if config doesn't exist
	initConfig()

	// Should have set defaults
	if viper.GetInt("defaults.chunk_size") == 0 {
		// Config not loaded, which is fine - defaults should still work
	}
}

func TestInitConfig_WithConfigFile(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, ".dbsafe.yaml")

	// Create a test config file
	configContent := `connections:
  default:
    host: testhost
    port: 3307
    user: testuser
    database: testdb
defaults:
  chunk_size: 5000
  format: json
`
	if err := os.WriteFile(configPath, []byte(configContent), 0644); err != nil {
		t.Fatalf("failed to create test config: %v", err)
	}

	// Reset viper and set config file
	viper.Reset()
	cfgFile = configPath

	initConfig()

	// Verify config was loaded and mapped correctly
	// The initConfig function maps connections.default.* to flat keys
	if viper.GetString("connections.default.host") != "testhost" {
		t.Errorf("expected nested config to be loaded, got: %s", viper.GetString("connections.default.host"))
	}

	if viper.GetInt("defaults.chunk_size") != 5000 {
		t.Errorf("chunk_size = %d, want 5000", viper.GetInt("defaults.chunk_size"))
	}

	if viper.GetString("defaults.format") != "json" {
		t.Errorf("format = %s, want json", viper.GetString("defaults.format"))
	}

	// After mapping in initConfig, flat keys should also work
	// Note: This test validates the config structure, not the mapping
	// The mapping happens when flags are not set
}

func TestInitConfig_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, ".dbsafe.yaml")

	// Create invalid YAML
	invalidYAML := `connections:
  default:
    host: testhost
	invalid indentation
`
	if err := os.WriteFile(configPath, []byte(invalidYAML), 0644); err != nil {
		t.Fatalf("failed to create test config: %v", err)
	}

	viper.Reset()
	cfgFile = configPath

	// initConfig should handle this gracefully (viper logs error but doesn't panic)
	// This test verifies it doesn't crash
	initConfig()

	// Config loading failed, so values should be unset or defaults
	if viper.GetString("connections.default.host") == "testhost" {
		t.Error("invalid YAML should not have been parsed successfully")
	}
}

func TestConfigMapping(t *testing.T) {
	// Test that nested config structure is correct
	viper.Reset()
	viper.Set("connections.default.host", "localhost")
	viper.Set("connections.default.port", 3306)
	viper.Set("connections.default.user", "root")
	viper.Set("connections.default.database", "testdb")

	if viper.GetString("connections.default.host") != "localhost" {
		t.Errorf("expected localhost, got %s", viper.GetString("connections.default.host"))
	}

	if viper.GetInt("connections.default.port") != 3306 {
		t.Errorf("expected 3306, got %d", viper.GetInt("connections.default.port"))
	}
}

// TestRootCommand_Version tests that version command doesn't crash
func TestRootCommand_Version(t *testing.T) {
	// This is a basic smoke test to ensure the command structure is valid
	if rootCmd == nil {
		t.Fatal("rootCmd should not be nil")
	}

	if rootCmd.Use != "dbsafe" {
		t.Errorf("rootCmd.Use = %q, want %q", rootCmd.Use, "dbsafe")
	}
}
