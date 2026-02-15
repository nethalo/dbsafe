package cmd

import (
	"bytes"
	"strings"
	"testing"
)

func TestVersionCommand(t *testing.T) {
	// Save original values
	origVersion := Version
	origCommitSHA := CommitSHA
	origBuildDate := BuildDate

	// Set test values
	Version = "1.2.3"
	CommitSHA = "abc123"
	BuildDate = "2024-01-15"

	// Restore after test
	defer func() {
		Version = origVersion
		CommitSHA = origCommitSHA
		BuildDate = origBuildDate
	}()

	// Capture output
	output := &bytes.Buffer{}
	versionCmd.SetOut(output)
	versionCmd.SetErr(output)

	// Execute command
	versionCmd.Run(versionCmd, []string{})

	result := output.String()

	// Verify output contains version info
	if !strings.Contains(result, "1.2.3") {
		t.Errorf("output should contain version '1.2.3', got: %s", result)
	}

	if !strings.Contains(result, "abc123") {
		t.Errorf("output should contain commit SHA 'abc123', got: %s", result)
	}

	if !strings.Contains(result, "2024-01-15") {
		t.Errorf("output should contain build date '2024-01-15', got: %s", result)
	}

	// Verify supported versions are listed
	if !strings.Contains(result, "MySQL 8.0") {
		t.Errorf("output should mention supported MySQL versions, got: %s", result)
	}

	if !strings.Contains(result, "Percona XtraDB Cluster") {
		t.Errorf("output should mention Percona XtraDB Cluster support, got: %s", result)
	}
}

func TestVersionCommand_DevBuild(t *testing.T) {
	// Test default "dev" version
	origVersion := Version
	Version = "dev"
	defer func() { Version = origVersion }()

	output := &bytes.Buffer{}
	versionCmd.SetOut(output)
	versionCmd.SetErr(output)

	versionCmd.Run(versionCmd, []string{})

	result := output.String()

	if !strings.Contains(result, "dev") {
		t.Errorf("dev build should show 'dev' version, got: %s", result)
	}
}

func TestVersionCommand_Structure(t *testing.T) {
	if versionCmd == nil {
		t.Fatal("versionCmd should not be nil")
	}

	if versionCmd.Use != "version" {
		t.Errorf("versionCmd.Use = %q, want %q", versionCmd.Use, "version")
	}

	if versionCmd.Short == "" {
		t.Error("versionCmd.Short should not be empty")
	}

	// Verify command is registered with root
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "version" {
			found = true
			break
		}
	}
	if !found {
		t.Error("version command should be registered with root command")
	}
}
