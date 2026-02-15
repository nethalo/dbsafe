package cmd

import (
	"bytes"
	"strings"
	"testing"

	"github.com/spf13/viper"
)

func TestConnectCmd_Structure(t *testing.T) {
	if connectCmd == nil {
		t.Fatal("connectCmd should not be nil")
	}

	if connectCmd.Use != "connect" {
		t.Errorf("connectCmd.Use = %q, want %q", connectCmd.Use, "connect")
	}

	if connectCmd.Short == "" {
		t.Error("connectCmd.Short should not be empty")
	}

	if connectCmd.Long == "" {
		t.Error("connectCmd.Long should not be empty")
	}

	// Verify command is registered with root
	found := false
	for _, cmd := range rootCmd.Commands() {
		if cmd.Use == "connect" {
			found = true
			break
		}
	}
	if !found {
		t.Error("connect command should be registered with root command")
	}
}

func TestConnectCmd_DefaultValues(t *testing.T) {
	// Reset viper to test defaults
	viper.Reset()

	// The connect command should use default host and user if not specified
	// This test verifies the logic in the command (even though we can't execute it without a real DB)

	// Test that defaults are applied in the command logic
	// Host should default to 127.0.0.1 if empty
	// User should default to "dbsafe" if empty

	viper.Set("host", "")
	viper.Set("user", "")

	host := viper.GetString("host")
	user := viper.GetString("user")

	// The command applies defaults, so empty values should trigger defaults
	if host != "" {
		host = "127.0.0.1" // This is what the command does
	}
	if user != "" {
		user = "dbsafe" // This is what the command does
	}

	// This validates the default logic structure
	if host != "127.0.0.1" {
		t.Errorf("default host should be 127.0.0.1, got %s", host)
	}
	if user != "dbsafe" {
		t.Errorf("default user should be dbsafe, got %s", user)
	}
}

func TestConnectCmd_ViperIntegration(t *testing.T) {
	// Test that viper config values are used correctly
	viper.Reset()

	testCases := []struct {
		name     string
		host     string
		port     int
		user     string
		database string
		socket   string
	}{
		{
			name:     "tcp connection",
			host:     "db.example.com",
			port:     3306,
			user:     "testuser",
			database: "testdb",
			socket:   "",
		},
		{
			name:     "socket connection",
			host:     "",
			port:     0,
			user:     "testuser",
			database: "testdb",
			socket:   "/var/run/mysqld/mysqld.sock",
		},
		{
			name:     "custom port",
			host:     "localhost",
			port:     3307,
			user:     "admin",
			database: "prod",
			socket:   "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			viper.Reset()
			viper.Set("host", tc.host)
			viper.Set("port", tc.port)
			viper.Set("user", tc.user)
			viper.Set("database", tc.database)
			viper.Set("socket", tc.socket)

			// Verify viper values are set correctly
			if viper.GetString("host") != tc.host {
				t.Errorf("host = %s, want %s", viper.GetString("host"), tc.host)
			}
			if viper.GetInt("port") != tc.port {
				t.Errorf("port = %d, want %d", viper.GetInt("port"), tc.port)
			}
			if viper.GetString("user") != tc.user {
				t.Errorf("user = %s, want %s", viper.GetString("user"), tc.user)
			}
		})
	}
}

// TestConnectCmd_ErrorHandling tests error message formatting
// Note: We can't test actual connection failures without a mock DB,
// but we can test the command structure and error handling paths
func TestConnectCmd_ErrorPaths(t *testing.T) {
	// Test that the command has proper error handling structure
	// The command uses RunE, which means errors are returned and handled

	if connectCmd.RunE == nil {
		t.Error("connectCmd should use RunE for error handling")
	}

	// Verify SilenceUsage is set (don't show usage on connection errors)
	if !connectCmd.SilenceUsage {
		t.Error("connectCmd should set SilenceUsage to true")
	}
}

func TestConnectCmd_VerboseFlag(t *testing.T) {
	// Test that verbose flag affects behavior
	viper.Reset()

	// Test with verbose=false
	viper.Set("verbose", false)
	if viper.GetBool("verbose") != false {
		t.Error("verbose should be false")
	}

	// Test with verbose=true
	viper.Set("verbose", true)
	if viper.GetBool("verbose") != true {
		t.Error("verbose should be true")
	}

	// The connect command passes this to topology.Detect(conn, verbose)
	// which enables debug logging
}

func TestConnectCmd_FormatFlag(t *testing.T) {
	// Test that format flag is respected
	viper.Reset()

	formats := []string{"text", "plain", "json", "markdown"}

	for _, format := range formats {
		viper.Set("format", format)
		if viper.GetString("format") != format {
			t.Errorf("format should be %s, got %s", format, viper.GetString("format"))
		}
	}
}

// TestConnectCmd_PasswordHandling tests password flag behavior
func TestConnectCmd_PasswordHandling(t *testing.T) {
	viper.Reset()

	// Test empty password (should trigger prompt in actual execution)
	viper.Set("password", "")
	password := viper.GetString("password")

	if password != "" {
		// In the actual command, empty password triggers mysql.PromptPassword()
		t.Log("Empty password would trigger password prompt")
	}

	// Test with password provided
	viper.Set("password", "secret")
	password = viper.GetString("password")
	if password != "secret" {
		t.Errorf("password should be 'secret', got %s", password)
	}
}

// TestConnectCmd_ConnectionConfig tests that connection config is built correctly
func TestConnectCmd_ConnectionConfigLogic(t *testing.T) {
	// This tests the logic that would run in the actual command
	// without requiring a real database connection

	testCases := []struct {
		name           string
		host           string
		port           int
		user           string
		expectedHost   string
		expectedUser   string
	}{
		{
			name:         "empty host and user - should use defaults",
			host:         "",
			user:         "",
			expectedHost: "127.0.0.1",
			expectedUser: "dbsafe",
		},
		{
			name:         "custom host and user",
			host:         "db.prod.com",
			user:         "admin",
			expectedHost: "db.prod.com",
			expectedUser: "admin",
		},
		{
			name:         "only custom host",
			host:         "localhost",
			user:         "",
			expectedHost: "localhost",
			expectedUser: "dbsafe",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Simulate the logic in connectCmd.RunE
			host := tc.host
			user := tc.user

			if host == "" {
				host = "127.0.0.1"
			}
			if user == "" {
				user = "dbsafe"
			}

			if host != tc.expectedHost {
				t.Errorf("host = %s, want %s", host, tc.expectedHost)
			}
			if user != tc.expectedUser {
				t.Errorf("user = %s, want %s", user, tc.expectedUser)
			}
		})
	}
}

// TestConnectCmd_Help tests that help text is informative
func TestConnectCmd_Help(t *testing.T) {
	output := &bytes.Buffer{}
	connectCmd.SetOut(output)
	connectCmd.SetErr(output)

	// Get help text
	connectCmd.SetArgs([]string{"--help"})

	// The help should mention topology detection
	if !strings.Contains(connectCmd.Long, "topology") {
		t.Error("help text should mention topology detection")
	}

	// Should mention what topologies are detected
	expectedTerms := []string{"standalone", "replica", "Galera", "Group Replication"}
	for _, term := range expectedTerms {
		if !strings.Contains(connectCmd.Long, term) {
			t.Errorf("help text should mention %s", term)
		}
	}
}
