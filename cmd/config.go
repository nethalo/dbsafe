package cmd

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage dbsafe configuration",
}

var configInitCmd = &cobra.Command{
	Use:          "init",
	Short:        "Create config file interactively",
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		home, err := os.UserHomeDir()
		if err != nil {
			return err
		}

		configDir := filepath.Join(home, ".dbsafe")
		configPath := filepath.Join(configDir, "config.yaml")

		// Check if config already exists
		if _, err := os.Stat(configPath); err == nil {
			fmt.Printf("Config file already exists at %s\n", configPath)
			fmt.Print("Overwrite? [y/N]: ")
			reader := bufio.NewReader(os.Stdin)
			answer, _ := reader.ReadString('\n')
			if strings.TrimSpace(strings.ToLower(answer)) != "y" {
				fmt.Println("Aborted.")
				return nil
			}
		}

		// Create config directory
		if err := os.MkdirAll(configDir, 0700); err != nil {
			return fmt.Errorf("creating config directory: %w", err)
		}

		reader := bufio.NewReader(os.Stdin)

		fmt.Println("dbsafe configuration setup")
		fmt.Println("─────────────────────────")
		fmt.Println()

		fmt.Print("MySQL host [127.0.0.1]: ")
		host, _ := reader.ReadString('\n')
		host = strings.TrimSpace(host)
		if host == "" {
			host = "127.0.0.1"
		}

		fmt.Print("MySQL port [3306]: ")
		port, _ := reader.ReadString('\n')
		port = strings.TrimSpace(port)
		if port == "" {
			port = "3306"
		}

		fmt.Print("MySQL user [dbsafe]: ")
		user, _ := reader.ReadString('\n')
		user = strings.TrimSpace(user)
		if user == "" {
			user = "dbsafe"
		}

		fmt.Print("Default database (optional): ")
		database, _ := reader.ReadString('\n')
		database = strings.TrimSpace(database)

		fmt.Print("Default output format [text]: ")
		format, _ := reader.ReadString('\n')
		format = strings.TrimSpace(format)
		if format == "" {
			format = "text"
		}

		// Build config
		var config strings.Builder
		config.WriteString("# dbsafe configuration\n")
		config.WriteString("# https://github.com/nethalo/dbsafe\n\n")

		config.WriteString("connections:\n")
		config.WriteString("  default:\n")
		config.WriteString(fmt.Sprintf("    host: %s\n", host))
		config.WriteString(fmt.Sprintf("    port: %s\n", port))
		config.WriteString(fmt.Sprintf("    user: %s\n", user))
		config.WriteString("    # password: omitted for security, will prompt\n")
		if database != "" {
			config.WriteString(fmt.Sprintf("    database: %s\n", database))
		}

		config.WriteString("\ndefaults:\n")
		config.WriteString("  chunk_size: 10000\n")
		config.WriteString("  chunk_sleep: 0.5\n")
		config.WriteString(fmt.Sprintf("  format: %s\n", format))

		if err := os.WriteFile(configPath, []byte(config.String()), 0600); err != nil {
			return fmt.Errorf("writing config: %w", err)
		}

		fmt.Printf("\n✅ Config written to %s\n", configPath)

		// Don't recommend creating root user
		if user != "root" {
			fmt.Println("\nRecommended: create a read-only MySQL user for dbsafe:")
			fmt.Println()
			fmt.Printf("  CREATE USER '%s'@'%%' IDENTIFIED BY '<password>';\n", user)
			fmt.Printf("  GRANT SELECT ON *.* TO '%s'@'%%';\n", user)
			fmt.Printf("  GRANT PROCESS ON *.* TO '%s'@'%%';\n", user)
			fmt.Printf("  GRANT REPLICATION CLIENT ON *.* TO '%s'@'%%';\n", user)
			fmt.Println()
		}

		return nil
	},
}

var configShowCmd = &cobra.Command{
	Use:   "show",
	Short: "Show current configuration",
	RunE: func(cmd *cobra.Command, args []string) error {
		configFile := viper.ConfigFileUsed()
		if configFile == "" {
			fmt.Println("No config file found.")
			fmt.Println("Run 'dbsafe config init' to create one.")
			return nil
		}

		fmt.Printf("Config file: %s\n\n", configFile)

		data, err := os.ReadFile(configFile)
		if err != nil {
			return fmt.Errorf("reading config: %w", err)
		}

		fmt.Println(string(data))
		return nil
	},
}

func init() {
	rootCmd.AddCommand(configCmd)
	configCmd.AddCommand(configInitCmd)
	configCmd.AddCommand(configShowCmd)
}
