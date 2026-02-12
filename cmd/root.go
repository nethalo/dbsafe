package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

var rootCmd = &cobra.Command{
	Use:   "dbsafe",
	Short: "Pre-execution safety analysis for MySQL DDL/DML operations",
	Long: `dbsafe analyzes MySQL DDL and DML statements before you run them.

It tells you exactly what will happen: locking behavior, algorithm choice,
replication impact, row count, and whether you need gh-ost, pt-osc, or
chunked execution. It generates rollback plans so you're never stuck.

Know exactly what your DDL/DML will do before you run it. No guesses.`,
}

// Execute is called by main.main(). It adds all child commands to the root
// command and sets flags appropriately.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	// Global flags
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.dbsafe/config.yaml)")
	rootCmd.PersistentFlags().StringP("host", "H", "", "MySQL host")
	rootCmd.PersistentFlags().IntP("port", "P", 3306, "MySQL port")
	rootCmd.PersistentFlags().StringP("user", "u", "", "MySQL user")
	rootCmd.PersistentFlags().StringP("password", "p", "", "MySQL password (will prompt if flag present without value)")
	rootCmd.PersistentFlags().Lookup("password").NoOptDefVal = "" // Allow -p without value to trigger prompt
	rootCmd.PersistentFlags().StringP("database", "d", "", "Target database")
	rootCmd.PersistentFlags().StringP("socket", "S", "", "Unix socket path")
	rootCmd.PersistentFlags().StringP("format", "f", "text", "Output format: text, plain, json, markdown")
	rootCmd.PersistentFlags().BoolP("verbose", "v", false, "Show additional debug info")

	// Bind flags to viper
	viper.BindPFlag("host", rootCmd.PersistentFlags().Lookup("host"))
	viper.BindPFlag("port", rootCmd.PersistentFlags().Lookup("port"))
	viper.BindPFlag("user", rootCmd.PersistentFlags().Lookup("user"))
	viper.BindPFlag("database", rootCmd.PersistentFlags().Lookup("database"))
	viper.BindPFlag("socket", rootCmd.PersistentFlags().Lookup("socket"))
	viper.BindPFlag("format", rootCmd.PersistentFlags().Lookup("format"))
	viper.BindPFlag("verbose", rootCmd.PersistentFlags().Lookup("verbose"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		if err != nil {
			return
		}
		viper.AddConfigPath(home + "/.dbsafe")
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
	}

	viper.SetEnvPrefix("DBSAFE")
	viper.AutomaticEnv()

	// Silently ignore missing config file — it's optional
	if err := viper.ReadInConfig(); err == nil {
		// Map nested config structure to flat keys that flags expect
		// Only set these if the flags haven't been explicitly set by the user
		if !rootCmd.PersistentFlags().Changed("host") && viper.IsSet("connections.default.host") {
			viper.Set("host", viper.GetString("connections.default.host"))
		}
		if !rootCmd.PersistentFlags().Changed("port") && viper.IsSet("connections.default.port") {
			viper.Set("port", viper.GetInt("connections.default.port"))
		}
		if !rootCmd.PersistentFlags().Changed("user") && viper.IsSet("connections.default.user") {
			viper.Set("user", viper.GetString("connections.default.user"))
		}
		if !rootCmd.PersistentFlags().Changed("database") && viper.IsSet("connections.default.database") {
			viper.Set("database", viper.GetString("connections.default.database"))
		}
		if !rootCmd.PersistentFlags().Changed("format") && viper.IsSet("defaults.format") {
			viper.Set("format", viper.GetString("defaults.format"))
		}
	}
}
