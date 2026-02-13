package cmd

import (
	"fmt"
	"os"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/output"
	"github.com/nethalo/dbsafe/internal/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var connectCmd = &cobra.Command{
	Use:          "connect",
	Short:        "Test connection and show topology info",
	SilenceUsage: true, // Don't show usage on errors
	Long:         `Connect to a MySQL instance, detect topology (standalone, replica, Galera/PXC, Group Replication), and display cluster state.`,
	RunE: func(cmd *cobra.Command, args []string) error {
		connCfg := mysql.ConnectionConfig{
			Host:     viper.GetString("host"),
			Port:     viper.GetInt("port"),
			User:     viper.GetString("user"),
			Password: viper.GetString("password"),
			Database: viper.GetString("database"),
			Socket:   viper.GetString("socket"),
		}

		if connCfg.Host == "" && connCfg.Socket == "" {
			connCfg.Host = "127.0.0.1"
		}
		if connCfg.User == "" {
			connCfg.User = "dbsafe"
		}

		// Prompt for password if not provided
		if connCfg.Password == "" {
			connCfg.Password = mysql.PromptPassword()
		}

		conn, err := mysql.Connect(connCfg)
		if err != nil {
			return fmt.Errorf("connection failed: %w", err)
		}
		defer conn.Close()

		// Detect topology
		topo, err := topology.Detect(conn)
		if err != nil {
			return fmt.Errorf("topology detection failed: %w", err)
		}

		// Render output
		format := viper.GetString("format")
		renderer := output.NewRenderer(format, os.Stdout)
		renderer.RenderTopology(connCfg, topo)

		return nil
	},
}

func init() {
	rootCmd.AddCommand(connectCmd)
}
