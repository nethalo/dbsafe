package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/nethalo/dbsafe/internal/analyzer"
	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/output"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var planCmd = &cobra.Command{
	Use:   "plan [SQL statement]",
	Short: "Analyze a DDL or DML statement before execution",
	Long: `Analyze a MySQL DDL or DML statement and report:
  - Operation classification (INSTANT, INPLACE, COPY)
  - Locking behavior
  - Replication impact
  - Affected row count (for DML)
  - Execution method recommendation (native, gh-ost, pt-osc, chunked)
  - Rollback plan`,
	Args: cobra.MaximumNArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		// Get SQL from args or --file flag
		sqlText, err := getSQLInput(cmd, args)
		if err != nil {
			return err
		}

		// Parse the SQL
		parsed, err := parser.Parse(sqlText)
		if err != nil {
			return fmt.Errorf("SQL parse error: %w", err)
		}

		// Build connection config
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

		// Use database from parsed SQL if not specified via flag
		if connCfg.Database == "" && parsed.Database != "" {
			connCfg.Database = parsed.Database
		}

		// Require a database to be specified
		if connCfg.Database == "" {
			return fmt.Errorf("database not specified: use -d flag or specify database in SQL (e.g., ALTER TABLE mydb.users ...)")
		}

		// Prompt for password if not provided
		if connCfg.Password == "" {
			connCfg.Password = mysql.PromptPassword()
		}

		// Connect
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

		// Collect table metadata
		meta, err := mysql.GetTableMetadata(conn, connCfg.Database, parsed.Table)
		if err != nil {
			return fmt.Errorf("metadata collection failed: %w", err)
		}

		// Get server version
		version, err := mysql.GetServerVersion(conn)
		if err != nil {
			return fmt.Errorf("version detection failed: %w", err)
		}

		// Run analysis
		chunkSize, _ := cmd.Flags().GetInt("chunk-size")
		result := analyzer.Analyze(analyzer.Input{
			Parsed:    parsed,
			Meta:      meta,
			Topo:      topo,
			Version:   version,
			ChunkSize: chunkSize,
			Connection: &analyzer.ConnectionInfo{
				Host:   connCfg.Host,
				Port:   connCfg.Port,
				User:   connCfg.User,
				Socket: connCfg.Socket,
			},
		})

		// Render output
		format := viper.GetString("format")
		renderer := output.NewRenderer(format, os.Stdout)
		renderer.RenderPlan(result)

		// Write generated scripts if any
		if result.GeneratedScript != "" {
			scriptPath := result.ScriptPath
			if err := os.WriteFile(scriptPath, []byte(result.GeneratedScript), 0644); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: could not write script to %s: %v\n", scriptPath, err)
			}
		}

		return nil
	},
}

func init() {
	rootCmd.AddCommand(planCmd)
	planCmd.Flags().String("file", "", "Read SQL from file instead of argument")
	planCmd.Flags().Int("chunk-size", 10000, "Override default chunk size for DML recommendations")
}

func getSQLInput(cmd *cobra.Command, args []string) (string, error) {
	filePath, _ := cmd.Flags().GetString("file")

	if filePath != "" {
		data, err := os.ReadFile(filePath)
		if err != nil {
			return "", fmt.Errorf("could not read file %s: %w", filePath, err)
		}
		return strings.TrimSpace(string(data)), nil
	}

	if len(args) > 0 {
		return strings.TrimSpace(args[0]), nil
	}

	return "", fmt.Errorf("provide a SQL statement as argument or use --file flag")
}
