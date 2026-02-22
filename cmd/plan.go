package cmd

import (
	"fmt"
	"os"
	"path/filepath"
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
	Use:          "plan [SQL statement]",
	Short:        "Analyze a DDL or DML statement before execution",
	SilenceUsage: true, // Don't show usage on errors
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

		// Check if this is an unsupported operation (INSERT/LOAD DATA/CREATE TABLE)
		if (parsed.Type == parser.DML && (parsed.DMLOp == parser.Insert || parsed.DMLOp == parser.LoadData)) ||
			(parsed.Type == parser.DDL && parsed.DDLOp == parser.CreateTable) {
			operationName := "INSERT"
			if parsed.DMLOp == parser.LoadData {
				operationName = "LOAD DATA INFILE"
			} else if parsed.DDLOp == parser.CreateTable {
				operationName = "CREATE TABLE"
			}
			fmt.Fprintf(os.Stderr, "\n⚠️  dbsafe doesn't analyze %s statements\n\n", operationName)
			fmt.Fprintf(os.Stderr, "This tool is designed to analyze the \"UD\" in CRUD (UPDATE and DELETE),\n")
			fmt.Fprintf(os.Stderr, "as well as DDL modifications like ALTER TABLE.\n\n")
			fmt.Fprintf(os.Stderr, "For %s operations, dbsafe has nothing to report. 🤷\n\n", operationName)
			return nil
		}

		// Build connection config
		connCfg := mysql.ConnectionConfig{
			Host:     viper.GetString("host"),
			Port:     viper.GetInt("port"),
			User:     viper.GetString("user"),
			Password: viper.GetString("password"),
			Database: viper.GetString("database"),
			Socket:   viper.GetString("socket"),
			TLSMode:  viper.GetString("tls"),
			TLSCA:    viper.GetString("tls_ca"),
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
		verbose := viper.GetBool("verbose")
		topo, err := topology.Detect(conn, verbose)
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

		// For DML with WHERE clause, run EXPLAIN to estimate affected rows
		var estimatedRows int64
		if parsed.Type == parser.DML && parsed.HasWhere {
			estimatedRows, err = mysql.EstimateRowsAffected(conn, parsed.RawSQL)
			if err != nil {
				// Log warning but continue with 0 estimate
				fmt.Fprintf(os.Stderr, "Warning: EXPLAIN failed: %v\n", err)
			}
		}

		// Run analysis
		chunkSize, _ := cmd.Flags().GetInt("chunk-size")
		result := analyzer.Analyze(analyzer.Input{
			Parsed:        parsed,
			Meta:          meta,
			Topo:          topo,
			Version:       version,
			ChunkSize:     chunkSize,
			EstimatedRows: estimatedRows,
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
			// Security: Use 0600 (owner read/write only) to prevent exposure of sensitive SQL
			if err := os.WriteFile(scriptPath, []byte(result.GeneratedScript), 0600); err != nil {
				fmt.Fprintf(os.Stderr, "Warning: could not write script to %s: %v\n", scriptPath, err)
			} else {
				fmt.Fprintf(os.Stderr, "✓ Chunked script written to %s (permissions: 0600)\n", scriptPath)
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

// validateSQLFilePath checks if the file path is safe to read.
// This prevents path traversal attacks and reading sensitive system files.
func validateSQLFilePath(filePath string) error {
	// Clean the path to resolve .. and . components
	cleanPath := filepath.Clean(filePath)

	// Get absolute path
	absPath, err := filepath.Abs(cleanPath)
	if err != nil {
		return fmt.Errorf("invalid file path: %w", err)
	}

	// Check if file exists and is a regular file
	fileInfo, err := os.Stat(absPath)
	if err != nil {
		return fmt.Errorf("cannot access file: %w", err)
	}

	// Ensure it's a regular file (not a directory, symlink, device, etc.)
	if !fileInfo.Mode().IsRegular() {
		return fmt.Errorf("not a regular file: %s", absPath)
	}

	// Warn if file is larger than 10MB (likely not a SQL file)
	const maxFileSize = 10 * 1024 * 1024 // 10 MB
	if fileInfo.Size() > maxFileSize {
		return fmt.Errorf("file too large (>10MB): %s - this may not be a SQL file", absPath)
	}

	// Optional: Warn about sensitive paths (but don't block, as user might have legitimate SQL files there)
	sensitivePaths := []string{"/etc/", "/sys/", "/proc/", "/dev/"}
	for _, sensitive := range sensitivePaths {
		if strings.HasPrefix(absPath, sensitive) {
			fmt.Fprintf(os.Stderr, "⚠️  Warning: Reading from system path %s\n", absPath)
			break
		}
	}

	return nil
}

func getSQLInput(cmd *cobra.Command, args []string) (string, error) {
	filePath, _ := cmd.Flags().GetString("file")

	if filePath != "" {
		// Security: Validate file path before reading
		if err := validateSQLFilePath(filePath); err != nil {
			return "", fmt.Errorf("file validation failed: %w", err)
		}

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
