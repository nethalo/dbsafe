package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
)

const versionTemplate = `dbsafe {{.Version}}

Supported MySQL versions:
  • MySQL 8.0.0 – 8.0.x (including Percona Server 8.0)
  • MySQL 8.4 LTS (including Percona Server 8.4)
  • Percona XtraDB Cluster 8.0 / 8.4
  • MySQL Group Replication 8.0 / 8.4

MySQL 5.7 is not supported (EOL October 2023).
`

// Version is set at build time via ldflags
var (
	Version   = "dev"
	CommitSHA = "none"
	BuildDate = "unknown"
)

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print dbsafe version and supported MySQL versions",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Printf("dbsafe %s (commit: %s, built: %s)\n\n", Version, CommitSHA, BuildDate)
		fmt.Println("Supported MySQL versions:")
		fmt.Println("  • MySQL 8.0.0 – 8.0.x (including Percona Server 8.0)")
		fmt.Println("  • MySQL 8.4 LTS (including Percona Server 8.4)")
		fmt.Println("  • Percona XtraDB Cluster 8.0 / 8.4")
		fmt.Println("  • MySQL Group Replication 8.0 / 8.4")
		fmt.Println()
		fmt.Println("MySQL 5.7 is not supported (EOL October 2023).")
	},
}

func init() {
	rootCmd.AddCommand(versionCmd)

	// Enable the standard --version flag, matching the `version` subcommand output.
	rootCmd.Version = fmt.Sprintf("%s (commit: %s, built: %s)", Version, CommitSHA, BuildDate)
	rootCmd.SetVersionTemplate(versionTemplate)
}
