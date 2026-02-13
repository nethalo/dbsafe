package output

import (
	"fmt"
	"io"

	"github.com/nethalo/dbsafe/internal/analyzer"
	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// MarkdownRenderer produces markdown output for documentation/tickets.
type MarkdownRenderer struct {
	w io.Writer
}

func (r *MarkdownRenderer) RenderPlan(result *analyzer.Result) {
	fmt.Fprintf(r.w, "# dbsafe — %s Analysis\n\n", result.StatementType)
	fmt.Fprintf(r.w, "**Statement:** `%s`\n\n", result.Statement)

	// Table metadata
	fmt.Fprintf(r.w, "## Table Metadata\n\n")
	fmt.Fprintf(r.w, "| Property | Value |\n|---|---|\n")
	fmt.Fprintf(r.w, "| Table | `%s.%s` |\n", result.Database, result.Table)
	fmt.Fprintf(r.w, "| Size | %s |\n", result.TableMeta.TotalSizeHuman())
	fmt.Fprintf(r.w, "| Row count | ~%s |\n", formatNumber(result.TableMeta.RowCount))
	fmt.Fprintf(r.w, "| Indexes | %d |\n", len(result.TableMeta.Indexes))
	fmt.Fprintf(r.w, "| Foreign keys | %d |\n", len(result.TableMeta.ForeignKeys))
	fmt.Fprintf(r.w, "| Triggers | %d |\n", len(result.TableMeta.Triggers))
	fmt.Fprintf(r.w, "| Engine | %s |\n", result.TableMeta.Engine)
	fmt.Fprintf(r.w, "| MySQL version | %s |\n\n", result.Version.String())

	// Topology
	if result.Topology.Type != topology.Standalone {
		fmt.Fprintf(r.w, "## Topology\n\n")
		fmt.Fprintf(r.w, "| Property | Value |\n|---|---|\n")
		fmt.Fprintf(r.w, "| Type | %s |\n", formatTopoType(result.Topology))
		switch result.Topology.Type {
		case topology.Galera:
			fmt.Fprintf(r.w, "| OSU method | %s |\n", result.Topology.GaleraOSUMethod)
			fmt.Fprintf(r.w, "| Node state | %s |\n", result.Topology.GaleraNodeState)
			fmt.Fprintf(r.w, "| Flow control | %s |\n", result.Topology.FlowControlPausedPct)
		case topology.GroupRepl:
			fmt.Fprintf(r.w, "| Mode | %s |\n", result.Topology.GRMode)
			fmt.Fprintf(r.w, "| Members | %d |\n", result.Topology.GRMemberCount)
		}
		fmt.Fprintln(r.w)
	}

	// Operation
	fmt.Fprintf(r.w, "## Operation\n\n")
	if result.StatementType == parser.DDL {
		fmt.Fprintf(r.w, "| Property | Value |\n|---|---|\n")
		fmt.Fprintf(r.w, "| Type | %s |\n", result.DDLOp)
		fmt.Fprintf(r.w, "| Algorithm | **%s** |\n", result.Classification.Algorithm)
		fmt.Fprintf(r.w, "| Lock | %s |\n", result.Classification.Lock)
		fmt.Fprintf(r.w, "| Rebuilds table | %v |\n\n", result.Classification.RebuildsTable)
	} else {
		fmt.Fprintf(r.w, "| Property | Value |\n|---|---|\n")
		fmt.Fprintf(r.w, "| Type | %s |\n", result.DMLOp)
		fmt.Fprintf(r.w, "| Affected rows | ~%s (%.1f%%) |\n", formatNumber(result.AffectedRows), result.AffectedPct)
		if result.WriteSetSize > 0 {
			fmt.Fprintf(r.w, "| Write-set estimate | %s |\n", humanBytes(result.WriteSetSize))
		}
		fmt.Fprintln(r.w)
	}

	// Warnings
	if len(result.Warnings) > 0 || len(result.ClusterWarnings) > 0 {
		fmt.Fprintf(r.w, "## ⚠ Warnings\n\n")
		for _, w := range result.Warnings {
			fmt.Fprintf(r.w, "- **Warning:** %s\n", w)
		}
		for _, w := range result.ClusterWarnings {
			fmt.Fprintf(r.w, "- **Cluster:** %s\n", w)
		}
		fmt.Fprintln(r.w)
	}

	// Recommendation
	riskEmoji := map[analyzer.RiskLevel]string{
		analyzer.RiskSafe:      "✅",
		analyzer.RiskCaution:   "⚠️",
		analyzer.RiskDangerous: "❌",
	}
	fmt.Fprintf(r.w, "## %s Recommendation: %s\n\n", riskEmoji[result.Risk], result.Risk)
	fmt.Fprintf(r.w, "**Method:** %s\n\n", result.Method)
	fmt.Fprintf(r.w, "%s\n\n", result.Recommendation)

	// Execution command (if available)
	if result.ExecutionCommand != "" {
		fmt.Fprintf(r.w, "## 🚀 Execution Command\n\n")
		fmt.Fprintf(r.w, "Ready-to-run command (review and adjust as needed):\n\n")
		fmt.Fprintf(r.w, "```bash\n%s\n```\n\n", result.ExecutionCommand)
	}

	// Rollback
	fmt.Fprintf(r.w, "## Rollback\n\n")
	if result.RollbackSQL != "" {
		fmt.Fprintf(r.w, "```sql\n%s\n```\n\n", result.RollbackSQL)
	}
	if result.RollbackNotes != "" {
		fmt.Fprintf(r.w, "%s\n\n", result.RollbackNotes)
	}
	for _, opt := range result.RollbackOptions {
		fmt.Fprintf(r.w, "### %s\n\n%s\n\n", opt.Label, opt.Description)
		if opt.SQL != "" {
			fmt.Fprintf(r.w, "```sql\n%s\n```\n\n", opt.SQL)
		}
	}

	if result.GeneratedScript != "" {
		fmt.Fprintf(r.w, "---\n\n*Chunked script written to: `%s`*\n", result.ScriptPath)
	}
}

func (r *MarkdownRenderer) RenderTopology(conn mysql.ConnectionConfig, topo *topology.Info) {
	addr := fmt.Sprintf("%s:%d", conn.Host, conn.Port)
	if conn.Socket != "" {
		addr = conn.Socket
	}

	fmt.Fprintf(r.w, "# dbsafe — Connection Info\n\n")
	fmt.Fprintf(r.w, "| Property | Value |\n|---|---|\n")
	fmt.Fprintf(r.w, "| Host | `%s` |\n", addr)
	fmt.Fprintf(r.w, "| Version | %s |\n", topo.Version.String())
	fmt.Fprintf(r.w, "| Topology | %s |\n", formatTopoType(topo))
	fmt.Fprintf(r.w, "| Read only | %v |\n", topo.ReadOnly)
}
