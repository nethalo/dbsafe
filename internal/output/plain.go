package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/nethalo/dbsafe/internal/analyzer"
	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// PlainRenderer produces unformatted text output safe for piping.
type PlainRenderer struct {
	w io.Writer
}

func (r *PlainRenderer) RenderPlan(result *analyzer.Result) {
	fmt.Fprintf(r.w, "=== dbsafe — %s Analysis ===\n\n", result.StatementType)

	// Table metadata
	fmt.Fprintf(r.w, "Table:         %s.%s\n", result.Database, result.Table)
	fmt.Fprintf(r.w, "Table size:    %s\n", result.TableMeta.TotalSizeHuman())
	fmt.Fprintf(r.w, "Row count:     ~%s\n", formatNumber(result.TableMeta.RowCount))
	fmt.Fprintf(r.w, "Indexes:       %d\n", len(result.TableMeta.Indexes))
	fmt.Fprintf(r.w, "Engine:        %s\n", result.TableMeta.Engine)
	fmt.Fprintln(r.w)

	// Topology
	if result.Topology.Type != topology.Standalone {
		fmt.Fprintf(r.w, "--- Topology ---\n")
		fmt.Fprintf(r.w, "Type:          %s\n", formatTopoType(result.Topology))
		fmt.Fprintln(r.w)
	}

	// Operation
	fmt.Fprintf(r.w, "--- Operation ---\n")
	if result.StatementType == parser.DDL {
		fmt.Fprintf(r.w, "Type:          %s\n", result.DDLOp)
		fmt.Fprintf(r.w, "Algorithm:     %s\n", result.Classification.Algorithm)
		fmt.Fprintf(r.w, "Lock:          %s\n", result.Classification.Lock)
		fmt.Fprintf(r.w, "Rebuilds:      %v\n", result.Classification.RebuildsTable)
	} else {
		fmt.Fprintf(r.w, "Type:          %s\n", result.DMLOp)
		fmt.Fprintf(r.w, "Affected rows: ~%s (%.1f%%)\n", formatNumber(result.AffectedRows), result.AffectedPct)
	}
	fmt.Fprintln(r.w)

	// Warnings
	for _, w := range result.Warnings {
		fmt.Fprintf(r.w, "WARNING: %s\n", w)
	}
	for _, w := range result.ClusterWarnings {
		fmt.Fprintf(r.w, "CLUSTER WARNING: %s\n", w)
	}
	if len(result.Warnings) > 0 || len(result.ClusterWarnings) > 0 {
		fmt.Fprintln(r.w)
	}

	// Recommendation
	fmt.Fprintf(r.w, "--- Recommendation ---\n")
	fmt.Fprintf(r.w, "Risk:          %s\n", result.Risk)
	fmt.Fprintf(r.w, "Method:        %s\n", result.Method)
	fmt.Fprintf(r.w, "%s\n\n", result.Recommendation)

	// Execution command (if available)
	if result.ExecutionCommand != "" {
		fmt.Fprintf(r.w, "--- Execution Command ---\n")
		fmt.Fprintf(r.w, "%s\n\n", result.ExecutionCommand)
	}

	// Rollback
	fmt.Fprintf(r.w, "--- Rollback ---\n")
	if result.RollbackSQL != "" {
		fmt.Fprintf(r.w, "%s\n", result.RollbackSQL)
	}
	if result.RollbackNotes != "" {
		fmt.Fprintf(r.w, "%s\n", result.RollbackNotes)
	}
	for _, opt := range result.RollbackOptions {
		fmt.Fprintf(r.w, "\n[%s]\n%s\n", opt.Label, opt.Description)
		if opt.SQL != "" {
			fmt.Fprintf(r.w, "%s\n", opt.SQL)
		}
	}

	if result.GeneratedScript != "" {
		fmt.Fprintf(r.w, "\nScript written to: %s\n", result.ScriptPath)
	}
}

func (r *PlainRenderer) RenderTopology(conn mysql.ConnectionConfig, topo *topology.Info) {
	addr := fmt.Sprintf("%s:%d", conn.Host, conn.Port)
	if conn.Socket != "" {
		addr = conn.Socket
	}

	fmt.Fprintf(r.w, "=== dbsafe — Connection Info ===\n\n")
	fmt.Fprintf(r.w, "Connected to:  %s\n", addr)
	fmt.Fprintf(r.w, "Version:       %s\n", topo.Version.String())
	fmt.Fprintf(r.w, "Topology:      %s\n", formatTopoType(topo))
	fmt.Fprintf(r.w, "Read only:     %v\n", topo.ReadOnly)

	switch topo.Type {
	case topology.Galera:
		fmt.Fprintf(r.w, "Cluster size:  %d nodes\n", topo.GaleraClusterSize)
		fmt.Fprintf(r.w, "Node state:    %s\n", topo.GaleraNodeState)
		fmt.Fprintf(r.w, "OSU method:    %s\n", topo.GaleraOSUMethod)
		fmt.Fprintf(r.w, "Flow control:  %s\n", topo.FlowControlPausedPct)
	case topology.GroupRepl:
		fmt.Fprintf(r.w, "Mode:          %s\n", topo.GRMode)
		fmt.Fprintf(r.w, "Members:       %d\n", topo.GRMemberCount)
	}

	_ = strings.Join // suppress unused import if needed
}
