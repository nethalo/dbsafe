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
	if result.Topology.Type != topology.Standalone || result.Topology.IsCloudManaged {
		fmt.Fprintf(r.w, "--- Topology ---\n")
		fmt.Fprintf(r.w, "Type:          %s\n", formatTopoType(result.Topology))
		if result.Topology.IsCloudManaged {
			fmt.Fprintf(r.w, "Cloud:         %s\n", result.Topology.CloudProvider)
		}
		fmt.Fprintln(r.w)
	}

	// For unparsable DDL, only show warnings
	if result.DDLOp == parser.OtherDDL {
		for _, w := range result.Warnings {
			fmt.Fprintf(r.w, "WARNING: %s\n", w)
		}
		return
	}

	// Operation
	fmt.Fprintf(r.w, "--- Operation ---\n")
	if result.StatementType == parser.DDL {
		fmt.Fprintf(r.w, "Type:          %s\n", result.DDLOp)
		if len(result.SubOpResults) > 0 {
			var parts []string
			for _, sr := range result.SubOpResults {
				parts = append(parts, fmt.Sprintf("%s (%s/%s)", sr.Op, sr.Classification.Algorithm, sr.Classification.Lock))
			}
			fmt.Fprintf(r.w, "Sub-ops:       %s\n", strings.Join(parts, ", "))
		}
		fmt.Fprintf(r.w, "Algorithm:     %s\n", result.Classification.Algorithm)
		fmt.Fprintf(r.w, "Lock:          %s\n", result.Classification.Lock)
		fmt.Fprintf(r.w, "Rebuilds:      %v\n", result.Classification.RebuildsTable)
		if result.OptimizedDDL != "" {
			fmt.Fprintf(r.w, "Suggested DDL: %s\n", result.OptimizedDDL)
		}
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
	fmt.Fprintf(r.w, "%s\n", result.Recommendation)
	if result.DiskEstimate != nil {
		fmt.Fprintf(r.w, "Disk required: ~%s (%s)\n", result.DiskEstimate.RequiredHuman, result.DiskEstimate.Reason)
	}
	fmt.Fprintln(r.w)

	// Execution command(s) (if available)
	if result.ExecutionCommand != "" {
		fmt.Fprintf(r.w, "--- Execution Commands ---\n")
		if result.AlternativeMethod != "" {
			fmt.Fprintf(r.w, "Option 1 (Recommended): %s\n%s\n\n", result.Method, result.ExecutionCommand)
			fmt.Fprintf(r.w, "Option 2: %s\n", result.AlternativeMethod)
			if result.AlternativeExecutionCommand != "" {
				fmt.Fprintf(r.w, "%s\n", result.AlternativeExecutionCommand)
			}
			if result.MethodRationale != "" {
				fmt.Fprintf(r.w, "\n%s\n", result.MethodRationale)
			}
		} else {
			fmt.Fprintf(r.w, "%s\n", result.ExecutionCommand)
			if result.MethodRationale != "" {
				fmt.Fprintf(r.w, "\n%s\n", result.MethodRationale)
			}
		}
		fmt.Fprintln(r.w)
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

	// Idempotent stored procedure
	if result.IdempotentSP != "" {
		fmt.Fprintf(r.w, "\n--- Idempotent Procedure ---\n")
		fmt.Fprintf(r.w, "%s\n", result.IdempotentSP)
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

}
