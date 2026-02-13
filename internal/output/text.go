package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/charmbracelet/lipgloss"
	"github.com/nethalo/dbsafe/internal/analyzer"
	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// TextRenderer produces Lip Gloss styled terminal output.
type TextRenderer struct {
	w io.Writer
}

func (r *TextRenderer) RenderPlan(result *analyzer.Result) {
	width := 60

	// Header
	header := TitleStyle.Render(fmt.Sprintf("dbsafe — %s Analysis", result.StatementType))
	fmt.Fprintln(r.w)

	// Table metadata box
	metaLines := []string{
		r.labelValue("Table:", fmt.Sprintf("%s.%s", result.Database, result.Table)),
		r.labelValue("Table size:", result.TableMeta.TotalSizeHuman()),
		r.labelValue("Row count:", fmt.Sprintf("~%s", formatNumber(result.TableMeta.RowCount))),
		r.labelValue("Indexes:", fmt.Sprintf("%d", len(result.TableMeta.Indexes))),
		r.labelValue("FK refs:", formatFKRefs(result.TableMeta.ForeignKeys)),
		r.labelValue("Triggers:", formatTriggers(result.TableMeta.Triggers)),
		r.labelValue("Engine:", result.TableMeta.Engine),
	}
	metaBox := BoxStyle.Width(width).Render(header + "\n" + strings.Join(metaLines, "\n"))
	fmt.Fprintln(r.w, metaBox)

	// Topology box (if not standalone)
	if result.Topology.Type != topology.Standalone {
		r.renderTopoBox(result, width)
	}

	// Operation box
	r.renderOperationBox(result, width)

	// Cluster warnings
	if len(result.ClusterWarnings) > 0 {
		r.renderClusterWarnings(result, width)
	}

	// Warnings
	if len(result.Warnings) > 0 {
		for _, w := range result.Warnings {
			warnBox := WarningBoxStyle.Width(width).Render(
				WarningText.Render(IconWarning+" Warning") + "\n" + w,
			)
			fmt.Fprintln(r.w, warnBox)
		}
	}

	// Recommendation box
	r.renderRecommendation(result, width)

	// Execution command box (if generated)
	if result.ExecutionCommand != "" {
		r.renderExecutionCommand(result, width)
	}

	// Rollback box
	r.renderRollback(result, width)

	// Script generated note
	if result.GeneratedScript != "" {
		note := MutedText.Render(fmt.Sprintf("Chunked script written to: %s", result.ScriptPath))
		fmt.Fprintln(r.w, note)
	}

	fmt.Fprintln(r.w)
}

func (r *TextRenderer) renderTopoBox(result *analyzer.Result, width int) {
	var lines []string
	lines = append(lines, r.labelValue("Type:", formatTopoType(result.Topology)))

	switch result.Topology.Type {
	case topology.Galera:
		lines = append(lines, r.labelValue("OSU method:", result.Topology.GaleraOSUMethod))
		lines = append(lines, r.labelValue("Node state:", result.Topology.GaleraNodeState))
		lines = append(lines, r.labelValue("Flow control:", result.Topology.FlowControlPausedPct))
	case topology.GroupRepl:
		lines = append(lines, r.labelValue("Mode:", result.Topology.GRMode))
		lines = append(lines, r.labelValue("Members:", fmt.Sprintf("%d", result.Topology.GRMemberCount)))
		lines = append(lines, r.labelValue("Role:", result.Topology.GRMemberRole))
	case topology.AsyncReplica, topology.SemiSyncReplica:
		if result.Topology.ReplicaLagSecs != nil {
			lines = append(lines, r.labelValue("Replica lag:", fmt.Sprintf("%ds", *result.Topology.ReplicaLagSecs)))
		}
	}

	title := TitleStyle.Render("Topology")
	topoBox := BoxStyle.Width(width).Render(title + "\n" + strings.Join(lines, "\n"))
	fmt.Fprintln(r.w, topoBox)
}

func (r *TextRenderer) renderOperationBox(result *analyzer.Result, width int) {
	var lines []string

	if result.StatementType == parser.DDL {
		lines = append(lines, r.labelValue("Type:", string(result.DDLOp)))
		lines = append(lines, r.labelValue("Algorithm:", r.colorAlgorithm(result.Classification.Algorithm)))
		lines = append(lines, r.labelValue("Lock:", string(result.Classification.Lock)))
		lines = append(lines, r.labelValue("Rebuilds table:", fmt.Sprintf("%v", result.Classification.RebuildsTable)))
	} else {
		lines = append(lines, r.labelValue("Type:", string(result.DMLOp)))
		lines = append(lines, r.labelValue("Affected rows:", fmt.Sprintf("~%s (%.1f%%)", formatNumber(result.AffectedRows), result.AffectedPct)))
		if result.WriteSetSize > 0 {
			lines = append(lines, r.labelValue("Write-set est:", humanBytes(result.WriteSetSize)))
		}
		if result.Method == analyzer.ExecChunked {
			lines = append(lines, r.labelValue("Chunks:", fmt.Sprintf("%d × %d rows", result.ChunkCount, result.ChunkSize)))
		}
	}

	title := TitleStyle.Render("Operation")
	opBox := BoxStyle.Width(width).Render(title + "\n" + strings.Join(lines, "\n"))
	fmt.Fprintln(r.w, opBox)
}

func (r *TextRenderer) renderClusterWarnings(result *analyzer.Result, width int) {
	var content strings.Builder
	content.WriteString(WarningText.Render(IconWarning + " Cluster Warning"))
	content.WriteString("\n")
	for _, w := range result.ClusterWarnings {
		content.WriteString("\n" + w)
	}
	warnBox := WarningBoxStyle.Width(width).Render(content.String())
	fmt.Fprintln(r.w, warnBox)
}

func (r *TextRenderer) renderRecommendation(result *analyzer.Result, width int) {
	var icon, label string
	var style lipgloss.Style

	switch result.Risk {
	case analyzer.RiskSafe:
		icon = IconSafe
		label = "Safe to run directly."
		style = SafeBoxStyle
	case analyzer.RiskCaution:
		icon = IconWarning
		label = "Proceed with caution."
		style = WarningBoxStyle
	case analyzer.RiskDangerous:
		icon = IconDanger
		label = "Dangerous — action required."
		style = DangerBoxStyle
	}

	title := TitleStyle.Render("Recommendation")
	content := fmt.Sprintf("%s\n%s %s\n\n%s\n\nMethod: %s", title, icon, label, result.Recommendation, result.Method)
	recBox := style.Width(width).Render(content)
	fmt.Fprintln(r.w, recBox)
}

func (r *TextRenderer) renderExecutionCommand(result *analyzer.Result, width int) {
	title := TitleStyle.Render("Execution Command")
	note := MutedText.Render("Ready-to-run command (review and adjust as needed):")
	content := fmt.Sprintf("%s\n%s\n\n%s", title, note, result.ExecutionCommand)
	cmdBox := BoxStyle.Width(width).Render(content)
	fmt.Fprintln(r.w, cmdBox)
}

func (r *TextRenderer) renderRollback(result *analyzer.Result, width int) {
	title := TitleStyle.Render("Rollback")

	var content strings.Builder
	content.WriteString(title + "\n")

	if result.RollbackSQL != "" {
		content.WriteString(CodeStyle.Render(result.RollbackSQL))
		content.WriteString("\n")
	}

	if result.RollbackNotes != "" {
		content.WriteString("\n" + MutedText.Render(result.RollbackNotes))
	}

	for _, opt := range result.RollbackOptions {
		content.WriteString("\n\n" + WarningText.Render(opt.Label))
		content.WriteString("\n" + opt.Description)
		if opt.SQL != "" {
			content.WriteString("\n" + CodeStyle.Render(opt.SQL))
		}
	}

	rollbackBox := BoxStyle.Width(width).Render(content.String())
	fmt.Fprintln(r.w, rollbackBox)
}

func (r *TextRenderer) RenderTopology(conn mysql.ConnectionConfig, topo *topology.Info) {
	width := 60
	fmt.Fprintln(r.w)

	var lines []string
	addr := fmt.Sprintf("%s:%d", conn.Host, conn.Port)
	if conn.Socket != "" {
		addr = conn.Socket
	}
	lines = append(lines, r.labelValue("Connected to:", addr))
	lines = append(lines, r.labelValue("Server version:", topo.Version.String()))
	lines = append(lines, r.labelValue("Topology:", formatTopoType(topo)))

	switch topo.Type {
	case topology.Galera:
		lines = append(lines, r.labelValue("Cluster size:", fmt.Sprintf("%d nodes", topo.GaleraClusterSize)))
		lines = append(lines, r.labelValue("Node state:", topo.GaleraNodeState))
		lines = append(lines, r.labelValue("wsrep_OSU_method:", topo.GaleraOSUMethod))
		lines = append(lines, r.labelValue("wsrep_max_ws_size:", fmt.Sprintf("%d (%s)", topo.WsrepMaxWsSize, humanBytes(topo.WsrepMaxWsSize))))
		lines = append(lines, r.labelValue("Flow control:", topo.FlowControlPausedPct))
	case topology.GroupRepl:
		lines = append(lines, r.labelValue("Mode:", topo.GRMode))
		lines = append(lines, r.labelValue("Members:", fmt.Sprintf("%d online", topo.GRMemberCount)))
		lines = append(lines, r.labelValue("Role:", topo.GRMemberRole))
		if topo.GRTransactionLimit > 0 {
			lines = append(lines, r.labelValue("TX size limit:", humanBytes(topo.GRTransactionLimit)))
		}
	case topology.AsyncReplica, topology.SemiSyncReplica:
		if topo.IsReplica {
			lag := "N/A"
			if topo.ReplicaLagSecs != nil {
				lag = fmt.Sprintf("%d seconds", *topo.ReplicaLagSecs)
			}
			lines = append(lines, r.labelValue("Replica lag:", lag))
		}
		if topo.IsPrimary {
			lines = append(lines, r.labelValue("Role:", "Primary (has replicas)"))
		}
	}

	lines = append(lines, r.labelValue("Read only:", fmt.Sprintf("%v", topo.ReadOnly)))

	title := TitleStyle.Render("dbsafe — Connection Info")
	box := SafeBoxStyle.Width(width).Render(title + "\n" + strings.Join(lines, "\n"))
	fmt.Fprintln(r.w, box)
	fmt.Fprintln(r.w)
}

// helpers

func (r *TextRenderer) labelValue(label, value string) string {
	return LabelStyle.Render(label) + " " + ValueStyle.Render(value)
}

func (r *TextRenderer) colorAlgorithm(algo analyzer.Algorithm) string {
	switch algo {
	case analyzer.AlgoInstant:
		return SafeText.Render(string(algo))
	case analyzer.AlgoInplace:
		return WarningText.Render(string(algo))
	case analyzer.AlgoCopy:
		return DangerText.Render(string(algo))
	default:
		return string(algo)
	}
}

func formatTopoType(topo *topology.Info) string {
	switch topo.Type {
	case topology.Galera:
		return fmt.Sprintf("Percona XtraDB Cluster (%d nodes)", topo.GaleraClusterSize)
	case topology.GroupRepl:
		return fmt.Sprintf("Group Replication (%s, %d members)", topo.GRMode, topo.GRMemberCount)
	case topology.AsyncReplica:
		return "Async Replication"
	case topology.SemiSyncReplica:
		return "Semi-sync Replication"
	default:
		return "Standalone"
	}
}

func formatFKRefs(fks []mysql.ForeignKeyInfo) string {
	if len(fks) == 0 {
		return "None"
	}
	var refs []string
	for _, fk := range fks {
		refs = append(refs, fmt.Sprintf("%s.%s", fk.ReferencedTable, strings.Join(fk.ReferencedCols, ",")))
	}
	return fmt.Sprintf("%d (%s)", len(fks), strings.Join(refs, ", "))
}

func formatTriggers(triggers []mysql.TriggerInfo) string {
	if len(triggers) == 0 {
		return "None"
	}
	var names []string
	for _, t := range triggers {
		names = append(names, fmt.Sprintf("%s %s → %s", t.Timing, t.Event, t.Name))
	}
	return fmt.Sprintf("%d (%s)", len(triggers), strings.Join(names, ", "))
}

func formatNumber(n int64) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.0f,000,000,000+", float64(n)/1_000_000_000)
	}
	// Simple comma formatting
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var result strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			result.WriteRune(',')
		}
		result.WriteRune(c)
	}
	return result.String()
}

func humanBytes(b int64) string {
	const (
		KB = 1024
		MB = KB * 1024
		GB = MB * 1024
	)
	switch {
	case b >= GB:
		return fmt.Sprintf("%.1f GB", float64(b)/float64(GB))
	case b >= MB:
		return fmt.Sprintf("%.1f MB", float64(b)/float64(MB))
	case b >= KB:
		return fmt.Sprintf("%.1f KB", float64(b)/float64(KB))
	default:
		return fmt.Sprintf("%d B", b)
	}
}
