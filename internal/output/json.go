package output

import (
	"encoding/json"
	"io"

	"github.com/nethalo/dbsafe/internal/analyzer"
	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// JSONRenderer produces machine-readable JSON output.
type JSONRenderer struct {
	w io.Writer
}

type jsonPlanOutput struct {
	Statement string `json:"statement"`
	Type      string `json:"type"`
	Database  string `json:"database"`
	Table     string `json:"table"`
	Version   string `json:"mysql_version"`

	TableMeta jsonTableMeta  `json:"table_metadata"`
	Topology  jsonTopology   `json:"topology"`
	Operation jsonOperation  `json:"operation"`
	Risk      string         `json:"risk"`
	Method    string         `json:"recommended_method"`
	Recommendation string   `json:"recommendation"`
	Warnings  []string       `json:"warnings,omitempty"`
	ClusterWarnings []string `json:"cluster_warnings,omitempty"`
	Rollback  jsonRollback   `json:"rollback"`
	Script    *jsonScript    `json:"generated_script,omitempty"`
}

type jsonTableMeta struct {
	SizeBytes    int64  `json:"size_bytes"`
	SizeHuman    string `json:"size_human"`
	RowCount     int64  `json:"row_count"`
	IndexCount   int    `json:"index_count"`
	FKCount      int    `json:"fk_count"`
	TriggerCount int    `json:"trigger_count"`
	Engine       string `json:"engine"`
}

type jsonTopology struct {
	Type        string `json:"type"`
	ClusterSize int    `json:"cluster_size,omitempty"`
	OSUMethod   string `json:"osu_method,omitempty"`
	NodeState   string `json:"node_state,omitempty"`
	GRMode      string `json:"gr_mode,omitempty"`
	ReadOnly    bool   `json:"read_only"`
}

type jsonOperation struct {
	// DDL
	DDLOp         string `json:"ddl_operation,omitempty"`
	Algorithm     string `json:"algorithm,omitempty"`
	Lock          string `json:"lock,omitempty"`
	RebuildsTable *bool  `json:"rebuilds_table,omitempty"`

	// DML
	DMLOp        string  `json:"dml_operation,omitempty"`
	AffectedRows int64   `json:"affected_rows,omitempty"`
	AffectedPct  float64 `json:"affected_pct,omitempty"`
	WriteSetSize int64   `json:"write_set_bytes,omitempty"`
	ChunkSize    int     `json:"chunk_size,omitempty"`
	ChunkCount   int64   `json:"chunk_count,omitempty"`
}

type jsonRollback struct {
	SQL     string              `json:"sql,omitempty"`
	Notes   string              `json:"notes,omitempty"`
	Options []jsonRollbackOption `json:"options,omitempty"`
}

type jsonRollbackOption struct {
	Label       string `json:"label"`
	Description string `json:"description"`
	SQL         string `json:"sql,omitempty"`
}

type jsonScript struct {
	Path string `json:"path"`
}

func (r *JSONRenderer) RenderPlan(result *analyzer.Result) {
	out := jsonPlanOutput{
		Statement: result.Statement,
		Type:      string(result.StatementType),
		Database:  result.Database,
		Table:     result.Table,
		Version:   result.Version.String(),
		TableMeta: jsonTableMeta{
			SizeBytes:    result.TableMeta.TotalSize(),
			SizeHuman:    result.TableMeta.TotalSizeHuman(),
			RowCount:     result.TableMeta.RowCount,
			IndexCount:   len(result.TableMeta.Indexes),
			FKCount:      len(result.TableMeta.ForeignKeys),
			TriggerCount: len(result.TableMeta.Triggers),
			Engine:       result.TableMeta.Engine,
		},
		Topology: jsonTopology{
			Type:     string(result.Topology.Type),
			ReadOnly: result.Topology.ReadOnly,
		},
		Risk:            string(result.Risk),
		Method:          string(result.Method),
		Recommendation:  result.Recommendation,
		Warnings:        result.Warnings,
		ClusterWarnings: result.ClusterWarnings,
	}

	// Topology details
	switch result.Topology.Type {
	case topology.Galera:
		out.Topology.ClusterSize = result.Topology.GaleraClusterSize
		out.Topology.OSUMethod = result.Topology.GaleraOSUMethod
		out.Topology.NodeState = result.Topology.GaleraNodeState
	case topology.GroupRepl:
		out.Topology.GRMode = result.Topology.GRMode
		out.Topology.ClusterSize = result.Topology.GRMemberCount
	}

	// Operation
	if result.StatementType == parser.DDL {
		rebuilds := result.Classification.RebuildsTable
		out.Operation = jsonOperation{
			DDLOp:         string(result.DDLOp),
			Algorithm:     string(result.Classification.Algorithm),
			Lock:          string(result.Classification.Lock),
			RebuildsTable: &rebuilds,
		}
	} else {
		out.Operation = jsonOperation{
			DMLOp:        string(result.DMLOp),
			AffectedRows: result.AffectedRows,
			AffectedPct:  result.AffectedPct,
			WriteSetSize: result.WriteSetSize,
			ChunkSize:    result.ChunkSize,
			ChunkCount:   result.ChunkCount,
		}
	}

	// Rollback
	out.Rollback = jsonRollback{
		SQL:   result.RollbackSQL,
		Notes: result.RollbackNotes,
	}
	for _, opt := range result.RollbackOptions {
		out.Rollback.Options = append(out.Rollback.Options, jsonRollbackOption{
			Label:       opt.Label,
			Description: opt.Description,
			SQL:         opt.SQL,
		})
	}

	if result.GeneratedScript != "" {
		out.Script = &jsonScript{Path: result.ScriptPath}
	}

	enc := json.NewEncoder(r.w)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}

func (r *JSONRenderer) RenderTopology(conn mysql.ConnectionConfig, topo *topology.Info) {
	out := map[string]interface{}{
		"host":         conn.Host,
		"port":         conn.Port,
		"version":      topo.Version.String(),
		"topology":     string(topo.Type),
		"read_only":    topo.ReadOnly,
	}

	switch topo.Type {
	case topology.Galera:
		out["cluster_size"] = topo.GaleraClusterSize
		out["node_state"] = topo.GaleraNodeState
		out["osu_method"] = topo.GaleraOSUMethod
		out["wsrep_max_ws_size"] = topo.WsrepMaxWsSize
		out["flow_control_paused"] = topo.FlowControlPausedPct
	case topology.GroupRepl:
		out["gr_mode"] = topo.GRMode
		out["member_count"] = topo.GRMemberCount
		out["member_role"] = topo.GRMemberRole
	}

	enc := json.NewEncoder(r.w)
	enc.SetIndent("", "  ")
	enc.Encode(out)
}
