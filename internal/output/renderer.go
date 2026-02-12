package output

import (
	"io"

	"github.com/nethalo/dbsafe/internal/analyzer"
	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/topology"
)

// Renderer defines the output interface.
type Renderer interface {
	RenderPlan(result *analyzer.Result)
	RenderTopology(conn mysql.ConnectionConfig, topo *topology.Info)
}

// NewRenderer creates a renderer for the given format.
func NewRenderer(format string, w io.Writer) Renderer {
	switch format {
	case "json":
		return &JSONRenderer{w: w}
	case "markdown":
		return &MarkdownRenderer{w: w}
	case "plain":
		return &PlainRenderer{w: w}
	default:
		return &TextRenderer{w: w}
	}
}
