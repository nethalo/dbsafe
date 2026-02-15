package output

import (
	"bytes"
	"testing"
)

// Benchmark rendering performance

func BenchmarkTextRenderer_RenderPlan_DDL(b *testing.B) {
	result := ddlResult()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &TextRenderer{w: &buf}
		r.RenderPlan(result)
	}
}

func BenchmarkTextRenderer_RenderPlan_DML(b *testing.B) {
	result := dmlResult()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &TextRenderer{w: &buf}
		r.RenderPlan(result)
	}
}

func BenchmarkPlainRenderer_RenderPlan_DDL(b *testing.B) {
	result := ddlResult()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &PlainRenderer{w: &buf}
		r.RenderPlan(result)
	}
}

func BenchmarkPlainRenderer_RenderPlan_DML(b *testing.B) {
	result := dmlResult()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &PlainRenderer{w: &buf}
		r.RenderPlan(result)
	}
}

func BenchmarkJSONRenderer_RenderPlan_DDL(b *testing.B) {
	result := ddlResult()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &JSONRenderer{w: &buf}
		r.RenderPlan(result)
	}
}

func BenchmarkJSONRenderer_RenderPlan_DML(b *testing.B) {
	result := dmlResult()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &JSONRenderer{w: &buf}
		r.RenderPlan(result)
	}
}

func BenchmarkMarkdownRenderer_RenderPlan_DDL(b *testing.B) {
	result := ddlResult()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &MarkdownRenderer{w: &buf}
		r.RenderPlan(result)
	}
}

func BenchmarkMarkdownRenderer_RenderPlan_DML(b *testing.B) {
	result := dmlResult()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &MarkdownRenderer{w: &buf}
		r.RenderPlan(result)
	}
}

func BenchmarkTextRenderer_RenderTopology(b *testing.B) {
	conn := sampleConn()
	topo := sampleTopo()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &TextRenderer{w: &buf}
		r.RenderTopology(conn, topo)
	}
}

func BenchmarkJSONRenderer_RenderTopology(b *testing.B) {
	conn := sampleConn()
	topo := sampleTopo()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var buf bytes.Buffer
		r := &JSONRenderer{w: &buf}
		r.RenderTopology(conn, topo)
	}
}

// Benchmark formatter functions

func BenchmarkFormatNumber(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatNumber(1234567890)
	}
}

func BenchmarkHumanBytes(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = humanBytes(5368709120) // 5 GB
	}
}

func BenchmarkFormatTopoType(b *testing.B) {
	topo := sampleTopo()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = formatTopoType(topo)
	}
}

// Benchmark concurrent rendering

func BenchmarkJSONRenderer_Concurrent(b *testing.B) {
	result := ddlResult()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var buf bytes.Buffer
			r := &JSONRenderer{w: &buf}
			r.RenderPlan(result)
		}
	})
}
