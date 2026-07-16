package analyzer

import (
	"strings"
	"testing"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// Issue #57: explicit ALGORITHM=/LOCK= clauses in the input statement.

func TestStripAlgorithmLockClauses(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "trailing algorithm and lock",
			in:   "ALTER TABLE t ADD COLUMN c INT, ALGORITHM=INPLACE, LOCK=NONE",
			want: "ALTER TABLE t ADD COLUMN c INT",
		},
		{
			name: "leading algorithm and lock",
			in:   "ALTER TABLE t ALGORITHM=COPY, LOCK=EXCLUSIVE, ADD COLUMN c INT",
			want: "ALTER TABLE t ADD COLUMN c INT",
		},
		{
			name: "no equals sign",
			in:   "ALTER TABLE t ADD COLUMN c INT, ALGORITHM INPLACE, LOCK NONE",
			want: "ALTER TABLE t ADD COLUMN c INT",
		},
		{
			name: "clause between operations",
			in:   "ALTER TABLE t ADD COLUMN a INT, ALGORITHM=INPLACE, ADD COLUMN b INT",
			want: "ALTER TABLE t ADD COLUMN a INT, ADD COLUMN b INT",
		},
		{
			name: "no clauses present",
			in:   "ALTER TABLE t ADD COLUMN c INT",
			want: "ALTER TABLE t ADD COLUMN c INT",
		},
		{
			name: "column named lock is untouched",
			in:   "ALTER TABLE t ADD COLUMN `lock` INT, ADD COLUMN c INT",
			want: "ALTER TABLE t ADD COLUMN `lock` INT, ADD COLUMN c INT",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := stripAlgorithmLockClauses(tt.in); got != tt.want {
				t.Errorf("stripAlgorithmLockClauses(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestBuildOptimizedDDL_NoDuplicateClauses(t *testing.T) {
	c := DDLClassification{Algorithm: AlgoInstant, Lock: LockNone}
	got := buildOptimizedDDL("ALTER TABLE t ADD COLUMN c INT, ALGORITHM=INPLACE, LOCK=NONE;", c)
	want := "ALTER TABLE t ADD COLUMN c INT, ALGORITHM=INSTANT, LOCK=NONE;"
	if got != want {
		t.Errorf("buildOptimizedDDL = %q, want %q", got, want)
	}
	if strings.Count(strings.ToUpper(got), "ALGORITHM") != 1 {
		t.Errorf("OptimizedDDL contains duplicated ALGORITHM clause: %q", got)
	}
}

func TestCheckRequestedAlgorithmLock(t *testing.T) {
	tests := []struct {
		name          string
		reqAlgo       string
		reqLock       string
		classAlgo     Algorithm
		classLock     LockLevel
		wantWarnCount int
		wantSubstring string
	}{
		{
			name:          "requested algorithm unsupported",
			reqAlgo:       "instant",
			classAlgo:     AlgoInplace,
			classLock:     LockNone,
			wantWarnCount: 1,
			wantSubstring: "not supported",
		},
		{
			name:          "requested algorithm too expensive",
			reqAlgo:       "copy",
			classAlgo:     AlgoInstant,
			classLock:     LockNone,
			wantWarnCount: 1,
			wantSubstring: "more expensive",
		},
		{
			name:      "requested algorithm matches",
			reqAlgo:   "inplace",
			classAlgo: AlgoInplace,
			classLock: LockNone,
		},
		{
			name:          "requested lock unsupported",
			reqLock:       "none",
			classAlgo:     AlgoCopy,
			classLock:     LockShared,
			wantWarnCount: 1,
			wantSubstring: "LOCK=NONE is not supported",
		},
		{
			name:      "requested lock ok",
			reqLock:   "shared",
			classAlgo: AlgoInplace,
			classLock: LockNone,
		},
		{
			name:      "algorithm default is ignored",
			reqAlgo:   "default",
			classAlgo: AlgoInplace,
			classLock: LockNone,
		},
		{
			name:      "depends classification is not validated",
			reqAlgo:   "instant",
			classAlgo: AlgoDepends,
			classLock: LockNone,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			parsed := &parser.ParsedSQL{
				RequestedAlgorithm: tt.reqAlgo,
				RequestedLock:      tt.reqLock,
			}
			result := &Result{Classification: DDLClassification{Algorithm: tt.classAlgo, Lock: tt.classLock}}
			checkRequestedAlgorithmLock(parsed, result)
			if len(result.Warnings) != tt.wantWarnCount {
				t.Fatalf("Warnings = %v, want %d warning(s)", result.Warnings, tt.wantWarnCount)
			}
			if tt.wantSubstring != "" && !strings.Contains(result.Warnings[0], tt.wantSubstring) {
				t.Errorf("Warnings[0] = %q, want substring %q", result.Warnings[0], tt.wantSubstring)
			}
		})
	}
}

// End-to-end: Parse → Analyze with ALGORITHM/LOCK clauses must classify the real
// operation (not MULTIPLE_OPS/OTHER) and produce a clean OptimizedDDL.
func TestAnalyze_AlterWithAlgorithmLock(t *testing.T) {
	parsed, err := parser.Parse("ALTER TABLE test ADD COLUMN new_col INT, ALGORITHM=INPLACE, LOCK=NONE")
	if err != nil {
		t.Fatalf("Parse error: %v", err)
	}
	if parsed.DDLOp != parser.AddColumn {
		t.Fatalf("DDLOp = %v, want ADD_COLUMN", parsed.DDLOp)
	}

	input := ddlInput(parser.AddColumn, mysql.ServerVersion{Major: 8, Minor: 0, Patch: 35}, 1<<20, topology.Standalone)
	input.Parsed = parsed

	result := Analyze(input)
	if result.DDLOp != parser.AddColumn {
		t.Errorf("result.DDLOp = %v, want ADD_COLUMN", result.DDLOp)
	}
	if result.OptimizedDDL != "" && strings.Count(strings.ToUpper(result.OptimizedDDL), "ALGORITHM") != 1 {
		t.Errorf("OptimizedDDL has duplicated ALGORITHM clause: %q", result.OptimizedDDL)
	}
	for _, w := range result.Warnings {
		if strings.Contains(w, "could not be fully parsed") {
			t.Errorf("unexpected parse warning: %q", w)
		}
	}
}

func TestExtractAlterSpec_StripsAlgorithmLock(t *testing.T) {
	got := extractAlterSpec("ALTER TABLE mydb.users ADD COLUMN c INT, ALGORITHM=INPLACE, LOCK=NONE")
	want := "ADD COLUMN c INT"
	if got != want {
		t.Errorf("extractAlterSpec = %q, want %q", got, want)
	}
}
