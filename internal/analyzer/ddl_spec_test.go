package analyzer

// ddl_spec_test.go verifies classification for every operation in DBSAFE_FULL_DDL_TEST_SPEC.md.
// Tests are organized by spec section and reference the spec item number (e.g. "3.6").

import (
	"strings"
	"testing"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
	"github.com/nethalo/dbsafe/internal/topology"
)

// =============================================================
// Section 1: Index Operations
// =============================================================

// 1.3 Renaming an Index — INPLACE, LOCK=NONE, no rebuild (all versions)
func TestSpec_1_3_RenameIndex(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.RenameIndex, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: RenameIndex Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: RenameIndex Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: RenameIndex RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// 1.4 Adding a FULLTEXT Index — INPLACE, LOCK=SHARED, rebuild=true (conservative baseline).
// The first FULLTEXT index on a table triggers a rebuild to add FTS_DOC_ID.
func TestSpec_1_4_AddFulltextIndex(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.AddFulltextIndex, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: AddFulltextIndex Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockShared {
			t.Errorf("v%d.%d.%d: AddFulltextIndex Lock = %q, want SHARED", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: AddFulltextIndex RebuildsTable = false, want true (conservative baseline for first FULLTEXT index)", v.Major, v.Minor, v.Patch)
		}
	}
}

// 1.5 Adding a SPATIAL Index — INPLACE, LOCK=SHARED, no rebuild (all versions)
func TestSpec_1_5_AddSpatialIndex(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.AddSpatialIndex, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: AddSpatialIndex Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockShared {
			t.Errorf("v%d.%d.%d: AddSpatialIndex Lock = %q, want SHARED", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: AddSpatialIndex RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// =============================================================
// Section 3: Column Operations
// =============================================================

// 3.6 Setting a Column Default Value — INSTANT, LOCK=NONE, no rebuild (all versions)
func TestSpec_3_6_SetDefault(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.SetDefault, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInstant {
			t.Errorf("v%d.%d.%d: SetDefault Algorithm = %q, want INSTANT", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: SetDefault Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: SetDefault RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// 3.9 Dropping a Column Default Value — INSTANT, LOCK=NONE, no rebuild (all versions)
func TestSpec_3_9_DropDefault(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.DropDefault, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInstant {
			t.Errorf("v%d.%d.%d: DropDefault Algorithm = %q, want INSTANT", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: DropDefault Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: DropDefault RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// 3.10 Changing the Auto-Increment Value — INPLACE, LOCK=NONE, no rebuild (all versions)
func TestSpec_3_10_ChangeAutoIncrement(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.ChangeAutoIncrement, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: ChangeAutoIncrement Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: ChangeAutoIncrement Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: ChangeAutoIncrement RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// 3.11 & 3.12 Nullability change (same base type) — INPLACE, LOCK=NONE, rebuild=true
func TestSpec_3_11_NullabilityChange_NullToNotNull_IsInplace(t *testing.T) {
	// Column was NULL, being changed to NOT NULL — same base type
	nullable := false // NOT NULL specified
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:             parser.DDL,
			RawSQL:           "ALTER TABLE t MODIFY COLUMN name VARCHAR(100) NOT NULL",
			Table:            "t",
			DDLOp:            parser.ModifyColumn,
			ColumnName:       "name",
			NewColumnType:    "varchar(100)",
			NewColumnNullable: &nullable,
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Nullable: false, Position: 1},
				{Name: "name", Type: "varchar(100)", Nullable: true, Position: 2}, // currently NULL
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("NOT NULL change: Algorithm = %q, want INPLACE", result.Classification.Algorithm)
	}
	if !result.Classification.RebuildsTable {
		t.Error("NOT NULL change: RebuildsTable = false, want true")
	}
	if result.Classification.Lock != LockNone {
		t.Errorf("NOT NULL change: Lock = %q, want NONE", result.Classification.Lock)
	}
}

func TestSpec_3_12_NullabilityChange_NotNullToNull_IsInplace(t *testing.T) {
	// Column was NOT NULL, being changed to NULL — same base type
	nullable := true // NULL specified
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:             parser.DDL,
			RawSQL:           "ALTER TABLE t MODIFY COLUMN name VARCHAR(100) NULL",
			Table:            "t",
			DDLOp:            parser.ModifyColumn,
			ColumnName:       "name",
			NewColumnType:    "varchar(100)",
			NewColumnNullable: &nullable,
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Nullable: false, Position: 1},
				{Name: "name", Type: "varchar(100)", Nullable: false, Position: 2}, // currently NOT NULL
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("NULL change: Algorithm = %q, want INPLACE", result.Classification.Algorithm)
	}
	if !result.Classification.RebuildsTable {
		t.Error("NULL change: RebuildsTable = false, want true")
	}
}

func TestSpec_3_12_NullabilityUnchanged_DoesNotOverride(t *testing.T) {
	// Column is already NOT NULL, modifying with NOT NULL → no nullability change → stays COPY
	nullable := false
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:             parser.DDL,
			RawSQL:           "ALTER TABLE t MODIFY COLUMN name VARCHAR(100) NOT NULL",
			Table:            "t",
			DDLOp:            parser.ModifyColumn,
			ColumnName:       "name",
			NewColumnType:    "varchar(100)",
			NewColumnNullable: &nullable,
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Nullable: false, Position: 1},
				{Name: "name", Type: "varchar(100)", Nullable: false, Position: 2}, // already NOT NULL
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	// Same varchar(100) with same nullability → no override, stays at matrix default or varchar check
	// Since varchar(100)→varchar(100) same tier, could be INPLACE via varchar check
	// But no rebuild forced by nullability (unchanged). Just verify it's not forced to rebuild by nullability.
	// The varchar extension path gives INPLACE no rebuild.
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("unchanged nullability: Algorithm = %q, want INPLACE (varchar same-size keeps INPLACE)", result.Classification.Algorithm)
	}
}

// 3.13 Modifying ENUM/SET Definition — append at end → INSTANT, no rebuild
func TestSpec_3_13_EnumAppendAtEnd_IsInstant(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			RawSQL:        "ALTER TABLE orders MODIFY COLUMN status ENUM('pending','processing','shipped','delivered','cancelled','refunded')",
			Table:         "orders",
			DDLOp:         parser.ModifyColumn,
			ColumnName:    "status",
			NewColumnType: "enum('pending','processing','shipped','delivered','cancelled','refunded')",
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "orders",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Position: 1},
				{Name: "status", Type: "enum('pending','processing','shipped','delivered','cancelled')", Position: 2},
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	if result.Classification.Algorithm != AlgoInstant {
		t.Errorf("ENUM append: Algorithm = %q, want INSTANT", result.Classification.Algorithm)
	}
	if result.Classification.RebuildsTable {
		t.Error("ENUM append: RebuildsTable = true, want false")
	}
}

func TestSpec_3_13_EnumReorder_IsCopy(t *testing.T) {
	// Adding 'new' at beginning (reorder) → must not be INSTANT → COPY
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			RawSQL:        "ALTER TABLE orders MODIFY COLUMN status ENUM('new','pending','processing','shipped','delivered','cancelled')",
			Table:         "orders",
			DDLOp:         parser.ModifyColumn,
			ColumnName:    "status",
			NewColumnType: "enum('new','pending','processing','shipped','delivered','cancelled')",
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "orders",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Position: 1},
				{Name: "status", Type: "enum('pending','processing','shipped','delivered','cancelled')", Position: 2},
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	if result.Classification.Algorithm == AlgoInstant {
		t.Error("ENUM reorder (insert in middle): should NOT be INSTANT, must be COPY")
	}
}

func TestSpec_3_13_EnumRemoveMember_IsCopy(t *testing.T) {
	// Removing a member → not append-only → COPY
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			RawSQL:        "ALTER TABLE orders MODIFY COLUMN status ENUM('pending','shipped','cancelled')",
			Table:         "orders",
			DDLOp:         parser.ModifyColumn,
			ColumnName:    "status",
			NewColumnType: "enum('pending','shipped','cancelled')",
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "orders",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Position: 1},
				{Name: "status", Type: "enum('pending','processing','shipped','delivered','cancelled')", Position: 2},
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	if result.Classification.Algorithm == AlgoInstant {
		t.Error("ENUM with removed member should NOT be INSTANT")
	}
}

// 3.5 Column reorder via MODIFY COLUMN ... AFTER — INPLACE, LOCK=NONE, rebuild=true
func TestSpec_3_5_ColumnReorder_IsInplaceWithRebuild(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			RawSQL:        "ALTER TABLE t MODIFY COLUMN name VARCHAR(100) AFTER id",
			Table:         "t",
			DDLOp:         parser.ModifyColumn,
			ColumnName:    "name",
			NewColumnType: "varchar(100)",
			IsFirstAfter:  true,
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Nullable: false, Position: 1},
				{Name: "name", Type: "varchar(100)", Nullable: true, Position: 3}, // same type, just reordering
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("column reorder: Algorithm = %q, want INPLACE", result.Classification.Algorithm)
	}
	if !result.Classification.RebuildsTable {
		t.Error("column reorder: RebuildsTable = false, want true (FIRST/AFTER requires rebuild)")
	}
	if result.Classification.Lock != LockNone {
		t.Errorf("column reorder: Lock = %q, want NONE", result.Classification.Lock)
	}
}

func TestSpec_3_5_ColumnReorder_TypeChangeOverridesReorder(t *testing.T) {
	// If type also changes, the type change wins (COPY), reorder alone doesn't apply
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:          parser.DDL,
			RawSQL:        "ALTER TABLE t MODIFY COLUMN amount DECIMAL(14,4) AFTER id",
			Table:         "t",
			DDLOp:         parser.ModifyColumn,
			ColumnName:    "amount",
			NewColumnType: "decimal(14,4)",
			IsFirstAfter:  true,
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Nullable: false, Position: 1},
				{Name: "amount", Type: "decimal(10,2)", Nullable: true, Position: 2}, // different type
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	// Type changed (decimal(10,2) → decimal(14,4)) → COPY
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("type change + reorder: Algorithm = %q, want COPY (type change takes precedence)", result.Classification.Algorithm)
	}
}

// =============================================================
// Section 5: Foreign Key Operations
// =============================================================

// 5.1 Adding a Foreign Key — INPLACE, LOCK=NONE, no rebuild (all versions)
func TestSpec_5_1_AddForeignKey(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.AddForeignKey, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: AddForeignKey Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: AddForeignKey Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: AddForeignKey RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// 5.2 Dropping a Foreign Key — INPLACE, LOCK=NONE, no rebuild (all versions)
func TestSpec_5_2_DropForeignKey(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.DropForeignKey, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: DropForeignKey Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: DropForeignKey Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: DropForeignKey RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// =============================================================
// Section 6: Table Operations
// =============================================================

// 6.6 ALTER TABLE ... ENGINE=InnoDB (explicit rebuild) — COPY, LOCK=SHARED, rebuild=true (all versions)
func TestSpec_6_6_ChangeEngine_IsCopy(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.ChangeEngine, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoCopy {
			t.Errorf("v%d.%d.%d: ChangeEngine Algorithm = %q, want COPY", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockShared {
			t.Errorf("v%d.%d.%d: ChangeEngine Lock = %q, want SHARED", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: ChangeEngine RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

// 6.6 ALTER TABLE ... FORCE — INPLACE, LOCK=NONE, rebuild=true (all versions)
func TestSpec_6_6_ForceRebuild(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.ForceRebuild, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: ForceRebuild Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: ForceRebuild Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: ForceRebuild RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

// =============================================================
// Section 8: Partitioning Operations
// =============================================================

// 8.3 REORGANIZE PARTITION — INPLACE, LOCK=SHARED (writes blocked), no full table rebuild.
// Copies data between partition definitions; other partitions are unaffected.
func TestSpec_8_3_ReorganizePartition(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.ReorganizePartition, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: ReorganizePartition Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockShared {
			t.Errorf("v%d.%d.%d: ReorganizePartition Lock = %q, want SHARED (concurrent DML blocked)", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: ReorganizePartition RebuildsTable = true, want false (partition only, not full table)", v.Major, v.Minor, v.Patch)
		}
	}
}

// 8.4 REBUILD PARTITION — INPLACE, LOCK=SHARED (writes blocked), no full table rebuild.
// Defragments the specified partition(s); other partitions are unaffected.
func TestSpec_8_4_RebuildPartition(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.RebuildPartition, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: RebuildPartition Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockShared {
			t.Errorf("v%d.%d.%d: RebuildPartition Lock = %q, want SHARED (concurrent DML blocked)", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: RebuildPartition RebuildsTable = true, want false (partition only, not full table)", v.Major, v.Minor, v.Patch)
		}
	}
}

// 8.5 TRUNCATE PARTITION — INPLACE, LOCK=EXCLUSIVE on affected partition, no rebuild.
// Exclusive lock is analogous to TRUNCATE TABLE; other partitions remain accessible.
func TestSpec_8_5_TruncatePartition(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.TruncatePartition, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: TruncatePartition Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockExclusive {
			t.Errorf("v%d.%d.%d: TruncatePartition Lock = %q, want EXCLUSIVE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: TruncatePartition RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// =============================================================
// Section 1 (new): Index Type Change via DROP+ADD — §1.6
// =============================================================

// 1.6 Index type change (DROP INDEX + ADD INDEX same name)
// INSTANT from 8.0.12+; INPLACE on 8.0.0-8.0.11 (INSTANT algorithm didn't exist yet).
// InnoDB always uses B-tree internally; the USING clause is stored in the data dictionary only.
func TestSpec_1_6_ChangeIndexType_IsInstant(t *testing.T) {
	// 8.0.12+: INSTANT
	for _, v := range []mysql.ServerVersion{v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.ChangeIndexType, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInstant {
			t.Errorf("v%d.%d.%d: ChangeIndexType Algorithm = %q, want INSTANT", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: ChangeIndexType Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: ChangeIndexType RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
	// 8.0.0-8.0.11: INPLACE (INSTANT not available)
	c := ClassifyDDL(parser.ChangeIndexType, v8_0_5.Major, v8_0_5.Minor, v8_0_5.Patch)
	if c.Algorithm != AlgoInplace {
		t.Errorf("v8.0.5: ChangeIndexType Algorithm = %q, want INPLACE (INSTANT not available before 8.0.12)", c.Algorithm)
	}
}

// =============================================================
// Section 2 (new): Primary Key Replacement — §2.3
// =============================================================

// 2.3 DROP PRIMARY KEY + ADD PRIMARY KEY combined — INPLACE, LOCK=NONE, rebuild=true
func TestSpec_2_3_ReplacePrimaryKey_IsInplace(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.ReplacePrimaryKey, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: ReplacePrimaryKey Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: ReplacePrimaryKey Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: ReplacePrimaryKey RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

// =============================================================
// Section 3.1 (new): ADD COLUMN with AUTO_INCREMENT edge case
// =============================================================

// 3.1 ADD COLUMN with AUTO_INCREMENT — must override to COPY+SHARED
func TestSpec_3_1_AddColumnAutoIncrement_IsCopy(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:             parser.DDL,
			RawSQL:           "ALTER TABLE t ADD COLUMN seq_id BIGINT AUTO_INCREMENT",
			Table:            "t",
			DDLOp:            parser.AddColumn,
			ColumnName:       "seq_id",
			HasAutoIncrement: true,
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "id", Type: "int", Nullable: false, Position: 1},
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("ADD COLUMN AUTO_INCREMENT: Algorithm = %q, want COPY", result.Classification.Algorithm)
	}
	if result.Classification.Lock != LockShared {
		t.Errorf("ADD COLUMN AUTO_INCREMENT: Lock = %q, want SHARED", result.Classification.Lock)
	}
	if !result.Classification.RebuildsTable {
		t.Error("ADD COLUMN AUTO_INCREMENT: RebuildsTable = false, want true")
	}
	if len(result.Warnings) == 0 {
		t.Error("ADD COLUMN AUTO_INCREMENT: expected warning about AUTO_INCREMENT forcing COPY")
	}
}

// =============================================================
// Section 6 (new): Table Option Operations — §6.2, §6.3
// =============================================================

// 6.2 KEY_BLOCK_SIZE — INPLACE, LOCK=NONE, rebuild=true (InnoDB rebuilds immediately)
func TestSpec_6_2_KeyBlockSize_IsInplaceWithRebuild(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.KeyBlockSize, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: KeyBlockSize Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: KeyBlockSize Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: KeyBlockSize RebuildsTable = false, want true (InnoDB rebuilds immediately)", v.Major, v.Minor, v.Patch)
		}
	}
}

// 6.3 STATS_PERSISTENT / STATS_SAMPLE_PAGES / STATS_AUTO_RECALC — INPLACE, LOCK=NONE (all versions)
func TestSpec_6_3_StatsOption_IsInplace(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.StatsOption, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: StatsOption Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: StatsOption Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: StatsOption RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// =============================================================
// Section 7 (new): Table Encryption — §7.2
// =============================================================

// 7.2 ENCRYPTION='Y'/'N' — COPY, LOCK=SHARED, rebuild=true + keyring warning
func TestSpec_7_2_TableEncryption_IsCopy(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.TableEncryption, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoCopy {
			t.Errorf("v%d.%d.%d: TableEncryption Algorithm = %q, want COPY", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockShared {
			t.Errorf("v%d.%d.%d: TableEncryption Lock = %q, want SHARED", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: TableEncryption RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

// 7.2 ENCRYPTION — keyring warning emitted by analyzer
func TestSpec_7_2_TableEncryption_EmitsKeyringWarning(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:   parser.DDL,
			RawSQL: "ALTER TABLE t ENCRYPTION='Y'",
			Table:  "t",
			DDLOp:  parser.TableEncryption,
		},
		Meta:    &mysql.TableMetadata{Database: "testdb", Table: "t"},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)

	hasKeyringWarn := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "keyring") {
			hasKeyringWarn = true
			break
		}
	}
	if !hasKeyringWarn {
		t.Errorf("ENCRYPTION: expected keyring plugin warning in Warnings, got %v", result.Warnings)
	}
}

// =============================================================
// Section 6.7: OPTIMIZE TABLE
// =============================================================

// §6.7 OPTIMIZE TABLE: INPLACE, RebuildsTable=true, LOCK=NONE
func TestSpec_6_7_OptimizeTable_IsInplaceWithRebuild(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.OptimizeTable, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: OptimizeTable Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: OptimizeTable Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if !c.RebuildsTable {
			t.Errorf("v%d.%d.%d: OptimizeTable RebuildsTable = false, want true", v.Major, v.Minor, v.Patch)
		}
	}
}

// Parser integration: OPTIMIZE TABLE produces OptimizeTable op with correct table name.
func TestSpec_6_7_OptimizeTable_ParsesCorrectly(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:   parser.DDL,
			RawSQL: "OPTIMIZE TABLE orders",
			Table:  "orders",
			DDLOp:  parser.OptimizeTable,
		},
		Meta:    &mysql.TableMetadata{Database: "testdb", Table: "orders"},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE", result.Classification.Algorithm)
	}
	if !result.Classification.RebuildsTable {
		t.Errorf("RebuildsTable = false, want true")
	}
}

// =============================================================
// Section 7.1: ALTER TABLESPACE RENAME
// =============================================================

// §7.1 ALTER TABLESPACE RENAME: INPLACE, RebuildsTable=false, LOCK=NONE
func TestSpec_7_1_AlterTablespaceRename_IsInplace(t *testing.T) {
	for _, v := range []mysql.ServerVersion{v8_0_5, v8_0_20, v8_0_35, v8_4_0} {
		c := ClassifyDDL(parser.AlterTablespace, v.Major, v.Minor, v.Patch)
		if c.Algorithm != AlgoInplace {
			t.Errorf("v%d.%d.%d: AlterTablespace Algorithm = %q, want INPLACE", v.Major, v.Minor, v.Patch, c.Algorithm)
		}
		if c.Lock != LockNone {
			t.Errorf("v%d.%d.%d: AlterTablespace Lock = %q, want NONE", v.Major, v.Minor, v.Patch, c.Lock)
		}
		if c.RebuildsTable {
			t.Errorf("v%d.%d.%d: AlterTablespace RebuildsTable = true, want false", v.Major, v.Minor, v.Patch)
		}
	}
}

// Full analysis: AlterTablespace produces SAFE + DIRECT with empty metadata.
func TestSpec_7_1_AlterTablespace_AnalyzeSafe(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:              parser.DDL,
			RawSQL:            "ALTER TABLESPACE ts1 RENAME TO ts2",
			DDLOp:             parser.AlterTablespace,
			TablespaceName:    "ts1",
			NewTablespaceName: "ts2",
		},
		Meta:    &mysql.TableMetadata{}, // tablespace ops have no table metadata
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE", result.Classification.Algorithm)
	}
	if result.Risk != RiskSafe {
		t.Errorf("Risk = %q, want SAFE", result.Risk)
	}
	if result.Method != ExecDirect {
		t.Errorf("Method = %q, want DIRECT", result.Method)
	}
}

// =============================================================
// MODIFY COLUMN charset change (Issue #26)
// =============================================================

// Changing charset on a VARCHAR column always requires COPY.
func TestSpec_ModifyColumn_CharsetChange_IsCopy(t *testing.T) {
	utf8mb3 := "utf8mb3"
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:             parser.DDL,
			RawSQL:           "ALTER TABLE t MODIFY COLUMN name VARCHAR(100) CHARACTER SET utf8mb4",
			Table:            "t",
			DDLOp:            parser.ModifyColumn,
			ColumnName:       "name",
			NewColumnType:    "varchar(100) character set utf8mb4",
			NewColumnCharset: "utf8mb4",
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "name", Type: "varchar(100)", CharacterSet: &utf8mb3},
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)
	if result.Classification.Algorithm != AlgoCopy {
		t.Errorf("Algorithm = %q, want COPY (charset change)", result.Classification.Algorithm)
	}
	// Should have a charset-change warning
	hasCharsetWarn := false
	for _, w := range result.Warnings {
		if strings.Contains(w, "charset change") {
			hasCharsetWarn = true
			break
		}
	}
	if !hasCharsetWarn {
		t.Errorf("expected charset change warning, got %v", result.Warnings)
	}
}

// Same charset explicitly specified: VARCHAR tier optimization still applies.
// Using varchar(100)→varchar(200) in utf8mb4: both need 2-byte prefix (100×4=400 > 255).
func TestSpec_ModifyColumn_SameCharsetExplicit_IsInplace(t *testing.T) {
	utf8mb4 := "utf8mb4"
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:             parser.DDL,
			RawSQL:           "ALTER TABLE t MODIFY COLUMN name VARCHAR(200) CHARACTER SET utf8mb4",
			Table:            "t",
			DDLOp:            parser.ModifyColumn,
			ColumnName:       "name",
			NewColumnType:    "varchar(200) character set utf8mb4",
			NewColumnCharset: "utf8mb4",
		},
		Meta: &mysql.TableMetadata{
			Database: "testdb",
			Table:    "t",
			Columns: []mysql.ColumnInfo{
				{Name: "name", Type: "varchar(100)", CharacterSet: &utf8mb4},
			},
		},
		Version: v8_0_35,
		Topo:    standaloneInfo(),
	}
	result := Analyze(input)
	// varchar(100)→varchar(200) in utf8mb4: both need 2-byte prefix tier → INPLACE
	if result.Classification.Algorithm != AlgoInplace {
		t.Errorf("Algorithm = %q, want INPLACE (same charset, varchar tier unchanged)", result.Classification.Algorithm)
	}
	if result.Classification.RebuildsTable {
		t.Errorf("RebuildsTable = true, want false for same-tier varchar extension")
	}
}

// =============================================================
// Helpers shared by spec tests
// =============================================================

func standaloneInfo() *topology.Info {
	return &topology.Info{Type: topology.Standalone}
}
