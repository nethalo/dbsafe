package analyzer

import "github.com/nethalo/dbsafe/internal/parser"

// Algorithm represents how MySQL executes an ALTER.
type Algorithm string

const (
	AlgoInstant Algorithm = "INSTANT"
	AlgoInplace Algorithm = "INPLACE"
	AlgoCopy    Algorithm = "COPY"
	AlgoDepends Algorithm = "DEPENDS" // varies by specifics
)

// LockLevel represents what lock MySQL requires during the operation.
type LockLevel string

const (
	LockNone      LockLevel = "NONE"
	LockShared    LockLevel = "SHARED"
	LockExclusive LockLevel = "EXCLUSIVE"
	LockDepends   LockLevel = "DEPENDS"
)

// DDLClassification holds the analysis result for a DDL operation.
type DDLClassification struct {
	Algorithm     Algorithm
	Lock          LockLevel
	RebuildsTable bool
	Notes         string // additional context
}

// VersionRange represents a MySQL version range for the matrix.
type VersionRange int

const (
	V8_0_Early   VersionRange = iota // 8.0.0 – 8.0.11
	V8_0_Instant                     // 8.0.12 – 8.0.28 (INSTANT for trailing ADD COLUMN)
	V8_0_Full                        // 8.0.29+ (expanded INSTANT)
	V8_4_LTS                         // 8.4.x LTS
)

// classifyVersion maps a parsed version to a matrix range.
func classifyVersion(major, minor, patch int) VersionRange {
	if major == 8 && minor == 4 {
		return V8_4_LTS
	}
	if major == 8 && minor == 0 {
		if patch >= 29 {
			return V8_0_Full
		}
		if patch >= 12 {
			return V8_0_Instant
		}
		return V8_0_Early
	}
	// Default to latest behavior for unknown versions
	return V8_0_Full
}

// matrixKey combines operation + version range for lookup.
type matrixKey struct {
	Op      parser.DDLOperation
	Version VersionRange
}

// ddlMatrix is the core classification lookup table.
// This is THE intellectual property of dbsafe.
var ddlMatrix = map[matrixKey]DDLClassification{

	// ═══════════════════════════════════════════════════
	// ADD COLUMN (trailing position, nullable or with DEFAULT)
	// ═══════════════════════════════════════════════════
	{parser.AddColumn, V8_0_Early}: {
		Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false,
		Notes: "INPLACE, concurrent DML allowed. Table rebuild depends on specifics.",
	},
	{parser.AddColumn, V8_0_Instant}: {
		Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false,
		Notes: "INSTANT for trailing column position. No table rebuild, metadata-only change.",
	},
	{parser.AddColumn, V8_0_Full}: {
		Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false,
		Notes: "INSTANT for any column position (8.0.29+). Metadata-only change.",
	},
	{parser.AddColumn, V8_4_LTS}: {
		Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false,
		Notes: "INSTANT for any column position. Metadata-only change.",
	},

	// ═══════════════════════════════════════════════════
	// DROP COLUMN
	// ═══════════════════════════════════════════════════
	{parser.DropColumn, V8_0_Early}: {
		Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true,
		Notes: "INPLACE but requires table rebuild. Concurrent DML allowed during rebuild.",
	},
	{parser.DropColumn, V8_0_Instant}: {
		Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true,
		Notes: "INPLACE but requires table rebuild. Concurrent DML allowed during rebuild.",
	},
	{parser.DropColumn, V8_0_Full}: {
		Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false,
		Notes: "INSTANT (8.0.29+). Metadata-only change, no table rebuild.",
	},
	{parser.DropColumn, V8_4_LTS}: {
		Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false,
		Notes: "INSTANT. Metadata-only change, no table rebuild.",
	},

	// ═══════════════════════════════════════════════════
	// MODIFY COLUMN (data type change)
	// ═══════════════════════════════════════════════════
	{parser.ModifyColumn, V8_0_Early}: {
		Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true,
		Notes: "COPY algorithm with SHARED lock. Reads allowed, writes blocked during rebuild.",
	},
	{parser.ModifyColumn, V8_0_Instant}: {
		Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true,
		Notes: "COPY algorithm with SHARED lock. Reads allowed, writes blocked during rebuild.",
	},
	{parser.ModifyColumn, V8_0_Full}: {
		Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true,
		Notes: "COPY algorithm with SHARED lock. Reads allowed, writes blocked during rebuild.",
	},
	{parser.ModifyColumn, V8_4_LTS}: {
		Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true,
		Notes: "COPY algorithm with SHARED lock. Reads allowed, writes blocked during rebuild.",
	},

	// ═══════════════════════════════════════════════════
	// CHANGE COLUMN (rename + possible type change)
	// ═══════════════════════════════════════════════════
	{parser.ChangeColumn, V8_0_Early}: {
		Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false,
		Notes: "INPLACE if only renaming. If data type changes, falls back to COPY.",
	},
	{parser.ChangeColumn, V8_0_Instant}: {
		Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false,
		Notes: "INPLACE if only renaming. If data type changes, falls back to COPY.",
	},
	{parser.ChangeColumn, V8_0_Full}: {
		Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false,
		Notes: "INSTANT for rename-only (8.0.29+). If data type changes, falls back to COPY.",
	},
	{parser.ChangeColumn, V8_4_LTS}: {
		Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false,
		Notes: "INSTANT for rename-only. If data type changes, falls back to COPY.",
	},

	// ═══════════════════════════════════════════════════
	// ADD INDEX
	// ═══════════════════════════════════════════════════
	{parser.AddIndex, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, concurrent DML allowed. Index built in background."},
	{parser.AddIndex, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, concurrent DML allowed. Index built in background."},
	{parser.AddIndex, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, concurrent DML allowed. Index built in background."},
	{parser.AddIndex, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, concurrent DML allowed. Index built in background."},

	// ═══════════════════════════════════════════════════
	// DROP INDEX
	// ═══════════════════════════════════════════════════
	{parser.DropIndex, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Very fast."},
	{parser.DropIndex, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Very fast."},
	{parser.DropIndex, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Very fast."},
	{parser.DropIndex, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Very fast."},

	// ═══════════════════════════════════════════════════
	// ADD FOREIGN KEY
	// ═══════════════════════════════════════════════════
	{parser.AddForeignKey, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE with foreign_key_checks=OFF. With checks ON, uses SHARED lock."},
	{parser.AddForeignKey, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE with foreign_key_checks=OFF. With checks ON, uses SHARED lock."},
	{parser.AddForeignKey, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE with foreign_key_checks=OFF. With checks ON, uses SHARED lock."},
	{parser.AddForeignKey, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE with foreign_key_checks=OFF. With checks ON, uses SHARED lock."},

	// ═══════════════════════════════════════════════════
	// DROP FOREIGN KEY
	// ═══════════════════════════════════════════════════
	{parser.DropForeignKey, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only."},
	{parser.DropForeignKey, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only."},
	{parser.DropForeignKey, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only."},
	{parser.DropForeignKey, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only."},

	// ═══════════════════════════════════════════════════
	// RENAME TABLE
	// ═══════════════════════════════════════════════════
	{parser.RenameTable, V8_0_Early}:   {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only, instant."},
	{parser.RenameTable, V8_0_Instant}: {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only, instant."},
	{parser.RenameTable, V8_0_Full}:    {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only, instant."},
	{parser.RenameTable, V8_4_LTS}:     {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only, instant."},

	// ═══════════════════════════════════════════════════
	// CHANGE ENGINE (InnoDB → InnoDB, effectively table rebuild)
	// ═══════════════════════════════════════════════════
	{parser.ChangeEngine, V8_0_Early}:   {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild using COPY algorithm."},
	{parser.ChangeEngine, V8_0_Instant}: {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild using COPY algorithm."},
	{parser.ChangeEngine, V8_0_Full}:    {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild using COPY algorithm."},
	{parser.ChangeEngine, V8_4_LTS}:     {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild using COPY algorithm."},

	// ═══════════════════════════════════════════════════
	// CHANGE CHARACTER SET
	// ═══════════════════════════════════════════════════
	{parser.ChangeCharset, V8_0_Early}:   {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild. Converts all text columns."},
	{parser.ChangeCharset, V8_0_Instant}: {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild. Converts all text columns."},
	{parser.ChangeCharset, V8_0_Full}:    {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild. Converts all text columns."},
	{parser.ChangeCharset, V8_4_LTS}:     {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild. Converts all text columns."},

	// ═══════════════════════════════════════════════════
	// SET DEFAULT / DROP DEFAULT
	// ═══════════════════════════════════════════════════
	{parser.SetDefault, V8_0_Early}:   {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only change."},
	{parser.SetDefault, V8_0_Instant}: {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only change."},
	{parser.SetDefault, V8_0_Full}:    {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only change."},
	{parser.SetDefault, V8_4_LTS}:     {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only change."},

	{parser.DropDefault, V8_0_Early}:   {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only change."},
	{parser.DropDefault, V8_0_Instant}: {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only change."},
	{parser.DropDefault, V8_0_Full}:    {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only change."},
	{parser.DropDefault, V8_4_LTS}:     {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only change."},

	// ═══════════════════════════════════════════════════
	// ADD PRIMARY KEY
	// InnoDB organizes data around the clustered index, so adding a primary key
	// always requires a full table rebuild across all versions.
	// ═══════════════════════════════════════════════════
	{parser.AddPrimaryKey, V8_0_Early}:   {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild required. InnoDB must reorganize all rows around the new clustered index."},
	{parser.AddPrimaryKey, V8_0_Instant}: {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild required. InnoDB must reorganize all rows around the new clustered index."},
	{parser.AddPrimaryKey, V8_0_Full}:    {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild required. InnoDB must reorganize all rows around the new clustered index."},
	{parser.AddPrimaryKey, V8_4_LTS}:     {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild required. InnoDB must reorganize all rows around the new clustered index."},

	// ═══════════════════════════════════════════════════
	// DROP PRIMARY KEY
	// Removing the clustered index also requires a full table rebuild.
	// ═══════════════════════════════════════════════════
	{parser.DropPrimaryKey, V8_0_Early}:   {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild required. InnoDB must reorganize all rows without the clustered index."},
	{parser.DropPrimaryKey, V8_0_Instant}: {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild required. InnoDB must reorganize all rows without the clustered index."},
	{parser.DropPrimaryKey, V8_0_Full}:    {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild required. InnoDB must reorganize all rows without the clustered index."},
	{parser.DropPrimaryKey, V8_4_LTS}:     {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rebuild required. InnoDB must reorganize all rows without the clustered index."},

	// ═══════════════════════════════════════════════════
	// CHANGE ROW FORMAT
	// INPLACE but requires table data rebuild. Concurrent DML is allowed.
	// ═══════════════════════════════════════════════════
	{parser.ChangeRowFormat, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with table rebuild. Concurrent DML allowed during rebuild."},
	{parser.ChangeRowFormat, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with table rebuild. Concurrent DML allowed during rebuild."},
	{parser.ChangeRowFormat, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with table rebuild. Concurrent DML allowed during rebuild."},
	{parser.ChangeRowFormat, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with table rebuild. Concurrent DML allowed during rebuild."},

	// ═══════════════════════════════════════════════════
	// ADD PARTITION
	// INPLACE, no rebuild of existing partitions.
	// ═══════════════════════════════════════════════════
	{parser.AddPartition, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE. Only adds new partition definition; existing data and partitions are unaffected."},
	{parser.AddPartition, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE. Only adds new partition definition; existing data and partitions are unaffected."},
	{parser.AddPartition, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE. Only adds new partition definition; existing data and partitions are unaffected."},
	{parser.AddPartition, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE. Only adds new partition definition; existing data and partitions are unaffected."},

	// ═══════════════════════════════════════════════════
	// DROP PARTITION
	// INPLACE. Deallocates the partition's tablespace; other partitions are untouched.
	// ═══════════════════════════════════════════════════
	{parser.DropPartition, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE. Removes partition and its rows; other partitions are not rebuilt."},
	{parser.DropPartition, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE. Removes partition and its rows; other partitions are not rebuilt."},
	{parser.DropPartition, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE. Removes partition and its rows; other partitions are not rebuilt."},
	{parser.DropPartition, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE. Removes partition and its rows; other partitions are not rebuilt."},
}

// ClassifyDDL looks up the DDL operation in the matrix.
func ClassifyDDL(op parser.DDLOperation, major, minor, patch int) DDLClassification {
	vr := classifyVersion(major, minor, patch)
	key := matrixKey{Op: op, Version: vr}

	if c, ok := ddlMatrix[key]; ok {
		return c
	}

	// Default: assume COPY with SHARED lock (safest assumption)
	return DDLClassification{
		Algorithm:     AlgoCopy,
		Lock:          LockShared,
		RebuildsTable: true,
		Notes:         "Operation not in classification matrix. Assuming worst case (COPY + SHARED lock).",
	}
}

// ClassifyDDLWithContext applies additional context-specific adjustments.
// For example, ADD COLUMN FIRST/AFTER on pre-8.0.29 is INPLACE, not INSTANT.
func ClassifyDDLWithContext(parsed *parser.ParsedSQL, major, minor, patch int) DDLClassification {
	c := ClassifyDDL(parsed.DDLOp, major, minor, patch)

	// Adjust for FIRST/AFTER on ADD COLUMN
	if parsed.DDLOp == parser.AddColumn && parsed.IsFirstAfter {
		vr := classifyVersion(major, minor, patch)
		if vr == V8_0_Instant {
			// 8.0.12-8.0.28: INSTANT only for trailing position
			c.Algorithm = AlgoInplace
			c.Notes = "ADD COLUMN with FIRST/AFTER uses INPLACE in 8.0.12-8.0.28. INSTANT only for trailing position."
		}
		// 8.0.29+ and 8.4: INSTANT works for any position, no change needed
	}

	return c
}
