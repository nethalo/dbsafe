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
		Notes: "INSTANT rename (≥8.0.29; MySQL Bug#33175960 shipped in 8.0.28 but V8_0_Full bucket starts at 8.0.29). If data type changes, requires COPY with SHARED lock.",
	},
	{parser.ChangeColumn, V8_4_LTS}: {
		Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false,
		Notes: "INSTANT rename. If data type changes, requires COPY with SHARED lock.",
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
	// ADD CHECK CONSTRAINT
	// Validates all existing rows against the expression. INPLACE, LOCK=NONE —
	// concurrent DML is allowed. No row rewrite; only the constraint metadata is added.
	// If any existing row violates the expression, the ALTER fails.
	// ═══════════════════════════════════════════════════
	{parser.AddCheckConstraint, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, LOCK=NONE. Validates existing rows against the check expression; concurrent DML allowed. Fails if any row violates the constraint."},
	{parser.AddCheckConstraint, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, LOCK=NONE. Validates existing rows against the check expression; concurrent DML allowed. Fails if any row violates the constraint."},
	{parser.AddCheckConstraint, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, LOCK=NONE. Validates existing rows against the check expression; concurrent DML allowed. Fails if any row violates the constraint."},
	{parser.AddCheckConstraint, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, LOCK=NONE. Validates existing rows against the check expression; concurrent DML allowed. Fails if any row violates the constraint."},

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
	// CHANGE CHARACTER SET (table default only)
	// ALTER TABLE ... CHARACTER SET = ... changes only the table's default character set
	// for future columns. Existing column data is NOT converted. Metadata-only change.
	// ═══════════════════════════════════════════════════
	{parser.ChangeCharset, V8_0_Early}:   {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only: updates the table's default character set for new columns. Existing column data is NOT converted."},
	{parser.ChangeCharset, V8_0_Instant}: {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only: updates the table's default character set for new columns. Existing column data is NOT converted."},
	{parser.ChangeCharset, V8_0_Full}:    {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only: updates the table's default character set for new columns. Existing column data is NOT converted."},
	{parser.ChangeCharset, V8_4_LTS}:     {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "Metadata-only: updates the table's default character set for new columns. Existing column data is NOT converted."},

	// ═══════════════════════════════════════════════════
	// CONVERT CHARACTER SET (CONVERT TO CHARACTER SET)
	// Rewrites every text column's data. Algorithm depends on whether any indexed
	// string column exists (WL#11605): COPY if yes, INPLACE if no — but SHARED lock
	// always applies; concurrent DML is never allowed regardless of algorithm.
	// The matrix baseline is COPY (conservative); the analyzer refines to INPLACE
	// when live metadata shows no indexed string columns.
	// ═══════════════════════════════════════════════════
	{parser.ConvertCharset, V8_0_Early}:   {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rewrite. COPY if indexed string columns exist (WL#11605); INPLACE otherwise — SHARED lock always applies, no concurrent DML."},
	{parser.ConvertCharset, V8_0_Instant}: {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rewrite. COPY if indexed string columns exist (WL#11605); INPLACE otherwise — SHARED lock always applies, no concurrent DML."},
	{parser.ConvertCharset, V8_0_Full}:    {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rewrite. COPY if indexed string columns exist (WL#11605); INPLACE otherwise — SHARED lock always applies, no concurrent DML."},
	{parser.ConvertCharset, V8_4_LTS}:     {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "Full table rewrite. COPY if indexed string columns exist (WL#11605); INPLACE otherwise — SHARED lock always applies, no concurrent DML."},

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
	// INPLACE with full table rebuild — InnoDB must reorganize rows around the new clustered
	// index. Concurrent DML is allowed. Exception: if any PK column is nullable, MySQL must
	// first convert it to NOT NULL, which requires COPY. The analyzer upgrades to COPY when
	// live schema metadata shows a nullable PK column.
	// ═══════════════════════════════════════════════════
	{parser.AddPrimaryKey, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with full table rebuild. Concurrent DML allowed. Requires all PK columns to be NOT NULL; nullable PK columns require COPY."},
	{parser.AddPrimaryKey, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with full table rebuild. Concurrent DML allowed. Requires all PK columns to be NOT NULL; nullable PK columns require COPY."},
	{parser.AddPrimaryKey, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with full table rebuild. Concurrent DML allowed. Requires all PK columns to be NOT NULL; nullable PK columns require COPY."},
	{parser.AddPrimaryKey, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with full table rebuild. Concurrent DML allowed. Requires all PK columns to be NOT NULL; nullable PK columns require COPY."},

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
	// RENAME INDEX
	// Metadata-only. MySQL renames the index in the data dictionary without touching data pages.
	// ═══════════════════════════════════════════════════
	{parser.RenameIndex, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Very fast."},
	{parser.RenameIndex, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Very fast."},
	{parser.RenameIndex, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Very fast."},
	{parser.RenameIndex, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Very fast."},

	// ═══════════════════════════════════════════════════
	// ADD FULLTEXT INDEX
	// INPLACE with SHARED lock — concurrent DML is blocked.
	// Conservative baseline: RebuildsTable=true because the FIRST FULLTEXT index requires a
	// table rebuild to add the hidden FTS_DOC_ID column. Subsequent FULLTEXT indexes do not
	// rebuild. The analyzer cannot currently distinguish first vs. subsequent without live
	// metadata inspection, so we use the worst-case baseline.
	// ═══════════════════════════════════════════════════
	{parser.AddFulltextIndex, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: true, Notes: "INPLACE with SHARED lock — writes blocked. First FULLTEXT index rebuilds the table to add FTS_DOC_ID column; subsequent ones do not."},
	{parser.AddFulltextIndex, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: true, Notes: "INPLACE with SHARED lock — writes blocked. First FULLTEXT index rebuilds the table to add FTS_DOC_ID column; subsequent ones do not."},
	{parser.AddFulltextIndex, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: true, Notes: "INPLACE with SHARED lock — writes blocked. First FULLTEXT index rebuilds the table to add FTS_DOC_ID column; subsequent ones do not."},
	{parser.AddFulltextIndex, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: true, Notes: "INPLACE with SHARED lock — writes blocked. First FULLTEXT index rebuilds the table to add FTS_DOC_ID column; subsequent ones do not."},

	// ═══════════════════════════════════════════════════
	// ADD SPATIAL INDEX
	// INPLACE but requires SHARED lock — concurrent DML is blocked.
	// ═══════════════════════════════════════════════════
	{parser.AddSpatialIndex, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked during spatial index build."},
	{parser.AddSpatialIndex, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked during spatial index build."},
	{parser.AddSpatialIndex, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked during spatial index build."},
	{parser.AddSpatialIndex, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked during spatial index build."},

	// ═══════════════════════════════════════════════════
	// CHANGE AUTO_INCREMENT
	// Modifies the next auto-increment counter value in memory and data dictionary.
	// No row rewrite; INPLACE with no lock.
	// ═══════════════════════════════════════════════════
	{parser.ChangeAutoIncrement, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, no row rewrite. Updates the auto-increment counter in the data dictionary."},
	{parser.ChangeAutoIncrement, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, no row rewrite. Updates the auto-increment counter in the data dictionary."},
	{parser.ChangeAutoIncrement, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, no row rewrite. Updates the auto-increment counter in the data dictionary."},
	{parser.ChangeAutoIncrement, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, no row rewrite. Updates the auto-increment counter in the data dictionary."},

	// ═══════════════════════════════════════════════════
	// FORCE REBUILD (ALTER TABLE ... FORCE)
	// Equivalent to ENGINE=InnoDB for InnoDB tables: rebuilds the clustered index and all
	// secondary indexes in place. Reclaims space, resets TOTAL_ROW_VERSIONS for INSTANT columns.
	// ═══════════════════════════════════════════════════
	{parser.ForceRebuild, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with table rebuild. Reclaims fragmented space. Concurrent DML allowed during rebuild."},
	{parser.ForceRebuild, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with table rebuild. Reclaims fragmented space. Concurrent DML allowed during rebuild."},
	{parser.ForceRebuild, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with table rebuild. Reclaims fragmented space and resets TOTAL_ROW_VERSIONS counter."},
	{parser.ForceRebuild, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with table rebuild. Reclaims fragmented space and resets TOTAL_ROW_VERSIONS counter."},

	// ═══════════════════════════════════════════════════
	// REORGANIZE PARTITION
	// Copies data between partition definitions. Does not rebuild the full table.
	// Requires SHARED lock — concurrent writes (DML) are blocked during the operation.
	// ═══════════════════════════════════════════════════
	{parser.ReorganizePartition, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked. Copies data between partition definitions; other partitions are untouched."},
	{parser.ReorganizePartition, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked. Copies data between partition definitions; other partitions are untouched."},
	{parser.ReorganizePartition, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked. Copies data between partition definitions; other partitions are untouched."},
	{parser.ReorganizePartition, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked. Copies data between partition definitions; other partitions are untouched."},

	// ═══════════════════════════════════════════════════
	// REBUILD PARTITION
	// Defragments and rebuilds the specified partition(s) in-place.
	// Requires SHARED lock — concurrent writes are blocked. Other partitions are unaffected.
	// ═══════════════════════════════════════════════════
	{parser.RebuildPartition, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked. Defragments the specified partition(s) only; other partitions untouched."},
	{parser.RebuildPartition, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked. Defragments the specified partition(s) only; other partitions untouched."},
	{parser.RebuildPartition, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked. Defragments the specified partition(s) only; other partitions untouched."},
	{parser.RebuildPartition, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockShared, RebuildsTable: false, Notes: "INPLACE with SHARED lock — writes blocked. Defragments the specified partition(s) only; other partitions untouched."},

	// ═══════════════════════════════════════════════════
	// TRUNCATE PARTITION
	// Drops all rows in the specified partition without rebuilding the structure.
	// Requires EXCLUSIVE lock on the affected partition (analogous to TRUNCATE TABLE).
	// Other partitions remain accessible.
	// ═══════════════════════════════════════════════════
	{parser.TruncatePartition, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockExclusive, RebuildsTable: false, Notes: "INPLACE with EXCLUSIVE lock on the affected partition. Drops all rows; partition structure remains. Other partitions are accessible."},
	{parser.TruncatePartition, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockExclusive, RebuildsTable: false, Notes: "INPLACE with EXCLUSIVE lock on the affected partition. Drops all rows; partition structure remains. Other partitions are accessible."},
	{parser.TruncatePartition, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockExclusive, RebuildsTable: false, Notes: "INPLACE with EXCLUSIVE lock on the affected partition. Drops all rows; partition structure remains. Other partitions are accessible."},
	{parser.TruncatePartition, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockExclusive, RebuildsTable: false, Notes: "INPLACE with EXCLUSIVE lock on the affected partition. Drops all rows; partition structure remains. Other partitions are accessible."},

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

	// ═══════════════════════════════════════════════════
	// KEY_BLOCK_SIZE (§6.2)
	// InnoDB immediately rebuilds the table using the new page size.
	// INPLACE with LOCK=NONE (concurrent DML allowed) but requires a full table rebuild.
	// Equivalent in cost to OPTIMIZE TABLE.
	// ═══════════════════════════════════════════════════
	{parser.KeyBlockSize, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with full table rebuild — cost equivalent to OPTIMIZE TABLE. Concurrent DML allowed during rebuild."},
	{parser.KeyBlockSize, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with full table rebuild — cost equivalent to OPTIMIZE TABLE. Concurrent DML allowed during rebuild."},
	{parser.KeyBlockSize, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with full table rebuild — cost equivalent to OPTIMIZE TABLE. Concurrent DML allowed during rebuild."},
	{parser.KeyBlockSize, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "INPLACE with full table rebuild — cost equivalent to OPTIMIZE TABLE. Concurrent DML allowed during rebuild."},

	// ═══════════════════════════════════════════════════
	// STATS_PERSISTENT / STATS_SAMPLE_PAGES / STATS_AUTO_RECALC (§6.3)
	// InnoDB statistics options update metadata only (mysql.innodb_table_stats /
	// information_schema). No row data or indexes are modified.
	// ═══════════════════════════════════════════════════
	{parser.StatsOption, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Updates InnoDB statistics configuration; no row data or indexes are modified."},
	{parser.StatsOption, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Updates InnoDB statistics configuration; no row data or indexes are modified."},
	{parser.StatsOption, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Updates InnoDB statistics configuration; no row data or indexes are modified."},
	{parser.StatsOption, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Updates InnoDB statistics configuration; no row data or indexes are modified."},

	// ═══════════════════════════════════════════════════
	// TABLE ENCRYPTION (§7.2)
	// Enabling/disabling InnoDB table encryption uses COPY algorithm with SHARED lock.
	// The data is re-encrypted by rebuilding the entire table. Requires keyring plugin.
	// ═══════════════════════════════════════════════════
	{parser.TableEncryption, V8_0_Early}:   {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "COPY algorithm — full table rebuild. Reads allowed, writes blocked during re-encryption. Requires keyring plugin (keyring_file or keyring_encrypted_file)."},
	{parser.TableEncryption, V8_0_Instant}: {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "COPY algorithm — full table rebuild. Reads allowed, writes blocked during re-encryption. Requires keyring plugin (keyring_file or keyring_encrypted_file)."},
	{parser.TableEncryption, V8_0_Full}:    {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "COPY algorithm — full table rebuild. Reads allowed, writes blocked during re-encryption. Requires keyring plugin (keyring_file, keyring_vault, or component_keyring_*)."},
	{parser.TableEncryption, V8_4_LTS}:     {Algorithm: AlgoCopy, Lock: LockShared, RebuildsTable: true, Notes: "COPY algorithm — full table rebuild. Reads allowed, writes blocked during re-encryption. Requires keyring plugin (keyring_file, keyring_vault, or component_keyring_*)."},

	// ═══════════════════════════════════════════════════
	// CHANGE INDEX TYPE (§1.6) — DROP INDEX + ADD INDEX (same name)
	// Changing only the USING clause (BTREE/HASH) on an existing index is metadata-only.
	// InnoDB always stores secondary indexes as B-trees regardless of the USING hint,
	// so this is INSTANT — only the data dictionary entry is updated.
	// ═══════════════════════════════════════════════════
	{parser.ChangeIndexType, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. INSTANT algorithm not available before 8.0.12. InnoDB always uses B-tree for secondary indexes; the USING clause is stored in the data dictionary only."},
	{parser.ChangeIndexType, V8_0_Instant}: {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "INSTANT, metadata-only. InnoDB always uses B-tree for secondary indexes; the USING clause is stored in the data dictionary only."},
	{parser.ChangeIndexType, V8_0_Full}:    {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "INSTANT, metadata-only. InnoDB always uses B-tree for secondary indexes; the USING clause is stored in the data dictionary only."},
	{parser.ChangeIndexType, V8_4_LTS}:     {Algorithm: AlgoInstant, Lock: LockNone, RebuildsTable: false, Notes: "INSTANT, metadata-only. InnoDB always uses B-tree for secondary indexes; the USING clause is stored in the data dictionary only."},

	// ═══════════════════════════════════════════════════
	// REPLACE PRIMARY KEY (§2.3) — DROP PRIMARY KEY + ADD PRIMARY KEY
	// The combined DROP+ADD PK is handled as a single InnoDB operation: INPLACE, LOCK=NONE,
	// but requires a table rebuild to reorganize the clustered index. Standalone DROP PK is COPY.
	// ═══════════════════════════════════════════════════
	{parser.ReplacePrimaryKey, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "Combined DROP PRIMARY KEY + ADD PRIMARY KEY: INPLACE with table rebuild. Concurrent DML allowed during rebuild."},
	{parser.ReplacePrimaryKey, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "Combined DROP PRIMARY KEY + ADD PRIMARY KEY: INPLACE with table rebuild. Concurrent DML allowed during rebuild."},
	{parser.ReplacePrimaryKey, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "Combined DROP PRIMARY KEY + ADD PRIMARY KEY: INPLACE with table rebuild. Concurrent DML allowed during rebuild."},
	{parser.ReplacePrimaryKey, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "Combined DROP PRIMARY KEY + ADD PRIMARY KEY: INPLACE with table rebuild. Concurrent DML allowed during rebuild."},

	// ═══════════════════════════════════════════════════
	// OPTIMIZE TABLE (§6.7)
	// MySQL maps OPTIMIZE TABLE to ALTER TABLE ... FORCE for InnoDB tables.
	// INPLACE algorithm with a full table rebuild; concurrent DML is allowed during the rebuild.
	// ═══════════════════════════════════════════════════
	{parser.OptimizeTable, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "Mapped to ALTER TABLE ... FORCE internally. INPLACE with full table rebuild. Reclaims fragmented space. Concurrent DML allowed during rebuild."},
	{parser.OptimizeTable, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "Mapped to ALTER TABLE ... FORCE internally. INPLACE with full table rebuild. Reclaims fragmented space. Concurrent DML allowed during rebuild."},
	{parser.OptimizeTable, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "Mapped to ALTER TABLE ... FORCE internally. INPLACE with full table rebuild. Reclaims fragmented space and resets TOTAL_ROW_VERSIONS counter."},
	{parser.OptimizeTable, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: true, Notes: "Mapped to ALTER TABLE ... FORCE internally. INPLACE with full table rebuild. Reclaims fragmented space and resets TOTAL_ROW_VERSIONS counter."},

	// ═══════════════════════════════════════════════════
	// ALTER TABLESPACE RENAME (§7.1)
	// Metadata-only rename of a general tablespace. INPLACE, LOCK=NONE.
	// Does not support the ALGORITHM clause explicitly; always uses INPLACE internally.
	// ═══════════════════════════════════════════════════
	{parser.AlterTablespace, V8_0_Early}:   {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "⚠️ ALTER TABLESPACE ... RENAME TO was introduced in MySQL 8.0.21 and does not exist in 8.0.0-8.0.11. The server will reject this statement with a syntax error on these versions."},
	{parser.AlterTablespace, V8_0_Instant}: {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Requires MySQL 8.0.21+; statement is rejected on 8.0.12-8.0.20. Renames the tablespace entry in the data dictionary. Does not accept ALGORITHM= clause explicitly."},
	{parser.AlterTablespace, V8_0_Full}:    {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Renames the tablespace entry in the data dictionary. Does not accept ALGORITHM= clause explicitly."},
	{parser.AlterTablespace, V8_4_LTS}:     {Algorithm: AlgoInplace, Lock: LockNone, RebuildsTable: false, Notes: "INPLACE, metadata-only. Renames the tablespace entry in the data dictionary. Does not accept ALGORITHM= clause explicitly."},
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
