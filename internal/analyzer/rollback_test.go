package analyzer

import (
	"strings"
	"testing"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
)

func TestGenerateDDLRollback(t *testing.T) {
	defaultVal := "0"
	tests := []struct {
		name              string
		input             Input
		wantRollbackSQL   string
		wantRollbackNotes string
	}{
		{
			name: "ADD COLUMN rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.AddColumn,
					Database:   "testdb",
					Table:      "users",
					ColumnName: "email",
				},
				Version: mysql.ServerVersion{Major: 8, Minor: 0, Patch: 30},
			},
			wantRollbackSQL:   "ALTER TABLE `testdb`.`users` DROP COLUMN `email`;",
			wantRollbackNotes: "DROP COLUMN is INSTANT",
		},
		{
			name: "ADD COLUMN rollback on old MySQL",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.AddColumn,
					Database:   "mydb",
					Table:      "test",
					ColumnName: "status",
				},
				Version: mysql.ServerVersion{Major: 8, Minor: 0, Patch: 11},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`test` DROP COLUMN `status`;",
			wantRollbackNotes: "INPLACE with table rebuild",
		},
		{
			name: "DROP COLUMN rollback - not reversible",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.DropColumn,
					Database:   "testdb",
					Table:      "users",
					ColumnName: "temp",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Cannot automatically reverse DROP COLUMN",
		},
		{
			name: "MODIFY COLUMN rollback with metadata",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.ModifyColumn,
					Database:   "testdb",
					Table:      "users",
					ColumnName: "email",
				},
				Meta: &mysql.TableMetadata{
					Columns: []mysql.ColumnInfo{
						{Name: "email", Type: "varchar(100)", Nullable: false, Default: nil},
					},
				},
			},
			wantRollbackSQL:   "ALTER TABLE `testdb`.`users` MODIFY COLUMN `email` varchar(100) NOT NULL;",
			wantRollbackNotes: "Reverses column change",
		},
		{
			name: "MODIFY COLUMN rollback without metadata",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.ModifyColumn,
					Database:   "testdb",
					Table:      "users",
					ColumnName: "email",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Cannot determine original column definition",
		},
		{
			name: "CHANGE COLUMN rollback with metadata",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:          parser.DDL,
					DDLOp:         parser.ChangeColumn,
					Database:      "testdb",
					Table:         "users",
					OldColumnName: "email",
					NewColumnName: "user_email",
				},
				Meta: &mysql.TableMetadata{
					Columns: []mysql.ColumnInfo{
						{Name: "email", Type: "varchar(100)", Nullable: false, Default: nil},
					},
				},
			},
			wantRollbackSQL:   "ALTER TABLE `testdb`.`users` CHANGE COLUMN `user_email` `email` varchar(100) NOT NULL;",
			wantRollbackNotes: "Reverses column change",
		},
		{
			name: "ADD INDEX rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:      parser.DDL,
					DDLOp:     parser.AddIndex,
					Database:  "mydb",
					Table:     "users",
					IndexName: "idx_email",
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`users` DROP INDEX `idx_email`;",
			wantRollbackNotes: "DROP INDEX is INPLACE with no lock",
		},
		{
			name: "ADD FULLTEXT INDEX rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:      parser.DDL,
					DDLOp:     parser.AddFulltextIndex,
					Database:  "mydb",
					Table:     "posts",
					IndexName: "ft_content",
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`posts` DROP INDEX `ft_content`;",
			wantRollbackNotes: "DROP INDEX is INPLACE",
		},
		{
			name: "ADD SPATIAL INDEX rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:      parser.DDL,
					DDLOp:     parser.AddSpatialIndex,
					Database:  "mydb",
					Table:     "locations",
					IndexName: "sp_coords",
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`locations` DROP INDEX `sp_coords`;",
			wantRollbackNotes: "DROP INDEX is INPLACE",
		},
		{
			name: "DROP INDEX rollback - need original definition",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:      parser.DDL,
					DDLOp:     parser.DropIndex,
					Database:  "testdb",
					Table:     "test",
					IndexName: "old_idx",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Recreate the index using the original definition",
		},
		{
			name: "ADD PRIMARY KEY rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.AddPrimaryKey,
					Database: "mydb",
					Table:    "events",
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`events` DROP PRIMARY KEY;",
			wantRollbackNotes: "DROP PRIMARY KEY requires COPY",
		},
		{
			name: "DROP PRIMARY KEY rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.DropPrimaryKey,
					Database: "mydb",
					Table:    "events",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Recreate the primary key",
		},
		{
			name: "ADD FOREIGN KEY rollback with constraint name",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:      parser.DDL,
					DDLOp:     parser.AddForeignKey,
					Database:  "mydb",
					Table:     "orders",
					IndexName: "fk_customer",
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`orders` DROP FOREIGN KEY `fk_customer`;",
			wantRollbackNotes: "DROP FOREIGN KEY is INPLACE",
		},
		{
			name: "DROP FOREIGN KEY rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:      parser.DDL,
					DDLOp:     parser.DropForeignKey,
					Database:  "mydb",
					Table:     "orders",
					IndexName: "fk_customer",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Recreate the foreign key",
		},
		{
			name: "ADD CHECK CONSTRAINT rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:      parser.DDL,
					DDLOp:     parser.AddCheckConstraint,
					Database:  "mydb",
					Table:     "orders",
					IndexName: "chk_amount",
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`orders` DROP CHECK `chk_amount`;",
			wantRollbackNotes: "DROP CHECK is an INPLACE",
		},
		{
			name: "RENAME TABLE rollback with new name",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:         parser.DDL,
					DDLOp:        parser.RenameTable,
					Database:     "db",
					Table:        "old_name",
					NewTableName: "new_name",
				},
			},
			wantRollbackSQL:   "RENAME TABLE `db`.`new_name` TO `db`.`old_name`;",
			wantRollbackNotes: "metadata-only operation",
		},
		{
			name: "RENAME TABLE rollback without new name",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.RenameTable,
					Database: "db",
					Table:    "old_name",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Reverse the RENAME TABLE",
		},
		{
			name: "RENAME INDEX rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:         parser.DDL,
					DDLOp:        parser.RenameIndex,
					Database:     "mydb",
					Table:        "users",
					IndexName:    "idx_old",
					NewIndexName: "idx_new",
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`users` RENAME INDEX `idx_new` TO `idx_old`;",
			wantRollbackNotes: "metadata-only operation",
		},
		{
			name: "CHANGE ENGINE rollback with metadata",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.ChangeEngine,
					Database: "mydb",
					Table:    "logs",
				},
				Meta: &mysql.TableMetadata{Engine: "InnoDB"},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`logs` ENGINE=InnoDB;",
			wantRollbackNotes: "Restores the original storage engine",
		},
		{
			name: "CHANGE ENGINE rollback without metadata",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.ChangeEngine,
					Database: "mydb",
					Table:    "logs",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Revert ENGINE using the original engine",
		},
		{
			name: "CHANGE ROW_FORMAT rollback with metadata",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.ChangeRowFormat,
					Database: "mydb",
					Table:    "data",
				},
				Meta: &mysql.TableMetadata{RowFormat: "Dynamic"},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`data` ROW_FORMAT=Dynamic;",
			wantRollbackNotes: "Restores the original row format",
		},
		{
			name: "SET DEFAULT rollback with original default",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.SetDefault,
					Database:   "mydb",
					Table:      "users",
					ColumnName: "status",
				},
				Meta: &mysql.TableMetadata{
					Columns: []mysql.ColumnInfo{
						{Name: "status", Type: "int", Default: &defaultVal},
					},
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`users` ALTER COLUMN `status` SET DEFAULT '0';",
			wantRollbackNotes: "SET DEFAULT is a metadata-only operation",
		},
		{
			name: "SET DEFAULT rollback without original default",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.SetDefault,
					Database:   "mydb",
					Table:      "users",
					ColumnName: "name",
				},
				Meta: &mysql.TableMetadata{
					Columns: []mysql.ColumnInfo{
						{Name: "name", Type: "varchar(50)"},
					},
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`users` ALTER COLUMN `name` DROP DEFAULT;",
			wantRollbackNotes: "Drops the default",
		},
		{
			name: "DROP DEFAULT rollback with original default",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.DropDefault,
					Database:   "mydb",
					Table:      "users",
					ColumnName: "status",
				},
				Meta: &mysql.TableMetadata{
					Columns: []mysql.ColumnInfo{
						{Name: "status", Type: "int", Default: &defaultVal},
					},
				},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`users` ALTER COLUMN `status` SET DEFAULT '0';",
			wantRollbackNotes: "Restores the original DEFAULT",
		},
		{
			name: "DROP DEFAULT rollback without original default",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:       parser.DDL,
					DDLOp:      parser.DropDefault,
					Database:   "mydb",
					Table:      "users",
					ColumnName: "name",
				},
				Meta: &mysql.TableMetadata{
					Columns: []mysql.ColumnInfo{
						{Name: "name", Type: "varchar(50)"},
					},
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Cannot determine original default",
		},
		{
			name: "CHANGE AUTO_INCREMENT rollback with metadata",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.ChangeAutoIncrement,
					Database: "mydb",
					Table:    "users",
				},
				Meta: &mysql.TableMetadata{AutoIncrement: 42},
			},
			wantRollbackSQL:   "ALTER TABLE `mydb`.`users` AUTO_INCREMENT=42;",
			wantRollbackNotes: "Restores the original AUTO_INCREMENT",
		},
		{
			name: "CREATE TABLE rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.CreateTable,
					Database: "mydb",
					Table:    "new_table",
				},
			},
			wantRollbackSQL:   "DROP TABLE IF EXISTS `mydb`.`new_table`;",
			wantRollbackNotes: "WARNING: DROP TABLE is irreversible",
		},
		{
			name: "FORCE REBUILD - no rollback needed",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.ForceRebuild,
					Database: "mydb",
					Table:    "data",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "No rollback needed",
		},
		{
			name: "OPTIMIZE TABLE - no rollback needed",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.OptimizeTable,
					Database: "mydb",
					Table:    "data",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "No rollback needed",
		},
		{
			name: "DROP PARTITION - data lost",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.DropPartition,
					Database: "mydb",
					Table:    "events",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Cannot reverse DROP PARTITION",
		},
		{
			name: "TRUNCATE PARTITION - data lost",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.TruncatePartition,
					Database: "mydb",
					Table:    "events",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Cannot reverse TRUNCATE PARTITION",
		},
		{
			name: "ADD PARTITION rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.AddPartition,
					Database: "mydb",
					Table:    "events",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Reverse with ALTER TABLE ... DROP PARTITION",
		},
		{
			name: "CHANGE CHARSET rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.ChangeCharset,
					Database: "mydb",
					Table:    "data",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Revert the table default character set",
		},
		{
			name: "CONVERT CHARSET rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.ConvertCharset,
					Database: "mydb",
					Table:    "data",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "CONVERT TO CHARACTER SET rewrites all string columns",
		},
		{
			name: "MULTIPLE OPS rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.MultipleOps,
					Database: "mydb",
					Table:    "data",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Multi-operation ALTER TABLE",
		},
		{
			name: "Other DDL - manual rollback",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DDL,
					DDLOp:    parser.OtherDDL,
					Database: "db",
					Table:    "table",
				},
			},
			wantRollbackSQL:   "",
			wantRollbackNotes: "Review SHOW CREATE TABLE",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &Result{
				Database: tt.input.Parsed.Database,
				Table:    tt.input.Parsed.Table,
			}

			generateDDLRollback(tt.input, result)

			if result.RollbackSQL != tt.wantRollbackSQL {
				t.Errorf("RollbackSQL = %q, want %q", result.RollbackSQL, tt.wantRollbackSQL)
			}

			if !strings.Contains(result.RollbackNotes, tt.wantRollbackNotes) {
				t.Errorf("RollbackNotes should contain %q, got %q", tt.wantRollbackNotes, result.RollbackNotes)
			}
		})
	}
}

func TestGenerateDMLRollback(t *testing.T) {
	tests := []struct {
		name                string
		input               Input
		wantRollbackOptions int
		wantBackupSQL       bool
		wantBinlogOption    bool
	}{
		{
			name: "DELETE with WHERE clause",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:        parser.DML,
					DMLOp:       parser.Delete,
					Database:    "mydb",
					Table:       "users",
					HasWhere:    true,
					WhereClause: "id > 1000",
				},
				Meta: &mysql.TableMetadata{
					Database:     "mydb",
					Table:        "users",
					AvgRowLength: 200,
				},
			},
			wantRollbackOptions: 2, // Pre-backup + Point-in-time
			wantBackupSQL:       true,
			wantBinlogOption:    true,
		},
		{
			name: "UPDATE without WHERE",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:     parser.DML,
					DMLOp:    parser.Update,
					Database: "testdb",
					Table:    "logs",
					HasWhere: false,
				},
				Meta: &mysql.TableMetadata{
					Database:     "testdb",
					Table:        "logs",
					AvgRowLength: 100,
				},
			},
			wantRollbackOptions: 2,
			wantBackupSQL:       true,
			wantBinlogOption:    true,
		},
		{
			name: "DELETE affecting many rows",
			input: Input{
				Parsed: &parser.ParsedSQL{
					Type:        parser.DML,
					DMLOp:       parser.Delete,
					Database:    "bigdb",
					Table:       "events",
					HasWhere:    true,
					WhereClause: "created_at < '2020-01-01'",
				},
				Meta: &mysql.TableMetadata{
					Database:     "bigdb",
					Table:        "events",
					AvgRowLength: 500,
				},
			},
			wantRollbackOptions: 2,
			wantBackupSQL:       true,
			wantBinlogOption:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := &Result{
				Database:     tt.input.Parsed.Database,
				Table:        tt.input.Parsed.Table,
				AffectedRows: 1000,
			}

			generateDMLRollback(tt.input, result)

			if len(result.RollbackOptions) != tt.wantRollbackOptions {
				t.Errorf("got %d rollback options, want %d", len(result.RollbackOptions), tt.wantRollbackOptions)
			}

			// Check for pre-backup option
			foundBackup := false
			foundBinlog := false
			for _, opt := range result.RollbackOptions {
				if strings.Contains(opt.Label, "Pre-backup") || strings.Contains(opt.Label, "backup") {
					foundBackup = true
					if !strings.Contains(opt.SQL, "CREATE TABLE") {
						t.Error("backup option should contain CREATE TABLE")
					}
					if !strings.Contains(opt.SQL, "INSERT INTO") {
						t.Error("backup option should contain INSERT INTO (restore command)")
					}
				}
				if strings.Contains(opt.Label, "Point-in-time") || strings.Contains(opt.Label, "binlog") {
					foundBinlog = true
					if !strings.Contains(opt.Description, "binlog") {
						t.Error("binlog option should mention binlog in description")
					}
				}
			}

			if tt.wantBackupSQL && !foundBackup {
				t.Error("should have pre-backup rollback option")
			}
			if tt.wantBinlogOption && !foundBinlog {
				t.Error("should have binlog-based rollback option")
			}
		})
	}
}

func TestGenerateDMLRollback_BackupTableNaming(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:        parser.DML,
			DMLOp:       parser.Delete,
			Database:    "mydb",
			Table:       "users",
			HasWhere:    true,
			WhereClause: "active = 0",
		},
		Meta: &mysql.TableMetadata{
			Database:     "mydb",
			Table:        "users",
			AvgRowLength: 100,
		},
	}

	result := &Result{
		Database:     "mydb",
		Table:        "users",
		AffectedRows: 100,
	}

	generateDMLRollback(input, result)

	// Find backup option
	var backupSQL string
	for _, opt := range result.RollbackOptions {
		if strings.Contains(opt.SQL, "CREATE TABLE") {
			backupSQL = opt.SQL
			break
		}
	}

	if backupSQL == "" {
		t.Fatal("should generate backup SQL")
	}

	// Backup table should be named: users_backup_YYYYMMDD
	if !strings.Contains(backupSQL, "users_backup_") {
		t.Errorf("backup table should be named users_backup_*, got: %s", backupSQL)
	}

	// Should include WHERE clause in backup
	if !strings.Contains(backupSQL, "active = 0") {
		t.Error("backup SQL should include WHERE clause")
	}
}

func TestGenerateDMLRollback_WithoutWhere(t *testing.T) {
	input := Input{
		Parsed: &parser.ParsedSQL{
			Type:     parser.DML,
			DMLOp:    parser.Update,
			Database: "db",
			Table:    "logs",
			HasWhere: false, // No WHERE clause
		},
		Meta: &mysql.TableMetadata{
			Database:     "db",
			Table:        "logs",
			AvgRowLength: 50,
		},
	}

	result := &Result{
		Database:     "db",
		Table:        "logs",
		AffectedRows: 5000,
	}

	generateDMLRollback(input, result)

	// Find backup option
	var backupSQL string
	for _, opt := range result.RollbackOptions {
		if strings.Contains(opt.SQL, "CREATE TABLE") {
			backupSQL = opt.SQL
			break
		}
	}

	// Backup without WHERE should select all rows
	if strings.Contains(backupSQL, "WHERE") {
		t.Error("backup SQL should not contain WHERE when original statement has no WHERE")
	}
}
