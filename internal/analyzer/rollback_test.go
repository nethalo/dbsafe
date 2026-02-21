package analyzer

import (
	"strings"
	"testing"

	"github.com/nethalo/dbsafe/internal/mysql"
	"github.com/nethalo/dbsafe/internal/parser"
)

func TestGenerateDDLRollback(t *testing.T) {
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
			name: "RENAME TABLE rollback",
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
