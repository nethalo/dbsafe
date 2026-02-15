package parser

import (
	"testing"
)

// Benchmark SQL parsing performance

func BenchmarkParse_SimpleSelect(b *testing.B) {
	sql := "SELECT * FROM users WHERE id = 1"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(sql)
	}
}

func BenchmarkParse_ComplexDDL(b *testing.B) {
	sql := `ALTER TABLE users
		ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT '',
		ADD INDEX idx_email (email),
		MODIFY COLUMN status INT DEFAULT 0`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(sql)
	}
}

func BenchmarkParse_LongTableName(b *testing.B) {
	sql := "ALTER TABLE very_long_database_name.very_long_table_name_with_many_characters ADD COLUMN test VARCHAR(100)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(sql)
	}
}

func BenchmarkParse_DMLWithComplexWhere(b *testing.B) {
	sql := `DELETE FROM logs
		WHERE created_at < '2020-01-01'
		AND status IN ('archived', 'deleted', 'expired')
		AND user_id NOT IN (SELECT id FROM active_users)`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(sql)
	}
}

func BenchmarkParse_MultipleAlterOperations(b *testing.B) {
	sql := `ALTER TABLE products
		ADD COLUMN description TEXT,
		ADD COLUMN price DECIMAL(10,2),
		ADD INDEX idx_price (price),
		MODIFY COLUMN status VARCHAR(50),
		DROP COLUMN deprecated_field`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(sql)
	}
}

func BenchmarkParse_WithBackticks(b *testing.B) {
	sql := "ALTER TABLE `my-database`.`my-table` ADD COLUMN `special-column` VARCHAR(100)"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(sql)
	}
}

func BenchmarkParse_InsertStatement(b *testing.B) {
	sql := "INSERT INTO users (name, email, created_at) VALUES ('John Doe', 'john@example.com', NOW())"
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(sql)
	}
}

func BenchmarkParse_UpdateWithJoin(b *testing.B) {
	sql := `UPDATE users u
		INNER JOIN orders o ON u.id = o.user_id
		SET u.last_order = o.created_at
		WHERE o.status = 'completed'`
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Parse(sql)
	}
}

// Benchmark end-to-end parsing with validation

func BenchmarkParse_FullValidation(b *testing.B) {
	sql := "ALTER TABLE users ADD COLUMN email VARCHAR(255) NOT NULL DEFAULT 'test@example.com'"
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		result, err := Parse(sql)
		if err != nil {
			b.Fatal(err)
		}
		_ = result.ColumnName
		_ = result.HasNotNull
		_ = result.HasDefault
	}
}

func BenchmarkParse_Concurrent(b *testing.B) {
	sql := "DELETE FROM logs WHERE created_at < '2020-01-01'"
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = Parse(sql)
		}
	})
}
