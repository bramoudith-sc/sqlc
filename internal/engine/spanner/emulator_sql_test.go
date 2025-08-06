//go:build emulator
// +build emulator

package spanner

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	_ "github.com/googleapis/go-sql-spanner"
)

// TestWithSQLDriverEmulator tests using the go-sql-spanner driver with autoConfigEmulator
// This is simpler as it automatically configures the emulator when SPANNER_EMULATOR_HOST is set
// Run with: SPANNER_EMULATOR_HOST=localhost:9010 go test -tags=emulator ./internal/engine/spanner/
func TestWithSQLDriverEmulator(t *testing.T) {
	ctx := context.Background()
	
	// The go-sql-spanner driver automatically configures the emulator
	// when SPANNER_EMULATOR_HOST is set and autoConfigEmulator=true
	dsn := "projects/test-project/instances/test-instance/databases/test-db?autoConfigEmulator=true"
	
	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test connection
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("Failed to ping database: %v", err)
	}

	// Create schema
	if err := createSchema(ctx, db); err != nil {
		t.Logf("Schema might already exist: %v", err)
	}

	// Run tests
	t.Run("BasicOperations", func(t *testing.T) {
		testBasicSQLOperations(t, ctx, db)
	})

	t.Run("IntervalType", func(t *testing.T) {
		testSQLIntervalType(t, ctx, db)
	})

	t.Run("JSONType", func(t *testing.T) {
		testSQLJSONType(t, ctx, db)
	})

	t.Run("ArrayType", func(t *testing.T) {
		testSQLArrayType(t, ctx, db)
	})

	t.Run("SafeFunctions", func(t *testing.T) {
		testSQLSafeFunctions(t, ctx, db)
	})

	t.Run("ThenReturn", func(t *testing.T) {
		testSQLThenReturn(t, ctx, db)
	})
}

func createSchema(ctx context.Context, db *sql.DB) error {
	// Create tables
	schemas := []string{
		`CREATE TABLE IF NOT EXISTS users (
			id INT64 NOT NULL,
			name STRING(100),
			email STRING(100),
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			metadata JSON
		) PRIMARY KEY (id)`,
		`CREATE TABLE IF NOT EXISTS posts (
			id INT64 NOT NULL,
			user_id INT64 NOT NULL,
			title STRING(200),
			content STRING(MAX),
			published BOOL,
			published_at TIMESTAMP,
			tags ARRAY<STRING(50)>,
			view_count INT64 DEFAULT (0),
			CONSTRAINT FK_UserPosts FOREIGN KEY (user_id) REFERENCES users (id)
		) PRIMARY KEY (id)`,
	}

	for _, schema := range schemas {
		if _, err := db.ExecContext(ctx, schema); err != nil {
			return fmt.Errorf("failed to create schema: %w", err)
		}
	}
	return nil
}

func testBasicSQLOperations(t *testing.T, ctx context.Context, db *sql.DB) {
	// Clean up first
	db.ExecContext(ctx, "DELETE FROM posts WHERE true")
	db.ExecContext(ctx, "DELETE FROM users WHERE true")

	// Test INSERT
	result, err := db.ExecContext(ctx, 
		"INSERT INTO users (id, name, email, created_at) VALUES (@id, @name, @email, @created_at)",
		sql.Named("id", 1),
		sql.Named("name", "Alice"),
		sql.Named("email", "alice@example.com"),
		sql.Named("created_at", time.Now()),
	)
	if err != nil {
		t.Fatalf("Failed to insert user: %v", err)
	}

	// Check rows affected
	if rows, _ := result.RowsAffected(); rows != 1 {
		t.Errorf("Expected 1 row affected, got %d", rows)
	}

	// Test SELECT
	var id int64
	var name, email sql.NullString
	var createdAt sql.NullTime
	
	row := db.QueryRowContext(ctx,
		"SELECT id, name, email, created_at FROM users WHERE id = @id",
		sql.Named("id", 1),
	)
	
	if err := row.Scan(&id, &name, &email, &createdAt); err != nil {
		t.Fatalf("Failed to scan user: %v", err)
	}

	if id != 1 || !name.Valid || name.String != "Alice" {
		t.Errorf("Unexpected user data: id=%d, name=%v", id, name)
	}

	// Test UPDATE
	_, err = db.ExecContext(ctx,
		"UPDATE users SET name = @name, updated_at = CURRENT_TIMESTAMP() WHERE id = @id",
		sql.Named("name", "Alice Updated"),
		sql.Named("id", 1),
	)
	if err != nil {
		t.Fatalf("Failed to update user: %v", err)
	}

	// Test DELETE
	_, err = db.ExecContext(ctx,
		"DELETE FROM users WHERE id = @id",
		sql.Named("id", 1),
	)
	if err != nil {
		t.Fatalf("Failed to delete user: %v", err)
	}
}

func testSQLIntervalType(t *testing.T, ctx context.Context, db *sql.DB) {
	// Test INTERVAL arithmetic
	rows, err := db.QueryContext(ctx, `
		SELECT 
			CURRENT_DATE() as today,
			DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) as tomorrow,
			DATE_SUB(CURRENT_DATE(), INTERVAL 1 MONTH) as last_month,
			TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR) as next_hour
	`)
	if err != nil {
		t.Fatalf("Failed to query with INTERVAL: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("No rows returned")
	}

	var today, tomorrow, lastMonth civil.Date
	var nextHour time.Time
	
	if err := rows.Scan(&today, &tomorrow, &lastMonth, &nextHour); err != nil {
		t.Fatalf("Failed to scan interval results: %v", err)
	}

	t.Logf("Interval operations: today=%v, tomorrow=%v, last_month=%v, next_hour=%v", 
		today, tomorrow, lastMonth, nextHour)

	// Verify the date arithmetic
	if tomorrow.Day != today.Day+1 && tomorrow.Day != 1 { // Handle month boundary
		t.Errorf("Tomorrow calculation incorrect: today=%v, tomorrow=%v", today, tomorrow)
	}
}

func testSQLJSONType(t *testing.T, ctx context.Context, db *sql.DB) {
	// Clean and insert test data
	db.ExecContext(ctx, "DELETE FROM users WHERE true")
	
	jsonData := `{"name": "Alice", "age": 30, "hobbies": ["reading", "coding"]}`
	
	// Use PARSE_JSON to convert string to JSON type
	_, err := db.ExecContext(ctx,
		"INSERT INTO users (id, name, metadata) VALUES (@id, @name, PARSE_JSON(@metadata))",
		sql.Named("id", 2),
		sql.Named("name", "JSON Test User"),
		sql.Named("metadata", jsonData),
	)
	if err != nil {
		t.Fatalf("Failed to insert user with JSON: %v", err)
	}

	// Test JSON functions
	testCases := []struct {
		name  string
		query string
		want  interface{}
	}{
		{
			name:  "JSON_VALUE string",
			query: "SELECT JSON_VALUE(metadata, '$.name') FROM users WHERE id = 2",
			want:  "Alice",
		},
		{
			name:  "JSON_VALUE number",
			query: "SELECT JSON_VALUE(metadata, '$.age') FROM users WHERE id = 2",
			want:  "30",
		},
		// JSON_QUERY is commented out because it returns spanner.NullJSON which needs special handling
		// {
		// 	name:  "JSON_QUERY array",
		// 	query: "SELECT JSON_QUERY(metadata, '$.hobbies') FROM users WHERE id = 2",
		// 	want:  `["reading","coding"]`,
		// },
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var result sql.NullString
			row := db.QueryRowContext(ctx, tc.query)
			if err := row.Scan(&result); err != nil {
				t.Fatalf("Failed to scan JSON result: %v", err)
			}
			
			if !result.Valid {
				t.Errorf("Expected valid result, got NULL")
			} else if result.String != tc.want {
				t.Errorf("Expected %q, got %q", tc.want, result.String)
			}
		})
	}
}

func testSQLArrayType(t *testing.T, ctx context.Context, db *sql.DB) {
	// Clean and insert test data
	db.ExecContext(ctx, "DELETE FROM posts WHERE true")
	db.ExecContext(ctx, "DELETE FROM users WHERE true")
	
	// Create user first for foreign key
	db.ExecContext(ctx,
		"INSERT INTO users (id, name) VALUES (@id, @name)",
		sql.Named("id", 2),
		sql.Named("name", "Test User"),
	)
	
	// Note: go-sql-spanner handles arrays differently
	// We'll use ARRAY constructor in SQL
	_, err := db.ExecContext(ctx, `
		INSERT INTO posts (id, user_id, title, tags) 
		VALUES (@id, @user_id, @title, ARRAY['golang', 'spanner', 'database'])`,
		sql.Named("id", 1),
		sql.Named("user_id", 2),
		sql.Named("title", "Test Post"),
	)
	if err != nil {
		t.Fatalf("Failed to insert post with array: %v", err)
	}

	// Test ARRAY functions
	var arrayLength sql.NullInt64
	row := db.QueryRowContext(ctx,
		"SELECT ARRAY_LENGTH(tags) FROM posts WHERE id = @id",
		sql.Named("id", 1),
	)
	
	if err := row.Scan(&arrayLength); err != nil {
		t.Fatalf("Failed to scan array length: %v", err)
	}

	if !arrayLength.Valid || arrayLength.Int64 != 3 {
		t.Errorf("Expected array length 3, got %v", arrayLength)
	}

	// Test ARRAY_INCLUDES
	var hasTag sql.NullBool
	row = db.QueryRowContext(ctx,
		"SELECT ARRAY_INCLUDES(tags, @tag) FROM posts WHERE id = @id",
		sql.Named("id", 1),
		sql.Named("tag", "golang"),
	)
	
	if err := row.Scan(&hasTag); err != nil {
		t.Fatalf("Failed to scan ARRAY_INCLUDES: %v", err)
	}

	if !hasTag.Valid || !hasTag.Bool {
		t.Errorf("Expected ARRAY_INCLUDES to return true, got %v", hasTag)
	}
}

func testSQLSafeFunctions(t *testing.T, ctx context.Context, db *sql.DB) {
	// Test SAFE functions that handle errors gracefully
	// Note: Some SAFE functions might not be available in emulator
	rows, err := db.QueryContext(ctx, `
		SELECT 
			SAFE_DIVIDE(10.0, 2.0) as normal_divide,
			SAFE_DIVIDE(10.0, 0.0) as divide_by_zero
	`)
	if err != nil {
		t.Fatalf("Failed to query SAFE functions: %v", err)
	}
	defer rows.Close()

	if !rows.Next() {
		t.Fatal("No rows returned")
	}

	var normalDivide, divideByZero sql.NullFloat64
	
	if err := rows.Scan(&normalDivide, &divideByZero); err != nil {
		t.Fatalf("Failed to scan SAFE results: %v", err)
	}

	// Check normal operation
	if !normalDivide.Valid || normalDivide.Float64 != 5.0 {
		t.Errorf("Expected normal divide to be 5.0, got %v", normalDivide)
	}

	// Check NULL results for error cases
	if divideByZero.Valid {
		t.Errorf("Expected NULL for divide by zero, got %v", divideByZero)
	}
}

func testSQLThenReturn(t *testing.T, ctx context.Context, db *sql.DB) {
	// Clean up
	db.ExecContext(ctx, "DELETE FROM posts WHERE true")
	db.ExecContext(ctx, "DELETE FROM users WHERE true")
	
	// Create user first for foreign key
	db.ExecContext(ctx,
		"INSERT INTO users (id, name) VALUES (@id, @name)",
		sql.Named("id", 2),
		sql.Named("name", "Test User"),
	)
	
	// Test INSERT ... THEN RETURN
	var returnedID int64
	var returnedTitle sql.NullString
	var returnedViewCount sql.NullInt64
	
	row := db.QueryRowContext(ctx, `
		INSERT INTO posts (id, user_id, title, published) 
		VALUES (@id, @user_id, @title, @published)
		THEN RETURN id, title, view_count`,
		sql.Named("id", 100),
		sql.Named("user_id", 2),
		sql.Named("title", "New Post"),
		sql.Named("published", true),
	)
	
	if err := row.Scan(&returnedID, &returnedTitle, &returnedViewCount); err != nil {
		t.Fatalf("Failed to scan THEN RETURN results: %v", err)
	}

	if returnedID != 100 {
		t.Errorf("Expected returned ID 100, got %d", returnedID)
	}

	if !returnedTitle.Valid || returnedTitle.String != "New Post" {
		t.Errorf("Expected returned title 'New Post', got %v", returnedTitle)
	}

	if !returnedViewCount.Valid || returnedViewCount.Int64 != 0 {
		t.Errorf("Expected default view_count 0, got %v", returnedViewCount)
	}

	// Test UPDATE ... THEN RETURN
	row = db.QueryRowContext(ctx, `
		UPDATE posts 
		SET title = @new_title, view_count = view_count + 1
		WHERE id = @id
		THEN RETURN id, title, view_count`,
		sql.Named("new_title", "Updated Post"),
		sql.Named("id", 100),
	)
	
	if err := row.Scan(&returnedID, &returnedTitle, &returnedViewCount); err != nil {
		t.Fatalf("Failed to scan UPDATE THEN RETURN: %v", err)
	}

	if returnedTitle.String != "Updated Post" {
		t.Errorf("Expected updated title 'Updated Post', got %v", returnedTitle)
	}

	if returnedViewCount.Int64 != 1 {
		t.Errorf("Expected incremented view_count 1, got %v", returnedViewCount)
	}
}