//go:build emulator
// +build emulator

package spanner_features

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	_ "github.com/googleapis/go-sql-spanner"
)

const (
	testProjectID  = "test-project"
	testInstanceID = "test-instance"
	testDatabaseID = "test-db"
)

// TestGeneratedCodeWithEmulator tests the sqlc-generated code with Spanner emulator
// Run with: SPANNER_EMULATOR_HOST=localhost:9010 go test -tags=emulator ./internal/endtoend/testdata/spanner_features/go/
func TestGeneratedCodeWithEmulator(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set, skipping emulator tests")
	}

	ctx := context.Background()

	// Connect using go-sql-spanner driver with autoConfigEmulator
	dsn := fmt.Sprintf("projects/%s/instances/%s/databases/%s?autoConfigEmulator=true",
		testProjectID, testInstanceID, testDatabaseID)

	db, err := sql.Open("spanner", dsn)
	if err != nil {
		t.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Create schema (if not exists)
	if err := createSchema(ctx, db); err != nil {
		t.Logf("Schema setup: %v", err)
	}

	// Setup test data
	if err := setupTestData(ctx, db); err != nil {
		t.Fatalf("Failed to setup test data: %v", err)
	}

	// Create Queries instance using generated code
	queries := New(db)

	// Test generated functions
	t.Run("GetUserGrade", func(t *testing.T) {
		testGetUserGrade(t, ctx, queries)
	})

	t.Run("GetUserIdAsInt", func(t *testing.T) {
		testGetUserIdAsInt(t, ctx, queries)
	})

	t.Run("GetUserDisplayName", func(t *testing.T) {
		testGetUserDisplayName(t, ctx, queries)
	})

	t.Run("GetActiveUsers", func(t *testing.T) {
		testGetActiveUsers(t, ctx, queries)
	})

	t.Run("GetDeletedUsers", func(t *testing.T) {
		testGetDeletedUsers(t, ctx, queries)
	})

	t.Run("GetUsersByStatus", func(t *testing.T) {
		testGetUsersByStatus(t, ctx, queries)
	})

	t.Run("GetUserWithPosts", func(t *testing.T) {
		testGetUserWithPosts(t, ctx, queries)
	})

	t.Run("GetUsersWithPostCount", func(t *testing.T) {
		testGetUsersWithPostCount(t, ctx, queries)
	})

	t.Run("GetUserNameOrDefault", func(t *testing.T) {
		testGetUserNameOrDefault(t, ctx, queries)
	})

	t.Run("GetUserStatusNullIfDeleted", func(t *testing.T) {
		testGetUserStatusNullIfDeleted(t, ctx, queries)
	})

	t.Run("GetFirstNonNullValue", func(t *testing.T) {
		testGetFirstNonNullValue(t, ctx, queries)
	})
}

func createSchema(ctx context.Context, db *sql.DB) error {
	// Drop existing tables first
	db.ExecContext(ctx, "DROP TABLE IF EXISTS posts")
	db.ExecContext(ctx, "DROP TABLE IF EXISTS users")
	
	// Create tables based on schema.sql
	schemas := []string{
		`CREATE TABLE users (
			id STRING(36) NOT NULL,
			name STRING(100),
			email STRING(255),
			score INT64,
			status STRING(50),
			created_at TIMESTAMP,
			updated_at TIMESTAMP,
			deleted_at TIMESTAMP,
		) PRIMARY KEY (id)`,
		`CREATE TABLE posts (
			id STRING(36) NOT NULL,
			user_id STRING(36) NOT NULL,
			title STRING(200),
			content STRING(MAX),
			created_at TIMESTAMP,
			CONSTRAINT FK_UserPosts FOREIGN KEY (user_id) REFERENCES users (id),
		) PRIMARY KEY (id)`,
	}

	for _, schema := range schemas {
		if _, err := db.ExecContext(ctx, schema); err != nil {
			// Ignore "already exists" errors
			continue
		}
	}
	return nil
}

func setupTestData(ctx context.Context, db *sql.DB) error {
	// Clean existing data
	db.ExecContext(ctx, "DELETE FROM posts WHERE TRUE")
	db.ExecContext(ctx, "DELETE FROM users WHERE TRUE")

	// Insert test users
	users := []struct {
		id        string
		name      sql.NullString
		email     sql.NullString
		score     sql.NullInt64
		status    sql.NullString
		deletedAt interface{}
	}{
		{
			"user-1",
			sql.NullString{String: "Alice", Valid: true},
			sql.NullString{String: "alice@example.com", Valid: true},
			sql.NullInt64{Int64: 95, Valid: true},
			sql.NullString{String: "active", Valid: true},
			spanner.NullTime{Valid: false},
		},
		{
			"user-2",
			sql.NullString{String: "Bob", Valid: true},
			sql.NullString{String: "bob@example.com", Valid: true},
			sql.NullInt64{Int64: 75, Valid: true},
			sql.NullString{String: "pending", Valid: true},
			spanner.NullTime{Valid: false},
		},
		{
			"user-3",
			sql.NullString{String: "Charlie", Valid: true},
			sql.NullString{String: "charlie@example.com", Valid: true},
			sql.NullInt64{Int64: 45, Valid: true},
			sql.NullString{String: "verified", Valid: true},
			spanner.NullTime{Valid: false},
		},
		{
			"user-4",
			sql.NullString{String: "David", Valid: true},
			sql.NullString{String: "david@example.com", Valid: true},
			sql.NullInt64{Int64: 85, Valid: true},
			sql.NullString{String: "active", Valid: true},
			spanner.NullTime{Valid: true}, // deleted user
		},
		{
			"user-5",
			sql.NullString{String: "Eve", Valid: true},
			sql.NullString{Valid: false}, // NULL email for COALESCE test
			sql.NullInt64{Int64: 60, Valid: true},
			sql.NullString{String: "inactive", Valid: true},
			spanner.NullTime{Valid: false},
		},
	}

	for _, u := range users {
		_, err := db.ExecContext(ctx,
			`INSERT INTO users (id, name, email, score, status, deleted_at) 
			 VALUES (@p1, @p2, @p3, @p4, @p5, @p6)`,
			u.id, u.name, u.email, u.score, u.status, u.deletedAt)
		if err != nil {
			return fmt.Errorf("failed to insert user %s: %w", u.id, err)
		}
	}

	// Insert test posts
	posts := []struct {
		id      string
		userId  string
		title   sql.NullString
		content sql.NullString
	}{
		{
			"post-1",
			"user-1",
			sql.NullString{String: "Alice's First Post", Valid: true},
			sql.NullString{String: "Content 1", Valid: true},
		},
		{
			"post-2",
			"user-1",
			sql.NullString{String: "Alice's Second Post", Valid: true},
			sql.NullString{String: "Content 2", Valid: true},
		},
		{
			"post-3",
			"user-2",
			sql.NullString{String: "Bob's Post", Valid: true},
			sql.NullString{String: "Content 3", Valid: true},
		},
		{
			"post-4",
			"user-3",
			sql.NullString{String: "Charlie's Post", Valid: true},
			sql.NullString{String: "Content 4", Valid: true},
		},
		// User 4 and 5 have no posts (for LEFT JOIN testing)
	}

	for _, p := range posts {
		_, err := db.ExecContext(ctx,
			`INSERT INTO posts (id, user_id, title, content) 
			 VALUES (@p1, @p2, @p3, @p4)`,
			p.id, p.userId, p.title, p.content)
		if err != nil {
			return fmt.Errorf("failed to insert post %s: %w", p.id, err)
		}
	}

	return nil
}


func testGetUserGrade(t *testing.T, ctx context.Context, queries *Queries) {
	result, err := queries.GetUserGrade(ctx, "user-1")
	if err != nil {
		t.Errorf("GetUserGrade failed: %v", err)
	}

	if result.Grade != "Excellent" {
		t.Errorf("Expected grade 'Excellent' for user-1 (score 95), got %s", result.Grade)
	}

	// Test "Good" grade
	result, err = queries.GetUserGrade(ctx, "user-2")
	if err != nil {
		t.Errorf("GetUserGrade failed: %v", err)
	}

	if result.Grade != "Good" {
		t.Errorf("Expected grade 'Good' for user-2 (score 75), got %s", result.Grade)
	}

	// Test "Fail" grade
	result, err = queries.GetUserGrade(ctx, "user-3")
	if err != nil {
		t.Errorf("GetUserGrade failed: %v", err)
	}

	if result.Grade != "Fail" {
		t.Errorf("Expected grade 'Fail' for user-3 (score 45), got %s", result.Grade)
	}
}

func testGetUserIdAsInt(t *testing.T, ctx context.Context, queries *Queries) {
	// Note: This test assumes ID can be cast to INT64, but our IDs are strings
	// So we'll modify the test to handle this appropriately
	// The CAST in the query would fail for non-numeric string IDs
	// This test demonstrates CAST functionality even if it might fail with our test data
	
	// We'll create a user with numeric string ID for this test
	ctx2 := context.Background()
	db := queries.db.(*sql.DB)
	db.ExecContext(ctx2, 
		`INSERT INTO users (id, name, score) VALUES (@p1, @p2, @p3)`,
		"123", "Numeric ID User", int64(50))
	
	numericId, err := queries.GetUserIdAsInt(ctx, "123")
	if err != nil {
		t.Errorf("GetUserIdAsInt failed: %v", err)
	}

	if numericId != 123 {
		t.Errorf("Expected numeric ID 123, got %d", numericId)
	}

	// Clean up
	db.ExecContext(ctx2, `DELETE FROM users WHERE id = @p1`, "123")
}

func testGetUserDisplayName(t *testing.T, ctx context.Context, queries *Queries) {
	// Test user with email
	displayName, err := queries.GetUserDisplayName(ctx, "user-1")
	if err != nil {
		t.Errorf("GetUserDisplayName failed: %v", err)
	}

	// COALESCE should return the non-null name
	if displayName != "Alice" {
		t.Errorf("Expected display name 'Alice', got %v", displayName)
	}

	// Test user without email (user-5 has NULL email)
	displayName, err = queries.GetUserDisplayName(ctx, "user-5")
	if err != nil {
		t.Errorf("GetUserDisplayName failed: %v", err)
	}

	// Should still return the name since it's not NULL
	if displayName != "Eve" {
		t.Errorf("Expected display name 'Eve', got %v", displayName)
	}
}

func testGetActiveUsers(t *testing.T, ctx context.Context, queries *Queries) {
	users, err := queries.GetActiveUsers(ctx)
	if err != nil {
		t.Errorf("GetActiveUsers failed: %v", err)
	}

	// Should return 4 users (all except user-4 who is deleted)
	if len(users) != 4 {
		t.Errorf("Expected 4 active users, got %d", len(users))
	}

	// Check that user-4 (David) is not in the results
	for _, u := range users {
		if u.ID == "user-4" {
			t.Errorf("Deleted user-4 should not be in active users")
		}
	}
}

func testGetDeletedUsers(t *testing.T, ctx context.Context, queries *Queries) {
	users, err := queries.GetDeletedUsers(ctx)
	if err != nil {
		t.Errorf("GetDeletedUsers failed: %v", err)
	}

	// Should return 1 user (user-4 who is deleted)
	if len(users) != 1 {
		t.Errorf("Expected 1 deleted user, got %d", len(users))
	}

	if len(users) > 0 && users[0].ID != "user-4" {
		t.Errorf("Expected deleted user to be user-4, got %s", users[0].ID)
	}
}

func testGetUsersByStatus(t *testing.T, ctx context.Context, queries *Queries) {
	users, err := queries.GetUsersByStatus(ctx)
	if err != nil {
		t.Errorf("GetUsersByStatus failed: %v", err)
	}

	// Should return users with status 'active', 'pending', or 'verified'
	// That's user-1 (active), user-2 (pending), user-3 (verified), user-4 (active)
	if len(users) != 4 {
		t.Errorf("Expected 4 users with specified statuses, got %d", len(users))
	}

	// Check that user-5 (inactive) is not in the results
	for _, u := range users {
		if u.ID == "user-5" {
			t.Errorf("User-5 with 'inactive' status should not be in results")
		}
	}
}

func testGetUserWithPosts(t *testing.T, ctx context.Context, queries *Queries) {
	posts, err := queries.GetUserWithPosts(ctx)
	if err != nil {
		t.Errorf("GetUserWithPosts failed: %v", err)
	}

	// Should return 4 rows (2 for Alice, 1 for Bob, 1 for Charlie)
	// User-4 is deleted so excluded
	if len(posts) != 4 {
		t.Errorf("Expected 4 posts from active users, got %d", len(posts))
	}

	// Count posts per user
	userPostCount := make(map[string]int)
	for _, p := range posts {
		userPostCount[p.UserID]++
	}

	// Alice should have 2 posts
	if userPostCount["user-1"] != 2 {
		t.Errorf("Expected 2 posts for user-1, got %d", userPostCount["user-1"])
	}

	// Bob should have 1 post
	if userPostCount["user-2"] != 1 {
		t.Errorf("Expected 1 post for user-2, got %d", userPostCount["user-2"])
	}
}

func testGetUsersWithPostCount(t *testing.T, ctx context.Context, queries *Queries) {
	results, err := queries.GetUsersWithPostCount(ctx)
	if err != nil {
		t.Errorf("GetUsersWithPostCount failed: %v", err)
	}

	// Should return all 5 users with their post counts
	if len(results) != 5 {
		t.Errorf("Expected 5 users with post counts, got %d", len(results))
	}

	// Check specific post counts
	postCounts := make(map[string]int64)
	for _, r := range results {
		postCounts[r.ID] = r.PostCount
	}

	expectedCounts := map[string]int64{
		"user-1": 2, // Alice has 2 posts
		"user-2": 1, // Bob has 1 post
		"user-3": 1, // Charlie has 1 post
		"user-4": 0, // David has no posts
		"user-5": 0, // Eve has no posts
	}

	for userId, expected := range expectedCounts {
		if actual := postCounts[userId]; actual != expected {
			t.Errorf("User %s: expected %d posts, got %d", userId, expected, actual)
		}
	}
}

func testGetUserNameOrDefault(t *testing.T, ctx context.Context, queries *Queries) {
	// Test with user that has a name
	name, err := queries.GetUserNameOrDefault(ctx, "user-1")
	if err != nil {
		t.Fatalf("GetUserNameOrDefault failed: %v", err)
	}
	if name != "Alice" {
		t.Errorf("Expected 'Alice', got %v", name)
	}

	// Test with user that has a name (David)
	name, err = queries.GetUserNameOrDefault(ctx, "user-4")
	if err != nil {
		t.Fatalf("GetUserNameOrDefault failed: %v", err)
	}
	if name != "David" {
		t.Errorf("Expected 'David', got %v", name)
	}
}

func testGetUserStatusNullIfDeleted(t *testing.T, ctx context.Context, queries *Queries) {
	// Test with user with active status
	status, err := queries.GetUserStatusNullIfDeleted(ctx, "user-1")
	if err != nil {
		t.Fatalf("GetUserStatusNullIfDeleted failed: %v", err)
	}
	if status != "active" {
		t.Errorf("Expected 'active', got %v", status)
	}

	// Test with user with 'inactive' status
	status, err = queries.GetUserStatusNullIfDeleted(ctx, "user-5")
	if err != nil {
		t.Fatalf("GetUserStatusNullIfDeleted failed: %v", err)
	}
	if status != "inactive" {
		t.Errorf("Expected 'inactive', got %v", status)
	}
}

func testGetFirstNonNullValue(t *testing.T, ctx context.Context, queries *Queries) {
	// Test with user that has name
	value, err := queries.GetFirstNonNullValue(ctx, "user-1")
	if err != nil {
		t.Fatalf("GetFirstNonNullValue failed: %v", err)
	}
	if value != "Alice" {
		t.Errorf("Expected 'Alice' (first non-null), got %v", value)
	}

	// Test with user that has both name and status
	value, err = queries.GetFirstNonNullValue(ctx, "user-4")
	if err != nil {
		t.Fatalf("GetFirstNonNullValue failed: %v", err)
	}
	if value != "David" {
		t.Errorf("Expected 'David' (first non-null), got %v", value)
	}
}