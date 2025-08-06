//go:build emulator
// +build emulator

package spanner

import (
	"context"
	"fmt"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	instance "cloud.google.com/go/spanner/admin/instance/apiv1"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	testProjectID  = "test-project"
	testInstanceID = "test-instance"
	testDatabaseID = "test-db"
)

// TestWithEmulator runs tests against the Spanner emulator
// Run with: SPANNER_EMULATOR_HOST=localhost:9010 go test -tags=emulator ./internal/engine/spanner/
func TestWithEmulator(t *testing.T) {
	if os.Getenv("SPANNER_EMULATOR_HOST") == "" {
		t.Skip("SPANNER_EMULATOR_HOST not set, skipping emulator tests")
	}

	ctx := context.Background()
	
	// Create clients for emulator
	conn, err := grpc.Dial(os.Getenv("SPANNER_EMULATOR_HOST"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("Failed to connect to emulator: %v", err)
	}
	defer conn.Close()

	// Create instance admin client
	instanceAdmin, err := instance.NewInstanceAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create instance admin client: %v", err)
	}
	defer instanceAdmin.Close()

	// Create database admin client
	databaseAdmin, err := database.NewDatabaseAdminClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create database admin client: %v", err)
	}
	defer databaseAdmin.Close()

	// Create test instance
	instancePath := fmt.Sprintf("projects/%s/instances/%s", testProjectID, testInstanceID)
	_, err = instanceAdmin.CreateInstance(ctx, &instancepb.CreateInstanceRequest{
		Parent:     fmt.Sprintf("projects/%s", testProjectID),
		InstanceId: testInstanceID,
		Instance: &instancepb.Instance{
			Name:        instancePath,
			DisplayName: "Test Instance",
			NodeCount:   1,
		},
	})
	if err != nil {
		t.Logf("Instance might already exist: %v", err)
	}

	// Create test database with schema
	dbPath := fmt.Sprintf("%s/databases/%s", instancePath, testDatabaseID)
	op, err := databaseAdmin.CreateDatabase(ctx, &databasepb.CreateDatabaseRequest{
		Parent:          instancePath,
		CreateStatement: fmt.Sprintf("CREATE DATABASE `%s`", testDatabaseID),
		ExtraStatements: []string{
			`CREATE TABLE users (
				id INT64 NOT NULL,
				name STRING(100),
				email STRING(100),
				created_at TIMESTAMP,
				updated_at TIMESTAMP,
				metadata JSON,
				interval_col INTERVAL,
			) PRIMARY KEY (id)`,
			`CREATE TABLE posts (
				id INT64 NOT NULL,
				user_id INT64 NOT NULL,
				title STRING(200),
				content STRING(MAX),
				published BOOL,
				published_at TIMESTAMP,
				tags ARRAY<STRING(50)>,
				CONSTRAINT FK_UserPosts FOREIGN KEY (user_id) REFERENCES users (id),
			) PRIMARY KEY (id)`,
		},
	})
	if err != nil {
		t.Logf("Database might already exist: %v", err)
	} else {
		// Wait for database creation
		_, err = op.Wait(ctx)
		if err != nil {
			t.Fatalf("Failed to create database: %v", err)
		}
	}

	// Create Spanner client
	client, err := spanner.NewClient(ctx, dbPath, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatalf("Failed to create spanner client: %v", err)
	}
	defer client.Close()

	// Run test queries
	t.Run("TestBasicQueries", func(t *testing.T) {
		testBasicQueries(t, ctx, client)
	})

	t.Run("TestIntervalType", func(t *testing.T) {
		testIntervalType(t, ctx, client)
	})

	t.Run("TestJSONType", func(t *testing.T) {
		testJSONType(t, ctx, client)
	})

	t.Run("TestArrayType", func(t *testing.T) {
		testArrayType(t, ctx, client)
	})

	t.Run("TestSafeFunctions", func(t *testing.T) {
		testSafeFunctions(t, ctx, client)
	})
}

func testBasicQueries(t *testing.T, ctx context.Context, client *spanner.Client) {
	// Test basic INSERT and SELECT
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `INSERT INTO users (id, name, email) VALUES (@id, @name, @email)`,
			Params: map[string]interface{}{
				"id":    1,
				"name":  "Test User",
				"email": "test@example.com",
			},
		}
		_, err := txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		t.Errorf("Failed to insert user: %v", err)
	}

	// Test SELECT
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT id, name, email FROM users WHERE id = @id`,
		Params: map[string]interface{}{
			"id": 1,
		},
	})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		t.Errorf("Failed to query user: %v", err)
	}

	var id int64
	var name, email string
	if err := row.Columns(&id, &name, &email); err != nil {
		t.Errorf("Failed to scan row: %v", err)
	}

	if id != 1 || name != "Test User" || email != "test@example.com" {
		t.Errorf("Unexpected values: id=%d, name=%s, email=%s", id, name, email)
	}
}

func testIntervalType(t *testing.T, ctx context.Context, client *spanner.Client) {
	// Test INTERVAL type operations
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT 
			CURRENT_DATE() as today,
			DATE_ADD(CURRENT_DATE(), INTERVAL 1 DAY) as tomorrow,
			DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) as yesterday`,
	})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		t.Errorf("Failed to query with INTERVAL: %v", err)
	}

	var today, tomorrow, yesterday spanner.NullDate
	if err := row.Columns(&today, &tomorrow, &yesterday); err != nil {
		t.Errorf("Failed to scan dates: %v", err)
	}

	t.Logf("Date operations: today=%v, tomorrow=%v, yesterday=%v", today, tomorrow, yesterday)
}

func testJSONType(t *testing.T, ctx context.Context, client *spanner.Client) {
	// Test JSON type operations
	jsonData := `{"key": "value", "number": 42}`
	
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `UPDATE users SET metadata = @metadata WHERE id = @id`,
			Params: map[string]interface{}{
				"id":       1,
				"metadata": spanner.NullJSON{Value: []byte(jsonData), Valid: true},
			},
		}
		_, err := txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		t.Errorf("Failed to update JSON: %v", err)
	}

	// Query JSON
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT JSON_VALUE(metadata, '$.key') as json_key FROM users WHERE id = @id`,
		Params: map[string]interface{}{
			"id": 1,
		},
	})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		t.Errorf("Failed to query JSON: %v", err)
	}

	var jsonKey spanner.NullString
	if err := row.Columns(&jsonKey); err != nil {
		t.Errorf("Failed to scan JSON value: %v", err)
	}

	if !jsonKey.Valid || jsonKey.StringVal != "value" {
		t.Errorf("Unexpected JSON value: %v", jsonKey)
	}
}

func testArrayType(t *testing.T, ctx context.Context, client *spanner.Client) {
	// Test ARRAY type operations
	tags := []string{"golang", "spanner", "database"}
	
	_, err := client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		stmt := spanner.Statement{
			SQL: `INSERT INTO posts (id, user_id, title, tags) VALUES (@id, @user_id, @title, @tags)`,
			Params: map[string]interface{}{
				"id":      1,
				"user_id": 1,
				"title":   "Test Post",
				"tags":    tags,
			},
		}
		_, err := txn.Update(ctx, stmt)
		return err
	})
	if err != nil {
		t.Errorf("Failed to insert post with array: %v", err)
	}

	// Query with ARRAY functions
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT ARRAY_LENGTH(tags) as tag_count FROM posts WHERE id = @id`,
		Params: map[string]interface{}{
			"id": 1,
		},
	})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		t.Errorf("Failed to query array length: %v", err)
	}

	var tagCount spanner.NullInt64
	if err := row.Columns(&tagCount); err != nil {
		t.Errorf("Failed to scan array length: %v", err)
	}

	if !tagCount.Valid || tagCount.Int64 != 3 {
		t.Errorf("Unexpected array length: %v", tagCount)
	}
}

func testSafeFunctions(t *testing.T, ctx context.Context, client *spanner.Client) {
	// Test SAFE functions
	iter := client.Single().Query(ctx, spanner.Statement{
		SQL: `SELECT 
			SAFE_DIVIDE(10, 2) as normal_divide,
			SAFE_DIVIDE(10, 0) as divide_by_zero,
			SAFE.SUBSTR("hello", 10, 5) as safe_substr`,
	})
	defer iter.Stop()

	row, err := iter.Next()
	if err != nil {
		t.Errorf("Failed to query SAFE functions: %v", err)
	}

	var normalDivide, divideByZero spanner.NullFloat64
	var safeSubstr spanner.NullString
	if err := row.Columns(&normalDivide, &divideByZero, &safeSubstr); err != nil {
		t.Errorf("Failed to scan SAFE function results: %v", err)
	}

	if !normalDivide.Valid || normalDivide.Float64 != 5.0 {
		t.Errorf("Unexpected normal divide result: %v", normalDivide)
	}

	if divideByZero.Valid {
		t.Errorf("Expected NULL for divide by zero, got: %v", divideByZero)
	}

	if safeSubstr.Valid {
		t.Errorf("Expected NULL for out of range substr, got: %v", safeSubstr)
	}
}