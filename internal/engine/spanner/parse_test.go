package spanner

import (
	"strings"
	"testing"

	"github.com/sqlc-dev/sqlc/internal/sql/ast"
)

func TestParse(t *testing.T) {
	p := NewParser()

	testCases := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{
			name:    "CREATE TABLE",
			input:   "CREATE TABLE users (id INT64 NOT NULL, name STRING(100)) PRIMARY KEY (id);",
			wantErr: false,
		},
		{
			name:    "SELECT simple",
			input:   "SELECT * FROM users;",
			wantErr: false,
		},
		{
			name:    "SELECT with WHERE",
			input:   "SELECT id, name FROM users WHERE id = 1;",
			wantErr: false,
		},
		{
			name:    "INSERT",
			input:   "INSERT INTO users (id, name) VALUES (1, 'Alice');",
			wantErr: false,
		},
		{
			name:    "UPDATE",
			input:   "UPDATE users SET name = 'Bob' WHERE id = 1;",
			wantErr: false,
		},
		{
			name:    "DELETE",
			input:   "DELETE FROM users WHERE id = 1;",
			wantErr: false,
		},
		{
			name:    "SAFE prefix function",
			input:   "SELECT SAFE.SUBSTR(name, 0, -2) FROM users;",
			wantErr: false,
		},
		{
			name:    "Multiple statements",
			input:   "SELECT * FROM users; SELECT * FROM products;",
			wantErr: false,
		},
		{
			name:    "Syntax error",
			input:   "SELECT FROM users;",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(tc.input)
			stmts, err := p.Parse(r)

			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(stmts) == 0 {
				t.Fatalf("expected at least one statement, got none")
			}

			// Check that statements are not TODO nodes
			for i, stmt := range stmts {
				if stmt.Raw == nil {
					t.Errorf("statement %d: Raw is nil", i)
					continue
				}
				if _, ok := stmt.Raw.Stmt.(*ast.TODO); ok {
					t.Errorf("statement %d: got TODO node, expected parsed statement", i)
				}
			}
		})
	}
}

func TestCommentSyntax(t *testing.T) {
	p := NewParser()
	syntax := p.CommentSyntax()

	if !syntax.Dash {
		t.Error("expected Dash comment syntax to be supported")
	}
	if !syntax.SlashStar {
		t.Error("expected SlashStar comment syntax to be supported")
	}
	if !syntax.Hash {
		t.Error("expected Hash comment syntax to be supported")
	}
}

func TestParseWithParams(t *testing.T) {
	p := NewParser()

	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "Positional parameter",
			input: "SELECT * FROM users WHERE id = @p1;",
		},
		{
			name:  "Named parameter",
			input: "SELECT * FROM users WHERE name = @name;",
		},
		{
			name:  "Multiple parameters",
			input: "INSERT INTO users (id, name, email) VALUES (@id, @name, @email);",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(tc.input)
			stmts, err := p.Parse(r)

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(stmts) != 1 {
				t.Fatalf("expected 1 statement, got %d", len(stmts))
			}

			// Ensure it's not a TODO node
			if _, ok := stmts[0].Raw.Stmt.(*ast.TODO); ok {
				t.Error("got TODO node for parameterized query")
			}
		})
	}
}

func TestConvertError(t *testing.T) {
	p := NewParser()

	// Test various syntax errors
	testCases := []struct {
		name  string
		input string
	}{
		{
			name:  "Missing expression after SELECT",
			input: "SELECT FROM users;",
		},
		{
			name:  "Invalid keyword",
			input: "SELEKT * FROM users;",
		},
		{
			name:  "Unclosed string",
			input: "SELECT * FROM users WHERE name = 'unclosed;",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			r := strings.NewReader(tc.input)
			_, err := p.Parse(r)

			if err == nil {
				t.Fatal("expected error, got nil")
			}

			// Check that the error contains line/column info
			errStr := err.Error()
			if !strings.Contains(errStr, "syntax error") && !strings.Contains(errStr, "parse error") {
				t.Errorf("expected syntax/parse error, got: %s", errStr)
			}
		})
	}
}
