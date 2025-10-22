package spanner

import (
	"testing"

	"github.com/cloudspannerecosystem/memefish"
)

func TestExtractParameters(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected []string
	}{
		{
			name:     "SELECT with one parameter",
			sql:      "SELECT * FROM users WHERE id = @user_id",
			expected: []string{"user_id"},
		},
		{
			name:     "INSERT with multiple parameters",
			sql:      "INSERT INTO users (id, name) VALUES (@id, @name)",
			expected: []string{"id", "name"},
		},
		{
			name:     "UPDATE with repeated parameters",
			sql:      "UPDATE users SET name = @name WHERE id = @id AND name != @name",
			expected: []string{"name", "id", "name"},
		},
		{
			name:     "No parameters",
			sql:      "SELECT * FROM users",
			expected: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			node, err := memefish.ParseStatement("<test>", tt.sql)
			if err != nil {
				t.Fatalf("Failed to parse SQL: %v", err)
			}

			// Extract parameters
			params := ExtractParameters(node)

			// Check count
			if len(params) != len(tt.expected) {
				t.Errorf("Expected %d parameters, got %d", len(tt.expected), len(params))
			}

			// Check names
			for i, param := range params {
				if i < len(tt.expected) && param.Name != tt.expected[i] {
					t.Errorf("Parameter %d: expected name %q, got %q", i, tt.expected[i], param.Name)
				}
			}
		})
	}
}

func TestExtractParametersUnique(t *testing.T) {
	sql := "UPDATE users SET name = @name WHERE id = @id AND name != @name"

	node, err := memefish.ParseStatement("<test>", sql)
	if err != nil {
		t.Fatalf("Failed to parse SQL: %v", err)
	}

	params := ExtractParameters(node)

	// Create a map to get unique parameters
	unique := make(map[string]bool)
	for _, p := range params {
		unique[p.Name] = true
	}

	if len(unique) != 2 {
		t.Errorf("Expected 2 unique parameters, got %d", len(unique))
	}

	if !unique["name"] || !unique["id"] {
		t.Errorf("Expected parameters 'name' and 'id', got %v", unique)
	}
}
