package spanner

import (
	"testing"
)

func TestIsReservedKeyword(t *testing.T) {
	p := &Parser{}

	tests := []struct {
		input    string
		expected bool
	}{
		// True reserved keywords (cannot be used as identifiers)
		{"SELECT", true},
		{"select", true},
		{"FROM", true},
		{"WHERE", true},
		{"CREATE", true},
		{"JOIN", true},
		{"LEFT", true},
		{"RIGHT", true},
		{"INNER", true},
		{"OUTER", true},
		{"AS", true},
		{"AND", true},
		{"OR", true},
		{"NOT", true},
		{"NULL", true},
		{"TRUE", true},
		{"FALSE", true},
		{"CASE", true},
		{"WHEN", true},
		{"THEN", true},
		{"ELSE", true},
		{"END", true},
		{"GROUP", true},
		{"BY", true},
		{"ORDER", true},
		{"HAVING", true},
		{"LIMIT", true},
		{"UNION", true},
		{"ALL", true},
		{"DISTINCT", true},
		{"BETWEEN", true},
		{"IN", true},
		{"EXISTS", true},
		{"LIKE", true},
		{"IS", true},
		{"CAST", true},
		{"EXTRACT", true},
		{"INTERVAL", true},
		{"ARRAY", true},
		{"STRUCT", true},
		{"WITH", true},
		{"WINDOW", true},
		{"PARTITION", true},
		{"OVER", true},
		{"ROWS", true},
		{"RANGE", true},
		{"UNBOUNDED", true},
		{"PRECEDING", true},
		{"FOLLOWING", true},
		{"CURRENT", true},
		{"EXCLUDE", true},
		{"GROUPS", true},
		{"NO", true},
		{"RECURSIVE", true},
		{"CROSS", true},
		{"FULL", true},
		{"NATURAL", true},
		{"USING", true},
		{"ON", true},
		{"IF", true},
		{"DEFAULT", true},
		{"SET", true},
		{"COLLATE", true},
		{"ASC", true},
		{"DESC", true},
		{"NULLS", true},
		{"ESCAPE", true},
		{"INTERSECT", true},
		{"EXCEPT", true},
		{"FOR", true},
		{"TABLESAMPLE", true},
		{"CUBE", true},
		{"ROLLUP", true},
		{"GROUPING", true},
		{"LATERAL", true},
		{"UNNEST", true},
		{"EXCLUDE", true},
		{"RESPECT", true},
		{"IGNORE", true},
		{"FETCH", true},
		{"OF", true},
		{"TO", true},
		{"AT", true},
		{"CONTAINS", true},
		{"MERGE", true},
		{"HASH", true},
		{"WITHIN", true},
		{"LOOKUP", true},
		{"PROTO", true},
		{"ENUM", true},
		{"DEFINE", true},
		{"ASSERT_ROWS_MODIFIED", true},

		// Context-dependent keywords (can be used as identifiers)
		// These are recognized as keywords only in specific contexts
		{"INSERT", false},
		{"UPDATE", false},
		{"DELETE", false},
		{"DROP", false},
		{"ALTER", false},
		{"TABLE", false},
		{"INDEX", false},
		{"VIEW", false},
		{"FUNCTION", false},
		{"PROCEDURE", false},
		{"TRIGGER", false},
		{"MATCHED", false},
		{"OTHERS", false},
		{"TIES", false},
		{"FIRST", false},
		{"LAST", false},
		{"ORDINALITY", false},
		{"REPEATABLE", false},
		{"SETS", false},
		{"QUALIFY", false},
		{"VALUE", false},
		{"VALUES", false},
		{"OFFSET", false}, // OFFSET is context-dependent in Spanner

		// Non-reserved words (should be identifiers)
		{"mycolumn", false},
		{"user_id", false},
		{"customer_name", false},
		{"order_date", false},
		{"_underscore", false},

		// Multi-word strings (not single keywords)
		{"SELECT FROM", false},
		{"ORDER BY", false},

		// Empty or invalid
		{"", false},
		{"123", false},
		{"@param", false},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := p.IsReservedKeyword(tt.input)
			if result != tt.expected {
				t.Errorf("IsReservedKeyword(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}
