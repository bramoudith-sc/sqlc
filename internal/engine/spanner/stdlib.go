package spanner

import (
	"github.com/sqlc-dev/sqlc/internal/sql/ast"
	"github.com/sqlc-dev/sqlc/internal/sql/catalog"
)

func defaultSchema(name string) *catalog.Schema {
	s := &catalog.Schema{Name: name}
	
	s.Funcs = []*catalog.Function{
		// Mathematical Functions
		{
			Name: "ABS",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "ABS",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "CEIL",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "FLOOR",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "ROUND",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "ROUND",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "SQRT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "POW",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "MOD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},

		// String Functions
		{
			Name: "CONCAT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "LENGTH",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "LOWER",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "UPPER",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "SUBSTR",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "SUBSTR",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "TRIM",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "LTRIM",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "RTRIM",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "REPLACE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "SPLIT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "array"},
		},

		// Date and Time Functions
		{
			Name: "CURRENT_DATE",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "date"},
		},
		{
			Name: "CURRENT_TIMESTAMP",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "timestamp"},
		},
		{
			Name: "DATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "timestamp"}},
			},
			ReturnType: &ast.TypeName{Name: "date"},
		},
		{
			Name: "DATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "date"},
		},
		{
			Name: "TIMESTAMP",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "timestamp"},
		},
		{
			Name: "EXTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}}, // date part
				{Type: &ast.TypeName{Name: "date"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "DATE_ADD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "date"}},
				{Type: &ast.TypeName{Name: "interval"}},
			},
			ReturnType: &ast.TypeName{Name: "date"},
		},
		{
			Name: "DATE_SUB",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "date"}},
				{Type: &ast.TypeName{Name: "interval"}},
			},
			ReturnType: &ast.TypeName{Name: "date"},
		},
		{
			Name: "DATE_DIFF",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "date"}},
				{Type: &ast.TypeName{Name: "date"}},
				{Type: &ast.TypeName{Name: "any"}}, // date part
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},

		// Array Functions
		{
			Name: "ARRAY_LENGTH",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "array"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "ARRAY_TO_STRING",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "array"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},

		// Aggregate Functions
		{
			Name: "COUNT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "SUM",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "SUM",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "AVG",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "AVG",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "MIN",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "MAX",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "STRING_AGG",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "STRING_AGG",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "ARRAY_AGG",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "array"},
		},

		// Type Conversion Functions
		{
			Name: "CAST",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "SAFE_CAST",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},

		// Conditional Functions
		{
			Name: "IF",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bool"}},
				{Type: &ast.TypeName{Name: "any"}},
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "IFNULL",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "NULLIF",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "COALESCE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},

		// Spanner-specific Functions
		{
			Name: "PENDING_COMMIT_TIMESTAMP",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "timestamp"},
		},
		{
			Name: "GENERATE_UUID",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "FARM_FINGERPRINT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "SHA1",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bytes"}},
			},
			ReturnType: &ast.TypeName{Name: "bytes"},
		},
		{
			Name: "SHA256",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bytes"}},
			},
			ReturnType: &ast.TypeName{Name: "bytes"},
		},
		{
			Name: "SHA512",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bytes"}},
			},
			ReturnType: &ast.TypeName{Name: "bytes"},
		},

		// JSON Functions
		{
			Name: "JSON_EXTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "json"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "json"},
		},
		{
			Name: "JSON_EXTRACT_SCALAR",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "json"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "JSON_QUERY",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "json"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "json"},
		},
		{
			Name: "JSON_VALUE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "json"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "TO_JSON",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "json"},
		},
		{
			Name: "TO_JSON_STRING",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},

		// Window Functions
		{
			Name: "ROW_NUMBER",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "RANK",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "DENSE_RANK",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "PERCENT_RANK",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "CUME_DIST",
			Args: []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "NTILE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "LAG",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "LEAD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "FIRST_VALUE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "LAST_VALUE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
	}
	return s
}