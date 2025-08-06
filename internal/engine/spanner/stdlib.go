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
			Name: "ABS",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType: &ast.TypeName{Name: "numeric"},
		},
		{
			Name: "CEIL",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "CEILING",
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
		{
			Name: "LOG",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "LOG",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "LOG10",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "EXP",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "SIGN",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "SIGN",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "GREATEST",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},
		{
			Name: "LEAST",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
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
		{
			Name: "STARTS_WITH",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "bool"},
		},
		{
			Name: "ENDS_WITH",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "bool"},
		},
		{
			Name: "STRPOS",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "REVERSE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "FORMAT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "REGEXP_CONTAINS",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "bool"},
		},
		{
			Name: "REGEXP_EXTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType:         &ast.TypeName{Name: "string"},
			ReturnTypeNullable: true,
		},
		{
			Name: "REGEXP_EXTRACT_ALL",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "array"},
		},
		{
			Name: "REGEXP_REPLACE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},

		// Date and Time Functions
		{
			Name:       "CURRENT_DATE",
			Args:       []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "date"},
		},
		{
			Name:       "CURRENT_TIMESTAMP",
			Args:       []*catalog.Argument{},
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
		{
			Name: "TIMESTAMP_ADD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "timestamp"}},
				{Type: &ast.TypeName{Name: "interval"}},
			},
			ReturnType: &ast.TypeName{Name: "timestamp"},
		},
		{
			Name: "TIMESTAMP_SUB",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "timestamp"}},
				{Type: &ast.TypeName{Name: "interval"}},
			},
			ReturnType: &ast.TypeName{Name: "timestamp"},
		},
		// INTERVAL functions
		{
			Name: "EXTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "interval"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "TIMESTAMP_DIFF",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "timestamp"}},
				{Type: &ast.TypeName{Name: "timestamp"}},
				{Type: &ast.TypeName{Name: "any"}}, // date part
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "FORMAT_DATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "date"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "FORMAT_TIMESTAMP",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "timestamp"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "PARSE_DATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "date"},
		},
		{
			Name: "PARSE_TIMESTAMP",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "timestamp"},
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
		{
			Name: "ARRAY_CONCAT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "array"}},
			},
			ReturnType: &ast.TypeName{Name: "array"},
		},
		{
			Name: "ARRAY_REVERSE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "array"}},
			},
			ReturnType: &ast.TypeName{Name: "array"},
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
		{
			Name: "COUNT_IF",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bool"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "STDDEV",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "STDDEV_POP",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "STDDEV_SAMP",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "VARIANCE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "VAR_POP",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name: "VAR_SAMP",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType: &ast.TypeName{Name: "float64"},
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

		// SAFE arithmetic functions - return NULL on overflow/error instead of raising errors
		{
			Name: "SAFE_ADD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "int64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_ADD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType:         &ast.TypeName{Name: "float64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_ADD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_SUBTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "int64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_SUBTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType:         &ast.TypeName{Name: "float64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_SUBTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_MULTIPLY",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "int64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_MULTIPLY",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType:         &ast.TypeName{Name: "float64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_MULTIPLY",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_DIVIDE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType:         &ast.TypeName{Name: "float64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_DIVIDE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "float64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_DIVIDE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_NEGATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "int64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_NEGATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType:         &ast.TypeName{Name: "float64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_NEGATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},

		// Common SAFE. prefix functions
		// Note: SAFE. prefix makes functions return NULL instead of raising errors
		// TODO: Consider implementing dynamic SAFE. prefix handling in catalog resolution
		{
			Name: "SAFE.SUBSTR",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "string"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE.SUBSTR",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "string"},
			ReturnTypeNullable: true,
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
			Name:       "PENDING_COMMIT_TIMESTAMP",
			Args:       []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "timestamp"},
		},
		{
			Name:       "GENERATE_UUID",
			Args:       []*catalog.Argument{},
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
		{
			Name: "MD5",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bytes"}},
			},
			ReturnType: &ast.TypeName{Name: "bytes"},
		},
		{
			Name: "TO_BASE64",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bytes"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "FROM_BASE64",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "bytes"},
		},
		{
			Name: "TO_HEX",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bytes"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "FROM_HEX",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
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
		{
			Name: "PARSE_JSON",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "json"},
		},
		{
			Name: "JSON_EXTRACT_ARRAY",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "json"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "array"},
		},
		{
			Name: "JSON_EXTRACT_STRING_ARRAY",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "json"}},
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "array"},
		},

		// Window Functions
		{
			Name:       "ROW_NUMBER",
			Args:       []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name:       "RANK",
			Args:       []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name:       "DENSE_RANK",
			Args:       []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name:       "PERCENT_RANK",
			Args:       []*catalog.Argument{},
			ReturnType: &ast.TypeName{Name: "float64"},
		},
		{
			Name:       "CUME_DIST",
			Args:       []*catalog.Argument{},
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
		{
			Name: "NTH_VALUE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "any"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "any"},
		},

		// Bit Functions
		{
			Name: "BIT_AND",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "BIT_OR",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "BIT_XOR",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "BIT_NOT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "BIT_COUNT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},

		// Network Functions
		// SAFE arithmetic functions (handle overflow/errors)
		{
			Name: "SAFE_ADD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "int64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_ADD",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_SUBTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "int64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_SUBTRACT",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_MULTIPLY",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "int64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_MULTIPLY",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_DIVIDE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "float64"}},
				{Type: &ast.TypeName{Name: "float64"}},
			},
			ReturnType:         &ast.TypeName{Name: "float64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_DIVIDE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_NEGATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType:         &ast.TypeName{Name: "int64"},
			ReturnTypeNullable: true,
		},
		{
			Name: "SAFE_NEGATE",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "numeric"}},
			},
			ReturnType:         &ast.TypeName{Name: "numeric"},
			ReturnTypeNullable: true,
		},
		
		{
			Name: "NET.IPV4_TO_INT64",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "int64"},
		},
		{
			Name: "NET.INT64_TO_IPV4",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "int64"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "NET.IP_FROM_STRING",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "bytes"},
		},
		{
			Name: "NET.IP_TO_STRING",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "bytes"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "NET.HOST",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "NET.PUBLIC_SUFFIX",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
		{
			Name: "NET.REG_DOMAIN",
			Args: []*catalog.Argument{
				{Type: &ast.TypeName{Name: "string"}},
			},
			ReturnType: &ast.TypeName{Name: "string"},
		},
	}

	// Automatically generate SAFE. versions for most functions
	// SAFE. prefix makes functions return NULL instead of raising errors
	baseFuncs := make([]*catalog.Function, len(s.Funcs))
	copy(baseFuncs, s.Funcs)
	
	for _, fn := range baseFuncs {
		// Skip functions that already have SAFE in the name or are SAFE_* arithmetic functions
		if len(fn.Name) >= 4 && fn.Name[:4] == "SAFE" {
			continue
		}
		// Skip aggregate functions (they don't have SAFE. versions)
		if isAggregateFunction(fn.Name) {
			continue
		}
		
		// Create SAFE. version (works for both regular and namespaced functions)
		safeFn := &catalog.Function{
			Name:               "SAFE." + fn.Name,
			Args:               fn.Args,
			ReturnType:         fn.ReturnType,
			ReturnTypeNullable: true, // SAFE functions always return nullable types
		}
		s.Funcs = append(s.Funcs, safeFn)
	}

	return s
}

func isAggregateFunction(name string) bool {
	aggregates := map[string]bool{
		"AVG":         true,
		"COUNT":       true,
		"MAX":         true,
		"MIN":         true,
		"SUM":         true,
		"STRING_AGG":  true,
		"ARRAY_AGG":   true,
		"BIT_AND":     true,
		"BIT_OR":      true,
		"BIT_XOR":     true,
		"LOGICAL_AND": true,
		"LOGICAL_OR":  true,
		"STDDEV":      true,
		"STDDEV_POP":  true,
		"STDDEV_SAMP": true,
		"VARIANCE":    true,
		"VAR_POP":     true,
		"VAR_SAMP":    true,
	}
	return aggregates[name]
}
