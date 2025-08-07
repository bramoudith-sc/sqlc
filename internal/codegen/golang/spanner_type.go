package golang

import (
	"strings"

	"github.com/sqlc-dev/sqlc/internal/codegen/golang/opts"
	"github.com/sqlc-dev/sqlc/internal/codegen/sdk"
	"github.com/sqlc-dev/sqlc/internal/plugin"
)

// spannerType maps Cloud Spanner SQL types to Go types
// Following the official Cloud Spanner Go client library conventions:
// https://pkg.go.dev/cloud.google.com/go/spanner#hdr-Updating_a_row
func spannerType(req *plugin.GenerateRequest, options *opts.Options, col *plugin.Column) string {
	dt := strings.ToLower(sdk.DataType(col.Type))
	notNull := col.NotNull || col.IsArray
	emitPointersForNull := options.EmitPointersForNullTypes

	// Handle array types
	if col.IsArray {
		baseType := spannerType(req, options, &plugin.Column{
			Type:    col.Type,
			NotNull: true,
		})
		return "[]" + baseType
	}

	// Handle sized types (e.g., STRING(100), STRING(MAX))
	if idx := strings.Index(dt, "("); idx > 0 {
		dt = dt[:idx]
	}

	switch dt {
	case "int", "int64":
		// INT64 - following Spanner Go client conventions
		// Spanner client: int, int64, *int64, NullInt64
		if notNull {
			return "int64"
		}
		if emitPointersForNull {
			return "*int64"
		}
		return "sql.NullInt64" // Using database/sql for compatibility

	case "float32":
		// FLOAT32 - Spanner supports but rarely used
		if notNull {
			return "float32"
		}
		if emitPointersForNull {
			return "*float32"
		}
		return "sql.NullFloat64" // No NullFloat32 in database/sql

	case "float", "float64":
		// FLOAT64 - following Spanner Go client conventions
		// Spanner client: float64, *float64, NullFloat64
		if notNull {
			return "float64"
		}
		if emitPointersForNull {
			return "*float64"
		}
		return "sql.NullFloat64"

	case "numeric":
		// NUMERIC - uses big.Rat in Spanner Go client
		// For database/sql compatibility, we use string to preserve precision
		if notNull {
			return "string" // Preserve precision as string
		}
		if emitPointersForNull {
			return "*string"
		}
		return "sql.NullString"

	case "bool", "boolean":
		// BOOL - following Spanner Go client conventions
		// Spanner client: bool, *bool, NullBool
		if notNull {
			return "bool"
		}
		if emitPointersForNull {
			return "*bool"
		}
		return "sql.NullBool"

	case "string", "text":
		// STRING - following Spanner Go client conventions
		// Spanner client: string, *string, NullString
		if notNull {
			return "string"
		}
		if emitPointersForNull {
			return "*string"
		}
		return "sql.NullString"

	case "bytes":
		// BYTES - []byte in Spanner Go client (always non-null as a slice)
		return "[]byte"

	case "date":
		// DATE - uses civil.Date in Spanner Go client
		// For database/sql compatibility, use time.Time
		if notNull {
			return "time.Time"
		}
		if emitPointersForNull {
			return "*time.Time"
		}
		return "sql.NullTime"

	case "timestamp":
		// TIMESTAMP - uses time.Time in Spanner Go client
		// Spanner client: time.Time, *time.Time, NullTime
		if notNull {
			return "time.Time"
		}
		if emitPointersForNull {
			return "*time.Time"
		}
		return "sql.NullTime"

	case "json", "jsonb":
		// JSON - Spanner JSON type
		// Using json.RawMessage for database/sql compatibility
		return "json.RawMessage"

	case "interval":
		// INTERVAL - Spanner supports INTERVAL type
		// https://cloud.google.com/spanner/docs/reference/standard-sql/data-types#interval_type
		// https://pkg.go.dev/cloud.google.com/go/spanner#Interval
		if notNull {
			// Use spanner.Interval for non-null INTERVAL type
			// Note: spanner.Interval may not implement sql.Scanner
			return "spanner.Interval"
		}
		// For nullable interval, use spanner.NullInterval
		return "spanner.NullInterval"

	case "any":
		return "interface{}"

	default:
		// Default to interface{} for unknown types
		return "interface{}"
	}
}
