# Cloud Spanner Engine for sqlc

This package provides Cloud Spanner (GoogleSQL) support for sqlc using the memefish parser.

## Implementation Overview

The Spanner engine implementation consists of several key components:

- **Parser** (`parse.go`): Handles SQL statement parsing using memefish and metadata extraction
- **AST Converter** (`convert.go`): Converts memefish AST to sqlc's internal AST format
- **Standard Library** (`stdlib.go`): Defines Spanner's built-in functions and types
- **Reserved Words** (`reserved.go`): Handles Spanner SQL reserved keywords

## Key Features

### Complete Support
- All DML operations (SELECT, INSERT, UPDATE, DELETE)
- THEN RETURN clause (Spanner's equivalent of RETURNING)
- Common Table Expressions (WITH clause)
- All JOIN types (INNER, LEFT, RIGHT, FULL, CROSS)
- Comprehensive subquery support (scalar, EXISTS, IN, ARRAY, table)
- Full operator support (arithmetic, comparison, logical, bitwise)
- CASE expressions and conditional logic
- Type casting and conversions
- Aggregate functions
- TABLESAMPLE for random sampling
- Parameter support with @ syntax

### Partial Support
- SELECT AS STRUCT/VALUE (detected but not fully transformed)
- SELECT * EXCEPT/REPLACE (detected but not implemented)
- DotStar (table.*) syntax (parsed but not expanded)
- DDL operations (basic CREATE/DROP TABLE only)

## Architecture Decisions

1. **Parser Choice**: Uses memefish (Cloud Spanner SQL parser) instead of ZetaSQL to avoid CGO dependencies
2. **AST Conversion**: Direct conversion from memefish AST to sqlc's PostgreSQL-based AST
3. **Function Names**: Preserved as-is for case-insensitive catalog lookup
4. **List Initialization**: All walked Lists must be initialized with empty Items arrays

## Testing

Run tests with:
```bash
go test ./internal/engine/spanner/...
```

Test with Spanner emulator:
```bash
# Start emulator
gcloud emulators spanner start

# Set environment
export SPANNER_EMULATOR_HOST=localhost:9010

# Run tests
go test ./internal/engine/spanner/...
```

## Known Limitations

1. **DotStar Expansion**: table.* generates interface{} instead of expanding columns
2. **STRUCT Type Inference**: Limited with column references
3. **DDL Support**: Limited to basic table operations
4. **EXCEPT/REPLACE**: Not implemented for column filtering

See FEATURES.md for detailed feature status.

## Contributing

When adding new features:
1. Check how other engines (PostgreSQL, SQLite) handle similar features
2. Add tests in internal/endtoend/testdata/spanner_features/
3. Update FEATURES.md with implementation status
4. Follow the patterns established in convert.go for AST conversion