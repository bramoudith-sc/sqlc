# Spanner Features End-to-End Test

This directory contains end-to-end tests for sqlc's Spanner engine, testing the generated code with the actual Spanner emulator.

## Features Tested

- **CASE WHEN expressions** - Conditional logic in queries
- **CAST operations** - Type conversions
- **COALESCE function** - Handling NULL values
- **IS NULL/IS NOT NULL** - NULL checking
- **IN operator** - Value list and subquery conditions
- **JOIN operations** - INNER, LEFT, RIGHT, FULL joins
- **GROUP BY with aggregations** - COUNT and other aggregate functions

## Prerequisites

1. Docker (for running Spanner emulator)
2. Go 1.21 or later
3. sqlc binary built from source

## Running Tests

### Quick Start

```bash
# Run complete test suite
make all
```

This will:
1. Start the Spanner emulator in Docker
2. Generate Go code using sqlc
3. Run tests against the emulator
4. Stop the emulator

### Manual Steps

```bash
# 1. Start Spanner emulator
make emulator-start

# 2. Generate code with sqlc
make generate

# 3. Run tests
make test

# 4. Stop emulator when done
make emulator-stop
```

### Running Tests Directly

If you have the Spanner emulator running elsewhere:

```bash
cd go
SPANNER_EMULATOR_HOST=localhost:9010 go test -tags=emulator -v
```

## Test Structure

- `schema.sql` - Database schema definition
- `query.sql` - SQL queries to be tested
- `sqlc.yaml` - sqlc configuration
- `go/` - Generated Go code and tests
  - `*.sql.go` - Generated query functions
  - `emulator_test.go` - Tests for generated code

## Troubleshooting

### Emulator Connection Issues

Ensure the emulator is running:
```bash
docker ps | grep spanner-emulator
```

Check emulator logs:
```bash
docker logs spanner-emulator
```

### Test Failures

1. Check that the schema matches between `schema.sql` and the test setup
2. Verify that test data setup is correct
3. Review generated code in `go/*.sql.go` files

## Adding New Tests

1. Add new queries to `query.sql`
2. Run `make generate` to regenerate code
3. Add test functions in `go/emulator_test.go`
4. Run `make test` to verify