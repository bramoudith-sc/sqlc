# Cloud Spanner Engine Feature Status

## Fully Implemented Features

### DML Operations
- SELECT statements with all basic clauses
- INSERT with VALUES and SELECT
- UPDATE with SET and WHERE
- DELETE with WHERE
- THEN RETURN clause (Spanner's RETURNING equivalent)

### Query Features
- WITH clause (Common Table Expressions)
- JOINs (INNER, LEFT, RIGHT, FULL)
- Subqueries (scalar, EXISTS, IN, array)
- GROUP BY and HAVING
- ORDER BY
- LIMIT and OFFSET
- UNION/INTERSECT/EXCEPT

### Functions and Operators
- Arithmetic operators (+, -, *, /)
- Comparison operators (=, !=, <, >, <=, >=)
- Logical operators (AND, OR, NOT)
- Bitwise operators (~, |, ^, &, <<, >>)
- String concatenation (||)
- LIKE and NOT LIKE operators
- IN operator with subqueries and value lists
- EXISTS operator
- BETWEEN operator
- NULL handling (IS NULL, IS NOT NULL, COALESCE, IFNULL, NULLIF)
- CASE expressions
- CAST operations
- String functions (via stdlib.go)
- Date/time functions (including EXTRACT)
- Aggregate functions (COUNT, SUM, AVG, MIN, MAX, ARRAY_AGG, STRING_AGG)
- SAFE functions (SAFE.DIVIDE, etc.)

### Type Support
- Basic types (INT64, FLOAT64, STRING, BOOL, BYTES)
- DATE, TIMESTAMP
- NUMERIC, JSON
- ARRAY types
- STRUCT types (typed and untyped)
- INTERVAL literals

### Advanced Features
- Array indexing (array[1], array[OFFSET(n)])
- Struct field access (struct.field)
- Parameter support (@param_name)
- TABLESAMPLE (BERNOULLI and RESERVOIR methods)
- DotStar syntax (table.*)

## Partially Implemented Features

### SELECT Modifiers
- SELECT AS STRUCT - detected but not fully transformed
- SELECT AS VALUE - detected but not validated
- SELECT * EXCEPT - detected but column exclusion not implemented
- SELECT * REPLACE - detected but column replacement not implemented

### UNNEST
- Basic UNNEST in FROM clause - implemented
- WITH OFFSET - TODO

### DDL Operations
- CREATE TABLE - basic implementation
- DROP TABLE - basic implementation
- Missing: indexes, constraints, interleaving, TTL

## Not Yet Implemented

### DDL Operations
- CREATE/DROP INDEX
- ALTER TABLE
- CREATE/DROP VIEW
- CREATE/DROP SEQUENCE

### Spanner-Specific Features
- INTERLEAVE IN PARENT
- ROW DELETION POLICY (TTL)
- Table hints
- Statement hints
- TABLESAMPLE
- ML.* functions
- Table-valued functions (TVFs)

### Type Features
- PROTO types
- GRAPH types

## Testing Coverage

### Comprehensive Test Coverage
- Basic DML operations
- NULL handling
- CASE expressions
- JOINs
- Subqueries
- CTEs
- Aggregate functions
- Type casting
- Array and struct operations

### Areas Needing More Tests
- Complex nested structures
- Edge cases in type inference
- DDL operations
- Error handling scenarios

## Known Limitations

1. **Struct Field Type Inference**: Type inference for struct field access only works with:
   - Typed STRUCT literals
   - Untyped STRUCT with literal values
   - Does NOT work with column references in untyped STRUCTs

2. **INTERVAL Type**: Uses interface{} by default to avoid Spanner package dependency

3. **DDL Support**: Limited to basic CREATE/DROP TABLE

4. **Error Messages**: Some error messages could be more descriptive

## Future Improvements

1. Complete SELECT AS STRUCT/VALUE implementation
2. Add full DDL support
3. Implement UNNEST WITH OFFSET
4. Add support for table hints and statement hints
5. Improve error messages and debugging information
6. Add support for more Spanner-specific features