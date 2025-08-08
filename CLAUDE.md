# Cloud Spanner Engine Development Guide for sqlc

## Project Context
This is the sqlc project with a Cloud Spanner engine implementation using the memefish parser. The Spanner engine is located in `internal/engine/spanner/`.

## Key Technical Decisions

### Parser Choice
- Using `github.com/cloudspannerecosystem/memefish` - a non-CGO Cloud Spanner SQL parser
- Chosen over ZetaSQL to avoid CGO dependencies and simplify distribution

### Architecture Principles
1. **Simplicity over consistency** - Use the simplest, most idiomatic solution even if it differs from other engines
2. **Leverage modern Go features** - Use Go 1.23+ features like `iter.Seq` when they simplify code
3. **Minimize maintenance burden** - Prefer built-in functions over hardcoded lists (e.g., `token.IsKeyword()`)

## Code Style Guidelines

### General Guidelines
- **No emojis**: Do not use emojis in code, comments, or documentation
- **Git safety**: Always use `git status` before `git commit` to review changes. Never use `git add -A` followed by immediate commit

### AST Traversal
- Use `ast.Preorder` for simple traversals (cleaner with range-over-func)
- Use `ast.Inspect` only when you need to skip subtrees with `return false`
- Avoid the Visitor pattern unless absolutely necessary

### Case Sensitivity
- **Identifiers**: Convert to lowercase in `convert.go` via `identifier()` function
- **Function names**: Preserve original case, handle case-insensitively in catalog lookup
- Document case handling decisions with comments

### Error Handling
- Use memefish's `MultiError` type correctly (it's a slice, not a struct)
- Preserve position information for better error messages
- Handle both single errors and multi-errors appropriately

## Critical Implementation Details

### List Initialization in AST
**ALWAYS** initialize `sqlcast.List` fields with empty `Items` arrays, not nil:
```go
// CORRECT - prevents nil panics in compiler Walk
TargetList: &sqlcast.List{Items: []sqlcast.Node{}}

// WRONG - will cause panics
TargetList: nil
```

### Star (*) Node Handling
Spanner's `Star` nodes must be wrapped in `ColumnRef` to match PostgreSQL's structure:
```go
// Pattern: ResTarget -> ColumnRef -> A_Star
&sqlcast.ResTarget{
    Val: &sqlcast.ColumnRef{
        Fields: &sqlcast.List{
            Items: []sqlcast.Node{&sqlcast.A_Star{}},
        },
    },
}
```

### THEN RETURN Conversion
Convert Spanner's `THEN RETURN` to PostgreSQL's `RETURNING` maintaining the same wrapping patterns as SELECT.

## Testing Guidelines

### Always Test
- Run `go test ./internal/engine/spanner/...` after any changes
- Test with the emulator using `examples/spanner_test/`
- Verify generated code with `sqlc generate -f examples/spanner_test/sqlc.yaml`

### Emulator Testing
```bash
# Start emulator
gcloud emulators spanner start

# Set environment
export SPANNER_EMULATOR_HOST=localhost:9010

# Run tests with autoConfigEmulator=true in connection string
```

## Common Pitfalls to Avoid

1. **Don't maintain hardcoded keyword lists** - Use `token.IsKeyword()`
2. **Don't use `SplitRawStatements` for metadata comments** - Use Lexer directly
3. **Don't assume function names are simple strings** - They can be paths (e.g., `NET.IPV4_TO_INT64`)
4. **Don't lowercase function names** - Preserve case for proper catalog matching
5. **Don't leave Lists nil** - Initialize with empty Items arrays

## Useful memefish Features

### For Statement Splitting
```go
lexer := &memefish.Lexer{
    File: &token.File{
        FilePath: filename,
        Buffer:   content,
    },
}
// Process tokens and track comments naturally via tok.Comments
```

### For Parameter Extraction
```go
for n := range ast.Preorder(node) {
    if param, ok := n.(*ast.Param); ok {
        // Process parameter
    }
}
```

### For Reserved Keyword Checking
```go
func (p *Parser) IsReservedKeyword(s string) bool {
    return token.IsKeyword(s)
}
```

### For AST Debugging and Analysis
Use the memefish parse tool to examine AST structure directly:
```bash
# Parse a complete SQL statement
go run github.com/cloudspannerecosystem/memefish/tools/parse@latest 'SELECT * FROM users'

# Parse an expression with -mode expr
go run github.com/cloudspannerecosystem/memefish/tools/parse@latest -mode expr 'STRUCT(1 as id, "John" as name).name'

# Parse DDL statements with -mode ddl  
go run github.com/cloudspannerecosystem/memefish/tools/parse@latest -mode ddl 'CREATE TABLE users (id INT64) PRIMARY KEY (id)'
```

This tool displays:
- Colored AST structure with node types
- Field names and values
- Position information
- Reconstructed SQL from the AST

Very useful for understanding how memefish parses specific SQL constructs and what AST nodes to handle in convert.go.

## Development Workflow

1. **Make changes** in `internal/engine/spanner/`
2. **Run tests** with `go test ./internal/engine/spanner/...`
3. **Test generation** with `sqlc generate -f examples/spanner_test/sqlc.yaml`
4. **Verify output** in `examples/spanner_test/db/`
5. **Format code** with `make fmt` before committing
6. **Update docs** in `docs/memefish-feedback.md` if discovering new patterns

## Resources

- [memefish documentation](https://pkg.go.dev/github.com/cloudspannerecosystem/memefish)
- [Cloud Spanner SQL reference](https://cloud.google.com/spanner/docs/reference/standard-sql/data-definition-language)
- [sqlc architecture](https://docs.sqlc.dev/en/latest/guides/architecture.html)

## Current Status

### Implemented
- ✅ Basic DML (SELECT, INSERT, UPDATE, DELETE)
- ✅ THEN RETURN clause support
- ✅ Parameter extraction with @param syntax
- ✅ Reserved keyword checking via token.IsKeyword
- ✅ Lexer-based statement splitting
- ✅ Case-insensitive identifier/function handling

### TODO
- [ ] Full DDL support (CREATE INDEX, ALTER TABLE, etc.)
- [ ] INTERLEAVE and TTL support
- [ ] Analyzer with INFORMATION_SCHEMA integration
- [ ] Support for cloud.google.com/go/spanner client
- [ ] Array and struct type handling
- [ ] Transaction hints and statement hints

## Implementation Patterns

### When in Doubt, Reference Other Engines
When implementing new features or unsure about the correct approach:
1. Check how PostgreSQL engine handles it (`internal/engine/postgresql/`)
2. Review SQLite implementation (`internal/engine/sqlite/`) 
3. Look at Dolphin/MySQL approach (`internal/engine/dolphin/`)
4. Follow the most appropriate pattern for Spanner's needs

Common patterns to look for:
- AST node conversion approaches
- Error handling strategies (e.g., TODO/unsupported node handling)
- Type inference logic
- Case sensitivity handling
- List initialization patterns

## Questions to Consider

When implementing new features, ask:
1. Is there a simpler way using memefish utilities?
2. Can this be maintenance-free using built-in functions?
3. Does this follow the established patterns for Lists and wrapping?
4. Have I tested with both emulator and generated code?
5. How do other engines handle this scenario?

## Critical Development Principles for Engine Implementation

### NEVER Modify Common Logic Without Exhaustive Verification

**Principle**: Common logic modifications should be the absolute last resort. Always exhaust all engine-specific alternatives first.

#### Development Process for New Features

1. **Engine-Specific Solution First**
   - Implement the feature entirely within the engine directory (`internal/engine/spanner/`)
   - Use convert.go to transform AST nodes to achieve desired behavior
   - Consider creating wrapper structures or intermediate representations

2. **Investigate Existing Patterns**
   - Check how PostgreSQL/SQLite/MySQL handle similar features
   - Verify if they required common logic changes or solved it engine-side
   - Test the same SQL patterns with other engines to understand behavior

3. **Document Investigation Results**
   - If common logic changes seem necessary, document ALL attempted alternatives
   - Include specific test cases that fail without common logic changes
   - Explain why engine-specific solutions are insufficient

4. **Test Without Common Logic Changes**
   - Create comprehensive test cases
   - Verify that the issue is truly unsolvable without common changes
   - Check if the "problem" is actually just a missing/incorrect test setup

5. **If Common Logic Changes Are Unavoidable**
   - DO NOT COMMIT the changes to common logic
   - Instead, add detailed comments in the engine code explaining:
     ```go
     // LIMITATION: Feature X requires common logic changes
     // Attempted solutions:
     // 1. [Approach A] - Failed because...
     // 2. [Approach B] - Failed because...
     // Required change: [File] needs [specific modification]
     // Workaround: Currently returns interface{} instead of proper type
     ```

### Common Logic Modification Anti-Patterns (AVOID)

1. **Adding engine-specific cases to output_columns.go**
   - Wrong: Adding special handling for Spanner-specific nodes
   - Right: Transform nodes in convert.go to existing supported structures

2. **Modifying resolve.go for parameter handling**
   - Wrong: Adding new parameter resolution cases
   - Right: Ensure proper schema/table setup and use existing resolution

3. **Type inference heuristics**
   - Wrong: Guessing types based on field names or patterns
   - Right: Use explicit type information or return interface{}

### Lessons from Real Issues

#### Example 1: UNNEST Support
- **Problem**: Value table semantics don't map to PostgreSQL model
- **Attempted**: Multiple AST transformation approaches
- **Result**: Documented as limitation requiring common logic changes
- **Learning**: Some semantic mismatches are fundamental and cannot be bridged

#### Example 2: ColumnRef in INSERT
- **Problem**: Parameters not resolving correctly in INSERT statements
- **Root Cause**: Missing column in test schema, not a code issue
- **Learning**: Always verify test setup before assuming code changes needed

#### Example 3: A_Indirection for Array Access
- **Initial Assumption**: Spanner-specific feature needing common logic
- **Investigation**: PostgreSQL also uses A_Indirection for array access
- **Finding**: PostgreSQL has analyzer for type resolution, Spanner doesn't
- **Decision**: Minimal support acceptable as it benefits multiple engines

### Testing Protocol Before Claiming Common Logic Need

1. Create minimal reproduction case
2. Test with all engines to verify engine-specific issue
3. Check if feature works with different SQL syntax
4. Verify schema and test data are correct
5. Try alternative AST transformations in convert.go
6. Document all attempted workarounds

### Documentation Requirements

When a limitation requires common logic changes, document in `internal/engine/spanner/FEATURES.md`:

```markdown
### Feature Name
- **Status**: Partially implemented / Not implemented
- **Limitation**: [Specific description]
- **Root Cause**: [Technical explanation]
- **Required Common Logic Change**: [File and modification needed]
- **Workaround**: [Current behavior if any]
- **Test Cases**: [List of SQL that should work but doesn't]
```

This ensures future developers understand exactly what's needed and why.