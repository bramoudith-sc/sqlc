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

## Questions to Consider

When implementing new features, ask:
1. Is there a simpler way using memefish utilities?
2. Can this be maintenance-free using built-in functions?
3. Does this follow the established patterns for Lists and wrapping?
4. Have I tested with both emulator and generated code?