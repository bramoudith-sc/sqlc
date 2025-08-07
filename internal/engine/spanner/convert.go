// Package spanner implements AST conversion from memefish (Spanner SQL parser) to sqlc's internal AST.
//
// Key architectural decisions:
//
//  1. List initialization: All sqlcast.List fields that are walked by the compiler must be initialized
//     with empty Items arrays, not nil. The compiler's Walk function expects to iterate over these lists
//     and will panic on nil. Fields that are conditionally accessed (like WhereClause) can be nil.
//
//  2. Star (*) handling: Spanner's Star nodes must be wrapped in ColumnRef to match PostgreSQL's AST
//     structure. This wrapping is critical for the compiler's hasStarRef() and column expansion logic
//     to work correctly. The pattern is: ResTarget -> ColumnRef -> A_Star.
//
//  3. THEN RETURN conversion: Spanner's THEN RETURN clause is converted to PostgreSQL's RETURNING
//     clause, maintaining the same AST structure patterns (especially for Star nodes).
//
//  4. Function names: Spanner supports namespaced functions (e.g., NET.IPV4_TO_INT64, SAFE.DIVIDE).
//     All path components are joined with dots to preserve the full function name for resolution.
package spanner

import (
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/davecgh/go-spew/spew"

	"github.com/sqlc-dev/sqlc/internal/debug"
	sqlcast "github.com/sqlc-dev/sqlc/internal/sql/ast"
)

type cc struct {
	paramCount     int
	paramMap       map[string]int // Map parameter names to their position
	paramsByNum    map[int]string // Map position to parameter name
	positionOffset int            // Offset to adjust AST positions to file positions
}

func todo(funcname string, n ast.Node) *sqlcast.TODO {
	if debug.Active {
		log.Printf("spanner.%s: Unknown node type %T\n", funcname, n)
	}
	return &sqlcast.TODO{}
}

func identifier(id string) string {
	// Spanner identifiers are case-insensitive
	return strings.ToLower(id)
}

func NewIdentifier(t string) *sqlcast.String {
	return &sqlcast.String{Str: identifier(t)}
}

func (c *cc) convert(n ast.Node) sqlcast.Node {
	if n == nil {
		return nil
	}

	switch node := n.(type) {
	// DDL Statements
	case *ast.CreateTable:
		return c.convertCreateTable(node)
	case *ast.DropTable:
		return c.convertDropTable(node)
	case *ast.CreateIndex:
		return c.convertCreateIndex(node)
	case *ast.DropIndex:
		return c.convertDropIndex(node)
	case *ast.AlterTable:
		return c.convertAlterTable(node)
	case *ast.CreateView:
		return c.convertCreateView(node)
	case *ast.DropView:
		return c.convertDropView(node)

	// DML Statements
	case *ast.Insert:
		return c.convertInsert(node)
	case *ast.Update:
		return c.convertUpdate(node)
	case *ast.Delete:
		return c.convertDelete(node)

	// Query Statements
	case *ast.QueryStatement:
		return c.convertQueryStatement(node)
	case *ast.Query:
		return c.convertQuery(node)
	case *ast.Select:
		return c.convertSelect(node)

	// Expressions
	case *ast.Ident:
		return c.convertIdent(node)
	case *ast.Path:
		return c.convertPath(node)
	case *ast.IntLiteral:
		return c.convertIntLiteral(node)
	case *ast.StringLiteral:
		return c.convertStringLiteral(node)
	case *ast.BoolLiteral:
		return c.convertBoolLiteral(node)
	case *ast.NullLiteral:
		return &sqlcast.Null{}
	case *ast.BinaryExpr:
		return c.convertBinaryExpr(node)
	case *ast.UnaryExpr:
		return c.convertUnaryExpr(node)
	case *ast.CallExpr:
		return c.convertCallExpr(node)
	case *ast.CountStarExpr:
		return c.convertCountStarExpr(node)
	case *ast.CaseExpr:
		return c.convertCaseExpr(node)
	case *ast.CastExpr:
		return c.convertCastExpr(node)
	case *ast.InExpr:
		return c.convertInExpr(node)
	case *ast.IsNullExpr:
		return c.convertIsNullExpr(node)
	case *ast.BetweenExpr:
		return c.convertBetweenExpr(node)
	case *ast.ExtractExpr:
		return c.convertExtractExpr(node)
	case *ast.IfExpr:
		if debug.Active {
			log.Printf("Converting IfExpr to CaseExpr\n")
		}
		return c.convertIfExpr(node)
	case *ast.ParenExpr:
		return c.convertParenExpr(node)
	case *ast.Param:
		return c.convertParam(node)
	case *ast.DefaultExpr:
		return c.convertDefaultExpr(node)
	
	// Spanner-specific literal types
	case *ast.DateLiteral:
		return c.convertDateLiteral(node)
	case *ast.TimestampLiteral:
		return c.convertTimestampLiteral(node)
	case *ast.NumericLiteral:
		return c.convertNumericLiteral(node)
	case *ast.JSONLiteral:
		return c.convertJSONLiteral(node)
	case *ast.BytesLiteral:
		return c.convertBytesLiteral(node)
	case *ast.ArrayLiteral:
		return c.convertArrayLiteral(node)
	case *ast.FloatLiteral:
		return c.convertFloatLiteral(node)
	
	// Subquery expressions
	case *ast.ScalarSubQuery:
		return c.convertScalarSubQuery(node)
	case *ast.ArraySubQuery:
		return c.convertArraySubQuery(node)
	case *ast.ExistsSubQuery:
		return c.convertExistsSubQuery(node)
	
	// STRUCT literals
	case *ast.TypedStructLiteral:
		return c.convertTypedStructLiteral(node)
	case *ast.TypelessStructLiteral:
		return c.convertTypelessStructLiteral(node)
	case *ast.TupleStructLiteral:
		return c.convertTupleStructLiteral(node)
	
	// INTERVAL literals
	case *ast.IntervalLiteralSingle:
		return c.convertIntervalLiteralSingle(node)
	case *ast.IntervalLiteralRange:
		return c.convertIntervalLiteralRange(node)
	
	// Array/Struct access
	case *ast.IndexExpr:
		return c.convertIndexExpr(node)
	case *ast.SelectorExpr:
		return c.convertSelectorExpr(node)
	case *ast.Unnest:
		return c.convertUnnest(node)

	// Other nodes
	case *ast.Star:
		// Wrap A_Star in ColumnRef to match PostgreSQL's AST structure.
		// PostgreSQL represents SELECT * as ColumnRef containing A_Star,
		// not as a bare A_Star node. This wrapping is essential for
		// proper type resolution in the compiler's outputColumns function.
		return &sqlcast.ColumnRef{
			Fields: &sqlcast.List{
				Items: []sqlcast.Node{
					&sqlcast.A_Star{},
				},
			},
		}

	default:
		return todo("convert", n)
	}
}

// DDL Conversions
func (c *cc) convertCreateTable(n *ast.CreateTable) *sqlcast.CreateTableStmt {
	stmt := &sqlcast.CreateTableStmt{
		IfNotExists: n.IfNotExists,
		Name:        parseTableName(n.Name),
		Cols:        []*sqlcast.ColumnDef{},
	}

	// Convert columns
	for _, col := range n.Columns {
		typeName := c.convertSchemaType(col.Type)
		colDef := &sqlcast.ColumnDef{
			Colname: identifier(col.Name.Name),
			TypeName: &sqlcast.TypeName{
				Name: typeName,
				Names: &sqlcast.List{
					Items: []sqlcast.Node{
						&sqlcast.String{Str: typeName},
					},
				},
			},
			IsNotNull: col.NotNull,
		}
		stmt.Cols = append(stmt.Cols, colDef)
	}

	// TODO: Convert table constraints and other features when needed:
	// - INTERLEAVE IN PARENT clause for parent-child relationships
	// - ROW DELETION POLICY for TTL support
	// - Table-level CHECK constraints
	// These features are Spanner-specific and may require extending sqlc's AST
	return stmt
}

func (c *cc) convertDropTable(n *ast.DropTable) *sqlcast.DropTableStmt {
	return &sqlcast.DropTableStmt{
		IfExists: n.IfExists,
		Tables: []*sqlcast.TableName{
			parseTableName(n.Name),
		},
	}
}

func (c *cc) convertCreateIndex(n *ast.CreateIndex) *sqlcast.IndexStmt {
	stmt := &sqlcast.IndexStmt{
		Idxname:     identifier(strings.Join(pathToStrings(n.Name), ".")),
		Relation:    convertPathToRangeVar(n.TableName),
		Unique:      n.Unique,
		IfNotExists: n.IfNotExists,
		Params:      &sqlcast.List{Items: []sqlcast.Node{}},
	}
	
	// Convert index keys to column names
	for _, key := range n.Keys {
		if key.Name != nil {
			// Handle column reference
			colName := identifier(strings.Join(pathToStrings(key.Name), "."))
			stmt.Params.Items = append(stmt.Params.Items, &sqlcast.IndexElem{
				Name: &colName,
				// Spanner supports ASC/DESC in indexes
				Ordering: convertSortDirection(key.Dir),
			})
		}
	}
	
	// Note: STORING, INTERLEAVE IN, and OPTIONS are Spanner-specific
	// and don't have direct equivalents in PostgreSQL's AST
	if n.Storing != nil && debug.Active {
		log.Printf("spanner.convertCreateIndex: STORING clause not fully supported\n")
	}
	if n.InterleaveIn != nil && debug.Active {
		log.Printf("spanner.convertCreateIndex: INTERLEAVE IN clause not fully supported\n")
	}
	
	return stmt
}

func (c *cc) convertDropIndex(n *ast.DropIndex) *sqlcast.DropStmt {
	indexName := identifier(strings.Join(pathToStrings(n.Name), "."))
	return &sqlcast.DropStmt{
		RemoveType: sqlcast.OBJECT_INDEX,
		IfExists:   n.IfExists,
		Objects: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: indexName},
			},
		},
	}
}

func (c *cc) convertAlterTable(n *ast.AlterTable) *sqlcast.AlterTableStmt {
	stmt := &sqlcast.AlterTableStmt{
		Table: convertPathToRangeVar(n.Name),
		Cmds:  &sqlcast.List{Items: []sqlcast.Node{}},
	}
	
	// Handle different types of table alterations
	switch alt := n.TableAlteration.(type) {
	case *ast.AddColumn:
		for _, col := range alt.Columns {
			colDef := &sqlcast.ColumnDef{
				Colname: identifier(col.Name.Name),
			}
			// Convert column type
			if col.Type != nil {
				colDef.TypeName = convertType(col.Type)
			}
			// Handle NOT NULL constraint
			if col.NotNull {
				colDef.Constraints = &sqlcast.List{
					Items: []sqlcast.Node{
						&sqlcast.Constraint{
							Contype: sqlcast.CONSTR_NOTNULL,
						},
					},
				}
			}
			
			stmt.Cmds.Items = append(stmt.Cmds.Items, &sqlcast.AlterTableCmd{
				Subtype: sqlcast.AT_AddColumn,
				Def:     colDef,
			})
		}
	case *ast.DropColumn:
		for _, col := range alt.Names {
			colName := identifier(col.Name)
			stmt.Cmds.Items = append(stmt.Cmds.Items, &sqlcast.AlterTableCmd{
				Subtype: sqlcast.AT_DropColumn,
				Name:    &colName,
			})
		}
	case *ast.AlterColumn:
		// Handle ALTER COLUMN
		if alt.Name != nil {
			colName := identifier(alt.Name.Name)
			cmd := &sqlcast.AlterTableCmd{
				Name: &colName,
			}
			
			// Determine the alteration type
			if alt.Alteration != nil {
				switch alteration := alt.Alteration.(type) {
				case *ast.AlterColumnSetType:
					cmd.Subtype = sqlcast.AT_AlterColumnType
					cmd.Def = &sqlcast.ColumnDef{
						TypeName: convertType(alteration.Type),
					}
				case *ast.AlterColumnSetDefault:
					cmd.Subtype = sqlcast.AT_ColumnDefault
					// Convert default expression
				case *ast.AlterColumnDropDefault:
					cmd.Subtype = sqlcast.AT_DropNotNull
				}
			}
			
			stmt.Cmds.Items = append(stmt.Cmds.Items, cmd)
		}
	default:
		if debug.Active {
			log.Printf("spanner.convertAlterTable: Unsupported alteration type %T\n", alt)
		}
	}
	
	return stmt
}

// Helper functions for DDL conversions
func pathToStrings(p *ast.Path) []string {
	if p == nil {
		return nil
	}
	var result []string
	for _, ident := range p.Idents {
		result = append(result, ident.Name)
	}
	return result
}

func convertPathToRangeVar(p *ast.Path) *sqlcast.RangeVar {
	if p == nil {
		return nil
	}
	parts := pathToStrings(p)
	if len(parts) == 0 {
		return nil
	}
	
	// Take the last part as the table name
	tableName := identifier(parts[len(parts)-1])
	rangeVar := &sqlcast.RangeVar{
		Relname: &tableName,
	}
	
	// If there are more parts, they represent the schema
	if len(parts) > 1 {
		schemaName := identifier(strings.Join(parts[:len(parts)-1], "."))
		rangeVar.Schemaname = &schemaName
	}
	
	return rangeVar
}

func convertSortDirection(dir ast.Dir) sqlcast.SortByDir {
	switch dir {
	case ast.DirAsc:
		return sqlcast.SORTBY_ASC
	case ast.DirDesc:
		return sqlcast.SORTBY_DESC
	default:
		return sqlcast.SORTBY_DEFAULT
	}
}

func (c *cc) convertCreateView(n *ast.CreateView) *sqlcast.ViewStmt {
	viewName := identifier(strings.Join(pathToStrings(n.Name), "."))
	return &sqlcast.ViewStmt{
		View: &sqlcast.RangeVar{
			Relname: &viewName,
		},
		Query:   c.convert(n.Query),
		Replace: n.OrReplace,
	}
}

func (c *cc) convertDropView(n *ast.DropView) *sqlcast.DropStmt {
	viewName := identifier(strings.Join(pathToStrings(n.Name), "."))
	return &sqlcast.DropStmt{
		RemoveType: sqlcast.OBJECT_VIEW,
		IfExists:   n.IfExists,
		Objects: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: viewName},
			},
		},
	}
}

// DML Conversions
func (c *cc) convertInsert(n *ast.Insert) *sqlcast.InsertStmt {
	// IMPORTANT: List fields must be initialized with empty Items arrays, not nil.
	// The sqlc compiler's Walk function expects Lists to be iterable even when empty.
	// Using nil would cause a panic during AST traversal.
	stmt := &sqlcast.InsertStmt{
		Relation:      convertTableNameToRangeVar(n.TableName),
		Cols:          &sqlcast.List{Items: []sqlcast.Node{}}, // Must initialize with empty array
		SelectStmt:    nil,                                    // Can be nil - not always walked
		ReturningList: &sqlcast.List{Items: []sqlcast.Node{}}, // Must initialize for THEN RETURN support
	}

	// Convert column names
	for _, col := range n.Columns {
		stmt.Cols.Items = append(stmt.Cols.Items, c.convertIdent(col))
	}

	// Convert input (VALUES)
	if n.Input != nil {
		switch input := n.Input.(type) {
		case *ast.ValuesInput:
			stmt.SelectStmt = c.convertValuesInput(input)
		case *ast.SubQueryInput:
			if input.Query != nil {
				stmt.SelectStmt = c.convert(input.Query)
			}
		default:
			// Handle other input types
		}
	}

	// Convert THEN RETURN clause to RETURNING
	if n.ThenReturn != nil {
		stmt.ReturningList = c.convertThenReturn(n.ThenReturn)
	}

	return stmt
}

func (c *cc) convertUpdate(n *ast.Update) *sqlcast.UpdateStmt {
	// Initialize all List fields with empty Items arrays to prevent nil pointer panics
	stmt := &sqlcast.UpdateStmt{
		Relations:     &sqlcast.List{Items: []sqlcast.Node{}}, // Table being updated
		TargetList:    &sqlcast.List{Items: []sqlcast.Node{}}, // SET clause items
		WhereClause:   nil,                                    // Can be nil - conditional
		FromClause:    &sqlcast.List{Items: []sqlcast.Node{}}, // Additional FROM tables
		ReturningList: &sqlcast.List{Items: []sqlcast.Node{}}, // THEN RETURN -> RETURNING
		WithClause:    nil,                                    // Can be nil - optional
	}

	// Add table to relations
	stmt.Relations.Items = append(stmt.Relations.Items,
		convertTableNameToRangeVar(n.TableName))

	// Convert UPDATE SET items
	for _, item := range n.Updates {
		// Get the value expression
		var value sqlcast.Node
		if item.DefaultExpr != nil && !item.DefaultExpr.Default {
			// Only convert if it's an expression, not DEFAULT keyword
			value = c.convert(item.DefaultExpr.Expr)
		}

		if len(item.Path) > 0 && value != nil {
			// Get the column name from the path
			colName := item.Path[len(item.Path)-1].Name

			// Create ResTarget for the update
			stmt.TargetList.Items = append(stmt.TargetList.Items, &sqlcast.ResTarget{
				Name: &colName,
				Val:  value,
			})
		}
	}

	// Convert WHERE clause
	if n.Where != nil {
		stmt.WhereClause = c.convert(n.Where.Expr)
	}

	// Convert THEN RETURN clause to RETURNING
	if n.ThenReturn != nil {
		stmt.ReturningList = c.convertThenReturn(n.ThenReturn)
	}

	return stmt
}

func (c *cc) convertDelete(n *ast.Delete) *sqlcast.DeleteStmt {
	// Lists must be initialized even when empty to support AST traversal
	stmt := &sqlcast.DeleteStmt{
		Relations:     &sqlcast.List{Items: []sqlcast.Node{}}, // Tables to delete from
		UsingClause:   &sqlcast.List{Items: []sqlcast.Node{}}, // USING clause tables
		WhereClause:   nil,                                    // Can be nil
		ReturningList: &sqlcast.List{Items: []sqlcast.Node{}}, // For THEN RETURN support
		WithClause:    nil,                                    // Can be nil
	}

	// Add table to relations
	stmt.Relations.Items = append(stmt.Relations.Items,
		convertTableNameToRangeVar(n.TableName))

	if n.Where != nil {
		stmt.WhereClause = c.convert(n.Where.Expr)
	}

	// Convert THEN RETURN clause to RETURNING
	if n.ThenReturn != nil {
		stmt.ReturningList = c.convertThenReturn(n.ThenReturn)
	}

	return stmt
}

// Query Conversions
func (c *cc) convertQueryStatement(n *ast.QueryStatement) sqlcast.Node {
	// QueryStatement wraps a Query
	return c.convert(n.Query)
}

func (c *cc) convertQuery(n *ast.Query) sqlcast.Node {
	// Query contains the actual SELECT with ORDER BY and LIMIT
	var baseStmt *sqlcast.SelectStmt

	// Convert the inner query expression
	if n.Query != nil {
		if stmt, ok := c.convert(n.Query).(*sqlcast.SelectStmt); ok {
			baseStmt = stmt
		} else {
			// If it's not a SelectStmt, create a new one
			baseStmt = &sqlcast.SelectStmt{}
		}
	} else {
		baseStmt = &sqlcast.SelectStmt{}
	}

	// Add ORDER BY
	if n.OrderBy != nil {
		baseStmt.SortClause = c.convertOrderBy(n.OrderBy)
	}

	// Add LIMIT
	if n.Limit != nil {
		baseStmt.LimitCount = c.convert(n.Limit.Count)
		if n.Limit.Offset != nil {
			baseStmt.LimitOffset = c.convert(n.Limit.Offset)
		}
	}

	// Handle WITH clause
	if n.With != nil {
		baseStmt.WithClause = c.convertWithClause(n.With)
	}

	return baseStmt
}

func (c *cc) convertSelect(n *ast.Select) *sqlcast.SelectStmt {
	stmt := &sqlcast.SelectStmt{
		// CRITICAL: Lists that are always walked must be initialized with Items arrays.
		// The compiler's Walk function will panic if it encounters a nil List where
		// it expects to iterate. This is a key difference from other AST nodes that
		// can be nil (like WhereClause, which is checked before use).
		TargetList:  &sqlcast.List{Items: []sqlcast.Node{}}, // SELECT items - always walked
		FromClause:  &sqlcast.List{Items: []sqlcast.Node{}}, // FROM tables - always walked
		WhereClause: nil,                                    // Can be nil - conditionally accessed
		GroupClause: nil,                                    // Can be nil - only set if GROUP BY exists
		SortClause:  nil,                                    // Can be nil - only set if ORDER BY exists
		LimitCount:  nil,                                    // Can be nil - scalar value
		LimitOffset: nil,                                    // Can be nil - scalar value
		ValuesLists: nil,                                    // Can be nil - only for VALUES queries
	}

	// Handle SELECT AS STRUCT / AS VALUE modifiers
	// AS STRUCT returns a single STRUCT containing all selected columns
	// AS VALUE returns a single scalar value (must select exactly one column)
	if n.As != nil {
		switch n.As.(type) {
		case *ast.AsStruct:
			// TODO: SELECT AS STRUCT needs special handling
			// It should return a single STRUCT column containing all selected fields
			// For now, we'll process it as a regular SELECT
			if debug.Active {
				log.Printf("spanner.convertSelect: SELECT AS STRUCT not fully implemented\n")
			}
		case *ast.AsValue:
			// TODO: SELECT AS VALUE needs validation (must have exactly one column)
			// It returns the single selected value directly instead of a row
			if debug.Active {
				log.Printf("spanner.convertSelect: SELECT AS VALUE not fully implemented\n")
			}
		}
	}

	// Convert SELECT items
	for _, item := range n.Results {
		switch i := item.(type) {
		case *ast.Star:
			// SELECT * must be wrapped: ResTarget -> ColumnRef -> A_Star
			// This three-level structure matches PostgreSQL and enables
			// the hasStarRef() check in output_columns.go to work correctly.
			
			// Handle EXCEPT and REPLACE modifiers
			if i.Except != nil || i.Replace != nil {
				// TODO: SELECT * EXCEPT and REPLACE require special handling
				// EXCEPT: Should exclude specified columns from the result
				// REPLACE: Should replace specified column expressions
				// For now, we'll treat it as a regular * and log the limitation
				if debug.Active {
					if i.Except != nil {
						log.Printf("spanner.convertSelect: SELECT * EXCEPT not fully implemented\n")
					}
					if i.Replace != nil {
						log.Printf("spanner.convertSelect: SELECT * REPLACE not fully implemented\n")
					}
				}
			}
			
			stmt.TargetList.Items = append(stmt.TargetList.Items, &sqlcast.ResTarget{
				Val: &sqlcast.ColumnRef{
					Fields: &sqlcast.List{
						Items: []sqlcast.Node{
							&sqlcast.A_Star{},
						},
					},
					Location: int(i.Star) + c.positionOffset,
				},
				Location: int(i.Star) + c.positionOffset, // Adjust position to file offset
			})
		case *ast.DotStar:
			// Handle table.* syntax
			// Convert the expression part and wrap in ColumnRef with A_Star
			var fields []sqlcast.Node
			
			// Add the table/expression reference
			switch expr := i.Expr.(type) {
			case *ast.Ident:
				fields = append(fields, NewIdentifier(expr.Name))
			case *ast.Path:
				for _, ident := range expr.Idents {
					fields = append(fields, NewIdentifier(ident.Name))
				}
			default:
				// For complex expressions, treat as single field
				fields = append(fields, c.convert(expr))
			}
			
			// Add the star
			fields = append(fields, &sqlcast.A_Star{})
			
			// Handle EXCEPT and REPLACE modifiers (same as Star)
			if i.Except != nil || i.Replace != nil {
				if debug.Active {
					if i.Except != nil {
						log.Printf("spanner.convertSelect: table.* EXCEPT not fully implemented\n")
					}
					if i.Replace != nil {
						log.Printf("spanner.convertSelect: table.* REPLACE not fully implemented\n")
					}
				}
			}
			
			stmt.TargetList.Items = append(stmt.TargetList.Items, &sqlcast.ResTarget{
				Val: &sqlcast.ColumnRef{
					Fields: &sqlcast.List{
						Items: fields,
					},
				},
			})
		case *ast.Alias:
			// Handle alias
			var name *string
			if i.As != nil && i.As.Alias != nil {
				name = &i.As.Alias.Name
			}
			stmt.TargetList.Items = append(stmt.TargetList.Items, &sqlcast.ResTarget{
				Name: name,
				Val:  c.convert(i.Expr),
			})
		default:
			// For simple column references wrapped in ExprSelectItem, extract the name
			var colName *string
			var val sqlcast.Node

			if exprItem, ok := item.(*ast.ExprSelectItem); ok {
				// Extract the expression from the wrapper
				if ident, ok := exprItem.Expr.(*ast.Ident); ok {
					name := ident.Name
					colName = &name
				} else if path, ok := exprItem.Expr.(*ast.Path); ok && len(path.Idents) > 0 {
					// For path expressions like table.column, use the last identifier
					name := path.Idents[len(path.Idents)-1].Name
					colName = &name
				}
				val = c.convert(exprItem.Expr)
			} else {
				// Fallback for other types
				val = c.convert(item)
			}

			stmt.TargetList.Items = append(stmt.TargetList.Items, &sqlcast.ResTarget{
				Name: colName,
				Val:  val,
			})
		}
	}

	// Convert FROM clause
	if n.From != nil && n.From.Source != nil {
		stmt.FromClause.Items = append(stmt.FromClause.Items, c.convertTableExpr(n.From.Source))
	}

	// Convert WHERE clause
	if n.Where != nil {
		stmt.WhereClause = c.convert(n.Where.Expr)
	}

	// Convert GROUP BY
	if n.GroupBy != nil && len(n.GroupBy.Exprs) > 0 {
		stmt.GroupClause = &sqlcast.List{Items: []sqlcast.Node{}}
		for _, expr := range n.GroupBy.Exprs {
			stmt.GroupClause.Items = append(stmt.GroupClause.Items, c.convert(expr))
		}
	}

	return stmt
}

// Expression Conversions
func (c *cc) convertIdent(n *ast.Ident) *sqlcast.ColumnRef {
	return &sqlcast.ColumnRef{
		Fields: &sqlcast.List{
			Items: []sqlcast.Node{
				NewIdentifier(n.Name),
			},
		},
	}
}

func (c *cc) convertPath(n *ast.Path) *sqlcast.ColumnRef {
	// Debug: Dump the AST structure for STRUCT field access
	if debug.Active && len(n.Idents) > 0 {
		log.Printf("=== convertPath: Path with %d idents ===\n", len(n.Idents))
		for i, ident := range n.Idents {
			log.Printf("  Ident[%d]: %s\n", i, ident.Name)
		}
		log.Printf("Full AST dump:\n%s\n", spew.Sdump(n))
	}
	
	items := []sqlcast.Node{}
	for _, ident := range n.Idents {
		items = append(items, NewIdentifier(ident.Name))
	}
	return &sqlcast.ColumnRef{
		Fields: &sqlcast.List{Items: items},
	}
}

func (c *cc) convertIntLiteral(n *ast.IntLiteral) *sqlcast.A_Const {
	// Convert string value to int64
	ival, _ := strconv.ParseInt(n.Value, n.Base, 64)
	return &sqlcast.A_Const{
		Val: &sqlcast.Integer{Ival: ival},
	}
}

func (c *cc) convertStringLiteral(n *ast.StringLiteral) *sqlcast.A_Const {
	return &sqlcast.A_Const{
		Val: &sqlcast.String{Str: n.Value},
	}
}

func (c *cc) convertBoolLiteral(n *ast.BoolLiteral) *sqlcast.A_Const {
	str := "false"
	if n.Value {
		str = "true"
	}
	return &sqlcast.A_Const{
		Val: &sqlcast.String{Str: str},
	}
}

func (c *cc) convertBinaryExpr(n *ast.BinaryExpr) *sqlcast.A_Expr {
	return &sqlcast.A_Expr{
		Name: &sqlcast.List{
			Items: []sqlcast.Node{
				NewIdentifier(string(n.Op)),
			},
		},
		Lexpr: c.convert(n.Left),
		Rexpr: c.convert(n.Right),
	}
}

func (c *cc) convertCallExpr(n *ast.CallExpr) sqlcast.Node {
	// Extract function name from path
	var funcName string
	if n.Func != nil && len(n.Func.Idents) > 0 {
		// Join all identifiers with dots to support namespaced functions.
		// Examples: NET.IPV4_TO_INT64, SAFE.DIVIDE, SAFE.NET.IPV4_TO_INT64
		// This preserves the full function path for proper resolution.
		//
		// NOTE: Function names are preserved as-is (not lowercased) here.
		// Case-insensitive matching happens later in the catalog lookup
		// (see internal/sql/catalog/public.go:ListFuncsByName which lowercases
		// the name for comparison). This allows the original case to be preserved
		// in generated code while still supporting case-insensitive SQL.
		var parts []string
		for _, ident := range n.Func.Idents {
			parts = append(parts, ident.Name)
		}
		funcName = strings.Join(parts, ".")
	}
	
	// Convert arguments first for conditional expression handling
	var args []sqlcast.Node
	for _, arg := range n.Args {
		switch a := arg.(type) {
		case *ast.ExprArg:
			args = append(args, c.convert(a.Expr))
		default:
			// Handle other arg types
		}
	}
	
	// Handle conditional expressions that should be converted to CASE
	funcNameLower := strings.ToLower(funcName)
	switch funcNameLower {
	case "ifnull":
		// IFNULL(expr, null_result) -> CASE WHEN expr IS NULL THEN null_result ELSE expr END
		if len(args) == 2 {
			return c.convertIfNullToCase(args[0], args[1], int(n.Func.Pos()))
		}
	case "nullif":
		// NULLIF(expr, expr_to_match) -> CASE WHEN expr = expr_to_match THEN NULL ELSE expr END
		if len(args) == 2 {
			return c.convertNullIfToCase(args[0], args[1], int(n.Func.Pos()))
		}
	case "coalesce":
		// Use native CoalesceExpr for better type inference
		if len(args) >= 1 {
			return &sqlcast.CoalesceExpr{
				Args:     &sqlcast.List{Items: args},
				Location: int(n.Func.Pos()),
			}
		}
	}

	funcCall := &sqlcast.FuncCall{
		Func: &sqlcast.FuncName{
			Name: funcName,
		},
		Args: &sqlcast.List{Items: args},
	}

	return funcCall
}

func (c *cc) convertParam(n *ast.Param) sqlcast.Node {
	// For Spanner, we track parameters by name
	paramName := n.Name

	// Check if we've seen this parameter before
	if num, exists := c.paramMap[paramName]; exists {
		return &sqlcast.ParamRef{
			Number:   num,
			Location: int(n.Pos()),
		}
	}

	// New parameter - assign it a number
	c.paramCount++
	c.paramMap[paramName] = c.paramCount
	c.paramsByNum[c.paramCount] = paramName

	return &sqlcast.ParamRef{
		Number:   c.paramCount,
		Location: int(n.Pos()),
	}
}

func (c *cc) convertDefaultExpr(n *ast.DefaultExpr) sqlcast.Node {
	if n.Default {
		// DEFAULT keyword - use a string constant
		return &sqlcast.A_Const{
			Val: &sqlcast.String{Str: "DEFAULT"},
		}
	}
	if n.Expr != nil {
		return c.convert(n.Expr)
	}
	return &sqlcast.TODO{}
}

func (c *cc) convertTableExpr(n ast.TableExpr) sqlcast.Node {
	switch t := n.(type) {
	case *ast.TableName:
		name := identifier(t.Table.Name)
		rangeVar := &sqlcast.RangeVar{
			Relname: &name,
		}
		// Handle table alias
		if t.As != nil {
			alias := identifier(t.As.Alias.Name)
			rangeVar.Alias = &sqlcast.Alias{
				Aliasname: &alias,
			}
		}
		// TABLESAMPLE clause is parsed but doesn't affect code generation
		// It only affects runtime row sampling, not the query structure
		if t.Sample != nil && debug.Active {
			log.Printf("spanner.convertTableExpr: TABLESAMPLE %s (runtime sampling only)\n", t.Sample.Method)
		}
		return rangeVar
	case *ast.Join:
		return c.convertJoin(t)
	case *ast.ParenTableExpr:
		// Handle parenthesized table expressions
		return c.convertTableExpr(t.Source)
	case *ast.SubQueryTableExpr:
		// Handle subquery in FROM clause
		subquery := &sqlcast.RangeSubselect{
			Subquery: c.convert(t.Query),
		}
		if t.As != nil {
			alias := identifier(t.As.Alias.Name)
			subquery.Alias = &sqlcast.Alias{
				Aliasname: &alias,
			}
		}
		// TABLESAMPLE on subquery (runtime sampling only)
		if t.Sample != nil && debug.Active {
			log.Printf("spanner.convertTableExpr: TABLESAMPLE on subquery (runtime sampling only)\n")
		}
		return subquery
	case *ast.Unnest:
		// Handle UNNEST in FROM clause
		return c.convertUnnest(t)
	default:
		return todo("convertTableExpr", n)
	}
}

func (c *cc) convertJoin(n *ast.Join) *sqlcast.JoinExpr {
	if n == nil {
		return nil
	}
	
	// Map Spanner join types to PostgreSQL join types
	var joinType sqlcast.JoinType
	switch n.Op {
	case ast.CommaJoin:
		joinType = sqlcast.JoinTypeInner
	case ast.CrossJoin:
		joinType = sqlcast.JoinTypeInner // Cross join can be represented as inner join without condition
	case ast.InnerJoin:
		joinType = sqlcast.JoinTypeInner
	case ast.LeftOuterJoin:
		joinType = sqlcast.JoinTypeLeft
	case ast.RightOuterJoin:
		joinType = sqlcast.JoinTypeRight
	case ast.FullOuterJoin:
		joinType = sqlcast.JoinTypeFull
	default:
		joinType = sqlcast.JoinTypeInner
	}
	
	joinExpr := &sqlcast.JoinExpr{
		Jointype: joinType,
		Larg:     c.convertTableExpr(n.Left),
		Rarg:     c.convertTableExpr(n.Right),
	}
	
	// Convert join condition
	if n.Cond != nil {
		switch cond := n.Cond.(type) {
		case *ast.On:
			joinExpr.Quals = c.convert(cond.Expr)
		case *ast.Using:
			// Convert USING clause to equality conditions
			// This is a simplified implementation
			var usingList []sqlcast.Node
			for _, col := range cond.Idents {
				usingList = append(usingList, &sqlcast.String{Str: identifier(col.Name)})
			}
			joinExpr.UsingClause = &sqlcast.List{Items: usingList}
		}
	}
	
	return joinExpr
}

func (c *cc) convertOrderBy(n *ast.OrderBy) *sqlcast.List {
	orderList := &sqlcast.List{Items: []sqlcast.Node{}}
	for _, item := range n.Items {
		sortBy := &sqlcast.SortBy{
			Node: c.convert(item.Expr),
		}
		if item.Dir != "" {
			switch item.Dir {
			case ast.DirectionAsc:
				sortBy.SortbyDir = sqlcast.SortByDirAsc
			case ast.DirectionDesc:
				sortBy.SortbyDir = sqlcast.SortByDirDesc
			}
		}
		orderList.Items = append(orderList.Items, sortBy)
	}
	return orderList
}

func (c *cc) convertWithClause(n *ast.With) *sqlcast.WithClause {
	clause := &sqlcast.WithClause{
		Ctes: &sqlcast.List{Items: []sqlcast.Node{}},
	}

	for _, cte := range n.CTEs {
		name := cte.Name.Name
		commonTableExpr := &sqlcast.CommonTableExpr{
			Ctename:  &name,
			Ctequery: c.convert(cte.QueryExpr),
		}

		// Note: ARRAY subqueries in Spanner must return either:
		// - A single column: ARRAY(SELECT col FROM table)
		// - A STRUCT: ARRAY(SELECT AS STRUCT col1 AS name1, col2 AS name2 FROM table)
		// Column aliases in CTE are not currently exposed by memefish API

		clause.Ctes.Items = append(clause.Ctes.Items, commonTableExpr)
	}

	return clause
}

func (c *cc) convertValuesInput(n *ast.ValuesInput) *sqlcast.SelectStmt {
	// Convert VALUES clause to a SELECT statement
	stmt := &sqlcast.SelectStmt{
		TargetList:  &sqlcast.List{},
		ValuesLists: &sqlcast.List{},
	}

	for _, row := range n.Rows {
		rowList := &sqlcast.List{Items: []sqlcast.Node{}}
		for _, expr := range row.Exprs {
			rowList.Items = append(rowList.Items, c.convert(expr))
		}
		stmt.ValuesLists.Items = append(stmt.ValuesLists.Items, rowList)
	}

	return stmt
}

// convertThenReturn converts Spanner's THEN RETURN clause to PostgreSQL-style RETURNING
func (c *cc) convertThenReturn(n *ast.ThenReturn) *sqlcast.List {
	if n == nil {
		return nil
	}

	returningList := &sqlcast.List{Items: []sqlcast.Node{}}

	// Convert each SelectItem to ResTarget
	for _, item := range n.Items {
		switch i := item.(type) {
		case *ast.Star:
			// THEN RETURN * -> RETURNING *
			// Must maintain the same ColumnRef wrapping pattern as SELECT *
			// to ensure consistent handling throughout the compiler.
			returningList.Items = append(returningList.Items, &sqlcast.ResTarget{
				Val: &sqlcast.ColumnRef{
					Fields: &sqlcast.List{
						Items: []sqlcast.Node{
							&sqlcast.A_Star{},
						},
					},
				},
			})
		case *ast.Alias:
			// THEN RETURN expr AS alias -> RETURNING expr AS alias
			var name *string
			if i.As != nil && i.As.Alias != nil {
				name = &i.As.Alias.Name
			}
			returningList.Items = append(returningList.Items, &sqlcast.ResTarget{
				Name: name,
				Val:  c.convert(i.Expr),
			})
		case *ast.ExprSelectItem:
			// THEN RETURN expr -> RETURNING expr
			returningList.Items = append(returningList.Items, &sqlcast.ResTarget{
				Val: c.convert(i.Expr),
			})
		default:
			// Handle other SelectItem types if needed
		}
	}

	return returningList
}

func parseTableName(path *ast.Path) *sqlcast.TableName {
	if path == nil || len(path.Idents) == 0 {
		name := "unknown"
		return &sqlcast.TableName{
			Name: name,
		}
	}

	if len(path.Idents) == 1 {
		name := identifier(path.Idents[0].Name)
		return &sqlcast.TableName{
			Name: name,
		}
	} else if len(path.Idents) == 2 {
		schema := identifier(path.Idents[0].Name)
		name := identifier(path.Idents[1].Name)
		return &sqlcast.TableName{
			Schema: schema,
			Name:   name,
		}
	} else if len(path.Idents) == 3 {
		catalog := identifier(path.Idents[0].Name)
		schema := identifier(path.Idents[1].Name)
		name := identifier(path.Idents[2].Name)
		return &sqlcast.TableName{
			Catalog: catalog,
			Schema:  schema,
			Name:    name,
		}
	}
	// Default case
	name := "unknown"
	return &sqlcast.TableName{
		Name: name,
	}
}

func convertTableNameToRangeVar(path *ast.Path) *sqlcast.RangeVar {
	if path == nil || len(path.Idents) == 0 {
		name := "unknown"
		return &sqlcast.RangeVar{
			Relname: &name,
		}
	}

	name := identifier(path.Idents[len(path.Idents)-1].Name)
	rangeVar := &sqlcast.RangeVar{
		Relname: &name,
	}

	// If there's a schema
	if len(path.Idents) >= 2 {
		schema := identifier(path.Idents[len(path.Idents)-2].Name)
		rangeVar.Schemaname = &schema
	}

	// If there's a catalog
	if len(path.Idents) >= 3 {
		catalog := identifier(path.Idents[0].Name)
		rangeVar.Catalogname = &catalog
	}

	return rangeVar
}

func (c *cc) convertSchemaType(t ast.SchemaType) string {
	switch schemaType := t.(type) {
	case *ast.ScalarSchemaType:
		// Convert scalar types
		switch schemaType.Name {
		case "BOOL":
			return "bool"
		case "INT64":
			return "int64"
		case "FLOAT32":
			return "float32"
		case "FLOAT64":
			return "float64"
		case "DATE":
			return "date"
		case "TIMESTAMP":
			return "timestamp"
		case "INTERVAL":
			return "interval"
		case "JSON":
			return "json"
		case "TOKENLIST":
			return "tokenlist"
		default:
			return strings.ToLower(string(schemaType.Name))
		}
	case *ast.SizedSchemaType:
		// Convert sized types (STRING, BYTES)
		if schemaType.Max {
			return strings.ToLower(string(schemaType.Name)) + "(max)"
		}
		return fmt.Sprintf("%s(%s)", strings.ToLower(string(schemaType.Name)), schemaType.Size.SQL())
	case *ast.ArraySchemaType:
		// Convert array types
		elemType := c.convertSchemaType(schemaType.Item)
		return elemType + "[]"
	default:
		// For other types, return a generic text type
		return "text"
	}
}

// Additional Expression Conversions
func (c *cc) convertCaseExpr(n *ast.CaseExpr) *sqlcast.CaseExpr {
	if n == nil {
		return nil
	}
	
	// Simplified: just return a TODO for now to test
	// return &sqlcast.TODO{}
	
	// Convert WHEN clauses
	var args []sqlcast.Node
	for _, when := range n.Whens {
		caseWhen := &sqlcast.CaseWhen{
			Expr:     c.convert(when.Cond),
			Result:   c.convert(when.Then),
			Location: int(when.When) - c.positionOffset,
		}
		args = append(args, caseWhen)
	}
	
	// Convert ELSE clause
	var defResult sqlcast.Node
	if n.Else != nil {
		defResult = c.convert(n.Else.Expr)
	}
	
	return &sqlcast.CaseExpr{
		Arg:       c.convert(n.Expr), // The expression after CASE (if any)
		Args:      &sqlcast.List{Items: args},
		Defresult: defResult,
		Location:  int(n.Case) - c.positionOffset,
	}
}

func (c *cc) convertCastExpr(n *ast.CastExpr) *sqlcast.TypeCast {
	if n == nil {
		return nil
	}
	
	return &sqlcast.TypeCast{
		Arg:      c.convert(n.Expr),
		TypeName: c.convertType(n.Type),
		Location: int(n.Cast) - c.positionOffset,
	}
}

func (c *cc) convertInExpr(n *ast.InExpr) sqlcast.Node {
	if n == nil {
		return nil
	}
	
	// Convert the IN expression based on the condition type
	var right sqlcast.Node
	switch cond := n.Right.(type) {
	case *ast.ValuesInCondition:
		// IN (value1, value2, ...)
		var items []sqlcast.Node
		for _, expr := range cond.Exprs {
			items = append(items, c.convert(expr))
		}
		right = &sqlcast.List{Items: items}
	case *ast.SubQueryInCondition:
		// IN (SELECT ...)
		right = c.convert(cond.Query)
	case *ast.UnnestInCondition:
		// IN UNNEST(array_expr)
		right = c.convert(cond.Expr)
	default:
		right = todo(cond)
	}
	
	// Create the appropriate comparison node
	if n.Not {
		// NOT IN expression
		return &sqlcast.A_Expr{
			Kind: sqlcast.A_Expr_Kind(0), // AEXPR_OP
			Name: &sqlcast.List{
				Items: []sqlcast.Node{
					&sqlcast.String{Str: "<>"},
					&sqlcast.String{Str: "ALL"},
				},
			},
			Lexpr:    c.convert(n.Left),
			Rexpr:    right,
			Location: -1,
		}
	}
	
	// IN expression  
	return &sqlcast.A_Expr{
		Kind: sqlcast.A_Expr_Kind_IN,
		Name: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: "="},
			},
		},
		Lexpr:    c.convert(n.Left),
		Rexpr:    right,
		Location: -1,
	}
}

func (c *cc) convertIsNullExpr(n *ast.IsNullExpr) *sqlcast.NullTest {
	if n == nil {
		return nil
	}
	
	var nullTestType sqlcast.NullTestType
	if n.Not {
		nullTestType = 1 // IS_NOT_NULL
	} else {
		nullTestType = 0 // IS_NULL
	}
	
	return &sqlcast.NullTest{
		Arg:          c.convert(n.Left),
		Nulltesttype: nullTestType,
		Location:     int(n.Null) - c.positionOffset,
	}
}

func (c *cc) convertType(t ast.Type) *sqlcast.TypeName {
	if t == nil {
		return nil
	}
	
	// Convert Spanner type to PostgreSQL-style type name
	var typeName string
	switch typ := t.(type) {
	case *ast.SimpleType:
		typeName = strings.ToLower(string(typ.Name))
	case *ast.ArrayType:
		// Handle array types
		elemType := c.convertType(typ.Item)
		if elemType != nil && len(elemType.Names.Items) > 0 {
			if str, ok := elemType.Names.Items[0].(*sqlcast.String); ok {
				typeName = str.Str + "[]"
			}
		}
	default:
		typeName = "unknown"
	}
	
	return &sqlcast.TypeName{
		Names: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: typeName},
			},
		},
	}
}

// Additional expression converters
func (c *cc) convertUnaryExpr(n *ast.UnaryExpr) sqlcast.Node {
	if n == nil {
		return nil
	}
	
	// Handle different unary operators
	switch n.Op {
	case ast.OpNot:
		// NOT operator
		return &sqlcast.BoolExpr{
			Xpr: nil,
			Boolop: sqlcast.BoolExprTypeNot,
			Args: &sqlcast.List{
				Items: []sqlcast.Node{c.convert(n.Expr)},
			},
			Location: int(n.OpPos) - c.positionOffset,
		}
	case ast.OpPlus, ast.OpMinus:
		// Unary plus/minus
		return &sqlcast.A_Expr{
			Kind: sqlcast.A_Expr_Kind(0), // AEXPR_OP
			Name: &sqlcast.List{
				Items: []sqlcast.Node{
					&sqlcast.String{Str: string(n.Op)},
				},
			},
			Rexpr:    c.convert(n.Expr),
			Location: int(n.OpPos) - c.positionOffset,
		}
	case ast.OpBitNot:
		// Bitwise NOT
		return &sqlcast.A_Expr{
			Kind: sqlcast.A_Expr_Kind(0), // AEXPR_OP
			Name: &sqlcast.List{
				Items: []sqlcast.Node{
					&sqlcast.String{Str: "~"},
				},
			},
			Rexpr:    c.convert(n.Expr),
			Location: int(n.OpPos) - c.positionOffset,
		}
	default:
		return todo("convertExpr", n)
	}
}

func (c *cc) convertCountStarExpr(n *ast.CountStarExpr) *sqlcast.FuncCall {
	if n == nil {
		return nil
	}
	
	// COUNT(*) is represented as a FuncCall with AggStar set to true
	return &sqlcast.FuncCall{
		Func: &sqlcast.FuncName{
			Name: "count",
		},
		AggStar:  true, // This tells sqlc that it's COUNT(*)
		Location: int(n.Count) - c.positionOffset,
	}
}

func (c *cc) convertBetweenExpr(n *ast.BetweenExpr) sqlcast.Node {
	if n == nil {
		return nil
	}
	
	// BETWEEN is converted to AND comparison: left >= rightStart AND left <= rightEnd
	geExpr := &sqlcast.A_Expr{
		Kind: sqlcast.A_Expr_Kind(0), // AEXPR_OP
		Name: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: ">="},
			},
		},
		Lexpr: c.convert(n.Left),
		Rexpr: c.convert(n.RightStart),
	}
	
	leExpr := &sqlcast.A_Expr{
		Kind: sqlcast.A_Expr_Kind(0), // AEXPR_OP
		Name: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: "<="},
			},
		},
		Lexpr: c.convert(n.Left),
		Rexpr: c.convert(n.RightEnd),
	}
	
	// Combine with AND (or NOT AND if NOT BETWEEN)
	andExpr := &sqlcast.BoolExpr{
		Boolop: sqlcast.BoolExprTypeAnd,
		Args: &sqlcast.List{
			Items: []sqlcast.Node{geExpr, leExpr},
		},
	}
	
	if n.Not {
		// NOT BETWEEN - wrap in NOT
		return &sqlcast.BoolExpr{
			Boolop: sqlcast.BoolExprTypeNot,
			Args: &sqlcast.List{
				Items: []sqlcast.Node{andExpr},
			},
		}
	}
	
	return andExpr
}

func (c *cc) convertExtractExpr(n *ast.ExtractExpr) *sqlcast.FuncCall {
	if n == nil {
		return nil
	}
	
	// EXTRACT(part FROM expr) is converted to a function call
	return &sqlcast.FuncCall{
		Func: &sqlcast.FuncName{
			Name: "extract",
		},
		Args: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: n.Part.Name}, // DATE_PART like YEAR, MONTH, etc.
				c.convert(n.Expr),
			},
		},
		Location: int(n.Extract) - c.positionOffset,
	}
}

func (c *cc) convertIfExpr(n *ast.IfExpr) *sqlcast.CaseExpr {
	if n == nil {
		return nil
	}
	
	// IF(cond, true_val, false_val) is converted to CASE WHEN cond THEN true_val ELSE false_val END
	// Note: This should produce the same AST structure as a native CASE expression
	caseWhen := &sqlcast.CaseWhen{
		Expr:     c.convert(n.Expr),
		Result:   c.convert(n.TrueResult),
		Location: int(n.If) - c.positionOffset,
	}
	
	return &sqlcast.CaseExpr{
		Arg: nil, // Simple CASE (no expression after CASE keyword)
		Args: &sqlcast.List{
			Items: []sqlcast.Node{caseWhen},
		},
		Defresult: c.convert(n.ElseResult),
		Location:  int(n.If) - c.positionOffset,
	}
}

func (c *cc) convertParenExpr(n *ast.ParenExpr) sqlcast.Node {
	if n == nil {
		return nil
	}
	
	// Parenthesized expressions don't have a direct equivalent in PostgreSQL AST
	// We just return the inner expression
	return c.convert(n.Expr)
}

func (c *cc) convertIfNullToCase(expr, nullResult sqlcast.Node, location int) sqlcast.Node {
	// IFNULL(expr, null_result) -> CASE WHEN expr IS NOT NULL THEN expr ELSE null_result END
	// Reordered to put the literal/constant in Defresult for better type inference
	nullTest := &sqlcast.NullTest{
		Arg:          expr,
		Nulltesttype: 1, // IS_NOT_NULL
		Location:     location,
	}
	
	caseWhen := &sqlcast.CaseWhen{
		Expr:     nullTest,
		Result:   expr,
		Location: location,
	}
	
	return &sqlcast.CaseExpr{
		Args: &sqlcast.List{
			Items: []sqlcast.Node{caseWhen},
		},
		Defresult: nullResult, // Put the literal/constant here for type inference
		Location:  location,
	}
}

func (c *cc) convertNullIfToCase(expr, exprToMatch sqlcast.Node, location int) sqlcast.Node {
	// NULLIF(expr, expr_to_match) -> CASE WHEN expr = expr_to_match THEN NULL ELSE expr END
	equalExpr := &sqlcast.A_Expr{
		Kind:     0, // AEXPR_OP
		Name:     &sqlcast.List{Items: []sqlcast.Node{&sqlcast.String{Str: "="}}},
		Lexpr:    expr,
		Rexpr:    exprToMatch,
		Location: location,
	}
	
	caseWhen := &sqlcast.CaseWhen{
		Expr:     equalExpr,
		Result:   &sqlcast.A_Const{Val: &sqlcast.Null{}},
		Location: location,
	}
	
	return &sqlcast.CaseExpr{
		Args: &sqlcast.List{
			Items: []sqlcast.Node{caseWhen},
		},
		Defresult: expr,
		Location:  location,
	}
}

// convertCoalesceToCase is no longer needed since we use CoalesceExpr directly

func (c *cc) convertFloatLiteral(n *ast.FloatLiteral) *sqlcast.A_Const {
	return &sqlcast.A_Const{
		Val: &sqlcast.Float{Str: n.Value},
	}
}

func (c *cc) convertBytesLiteral(n *ast.BytesLiteral) *sqlcast.A_Const {
	// Bytes literals in Spanner are like b'hello'
	return &sqlcast.A_Const{
		Val: &sqlcast.String{Str: string(n.Value)}, // Convert bytes to string
	}
}

func (c *cc) convertDateLiteral(n *ast.DateLiteral) sqlcast.Node {
	// DATE '2024-01-01' -> Direct TypeCast with proper type
	// Debug: Use the same conversion path as CAST() to ensure consistency
	if debug.Active {
		log.Printf("Converting DateLiteral: %s\n", n.Value.Value)
	}
	
	// Create the same TypeName structure as convertType would
	typeName := &sqlcast.TypeName{
		Names: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: "date"}, // Must match what convertType produces
			},
		},
	}
	
	result := &sqlcast.TypeCast{
		Arg: &sqlcast.A_Const{
			Val: &sqlcast.String{Str: n.Value.Value},
		},
		TypeName: typeName,
		Location: int(n.Date),
	}
	
	if debug.Active {
		log.Printf("Created TypeCast with TypeName: %v\n", typeName)
	}
	
	return result
}

func (c *cc) convertTimestampLiteral(n *ast.TimestampLiteral) sqlcast.Node {
	return &sqlcast.TypeCast{
		Arg: &sqlcast.A_Const{
			Val: &sqlcast.String{Str: n.Value.Value},
		},
		TypeName: &sqlcast.TypeName{
			Names: &sqlcast.List{
				Items: []sqlcast.Node{
					&sqlcast.String{Str: "timestamp"}, // lowercase
				},
			},
		},
		Location: int(n.Timestamp),
	}
}

func (c *cc) convertNumericLiteral(n *ast.NumericLiteral) sqlcast.Node {
	return &sqlcast.TypeCast{
		Arg: &sqlcast.A_Const{
			Val: &sqlcast.String{Str: n.Value.Value},
		},
		TypeName: &sqlcast.TypeName{
			Names: &sqlcast.List{
				Items: []sqlcast.Node{
					&sqlcast.String{Str: "numeric"}, // lowercase
				},
			},
		},
		Location: int(n.Numeric),
	}
}

func (c *cc) convertJSONLiteral(n *ast.JSONLiteral) sqlcast.Node {
	return &sqlcast.TypeCast{
		Arg: &sqlcast.A_Const{
			Val: &sqlcast.String{Str: n.Value.Value},
		},
		TypeName: &sqlcast.TypeName{
			Names: &sqlcast.List{
				Items: []sqlcast.Node{
					&sqlcast.String{Str: "json"}, // lowercase
				},
			},
		},
		Location: int(n.JSON),
	}
}

func (c *cc) convertScalarSubQuery(n *ast.ScalarSubQuery) sqlcast.Node {
	// Scalar subquery: (SELECT expr FROM ...)
	// Convert to SubLink with EXPR_SUBLINK type
	return &sqlcast.SubLink{
		SubLinkType: sqlcast.EXPR_SUBLINK,
		Subselect:   c.convert(n.Query),
		Location:    int(n.Lparen),
	}
}

func (c *cc) convertArraySubQuery(n *ast.ArraySubQuery) sqlcast.Node {
	// ARRAY(SELECT ...) 
	// Convert to ArrayExpr with subquery
	return &sqlcast.A_ArrayExpr{
		Elements: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.SubLink{
					SubLinkType: sqlcast.ARRAY_SUBLINK,
					Subselect:   c.convert(n.Query),
					Location:    int(n.Array),
				},
			},
		},
		Location: int(n.Array),
	}
}

func (c *cc) convertExistsSubQuery(n *ast.ExistsSubQuery) sqlcast.Node {
	// EXISTS(SELECT ...)
	// Convert to SubLink with EXISTS_SUBLINK type
	return &sqlcast.SubLink{
		SubLinkType: sqlcast.EXISTS_SUBLINK,
		Subselect:   c.convert(n.Query),
		Location:    int(n.Exists),
	}
}

func (c *cc) convertArrayLiteral(n *ast.ArrayLiteral) sqlcast.Node {
	// [1, 2, 3] -> A_ArrayExpr
	var elements []sqlcast.Node
	for _, elem := range n.Values {
		elements = append(elements, c.convert(elem))
	}
	return &sqlcast.A_ArrayExpr{
		Elements: &sqlcast.List{Items: elements},
	}
}

// spannerTypeToSQLType converts Spanner type names to sqlc internal types
func spannerTypeToSQLType(spannerType string) string {
	switch strings.ToUpper(spannerType) {
	case "INT64":
		return "int"
	case "STRING":
		return "text"
	case "BOOL", "BOOLEAN":
		return "bool"
	case "FLOAT64":
		return "float"
	case "DATE":
		return "date"
	case "TIMESTAMP":
		return "timestamp"
	case "NUMERIC":
		return "numeric"
	case "JSON", "JSONB":
		return "json"
	case "BYTES":
		return "bytea"
	default:
		return ""
	}
}

func (c *cc) convertTypedStructLiteral(n *ast.TypedStructLiteral) sqlcast.Node {
	// STRUCT<x INT64, y STRING>(1, 'hello') -> RowExpr
	// Convert to ROW expression which is similar to STRUCT
	//
	// NOTE: Type information is preserved in Colnames using "fieldname:TYPE" format.
	// This enables proper type inference for struct field access in the Spanner engine.
	// For example, STRUCT<id INT64, name STRING>(42, 'Alice').name will correctly
	// infer STRING type for the 'name' field.
	var args []sqlcast.Node
	var colnames []sqlcast.Node
	
	// Convert values
	for _, val := range n.Values {
		args = append(args, c.convert(val))
	}
	
	// Store field names and types in Colnames
	// This preserves the type information for later type inference
	for _, field := range n.Fields {
		// Store field name and type as a composite structure
		// We encode type info in the field name for retrieval
		colnames = append(colnames, &sqlcast.String{
			Str: field.Ident.Name + ":" + string(field.Type.(*ast.SimpleType).Name),
		})
	}
	
	return &sqlcast.RowExpr{
		Args:      &sqlcast.List{Items: args},
		Colnames:  &sqlcast.List{Items: colnames},
		RowFormat: sqlcast.CoercionForm(0), // COERCE_EXPLICIT_CALL equivalent
		Location:  int(n.Struct),
	}
}

func (c *cc) convertTypelessStructLiteral(n *ast.TypelessStructLiteral) sqlcast.Node {
	// STRUCT(1 AS id, 'hello' AS name) -> RowExpr
	//
	// Type inference capabilities:
	// - Works: Literal values (strings, numbers, dates, etc.)
	//   Example: STRUCT(1 AS id, 'text' AS name).name returns STRING
	// - Doesn't work: Column references from tables
	//   Example: STRUCT(u.id AS uid, u.name AS uname).uname returns interface{}/any
	//
	// Technical limitation: The AST conversion phase doesn't have access to the catalog/schema,
	// so we cannot look up column types from table definitions. Type resolution happens
	// later in the compiler pipeline, but by then the STRUCT field information is lost.
	//
	// Workaround: Use typed STRUCT literals to explicitly specify field types:
	//   STRUCT<uid INT64, uname STRING>(u.id, u.name).uname returns STRING correctly
	var args []sqlcast.Node
	var colnames []sqlcast.Node
	
	for _, val := range n.Values {
		// Handle TypelessStructLiteralArg interface
		switch arg := val.(type) {
		case *ast.ExprArg:
			args = append(args, c.convert(arg.Expr))
			// No alias, no field name
			colnames = append(colnames, &sqlcast.String{Str: ""})
		case *ast.Alias:
			// Handle alias within struct
			args = append(args, c.convert(arg.Expr))
			// Store field name with inferred type from the expression
			fieldName := arg.As.Alias.Name
			typeHint := ""
			// Infer type from the expression
			switch expr := arg.Expr.(type) {
			case *ast.IntLiteral:
				typeHint = "INT64"
			case *ast.StringLiteral:
				typeHint = "STRING"
			case *ast.BoolLiteral:
				typeHint = "BOOL"
			case *ast.FloatLiteral:
				typeHint = "FLOAT64"
			case *ast.DateLiteral:
				typeHint = "DATE"
			case *ast.TimestampLiteral:
				typeHint = "TIMESTAMP"
			case *ast.NumericLiteral:
				typeHint = "NUMERIC"
			case *ast.JSONLiteral:
				typeHint = "JSON"
			case *ast.BytesLiteral:
				typeHint = "BYTES"
			default:
				// LIMITATION: For column references (Path nodes) and other complex expressions,
				// we cannot infer the type at AST conversion time because we don't have access
				// to the catalog/schema. Type will fall back to interface{}/any.
				// Workaround: Use typed STRUCT literals to explicitly specify field types.
				_ = expr
			}
			if typeHint != "" {
				colnames = append(colnames, &sqlcast.String{
					Str: fieldName + ":" + typeHint,
				})
			} else {
				colnames = append(colnames, &sqlcast.String{Str: fieldName})
			}
		default:
			args = append(args, todo(val))
			colnames = append(colnames, &sqlcast.String{Str: ""})
		}
	}
	
	return &sqlcast.RowExpr{
		Args:      &sqlcast.List{Items: args},
		Colnames:  &sqlcast.List{Items: colnames},
		RowFormat: sqlcast.CoercionForm(0), // COERCE_EXPLICIT_CALL equivalent
		Location:  int(n.Struct),
	}
}

func (c *cc) convertTupleStructLiteral(n *ast.TupleStructLiteral) sqlcast.Node {
	// (1, 'hello') as tuple -> RowExpr  
	var args []sqlcast.Node
	for _, val := range n.Values {
		args = append(args, c.convert(val))
	}
	
	return &sqlcast.RowExpr{
		Args: &sqlcast.List{Items: args},
		RowFormat: sqlcast.CoercionForm(1), // COERCE_IMPLICIT_CAST equivalent
		Location: int(n.Lparen),
	}
}

func (c *cc) convertIntervalLiteralSingle(n *ast.IntervalLiteralSingle) sqlcast.Node {
	// INTERVAL 5 DAY -> TypeCast with interval type
	// Convert the value and create an interval type cast
	typeName := &sqlcast.TypeName{
		Names: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: "interval"},
			},
		},
	}
	
	// Combine value with date part as a string for the interval
	// e.g., "5 DAY"
	var intervalStr string
	// n.Value is IntValue interface - convert it
	switch v := n.Value.(type) {
	case *ast.IntLiteral:
		intervalStr = v.Value
	case *ast.Param:
		// Handle parameter case
		return c.convert(v)
	default:
		intervalStr = "0"
	}
	
	// Add the date/time part
	intervalStr += " " + string(n.DateTimePart)
	
	return &sqlcast.TypeCast{
		Arg: &sqlcast.A_Const{
			Val: &sqlcast.String{Str: intervalStr},
		},
		TypeName: typeName,
		Location: int(n.Interval),
	}
}

func (c *cc) convertSelectorExpr(n *ast.SelectorExpr) sqlcast.Node {
	// STRUCT(...).field -> A_Indirection with field name
	// Convert to A_Indirection to represent field access
	// 
	// NOTE: Type inference for struct field access works for:
	// - Typed STRUCT literals: STRUCT<id INT64, name STRING>(...).name
	// - Untyped STRUCT with literal values: STRUCT(1 as id, 'text' as name).name
	// 
	// LIMITATION: Type inference doesn't work for untyped STRUCT with column references:
	// - STRUCT(u.id as uid, u.name as uname).uname will return interface{}/any
	// - Workaround: Use typed STRUCT literals to specify field types explicitly
	return &sqlcast.A_Indirection{
		Arg: c.convert(n.Expr),
		Indirection: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: n.Ident.Name}, // Field name as string
			},
		},
	}
}

func (c *cc) convertUnnest(n *ast.Unnest) sqlcast.Node {
	// UNNEST converts an array to a table-valued function result
	// It can be used in FROM clause with optional WITH OFFSET
	
	// Convert to RangeFunction for use in FROM clause
	rangeFunc := &sqlcast.RangeFunction{
		Functions: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.List{
					Items: []sqlcast.Node{
						&sqlcast.FuncCall{
							Func: &sqlcast.FuncName{
								Names: &sqlcast.List{
									Items: []sqlcast.Node{
										&sqlcast.String{Str: "unnest"},
									},
								},
							},
							Args: &sqlcast.List{
								Items: []sqlcast.Node{
									c.convert(n.Expr),
								},
							},
						},
					},
				},
			},
		},
	}
	
	// Handle WITH OFFSET clause
	// In PostgreSQL, this is represented as WITH ORDINALITY
	if n.WithOffset != nil {
		rangeFunc.Ordinality = true
		
		// If WITH OFFSET has an alias, it becomes a column alias
		// Note: PostgreSQL's WITH ORDINALITY adds a column named "ordinality" by default
		// Spanner's WITH OFFSET AS alias allows custom naming
		if n.WithOffset.As != nil && n.WithOffset.As.Alias != nil {
			// The offset column alias is handled separately in Spanner
			// but PostgreSQL doesn't have direct support for renaming the ordinality column
			// in the UNNEST clause itself
			if debug.Active {
				log.Printf("spanner.convertUnnest: WITH OFFSET AS alias - ordinality column aliasing may need manual handling\n")
			}
		}
	}
	
	// Handle alias for the value column
	if n.As != nil && n.As.Alias != nil {
		alias := identifier(n.As.Alias.Name)
		rangeFunc.Alias = &sqlcast.Alias{
			Aliasname: &alias,
		}
	}
	
	return rangeFunc
}

func (c *cc) convertIndexExpr(n *ast.IndexExpr) sqlcast.Node {
	// array[index] or array[OFFSET(n)] or array[ORDINAL(n)]
	// Convert to A_Indirection or A_ArrayExpr subscript
	return &sqlcast.A_Indirection{
		Arg: c.convert(n.Expr),
		Indirection: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.A_Indices{
					Lidx: c.convert(n.Index),
				},
			},
		},
	}
}

func (c *cc) convertIntervalLiteralRange(n *ast.IntervalLiteralRange) sqlcast.Node {
	// INTERVAL '1-2' YEAR TO MONTH -> TypeCast with interval type
	typeName := &sqlcast.TypeName{
		Names: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.String{Str: "interval"},
			},
		},
	}
	
	// Get the value string (n.Value is already *StringLiteral)
	intervalStr := n.Value.Value
	
	// Add the range parts (e.g., "YEAR TO MONTH")
	intervalStr += " " + string(n.StartingDateTimePart) + 
	               " TO " + string(n.EndingDateTimePart)
	
	return &sqlcast.TypeCast{
		Arg: &sqlcast.A_Const{
			Val: &sqlcast.String{Str: intervalStr},
		},
		TypeName: typeName,
		Location: int(n.Interval),
	}
}
