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

	"github.com/sqlc-dev/sqlc/internal/debug"
	sqlcast "github.com/sqlc-dev/sqlc/internal/sql/ast"
)

type cc struct {
	paramCount     int
	paramMap       map[string]int // Map parameter names to their position
	paramsByNum    map[int]string // Map position to parameter name
	positionOffset int            // Offset to adjust AST positions to file positions
}

func todo(n ast.Node) *sqlcast.TODO {
	if debug.Active {
		log.Printf("spanner.convert: Unknown node type %T\n", n)
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
		return c.convertIfExpr(node)
	case *ast.ParenExpr:
		return c.convertParenExpr(node)
	case *ast.Param:
		return c.convertParam(node)
	case *ast.DefaultExpr:
		return c.convertDefaultExpr(node)

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
		return todo(n)
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

	// TODO: Convert table constraints and other features
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

	// Convert SELECT items
	for _, item := range n.Results {
		switch i := item.(type) {
		case *ast.Star:
			// SELECT * must be wrapped: ResTarget -> ColumnRef -> A_Star
			// This three-level structure matches PostgreSQL and enables
			// the hasStarRef() check in output_columns.go to work correctly.
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

func (c *cc) convertCallExpr(n *ast.CallExpr) *sqlcast.FuncCall {
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

	funcCall := &sqlcast.FuncCall{
		Func: &sqlcast.FuncName{
			Name: funcName,
		},
		Args: &sqlcast.List{Items: []sqlcast.Node{}},
	}

	// Convert arguments
	for _, arg := range n.Args {
		// Handle different arg types
		switch a := arg.(type) {
		case *ast.ExprArg:
			funcCall.Args.Items = append(funcCall.Args.Items, c.convert(a.Expr))
		default:
			// Handle other arg types
		}
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
		return subquery
	default:
		return todo(n)
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

		// TODO: Handle column aliases when available in memefish API

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
		return todo(n)
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
				&sqlcast.String{Str: string(n.Part)}, // DATE_PART like YEAR, MONTH, etc.
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
	return &sqlcast.CaseExpr{
		Args: &sqlcast.List{
			Items: []sqlcast.Node{
				&sqlcast.CaseWhen{
					Expr:   c.convert(n.Cond),
					Result: c.convert(n.TrueResult),
				},
			},
		},
		Defresult: c.convert(n.FalseResult),
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
