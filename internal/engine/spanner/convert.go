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
	case *ast.CallExpr:
		return c.convertCallExpr(node)
	case *ast.Param:
		return c.convertParam(node)
	case *ast.DefaultExpr:
		return c.convertDefaultExpr(node)

	// Other nodes
	case *ast.Star:
		// For standalone star, wrap in ColumnRef to match PostgreSQL structure
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
	stmt := &sqlcast.InsertStmt{
		Relation:      convertTableNameToRangeVar(n.TableName),
		Cols:          &sqlcast.List{Items: []sqlcast.Node{}},
		SelectStmt:    nil,
		ReturningList: &sqlcast.List{Items: []sqlcast.Node{}},
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
	stmt := &sqlcast.UpdateStmt{
		Relations:     &sqlcast.List{Items: []sqlcast.Node{}},
		TargetList:    &sqlcast.List{Items: []sqlcast.Node{}},
		WhereClause:   nil,
		FromClause:    &sqlcast.List{Items: []sqlcast.Node{}},
		ReturningList: &sqlcast.List{Items: []sqlcast.Node{}},
		WithClause:    nil,
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
	stmt := &sqlcast.DeleteStmt{
		Relations:     &sqlcast.List{Items: []sqlcast.Node{}},
		UsingClause:   &sqlcast.List{Items: []sqlcast.Node{}},
		WhereClause:   nil,
		ReturningList: &sqlcast.List{Items: []sqlcast.Node{}},
		WithClause:    nil,
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
		TargetList:   &sqlcast.List{Items: []sqlcast.Node{}},
		FromClause:   &sqlcast.List{Items: []sqlcast.Node{}},
		WhereClause:  nil,
		GroupClause:  &sqlcast.List{Items: []sqlcast.Node{}},
		SortClause:   &sqlcast.List{Items: []sqlcast.Node{}},
		LimitCount:   nil,
		LimitOffset:  nil,
		ValuesLists:  &sqlcast.List{Items: []sqlcast.Node{}},
	}

	// Convert SELECT items
	for _, item := range n.Results {
		switch i := item.(type) {
		case *ast.Star:
			// Wrap A_Star in ColumnRef to match PostgreSQL's AST structure
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
	if n.GroupBy != nil {
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
		// Use the last identifier as the function name
		funcName = n.Func.Idents[len(n.Func.Idents)-1].Name
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
		return &sqlcast.RangeVar{
			Relname: &name,
		}
	default:
		return todo(n)
	}
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
		return &sqlcast.List{Items: []sqlcast.Node{}}
	}

	returningList := &sqlcast.List{Items: []sqlcast.Node{}}

	// Convert each SelectItem to ResTarget
	for _, item := range n.Items {
		switch i := item.(type) {
		case *ast.Star:
			// THEN RETURN * -> RETURNING *
			// Wrap A_Star in ColumnRef to match PostgreSQL's AST structure
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