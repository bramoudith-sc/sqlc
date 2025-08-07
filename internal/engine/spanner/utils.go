package spanner

import (
	"github.com/cloudspannerecosystem/memefish/ast"
	"github.com/cloudspannerecosystem/memefish/token"
)

// Parameter represents a query parameter with its name and position
type Parameter struct {
	Name     string
	Position token.Pos
}

// ExtractParameters extracts all @param style parameters from an AST node
// Uses ast.Inspect for simpler implementation
func ExtractParameters(node ast.Node) []Parameter {
	var params []Parameter
	
	ast.Inspect(node, func(n ast.Node) bool {
		if param, ok := n.(*ast.Param); ok {
			params = append(params, Parameter{
				Name:     param.Name,
				Position: param.Pos(),
			})
		}
		return true // Continue traversing
	})
	
	return params
}