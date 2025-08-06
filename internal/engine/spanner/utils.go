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

// parameterExtractor implements ast.Visitor to collect parameters
type parameterExtractor struct {
	params []Parameter
}

func (e *parameterExtractor) Visit(node ast.Node) ast.Visitor {
	if param, ok := node.(*ast.Param); ok {
		e.params = append(e.params, Parameter{
			Name:     param.Name,
			Position: param.Pos(),
		})
	}
	return e
}

func (e *parameterExtractor) VisitMany(nodes []ast.Node) ast.Visitor {
	return e
}

func (e *parameterExtractor) Field(name string) ast.Visitor {
	return e
}

func (e *parameterExtractor) Index(index int) ast.Visitor {
	return e
}

// ExtractParameters extracts all @param style parameters from an AST node
func ExtractParameters(node ast.Node) []Parameter {
	extractor := &parameterExtractor{}
	ast.Walk(node, extractor)
	return extractor.params
}