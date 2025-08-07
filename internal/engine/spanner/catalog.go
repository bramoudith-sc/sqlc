package spanner

import (
	"github.com/sqlc-dev/sqlc/internal/sql/catalog"
)

func NewCatalog() *catalog.Catalog {
	// Spanner doesn't have a default schema concept like PostgreSQL
	// but we'll use empty string as default
	def := ""
	return &catalog.Catalog{
		DefaultSchema: def,
		Schemas: []*catalog.Schema{
			defaultSchema(def),
		},
		Extensions: map[string]struct{}{},
	}
}
