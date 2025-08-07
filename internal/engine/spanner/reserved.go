package spanner

import (
	"github.com/cloudspannerecosystem/memefish/token"
)

// IsReservedKeyword checks if a string is a reserved keyword in Spanner SQL.
// It uses memefish's built-in IsKeyword function which perfectly matches
// the official Cloud Spanner reserved keywords list.
//
// This implementation is maintenance-free as it automatically stays in sync
// with memefish's keyword definitions, which are based on the official
// Spanner SQL specification.
func (p *Parser) IsReservedKeyword(s string) bool {
	// token.IsKeyword perfectly matches the official reserved keywords list
	// It returns true for all reserved keywords (SELECT, FROM, WHERE, etc.)
	// and false for context-dependent keywords (INSERT, UPDATE, DELETE, TABLE, etc.)
	// which can be used as identifiers
	return token.IsKeyword(s)
}