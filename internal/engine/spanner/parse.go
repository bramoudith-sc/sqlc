package spanner

// TODO: Future enhancements for Cloud Spanner engine:
// 1. Analyzer support - Connect to Cloud Spanner to fetch schema information from INFORMATION_SCHEMA tables
// 2. sql_package options - Support both cloud.google.com/go/spanner and database/sql (go-sql-spanner)
// 3. Transaction isolation levels - Support Spanner-specific transaction modes (read-only, stale reads)

import (
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/cloudspannerecosystem/memefish"

	"github.com/sqlc-dev/sqlc/internal/source"
	sqlcast "github.com/sqlc-dev/sqlc/internal/sql/ast"
	"github.com/sqlc-dev/sqlc/internal/sql/sqlerr"
)

func NewParser() *Parser {
	return &Parser{}
}

type Parser struct{}

func (p *Parser) Parse(r io.Reader) ([]sqlcast.Statement, error) {
	blob, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	content := string(blob)
	
	// First split raw statements
	rawStatements, err := memefish.SplitRawStatements("<input>", content)
	if err != nil {
		return nil, convertError(err)
	}
	
	var stmts []sqlcast.Statement
	
	// Process each statement and look for metadata comments before it
	for i, rawStmt := range rawStatements {
		if rawStmt == nil || rawStmt.Statement == "" {
			continue
		}
		
		// Skip empty statements
		trimmed := strings.TrimSpace(rawStmt.Statement)
		if trimmed == "" {
			continue
		}
		
		// Check if this statement has SQL (not just comments)
		hasSQL := false
		for _, line := range strings.Split(rawStmt.Statement, "\n") {
			trimmedLine := strings.TrimSpace(line)
			if trimmedLine != "" && !strings.HasPrefix(trimmedLine, "--") {
				hasSQL = true
				break
			}
		}
		if !hasSQL {
			continue
		}
		
		// Find the start position including any metadata comment
		stmtStart := int(rawStmt.Pos)
		
		// Look backwards from the statement start to find metadata comments
		if stmtStart > 0 {
			// Look for metadata comment in the lines immediately before the statement
			searchEnd := stmtStart
			searchStart := stmtStart - 1
			
			// Find start of the line or lines before the statement
			lineCount := 0
			for searchStart > 0 && lineCount < 5 { // Look up to 5 lines back
				if content[searchStart] == '\n' {
					lineCount++
				}
				searchStart--
			}
			
			// Extract the text before the statement
			beforeText := content[searchStart:searchEnd]
			
			// Look for the last occurrence of "-- name:" in this text
			if idx := strings.LastIndex(beforeText, "-- name:"); idx >= 0 {
				// Find the start of the line containing "-- name:"
				lineStart := searchStart + idx
				for lineStart > 0 && lineStart > searchStart && content[lineStart-1] != '\n' {
					lineStart--
				}
				stmtStart = lineStart
			}
		}
		
		// Calculate the full statement including metadata
		stmtEnd := int(rawStmt.End)
		if i < len(rawStatements)-1 && rawStatements[i+1] != nil {
			// Check if there's a semicolon between statements
			nextStart := int(rawStatements[i+1].Pos)
			for j := stmtEnd; j < nextStart && j < len(content); j++ {
				if content[j] == ';' {
					stmtEnd = j + 1
					break
				} else if content[j] != ' ' && content[j] != '\t' && content[j] != '\n' && content[j] != '\r' {
					break
				}
			}
		} else {
			// Last statement, check for trailing semicolon
			for j := stmtEnd; j < len(content); j++ {
				if content[j] == ';' {
					stmtEnd = j + 1
					break
				} else if content[j] != ' ' && content[j] != '\t' && content[j] != '\n' && content[j] != '\r' {
					break
				}
			}
		}
		
		// Extract the full statement text
		fullStatement := content[stmtStart:stmtEnd]
		
		// Find where the actual SQL starts within the full statement
		sqlStartOffset := 0
		for _, line := range strings.Split(fullStatement, "\n") {
			trimmedLine := strings.TrimSpace(line)
			if trimmedLine != "" && !strings.HasPrefix(trimmedLine, "--") {
				sqlStartOffset = strings.Index(fullStatement, line)
				break
			}
		}
		
		// Parse just the SQL portion
		sqlOnly := fullStatement[sqlStartOffset:]
		// Remove trailing semicolon for parsing
		sqlOnly = strings.TrimRight(sqlOnly, "; \t\n\r")
		
		node, err := memefish.ParseStatement("<input>", sqlOnly)
		if err != nil {
			return nil, convertError(err)
		}
		
		converter := &cc{
			paramMap:    make(map[string]int),
			paramsByNum: make(map[int]string),
			// Offset to adjust positions from sqlOnly to original file positions
			positionOffset: stmtStart + sqlStartOffset,
		}
		out := converter.convert(node)
		if _, ok := out.(*sqlcast.TODO); ok {
			continue
		}
		
		stmts = append(stmts, sqlcast.Statement{
			Raw: &sqlcast.RawStmt{
				Stmt:         out,
				StmtLocation: stmtStart,
				StmtLen:      stmtEnd - stmtStart,
			},
		})
	}
	
	return stmts, nil
}

// CommentSyntax returns the comment syntax supported by Spanner
func (p *Parser) CommentSyntax() source.CommentSyntax {
	return source.CommentSyntax{
		Dash:      true,
		SlashStar: true,
		Hash:      true,
	}
}

// convertError converts memefish errors to sqlc errors
func convertError(err error) error {
	if err == nil {
		return nil
	}
	
	// Check if it's a memefish.Error type
	if memefishErr, ok := err.(*memefish.Error); ok {
		line := 1
		col := 1
		if memefishErr.Position != nil {
			// Convert 0-based to 1-based line/column numbers
			line = memefishErr.Position.Line + 1
			col = memefishErr.Position.Column + 1
		}
		return &sqlerr.Error{
			Message: "syntax error",
			Err:     errors.New(memefishErr.Message),
			Line:    line,
			Column:  col,
		}
	}
	
	// MultiError might not exist or have different structure
	// For now, just handle generic errors
	
	// Generic error
	return fmt.Errorf("parse error: %v", err)
}