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
	"github.com/cloudspannerecosystem/memefish/token"

	"github.com/sqlc-dev/sqlc/internal/source"
	sqlcast "github.com/sqlc-dev/sqlc/internal/sql/ast"
	"github.com/sqlc-dev/sqlc/internal/sql/sqlerr"
)

func NewParser() *Parser {
	return &Parser{}
}

type Parser struct{}

// statementWithMetadata represents a SQL statement with its metadata comments
type statementWithMetadata struct {
	sql         string    // The SQL statement text (without comments)
	sqlStartPos token.Pos // Start position of actual SQL (for offset calculation)
	startPos    token.Pos // Start position including metadata comments
	endPos      token.Pos // End position including semicolon
	comments    []string  // Comments preceding the statement
}

// splitStatements splits SQL text into statements using Lexer directly
func (p *Parser) splitStatements(filename, content string) ([]statementWithMetadata, error) {
	lexer := &memefish.Lexer{
		File: &token.File{
			FilePath: filename,
			Buffer:   content,
		},
	}
	
	var statements []statementWithMetadata
	var currentComments []string
	var stmtStartPos token.Pos = -1
	var firstTokenPos token.Pos = -1
	
	for {
		err := lexer.NextToken()
		if err != nil {
			return nil, convertError(err)
		}
		
		tok := lexer.Token
		
		// Collect comments from this token
		if len(tok.Comments) > 0 {
			for _, comment := range tok.Comments {
				commentText := content[comment.Pos:comment.End]
				currentComments = append(currentComments, commentText)
				// Track the earliest position including comments
				if stmtStartPos == -1 || comment.Pos < stmtStartPos {
					stmtStartPos = comment.Pos
				}
			}
		}
		
		// Track the first non-semicolon token position for SQL extraction
		if tok.Kind != ";" && tok.Kind != token.TokenEOF && firstTokenPos == -1 {
			firstTokenPos = tok.Pos
			// If we haven't seen any comments yet, start from this token
			if stmtStartPos == -1 {
				stmtStartPos = tok.Pos
			}
		}
		
		// Check for statement terminator (semicolon or EOF)
		if tok.Kind == ";" || tok.Kind == token.TokenEOF {
			// Add statement if we have content
			if firstTokenPos != -1 && stmtStartPos != -1 {
				stmtSQL := content[firstTokenPos:tok.Pos]
				stmtSQL = strings.TrimSpace(stmtSQL)
				
				if stmtSQL != "" {
					// For semicolon, include it in endPos; for EOF, use tok.Pos
					endPos := tok.Pos
					if tok.Kind == ";" {
						endPos = tok.End
					}
					
					statements = append(statements, statementWithMetadata{
						sql:         stmtSQL,
						sqlStartPos: firstTokenPos,
						startPos:    stmtStartPos,
						endPos:      endPos,
						comments:    currentComments,
					})
				}
			}
			
			// If EOF, we're done
			if tok.Kind == token.TokenEOF {
				break
			}
			
			// Reset for next statement (only for semicolon)
			currentComments = nil
			stmtStartPos = -1
			firstTokenPos = -1
		}
	}
	
	return statements, nil
}

func (p *Parser) Parse(r io.Reader) ([]sqlcast.Statement, error) {
	blob, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	content := string(blob)
	
	// Split statements using Lexer
	statements, err := p.splitStatements("<input>", content)
	if err != nil {
		return nil, err
	}
	
	var stmts []sqlcast.Statement
	
	for _, stmt := range statements {
		// Skip empty statements
		if strings.TrimSpace(stmt.sql) == "" {
			continue
		}
		
		// Parse the SQL statement
		node, err := memefish.ParseStatement("<input>", stmt.sql)
		if err != nil {
			return nil, convertError(err)
		}
		
		converter := &cc{
			paramMap:    make(map[string]int),
			paramsByNum: make(map[int]string),
			// Offset to adjust positions from parsed SQL to original file positions
			positionOffset: int(stmt.sqlStartPos),
		}
		out := converter.convert(node)
		if _, ok := out.(*sqlcast.TODO); ok {
			continue
		}
		
		stmts = append(stmts, sqlcast.Statement{
			Raw: &sqlcast.RawStmt{
				Stmt:         out,
				StmtLocation: int(stmt.startPos), // Already includes metadata comments
				StmtLen:      int(stmt.endPos) - int(stmt.startPos),
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
	
	// Check if it's a memefish.MultiError type
	if multiErr, ok := err.(*memefish.MultiError); ok {
		if len(*multiErr) > 0 {
			firstErr := (*multiErr)[0]
			line := 1
			col := 1
			if firstErr.Position != nil {
				line = firstErr.Position.Line + 1
				col = firstErr.Position.Column + 1
			}
			return &sqlerr.Error{
				Message: "syntax error",
				Err:     errors.New(firstErr.Message),
				Line:    line,
				Column:  col,
			}
		}
	}
	
	// For other error types, wrap as-is
	return fmt.Errorf("spanner parser error: %w", err)
}