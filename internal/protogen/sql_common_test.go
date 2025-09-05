package protogen

import (
	"fmt"
	"strings"
	"testing"
)

func TestBuildParameterizedQueryWithOptions(t *testing.T) {
	tests := []struct {
		name         string
		database     string
		table        string
		withFinal    bool
		expectedFrom string
	}{
		{
			name:         "Without FINAL option",
			database:     "mydb",
			table:        "mytable",
			withFinal:    false,
			expectedFrom: "FROM mydb.mytable",
		},
		{
			name:         "With FINAL option",
			database:     "mydb",
			table:        "mytable",
			withFinal:    true,
			expectedFrom: "FROM mydb.mytable FINAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock query builder
			qb := &mockQueryBuilder{}

			// Build options
			var options []mockQueryOption
			if tt.withFinal {
				options = append(options, mockWithFinal())
			}

			// Build the query
			sql := mockBuildParameterizedQuery(tt.database, tt.table, qb, "", 0, 0, options...)

			// Check if the FROM clause is correct
			if !strings.Contains(sql.Query, tt.expectedFrom) {
				t.Errorf("Expected query to contain '%s', but got: %s", tt.expectedFrom, sql.Query)
			}
		})
	}
}

func TestBuildParameterizedQueryWithOrderByAndOptions(t *testing.T) {
	tests := []struct {
		name          string
		database      string
		table         string
		orderBy       string
		withFinal     bool
		expectedFrom  string
		expectedOrder string
	}{
		{
			name:          "Without FINAL, with ORDER BY",
			database:      "mydb",
			table:         "mytable",
			orderBy:       " ORDER BY id DESC",
			withFinal:     false,
			expectedFrom:  "FROM mydb.mytable",
			expectedOrder: "ORDER BY id DESC",
		},
		{
			name:          "With FINAL and ORDER BY",
			database:      "mydb",
			table:         "mytable",
			orderBy:       " ORDER BY id DESC",
			withFinal:     true,
			expectedFrom:  "FROM mydb.mytable FINAL",
			expectedOrder: "ORDER BY id DESC",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock query builder
			qb := &mockQueryBuilder{}

			// Build options
			var options []mockQueryOption
			if tt.withFinal {
				options = append(options, mockWithFinal())
			}

			// Build the query
			sql := mockBuildParameterizedQuery(tt.database, tt.table, qb, tt.orderBy, 0, 0, options...)

			// Check if the FROM clause is correct
			if !strings.Contains(sql.Query, tt.expectedFrom) {
				t.Errorf("Expected query to contain '%s', but got: %s", tt.expectedFrom, sql.Query)
			}

			// Check if the ORDER BY clause is present
			if tt.orderBy != "" && !strings.Contains(sql.Query, tt.expectedOrder) {
				t.Errorf("Expected query to contain '%s', but got: %s", tt.expectedOrder, sql.Query)
			}
		})
	}
}

// Mock types to simulate the generated code

type mockQueryBuilder struct{}

func (qb *mockQueryBuilder) GetWhereClause() string {
	return ""
}

func (qb *mockQueryBuilder) GetArgs() []interface{} {
	return nil
}

type mockSQLQuery struct {
	Query string
	Args  []interface{}
}

type mockQueryOptions struct {
	AddFinal bool
}

type mockQueryOption func(*mockQueryOptions)

func mockWithFinal() mockQueryOption {
	return func(opts *mockQueryOptions) {
		opts.AddFinal = true
	}
}

func mockBuildParameterizedQuery(database, table string, qb *mockQueryBuilder, orderByClause string, limit, offset uint32, options ...mockQueryOption) mockSQLQuery {
	// Apply options
	opts := &mockQueryOptions{}
	for _, opt := range options {
		opt(opts)
	}

	// Build FROM clause with optional FINAL
	fromClause := database + "." + table
	if opts.AddFinal {
		fromClause += " FINAL"
	}

	query := "SELECT * FROM " + fromClause

	// Add WHERE clause
	query += qb.GetWhereClause()

	// Add ORDER BY clause
	query += orderByClause

	// Add LIMIT and OFFSET
	if limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", limit)
		if offset > 0 {
			query += fmt.Sprintf(" OFFSET %d", offset)
		}
	}

	return mockSQLQuery{
		Query: query,
		Args:  qb.GetArgs(),
	}
}
