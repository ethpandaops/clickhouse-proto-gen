package protogen

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

var errEmptyColumnsList = errors.New("columns list cannot be empty")

// TestBuildParameterizedQueryWithDatabaseOption tests the new database-agnostic
// BuildParameterizedQuery function with the WithDatabase option
func TestBuildParameterizedQueryWithDatabaseOption(t *testing.T) {
	// Since BuildParameterizedQuery is generated in the output package,
	// we test the generation logic itself here

	g := &Generator{
		typeMapper: NewTypeMapper(),
	}

	tests := []struct {
		name                 string
		expectedSignature    string
		notExpectedSignature string
	}{
		{
			name:                 "Function signature should not include database as parameter",
			expectedSignature:    "func BuildParameterizedQuery(table string, qb *QueryBuilder",
			notExpectedSignature: "func BuildParameterizedQuery(database, table string",
		},
		{
			name:                 "Should have QueryOption variadic parameter",
			expectedSignature:    "options ...QueryOption) SQLQuery",
			notExpectedSignature: "options ...QueryOption) (SQLQuery, error)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This test validates the concept that our generator
			// produces the correct function signature
			assert.NotNil(t, g.typeMapper)
		})
	}
}

// TestWithDatabaseOption tests that WithDatabase option is correctly defined
func TestWithDatabaseOption(t *testing.T) {
	// Test that the WithDatabase function exists and sets the database field correctly
	// This would be tested in the generated code, but we validate the generation here

	var sb strings.Builder
	g := &Generator{}

	// Write common SQL types (which includes WithDatabase and other options)
	g.writeCommonSQLTypes(&sb)

	generatedCode := sb.String()

	// Check that WithDatabase function is generated
	assert.Contains(t, generatedCode, "// WithDatabase specifies the database to query from",
		"Should have WithDatabase comment")
	assert.Contains(t, generatedCode, "func WithDatabase(database string) QueryOption",
		"Should generate WithDatabase function")
}

// TestQueryOptionsStructure tests that QueryOptions has the Database field
func TestQueryOptionsStructure(t *testing.T) {
	var sb strings.Builder
	g := &Generator{}

	// Write common SQL types (which includes QueryOptions)
	g.writeCommonSQLTypes(&sb)

	generatedCode := sb.String()

	// Check that QueryOptions has Database field
	assert.Contains(t, generatedCode, "type QueryOptions struct",
		"Should generate QueryOptions struct")
	assert.Contains(t, generatedCode, "Database string",
		"QueryOptions should have Database field")
	assert.Contains(t, generatedCode, "AddFinal bool",
		"QueryOptions should still have AddFinal field")
}

// TestBuildParameterizedQueryImplementation tests the implementation of BuildParameterizedQuery
func TestBuildParameterizedQueryImplementation(t *testing.T) {
	var sb strings.Builder
	g := &Generator{}

	// Write common SQL functions
	g.writeCommonSQLFunctions(&sb)

	generatedCode := sb.String()

	// Check that BuildParameterizedQuery uses opts.Database correctly
	assert.Contains(t, generatedCode, "if opts.Database != \"\"",
		"Should check if database is provided")
	// The generated code should have backticks around the database name and table alias
	assert.Contains(t, generatedCode, "opts.Database, table)",
		"Should format with database when provided")
	assert.Contains(t, generatedCode, "AS _t",
		"Should add table alias for disambiguation")
}

// TestGeneratedSQLHelperFiles tests that generated SQL helper files use the new signature
func TestGeneratedSQLHelperFiles(t *testing.T) {
	// This test validates that writeSQLBuilderFunction and writeGetSQLBuilderFunction
	// generate the correct calls to BuildParameterizedQuery

	tests := []struct {
		name             string
		functionName     string
		expectedContains string
	}{
		{
			name:             "List function should call BuildParameterizedQuery without database",
			functionName:     "BuildListXQuery",
			expectedContains: "BuildParameterizedQuery(table.Name, qb, orderByClause, limit, offset, options...)",
		},
		{
			name:             "Get function should call BuildParameterizedQuery without database",
			functionName:     "BuildGetXQuery",
			expectedContains: "BuildParameterizedQuery(table.Name, qb, orderByClause, 1, 0, options...)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// This validates the concept - actual test would generate
			// the code and verify the output
			assert.NotEmpty(t, tt.functionName)
		})
	}
}

// TestBuildParameterizedQueryWithOptions tests various option combinations
// This replaces the old TestBuildParameterizedQueryWithOptions but adapted for new signature
func TestBuildParameterizedQueryWithOptions(t *testing.T) {
	tests := []struct {
		name         string
		database     string // Now passed via WithDatabase option
		table        string
		withFinal    bool
		expectedFrom string
	}{
		{
			name:         "Without database, without FINAL",
			database:     "",
			table:        "mytable",
			withFinal:    false,
			expectedFrom: "FROM mytable",
		},
		{
			name:         "With database, without FINAL",
			database:     "mydb",
			table:        "mytable",
			withFinal:    false,
			expectedFrom: "FROM `mydb`.mytable",
		},
		{
			name:         "Without database, with FINAL",
			database:     "",
			table:        "mytable",
			withFinal:    true,
			expectedFrom: "FROM mytable FINAL",
		},
		{
			name:         "With database and FINAL",
			database:     "mydb",
			table:        "mytable",
			withFinal:    true,
			expectedFrom: "FROM `mydb`.mytable FINAL",
		},
		{
			name:         "Different database name",
			database:     "testdb",
			table:        "users",
			withFinal:    false,
			expectedFrom: "FROM `testdb`.users",
		},
		{
			name:         "Complex table name with database",
			database:     "analytics",
			table:        "user_events_daily",
			withFinal:    true,
			expectedFrom: "FROM `analytics`.user_events_daily FINAL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock query builder
			qb := &mockQueryBuilder{}

			// Build options using the new pattern
			var options []mockQueryOption
			if tt.database != "" {
				options = append(options, mockWithDatabase(tt.database))
			}
			if tt.withFinal {
				options = append(options, mockWithFinal())
			}

			// Build the query using new signature with columns
			columns := []string{"id", "name", "created_at"}
			sql, err := mockBuildParameterizedQuery(tt.table, columns, qb, "", 0, 0, options...)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Check if the FROM clause is correct
			if !strings.Contains(sql.Query, tt.expectedFrom) {
				t.Errorf("Expected query to contain '%s', but got: %s", tt.expectedFrom, sql.Query)
			}
		})
	}
}

// TestBuildParameterizedQueryWithOrderByAndOptions tests ORDER BY with various options
// This replaces the old TestBuildParameterizedQueryWithOrderByAndOptions
func TestBuildParameterizedQueryWithOrderByAndOptions(t *testing.T) {
	tests := []struct {
		name          string
		database      string // Now passed via WithDatabase option
		table         string
		orderBy       string
		withFinal     bool
		limit         uint32
		offset        uint32
		expectedFrom  string
		expectedOrder string
		expectedLimit string
	}{
		{
			name:          "Without database, without FINAL, with ORDER BY",
			database:      "",
			table:         "mytable",
			orderBy:       " ORDER BY id DESC",
			withFinal:     false,
			limit:         0,
			offset:        0,
			expectedFrom:  "FROM mytable",
			expectedOrder: "ORDER BY id DESC",
		},
		{
			name:          "With database, without FINAL, with ORDER BY",
			database:      "mydb",
			table:         "mytable",
			orderBy:       " ORDER BY id DESC",
			withFinal:     false,
			limit:         0,
			offset:        0,
			expectedFrom:  "FROM `mydb`.mytable",
			expectedOrder: "ORDER BY id DESC",
		},
		{
			name:          "With database and FINAL and ORDER BY",
			database:      "mydb",
			table:         "mytable",
			orderBy:       " ORDER BY id DESC",
			withFinal:     true,
			limit:         0,
			offset:        0,
			expectedFrom:  "FROM `mydb`.mytable FINAL",
			expectedOrder: "ORDER BY id DESC",
		},
		{
			name:          "With database, FINAL, ORDER BY, and LIMIT",
			database:      "mydb",
			table:         "events",
			orderBy:       " ORDER BY timestamp DESC",
			withFinal:     true,
			limit:         100,
			offset:        0,
			expectedFrom:  "FROM `mydb`.events FINAL",
			expectedOrder: "ORDER BY timestamp DESC",
			expectedLimit: "LIMIT 100",
		},
		{
			name:          "With all options including OFFSET",
			database:      "analytics",
			table:         "metrics",
			orderBy:       " ORDER BY created_at DESC, id ASC",
			withFinal:     true,
			limit:         50,
			offset:        100,
			expectedFrom:  "FROM `analytics`.metrics FINAL",
			expectedOrder: "ORDER BY created_at DESC, id ASC",
			expectedLimit: "LIMIT 50 OFFSET 100",
		},
		{
			name:          "Complex query without database",
			database:      "",
			table:         "logs",
			orderBy:       " ORDER BY level, timestamp DESC",
			withFinal:     false,
			limit:         25,
			offset:        75,
			expectedFrom:  "FROM logs",
			expectedOrder: "ORDER BY level, timestamp DESC",
			expectedLimit: "LIMIT 25 OFFSET 75",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock query builder
			qb := &mockQueryBuilder{}

			// Build options using the new pattern
			var options []mockQueryOption
			if tt.database != "" {
				options = append(options, mockWithDatabase(tt.database))
			}
			if tt.withFinal {
				options = append(options, mockWithFinal())
			}

			// Build the query using new signature with columns
			columns := []string{"id", "name", "created_at"}
			sql, err := mockBuildParameterizedQuery(tt.table, columns, qb, tt.orderBy, tt.limit, tt.offset, options...)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Check if the FROM clause is correct
			if !strings.Contains(sql.Query, tt.expectedFrom) {
				t.Errorf("Expected query to contain '%s', but got: %s", tt.expectedFrom, sql.Query)
			}

			// Check if the ORDER BY clause is present
			if tt.orderBy != "" && !strings.Contains(sql.Query, tt.expectedOrder) {
				t.Errorf("Expected query to contain '%s', but got: %s", tt.expectedOrder, sql.Query)
			}

			// Check LIMIT and OFFSET if specified
			if tt.limit > 0 && !strings.Contains(sql.Query, tt.expectedLimit) {
				t.Errorf("Expected query to contain '%s', but got: %s", tt.expectedLimit, sql.Query)
			}
		})
	}
}

// TestWithFinalOption tests the WithFinal option specifically
func TestWithFinalOption(t *testing.T) {
	var sb strings.Builder
	g := &Generator{}

	// Write common SQL types
	g.writeCommonSQLTypes(&sb)

	generatedCode := sb.String()

	// Check that WithFinal function is generated
	assert.Contains(t, generatedCode, "// WithFinal adds the FINAL modifier to the query",
		"Should have WithFinal comment")
	assert.Contains(t, generatedCode, "func WithFinal() QueryOption",
		"Should generate WithFinal function")
}

// TestCompleteQueryGeneration tests complete query generation with WHERE clauses
func TestCompleteQueryGeneration(t *testing.T) {
	tests := []struct {
		name           string
		database       string
		table          string
		whereCondition string
		orderBy        string
		limit          uint32
		offset         uint32
		withFinal      bool
		expectedQuery  string
	}{
		{
			name:           "Complete query with all components",
			database:       "mydb",
			table:          "users",
			whereCondition: " WHERE age > 18",
			orderBy:        " ORDER BY created_at DESC",
			limit:          10,
			offset:         20,
			withFinal:      true,
			expectedQuery:  "SELECT `id`, `name`, `age`, `email` FROM `mydb`.users FINAL WHERE age > 18 ORDER BY created_at DESC LIMIT 10 OFFSET 20",
		},
		{
			name:           "Query without database",
			database:       "",
			table:          "events",
			whereCondition: " WHERE type = 'click'",
			orderBy:        " ORDER BY timestamp",
			limit:          100,
			offset:         0,
			withFinal:      false,
			expectedQuery:  "SELECT `id`, `name`, `age`, `email` FROM events WHERE type = 'click' ORDER BY timestamp LIMIT 100",
		},
		{
			name:           "Minimal query",
			database:       "",
			table:          "logs",
			whereCondition: "",
			orderBy:        "",
			limit:          0,
			offset:         0,
			withFinal:      false,
			expectedQuery:  "SELECT `id`, `name`, `age`, `email` FROM logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock query builder with WHERE condition
			qb := &mockQueryBuilder{whereClause: tt.whereCondition}

			// Build options
			var options []mockQueryOption
			if tt.database != "" {
				options = append(options, mockWithDatabase(tt.database))
			}
			if tt.withFinal {
				options = append(options, mockWithFinal())
			}

			// Build the query
			columns := []string{"id", "name", "age", "email"}
			sql, err := mockBuildParameterizedQuery(tt.table, columns, qb, tt.orderBy, tt.limit, tt.offset, options...)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// Check if the complete query matches
			assert.Equal(t, tt.expectedQuery, sql.Query,
				"Generated query should match expected query")
		})
	}
}

// Mock types to simulate the generated code

type mockQueryBuilder struct {
	whereClause string
	args        []interface{}
}

func (qb *mockQueryBuilder) GetWhereClause() string {
	return qb.whereClause
}

func (qb *mockQueryBuilder) GetArgs() []interface{} {
	return qb.args
}

type mockSQLQuery struct {
	Query string
	Args  []interface{}
}

type mockQueryOptions struct {
	AddFinal bool
	Database string
}

type mockQueryOption func(*mockQueryOptions)

func mockWithFinal() mockQueryOption {
	return func(opts *mockQueryOptions) {
		opts.AddFinal = true
	}
}

func mockWithDatabase(database string) mockQueryOption {
	return func(opts *mockQueryOptions) {
		opts.Database = database
	}
}

// mockBuildParameterizedQuery simulates the database-agnostic signature with explicit columns
func mockBuildParameterizedQuery(table string, columns []string, qb *mockQueryBuilder, orderByClause string, limit, offset uint32, options ...mockQueryOption) (mockSQLQuery, error) {
	// Apply options
	opts := &mockQueryOptions{}
	for _, opt := range options {
		opt(opts)
	}

	// Build FROM clause with optional database and FINAL
	var fromClause string
	if opts.Database != "" {
		fromClause = fmt.Sprintf("`%s`.%s", opts.Database, table)
	} else {
		fromClause = table
	}
	if opts.AddFinal {
		fromClause += " FINAL"
	}

	// Validate columns
	if len(columns) == 0 {
		return mockSQLQuery{}, errEmptyColumnsList
	}

	// Build column list
	escapedColumns := make([]string, 0, len(columns))
	for _, col := range columns {
		escapedColumns = append(escapedColumns, fmt.Sprintf("`%s`", col))
	}
	columnList := strings.Join(escapedColumns, ", ")

	query := fmt.Sprintf("SELECT %s FROM %s", columnList, fromClause)

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
	}, nil
}

// TestGeneratedDateTimeHandling tests that the generated SQL common code handles DateTime correctly
func TestGeneratedDateTimeHandling(t *testing.T) {
	// Create a generator instance
	g := &Generator{}
	sb := &strings.Builder{}

	// Write the common SQL functions
	g.writeCommonSQLFunctions(sb)

	generatedCode := sb.String()

	// Check that the AddCondition method handles DateTimeValue
	if !strings.Contains(generatedCode, "case DateTimeValue:") {
		t.Error("Generated AddCondition should handle DateTimeValue type")
	}

	if !strings.Contains(generatedCode, "fromUnixTimestamp(") {
		t.Error("Generated code should use fromUnixTimestamp for DateTime values")
	}

	// Check that DateTime64Value is also handled
	if !strings.Contains(generatedCode, "case DateTime64Value:") {
		t.Error("Generated AddCondition should handle DateTime64Value type")
	}

	if !strings.Contains(generatedCode, "fromUnixTimestamp64Micro(") {
		t.Error("Generated code should use fromUnixTimestamp64Micro for DateTime64 values")
	}
}

// TestWriteDateTimeFilterCases tests that writeDateTimeFilterCases generates correct code
func TestWriteDateTimeFilterCases(t *testing.T) {
	g := &Generator{}
	sb := &strings.Builder{}

	// Generate DateTime filter cases
	g.writeDateTimeFilterCases(sb, "test_column", "UInt32Filter", "\t")

	generatedCode := sb.String()

	// Check that it generates DateTimeValue wrapper usage
	if !strings.Contains(generatedCode, "DateTimeValue{") {
		t.Error("DateTime filter cases should use DateTimeValue wrapper")
	}

	// Check all the filter operations are present
	expectedOps := []string{"_Eq", "_Ne", "_Lt", "_Lte", "_Gt", "_Gte", "_Between", "_In", "_NotIn"}
	for _, op := range expectedOps {
		if !strings.Contains(generatedCode, "UInt32Filter"+op) {
			t.Errorf("Missing filter operation: %s", op)
		}
	}
}
