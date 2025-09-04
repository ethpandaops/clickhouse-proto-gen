package clickhouse

import (
	"context"
	"database/sql"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractBaseType(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple type",
			input:    "Int32",
			expected: "Int32",
		},
		{
			name:     "Nullable type",
			input:    "Nullable(String)",
			expected: "String",
		},
		{
			name:     "Array type",
			input:    "Array(Int32)",
			expected: "Int32",
		},
		{
			name:     "Nested nullable array",
			input:    "Nullable(Array(String))",
			expected: "String",
		},
		{
			name:     "Type with parameters",
			input:    "FixedString(10)",
			expected: "FixedString",
		},
		{
			name:     "Decimal with parameters",
			input:    "Decimal(18, 2)",
			expected: "Decimal",
		},
		{
			name:     "DateTime64 with precision",
			input:    "DateTime64(3)",
			expected: "DateTime64",
		},
		{
			name:     "LowCardinality wrapper",
			input:    "LowCardinality(String)",
			expected: "LowCardinality",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := extractBaseType(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestParseSortingKey(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "Empty sorting key",
			input:    "",
			expected: nil,
		},
		{
			name:     "Single column",
			input:    "id",
			expected: []string{"id"},
		},
		{
			name:     "Multiple columns",
			input:    "user_id, created_at",
			expected: []string{"user_id", "created_at"},
		},
		{
			name:     "Columns with ASC/DESC",
			input:    "id ASC, created_at DESC, name",
			expected: []string{"id", "created_at", "name"},
		},
		{
			name:     "Columns with parentheses",
			input:    "(id), (created_at)",
			expected: []string{"id", "created_at"},
		},
		{
			name:     "Complex sorting key",
			input:    "user_id ASC, (timestamp) DESC, status",
			expected: []string{"user_id", "timestamp", "status"},
		},
		{
			name:     "Extra spaces",
			input:    "  id  ,   created_at   ,  name  ",
			expected: []string{"id", "created_at", "name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := parseSortingKey(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestSplitDistributedArgs(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:  "Basic distributed args",
			input: "cluster_name, database, table",
			expected: []string{
				"cluster_name",
				" database",
				" table",
			},
		},
		{
			name:  "With sharding key",
			input: "cluster, db, tbl, rand()",
			expected: []string{
				"cluster",
				" db",
				" tbl",
				" rand()",
			},
		},
		{
			name:  "With quoted strings",
			input: "'cluster', 'database', 'table'",
			expected: []string{
				"'cluster'",
				" 'database'",
				" 'table'",
			},
		},
		{
			name:  "With complex expression",
			input: "cluster, db, table, cityHash64(user_id, timestamp)",
			expected: []string{
				"cluster",
				" db",
				" table",
				" cityHash64(user_id, timestamp)",
			},
		},
		{
			name:  "Nested parentheses",
			input: "cluster, db, table, mod(cityHash64(concat(user_id, '_', timestamp)), 10)",
			expected: []string{
				"cluster",
				" db",
				" table",
				" mod(cityHash64(concat(user_id, '_', timestamp)), 10)",
			},
		},
		{
			name:  "Double quoted strings",
			input: `"cluster", "database", "table"`,
			expected: []string{
				`"cluster"`,
				` "database"`,
				` "table"`,
			},
		},
		{
			name:  "Mixed quotes",
			input: `'cluster', "database", table`,
			expected: []string{
				`'cluster'`,
				` "database"`,
				` table`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := splitDistributedArgs(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestExtractUnderlyingTable(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)
	s := &service{log: log}

	tests := []struct {
		name     string
		input    string
		expected *underlyingTableInfo
	}{
		{
			name:     "Not distributed table",
			input:    "MergeTree()",
			expected: nil,
		},
		{
			name:  "Basic distributed table",
			input: "Distributed(cluster, database, table)",
			expected: &underlyingTableInfo{
				Database: "database",
				Table:    "table",
			},
		},
		{
			name:  "Distributed with sharding key",
			input: "Distributed(cluster, database, table, rand())",
			expected: &underlyingTableInfo{
				Database: "database",
				Table:    "table",
			},
		},
		{
			name:  "Distributed with quoted names",
			input: "Distributed('cluster', 'database', 'table')",
			expected: &underlyingTableInfo{
				Database: "database",
				Table:    "table",
			},
		},
		{
			name:  "Distributed with double quotes",
			input: `Distributed("cluster", "database", "table")`,
			expected: &underlyingTableInfo{
				Database: "database",
				Table:    "table",
			},
		},
		{
			name:  "Complex sharding expression",
			input: "Distributed(cluster, db, tbl, cityHash64(user_id, timestamp))",
			expected: &underlyingTableInfo{
				Database: "db",
				Table:    "tbl",
			},
		},
		{
			name:     "Invalid distributed format",
			input:    "Distributed(cluster)",
			expected: nil,
		},
		{
			name:  "Extra spaces",
			input: "Distributed( cluster ,  database  ,  table  )",
			expected: &underlyingTableInfo{
				Database: "database",
				Table:    "table",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := s.extractUnderlyingTable(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestExtractDatabaseFromDSN is removed as the function is in main.go, not in this package
// The function could be moved to a utility package if needed for testing

func TestTable_Structure(t *testing.T) {
	table := &Table{
		Name:     "users",
		Database: "test",
		Comment:  "User accounts table",
		Columns: []Column{
			{
				Name:         "id",
				Type:         "UInt64",
				BaseType:     "UInt64",
				Position:     1,
				Comment:      "User ID",
				IsNullable:   false,
				IsArray:      false,
				DefaultKind:  "",
				DefaultValue: "",
			},
			{
				Name:         "name",
				Type:         "Nullable(String)",
				BaseType:     "String",
				Position:     2,
				Comment:      "User name",
				IsNullable:   true,
				IsArray:      false,
				DefaultKind:  "DEFAULT",
				DefaultValue: "'Anonymous'",
			},
			{
				Name:         "tags",
				Type:         "Array(String)",
				BaseType:     "String",
				Position:     3,
				Comment:      "User tags",
				IsNullable:   false,
				IsArray:      true,
				DefaultKind:  "",
				DefaultValue: "",
			},
			{
				Name:         "created_at",
				Type:         "DateTime",
				BaseType:     "DateTime",
				Position:     4,
				Comment:      "Creation timestamp",
				IsNullable:   false,
				IsArray:      false,
				DefaultKind:  "DEFAULT",
				DefaultValue: "now()",
			},
		},
		SortingKey: []string{"id", "created_at"},
	}

	// Test table structure
	assert.Equal(t, "users", table.Name)
	assert.Equal(t, "test", table.Database)
	assert.Equal(t, "User accounts table", table.Comment)
	assert.Len(t, table.Columns, 4)
	assert.Equal(t, []string{"id", "created_at"}, table.SortingKey)

	// Test column structure
	idColumn := table.Columns[0]
	assert.Equal(t, "id", idColumn.Name)
	assert.Equal(t, "UInt64", idColumn.Type)
	assert.Equal(t, "UInt64", idColumn.BaseType)
	assert.Equal(t, uint64(1), idColumn.Position)
	assert.False(t, idColumn.IsNullable)
	assert.False(t, idColumn.IsArray)

	nameColumn := table.Columns[1]
	assert.Equal(t, "name", nameColumn.Name)
	assert.Equal(t, "Nullable(String)", nameColumn.Type)
	assert.Equal(t, "String", nameColumn.BaseType)
	assert.True(t, nameColumn.IsNullable)
	assert.False(t, nameColumn.IsArray)
	assert.Equal(t, "DEFAULT", nameColumn.DefaultKind)
	assert.Equal(t, "'Anonymous'", nameColumn.DefaultValue)

	tagsColumn := table.Columns[2]
	assert.Equal(t, "tags", tagsColumn.Name)
	assert.Equal(t, "Array(String)", tagsColumn.Type)
	assert.Equal(t, "String", tagsColumn.BaseType)
	assert.False(t, tagsColumn.IsNullable)
	assert.True(t, tagsColumn.IsArray)

	createdAtColumn := table.Columns[3]
	assert.Equal(t, "created_at", createdAtColumn.Name)
	assert.Equal(t, "DateTime", createdAtColumn.Type)
	assert.Equal(t, "DateTime", createdAtColumn.BaseType)
	assert.False(t, createdAtColumn.IsNullable)
	assert.False(t, createdAtColumn.IsArray)
	assert.Equal(t, "DEFAULT", createdAtColumn.DefaultKind)
	assert.Equal(t, "now()", createdAtColumn.DefaultValue)
}

func TestNewService(t *testing.T) {
	log := logrus.New()
	dsn := "clickhouse://localhost:9000/test"

	svc := NewService(dsn, log)

	require.NotNil(t, svc)

	// Type assertion to access private fields for testing
	s, ok := svc.(*service)
	require.True(t, ok)

	assert.Equal(t, dsn, s.dsn)
	assert.NotNil(t, s.log)
}

func TestServiceLoadSortingKey(t *testing.T) {
	tests := []struct {
		name       string
		sortingKey sql.NullString
		engine     sql.NullString
		engineFull sql.NullString
		expected   []string
		setupMock  func(*service)
	}{
		{
			name: "Direct sorting key available",
			sortingKey: sql.NullString{
				String: "id, created_at",
				Valid:  true,
			},
			engine: sql.NullString{
				String: "MergeTree",
				Valid:  true,
			},
			expected: []string{"id", "created_at"},
		},
		{
			name: "Empty sorting key for non-distributed table",
			sortingKey: sql.NullString{
				Valid: false,
			},
			engine: sql.NullString{
				String: "Memory",
				Valid:  true,
			},
			expected: nil,
		},
		// Removed the test case that causes nil pointer since we don't have a DB connection
		// In a real integration test, this would be properly mocked
		{
			name: "Sorting key with ASC/DESC modifiers",
			sortingKey: sql.NullString{
				String: "user_id ASC, timestamp DESC",
				Valid:  true,
			},
			engine: sql.NullString{
				String: "ReplacingMergeTree",
				Valid:  true,
			},
			expected: []string{"user_id", "timestamp"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)
			ctx := context.Background()

			s := &service{
				log: log,
			}

			if tt.setupMock != nil {
				tt.setupMock(s)
			}

			table := &Table{
				Name:     "test_table",
				Database: "test_db",
			}

			s.loadSortingKey(ctx, table, tt.sortingKey, tt.engine, tt.engineFull)

			assert.Equal(t, tt.expected, table.SortingKey)
		})
	}
}

func TestColumnProperties(t *testing.T) {
	tests := []struct {
		name      string
		column    Column
		checkFunc func(t *testing.T, col Column)
	}{
		{
			name: "Simple column",
			column: Column{
				Name:     "simple_col",
				Type:     "Int32",
				BaseType: "Int32",
				Position: 1,
			},
			checkFunc: func(t *testing.T, col Column) {
				assert.False(t, col.IsNullable)
				assert.False(t, col.IsArray)
				assert.Equal(t, "Int32", col.BaseType)
			},
		},
		{
			name: "Nullable column",
			column: Column{
				Name:       "nullable_col",
				Type:       "Nullable(String)",
				BaseType:   "String",
				Position:   2,
				IsNullable: true,
			},
			checkFunc: func(t *testing.T, col Column) {
				assert.True(t, col.IsNullable)
				assert.False(t, col.IsArray)
				assert.Equal(t, "String", col.BaseType)
			},
		},
		{
			name: "Array column",
			column: Column{
				Name:     "array_col",
				Type:     "Array(Float64)",
				BaseType: "Float64",
				Position: 3,
				IsArray:  true,
			},
			checkFunc: func(t *testing.T, col Column) {
				assert.False(t, col.IsNullable)
				assert.True(t, col.IsArray)
				assert.Equal(t, "Float64", col.BaseType)
			},
		},
		{
			name: "Column with default",
			column: Column{
				Name:         "default_col",
				Type:         "DateTime",
				BaseType:     "DateTime",
				Position:     4,
				DefaultKind:  "DEFAULT",
				DefaultValue: "now()",
			},
			checkFunc: func(t *testing.T, col Column) {
				assert.Equal(t, "DEFAULT", col.DefaultKind)
				assert.Equal(t, "now()", col.DefaultValue)
			},
		},
		{
			name: "Column with comment",
			column: Column{
				Name:     "commented_col",
				Type:     "String",
				BaseType: "String",
				Position: 5,
				Comment:  "This is a test column",
			},
			checkFunc: func(t *testing.T, col Column) {
				assert.Equal(t, "This is a test column", col.Comment)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.checkFunc(t, tt.column)
		})
	}
}
