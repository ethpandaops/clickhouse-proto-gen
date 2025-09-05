package protogen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/clickhouse"
	"github.com/ethpandaops/clickhouse-proto-gen/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGenerateSQLHelperWithPrimaryKeyConst(t *testing.T) {
	// Create a temporary directory for test output
	tmpDir := t.TempDir()

	// Create a test table
	table := &clickhouse.Table{
		Database:   "testdb",
		Name:       "test_table",
		SortingKey: []string{"user_id"},
		Columns: []clickhouse.Column{
			{
				Name: "user_id",
				Type: "String",
			},
			{
				Name: "created_at",
				Type: "DateTime",
			},
		},
	}

	// Create generator
	g := &Generator{
		config: &config.Config{
			GoPackage: "github.com/test/pkg",
			OutputDir: tmpDir,
		},
		log:        logrus.New(),
		typeMapper: NewTypeMapper(),
	}

	// Generate SQL helper
	err := g.generateSQLHelper(table)
	require.NoError(t, err)

	// Read the generated file
	content, err := os.ReadFile(filepath.Join(tmpDir, "test_table.go"))
	require.NoError(t, err)

	// Check that the const is generated
	assert.Contains(t, string(content), "const PrimaryKeyTestTable = \"user_id\"", "Should generate primary key const")
	assert.Contains(t, string(content), "// PrimaryKeyTestTable is the primary key column for testdb.test_table", "Should include comment for const")
}

func TestGenerateSQLHelperWithNumericTableName(t *testing.T) {
	// Create a temporary directory for test output
	tmpDir := t.TempDir()

	// Create a test table with a name starting with a number
	table := &clickhouse.Table{
		Database:   "mainnet",
		Name:       "24h_stats",
		SortingKey: []string{"address"},
		Columns: []clickhouse.Column{
			{
				Name: "address",
				Type: "String",
			},
			{
				Name: "value",
				Type: "UInt64",
			},
		},
	}

	// Create generator
	g := &Generator{
		config: &config.Config{
			GoPackage: "github.com/test/pkg",
			OutputDir: tmpDir,
		},
		log:        logrus.New(),
		typeMapper: NewTypeMapper(),
	}

	// Generate SQL helper
	err := g.generateSQLHelper(table)
	require.NoError(t, err)

	// Read the generated file
	content, err := os.ReadFile(filepath.Join(tmpDir, "24h_stats.go"))
	require.NoError(t, err)

	// Check that the const is generated with proper naming (24h becomes 24HStats)
	assert.Contains(t, string(content), "const PrimaryKey24HStats = \"address\"", "Should generate primary key const with sanitized name")
}

func TestPrimaryKeyConstInGeneratedCode(t *testing.T) {
	// Create a temporary directory for test output
	tmpDir := t.TempDir()

	// Test that the const is properly placed in the generated code
	table := &clickhouse.Table{
		Database:   "test",
		Name:       "users",
		SortingKey: []string{"id"},
		Columns: []clickhouse.Column{
			{
				Name: "id",
				Type: "UInt64",
			},
			{
				Name: "name",
				Type: "String",
			},
		},
	}

	g := &Generator{
		config: &config.Config{
			GoPackage: "github.com/test/pkg",
			OutputDir: tmpDir,
		},
		log:        logrus.New(),
		typeMapper: NewTypeMapper(),
	}

	err := g.generateSQLHelper(table)
	require.NoError(t, err)

	// Read the generated file
	generatedContent, err := os.ReadFile(filepath.Join(tmpDir, "users.go"))
	require.NoError(t, err)

	// Verify the const appears after imports but before functions
	lines := strings.Split(string(generatedContent), "\n")

	importFound := false
	constFound := false
	functionFound := false

	for _, line := range lines {
		if strings.Contains(line, "import (") {
			importFound = true
		}
		if strings.Contains(line, "const PrimaryKeyUsers") {
			constFound = true
			assert.True(t, importFound, "Const should appear after imports")
			assert.False(t, functionFound, "Const should appear before functions")
		}
		if strings.Contains(line, "func BuildListUsersQuery") {
			functionFound = true
		}
	}

	assert.True(t, constFound, "Primary key const should be generated")
	assert.True(t, functionFound, "Function should be generated")
}
