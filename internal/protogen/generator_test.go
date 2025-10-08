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

func TestNewGenerator(t *testing.T) {
	cfg := &config.Config{
		OutputDir:       "/tmp/proto",
		Package:         "test.v1",
		GoPackage:       "github.com/test/proto",
		IncludeComments: true,
	}

	log := logrus.New()
	log.SetOutput(os.Stdout)

	gen := NewGenerator(cfg, log)

	require.NotNil(t, gen)
	assert.Equal(t, cfg, gen.config)
	assert.NotNil(t, gen.typeMapper)
	assert.NotNil(t, gen.log)
}

func TestGenerator_Generate(t *testing.T) {
	// Create a temp directory for test output
	tempDir, err := os.MkdirTemp("", "protogen_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		OutputDir:       tempDir,
		Package:         "test.v1",
		GoPackage:       "github.com/test/proto",
		IncludeComments: true,
	}

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	gen := NewGenerator(cfg, log)

	tables := []*clickhouse.Table{
		{
			Name:     "users",
			Database: "test",
			Comment:  "User accounts table",
			Columns: []clickhouse.Column{
				{
					Name:     "id",
					Type:     "UInt64",
					BaseType: "UInt64",
					Position: 1,
					Comment:  "User ID",
				},
				{
					Name:       "name",
					Type:       "Nullable(String)",
					BaseType:   "String",
					Position:   2,
					IsNullable: true,
					Comment:    "User name",
				},
			},
			SortingKey: []string{"id"},
		},
	}

	err = gen.Generate(tables)
	require.NoError(t, err)

	// Check that common.proto was created
	commonProtoPath := filepath.Join(tempDir, "common.proto")
	assert.FileExists(t, commonProtoPath)

	// Check that table proto file was created
	usersProtoPath := filepath.Join(tempDir, "users.proto")
	assert.FileExists(t, usersProtoPath)

	// Read and verify content
	content, err := os.ReadFile(usersProtoPath)
	require.NoError(t, err)

	contentStr := string(content)
	assert.Contains(t, contentStr, "syntax = \"proto3\"")
	assert.Contains(t, contentStr, "package test.v1")
	assert.Contains(t, contentStr, "option go_package = \"github.com/test/proto\"")
	assert.Contains(t, contentStr, "message Users")
	assert.Contains(t, contentStr, "uint64 id = 11")
	assert.Contains(t, contentStr, "google.protobuf.StringValue name = 12")

	// Should have service definitions since it has a sorting key
	assert.Contains(t, contentStr, "service UsersService")
	assert.Contains(t, contentStr, "message ListUsersRequest")
	assert.Contains(t, contentStr, "message ListUsersResponse")
}

func TestGenerator_CheckNeedsWrapper(t *testing.T) {
	cfg := &config.Config{}
	log := logrus.New()
	gen := NewGenerator(cfg, log)

	tests := []struct {
		name     string
		tables   []*clickhouse.Table
		expected bool
	}{
		{
			name: "Table with nullable column needs wrapper",
			tables: []*clickhouse.Table{
				{
					Columns: []clickhouse.Column{
						{
							Name:       "nullable_field",
							Type:       "Nullable(Int32)",
							BaseType:   "Int32",
							IsNullable: true,
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "Table without nullable columns doesn't need wrapper",
			tables: []*clickhouse.Table{
				{
					Columns: []clickhouse.Column{
						{
							Name:     "regular_field",
							Type:     "Int32",
							BaseType: "Int32",
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "Nullable array doesn't trigger wrapper (arrays handle null differently)",
			tables: []*clickhouse.Table{
				{
					Columns: []clickhouse.Column{
						{
							Name:       "array_field",
							Type:       "Array(String)",
							BaseType:   "String",
							IsArray:    true,
							IsNullable: true,
						},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := gen.checkNeedsWrapper(tt.tables)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerator_WriteServiceDefinitions(t *testing.T) {
	cfg := &config.Config{
		IncludeComments: true,
	}
	log := logrus.New()
	gen := NewGenerator(cfg, log)

	tests := []struct {
		name            string
		table           *clickhouse.Table
		expectedContent []string
		notExpected     []string
	}{
		{
			name: "Table with sorting key generates service",
			table: &clickhouse.Table{
				Name: "orders",
				Columns: []clickhouse.Column{
					{
						Name:     "order_id",
						Type:     "UInt64",
						BaseType: "UInt64",
						Position: 1,
					},
					{
						Name:     "user_id",
						Type:     "UInt64",
						BaseType: "UInt64",
						Position: 2,
					},
					{
						Name:     "created_at",
						Type:     "DateTime",
						BaseType: "DateTime",
						Position: 3,
					},
				},
				SortingKey: []string{"order_id", "created_at"},
			},
			expectedContent: []string{
				"message ListOrdersRequest",
				"message ListOrdersResponse",
				"message GetOrdersRequest",
				"message GetOrdersResponse",
				"service OrdersService",
				"rpc List(ListOrdersRequest) returns (ListOrdersResponse)",
				"rpc Get(GetOrdersRequest) returns (GetOrdersResponse)",
				"PRIMARY KEY - required",
				"ORDER BY column 2 - optional",
				"page_size",
				"page_token",
				"order_by",
			},
			notExpected: []string{},
		},
		{
			name: "Table without sorting key doesn't generate service",
			table: &clickhouse.Table{
				Name: "logs",
				Columns: []clickhouse.Column{
					{
						Name:     "message",
						Type:     "String",
						BaseType: "String",
						Position: 1,
					},
				},
				SortingKey: []string{},
			},
			expectedContent: []string{},
			notExpected: []string{
				"service LogsService",
				"message ListLogsRequest",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sb strings.Builder
			gen.writeServiceDefinitions(&sb, tt.table)
			result := sb.String()

			for _, expected := range tt.expectedContent {
				assert.Contains(t, result, expected, "Expected content not found: %s", expected)
			}

			for _, notExpected := range tt.notExpected {
				assert.NotContains(t, result, notExpected, "Unexpected content found: %s", notExpected)
			}
		})
	}
}

func TestGenerator_WriteMessage(t *testing.T) {
	cfg := &config.Config{
		IncludeComments: true,
	}
	log := logrus.New()
	gen := NewGenerator(cfg, log)

	table := &clickhouse.Table{
		Name:    "products",
		Comment: "Product catalog",
		Columns: []clickhouse.Column{
			{
				Name:     "id",
				Type:     "UInt64",
				BaseType: "UInt64",
				Position: 1,
				Comment:  "Product ID",
			},
			{
				Name:     "name",
				Type:     "String",
				BaseType: "String",
				Position: 2,
				Comment:  "Product name",
			},
			{
				Name:       "price",
				Type:       "Nullable(Float64)",
				BaseType:   "Float64",
				Position:   3,
				IsNullable: true,
				Comment:    "Product price",
			},
			{
				Name:     "tags",
				Type:     "Array(String)",
				BaseType: "String",
				Position: 4,
				IsArray:  true,
				Comment:  "Product tags",
			},
		},
	}

	var sb strings.Builder
	gen.writeMessage(&sb, table)
	result := sb.String()

	// Check message structure
	assert.Contains(t, result, "// Product catalog")
	assert.Contains(t, result, "message Products {")
	assert.Contains(t, result, "// Product ID")
	assert.Contains(t, result, "uint64 id = 11")
	assert.Contains(t, result, "// Product name")
	assert.Contains(t, result, "string name = 12")
	assert.Contains(t, result, "// Product price")
	assert.Contains(t, result, "google.protobuf.DoubleValue price = 13")
	assert.Contains(t, result, "// Product tags")
	assert.Contains(t, result, "repeated string tags = 14")
}

func TestGenerator_WriteComment(t *testing.T) {
	cfg := &config.Config{
		IncludeComments: true,
	}
	log := logrus.New()
	gen := NewGenerator(cfg, log)

	tests := []struct {
		name     string
		comment  string
		indent   string
		expected string
	}{
		{
			name:     "Single line comment",
			comment:  "This is a test",
			indent:   "  ",
			expected: "  // This is a test\n",
		},
		{
			name:     "Multi-line comment",
			comment:  "Line 1\nLine 2\nLine 3",
			indent:   "  ",
			expected: "  // Line 1\n  // Line 2\n  // Line 3\n",
		},
		{
			name:     "Comment with empty lines",
			comment:  "Line 1\n\nLine 3",
			indent:   "",
			expected: "// Line 1\n// Line 3\n",
		},
		{
			name:     "No indent",
			comment:  "Test comment",
			indent:   "",
			expected: "// Test comment\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sb strings.Builder
			gen.writeComment(&sb, tt.comment, tt.indent)
			assert.Equal(t, tt.expected, sb.String())
		})
	}
}

func TestGenerator_WriteCommentDisabled(t *testing.T) {
	cfg := &config.Config{
		IncludeComments: false, // Comments disabled
	}
	log := logrus.New()
	gen := NewGenerator(cfg, log)

	var sb strings.Builder
	gen.writeComment(&sb, "This comment should not appear", "  ")
	assert.Empty(t, sb.String())
}

func TestGenerator_WriteField(t *testing.T) {
	cfg := &config.Config{
		IncludeComments: true,
	}
	log := logrus.New()
	gen := NewGenerator(cfg, log)

	tests := []struct {
		name     string
		field    *ProtoField
		expected string
	}{
		{
			name: "Simple field",
			field: &ProtoField{
				Name:    "user_id",
				Type:    "uint64",
				Number:  1,
				Comment: "",
			},
			expected: "  uint64 user_id = 1;\n",
		},
		{
			name: "Field with comment",
			field: &ProtoField{
				Name:    "username",
				Type:    "string",
				Number:  2,
				Comment: "The username",
			},
			expected: "  // The username\n  string username = 2;\n",
		},
		{
			name: "Repeated field",
			field: &ProtoField{
				Name:    "tags",
				Type:    "repeated string",
				Number:  3,
				Comment: "List of tags",
			},
			expected: "  // List of tags\n  repeated string tags = 3;\n",
		},
		{
			name: "Wrapper type field",
			field: &ProtoField{
				Name:    "age",
				Type:    "google.protobuf.Int32Value",
				Number:  4,
				Comment: "Optional age",
			},
			expected: "  // Optional age\n  google.protobuf.Int32Value age = 4;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var sb strings.Builder
			gen.writeField(&sb, tt.field)
			assert.Equal(t, tt.expected, sb.String())
		})
	}
}

func TestGetProtoType(t *testing.T) {
	tests := []struct {
		baseType string
		expected string
	}{
		{"Int8", "int32"},
		{"Int16", "int32"},
		{"Int32", "int32"},
		{"Int64", "int64"},
		{"UInt8", "uint32"},
		{"UInt16", "uint32"},
		{"UInt32", "uint32"},
		{"UInt64", "uint64"},
		{"String", "string"},
		{"UnknownType", "string"},
	}

	for _, tt := range tests {
		t.Run(tt.baseType, func(t *testing.T) {
			result := getProtoType(tt.baseType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetProtoTypeForColumn(t *testing.T) {
	tests := []struct {
		name     string
		column   *clickhouse.Column
		expected string
	}{
		{
			name: "Simple column",
			column: &clickhouse.Column{
				BaseType: "Int32",
				IsArray:  false,
			},
			expected: "int32",
		},
		{
			name: "Array column",
			column: &clickhouse.Column{
				BaseType: "String",
				IsArray:  true,
			},
			expected: "repeated string",
		},
		{
			name: "UInt64 column",
			column: &clickhouse.Column{
				BaseType: "UInt64",
				IsArray:  false,
			},
			expected: "uint64",
		},
		{
			name: "Array of UInt32",
			column: &clickhouse.Column{
				BaseType: "UInt32",
				IsArray:  true,
			},
			expected: "repeated uint32",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getProtoTypeForColumn(tt.column)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerator_ShouldGenerateAPI(t *testing.T) {
	tests := []struct {
		name      string
		config    *config.Config
		tableName string
		shouldGen bool
	}{
		{
			name: "fct_ table with prefix filter",
			config: &config.Config{
				EnableAPI:        true,
				APITablePrefixes: []string{"fct_"},
			},
			tableName: "fct_block",
			shouldGen: true,
		},
		{
			name: "external table with prefix filter",
			config: &config.Config{
				EnableAPI:        true,
				APITablePrefixes: []string{"fct_"},
			},
			tableName: "beacon_api_eth_v1_events_block",
			shouldGen: false,
		},
		{
			name: "int_ table with prefix filter",
			config: &config.Config{
				EnableAPI:        true,
				APITablePrefixes: []string{"fct_"},
			},
			tableName: "int_block_processing",
			shouldGen: false,
		},
		{
			name: "any table with no prefix filter",
			config: &config.Config{
				EnableAPI:        true,
				APITablePrefixes: []string{},
			},
			tableName: "beacon_api_eth_v1_events_block",
			shouldGen: true,
		},
		{
			name: "API generation disabled",
			config: &config.Config{
				EnableAPI:        false,
				APITablePrefixes: []string{"fct_"},
			},
			tableName: "fct_block",
			shouldGen: false,
		},
		{
			name: "multiple prefixes - matches first",
			config: &config.Config{
				EnableAPI:        true,
				APITablePrefixes: []string{"fct_", "dim_"},
			},
			tableName: "fct_attestation",
			shouldGen: true,
		},
		{
			name: "multiple prefixes - matches second",
			config: &config.Config{
				EnableAPI:        true,
				APITablePrefixes: []string{"fct_", "dim_"},
			},
			tableName: "dim_validator",
			shouldGen: true,
		},
		{
			name: "multiple prefixes - no match",
			config: &config.Config{
				EnableAPI:        true,
				APITablePrefixes: []string{"fct_", "dim_"},
			},
			tableName: "int_block_processing",
			shouldGen: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			gen := NewGenerator(tt.config, log)
			result := gen.shouldGenerateAPI(tt.tableName)
			assert.Equal(t, tt.shouldGen, result)
		})
	}
}

func TestGenerator_ServiceWithHTTPAnnotations(t *testing.T) {
	tests := []struct {
		name               string
		config             *config.Config
		table              *clickhouse.Table
		expectedHTTP       bool
		expectedAnnotation bool
	}{
		{
			name: "fct_ table should have HTTP annotations",
			config: &config.Config{
				EnableAPI:        true,
				APIBasePath:      "/api/v1",
				APITablePrefixes: []string{"fct_"},
				IncludeComments:  true,
			},
			table: &clickhouse.Table{
				Name: "fct_block",
				Columns: []clickhouse.Column{
					{
						Name:     "slot",
						Type:     "UInt32",
						BaseType: "UInt32",
						Position: 1,
					},
				},
				SortingKey: []string{"slot"},
			},
			expectedHTTP:       true,
			expectedAnnotation: true,
		},
		{
			name: "external table should NOT have HTTP annotations",
			config: &config.Config{
				EnableAPI:        true,
				APIBasePath:      "/api/v1",
				APITablePrefixes: []string{"fct_"},
				IncludeComments:  true,
			},
			table: &clickhouse.Table{
				Name: "beacon_api_eth_v1_events_block",
				Columns: []clickhouse.Column{
					{
						Name:     "slot",
						Type:     "UInt32",
						BaseType: "UInt32",
						Position: 1,
					},
				},
				SortingKey: []string{"slot"},
			},
			expectedHTTP:       false,
			expectedAnnotation: false,
		},
		{
			name: "int_ table should NOT have HTTP annotations",
			config: &config.Config{
				EnableAPI:        true,
				APIBasePath:      "/api/v1",
				APITablePrefixes: []string{"fct_"},
				IncludeComments:  true,
			},
			table: &clickhouse.Table{
				Name: "int_block_processing",
				Columns: []clickhouse.Column{
					{
						Name:     "slot",
						Type:     "UInt32",
						BaseType: "UInt32",
						Position: 1,
					},
				},
				SortingKey: []string{"slot"},
			},
			expectedHTTP:       false,
			expectedAnnotation: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			gen := NewGenerator(tt.config, log)

			var sb strings.Builder
			gen.writeServiceDefinitions(&sb, tt.table)
			result := sb.String()

			if tt.expectedHTTP {
				// Should have HTTP annotations with pipe-separated comment format
				assert.Contains(t, result, "option (google.api.http)", "Expected HTTP annotations")
				assert.Contains(t, result, "get: \"/api/v1/"+tt.table.Name+"\"", "Expected GET endpoint")
				assert.Contains(t, result, " | ", "Expected pipe-separated comment format")
			} else {
				// Should NOT have HTTP annotations
				assert.NotContains(t, result, "option (google.api.http)", "Should not have HTTP annotations")
			}

			if tt.expectedAnnotation {
				// Should have field_behavior annotations
				assert.Contains(t, result, "google.api.field_behavior", "Expected field_behavior annotations")
				assert.Contains(t, result, "REQUIRED", "Expected REQUIRED annotation for primary key")
				assert.Contains(t, result, "OPTIONAL", "Expected OPTIONAL annotations")
			} else {
				// Should NOT have field_behavior annotations
				assert.NotContains(t, result, "google.api.field_behavior", "Should not have field_behavior annotations")
			}
		})
	}
}

func TestGenerator_GenerateProtoWithAPIAnnotations(t *testing.T) {
	// Create a temp directory for test output
	tempDir, err := os.MkdirTemp("", "protogen_api_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	cfg := &config.Config{
		OutputDir:        tempDir,
		Package:          "clickhouse.v1",
		GoPackage:        "github.com/test/proto/clickhouse",
		IncludeComments:  true,
		EnableAPI:        true,
		APIBasePath:      "/api/v1",
		APITablePrefixes: []string{"fct_"},
		MaxPageSize:      10000,
	}

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	gen := NewGenerator(cfg, log)

	// Create test tables: one fct_ table and one non-fct_ table
	tables := []*clickhouse.Table{
		{
			Name:     "fct_block",
			Database: "mainnet",
			Comment:  "Block data from the beacon chain",
			Columns: []clickhouse.Column{
				{
					Name:     "slot",
					Type:     "UInt32",
					BaseType: "UInt32",
					Position: 1,
					Comment:  "Slot number",
				},
				{
					Name:     "proposer_index",
					Type:     "UInt32",
					BaseType: "UInt32",
					Position: 2,
					Comment:  "Proposer validator index",
				},
			},
			SortingKey: []string{"slot"},
		},
		{
			Name:     "beacon_api_eth_v1_events_block",
			Database: "mainnet",
			Comment:  "Raw beacon API events",
			Columns: []clickhouse.Column{
				{
					Name:     "slot",
					Type:     "UInt32",
					BaseType: "UInt32",
					Position: 1,
					Comment:  "Slot number",
				},
			},
			SortingKey: []string{"slot"},
		},
	}

	err = gen.Generate(tables)
	require.NoError(t, err)

	// Read and verify fct_block.proto
	fctBlockProtoPath := filepath.Join(tempDir, "fct_block.proto")
	assert.FileExists(t, fctBlockProtoPath)

	fctContent, err := os.ReadFile(fctBlockProtoPath)
	require.NoError(t, err)

	fctContentStr := string(fctContent)

	// Verify basic structure
	assert.Contains(t, fctContentStr, "syntax = \"proto3\"")
	assert.Contains(t, fctContentStr, "package clickhouse.v1")
	assert.Contains(t, fctContentStr, "option go_package = \"github.com/test/proto/clickhouse\"")

	// Verify Google API imports are present for fct_ table
	assert.Contains(t, fctContentStr, "import \"google/api/annotations.proto\"")
	assert.Contains(t, fctContentStr, "import \"google/api/field_behavior.proto\"")

	// Verify HTTP annotations
	assert.Contains(t, fctContentStr, "option (google.api.http) = {")
	assert.Contains(t, fctContentStr, "get: \"/api/v1/fct_block\"")

	// Verify pipe-separated comment format for service
	assert.Contains(t, fctContentStr, " | Retrieve a paginated list of")

	// Verify field_behavior annotations
	assert.Contains(t, fctContentStr, "[(google.api.field_behavior) = REQUIRED]")
	assert.Contains(t, fctContentStr, "[(google.api.field_behavior) = OPTIONAL]")

	// Read and verify beacon_api_eth_v1_events_block.proto
	beaconProtoPath := filepath.Join(tempDir, "beacon_api_eth_v1_events_block.proto")
	assert.FileExists(t, beaconProtoPath)

	beaconContent, err := os.ReadFile(beaconProtoPath)
	require.NoError(t, err)

	beaconContentStr := string(beaconContent)

	// Verify basic structure
	assert.Contains(t, beaconContentStr, "syntax = \"proto3\"")

	// Verify Google API imports are NOT present for non-fct_ table
	assert.NotContains(t, beaconContentStr, "import \"google/api/annotations.proto\"")
	assert.NotContains(t, beaconContentStr, "import \"google/api/field_behavior.proto\"")

	// Verify NO HTTP annotations
	assert.NotContains(t, beaconContentStr, "option (google.api.http)")

	// Verify NO field_behavior annotations
	assert.NotContains(t, beaconContentStr, "google.api.field_behavior")
}
