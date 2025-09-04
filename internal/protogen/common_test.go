package protogen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/config"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSanitizeName(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		// Basic names
		{
			name:     "Simple valid name",
			input:    "user_id",
			expected: "user_id",
		},
		{
			name:     "Name with letters and numbers",
			input:    "field123",
			expected: "field123",
		},
		{
			name:     "Uppercase letters",
			input:    "UserID",
			expected: "UserID",
		},
		{
			name:     "Mixed case with underscores",
			input:    "User_ID_123",
			expected: "User_ID_123",
		},

		// Names with special characters
		{
			name:     "Name with hyphens",
			input:    "user-name",
			expected: "user_name",
		},
		{
			name:     "Name with dots",
			input:    "user.name",
			expected: "user_name",
		},
		{
			name:     "Name with spaces",
			input:    "user name",
			expected: "user_name",
		},
		{
			name:     "Name with multiple special chars",
			input:    "user@name#field$test",
			expected: "user_name_field_test",
		},
		{
			name:     "Name with parentheses",
			input:    "function(arg)",
			expected: "function_arg_",
		},
		{
			name:     "Name with brackets",
			input:    "array[0]",
			expected: "array_0_",
		},

		// Names starting with numbers
		{
			name:     "Name starting with number",
			input:    "123field",
			expected: "f_123field",
		},
		{
			name:     "Name starting with zero",
			input:    "0value",
			expected: "f_0value",
		},
		{
			name:     "All numbers",
			input:    "12345",
			expected: "f_12345",
		},

		// Reserved keywords
		{
			name:     "Reserved keyword: message",
			input:    "message",
			expected: "message_field",
		},
		{
			name:     "Reserved keyword: service",
			input:    "service",
			expected: "service_field",
		},
		{
			name:     "Reserved keyword: string",
			input:    "string",
			expected: "string_field",
		},
		{
			name:     "Reserved keyword: bool",
			input:    "bool",
			expected: "bool_field",
		},
		{
			name:     "Reserved keyword: int32",
			input:    "int32",
			expected: "int32_field",
		},
		{
			name:     "Reserved keyword uppercase",
			input:    "MESSAGE",
			expected: "MESSAGE_field",
		},
		{
			name:     "Reserved keyword mixed case",
			input:    "Message",
			expected: "Message_field",
		},

		// Edge cases
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "Only underscores",
			input:    "___",
			expected: "___",
		},
		{
			name:     "Only special characters",
			input:    "@#$%",
			expected: "____",
		},
		{
			name:     "Unicode characters",
			input:    "user_名前_field",
			expected: "user____field", // Two Unicode characters (名 and 前) each replaced with underscore
		},
		{
			name:     "Name with emoji",
			input:    "field_😀_test",
			expected: "field___test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := SanitizeName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsReservedKeyword(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Proto keywords
		{"Proto keyword: syntax", "syntax", true},
		{"Proto keyword: package", "package", true},
		{"Proto keyword: import", "import", true},
		{"Proto keyword: message", "message", true},
		{"Proto keyword: enum", "enum", true},
		{"Proto keyword: service", "service", true},
		{"Proto keyword: rpc", "rpc", true},
		{"Proto keyword: returns", "returns", true},
		{"Proto keyword: repeated", "repeated", true},
		{"Proto keyword: optional", "optional", true},
		{"Proto keyword: required", "required", true},
		{"Proto keyword: reserved", "reserved", true},
		{"Proto keyword: oneof", "oneof", true},
		{"Proto keyword: map", "map", true},

		// Type keywords
		{"Type keyword: bool", "bool", true},
		{"Type keyword: string", "string", true},
		{"Type keyword: bytes", "bytes", true},
		{"Type keyword: float", "float", true},
		{"Type keyword: double", "double", true},
		{"Type keyword: int32", "int32", true},
		{"Type keyword: int64", "int64", true},
		{"Type keyword: uint32", "uint32", true},
		{"Type keyword: uint64", "uint64", true},
		{"Type keyword: sint32", "sint32", true},
		{"Type keyword: sint64", "sint64", true},
		{"Type keyword: fixed32", "fixed32", true},
		{"Type keyword: fixed64", "fixed64", true},
		{"Type keyword: sfixed32", "sfixed32", true},
		{"Type keyword: sfixed64", "sfixed64", true},

		// Case insensitive
		{"Uppercase keyword", "MESSAGE", true},
		{"Mixed case keyword", "Message", true},
		{"Lowercase keyword", "message", true},

		// Non-keywords
		{"Regular word", "user", false},
		{"Regular word", "field", false},
		{"Regular word", "value", false},
		{"Regular word", "data", false},
		{"Regular word with underscore", "user_id", false},
		{"Empty string", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isReservedKeyword(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToPascalCase(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Simple snake_case",
			input:    "user_name",
			expected: "UserName",
		},
		{
			name:     "Single word",
			input:    "user",
			expected: "User",
		},
		{
			name:     "Multiple underscores",
			input:    "user_profile_settings",
			expected: "UserProfileSettings",
		},
		{
			name:     "Already PascalCase",
			input:    "UserName",
			expected: "Username",
		},
		{
			name:     "Mixed case input",
			input:    "USER_NAME",
			expected: "UserName",
		},
		{
			name:     "Numbers in name",
			input:    "field_123_test",
			expected: "Field123Test",
		},
		{
			name:     "Leading underscore",
			input:    "_private_field",
			expected: "PrivateField",
		},
		{
			name:     "Trailing underscore",
			input:    "field_name_",
			expected: "FieldName",
		},
		{
			name:     "Multiple consecutive underscores",
			input:    "field__name",
			expected: "FieldName",
		},
		{
			name:     "Empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "Only underscores",
			input:    "___",
			expected: "",
		},
		{
			name:     "Single character",
			input:    "x",
			expected: "X",
		},
		{
			name:     "Acronym handling",
			input:    "http_api_client",
			expected: "HttpApiClient",
		},
		{
			name:     "Mixed numbers and letters",
			input:    "field1_name2",
			expected: "Field1Name2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ToPascalCase(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGetFieldNumber(t *testing.T) {
	tests := []struct {
		name     string
		position uint64
		expected int32
	}{
		{
			name:     "Position 1",
			position: 1,
			expected: 11,
		},
		{
			name:     "Position 2",
			position: 2,
			expected: 12,
		},
		{
			name:     "Position 10",
			position: 10,
			expected: 20,
		},
		{
			name:     "Position 100",
			position: 100,
			expected: 110,
		},
		{
			name:     "Position 0",
			position: 0,
			expected: 10,
		},
		{
			name:     "Large position",
			position: 1000,
			expected: 1010,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetFieldNumber(tt.position)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestGenerator_GenerateCommonProto(t *testing.T) {
	// Create a temp directory for test output
	tempDir, err := os.MkdirTemp("", "common_proto_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name            string
		config          *config.Config
		expectedContent []string
		notExpected     []string
	}{
		{
			name: "Full configuration",
			config: &config.Config{
				OutputDir:       tempDir,
				Package:         "test.v1",
				GoPackage:       "github.com/test/proto",
				IncludeComments: true,
			},
			expectedContent: []string{
				"syntax = \"proto3\"",
				"package test.v1",
				"option go_package = \"github.com/test/proto\"",
				"import \"google/protobuf/wrappers.proto\"",
				"import \"google/protobuf/empty.proto\"",
				"// Common types used across all generated services",
				"message UInt32Filter",
				"message NullableUInt32Filter",
				"message UInt64Filter",
				"message NullableUInt64Filter",
				"message Int32Filter",
				"message NullableInt32Filter",
				"message Int64Filter",
				"message NullableInt64Filter",
				"message StringFilter",
				"message NullableStringFilter",
				"message UInt32Range",
				"message UInt64Range",
				"message Int32Range",
				"message Int64Range",
				"message StringList",
				"enum SortOrder",
				"ASC = 0",
				"DESC = 1",
				"google.protobuf.Empty is_null",
				"google.protobuf.Empty is_not_null",
			},
			notExpected: []string{},
		},
		{
			name: "Minimal configuration",
			config: &config.Config{
				OutputDir: tempDir,
				Package:   "minimal.v1",
			},
			expectedContent: []string{
				"syntax = \"proto3\"",
				"package minimal.v1",
				"import \"google/protobuf/wrappers.proto\"",
				"import \"google/protobuf/empty.proto\"",
			},
			notExpected: []string{
				"option go_package",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)

			gen := NewGenerator(tt.config, log)
			err := gen.GenerateCommonProto()
			require.NoError(t, err)

			// Read the generated file
			commonProtoPath := filepath.Join(tempDir, "common.proto")
			require.FileExists(t, commonProtoPath)

			content, err := os.ReadFile(commonProtoPath)
			require.NoError(t, err)

			contentStr := string(content)

			// Check expected content
			for _, expected := range tt.expectedContent {
				assert.Contains(t, contentStr, expected, "Expected content not found: %s", expected)
			}

			// Check not expected content
			for _, notExpected := range tt.notExpected {
				assert.NotContains(t, contentStr, notExpected, "Unexpected content found: %s", notExpected)
			}

			// Clean up for next test
			os.Remove(commonProtoPath)
		})
	}
}

func TestGenerator_WriteRangeTypes(t *testing.T) {
	cfg := &config.Config{
		IncludeComments: true,
	}
	log := logrus.New()
	gen := NewGenerator(cfg, log)

	var sb strings.Builder
	gen.writeRangeTypes(&sb)
	result := sb.String()

	// Check that all filter types are present
	expectedFilters := []string{
		"message UInt32Filter",
		"message NullableUInt32Filter",
		"message UInt64Filter",
		"message NullableUInt64Filter",
		"message Int32Filter",
		"message NullableInt32Filter",
		"message Int64Filter",
		"message NullableInt64Filter",
		"message StringFilter",
		"message NullableStringFilter",
	}

	for _, filter := range expectedFilters {
		assert.Contains(t, result, filter)
	}

	// Check filter operations
	expectedOps := []string{
		"eq = 1",
		"ne = 2",
		"lt = 3",
		"lte = 4",
		"gt = 5",
		"gte = 6",
		"between = 7",
		"in = 8",
		"not_in = 9",
	}

	for _, op := range expectedOps {
		assert.Contains(t, result, op)
	}

	// Check nullable-specific operations
	assert.Contains(t, result, "google.protobuf.Empty is_null")
	assert.Contains(t, result, "google.protobuf.Empty is_not_null")

	// Check string-specific operations
	assert.Contains(t, result, "contains = 3")
	assert.Contains(t, result, "starts_with = 4")
	assert.Contains(t, result, "ends_with = 5")
	assert.Contains(t, result, "like = 6")
	assert.Contains(t, result, "not_like = 7")
	assert.Contains(t, result, "in = 8")
	assert.Contains(t, result, "not_in = 9")

	// Check range types
	assert.Contains(t, result, "message UInt32Range")
	assert.Contains(t, result, "message UInt64Range")
	assert.Contains(t, result, "message Int32Range")
	assert.Contains(t, result, "message Int64Range")

	// Check list types
	assert.Contains(t, result, "message UInt32List")
	assert.Contains(t, result, "message UInt64List")
	assert.Contains(t, result, "message Int32List")
	assert.Contains(t, result, "message Int64List")
	assert.Contains(t, result, "message StringList")
}

func TestGenerator_WriteCommonTypes(t *testing.T) {
	cfg := &config.Config{
		IncludeComments: true,
	}
	log := logrus.New()
	gen := NewGenerator(cfg, log)

	var sb strings.Builder
	gen.writeCommonTypes(&sb)
	result := sb.String()

	// Check SortOrder enum
	assert.Contains(t, result, "enum SortOrder")
	assert.Contains(t, result, "ASC = 0")
	assert.Contains(t, result, "DESC = 1")

	// Check comments
	assert.Contains(t, result, "// SortOrder defines the order of results")
}
