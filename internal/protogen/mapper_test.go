package protogen

import (
	"testing"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/clickhouse"
	"github.com/ethpandaops/clickhouse-proto-gen/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypeMapper_MapType(t *testing.T) {
	tm := NewTypeMapper()

	tests := []struct {
		name     string
		column   clickhouse.Column
		expected string
		wantErr  bool
	}{
		// Basic integer types
		{
			name: "Int8",
			column: clickhouse.Column{
				Name:     "test_int8",
				Type:     "Int8",
				BaseType: "Int8",
			},
			expected: "int32",
		},
		{
			name: "Int16",
			column: clickhouse.Column{
				Name:     "test_int16",
				Type:     "Int16",
				BaseType: "Int16",
			},
			expected: "int32",
		},
		{
			name: "Int32",
			column: clickhouse.Column{
				Name:     "test_int32",
				Type:     "Int32",
				BaseType: "Int32",
			},
			expected: "int32",
		},
		{
			name: "Int64",
			column: clickhouse.Column{
				Name:     "test_int64",
				Type:     "Int64",
				BaseType: "Int64",
			},
			expected: "int64",
		},

		// Unsigned integer types
		{
			name: "UInt8",
			column: clickhouse.Column{
				Name:     "test_uint8",
				Type:     "UInt8",
				BaseType: "UInt8",
			},
			expected: "uint32",
		},
		{
			name: "UInt16",
			column: clickhouse.Column{
				Name:     "test_uint16",
				Type:     "UInt16",
				BaseType: "UInt16",
			},
			expected: "uint32",
		},
		{
			name: "UInt32",
			column: clickhouse.Column{
				Name:     "test_uint32",
				Type:     "UInt32",
				BaseType: "UInt32",
			},
			expected: "uint32",
		},
		{
			name: "UInt64",
			column: clickhouse.Column{
				Name:     "test_uint64",
				Type:     "UInt64",
				BaseType: "UInt64",
			},
			expected: "uint64",
		},

		// Float types
		{
			name: "Float32",
			column: clickhouse.Column{
				Name:     "test_float32",
				Type:     "Float32",
				BaseType: "Float32",
			},
			expected: "float",
		},
		{
			name: "Float64",
			column: clickhouse.Column{
				Name:     "test_float64",
				Type:     "Float64",
				BaseType: "Float64",
			},
			expected: "double",
		},

		// String types
		{
			name: "String",
			column: clickhouse.Column{
				Name:     "test_string",
				Type:     "String",
				BaseType: "String",
			},
			expected: "string",
		},
		{
			name: "FixedString",
			column: clickhouse.Column{
				Name:     "test_fixed_string",
				Type:     "FixedString(10)",
				BaseType: "FixedString",
			},
			expected: "string",
		},

		// Date and time types
		{
			name: "Date",
			column: clickhouse.Column{
				Name:     "test_date",
				Type:     "Date",
				BaseType: "Date",
			},
			expected: "string",
		},
		{
			name: "DateTime",
			column: clickhouse.Column{
				Name:     "test_datetime",
				Type:     "DateTime",
				BaseType: "DateTime",
			},
			expected: "uint32",
		},
		{
			name: "DateTime64",
			column: clickhouse.Column{
				Name:     "test_datetime64",
				Type:     "DateTime64(3)",
				BaseType: "DateTime64",
			},
			expected: "int64",
		},

		// Boolean type
		{
			name: "Bool",
			column: clickhouse.Column{
				Name:     "test_bool",
				Type:     "Bool",
				BaseType: "Bool",
			},
			expected: "bool",
		},

		// UUID type
		{
			name: "UUID",
			column: clickhouse.Column{
				Name:     "test_uuid",
				Type:     "UUID",
				BaseType: "UUID",
			},
			expected: "string",
		},

		// Array types
		{
			name: "Array(Int32)",
			column: clickhouse.Column{
				Name:     "test_array_int32",
				Type:     "Array(Int32)",
				BaseType: "Int32",
				IsArray:  true,
			},
			expected: "repeated int32",
		},
		{
			name: "Array(String)",
			column: clickhouse.Column{
				Name:     "test_array_string",
				Type:     "Array(String)",
				BaseType: "String",
				IsArray:  true,
			},
			expected: "repeated string",
		},
		{
			name: "Array(Float64)",
			column: clickhouse.Column{
				Name:     "test_array_float64",
				Type:     "Array(Float64)",
				BaseType: "Float64",
				IsArray:  true,
			},
			expected: "repeated double",
		},

		// Nullable types with wrapper types
		{
			name: "Nullable(Int32)",
			column: clickhouse.Column{
				Name:       "test_nullable_int32",
				Type:       "Nullable(Int32)",
				BaseType:   "Int32",
				IsNullable: true,
			},
			expected: "google.protobuf.Int32Value",
		},
		{
			name: "Nullable(Int64)",
			column: clickhouse.Column{
				Name:       "test_nullable_int64",
				Type:       "Nullable(Int64)",
				BaseType:   "Int64",
				IsNullable: true,
			},
			expected: "google.protobuf.Int64Value",
		},
		{
			name: "Nullable(String)",
			column: clickhouse.Column{
				Name:       "test_nullable_string",
				Type:       "Nullable(String)",
				BaseType:   "String",
				IsNullable: true,
			},
			expected: "google.protobuf.StringValue",
		},
		{
			name: "Nullable(Float64)",
			column: clickhouse.Column{
				Name:       "test_nullable_float64",
				Type:       "Nullable(Float64)",
				BaseType:   "Float64",
				IsNullable: true,
			},
			expected: "google.protobuf.DoubleValue",
		},
		{
			name: "Nullable(Bool)",
			column: clickhouse.Column{
				Name:       "test_nullable_bool",
				Type:       "Nullable(Bool)",
				BaseType:   "Bool",
				IsNullable: true,
			},
			expected: "google.protobuf.BoolValue",
		},

		// Complex types
		{
			name: "Map(String, Int32)",
			column: clickhouse.Column{
				Name:     "test_map",
				Type:     "Map(String, Int32)",
				BaseType: "Map",
			},
			expected: "map<string, int32>",
		},
		{
			name: "Map(String, String)",
			column: clickhouse.Column{
				Name:     "test_map_string_string",
				Type:     "Map(String, String)",
				BaseType: "Map",
			},
			expected: "map<string, string>",
		},
		{
			name: "Map(String, UInt64)",
			column: clickhouse.Column{
				Name:     "test_map_string_uint64",
				Type:     "Map(String, UInt64)",
				BaseType: "Map",
			},
			expected: "map<string, uint64>",
		},
		{
			name: "Tuple(String, Int32, Float64)",
			column: clickhouse.Column{
				Name:     "test_tuple",
				Type:     "Tuple(String, Int32, Float64)",
				BaseType: "Tuple",
			},
			expected: "string",
		},

		// Enum types
		{
			name: "Enum8",
			column: clickhouse.Column{
				Name:     "test_enum8",
				Type:     "Enum8('a' = 1, 'b' = 2)",
				BaseType: "Enum8",
			},
			expected: "string",
		},
		{
			name: "Enum16",
			column: clickhouse.Column{
				Name:     "test_enum16",
				Type:     "Enum16('a' = 1, 'b' = 2)",
				BaseType: "Enum16",
			},
			expected: "string",
		},

		// LowCardinality wrapper
		{
			name: "LowCardinality(String)",
			column: clickhouse.Column{
				Name:     "test_low_cardinality",
				Type:     "LowCardinality(String)",
				BaseType: "String",
			},
			expected: "string",
		},

		// Large integer types (no native proto support)
		{
			name: "Int128",
			column: clickhouse.Column{
				Name:     "test_int128",
				Type:     "Int128",
				BaseType: "Int128",
			},
			expected: "string",
		},
		{
			name: "Int256",
			column: clickhouse.Column{
				Name:     "test_int256",
				Type:     "Int256",
				BaseType: "Int256",
			},
			expected: "string",
		},
		{
			name: "UInt128",
			column: clickhouse.Column{
				Name:     "test_uint128",
				Type:     "UInt128",
				BaseType: "UInt128",
			},
			expected: "string",
		},
		{
			name: "UInt256",
			column: clickhouse.Column{
				Name:     "test_uint256",
				Type:     "UInt256",
				BaseType: "UInt256",
			},
			expected: "string",
		},

		// Decimal types
		{
			name: "Decimal32",
			column: clickhouse.Column{
				Name:     "test_decimal32",
				Type:     "Decimal32(2)",
				BaseType: "Decimal32",
			},
			expected: "string",
		},
		{
			name: "Decimal64",
			column: clickhouse.Column{
				Name:     "test_decimal64",
				Type:     "Decimal64(2)",
				BaseType: "Decimal64",
			},
			expected: "string",
		},
		{
			name: "Decimal128",
			column: clickhouse.Column{
				Name:     "test_decimal128",
				Type:     "Decimal128(2)",
				BaseType: "Decimal128",
			},
			expected: "string",
		},

		// IP address types
		{
			name: "IPv4",
			column: clickhouse.Column{
				Name:     "test_ipv4",
				Type:     "IPv4",
				BaseType: "IPv4",
			},
			expected: "string",
		},
		{
			name: "IPv6",
			column: clickhouse.Column{
				Name:     "test_ipv6",
				Type:     "IPv6",
				BaseType: "IPv6",
			},
			expected: "string",
		},

		// JSON type
		{
			name: "JSON",
			column: clickhouse.Column{
				Name:     "test_json",
				Type:     "JSON",
				BaseType: "JSON",
			},
			expected: "string",
		},

		// Unknown type defaults to string
		{
			name: "UnknownType",
			column: clickhouse.Column{
				Name:     "test_unknown",
				Type:     "UnknownType",
				BaseType: "UnknownType",
			},
			expected: "string",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tm.MapType(&tt.column, "test_table", &config.ConversionConfig{})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result, "Type mapping mismatch for %s", tt.name)
			}
		})
	}
}

func TestTypeMapper_ConvertColumn(t *testing.T) {
	tm := NewTypeMapper()

	tests := []struct {
		name     string
		column   clickhouse.Column
		expected ProtoField
		wantErr  bool
	}{
		{
			name: "Simple column",
			column: clickhouse.Column{
				Name:     "user_id",
				Type:     "UInt64",
				BaseType: "UInt64",
				Position: 1,
				Comment:  "User identifier",
			},
			expected: ProtoField{
				Name:    "user_id",
				Type:    "uint64",
				Number:  11,
				Comment: "User identifier",
			},
		},
		{
			name: "Column with special characters",
			column: clickhouse.Column{
				Name:     "user-name",
				Type:     "String",
				BaseType: "String",
				Position: 2,
			},
			expected: ProtoField{
				Name:    "user_name",
				Type:    "string",
				Number:  12,
				Comment: "",
			},
		},
		{
			name: "Array column",
			column: clickhouse.Column{
				Name:     "tags",
				Type:     "Array(String)",
				BaseType: "String",
				Position: 3,
				IsArray:  true,
			},
			expected: ProtoField{
				Name:    "tags",
				Type:    "repeated string",
				Number:  13,
				Comment: "",
			},
		},
		{
			name: "Nullable column",
			column: clickhouse.Column{
				Name:       "age",
				Type:       "Nullable(Int32)",
				BaseType:   "Int32",
				Position:   4,
				IsNullable: true,
			},
			expected: ProtoField{
				Name:    "age",
				Type:    "google.protobuf.Int32Value",
				Number:  14,
				Comment: "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tm.ConvertColumn(&tt.column, "test_table", &config.ConversionConfig{})

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, *result)
			}
		})
	}
}

func TestTypeMapper_ParseMapType(t *testing.T) {
	tm := NewTypeMapper()

	tests := []struct {
		name          string
		mapType       string
		expectedKey   string
		expectedValue string
	}{
		{
			name:          "Map(String, String)",
			mapType:       "Map(String, String)",
			expectedKey:   "String",
			expectedValue: "String",
		},
		{
			name:          "Map(String, UInt32)",
			mapType:       "Map(String, UInt32)",
			expectedKey:   "String",
			expectedValue: "UInt32",
		},
		{
			name:          "Map(String, Nullable(UInt32))",
			mapType:       "Map(String, Nullable(UInt32))",
			expectedKey:   "String",
			expectedValue: "UInt32",
		},
		{
			name:          "Map(String, Int64)",
			mapType:       "Map(String, Int64)",
			expectedKey:   "String",
			expectedValue: "Int64",
		},
		{
			name:          "Invalid map type",
			mapType:       "NotAMap(String, String)",
			expectedKey:   "",
			expectedValue: "",
		},
		{
			name:          "Map with nested types",
			mapType:       "Map(String, LowCardinality(String))",
			expectedKey:   "String",
			expectedValue: "LowCardinality(String)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, value := tm.parseMapType(tt.mapType)
			assert.Equal(t, tt.expectedKey, key)
			assert.Equal(t, tt.expectedValue, value)
		})
	}
}

func TestTypeMapper_GetFilterTypeForColumn(t *testing.T) {
	tm := NewTypeMapper()

	tests := []struct {
		name     string
		column   clickhouse.Column
		expected string
	}{
		{
			name: "Regular Int32 column",
			column: clickhouse.Column{
				Name:     "count",
				Type:     "Int32",
				BaseType: "Int32",
			},
			expected: "Int32Filter",
		},
		{
			name: "Nullable Int32 column",
			column: clickhouse.Column{
				Name:       "nullable_count",
				Type:       "Nullable(Int32)",
				BaseType:   "Int32",
				IsNullable: true,
			},
			expected: "NullableInt32Filter",
		},
		{
			name: "String column",
			column: clickhouse.Column{
				Name:     "name",
				Type:     "String",
				BaseType: "String",
			},
			expected: "StringFilter",
		},
		{
			name: "Nullable String column",
			column: clickhouse.Column{
				Name:       "nullable_name",
				Type:       "Nullable(String)",
				BaseType:   "String",
				IsNullable: true,
			},
			expected: "NullableStringFilter",
		},
		{
			name: "UInt64 column",
			column: clickhouse.Column{
				Name:     "id",
				Type:     "UInt64",
				BaseType: "UInt64",
			},
			expected: "UInt64Filter",
		},
		{
			name: "Float64 column",
			column: clickhouse.Column{
				Name:     "price",
				Type:     "Float64",
				BaseType: "Float64",
			},
			expected: "", // Float types don't have filter support in current implementation
		},
		{
			name: "Array column (no filter type)",
			column: clickhouse.Column{
				Name:     "tags",
				Type:     "Array(String)",
				BaseType: "String",
				IsArray:  true,
			},
			expected: "",
		},
		{
			name: "Map(String, String)",
			column: clickhouse.Column{
				Name:     "metadata",
				Type:     "Map(String, String)",
				BaseType: "Map",
			},
			expected: "MapStringStringFilter",
		},
		{
			name: "Map(String, UInt32)",
			column: clickhouse.Column{
				Name:     "metrics",
				Type:     "Map(String, UInt32)",
				BaseType: "Map",
			},
			expected: "MapStringUInt32Filter",
		},
		{
			name: "Map(String, Int64)",
			column: clickhouse.Column{
				Name:     "counters",
				Type:     "Map(String, Int64)",
				BaseType: "Map",
			},
			expected: "MapStringInt64Filter",
		},
		{
			name: "Map(UInt32, String) - unsupported",
			column: clickhouse.Column{
				Name:     "reverse_map",
				Type:     "Map(UInt32, String)",
				BaseType: "Map",
			},
			expected: "", // Unsupported Map type combination
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tm.GetFilterTypeForColumn(&tt.column, "test_table", &config.ConversionConfig{})
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTypeMapper_WrapperTypes(t *testing.T) {
	tm := NewTypeMapper()

	tests := []struct {
		name      string
		protoType string
		expected  string
	}{
		{
			name:      "int32 wrapper",
			protoType: "int32",
			expected:  "google.protobuf.Int32Value",
		},
		{
			name:      "int64 wrapper",
			protoType: "int64",
			expected:  "google.protobuf.Int64Value",
		},
		{
			name:      "uint32 wrapper",
			protoType: "uint32",
			expected:  "google.protobuf.UInt32Value",
		},
		{
			name:      "uint64 wrapper",
			protoType: "uint64",
			expected:  "google.protobuf.UInt64Value",
		},
		{
			name:      "float wrapper",
			protoType: "float",
			expected:  "google.protobuf.FloatValue",
		},
		{
			name:      "double wrapper",
			protoType: "double",
			expected:  "google.protobuf.DoubleValue",
		},
		{
			name:      "bool wrapper",
			protoType: "bool",
			expected:  "google.protobuf.BoolValue",
		},
		{
			name:      "string wrapper",
			protoType: "string",
			expected:  "google.protobuf.StringValue",
		},
		{
			name:      "bytes wrapper",
			protoType: "bytes",
			expected:  "google.protobuf.BytesValue",
		},
		{
			name:      "unknown type (no wrapper)",
			protoType: "custom_type",
			expected:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tm.getWrapperType(tt.protoType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTypeMapper_GetWrapperTypeForColumn(t *testing.T) {
	tm := NewTypeMapper()

	tests := []struct {
		name     string
		column   clickhouse.Column
		expected string
	}{
		{
			name: "Int32 column",
			column: clickhouse.Column{
				BaseType: "Int32",
			},
			expected: "google.protobuf.Int32Value",
		},
		{
			name: "String column",
			column: clickhouse.Column{
				BaseType: "String",
			},
			expected: "google.protobuf.StringValue",
		},
		{
			name: "Array column",
			column: clickhouse.Column{
				BaseType: "String",
				IsArray:  true,
			},
			expected: "repeated string", // Arrays use repeated base type, not wrapper type
		},
		{
			name: "Unknown type",
			column: clickhouse.Column{
				BaseType: "CustomType",
			},
			expected: "google.protobuf.StringValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tm.getWrapperTypeForColumn(&tt.column)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestIsFixedString(t *testing.T) {
	tests := []struct {
		name            string
		chType          string
		expectedIsFixed bool
		expectedLength  int
	}{
		{
			name:            "FixedString(32)",
			chType:          "FixedString(32)",
			expectedIsFixed: true,
			expectedLength:  32,
		},
		{
			name:            "FixedString(66)",
			chType:          "FixedString(66)",
			expectedIsFixed: true,
			expectedLength:  66,
		},
		{
			name:            "FixedString(10)",
			chType:          "FixedString(10)",
			expectedIsFixed: true,
			expectedLength:  10,
		},
		{
			name:            "Nullable(FixedString(32))",
			chType:          "Nullable(FixedString(32))",
			expectedIsFixed: true,
			expectedLength:  32,
		},
		{
			name:            "Nullable(FixedString(66))",
			chType:          "Nullable(FixedString(66))",
			expectedIsFixed: true,
			expectedLength:  66,
		},
		{
			name:            "String",
			chType:          "String",
			expectedIsFixed: false,
			expectedLength:  0,
		},
		{
			name:            "Nullable(String)",
			chType:          "Nullable(String)",
			expectedIsFixed: false,
			expectedLength:  0,
		},
		{
			name:            "Int32",
			chType:          "Int32",
			expectedIsFixed: false,
			expectedLength:  0,
		},
		{
			name:            "Invalid FixedString",
			chType:          "FixedString(abc)",
			expectedIsFixed: false,
			expectedLength:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isFixed, length := IsFixedString(tt.chType)
			assert.Equal(t, tt.expectedIsFixed, isFixed, "IsFixed mismatch for %s", tt.chType)
			assert.Equal(t, tt.expectedLength, length, "Length mismatch for %s", tt.chType)
		})
	}
}

// TestMapType_BigIntToStringConversion tests that UInt64 fields are converted to string
// when they are whitelisted in the conversion configuration
func TestMapType_BigIntToStringConversion(t *testing.T) {
	tm := NewTypeMapper()

	tests := []struct {
		name      string
		column    clickhouse.Column
		tableName string
		config    config.ConversionConfig
		expected  string
		desc      string
	}{
		// Regular UInt64 conversion
		{
			name: "UInt64 converted to string when whitelisted",
			column: clickhouse.Column{
				Name:     "consensus_payload_value",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			expected: "string",
			desc:     "UInt64 → string for JavaScript precision",
		},
		{
			name: "UInt64 not converted when not whitelisted",
			column: clickhouse.Column{
				Name:     "block_number",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "fct_prepared_block",
			config:    config.ConversionConfig{},
			expected:  "uint64",
			desc:      "UInt64 stays uint64 when not whitelisted",
		},

		// Nullable UInt64 conversion
		{
			name: "Nullable(UInt64) converted to StringValue when whitelisted",
			column: clickhouse.Column{
				Name:       "consensus_payload_value",
				BaseType:   "UInt64",
				Type:       "Nullable(UInt64)",
				IsNullable: true,
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			expected: "google.protobuf.StringValue",
			desc:     "Nullable(UInt64) → StringValue",
		},
		{
			name: "Nullable(UInt64) not converted when not whitelisted",
			column: clickhouse.Column{
				Name:       "block_number",
				BaseType:   "UInt64",
				Type:       "Nullable(UInt64)",
				IsNullable: true,
			},
			tableName: "fct_prepared_block",
			config:    config.ConversionConfig{},
			expected:  "google.protobuf.UInt64Value",
			desc:      "Nullable(UInt64) stays UInt64Value when not whitelisted",
		},

		// Array(UInt64) conversion
		{
			name: "Array(UInt64) converted to repeated string when whitelisted",
			column: clickhouse.Column{
				Name:     "values",
				BaseType: "UInt64",
				Type:     "Array(UInt64)",
				IsArray:  true,
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"values"},
				},
			},
			expected: "repeated string",
			desc:     "Array(UInt64) → repeated string",
		},
		{
			name: "Array(UInt64) not converted when not whitelisted",
			column: clickhouse.Column{
				Name:     "values",
				BaseType: "UInt64",
				Type:     "Array(UInt64)",
				IsArray:  true,
			},
			tableName: "fct_prepared_block",
			config:    config.ConversionConfig{},
			expected:  "repeated uint64",
			desc:      "Array(UInt64) stays repeated uint64 when not whitelisted",
		},

		// CLI pattern matching
		{
			name: "UInt64 converted via CLI wildcard pattern *.*",
			column: clickhouse.Column{
				Name:     "any_uint64_field",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "any_table",
			config: config.ConversionConfig{
				BigIntToStringFields: []string{"*.*"},
			},
			expected: "string",
			desc:     "*.* pattern converts all UInt64 fields",
		},
		{
			name: "UInt64 converted via CLI specific field pattern *.field",
			column: clickhouse.Column{
				Name:     "block_number",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "any_table",
			config: config.ConversionConfig{
				BigIntToStringFields: []string{"*.block_number"},
			},
			expected: "string",
			desc:     "*.field pattern converts specific field across all tables",
		},
		{
			name: "UInt64 converted via CLI table wildcard pattern table.*",
			column: clickhouse.Column{
				Name:     "any_field",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToStringFields: []string{"fct_prepared_block.*"},
			},
			expected: "string",
			desc:     "table.* pattern converts all fields in specific table",
		},

		// Non-UInt64 types should not be affected
		{
			name: "UInt32 not affected by UInt64 conversion config",
			column: clickhouse.Column{
				Name:     "consensus_payload_value",
				BaseType: "UInt32",
				Type:     "UInt32",
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			expected: "uint32",
			desc:     "UInt32 stays uint32 even if whitelisted",
		},
		{
			name: "String not affected by UInt64 conversion config",
			column: clickhouse.Column{
				Name:     "consensus_payload_value",
				BaseType: "String",
				Type:     "String",
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			expected: "string",
			desc:     "String stays string even if whitelisted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tm.MapType(&tt.column, tt.tableName, &tt.config)
			require.NoError(t, err, "MapType should not error")
			assert.Equal(t, tt.expected, result, tt.desc)
		})
	}
}

// TestGetFilterTypeForColumn_BigIntToStringConversion tests that filter types are correct
// for converted UInt64 fields
func TestGetFilterTypeForColumn_BigIntToStringConversion(t *testing.T) {
	tm := NewTypeMapper()

	tests := []struct {
		name      string
		column    clickhouse.Column
		tableName string
		config    config.ConversionConfig
		expected  string
		desc      string
	}{
		{
			name: "UInt64 filter becomes StringFilter when whitelisted",
			column: clickhouse.Column{
				Name:     "consensus_payload_value",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			expected: "StringFilter",
			desc:     "Converted UInt64 uses StringFilter",
		},
		{
			name: "UInt64 filter stays UInt64Filter when not whitelisted",
			column: clickhouse.Column{
				Name:     "block_number",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "fct_prepared_block",
			config:    config.ConversionConfig{},
			expected:  "UInt64Filter",
			desc:      "Non-converted UInt64 uses UInt64Filter",
		},
		{
			name: "Nullable(UInt64) filter becomes NullableStringFilter when whitelisted",
			column: clickhouse.Column{
				Name:       "consensus_payload_value",
				BaseType:   "UInt64",
				Type:       "Nullable(UInt64)",
				IsNullable: true,
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			expected: "NullableStringFilter",
			desc:     "Converted Nullable(UInt64) uses NullableStringFilter",
		},
		{
			name: "Nullable(UInt64) filter stays NullableUInt64Filter when not whitelisted",
			column: clickhouse.Column{
				Name:       "block_number",
				BaseType:   "UInt64",
				Type:       "Nullable(UInt64)",
				IsNullable: true,
			},
			tableName: "fct_prepared_block",
			config:    config.ConversionConfig{},
			expected:  "NullableUInt64Filter",
			desc:      "Non-converted Nullable(UInt64) uses NullableUInt64Filter",
		},
		{
			name: "Array(UInt64) has no filter regardless of conversion",
			column: clickhouse.Column{
				Name:     "values",
				BaseType: "UInt64",
				Type:     "Array(UInt64)",
				IsArray:  true,
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"values"},
				},
			},
			expected: "",
			desc:     "Arrays don't have filters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tm.GetFilterTypeForColumn(&tt.column, tt.tableName, &tt.config)
			assert.Equal(t, tt.expected, result, tt.desc)
		})
	}
}

// TestGetSelectColumnExpression_BigIntToStringConversion tests that SQL SELECT expressions
// use toString() for converted UInt64 fields
func TestGetSelectColumnExpression_BigIntToStringConversion(t *testing.T) {
	tests := []struct {
		name      string
		column    clickhouse.Column
		tableName string
		config    config.ConversionConfig
		expected  string
		desc      string
	}{
		{
			name: "UInt64 wrapped with toString() when whitelisted",
			column: clickhouse.Column{
				Name:     "consensus_payload_value",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			expected: "toString(`consensus_payload_value`) AS `consensus_payload_value`",
			desc:     "Converted UInt64 uses toString()",
		},
		{
			name: "UInt64 not wrapped when not whitelisted",
			column: clickhouse.Column{
				Name:     "block_number",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "fct_prepared_block",
			config:    config.ConversionConfig{},
			expected:  "block_number",
			desc:      "Non-converted UInt64 uses plain column name",
		},
		{
			name: "Array(UInt64) wrapped with arrayMap toString() when whitelisted",
			column: clickhouse.Column{
				Name:     "values",
				BaseType: "UInt64",
				Type:     "Array(UInt64)",
				IsArray:  true,
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"values"},
				},
			},
			expected: "arrayMap(x -> toString(x), `values`) AS `values`",
			desc:     "Converted Array(UInt64) uses arrayMap with toString()",
		},
		{
			name: "Array(Nullable(UInt64)) with NULL handling when whitelisted",
			column: clickhouse.Column{
				Name:       "values",
				BaseType:   "UInt64",
				Type:       "Array(Nullable(UInt64))",
				IsArray:    true,
				IsNullable: true,
			},
			tableName: "fct_prepared_block",
			config: config.ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"values"},
				},
			},
			expected: "arrayMap(x -> toString(coalesce(x, 0)), `values`) AS `values`",
			desc:     "Converted Array(Nullable(UInt64)) uses arrayMap with coalesce",
		},
		{
			name: "UInt64 converted via wildcard pattern *.*",
			column: clickhouse.Column{
				Name:     "any_field",
				BaseType: "UInt64",
				Type:     "UInt64",
			},
			tableName: "any_table",
			config: config.ConversionConfig{
				BigIntToStringFields: []string{"*.*"},
			},
			expected: "toString(`any_field`) AS `any_field`",
			desc:     "*.* pattern applies toString() to all UInt64 fields",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSelectColumnExpression(&tt.column, tt.tableName, &tt.config)
			assert.Equal(t, tt.expected, result, tt.desc)
		})
	}
}
