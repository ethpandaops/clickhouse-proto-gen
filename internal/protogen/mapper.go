// Package protogen handles the generation of Protocol Buffer schemas from ClickHouse tables
package protogen

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/clickhouse"
	"github.com/ethpandaops/clickhouse-proto-gen/internal/config"
)

// Proto type constants
const (
	protoInt32  = "int32"
	protoInt64  = "int64"
	protoUInt32 = "uint32"
	protoUInt64 = "uint64"
	protoFloat  = "float"
	protoDouble = "double"
	protoString = "string"
	protoBool   = "bool"
	protoBytes  = "bytes"

	// ClickHouse type names
	chTypeString = "String"
)

// TypeMapper handles conversion of ClickHouse types to Protobuf types
type TypeMapper struct{}

// NewTypeMapper creates a new TypeMapper
func NewTypeMapper() *TypeMapper {
	return &TypeMapper{}
}

// MapType converts a ClickHouse type to a Protobuf type
func (tm *TypeMapper) MapType(column *clickhouse.Column, tableName string, convConfig *config.ConversionConfig) (string, error) {
	baseType := column.BaseType

	// Check if this Int64/UInt64 field should be converted to string for JavaScript precision
	if (baseType == typeUInt64 || baseType == typeInt64) && convConfig.ShouldConvertToString(tableName, column.Name) {
		// Handle Array(Int64/UInt64) → repeated string
		if column.IsArray {
			return "repeated string", nil
		}
		// Handle Nullable(Int64/UInt64) → google.protobuf.StringValue
		if column.IsNullable {
			return "google.protobuf.StringValue", nil
		}
		// Regular Int64/UInt64 → string
		return protoString, nil
	}

	// Check for repeated field (Array)
	var repeated bool
	if column.IsArray {
		repeated = true
	}

	// Map base types
	protoType := tm.mapBaseType(baseType, column.Type)

	// Handle repeated modifier
	if repeated {
		return "repeated " + protoType, nil
	}

	// Handle nullable fields with Google wrapper types
	if column.IsNullable && !repeated {
		wrappedType := tm.getWrapperType(protoType)
		if wrappedType != "" {
			return wrappedType, nil
		}
	}

	return protoType, nil
}

func (tm *TypeMapper) mapBaseType(baseType, fullType string) string {
	// Handle DateTime64 specially to check precision
	if baseType == "DateTime64" {
		// DateTime64 uses int64 because toUnixTimestamp64Micro() returns Int64
		// The precision affects interpretation but not storage type
		return protoInt64
	}

	// Handle numeric types
	if protoType := tm.mapNumericType(baseType); protoType != "" {
		return protoType
	}

	// Handle string and text types
	if protoType := tm.mapStringType(baseType); protoType != "" {
		return protoType
	}

	// Handle special types
	if protoType := tm.mapSpecialType(baseType, fullType); protoType != "" {
		return protoType
	}

	// Unknown type, default to string
	return protoString
}

func (tm *TypeMapper) mapNumericType(baseType string) string {
	switch baseType {
	// Integer types
	case "Int8", "Int16", "Int32":
		return protoInt32
	case "Int64":
		return protoInt64
	case "Int128", "Int256":
		return protoString // No native int128/256 in protobuf

	// Unsigned integer types
	case "UInt8", "UInt16", "UInt32":
		return protoUInt32
	case typeUInt64:
		return protoUInt64
	case "UInt128", "UInt256":
		return protoString // No native uint128/256 in protobuf

	// Float types
	case "Float32":
		return protoFloat
	case "Float64":
		return protoDouble

	// Decimal types
	case "Decimal", "Decimal32", "Decimal64", "Decimal128", "Decimal256":
		return protoString // Represent decimals as strings to preserve precision

	// Boolean type
	case "Bool":
		return protoBool

	// DateTime type
	case "DateTime":
		return protoUInt32 // Unix timestamp in seconds
	}

	return ""
}

func (tm *TypeMapper) mapStringType(baseType string) string {
	switch baseType {
	// String types
	case chTypeString, "FixedString":
		return protoString

	// Date types (as strings for readability)
	case "Date", "Date32":
		return protoString // YYYY-MM-DD format

	// UUID type
	case "UUID":
		return protoString

	// IP address types
	case "IPv4", "IPv6":
		return protoString

	// JSON type
	case "JSON":
		return protoString

	// Binary data
	case "Binary":
		return protoBytes

	// Enum types
	case "Enum8", "Enum16":
		return protoString // Would need special handling for enum definitions

	// Geo types
	case "Point", "Ring", "Polygon", "MultiPolygon":
		return protoString // Could be custom message types
	}

	return ""
}

func (tm *TypeMapper) mapSpecialType(baseType, fullType string) string {
	switch baseType {
	// LowCardinality wrapper - use the inner type
	case "LowCardinality":
		if idx := strings.Index(fullType, "("); idx > 0 {
			innerType := extractInnerType(fullType)
			return tm.mapBaseType(innerType, innerType)
		}
		return protoString

	// Map type - use protobuf's native map syntax
	case "Map":
		keyType, valueType := tm.parseMapType(fullType)
		if keyType == "" || valueType == "" {
			// Invalid map format, fallback to string
			return protoString
		}

		// Map ClickHouse key type to protobuf key type
		protoKeyType := tm.mapClickHouseTypeToProto(keyType)
		if !tm.isValidProtoMapKey(protoKeyType) {
			// Protobuf only allows certain key types for maps
			// If invalid, fallback to string representation
			return protoString
		}

		// Map ClickHouse value type to protobuf value type
		protoValueType := tm.mapClickHouseTypeToProto(valueType)

		// Return protobuf map syntax: map<key_type, value_type>
		return fmt.Sprintf("map<%s, %s>", protoKeyType, protoValueType)

	// Tuple type
	case "Tuple":
		// Tuples would need to be converted to message types
		return protoString // Represent as JSON string for now
	}

	return ""
}

// mapClickHouseTypeToProto maps a ClickHouse type string to its protobuf equivalent
func (tm *TypeMapper) mapClickHouseTypeToProto(chType string) string {
	// Handle parameterized types by extracting base type
	if idx := strings.Index(chType, "("); idx > 0 {
		chType = chType[:idx]
	}

	return tm.mapBaseType(chType, chType)
}

// isValidProtoMapKey checks if a protobuf type is a valid map key type
// Protobuf spec allows: int32, int64, uint32, uint64, sint32, sint64,
// fixed32, fixed64, sfixed32, sfixed64, bool, string
func (tm *TypeMapper) isValidProtoMapKey(protoType string) bool {
	validKeys := map[string]bool{
		"int32":    true,
		"int64":    true,
		"uint32":   true,
		"uint64":   true,
		"sint32":   true,
		"sint64":   true,
		"fixed32":  true,
		"fixed64":  true,
		"sfixed32": true,
		"sfixed64": true,
		"bool":     true,
		"string":   true,
	}

	return validKeys[protoType]
}

func extractInnerType(wrappedType string) string {
	start := strings.Index(wrappedType, "(")
	end := strings.LastIndex(wrappedType, ")")

	if start > 0 && end > start {
		return strings.TrimSpace(wrappedType[start+1 : end])
	}

	return wrappedType
}

// getWrapperType returns the Google protobuf wrapper type for primitive types
func (tm *TypeMapper) getWrapperType(protoType string) string {
	switch protoType {
	case protoString:
		return "google.protobuf.StringValue"
	case protoBool:
		return "google.protobuf.BoolValue"
	case protoInt32:
		return "google.protobuf.Int32Value"
	case protoInt64:
		return "google.protobuf.Int64Value"
	case protoUInt32:
		return "google.protobuf.UInt32Value"
	case protoUInt64:
		return "google.protobuf.UInt64Value"
	case protoFloat:
		return "google.protobuf.FloatValue"
	case protoDouble:
		return "google.protobuf.DoubleValue"
	case protoBytes:
		return "google.protobuf.BytesValue"
	default:
		// For non-primitive types, return empty string
		return ""
	}
}

// getWrapperTypeForColumn returns the appropriate wrapper type for a column
func (tm *TypeMapper) getWrapperTypeForColumn(column *clickhouse.Column) string {
	protoType := tm.mapBaseType(column.BaseType, column.Type)

	// Arrays get repeated, not wrapped
	if column.IsArray {
		return "repeated " + protoType
	}

	// Get wrapper for nullable types
	if wrapper := tm.getWrapperType(protoType); wrapper != "" {
		return wrapper
	}

	// Fallback to regular type
	return protoType
}

// GetFieldNumber generates a deterministic field number for a column
func GetFieldNumber(position uint64) int32 {
	// Add offset of 10 to avoid low field numbers
	// Field numbers 1-10 are often reserved for future use
	const offset = 10
	const maxInt32 = 2147483647

	fieldNum := position + offset
	if fieldNum > maxInt32 {
		return maxInt32
	}

	// Ensure we never return 0 (invalid in protobuf)
	if fieldNum == 0 {
		return 1
	}

	return int32(fieldNum)
}

// SanitizeName converts a name to be valid for protobuf
func SanitizeName(name string) string {
	// Replace invalid characters with underscores
	result := strings.Builder{}
	for i, ch := range name {
		switch {
		case (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_':
			result.WriteRune(ch)
		case ch >= '0' && ch <= '9':
			if i == 0 {
				result.WriteString("f_") // Prefix with f_ if starts with number
			}
			result.WriteRune(ch)
		default:
			result.WriteRune('_')
		}
	}

	sanitized := result.String()

	// Handle reserved keywords
	if isReservedKeyword(sanitized) {
		return sanitized + "_field"
	}

	return sanitized
}

func isReservedKeyword(word string) bool {
	reserved := map[string]bool{
		"syntax":     true,
		"package":    true,
		"import":     true,
		"public":     true,
		"option":     true,
		"message":    true,
		"enum":       true,
		"service":    true,
		"rpc":        true,
		"returns":    true,
		"stream":     true,
		"repeated":   true,
		"optional":   true,
		"required":   true,
		"reserved":   true,
		"extensions": true,
		"extend":     true,
		"oneof":      true,
		"map":        true,
		"bool":       true,
		"string":     true,
		"bytes":      true,
		"float":      true,
		"double":     true,
		"int32":      true,
		"int64":      true,
		"uint32":     true,
		"uint64":     true,
		"sint32":     true,
		"sint64":     true,
		"fixed32":    true,
		"fixed64":    true,
		"sfixed32":   true,
		"sfixed64":   true,
	}

	return reserved[strings.ToLower(word)]
}

// ToPascalCase converts a snake_case string to PascalCase
func ToPascalCase(name string) string {
	parts := strings.Split(name, "_")
	for i, part := range parts {
		if part != "" {
			parts[i] = strings.ToUpper(string(part[0])) + strings.ToLower(part[1:])
		}
	}
	return strings.Join(parts, "")
}

// ProtoField represents a protobuf field definition
type ProtoField struct {
	Name    string
	Type    string
	Number  int32
	Comment string
}

// ConvertColumn converts a ClickHouse column to a ProtoField
func (tm *TypeMapper) ConvertColumn(column *clickhouse.Column, tableName string, convConfig *config.ConversionConfig) (*ProtoField, error) {
	protoType, err := tm.MapType(column, tableName, convConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to map type for column %s: %w", column.Name, err)
	}

	field := &ProtoField{
		Name:    SanitizeName(column.Name),
		Type:    protoType,
		Number:  GetFieldNumber(column.Position),
		Comment: column.Comment,
	}

	return field, nil
}

// parseMapType parses a Map(K, V) type string and returns the key and value types
func (tm *TypeMapper) parseMapType(mapType string) (keyType, valueType string) {
	// Check if it starts with Map(
	if !strings.HasPrefix(mapType, "Map(") || !strings.HasSuffix(mapType, ")") {
		return "", ""
	}

	// Extract the inner types
	inner := mapType[4 : len(mapType)-1] // Remove "Map(" and ")"

	// Handle nested types by counting parentheses
	parenCount := 0
	commaPos := -1

	for i, ch := range inner {
		switch ch {
		case '(':
			parenCount++
		case ')':
			parenCount--
		case ',':
			if parenCount == 0 && commaPos == -1 {
				commaPos = i
			}
		}
	}

	if commaPos == -1 {
		return "", ""
	}

	keyType = strings.TrimSpace(inner[:commaPos])
	valueType = strings.TrimSpace(inner[commaPos+1:])

	// Remove Nullable wrapper if present for the value type
	if strings.HasPrefix(valueType, "Nullable(") && strings.HasSuffix(valueType, ")") {
		valueType = valueType[9 : len(valueType)-1]
	}

	return keyType, valueType
}

// getMapFilterType returns the filter type for Map columns
func (tm *TypeMapper) getMapFilterType(columnType string) string {
	keyType, valueType := tm.parseMapType(columnType)
	if keyType == "" || valueType == "" {
		return "" // Invalid map type
	}

	// Currently supporting common combinations with String keys
	if keyType == "String" {
		switch valueType {
		case "String":
			return "MapStringStringFilter"
		case "UInt32", "UInt8", "UInt16":
			return "MapStringUInt32Filter"
		case typeUInt64:
			return "MapStringUInt64Filter"
		case "Int32", "Int8", "Int16":
			return "MapStringInt32Filter"
		case "Int64":
			return "MapStringInt64Filter"
		}
	}
	// Unsupported Map combination
	return ""
}

// getScalarFilterType returns the filter type for scalar (non-Map) columns
func (tm *TypeMapper) getScalarFilterType(column *clickhouse.Column) string {
	// Get the base proto type (without wrapper or repeated)
	protoType := tm.mapBaseType(column.BaseType, column.Type)

	// Determine the base filter type
	var baseFilterType string
	switch protoType {
	case protoInt32:
		baseFilterType = "Int32Filter"
	case protoInt64:
		baseFilterType = "Int64Filter"
	case protoUInt32:
		baseFilterType = "UInt32Filter"
	case protoUInt64:
		baseFilterType = "UInt64Filter"
	case protoString:
		baseFilterType = "StringFilter"
	case protoBool:
		baseFilterType = "BoolFilter"
	default:
		// For other types, no filter type available
		return ""
	}

	// Add Nullable prefix if column is nullable
	if column.IsNullable {
		return "Nullable" + baseFilterType
	}

	return baseFilterType
}

// GetFilterTypeForColumn returns the appropriate filter type for a column based on its type and nullability
func (tm *TypeMapper) GetFilterTypeForColumn(column *clickhouse.Column, tableName string, convConfig *config.ConversionConfig) string {
	// Arrays don't use filter types
	if column.IsArray {
		return ""
	}

	// Check if this Int64/UInt64 should be converted to string
	if (column.BaseType == typeUInt64 || column.BaseType == typeInt64) && convConfig.ShouldConvertToString(tableName, column.Name) {
		// Use StringFilter for converted Int64/UInt64 fields
		if column.IsNullable {
			return "NullableStringFilter"
		}
		return "StringFilter"
	}

	// Check if it's a Map type
	if column.BaseType == "Map" {
		return tm.getMapFilterType(column.Type)
	}

	// Handle scalar types
	return tm.getScalarFilterType(column)
}

// IsFixedString checks if a ClickHouse type is FixedString and returns its length
// Handles both FixedString(N) and Nullable(FixedString(N))
func IsFixedString(chType string) (isFixed bool, length int) {
	// Strip Nullable wrapper if present
	typeToCheck := chType
	if strings.HasPrefix(typeToCheck, "Nullable(") && strings.HasSuffix(typeToCheck, ")") {
		typeToCheck = typeToCheck[9 : len(typeToCheck)-1] // Remove "Nullable(" and ")"
	}

	if !strings.HasPrefix(typeToCheck, "FixedString(") {
		return false, 0
	}

	// Parse: FixedString(66) -> 66
	lengthStr := strings.TrimSuffix(strings.TrimPrefix(typeToCheck, "FixedString("), ")")
	length, err := strconv.Atoi(lengthStr)
	if err != nil {
		return false, 0
	}
	return true, length
}
