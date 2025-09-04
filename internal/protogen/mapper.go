// Package protogen handles the generation of Protocol Buffer schemas from ClickHouse tables
package protogen

import (
	"fmt"
	"strings"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/clickhouse"
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
)

// TypeMapper handles conversion of ClickHouse types to Protobuf types
type TypeMapper struct{}

// NewTypeMapper creates a new TypeMapper
func NewTypeMapper() *TypeMapper {
	return &TypeMapper{}
}

// MapType converts a ClickHouse type to a Protobuf type
func (tm *TypeMapper) MapType(column *clickhouse.Column) (string, error) {
	baseType := column.BaseType

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
		// DateTime64 always uses uint64 regardless of precision
		// The precision affects interpretation but not storage type
		return protoUInt64
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
	case "UInt64":
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
	case "String", "FixedString":
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

	// Map type
	case "Map":
		// Maps in protobuf require special handling
		return protoString // Represent as JSON string for now

	// Tuple type
	case "Tuple":
		// Tuples would need to be converted to message types
		return protoString // Represent as JSON string for now
	}

	return ""
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
	// Both Protobuf field numbers and ClickHouse positions start at 1
	// So we can use the position directly
	const maxInt32 = 2147483647

	if position > maxInt32 {
		return maxInt32
	}

	// Ensure we never return 0 (invalid in protobuf)
	if position == 0 {
		return 1
	}

	return int32(position)
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
func (tm *TypeMapper) ConvertColumn(column *clickhouse.Column) (*ProtoField, error) {
	protoType, err := tm.MapType(column)
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

// GetFilterTypeForColumn returns the appropriate filter type for a column based on its type and nullability
func (tm *TypeMapper) GetFilterTypeForColumn(column *clickhouse.Column) string {
	// Get the base proto type (without wrapper or repeated)
	protoType := tm.mapBaseType(column.BaseType, column.Type)

	// Arrays don't use filter types
	if column.IsArray {
		return ""
	}

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
