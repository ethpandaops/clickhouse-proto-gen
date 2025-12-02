package protogen

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// GenerateCommonProto generates the common.proto file with shared types
func (g *Generator) GenerateCommonProto() error {
	filename := filepath.Join(g.config.OutputDir, "common.proto")

	var sb strings.Builder

	// Write header
	sb.WriteString("syntax = \"proto3\";\n\n")

	if g.config.Package != "" {
		fmt.Fprintf(&sb, "package %s;\n", g.config.Package)
	}

	sb.WriteString("\nimport \"google/protobuf/wrappers.proto\";\n")
	sb.WriteString("import \"google/protobuf/empty.proto\";\n")

	if g.config.GoPackage != "" {
		fmt.Fprintf(&sb, "option go_package = \"%s\";\n", g.config.GoPackage)
	}

	sb.WriteString("\n// Common types used across all generated services\n\n")

	// Generate range types for integers
	g.writeRangeTypes(&sb)

	// Generate common request/response types
	g.writeCommonTypes(&sb)

	return g.writeFile(filename, sb.String())
}

func (g *Generator) writeRangeTypes(sb *strings.Builder) {
	// UInt32 types for non-nullable fields
	sb.WriteString("// UInt32Filter represents filtering options for non-nullable uint32 values\n")
	sb.WriteString("message UInt32Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    uint32 eq = 1;                 // Equal to value\n")
	sb.WriteString("    uint32 ne = 2;                 // Not equal to value\n")
	sb.WriteString("    uint32 lt = 3;                 // Less than value\n")
	sb.WriteString("    uint32 lte = 4;                // Less than or equal to value\n")
	sb.WriteString("    uint32 gt = 5;                 // Greater than value\n")
	sb.WriteString("    uint32 gte = 6;                // Greater than or equal to value\n")
	sb.WriteString("    UInt32Range between = 7;       // Between min and max (inclusive)\n")
	sb.WriteString("    UInt32List in = 8;             // In list of values\n")
	sb.WriteString("    UInt32List not_in = 9;         // Not in list of values\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// Nullable UInt32 filter
	sb.WriteString("// NullableUInt32Filter represents filtering options for nullable uint32 values\n")
	sb.WriteString("message NullableUInt32Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    uint32 eq = 1;                 // Equal to value\n")
	sb.WriteString("    uint32 ne = 2;                 // Not equal to value\n")
	sb.WriteString("    uint32 lt = 3;                 // Less than value\n")
	sb.WriteString("    uint32 lte = 4;                // Less than or equal to value\n")
	sb.WriteString("    uint32 gt = 5;                 // Greater than value\n")
	sb.WriteString("    uint32 gte = 6;                // Greater than or equal to value\n")
	sb.WriteString("    UInt32Range between = 7;       // Between min and max (inclusive)\n")
	sb.WriteString("    UInt32List in = 8;             // In list of values\n")
	sb.WriteString("    UInt32List not_in = 9;         // Not in list of values\n")
	sb.WriteString("    google.protobuf.Empty is_null = 10;     // IS NULL check\n")
	sb.WriteString("    google.protobuf.Empty is_not_null = 11; // IS NOT NULL check\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// UInt32Range represents a range of uint32 values\n")
	sb.WriteString("message UInt32Range {\n")
	sb.WriteString("  uint32 min = 1;\n")
	sb.WriteString("  google.protobuf.UInt32Value max = 2; // If not set, matches exact value (min)\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// UInt32List represents a list of uint32 values\n")
	sb.WriteString("message UInt32List {\n")
	sb.WriteString("  repeated uint32 values = 1;\n")
	sb.WriteString("}\n\n")

	// UInt64 types for non-nullable fields
	sb.WriteString("// UInt64Filter represents filtering options for non-nullable uint64 values\n")
	sb.WriteString("message UInt64Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    uint64 eq = 1;                 // Equal to value\n")
	sb.WriteString("    uint64 ne = 2;                 // Not equal to value\n")
	sb.WriteString("    uint64 lt = 3;                 // Less than value\n")
	sb.WriteString("    uint64 lte = 4;                // Less than or equal to value\n")
	sb.WriteString("    uint64 gt = 5;                 // Greater than value\n")
	sb.WriteString("    uint64 gte = 6;                // Greater than or equal to value\n")
	sb.WriteString("    UInt64Range between = 7;       // Between min and max (inclusive)\n")
	sb.WriteString("    UInt64List in = 8;             // In list of values\n")
	sb.WriteString("    UInt64List not_in = 9;         // Not in list of values\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// Nullable UInt64 filter
	sb.WriteString("// NullableUInt64Filter represents filtering options for nullable uint64 values\n")
	sb.WriteString("message NullableUInt64Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    uint64 eq = 1;                 // Equal to value\n")
	sb.WriteString("    uint64 ne = 2;                 // Not equal to value\n")
	sb.WriteString("    uint64 lt = 3;                 // Less than value\n")
	sb.WriteString("    uint64 lte = 4;                // Less than or equal to value\n")
	sb.WriteString("    uint64 gt = 5;                 // Greater than value\n")
	sb.WriteString("    uint64 gte = 6;                // Greater than or equal to value\n")
	sb.WriteString("    UInt64Range between = 7;       // Between min and max (inclusive)\n")
	sb.WriteString("    UInt64List in = 8;             // In list of values\n")
	sb.WriteString("    UInt64List not_in = 9;         // Not in list of values\n")
	sb.WriteString("    google.protobuf.Empty is_null = 10;     // IS NULL check\n")
	sb.WriteString("    google.protobuf.Empty is_not_null = 11; // IS NOT NULL check\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// UInt64Range represents a range of uint64 values\n")
	sb.WriteString("message UInt64Range {\n")
	sb.WriteString("  uint64 min = 1;\n")
	sb.WriteString("  google.protobuf.UInt64Value max = 2;\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// UInt64List represents a list of uint64 values\n")
	sb.WriteString("message UInt64List {\n")
	sb.WriteString("  repeated uint64 values = 1;\n")
	sb.WriteString("}\n\n")

	// Int32 types for non-nullable fields
	sb.WriteString("// Int32Filter represents filtering options for non-nullable int32 values\n")
	sb.WriteString("message Int32Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    int32 eq = 1;                  // Equal to value\n")
	sb.WriteString("    int32 ne = 2;                  // Not equal to value\n")
	sb.WriteString("    int32 lt = 3;                  // Less than value\n")
	sb.WriteString("    int32 lte = 4;                 // Less than or equal to value\n")
	sb.WriteString("    int32 gt = 5;                  // Greater than value\n")
	sb.WriteString("    int32 gte = 6;                 // Greater than or equal to value\n")
	sb.WriteString("    Int32Range between = 7;        // Between min and max (inclusive)\n")
	sb.WriteString("    Int32List in = 8;              // In list of values\n")
	sb.WriteString("    Int32List not_in = 9;          // Not in list of values\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// Nullable Int32 filter
	sb.WriteString("// NullableInt32Filter represents filtering options for nullable int32 values\n")
	sb.WriteString("message NullableInt32Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    int32 eq = 1;                  // Equal to value\n")
	sb.WriteString("    int32 ne = 2;                  // Not equal to value\n")
	sb.WriteString("    int32 lt = 3;                  // Less than value\n")
	sb.WriteString("    int32 lte = 4;                 // Less than or equal to value\n")
	sb.WriteString("    int32 gt = 5;                  // Greater than value\n")
	sb.WriteString("    int32 gte = 6;                 // Greater than or equal to value\n")
	sb.WriteString("    Int32Range between = 7;        // Between min and max (inclusive)\n")
	sb.WriteString("    Int32List in = 8;              // In list of values\n")
	sb.WriteString("    Int32List not_in = 9;          // Not in list of values\n")
	sb.WriteString("    google.protobuf.Empty is_null = 10;     // IS NULL check\n")
	sb.WriteString("    google.protobuf.Empty is_not_null = 11; // IS NOT NULL check\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// Int32Range represents a range of int32 values\n")
	sb.WriteString("message Int32Range {\n")
	sb.WriteString("  int32 min = 1;\n")
	sb.WriteString("  google.protobuf.Int32Value max = 2;\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// Int32List represents a list of int32 values\n")
	sb.WriteString("message Int32List {\n")
	sb.WriteString("  repeated int32 values = 1;\n")
	sb.WriteString("}\n\n")

	// Int64 types for non-nullable fields
	sb.WriteString("// Int64Filter represents filtering options for non-nullable int64 values\n")
	sb.WriteString("message Int64Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    int64 eq = 1;                  // Equal to value\n")
	sb.WriteString("    int64 ne = 2;                  // Not equal to value\n")
	sb.WriteString("    int64 lt = 3;                  // Less than value\n")
	sb.WriteString("    int64 lte = 4;                 // Less than or equal to value\n")
	sb.WriteString("    int64 gt = 5;                  // Greater than value\n")
	sb.WriteString("    int64 gte = 6;                 // Greater than or equal to value\n")
	sb.WriteString("    Int64Range between = 7;        // Between min and max (inclusive)\n")
	sb.WriteString("    Int64List in = 8;              // In list of values\n")
	sb.WriteString("    Int64List not_in = 9;          // Not in list of values\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// Nullable Int64 filter
	sb.WriteString("// NullableInt64Filter represents filtering options for nullable int64 values\n")
	sb.WriteString("message NullableInt64Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    int64 eq = 1;                  // Equal to value\n")
	sb.WriteString("    int64 ne = 2;                  // Not equal to value\n")
	sb.WriteString("    int64 lt = 3;                  // Less than value\n")
	sb.WriteString("    int64 lte = 4;                 // Less than or equal to value\n")
	sb.WriteString("    int64 gt = 5;                  // Greater than value\n")
	sb.WriteString("    int64 gte = 6;                 // Greater than or equal to value\n")
	sb.WriteString("    Int64Range between = 7;        // Between min and max (inclusive)\n")
	sb.WriteString("    Int64List in = 8;              // In list of values\n")
	sb.WriteString("    Int64List not_in = 9;          // Not in list of values\n")
	sb.WriteString("    google.protobuf.Empty is_null = 10;     // IS NULL check\n")
	sb.WriteString("    google.protobuf.Empty is_not_null = 11; // IS NOT NULL check\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// Int64Range represents a range of int64 values\n")
	sb.WriteString("message Int64Range {\n")
	sb.WriteString("  int64 min = 1;\n")
	sb.WriteString("  google.protobuf.Int64Value max = 2;\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// Int64List represents a list of int64 values\n")
	sb.WriteString("message Int64List {\n")
	sb.WriteString("  repeated int64 values = 1;\n")
	sb.WriteString("}\n\n")

	// String filter types for non-nullable fields
	sb.WriteString("// StringFilter represents filtering options for non-nullable string values\n")
	sb.WriteString("message StringFilter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    string eq = 1;                 // Equal to value\n")
	sb.WriteString("    string ne = 2;                 // Not equal to value\n")
	sb.WriteString("    string contains = 3;           // Contains substring (SQL LIKE '%value%')\n")
	sb.WriteString("    string starts_with = 4;        // Starts with prefix (SQL LIKE 'value%')\n")
	sb.WriteString("    string ends_with = 5;          // Ends with suffix (SQL LIKE '%value')\n")
	sb.WriteString("    string like = 6;               // SQL LIKE pattern (% and _ wildcards)\n")
	sb.WriteString("    string not_like = 7;           // SQL NOT LIKE pattern\n")
	sb.WriteString("    StringList in = 8;             // In list of values\n")
	sb.WriteString("    StringList not_in = 9;         // Not in list of values\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// Nullable String filter
	sb.WriteString("// NullableStringFilter represents filtering options for nullable string values\n")
	sb.WriteString("message NullableStringFilter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    string eq = 1;                 // Equal to value\n")
	sb.WriteString("    string ne = 2;                 // Not equal to value\n")
	sb.WriteString("    string contains = 3;           // Contains substring (SQL LIKE '%value%')\n")
	sb.WriteString("    string starts_with = 4;        // Starts with prefix (SQL LIKE 'value%')\n")
	sb.WriteString("    string ends_with = 5;          // Ends with suffix (SQL LIKE '%value')\n")
	sb.WriteString("    string like = 6;               // SQL LIKE pattern (% and _ wildcards)\n")
	sb.WriteString("    string not_like = 7;           // SQL NOT LIKE pattern\n")
	sb.WriteString("    StringList in = 8;             // In list of values\n")
	sb.WriteString("    StringList not_in = 9;         // Not in list of values\n")
	sb.WriteString("    google.protobuf.Empty is_null = 10;     // IS NULL check\n")
	sb.WriteString("    google.protobuf.Empty is_not_null = 11; // IS NOT NULL check\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// StringList represents a list of string values\n")
	sb.WriteString("message StringList {\n")
	sb.WriteString("  repeated string values = 1;\n")
	sb.WriteString("}\n\n")

	// Bool filter types for non-nullable fields
	sb.WriteString("// BoolFilter represents filtering options for non-nullable bool values\n")
	sb.WriteString("message BoolFilter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    bool eq = 1;                   // Equal to value\n")
	sb.WriteString("    bool ne = 2;                   // Not equal to value\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// Nullable Bool filter
	sb.WriteString("// NullableBoolFilter represents filtering options for nullable bool values\n")
	sb.WriteString("message NullableBoolFilter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    bool eq = 1;                   // Equal to value\n")
	sb.WriteString("    bool ne = 2;                   // Not equal to value\n")
	sb.WriteString("    google.protobuf.Empty is_null = 3;     // IS NULL check\n")
	sb.WriteString("    google.protobuf.Empty is_not_null = 4; // IS NOT NULL check\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// Map filter types
	sb.WriteString("// MapKeyValueStringString represents a key-value pair filter for Map(String, String)\n")
	sb.WriteString("message MapKeyValueStringString {\n")
	sb.WriteString("  string key = 1;\n")
	sb.WriteString("  StringFilter value_filter = 2;\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapStringStringFilter represents filtering options for Map(String, String) values\n")
	sb.WriteString("message MapStringStringFilter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    MapKeyValueStringString key_value = 1;  // mapColumn['key'] op 'value'\n")
	sb.WriteString("    string has_key = 2;                     // mapContains(mapColumn, 'key')\n")
	sb.WriteString("    string not_has_key = 3;                 // NOT mapContains(mapColumn, 'key')\n")
	sb.WriteString("    StringList has_any_key = 4;             // mapContainsAny(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("    StringList has_all_keys = 5;            // mapContainsAll(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapKeyValueStringUInt32 represents a key-value pair filter for Map(String, UInt32)\n")
	sb.WriteString("message MapKeyValueStringUInt32 {\n")
	sb.WriteString("  string key = 1;\n")
	sb.WriteString("  UInt32Filter value_filter = 2;\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapStringUInt32Filter represents filtering options for Map(String, UInt32) values\n")
	sb.WriteString("message MapStringUInt32Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    MapKeyValueStringUInt32 key_value = 1;  // mapColumn['key'] op value\n")
	sb.WriteString("    string has_key = 2;                     // mapContains(mapColumn, 'key')\n")
	sb.WriteString("    string not_has_key = 3;                 // NOT mapContains(mapColumn, 'key')\n")
	sb.WriteString("    StringList has_any_key = 4;             // mapContainsAny(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("    StringList has_all_keys = 5;            // mapContainsAll(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapKeyValueStringInt32 represents a key-value pair filter for Map(String, Int32)\n")
	sb.WriteString("message MapKeyValueStringInt32 {\n")
	sb.WriteString("  string key = 1;\n")
	sb.WriteString("  Int32Filter value_filter = 2;\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapStringInt32Filter represents filtering options for Map(String, Int32) values\n")
	sb.WriteString("message MapStringInt32Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    MapKeyValueStringInt32 key_value = 1;   // mapColumn['key'] op value\n")
	sb.WriteString("    string has_key = 2;                     // mapContains(mapColumn, 'key')\n")
	sb.WriteString("    string not_has_key = 3;                 // NOT mapContains(mapColumn, 'key')\n")
	sb.WriteString("    StringList has_any_key = 4;             // mapContainsAny(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("    StringList has_all_keys = 5;            // mapContainsAll(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapKeyValueStringUInt64 represents a key-value pair filter for Map(String, UInt64)\n")
	sb.WriteString("message MapKeyValueStringUInt64 {\n")
	sb.WriteString("  string key = 1;\n")
	sb.WriteString("  UInt64Filter value_filter = 2;\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapStringUInt64Filter represents filtering options for Map(String, UInt64) values\n")
	sb.WriteString("message MapStringUInt64Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    MapKeyValueStringUInt64 key_value = 1;  // mapColumn['key'] op value\n")
	sb.WriteString("    string has_key = 2;                     // mapContains(mapColumn, 'key')\n")
	sb.WriteString("    string not_has_key = 3;                 // NOT mapContains(mapColumn, 'key')\n")
	sb.WriteString("    StringList has_any_key = 4;             // mapContainsAny(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("    StringList has_all_keys = 5;            // mapContainsAll(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapKeyValueStringInt64 represents a key-value pair filter for Map(String, Int64)\n")
	sb.WriteString("message MapKeyValueStringInt64 {\n")
	sb.WriteString("  string key = 1;\n")
	sb.WriteString("  Int64Filter value_filter = 2;\n")
	sb.WriteString("}\n\n")

	sb.WriteString("// MapStringInt64Filter represents filtering options for Map(String, Int64) values\n")
	sb.WriteString("message MapStringInt64Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    MapKeyValueStringInt64 key_value = 1;   // mapColumn['key'] op value\n")
	sb.WriteString("    string has_key = 2;                     // mapContains(mapColumn, 'key')\n")
	sb.WriteString("    string not_has_key = 3;                 // NOT mapContains(mapColumn, 'key')\n")
	sb.WriteString("    StringList has_any_key = 4;             // mapContainsAny(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("    StringList has_all_keys = 5;            // mapContainsAll(mapColumn, ['k1', 'k2'])\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// Array filter types
	g.writeArrayFilterTypes(sb)
}

// writeArrayFilterTypes generates Array*Filter message types for filtering Array columns
func (g *Generator) writeArrayFilterTypes(sb *strings.Builder) {
	// ArrayUInt32Filter
	sb.WriteString("// ArrayUInt32Filter represents filtering options for Array(UInt32) columns\n")
	sb.WriteString("message ArrayUInt32Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    uint32 has = 1;                         // has(arr, value) - array contains value\n")
	sb.WriteString("    UInt32List has_all = 2;                 // hasAll(arr, [v1, v2]) - contains all values\n")
	sb.WriteString("    UInt32List has_any = 3;                 // hasAny(arr, [v1, v2]) - contains any value\n")
	sb.WriteString("    uint32 length_eq = 4;                   // length(arr) = n\n")
	sb.WriteString("    uint32 length_gt = 5;                   // length(arr) > n\n")
	sb.WriteString("    uint32 length_gte = 6;                  // length(arr) >= n\n")
	sb.WriteString("    uint32 length_lt = 7;                   // length(arr) < n\n")
	sb.WriteString("    uint32 length_lte = 8;                  // length(arr) <= n\n")
	sb.WriteString("    google.protobuf.Empty is_empty = 9;     // empty(arr)\n")
	sb.WriteString("    google.protobuf.Empty is_not_empty = 10; // notEmpty(arr)\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// ArrayUInt64Filter
	sb.WriteString("// ArrayUInt64Filter represents filtering options for Array(UInt64) columns\n")
	sb.WriteString("message ArrayUInt64Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    uint64 has = 1;                         // has(arr, value) - array contains value\n")
	sb.WriteString("    UInt64List has_all = 2;                 // hasAll(arr, [v1, v2]) - contains all values\n")
	sb.WriteString("    UInt64List has_any = 3;                 // hasAny(arr, [v1, v2]) - contains any value\n")
	sb.WriteString("    uint32 length_eq = 4;                   // length(arr) = n\n")
	sb.WriteString("    uint32 length_gt = 5;                   // length(arr) > n\n")
	sb.WriteString("    uint32 length_gte = 6;                  // length(arr) >= n\n")
	sb.WriteString("    uint32 length_lt = 7;                   // length(arr) < n\n")
	sb.WriteString("    uint32 length_lte = 8;                  // length(arr) <= n\n")
	sb.WriteString("    google.protobuf.Empty is_empty = 9;     // empty(arr)\n")
	sb.WriteString("    google.protobuf.Empty is_not_empty = 10; // notEmpty(arr)\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// ArrayInt32Filter
	sb.WriteString("// ArrayInt32Filter represents filtering options for Array(Int32) columns\n")
	sb.WriteString("message ArrayInt32Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    int32 has = 1;                          // has(arr, value) - array contains value\n")
	sb.WriteString("    Int32List has_all = 2;                  // hasAll(arr, [v1, v2]) - contains all values\n")
	sb.WriteString("    Int32List has_any = 3;                  // hasAny(arr, [v1, v2]) - contains any value\n")
	sb.WriteString("    uint32 length_eq = 4;                   // length(arr) = n\n")
	sb.WriteString("    uint32 length_gt = 5;                   // length(arr) > n\n")
	sb.WriteString("    uint32 length_gte = 6;                  // length(arr) >= n\n")
	sb.WriteString("    uint32 length_lt = 7;                   // length(arr) < n\n")
	sb.WriteString("    uint32 length_lte = 8;                  // length(arr) <= n\n")
	sb.WriteString("    google.protobuf.Empty is_empty = 9;     // empty(arr)\n")
	sb.WriteString("    google.protobuf.Empty is_not_empty = 10; // notEmpty(arr)\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// ArrayInt64Filter
	sb.WriteString("// ArrayInt64Filter represents filtering options for Array(Int64) columns\n")
	sb.WriteString("message ArrayInt64Filter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    int64 has = 1;                          // has(arr, value) - array contains value\n")
	sb.WriteString("    Int64List has_all = 2;                  // hasAll(arr, [v1, v2]) - contains all values\n")
	sb.WriteString("    Int64List has_any = 3;                  // hasAny(arr, [v1, v2]) - contains any value\n")
	sb.WriteString("    uint32 length_eq = 4;                   // length(arr) = n\n")
	sb.WriteString("    uint32 length_gt = 5;                   // length(arr) > n\n")
	sb.WriteString("    uint32 length_gte = 6;                  // length(arr) >= n\n")
	sb.WriteString("    uint32 length_lt = 7;                   // length(arr) < n\n")
	sb.WriteString("    uint32 length_lte = 8;                  // length(arr) <= n\n")
	sb.WriteString("    google.protobuf.Empty is_empty = 9;     // empty(arr)\n")
	sb.WriteString("    google.protobuf.Empty is_not_empty = 10; // notEmpty(arr)\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")

	// ArrayStringFilter
	sb.WriteString("// ArrayStringFilter represents filtering options for Array(String) columns\n")
	sb.WriteString("message ArrayStringFilter {\n")
	sb.WriteString("  oneof filter {\n")
	sb.WriteString("    string has = 1;                         // has(arr, value) - array contains value\n")
	sb.WriteString("    StringList has_all = 2;                 // hasAll(arr, [v1, v2]) - contains all values\n")
	sb.WriteString("    StringList has_any = 3;                 // hasAny(arr, [v1, v2]) - contains any value\n")
	sb.WriteString("    uint32 length_eq = 4;                   // length(arr) = n\n")
	sb.WriteString("    uint32 length_gt = 5;                   // length(arr) > n\n")
	sb.WriteString("    uint32 length_gte = 6;                  // length(arr) >= n\n")
	sb.WriteString("    uint32 length_lt = 7;                   // length(arr) < n\n")
	sb.WriteString("    uint32 length_lte = 8;                  // length(arr) <= n\n")
	sb.WriteString("    google.protobuf.Empty is_empty = 9;     // empty(arr)\n")
	sb.WriteString("    google.protobuf.Empty is_not_empty = 10; // notEmpty(arr)\n")
	sb.WriteString("  }\n")
	sb.WriteString("}\n\n")
}

func (g *Generator) writeCommonTypes(sb *strings.Builder) {
	// SortOrder enum
	sb.WriteString("// SortOrder defines the order of results\n")
	sb.WriteString("enum SortOrder {\n")
	sb.WriteString("  ASC = 0;\n")
	sb.WriteString("  DESC = 1;\n")
	sb.WriteString("}\n")
}

// GenerateAnnotationsProto generates the clickhouse/annotations.proto file with custom field options
func (g *Generator) GenerateAnnotationsProto() error {
	// Create clickhouse subdirectory in output dir
	clickhouseDir := filepath.Join(g.config.OutputDir, "clickhouse")
	if err := os.MkdirAll(clickhouseDir, 0o750); err != nil {
		return fmt.Errorf("failed to create clickhouse directory: %w", err)
	}

	filename := filepath.Join(clickhouseDir, "annotations.proto")

	var sb strings.Builder

	// Write header
	sb.WriteString("syntax = \"proto3\";\n\n")

	// Annotations always use a fixed package name, not the user's configured package
	// This allows generated files to reference extensions as (clickhouse.v1.projection_name)
	sb.WriteString("package clickhouse.v1;\n")

	sb.WriteString("\nimport \"google/protobuf/descriptor.proto\";\n")

	// Use the user's configured go_package as the base for the annotations package
	// Since annotations.proto is in clickhouse/ subdirectory, append /clickhouse to the package
	if g.config.GoPackage != "" {
		// Remove trailing slash if present
		goPackage := strings.TrimSuffix(g.config.GoPackage, "/")
		fmt.Fprintf(&sb, "\noption go_package = \"%s/clickhouse\";\n", goPackage)
	}

	sb.WriteString("\n")

	// Write custom field options
	sb.WriteString("extend google.protobuf.FieldOptions {\n")
	sb.WriteString("  // Indicates this field can substitute for another field (typically a primary key).\n")
	sb.WriteString("  // Value is the field name this can substitute for.\n")
	sb.WriteString("  // Example: slot can substitute for slot_start_date_time when using a projection.\n")
	sb.WriteString("  string projection_alternative_for = 50001;\n\n")

	sb.WriteString("  // Name of the ClickHouse projection this field belongs to.\n")
	sb.WriteString("  // This helps identify which projection enables this alternative key.\n")
	sb.WriteString("  string projection_name = 50002;\n\n")

	sb.WriteString("  // Group name for \"at least one required\" validation.\n")
	sb.WriteString("  // All fields with the same required_group value form an OR constraint.\n")
	sb.WriteString("  // Example: All primary key alternatives should share the same required_group.\n")
	sb.WriteString("  string required_group = 50003;\n")
	sb.WriteString("}\n")

	return g.writeFile(filename, sb.String())
}
