package protogen

import (
	"fmt"
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
}

func (g *Generator) writeCommonTypes(sb *strings.Builder) {
	// SortOrder enum
	sb.WriteString("// SortOrder defines the order of results\n")
	sb.WriteString("enum SortOrder {\n")
	sb.WriteString("  ASC = 0;\n")
	sb.WriteString("  DESC = 1;\n")
	sb.WriteString("}\n")
}
