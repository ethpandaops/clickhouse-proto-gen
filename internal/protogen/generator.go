// Package protogen handles the generation of Protocol Buffer schemas from ClickHouse tables
package protogen

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/clickhouse"
	"github.com/ethpandaops/clickhouse-proto-gen/internal/config"
	"github.com/sirupsen/logrus"
)

// ClickHouse type constants
const (
	typeInt8   = "Int8"
	typeInt16  = "Int16"
	typeInt32  = "Int32"
	typeInt64  = "Int64"
	typeUInt8  = "UInt8"
	typeUInt16 = "UInt16"
	typeUInt32 = "UInt32"
	typeUInt64 = "UInt64"
)

// Generator creates protobuf files from ClickHouse tables
type Generator struct {
	config     *config.Config
	typeMapper *TypeMapper
	log        logrus.FieldLogger
}

// shouldGenerateAPI determines if a table should have HTTP API endpoints
func (g *Generator) shouldGenerateAPI(tableName string) bool {
	// If API generation is disabled, don't generate HTTP annotations
	if !g.config.EnableAPI {
		return false
	}

	// If no prefixes specified, generate API for all tables
	if len(g.config.APITablePrefixes) == 0 {
		return true
	}

	// Check if table matches any allowed prefix
	for _, prefix := range g.config.APITablePrefixes {
		if strings.HasPrefix(tableName, prefix) {
			return true
		}
	}

	// Table doesn't match any prefix - skip API generation
	return false
}

// NewGenerator creates a new proto file generator
func NewGenerator(cfg *config.Config, log logrus.FieldLogger) *Generator {
	return &Generator{
		config:     cfg,
		typeMapper: NewTypeMapper(),
		log:        log.WithField("component", "generator"),
	}
}

// Generate creates proto files for the given tables
func (g *Generator) Generate(tables []*clickhouse.Table) error {
	// Ensure output directory exists
	if err := os.MkdirAll(g.config.OutputDir, 0o750); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Always generate common.proto for service support
	if err := g.GenerateCommonProto(); err != nil {
		return fmt.Errorf("failed to generate common.proto: %w", err)
	}

	// Generate separate file for each table (includes both message and service)
	for _, table := range tables {
		if err := g.generateTableFile(table); err != nil {
			g.log.WithError(err).WithFields(logrus.Fields{
				"database": table.Database,
				"table":    table.Name,
			}).Error("Failed to generate proto file")
			return err
		}
	}

	// Generate SQL helper files
	if err := g.GenerateSQLHelpers(tables); err != nil {
		return fmt.Errorf("failed to generate SQL helpers: %w", err)
	}

	return nil
}

func (g *Generator) generateTableFile(table *clickhouse.Table) error {
	filename := filepath.Join(g.config.OutputDir,
		fmt.Sprintf("%s.proto", strings.ToLower(table.Name)))

	var sb strings.Builder

	// Check if this table needs wrapper types
	needsWrapper := g.checkNeedsWrapper([]*clickhouse.Table{table})
	// Check if service generation will need additional imports
	hasService := len(table.SortingKey) > 0
	g.writeTableHeader(&sb, needsWrapper, hasService, table.Name)

	// Write the message definition
	g.writeMessage(&sb, table)

	// Write service definitions if table has sorting keys
	if hasService {
		g.writeServiceDefinitions(&sb, table)
	}

	return g.writeFile(filename, sb.String())
}

func (g *Generator) checkNeedsWrapper(tables []*clickhouse.Table) bool {
	for _, table := range tables {
		// Check if nullable columns in the main message need wrappers
		if g.tableNeedsWrapperForMessage(table) {
			return true
		}

		// Check if service request messages need wrappers
		if g.tableNeedsWrapperForService(table) {
			return true
		}
	}
	return false
}

// tableNeedsWrapperForMessage checks if a table's nullable columns need wrapper types
func (g *Generator) tableNeedsWrapperForMessage(table *clickhouse.Table) bool {
	for _, column := range table.Columns {
		if column.IsNullable && !column.IsArray {
			// Check if the type would use a wrapper
			protoType := g.typeMapper.mapBaseType(column.BaseType, column.Type)
			if g.typeMapper.getWrapperType(protoType) != "" {
				return true
			}
		}
	}
	return false
}

// tableNeedsWrapperForService checks if a table's service definitions need wrapper types
func (g *Generator) tableNeedsWrapperForService(table *clickhouse.Table) bool {
	// Service is generated when table has sorting keys
	if len(table.SortingKey) == 0 {
		return false
	}

	// Check all columns that will be in the request message
	for _, column := range table.Columns {
		// Skip arrays - they use repeated, not wrappers
		if column.IsArray {
			continue
		}

		// Check if this column will use a filter type
		filterType := g.typeMapper.GetFilterTypeForColumn(&column)
		if filterType == "" {
			// No filter type available, will use wrapper type
			protoType := g.typeMapper.mapBaseType(column.BaseType, column.Type)
			if g.typeMapper.getWrapperType(protoType) != "" {
				return true
			}
		}
	}
	return false
}

func (g *Generator) writeTableHeader(sb *strings.Builder, needsWrapper, hasService bool, tableName string) {
	sb.WriteString("syntax = \"proto3\";\n\n")

	if g.config.Package != "" {
		fmt.Fprintf(sb, "package %s;\n", g.config.Package)
	}

	// Add imports
	if hasService {
		sb.WriteString("\nimport \"common.proto\";\n")
	}
	if needsWrapper {
		sb.WriteString("import \"google/protobuf/wrappers.proto\";\n")
	}

	// Add Google API annotations if this table has API endpoints
	if hasService && g.shouldGenerateAPI(tableName) {
		sb.WriteString("import \"google/api/annotations.proto\";\n")
		sb.WriteString("import \"google/api/field_behavior.proto\";\n")
	}

	if g.config.GoPackage != "" {
		fmt.Fprintf(sb, "\noption go_package = \"%s\";\n", g.config.GoPackage)
	}
}

func (g *Generator) writeMessage(sb *strings.Builder, table *clickhouse.Table) {
	messageName := ToPascalCase(table.Name)

	// Write message comment if available
	if g.config.IncludeComments && table.Comment != "" {
		g.writeComment(sb, table.Comment, "")
	}

	fmt.Fprintf(sb, "\nmessage %s {\n", messageName)

	// Process columns
	for _, column := range table.Columns {
		field, err := g.typeMapper.ConvertColumn(&column)
		if err != nil {
			g.log.WithError(err).WithField("column", column.Name).Warn("Failed to convert column")
			continue
		}

		g.writeField(sb, field)
	}

	sb.WriteString("}\n")
}

func (g *Generator) writeServiceDefinitions(sb *strings.Builder, table *clickhouse.Table) {
	if len(table.SortingKey) == 0 {
		// No sorting key, skip service generation
		return
	}

	messageName := ToPascalCase(table.Name)

	// Write request message
	fmt.Fprintf(sb, "\n// Request for listing %s records\n",
		table.Name)
	fmt.Fprintf(sb, "message List%sRequest {\n", messageName)

	fieldNumber := 1

	// Get column info for sorting keys
	columnMap := make(map[string]*clickhouse.Column)
	for i := range table.Columns {
		col := &table.Columns[i]
		columnMap[col.Name] = col
	}

	// Track which columns have been processed
	processedColumns := make(map[string]bool)

	// Process primary key (first sorting column) - REQUIRED
	if len(table.SortingKey) > 0 {
		fieldNumber = g.writePrimaryKeyField(sb, table.SortingKey[0], columnMap, processedColumns, fieldNumber, table.Name)
	}

	// Process remaining sorting columns - OPTIONAL
	for i := 1; i < len(table.SortingKey); i++ {
		fieldNumber = g.writeSortingKeyField(sb, table.SortingKey[i], columnMap, processedColumns, fieldNumber, i+1, table.Name)
	}

	// Process all other columns - OPTIONAL
	fieldNumber = g.writeRemainingColumnFilters(sb, table, processedColumns, fieldNumber)

	// Add pagination fields (AIP-132 standard)
	fmt.Fprintf(sb, "\n  // The maximum number of %s to return.\n", table.Name)
	fmt.Fprintf(sb, "  // If unspecified, at most 100 items will be returned.\n")
	fmt.Fprintf(sb, "  // The maximum value is %d; values above %d will be coerced to %d.\n", g.config.MaxPageSize, g.config.MaxPageSize, g.config.MaxPageSize)
	if g.shouldGenerateAPI(table.Name) {
		fmt.Fprintf(sb, "  int32 page_size = %d [(google.api.field_behavior) = OPTIONAL];\n", fieldNumber)
	} else {
		fmt.Fprintf(sb, "  int32 page_size = %d;\n", fieldNumber)
	}

	fieldNumber++
	fmt.Fprintf(sb, "  // A page token, received from a previous `List%s` call.\n", messageName)
	fmt.Fprintf(sb, "  // Provide this to retrieve the subsequent page.\n")
	if g.shouldGenerateAPI(table.Name) {
		fmt.Fprintf(sb, "  string page_token = %d [(google.api.field_behavior) = OPTIONAL];\n", fieldNumber)
	} else {
		fmt.Fprintf(sb, "  string page_token = %d;\n", fieldNumber)
	}

	fieldNumber++
	fmt.Fprintf(sb, "  // The order of results. Format: comma-separated list of fields.\n")
	fmt.Fprintf(sb, "  // Example: \"foo,bar\" or \"foo desc,bar\" for descending order on foo.\n")
	fmt.Fprintf(sb, "  // If unspecified, results will be returned in the default order.\n")
	if g.shouldGenerateAPI(table.Name) {
		fmt.Fprintf(sb, "  string order_by = %d [(google.api.field_behavior) = OPTIONAL];\n", fieldNumber)
	} else {
		fmt.Fprintf(sb, "  string order_by = %d;\n", fieldNumber)
	}
	sb.WriteString("}\n\n")

	// Write response message
	fmt.Fprintf(sb, "// Response for listing %s records\n",
		table.Name)
	fmt.Fprintf(sb, "message List%sResponse {\n", messageName)
	fmt.Fprintf(sb, "  // The list of %s.\n", table.Name)
	fmt.Fprintf(sb, "  repeated %s %s = 1;\n", messageName, strings.ToLower(table.Name))
	fmt.Fprintf(sb, "  // A token, which can be sent as `page_token` to retrieve the next page.\n")
	fmt.Fprintf(sb, "  // If this field is omitted, there are no subsequent pages.\n")
	fmt.Fprintf(sb, "  string next_page_token = 2;\n")
	sb.WriteString("}\n\n")

	// Write Get request message (takes only primary key)
	fmt.Fprintf(sb, "// Request for getting a single %s record by primary key\n",
		table.Name)
	fmt.Fprintf(sb, "message Get%sRequest {\n", messageName)

	// Add only the primary key field for Get request
	primaryKey := table.SortingKey[0]
	if column, exists := columnMap[primaryKey]; exists {
		primaryKeyField := SanitizeName(primaryKey)

		// Get the base proto type (not filter type) for the primary key
		protoType, _ := g.typeMapper.MapType(column)

		// Write field comment if available
		if g.config.IncludeComments && column.Comment != "" {
			g.writeComment(sb, column.Comment, "  ")
		}

		// Primary key as a simple scalar value
		fmt.Fprintf(sb, "  %s %s = 1; // Primary key (required)\n", protoType, primaryKeyField)
	}
	sb.WriteString("}\n\n")

	// Write Get response message
	fmt.Fprintf(sb, "// Response for getting a single %s record\n",
		table.Name)
	fmt.Fprintf(sb, "message Get%sResponse {\n", messageName)
	fmt.Fprintf(sb, "  %s item = 1;\n", messageName)
	sb.WriteString("}\n\n")

	// Write service definition with both List and Get
	fmt.Fprintf(sb, "// Query %s data\n",
		table.Name)
	fmt.Fprintf(sb, "service %sService {\n", messageName)

	// Check if this table should have HTTP annotations
	if g.shouldGenerateAPI(table.Name) {
		// Generate List RPC WITH HTTP annotations
		fmt.Fprintf(sb, "  // List records | Retrieve paginated results with optional filtering\n")
		fmt.Fprintf(sb, "  rpc List(List%sRequest) returns (List%sResponse) {\n",
			messageName, messageName)
		fmt.Fprintf(sb, "    option (google.api.http) = {\n")
		fmt.Fprintf(sb, "      get: \"%s/%s\"\n", g.config.APIBasePath, table.Name)
		fmt.Fprintf(sb, "    };\n")
		fmt.Fprintf(sb, "  }\n")

		// Generate Get RPC WITH HTTP annotations
		primaryKey := table.SortingKey[0]
		primaryKeyField := SanitizeName(primaryKey)
		fmt.Fprintf(sb, "  // Get record | Retrieve a single record by %s\n",
			primaryKey)
		fmt.Fprintf(sb, "  rpc Get(Get%sRequest) returns (Get%sResponse) {\n",
			messageName, messageName)
		fmt.Fprintf(sb, "    option (google.api.http) = {\n")
		fmt.Fprintf(sb, "      get: \"%s/%s/{%s}\"\n", g.config.APIBasePath, table.Name, primaryKeyField)
		fmt.Fprintf(sb, "    };\n")
		fmt.Fprintf(sb, "  }\n")
	} else {
		// Generate List RPC WITHOUT HTTP annotations (basic gRPC only)
		fmt.Fprintf(sb, "  // List records | Retrieve paginated results with optional filtering\n")
		fmt.Fprintf(sb, "  rpc List(List%sRequest) returns (List%sResponse);\n",
			messageName, messageName)
		fmt.Fprintf(sb, "  // Get record | Retrieve a single record by primary key\n")
		fmt.Fprintf(sb, "  rpc Get(Get%sRequest) returns (Get%sResponse);\n",
			messageName, messageName)
	}

	sb.WriteString("}\n")
}

// writePrimaryKeyField writes the primary key field (required) for service request
func (g *Generator) writePrimaryKeyField(sb *strings.Builder, sortCol string, columnMap map[string]*clickhouse.Column, processedColumns map[string]bool, fieldNumber int, tableName string) int {
	column, exists := columnMap[sortCol]
	if !exists {
		return fieldNumber
	}

	processedColumns[sortCol] = true

	// Build comment with optional ClickHouse column comment
	comment := fmt.Sprintf("Filter by %s (PRIMARY KEY - required)", sortCol)
	if g.config.IncludeComments && column.Comment != "" {
		comment = fmt.Sprintf("Filter by %s - %s (PRIMARY KEY - required)", sortCol, column.Comment)
	}

	// Get the appropriate filter type based on column type and nullability
	// Primary key uses filter types to allow range queries, but is marked as required
	filterType := g.typeMapper.GetFilterTypeForColumn(column)
	if filterType != "" {
		fmt.Fprintf(sb, "  // %s\n", comment)
		if g.shouldGenerateAPI(tableName) {
			fmt.Fprintf(sb, "  %s %s = %d [(google.api.field_behavior) = REQUIRED];\n", filterType, SanitizeName(sortCol), fieldNumber)
		} else {
			fmt.Fprintf(sb, "  %s %s = %d;\n", filterType, SanitizeName(sortCol), fieldNumber)
		}
		fieldNumber++
		fmt.Fprintf(sb, "\n")
	} else {
		// For types without filter support, use the proto type directly
		protoType := getProtoTypeForColumn(column)
		fmt.Fprintf(sb, "  // %s\n", comment)
		if g.shouldGenerateAPI(tableName) {
			fmt.Fprintf(sb, "  %s %s = %d [(google.api.field_behavior) = REQUIRED];\n", protoType, SanitizeName(sortCol), fieldNumber)
		} else {
			fmt.Fprintf(sb, "  %s %s = %d;\n", protoType, SanitizeName(sortCol), fieldNumber)
		}
		fieldNumber++
		fmt.Fprintf(sb, "\n")
	}

	return fieldNumber
}

// writeSortingKeyField writes a non-primary sorting key field (optional) for service request
func (g *Generator) writeSortingKeyField(sb *strings.Builder, sortCol string, columnMap map[string]*clickhouse.Column, processedColumns map[string]bool, fieldNumber, orderPosition int, tableName string) int {
	column, exists := columnMap[sortCol]
	if !exists {
		return fieldNumber
	}

	processedColumns[sortCol] = true

	// Build comment with optional ClickHouse column comment
	comment := fmt.Sprintf("Filter by %s (ORDER BY column %d - optional)", sortCol, orderPosition)
	if g.config.IncludeComments && column.Comment != "" {
		comment = fmt.Sprintf("Filter by %s - %s (ORDER BY column %d - optional)", sortCol, column.Comment, orderPosition)
	}

	// Get the appropriate filter type based on column type and nullability
	filterType := g.typeMapper.GetFilterTypeForColumn(column)
	if filterType != "" {
		fmt.Fprintf(sb, "  // %s\n", comment)
		if g.shouldGenerateAPI(tableName) {
			fmt.Fprintf(sb, "  %s %s = %d [(google.api.field_behavior) = OPTIONAL];\n", filterType, SanitizeName(sortCol), fieldNumber)
		} else {
			fmt.Fprintf(sb, "  %s %s = %d;\n", filterType, SanitizeName(sortCol), fieldNumber)
		}
		fieldNumber++
		fmt.Fprintf(sb, "\n")
	} else {
		// For types without filter support, use wrapper type for optional field
		wrapperType := g.typeMapper.getWrapperTypeForColumn(column)
		fmt.Fprintf(sb, "  // %s\n", comment)
		if g.shouldGenerateAPI(tableName) {
			fmt.Fprintf(sb, "  %s %s = %d [(google.api.field_behavior) = OPTIONAL];\n", wrapperType, SanitizeName(sortCol), fieldNumber)
		} else {
			fmt.Fprintf(sb, "  %s %s = %d;\n", wrapperType, SanitizeName(sortCol), fieldNumber)
		}
		fieldNumber++
		fmt.Fprintf(sb, "\n")
	}

	return fieldNumber
}

// writeRemainingColumnFilters writes filter fields for non-sorting columns
func (g *Generator) writeRemainingColumnFilters(sb *strings.Builder, table *clickhouse.Table, processedColumns map[string]bool, fieldNumber int) int {
	for _, column := range table.Columns {
		if processedColumns[column.Name] {
			continue // Already processed as sorting column
		}

		// Build comment with optional ClickHouse column comment
		comment := fmt.Sprintf("Filter by %s (optional)", column.Name)
		if g.config.IncludeComments && column.Comment != "" {
			comment = fmt.Sprintf("Filter by %s - %s (optional)", column.Name, column.Comment)
		}

		// Get the appropriate filter type based on column type and nullability
		filterType := g.typeMapper.GetFilterTypeForColumn(&column)
		if filterType != "" {
			fmt.Fprintf(sb, "  // %s\n", comment)
			if g.shouldGenerateAPI(table.Name) {
				fmt.Fprintf(sb, "  %s %s = %d [(google.api.field_behavior) = OPTIONAL];\n", filterType, SanitizeName(column.Name), fieldNumber)
			} else {
				fmt.Fprintf(sb, "  %s %s = %d;\n", filterType, SanitizeName(column.Name), fieldNumber)
			}
			fieldNumber++
		} else {
			// For types without filter support, use wrapper type for optional field
			wrapperType := g.typeMapper.getWrapperTypeForColumn(&column)
			fmt.Fprintf(sb, "  // %s\n", comment)
			if g.shouldGenerateAPI(table.Name) {
				fmt.Fprintf(sb, "  %s %s = %d [(google.api.field_behavior) = OPTIONAL];\n", wrapperType, SanitizeName(column.Name), fieldNumber)
			} else {
				fmt.Fprintf(sb, "  %s %s = %d;\n", wrapperType, SanitizeName(column.Name), fieldNumber)
			}
			fieldNumber++
		}
	}

	return fieldNumber
}

func (g *Generator) writeField(sb *strings.Builder, field *ProtoField) {
	// Write field comment if available
	if g.config.IncludeComments && field.Comment != "" {
		g.writeComment(sb, field.Comment, "  ")
	}

	// No need for optional modifier when using wrapper types
	fmt.Fprintf(sb, "  %s %s = %d;\n",
		field.Type, field.Name, field.Number)
}

func (g *Generator) writeComment(sb *strings.Builder, comment, indent string) {
	if !g.config.IncludeComments {
		return
	}
	lines := strings.Split(comment, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			fmt.Fprintf(sb, "%s// %s\n", indent, line)
		}
	}
}

func (g *Generator) writeFile(filename, content string) error {
	if err := os.WriteFile(filename, []byte(content), 0o600); err != nil {
		return fmt.Errorf("failed to write file %s: %w", filename, err)
	}

	g.log.WithField("file", filename).Info("Generated proto file")
	return nil
}

// getProtoType returns the proto type for a ClickHouse base type
func getProtoType(baseType string) string {
	switch baseType {
	case typeInt8, typeInt16, typeInt32:
		return "int32"
	case typeInt64:
		return "int64"
	case typeUInt8, typeUInt16, typeUInt32:
		return "uint32"
	case typeUInt64:
		return "uint64"
	default:
		return "string"
	}
}

// getProtoTypeForColumn returns the proto type for a ClickHouse column
func getProtoTypeForColumn(column *clickhouse.Column) string {
	// For arrays, add repeated modifier
	if column.IsArray {
		baseProtoType := getProtoType(column.BaseType)
		return "repeated " + baseProtoType
	}

	// For regular columns, just return the base proto type
	return getProtoType(column.BaseType)
}
