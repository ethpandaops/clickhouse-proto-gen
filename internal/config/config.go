// Package config provides configuration management for the ClickHouse proto generator.
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

// Define static errors for validation
var (
	ErrDSNRequired       = errors.New("DSN is required")
	ErrOutputDirRequired = errors.New("output directory is required")
	ErrPackageRequired   = errors.New("proto package is required")
	ErrTablesRequired    = errors.New("tables must be specified")
)

// Config holds the configuration for the ClickHouse proto generator.
type Config struct {
	DSN             string   `yaml:"dsn"`
	Tables          []string `yaml:"tables"`
	OutputDir       string   `yaml:"output_dir"`
	Package         string   `yaml:"package"`
	GoPackage       string   `yaml:"go_package"`
	IncludeComments bool     `yaml:"include_comments"`
	MaxPageSize     int32    `yaml:"max_page_size"`
	// API generation options
	APIBasePath      string   `yaml:"api_base_path"`      // e.g., "/api/v1"
	EnableAPI        bool     `yaml:"enable_api"`         // Enable HTTP annotations
	APITablePrefixes []string `yaml:"api_table_prefixes"` // Only generate APIs for tables matching these prefixes
	// Type conversion options
	Conversion ConversionConfig `yaml:"conversion"`
}

// ConversionConfig holds configuration for type conversions during proto generation.
type ConversionConfig struct {
	// UInt64ToString is a table-scoped map of field names to convert from UInt64 to string.
	// Map key is the table name, value is a list of field names in that table.
	// Example: {"fct_prepared_block": ["consensus_payload_value", "execution_payload_value"]}
	UInt64ToString map[string][]string `yaml:"uint64_to_string"`

	// UInt64ToStringFields is a flattened list for CLI compatibility.
	// Supports patterns like "table.field", "*.field", or "field".
	// Populated from CLI flags and merged with table-scoped configurations.
	UInt64ToStringFields []string `yaml:"uint64_to_string_fields"`
}

// NewConfig creates a new Config instance with default values.
func NewConfig() *Config {
	return &Config{
		OutputDir:        "./proto",
		Package:          "clickhouse.v1",
		IncludeComments:  true,
		MaxPageSize:      10000,
		APIBasePath:      "/api/v1",
		EnableAPI:        false,
		APITablePrefixes: []string{},
	}
}

// LoadFromFile loads configuration from a YAML file.
func (c *Config) LoadFromFile(path string, log logrus.FieldLogger) error {
	// Clean the path to prevent directory traversal
	cleanPath := filepath.Clean(path)
	data, err := os.ReadFile(cleanPath)
	if err != nil {
		return fmt.Errorf("failed to read config file: %w", err)
	}

	if err := yaml.Unmarshal(data, c); err != nil {
		return fmt.Errorf("failed to parse config file: %w", err)
	}

	log.WithField("config_file", path).Debug("Loaded configuration from file")
	return nil
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.DSN == "" {
		return ErrDSNRequired
	}

	if c.OutputDir == "" {
		return ErrOutputDirRequired
	}

	if c.Package == "" {
		return ErrPackageRequired
	}

	if len(c.Tables) == 0 {
		return ErrTablesRequired
	}

	return nil
}

// MergeFlags merges command-line flags into the configuration.
func (c *Config) MergeFlags(dsn, outputDir, pkg, goPkg, tables string, includeComments bool, maxPageSize int32, enableAPI bool, apiBasePath, apiTablePrefixes, uint64ToStringFields string) {
	if dsn != "" {
		c.DSN = dsn
	}
	if outputDir != "" {
		c.OutputDir = outputDir
	}
	if pkg != "" {
		c.Package = pkg
	}
	if goPkg != "" {
		c.GoPackage = goPkg
	}
	if tables != "" {
		c.Tables = strings.Split(tables, ",")
		for i := range c.Tables {
			c.Tables[i] = strings.TrimSpace(c.Tables[i])
		}
	}
	c.IncludeComments = includeComments
	if maxPageSize > 0 {
		c.MaxPageSize = maxPageSize
	}

	// API generation flags
	c.EnableAPI = enableAPI
	if apiBasePath != "" {
		c.APIBasePath = apiBasePath
	}
	if apiTablePrefixes != "" {
		c.APITablePrefixes = strings.Split(apiTablePrefixes, ",")
		for i := range c.APITablePrefixes {
			c.APITablePrefixes[i] = strings.TrimSpace(c.APITablePrefixes[i])
		}
	}

	// Type conversion flags
	if uint64ToStringFields != "" {
		c.Conversion.UInt64ToStringFields = strings.Split(uint64ToStringFields, ",")
		for i := range c.Conversion.UInt64ToStringFields {
			c.Conversion.UInt64ToStringFields[i] = strings.TrimSpace(c.Conversion.UInt64ToStringFields[i])
		}
	}
}

// ShouldConvertToString checks if a UInt64 field should be converted to string.
// It checks table-scoped and CLI-provided field patterns.
func (cc *ConversionConfig) ShouldConvertToString(tableName, fieldName string) bool {
	// Check table-scoped configuration
	if fields, ok := cc.UInt64ToString[tableName]; ok {
		for _, f := range fields {
			if f == fieldName {
				return true
			}
		}
	}

	// Check CLI-provided fields with pattern matching
	for _, pattern := range cc.UInt64ToStringFields {
		if matchesPattern(pattern, tableName, fieldName) {
			return true
		}
	}

	return false
}

// matchesPattern checks if a field matches a pattern.
// Supports patterns like:
//   - "table.field" (exact table and field match)
//   - "*.field" (field in any table)
//   - "*.*" (all fields in all tables)
//   - "field" (field in any table, fallback)
func matchesPattern(pattern, tableName, fieldName string) bool {
	parts := strings.Split(pattern, ".")

	// Handle single-part pattern (field name only)
	if len(parts) == 1 {
		return parts[0] == fieldName
	}

	// Handle two-part pattern (table.field)
	if len(parts) == 2 {
		return matchesTwoPartPattern(parts[0], parts[1], tableName, fieldName)
	}

	return false
}

// matchesTwoPartPattern checks if a table.field pattern matches
func matchesTwoPartPattern(tablePattern, fieldPattern, tableName, fieldName string) bool {
	// Check for *.*  (all tables, all fields)
	if tablePattern == "*" && fieldPattern == "*" {
		return true
	}

	// Wildcard table or exact table match with specific field
	if (tablePattern == "*" || tablePattern == tableName) && fieldPattern == fieldName {
		return true
	}

	// Exact table with wildcard field
	if tablePattern == tableName && fieldPattern == "*" {
		return true
	}

	return false
}
