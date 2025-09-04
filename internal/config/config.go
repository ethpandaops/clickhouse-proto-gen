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
}

// NewConfig creates a new Config instance with default values.
func NewConfig() *Config {
	return &Config{
		OutputDir:       "./proto",
		Package:         "clickhouse.v1",
		IncludeComments: true,
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
func (c *Config) MergeFlags(dsn, outputDir, pkg, goPkg, tables string, includeComments bool) {
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
}
