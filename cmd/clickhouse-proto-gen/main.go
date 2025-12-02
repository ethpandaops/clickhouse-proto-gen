// Package main provides the CLI entry point for clickhouse-proto-gen
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/clickhouse"
	"github.com/ethpandaops/clickhouse-proto-gen/internal/config"
	"github.com/ethpandaops/clickhouse-proto-gen/internal/protogen"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Error definitions
var (
	errNoValidTables = errors.New("no valid tables found to generate proto files")
)

//nolint:gochecknoglobals // Version info set by ldflags during build
var (
	// Version info (set by ldflags)
	Release = "dev"
	Commit  = "none"
)

// CLI flags - global variables are acceptable for cobra CLI applications
//
//nolint:gochecknoglobals
var (
	dsn                  string
	tables               string
	outputDir            string
	pkg                  string
	goPackage            string
	includeComments      bool
	configFile           string
	verbose              bool
	debug                bool
	maxPageSize          int32
	enableAPI            bool
	apiBasePath          string
	apiTablePrefixes     string
	bigIntToStringFields string
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

//nolint:gochecknoglobals // Standard cobra pattern for CLI root command
var rootCmd = &cobra.Command{
	Use:   "clickhouse-proto-gen",
	Short: "Generate Protocol Buffer schemas from ClickHouse tables",
	Long: `clickhouse-proto-gen is a CLI tool that connects to a ClickHouse database,
introspects table schemas, and generates corresponding Protocol Buffer (.proto) files.

Example usage:
  clickhouse-proto-gen --dsn "clickhouse://user:pass@localhost:9000/mydb" --tables users,orders --out ./proto

Or with a config file:
  clickhouse-proto-gen --config config.yaml`,
	Version: fmt.Sprintf("%s (commit: %s)", Release, Commit),
	RunE:    run,
}

func init() {
	// Database connection flags
	rootCmd.Flags().StringVar(&dsn, "dsn", "", "ClickHouse DSN (e.g., clickhouse://user:pass@host:9000/db)")

	// Table selection flags
	rootCmd.Flags().StringVar(&tables, "tables", "", "Comma-separated list of tables to generate (e.g., users,orders or db.users,db.orders)")

	// Output configuration flags
	rootCmd.Flags().StringVar(&outputDir, "out", "./proto", "Output directory for generated proto files")
	rootCmd.Flags().StringVar(&pkg, "package", "clickhouse.v1", "Protocol Buffer package name")
	rootCmd.Flags().StringVar(&goPackage, "go-package", "", "Go package path (e.g., github.com/acme/project/gen/clickhousev1)")
	rootCmd.Flags().BoolVar(&includeComments, "include-comments", true, "Include table and column comments in proto files")

	// Config file flag
	rootCmd.Flags().StringVarP(&configFile, "config", "c", "", "Path to YAML configuration file")

	// Logging flags
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose output")
	rootCmd.Flags().BoolVar(&debug, "debug", false, "Enable debug output")

	// Pagination flags
	rootCmd.Flags().Int32Var(&maxPageSize, "max-page-size", 10000, "Maximum page size for List operations (default: 10000)")

	// API generation flags
	rootCmd.Flags().BoolVar(&enableAPI, "enable-api", false, "Enable generation of HTTP annotations for REST API endpoints")
	rootCmd.Flags().StringVar(&apiBasePath, "api-base-path", "/api/v1", "Base path for API endpoints (e.g., /api/v1)")
	rootCmd.Flags().StringVar(&apiTablePrefixes, "api-table-prefixes", "", "Comma-separated list of table prefixes to expose via REST API (e.g., fct_,dim_)")

	// Type conversion flags
	rootCmd.Flags().StringVar(&bigIntToStringFields, "bigint-to-string", "", "Comma-separated list of Int64/UInt64 fields to convert to string for JavaScript precision (e.g., 'table.field,*.field')")
}

func run(_ *cobra.Command, _ []string) error {
	// Setup logger
	log := setupLogger()

	// Load configuration
	cfg := config.NewConfig()

	// Load from config file if provided
	if configFile != "" {
		if err := cfg.LoadFromFile(configFile, log); err != nil {
			return fmt.Errorf("failed to load config file: %w", err)
		}
	}

	// Merge command-line flags (override config file values)
	cfg.MergeFlags(dsn, outputDir, pkg, goPackage, tables, includeComments, maxPageSize, enableAPI, apiBasePath, apiTablePrefixes, bigIntToStringFields)

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	log.WithFields(logrus.Fields{
		"output_dir":  cfg.OutputDir,
		"package":     cfg.Package,
		"table_count": len(cfg.Tables),
	}).Info("Starting proto generation")

	// Create context
	ctx := context.Background()

	// Connect to ClickHouse
	ch := clickhouse.NewService(cfg.DSN, log)
	if err := ch.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer func() {
		if err := ch.Close(); err != nil {
			log.WithError(err).Warn("Failed to close ClickHouse connection")
		}
	}()

	// Get tables to process
	tablesToProcess := getTableList(ctx, ch, cfg, log)

	if len(tablesToProcess) == 0 {
		log.Warn("No tables found to process")
		return nil
	}

	log.WithField("table_count", len(tablesToProcess)).Info("Processing tables")

	// Fetch table schemas
	tables := make([]*clickhouse.Table, 0, len(tablesToProcess))
	for _, tableName := range tablesToProcess {
		parts := strings.Split(tableName, ".")
		var db, tbl string

		if len(parts) == 2 {
			db = parts[0]
			tbl = parts[1]
		} else {
			// Extract database from DSN if not specified
			db = extractDatabaseFromDSN(cfg.DSN)
			tbl = tableName
		}

		table, err := ch.GetTable(ctx, db, tbl)
		if err != nil {
			log.WithError(err).WithFields(logrus.Fields{
				"database": db,
				"table":    tbl,
			}).Warn("Failed to get table schema, skipping")
			continue
		}

		tables = append(tables, table)
	}

	if len(tables) == 0 {
		return errNoValidTables
	}

	// Generate proto files
	generator := protogen.NewGenerator(cfg, log)
	if err := generator.Generate(tables); err != nil {
		return fmt.Errorf("failed to generate proto files: %w", err)
	}

	log.WithFields(logrus.Fields{
		"tables_processed": len(tables),
		"output_dir":       cfg.OutputDir,
	}).Info("Proto generation completed successfully")

	return nil
}

func setupLogger() logrus.FieldLogger {
	log := logrus.New()
	log.SetFormatter(&logrus.TextFormatter{
		DisableTimestamp: false,
		FullTimestamp:    true,
	})

	switch {
	case debug:
		log.SetLevel(logrus.DebugLevel)
	case verbose:
		log.SetLevel(logrus.InfoLevel)
	default:
		log.SetLevel(logrus.WarnLevel)
	}

	return log
}

func getTableList(_ context.Context, _ clickhouse.Service, cfg *config.Config, log logrus.FieldLogger) []string {
	// Use specified tables
	tablesToProcess := cfg.Tables
	log.WithField("table_count", len(tablesToProcess)).Debug("Tables to process")
	return tablesToProcess
}

func extractDatabaseFromDSN(dsn string) string {
	// Basic extraction - finds the database name from DSN
	// Format: clickhouse://user:pass@host:port/database

	parts := strings.Split(dsn, "/")
	if len(parts) > 0 {
		dbPart := parts[len(parts)-1]
		// Remove any query parameters
		if idx := strings.Index(dbPart, "?"); idx > 0 {
			dbPart = dbPart[:idx]
		}
		if dbPart != "" {
			return dbPart
		}
	}

	return "default"
}
