// Package clickhouse provides types and utilities for interacting with ClickHouse databases
package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/sirupsen/logrus"
)

// Service defines the interface for ClickHouse operations
type Service interface {
	Connect(ctx context.Context) error
	Close() error
	ListTables(ctx context.Context) ([]string, error)
	GetTable(ctx context.Context, database, tableName string) (*Table, error)
	GetTables(ctx context.Context, database string, tableNames []string) ([]*Table, error)
}

type service struct {
	dsn  string
	conn driver.Conn
	log  logrus.FieldLogger
}

// NewService creates a new ClickHouse service
func NewService(dsn string, log logrus.FieldLogger) Service {
	return &service{
		dsn: dsn,
		log: log.WithField("component", "clickhouse"),
	}
}

func (s *service) Connect(ctx context.Context) error {
	options, err := clickhouse.ParseDSN(s.dsn)
	if err != nil {
		return fmt.Errorf("failed to parse DSN: %w", err)
	}

	conn, err := clickhouse.Open(options)
	if err != nil {
		return fmt.Errorf("failed to open connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping database: %w", err)
	}

	s.conn = conn
	s.log.WithFields(logrus.Fields{
		"database": options.Auth.Database,
		"address":  options.Addr,
	}).Info("Connected to ClickHouse")

	return nil
}

func (s *service) Close() error {
	if s.conn != nil {
		return s.conn.Close()
	}
	return nil
}

func (s *service) ListTables(ctx context.Context) ([]string, error) {
	query := `
		SELECT database || '.' || name AS full_name
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY database, name
	`

	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			s.log.WithError(err).Warn("Failed to close rows")
		}
	}()

	tables := make([]string, 0, 100)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		tables = append(tables, tableName)
	}

	return tables, rows.Err()
}

func (s *service) GetTable(ctx context.Context, database, tableName string) (*Table, error) {
	table := &Table{
		Name:        tableName,
		Database:    database,
		Columns:     []Column{},
		SortingKey:  []string{},
		Projections: []Projection{},
	}

	// Get table metadata
	if err := s.loadTableMetadata(ctx, database, tableName, table); err != nil {
		s.log.WithError(err).Warn("Failed to get table metadata")
	}

	// Get columns
	columns, err := s.loadTableColumns(ctx, database, tableName)
	if err != nil {
		return nil, err
	}
	table.Columns = columns

	// Get projections
	projections, err := s.loadTableProjections(ctx, database, tableName)
	if err != nil {
		s.log.WithError(err).Warn("Failed to get table projections")
		// Continue without projections as they're optional
	} else {
		table.Projections = projections
	}

	// For distributed tables, also get projections from the underlying local table
	s.loadDistributedTableProjections(ctx, database, tableName, table)

	s.log.WithFields(logrus.Fields{
		"database": database,
		"table":    tableName,
		"columns":  len(table.Columns),
	}).Debug("Retrieved table schema")

	return table, nil
}

// loadTableMetadata loads table metadata including comment and sorting key
func (s *service) loadTableMetadata(ctx context.Context, database, tableName string, table *Table) error {
	metaQuery := `
		SELECT comment, sorting_key, engine, engine_full
		FROM system.tables
		WHERE database = ? AND name = ?
	`
	var comment, sortingKey, engine, engineFull sql.NullString
	if err := s.conn.QueryRow(ctx, metaQuery, database, tableName).Scan(&comment, &sortingKey, &engine, &engineFull); err != nil {
		return err
	}

	if comment.Valid {
		table.Comment = comment.String
	}

	// Load sorting key
	s.loadSortingKey(ctx, table, sortingKey, engine, engineFull)
	return nil
}

// loadSortingKey loads the sorting key for a table
func (s *service) loadSortingKey(ctx context.Context, table *Table, sortingKey, engine, engineFull sql.NullString) {
	// Check if sorting key is directly available
	if sortingKey.Valid && sortingKey.String != "" {
		table.SortingKey = parseSortingKey(sortingKey.String)
		return
	}

	// For distributed tables, get sorting key from underlying table
	if !engine.Valid || engine.String != "Distributed" {
		return
	}

	underlyingTable := s.extractUnderlyingTable(engineFull.String)
	if underlyingTable == nil {
		return
	}

	s.log.WithFields(logrus.Fields{
		"distributed_table": fmt.Sprintf("%s.%s", table.Database, table.Name),
		"underlying_table":  fmt.Sprintf("%s.%s", underlyingTable.Database, underlyingTable.Table),
	}).Debug("Getting sorting key from underlying table")

	// Query underlying table for sorting key
	underlyingQuery := `
		SELECT sorting_key
		FROM system.tables
		WHERE database = ? AND name = ?
	`
	var underlyingSortingKey sql.NullString
	if err := s.conn.QueryRow(ctx, underlyingQuery, underlyingTable.Database, underlyingTable.Table).Scan(&underlyingSortingKey); err != nil {
		s.log.WithError(err).Warn("Failed to get underlying table sorting key")
		return
	}

	if underlyingSortingKey.Valid && underlyingSortingKey.String != "" {
		table.SortingKey = parseSortingKey(underlyingSortingKey.String)
	}
}

// loadTableColumns loads the columns for a table
func (s *service) loadTableColumns(ctx context.Context, database, tableName string) ([]Column, error) {
	columnsQuery := `
		SELECT 
			name,
			type,
			default_kind,
			default_expression,
			comment,
			position
		FROM system.columns
		WHERE database = ? AND table = ?
		ORDER BY position
	`

	rows, err := s.conn.Query(ctx, columnsQuery, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			s.log.WithError(err).Warn("Failed to close rows")
		}
	}()

	columns := make([]Column, 0, 100) // Pre-allocate with reasonable capacity
	for rows.Next() {
		var col Column
		var defaultKind, defaultExpr, comment sql.NullString

		if err := rows.Scan(
			&col.Name,
			&col.Type,
			&defaultKind,
			&defaultExpr,
			&comment,
			&col.Position,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		if defaultKind.Valid {
			col.DefaultKind = defaultKind.String
		}
		if defaultExpr.Valid {
			col.DefaultValue = defaultExpr.String
		}
		if comment.Valid {
			col.Comment = comment.String
		}

		// Parse type information
		col.IsNullable = strings.HasPrefix(col.Type, "Nullable(")
		col.IsArray = strings.HasPrefix(col.Type, "Array(")
		col.BaseType = extractBaseType(col.Type)

		columns = append(columns, col)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return columns, nil
}

func (s *service) GetTables(ctx context.Context, database string, tableNames []string) ([]*Table, error) {
	tables := make([]*Table, 0, len(tableNames))

	for _, tableName := range tableNames {
		// Parse database.table format if present
		parts := strings.Split(tableName, ".")
		db := database
		tbl := tableName

		if len(parts) == 2 {
			db = parts[0]
			tbl = parts[1]
		}

		table, err := s.GetTable(ctx, db, tbl)
		if err != nil {
			s.log.WithError(err).WithFields(logrus.Fields{
				"database": db,
				"table":    tbl,
			}).Warn("Failed to get table, skipping")
			continue
		}

		tables = append(tables, table)
	}

	return tables, nil
}

// underlyingTableInfo holds information about an underlying table for distributed tables
type underlyingTableInfo struct {
	Database string
	Table    string
}

// extractUnderlyingTable parses the engine_full string for distributed tables
// Format: Distributed(cluster, database, table, [sharding_key])
func (s *service) extractUnderlyingTable(engineFull string) *underlyingTableInfo {
	if !strings.HasPrefix(engineFull, "Distributed(") {
		return nil
	}

	// Remove "Distributed(" prefix and trailing ")"
	content := strings.TrimPrefix(engineFull, "Distributed(")
	content = strings.TrimSuffix(content, ")")

	// Split by comma (handling potential commas in expressions)
	parts := splitDistributedArgs(content)
	if len(parts) < 3 {
		s.log.WithField("engine_full", engineFull).Warn("Invalid Distributed engine format")
		return nil
	}

	// Extract database and table (positions 1 and 2)
	database := strings.Trim(parts[1], " '\"")
	table := strings.Trim(parts[2], " '\"")

	return &underlyingTableInfo{
		Database: database,
		Table:    table,
	}
}

// splitDistributedArgs splits the Distributed engine arguments
// Handles potential commas within expressions
func splitDistributedArgs(args string) []string {
	var result []string
	var current strings.Builder
	parenDepth := 0
	inString := false
	var stringChar rune

	for _, ch := range args {
		switch ch {
		case '\'', '"':
			if !inString {
				inString = true
				stringChar = ch
			} else if ch == stringChar {
				inString = false
			}
			current.WriteRune(ch)
		case '(':
			if !inString {
				parenDepth++
			}
			current.WriteRune(ch)
		case ')':
			if !inString {
				parenDepth--
			}
			current.WriteRune(ch)
		case ',':
			if !inString && parenDepth == 0 {
				result = append(result, current.String())
				current.Reset()
			} else {
				current.WriteRune(ch)
			}
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		result = append(result, current.String())
	}

	return result
}

func extractBaseType(clickhouseType string) string {
	// Remove Nullable wrapper
	if strings.HasPrefix(clickhouseType, "Nullable(") {
		clickhouseType = strings.TrimPrefix(clickhouseType, "Nullable(")
		clickhouseType = strings.TrimSuffix(clickhouseType, ")")
	}

	// Remove Array wrapper
	if strings.HasPrefix(clickhouseType, "Array(") {
		clickhouseType = strings.TrimPrefix(clickhouseType, "Array(")
		clickhouseType = strings.TrimSuffix(clickhouseType, ")")
	}

	// Extract base type (before parentheses)
	if idx := strings.Index(clickhouseType, "("); idx > 0 {
		return clickhouseType[:idx]
	}

	return clickhouseType
}

// loadTableProjections loads the projections for a table
func (s *service) loadTableProjections(ctx context.Context, database, tableName string) ([]Projection, error) {
	projectionsQuery := `
		SELECT 
			name,
			sorting_key,
			type
		FROM system.projections
		WHERE database = ? AND table = ?
		ORDER BY name
	`

	rows, err := s.conn.Query(ctx, projectionsQuery, database, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query projections: %w", err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			s.log.WithError(err).Warn("Failed to close rows")
		}
	}()

	projections := make([]Projection, 0)
	for rows.Next() {
		var proj Projection
		var sortingKeyArray []string
		var projType string

		if err := rows.Scan(
			&proj.Name,
			&sortingKeyArray,
			&projType,
		); err != nil {
			return nil, fmt.Errorf("failed to scan projection: %w", err)
		}

		// The sorting_key is already an array of strings, no need to parse
		proj.OrderByKey = sortingKeyArray
		proj.Type = projType

		projections = append(projections, proj)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating projections: %w", err)
	}

	return projections, nil
}

// isDistributedTable checks if a table is a distributed table
func (s *service) isDistributedTable(ctx context.Context, database, tableName string) bool {
	query := `
		SELECT engine
		FROM system.tables
		WHERE database = ? AND name = ?
	`
	var engine sql.NullString
	if err := s.conn.QueryRow(ctx, query, database, tableName).Scan(&engine); err != nil {
		return false
	}
	return engine.Valid && engine.String == "Distributed"
}

// getUnderlyingTableName gets the underlying table info for a distributed table
func (s *service) getUnderlyingTableName(ctx context.Context, database, tableName string) *underlyingTableInfo {
	query := `
		SELECT engine_full
		FROM system.tables
		WHERE database = ? AND name = ?
	`
	var engineFull sql.NullString
	if err := s.conn.QueryRow(ctx, query, database, tableName).Scan(&engineFull); err != nil {
		return nil
	}
	if !engineFull.Valid {
		return nil
	}
	return s.extractUnderlyingTable(engineFull.String)
}

// loadDistributedTableProjections loads projections from underlying local table for distributed tables
func (s *service) loadDistributedTableProjections(ctx context.Context, database, tableName string, table *Table) {
	if !s.isDistributedTable(ctx, database, tableName) {
		return
	}

	underlyingTable := s.getUnderlyingTableName(ctx, database, tableName)
	if underlyingTable == nil {
		return
	}

	localProjections, err := s.loadTableProjections(ctx, underlyingTable.Database, underlyingTable.Table)
	if err != nil {
		s.log.WithError(err).WithFields(logrus.Fields{
			"database": underlyingTable.Database,
			"table":    underlyingTable.Table,
		}).Debug("Failed to get projections from underlying local table")
		return
	}

	// Merge projections from local table
	table.Projections = append(table.Projections, localProjections...)
}

// parseSortingKey parses the sorting key expression from ClickHouse
// It handles expressions like "column1, column2" or "column1 ASC, column2 DESC"
func parseSortingKey(sortingKey string) []string {
	if sortingKey == "" {
		return nil
	}

	// Split by comma
	parts := strings.Split(sortingKey, ",")
	columns := make([]string, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Remove ASC/DESC modifiers and any parentheses
		part = strings.TrimSuffix(strings.TrimSuffix(part, " ASC"), " DESC")
		part = strings.Trim(part, "()")

		if part != "" {
			columns = append(columns, part)
		}
	}

	return columns
}
