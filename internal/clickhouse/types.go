// Package clickhouse provides types and utilities for interacting with ClickHouse databases
package clickhouse

// Table represents a ClickHouse table structure with its columns and metadata
type Table struct {
	Name       string
	Database   string
	Comment    string
	Columns    []Column
	SortingKey []string // ORDER BY columns
}

// Column represents a ClickHouse table column with its properties
type Column struct {
	Name         string
	Type         string
	DefaultKind  string
	DefaultValue string
	Comment      string
	Position     uint64
	IsNullable   bool
	IsArray      bool
	BaseType     string
}

// TableMetadata contains additional metadata about a ClickHouse table
type TableMetadata struct {
	Database    string
	Name        string
	Engine      string
	CreateTable string
	Comment     string
}
