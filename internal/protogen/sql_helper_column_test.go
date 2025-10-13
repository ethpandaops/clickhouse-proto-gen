package protogen

import (
	"testing"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/clickhouse"
	"github.com/stretchr/testify/assert"
)

// TestGetSelectColumnExpression tests that column expressions are generated correctly
// with appropriate type conversions for ClickHouse compatibility
func TestGetSelectColumnExpression(t *testing.T) {
	tests := []struct {
		name     string
		column   clickhouse.Column
		expected string
	}{
		// DateTime conversions
		{
			name: "DateTime to Unix timestamp",
			column: clickhouse.Column{
				Name:     "created_at",
				Type:     "DateTime",
				BaseType: "DateTime",
				IsArray:  false,
			},
			expected: "toUnixTimestamp(`created_at`) AS `created_at`",
		},
		{
			name: "DateTime64 to Unix timestamp micro",
			column: clickhouse.Column{
				Name:     "updated_at",
				Type:     "DateTime64(6)",
				BaseType: "DateTime64",
				IsArray:  false,
			},
			expected: "toUnixTimestamp64Micro(`updated_at`) AS `updated_at`",
		},
		{
			name: "Array(DateTime) with arrayMap",
			column: clickhouse.Column{
				Name:     "timestamps",
				Type:     "Array(DateTime)",
				BaseType: "DateTime",
				IsArray:  true,
			},
			expected: "arrayMap(x -> toUnixTimestamp(x), `timestamps`) AS `timestamps`",
		},
		{
			name: "Array(DateTime64) with arrayMap",
			column: clickhouse.Column{
				Name:     "timestamps64",
				Type:     "Array(DateTime64(6))",
				BaseType: "DateTime64",
				IsArray:  true,
			},
			expected: "arrayMap(x -> toUnixTimestamp64Micro(x), `timestamps64`) AS `timestamps64`",
		},

		// Date conversions (new)
		{
			name: "Date to string",
			column: clickhouse.Column{
				Name:     "birth_date",
				Type:     "Date",
				BaseType: "Date",
				IsArray:  false,
			},
			expected: "toString(`birth_date`) AS `birth_date`",
		},
		{
			name: "Date32 to string",
			column: clickhouse.Column{
				Name:     "event_date",
				Type:     "Date32",
				BaseType: "Date32",
				IsArray:  false,
			},
			expected: "toString(`event_date`) AS `event_date`",
		},
		{
			name: "Array(Date) with arrayMap",
			column: clickhouse.Column{
				Name:     "dates",
				Type:     "Array(Date)",
				BaseType: "Date",
				IsArray:  true,
			},
			expected: "arrayMap(x -> toString(x), `dates`) AS `dates`",
		},
		{
			name: "Array(Date32) with arrayMap",
			column: clickhouse.Column{
				Name:     "dates32",
				Type:     "Array(Date32)",
				BaseType: "Date32",
				IsArray:  true,
			},
			expected: "arrayMap(x -> toString(x), `dates32`) AS `dates32`",
		},

		// UInt8/UInt16 conversions (new)
		{
			name: "UInt8 to UInt32",
			column: clickhouse.Column{
				Name:     "status",
				Type:     "UInt8",
				BaseType: "UInt8",
				IsArray:  false,
			},
			expected: "toUInt32(`status`) AS `status`",
		},
		{
			name: "UInt16 to UInt32",
			column: clickhouse.Column{
				Name:     "port",
				Type:     "UInt16",
				BaseType: "UInt16",
				IsArray:  false,
			},
			expected: "toUInt32(`port`) AS `port`",
		},
		{
			name: "Array(UInt8) with arrayMap",
			column: clickhouse.Column{
				Name:     "flags",
				Type:     "Array(UInt8)",
				BaseType: "UInt8",
				IsArray:  true,
			},
			expected: "arrayMap(x -> toUInt32(x), `flags`) AS `flags`",
		},
		{
			name: "Array(UInt16) with arrayMap",
			column: clickhouse.Column{
				Name:     "ports",
				Type:     "Array(UInt16)",
				BaseType: "UInt16",
				IsArray:  true,
			},
			expected: "arrayMap(x -> toUInt32(x), `ports`) AS `ports`",
		},

		// Nullable versions
		{
			name: "Nullable(Date) to string",
			column: clickhouse.Column{
				Name:       "optional_date",
				Type:       "Nullable(Date)",
				BaseType:   "Date",
				IsNullable: true,
				IsArray:    false,
			},
			expected: "toString(`optional_date`) AS `optional_date`",
		},
		{
			name: "Nullable(UInt8) to UInt32",
			column: clickhouse.Column{
				Name:       "optional_status",
				Type:       "Nullable(UInt8)",
				BaseType:   "UInt8",
				IsNullable: true,
				IsArray:    false,
			},
			expected: "toUInt32(`optional_status`) AS `optional_status`",
		},

		// Large integer conversions (existing)
		{
			name: "UInt256 to string",
			column: clickhouse.Column{
				Name:     "hash",
				Type:     "UInt256",
				BaseType: "UInt256",
				IsArray:  false,
			},
			expected: "toString(`hash`) AS `hash`",
		},
		{
			name: "Array(UInt256) with arrayMap",
			column: clickhouse.Column{
				Name:     "hashes",
				Type:     "Array(UInt256)",
				BaseType: "UInt256",
				IsArray:  true,
			},
			expected: "arrayMap(x -> toString(x), `hashes`) AS `hashes`",
		},

		// Regular types (no conversion)
		{
			name: "String - no conversion",
			column: clickhouse.Column{
				Name:     "name",
				Type:     "String",
				BaseType: "String",
				IsArray:  false,
			},
			expected: "name",
		},
		{
			name: "UInt32 - no conversion",
			column: clickhouse.Column{
				Name:     "count",
				Type:     "UInt32",
				BaseType: "UInt32",
				IsArray:  false,
			},
			expected: "count",
		},
		{
			name: "UInt64 - no conversion",
			column: clickhouse.Column{
				Name:     "id",
				Type:     "UInt64",
				BaseType: "UInt64",
				IsArray:  false,
			},
			expected: "id",
		},
		{
			name: "Array(String) - no conversion",
			column: clickhouse.Column{
				Name:     "tags",
				Type:     "Array(String)",
				BaseType: "String",
				IsArray:  true,
			},
			expected: "tags",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getSelectColumnExpression(&tt.column)
			assert.Equal(t, tt.expected, result,
				"Column expression mismatch for %s (%s)",
				tt.column.Name, tt.column.Type)
		})
	}
}

// TestNeedsStringConversion tests the helper function for large integer detection
func TestNeedsStringConversion(t *testing.T) {
	tests := []struct {
		name     string
		column   clickhouse.Column
		expected bool
	}{
		{
			name:     "UInt128 needs string conversion",
			column:   clickhouse.Column{BaseType: "UInt128"},
			expected: true,
		},
		{
			name:     "UInt256 needs string conversion",
			column:   clickhouse.Column{BaseType: "UInt256"},
			expected: true,
		},
		{
			name:     "Int128 needs string conversion",
			column:   clickhouse.Column{BaseType: "Int128"},
			expected: true,
		},
		{
			name:     "Int256 needs string conversion",
			column:   clickhouse.Column{BaseType: "Int256"},
			expected: true,
		},
		{
			name:     "UInt8 does not need string conversion",
			column:   clickhouse.Column{BaseType: "UInt8"},
			expected: false,
		},
		{
			name:     "UInt16 does not need string conversion",
			column:   clickhouse.Column{BaseType: "UInt16"},
			expected: false,
		},
		{
			name:     "UInt32 does not need string conversion",
			column:   clickhouse.Column{BaseType: "UInt32"},
			expected: false,
		},
		{
			name:     "UInt64 does not need string conversion",
			column:   clickhouse.Column{BaseType: "UInt64"},
			expected: false,
		},
		{
			name:     "String does not need string conversion",
			column:   clickhouse.Column{BaseType: "String"},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := needsStringConversion(&tt.column)
			assert.Equal(t, tt.expected, result,
				"String conversion check failed for %s", tt.column.BaseType)
		})
	}
}
