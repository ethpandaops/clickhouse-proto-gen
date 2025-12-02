package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConfig(t *testing.T) {
	cfg := NewConfig()

	require.NotNil(t, cfg)
	assert.Equal(t, "./proto", cfg.OutputDir)
	assert.Equal(t, "clickhouse.v1", cfg.Package)
	assert.True(t, cfg.IncludeComments)
	assert.Empty(t, cfg.DSN)
	assert.Empty(t, cfg.Tables)
	assert.Empty(t, cfg.GoPackage)
}

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		wantErr   bool
		expectErr error
	}{
		{
			name: "Valid configuration",
			config: Config{
				DSN:       "clickhouse://localhost:9000/test",
				OutputDir: "./proto",
				Package:   "test.v1",
				Tables:    []string{"users", "orders"},
			},
			wantErr: false,
		},
		{
			name: "Missing DSN",
			config: Config{
				OutputDir: "./proto",
				Package:   "test.v1",
				Tables:    []string{"users"},
			},
			wantErr:   true,
			expectErr: ErrDSNRequired,
		},
		{
			name: "Missing output directory",
			config: Config{
				DSN:       "clickhouse://localhost:9000/test",
				OutputDir: "",
				Package:   "test.v1",
				Tables:    []string{"users"},
			},
			wantErr:   true,
			expectErr: ErrOutputDirRequired,
		},
		{
			name: "Missing package",
			config: Config{
				DSN:       "clickhouse://localhost:9000/test",
				OutputDir: "./proto",
				Package:   "",
				Tables:    []string{"users"},
			},
			wantErr:   true,
			expectErr: ErrPackageRequired,
		},
		{
			name: "Missing tables",
			config: Config{
				DSN:       "clickhouse://localhost:9000/test",
				OutputDir: "./proto",
				Package:   "test.v1",
				Tables:    []string{},
			},
			wantErr:   true,
			expectErr: ErrTablesRequired,
		},
		{
			name: "Nil tables",
			config: Config{
				DSN:       "clickhouse://localhost:9000/test",
				OutputDir: "./proto",
				Package:   "test.v1",
				Tables:    nil,
			},
			wantErr:   true,
			expectErr: ErrTablesRequired,
		},
		{
			name: "Complete configuration with GoPackage",
			config: Config{
				DSN:             "clickhouse://localhost:9000/test",
				OutputDir:       "./proto",
				Package:         "test.v1",
				GoPackage:       "github.com/test/proto",
				Tables:          []string{"users", "orders", "products"},
				IncludeComments: true,
			},
			wantErr: false,
		},
		{
			name: "Configuration with cross-database tables",
			config: Config{
				DSN:       "clickhouse://localhost:9000",
				OutputDir: "./proto",
				Package:   "test.v1",
				Tables:    []string{"db1.users", "db2.orders"},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()

			if tt.wantErr {
				require.Error(t, err)
				if tt.expectErr != nil {
					assert.ErrorIs(t, err, tt.expectErr)
				}
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestConfig_LoadFromFile(t *testing.T) {
	tests := []struct {
		name        string
		yamlContent string
		expectErr   bool
		validate    func(t *testing.T, cfg *Config)
	}{
		{
			name: "Valid YAML file",
			yamlContent: `
dsn: clickhouse://user:pass@localhost:9000/mydb
tables:
  - users
  - orders
  - products
output_dir: ./generated
package: myapp.v1
go_package: github.com/myapp/proto
include_comments: true
`,
			expectErr: false,
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "clickhouse://user:pass@localhost:9000/mydb", cfg.DSN)
				assert.Equal(t, []string{"users", "orders", "products"}, cfg.Tables)
				assert.Equal(t, "./generated", cfg.OutputDir)
				assert.Equal(t, "myapp.v1", cfg.Package)
				assert.Equal(t, "github.com/myapp/proto", cfg.GoPackage)
				assert.True(t, cfg.IncludeComments)
			},
		},
		{
			name: "Minimal YAML file",
			yamlContent: `
dsn: clickhouse://localhost:9000/test
tables:
  - users
`,
			expectErr: false,
			validate: func(t *testing.T, cfg *Config) {
				assert.Equal(t, "clickhouse://localhost:9000/test", cfg.DSN)
				assert.Equal(t, []string{"users"}, cfg.Tables)
				// Check defaults are preserved
				assert.Equal(t, "./proto", cfg.OutputDir)
				assert.Equal(t, "clickhouse.v1", cfg.Package)
			},
		},
		{
			name: "YAML with comments disabled",
			yamlContent: `
dsn: clickhouse://localhost:9000/test
tables:
  - users
include_comments: false
`,
			expectErr: false,
			validate: func(t *testing.T, cfg *Config) {
				assert.False(t, cfg.IncludeComments)
			},
		},
		{
			name:        "Invalid YAML",
			yamlContent: `invalid yaml content: [}`,
			expectErr:   true,
			validate:    nil,
		},
		{
			name:        "Empty YAML file",
			yamlContent: ``,
			expectErr:   false,
			validate: func(t *testing.T, cfg *Config) {
				// Should maintain defaults
				assert.Equal(t, "./proto", cfg.OutputDir)
				assert.Equal(t, "clickhouse.v1", cfg.Package)
				assert.True(t, cfg.IncludeComments)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temporary YAML file
			tmpFile, err := os.CreateTemp("", "config_test_*.yaml")
			require.NoError(t, err)
			defer os.Remove(tmpFile.Name())

			_, err = tmpFile.WriteString(tt.yamlContent)
			require.NoError(t, err)
			tmpFile.Close()

			// Load configuration
			cfg := NewConfig()
			log := logrus.New()
			log.SetLevel(logrus.DebugLevel)

			err = cfg.LoadFromFile(tmpFile.Name(), log)

			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.validate != nil {
					tt.validate(t, cfg)
				}
			}
		})
	}
}

func TestConfig_LoadFromFile_FileNotExists(t *testing.T) {
	cfg := NewConfig()
	log := logrus.New()

	err := cfg.LoadFromFile("/nonexistent/file.yaml", log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to read config file")
}

func TestConfig_MergeFlags(t *testing.T) {
	tests := []struct {
		name             string
		initial          Config
		dsn              string
		outputDir        string
		pkg              string
		goPkg            string
		tables           string
		includeComments  bool
		enableAPI        bool
		apiBasePath      string
		apiTablePrefixes string
		expected         Config
	}{
		{
			name: "Merge all flags",
			initial: Config{
				DSN:             "clickhouse://old:9000/olddb",
				OutputDir:       "./old",
				Package:         "old.v1",
				GoPackage:       "github.com/old/proto",
				Tables:          []string{"old_table"},
				IncludeComments: false,
			},
			dsn:              "clickhouse://new:9000/newdb",
			outputDir:        "./new",
			pkg:              "new.v1",
			goPkg:            "github.com/new/proto",
			tables:           "table1,table2,table3",
			includeComments:  true,
			enableAPI:        false,
			apiBasePath:      "",
			apiTablePrefixes: "",
			expected: Config{
				DSN:             "clickhouse://new:9000/newdb",
				OutputDir:       "./new",
				Package:         "new.v1",
				GoPackage:       "github.com/new/proto",
				Tables:          []string{"table1", "table2", "table3"},
				IncludeComments: true,
			},
		},
		{
			name: "Merge partial flags",
			initial: Config{
				DSN:             "clickhouse://old:9000/olddb",
				OutputDir:       "./old",
				Package:         "old.v1",
				GoPackage:       "github.com/old/proto",
				Tables:          []string{"old_table"},
				IncludeComments: true,
			},
			dsn:              "", // Keep old
			outputDir:        "./partial",
			pkg:              "", // Keep old
			goPkg:            "github.com/partial/proto",
			tables:           "", // Keep old
			includeComments:  false,
			enableAPI:        false,
			apiBasePath:      "",
			apiTablePrefixes: "",
			expected: Config{
				DSN:             "clickhouse://old:9000/olddb",
				OutputDir:       "./partial",
				Package:         "old.v1",
				GoPackage:       "github.com/partial/proto",
				Tables:          []string{"old_table"},
				IncludeComments: false,
			},
		},
		{
			name: "Parse tables with spaces",
			initial: Config{
				Tables: []string{"old"},
			},
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           " table1 , table2 , table3 ",
			includeComments:  true,
			enableAPI:        false,
			apiBasePath:      "",
			apiTablePrefixes: "",
			expected: Config{
				Tables:          []string{"table1", "table2", "table3"},
				IncludeComments: true,
			},
		},
		{
			name: "Empty flags don't override",
			initial: Config{
				DSN:             "clickhouse://localhost:9000/test",
				OutputDir:       "./proto",
				Package:         "test.v1",
				GoPackage:       "github.com/test/proto",
				Tables:          []string{"users"},
				IncludeComments: true,
			},
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           "",
			includeComments:  true,
			enableAPI:        false,
			apiBasePath:      "",
			apiTablePrefixes: "",
			expected: Config{
				DSN:             "clickhouse://localhost:9000/test",
				OutputDir:       "./proto",
				Package:         "test.v1",
				GoPackage:       "github.com/test/proto",
				Tables:          []string{"users"},
				IncludeComments: true,
			},
		},
		{
			name: "Cross-database tables",
			initial: Config{
				Tables: []string{"old"},
			},
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           "db1.users, db2.orders, db3.products",
			includeComments:  false,
			enableAPI:        false,
			apiBasePath:      "",
			apiTablePrefixes: "",
			expected: Config{
				Tables:          []string{"db1.users", "db2.orders", "db3.products"},
				IncludeComments: false,
			},
		},
		{
			name: "Enable API with defaults from NewConfig",
			initial: func() Config {
				cfg := NewConfig()
				cfg.DSN = "clickhouse://localhost:9000/test"
				cfg.OutputDir = "./proto"
				return *cfg
			}(),
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           "",
			includeComments:  true, // Preserve from initial
			enableAPI:        true,
			apiBasePath:      "",
			apiTablePrefixes: "",
			expected: Config{
				DSN:              "clickhouse://localhost:9000/test",
				OutputDir:        "./proto",
				Package:          "clickhouse.v1",
				IncludeComments:  true,
				MaxPageSize:      10000,
				EnableAPI:        true,
				APIBasePath:      "/api/v1", // Default from NewConfig()
				APITablePrefixes: []string{},
			},
		},
		{
			name: "Enable API with custom base path",
			initial: Config{
				DSN:       "clickhouse://localhost:9000/test",
				OutputDir: "./proto",
			},
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           "",
			includeComments:  false,
			enableAPI:        true,
			apiBasePath:      "/api/v2",
			apiTablePrefixes: "",
			expected: Config{
				DSN:         "clickhouse://localhost:9000/test",
				OutputDir:   "./proto",
				EnableAPI:   true,
				APIBasePath: "/api/v2",
			},
		},
		{
			name: "Enable API with table prefixes",
			initial: Config{
				DSN:       "clickhouse://localhost:9000/test",
				OutputDir: "./proto",
			},
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           "",
			includeComments:  false,
			enableAPI:        true,
			apiBasePath:      "/api/v1",
			apiTablePrefixes: "fct_,dim_",
			expected: Config{
				DSN:              "clickhouse://localhost:9000/test",
				OutputDir:        "./proto",
				EnableAPI:        true,
				APIBasePath:      "/api/v1",
				APITablePrefixes: []string{"fct_", "dim_"},
			},
		},
		{
			name: "Enable API with table prefixes and spaces",
			initial: Config{
				DSN:       "clickhouse://localhost:9000/test",
				OutputDir: "./proto",
			},
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           "",
			includeComments:  false,
			enableAPI:        true,
			apiBasePath:      "/api/v1",
			apiTablePrefixes: " fct_ , dim_ , stg_ ",
			expected: Config{
				DSN:              "clickhouse://localhost:9000/test",
				OutputDir:        "./proto",
				EnableAPI:        true,
				APIBasePath:      "/api/v1",
				APITablePrefixes: []string{"fct_", "dim_", "stg_"},
			},
		},
		{
			name: "Disable API (default)",
			initial: Config{
				DSN:              "clickhouse://localhost:9000/test",
				OutputDir:        "./proto",
				EnableAPI:        true, // Start with API enabled
				APIBasePath:      "/api/v1",
				APITablePrefixes: []string{"fct_"},
			},
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           "",
			includeComments:  false,
			enableAPI:        false,
			apiBasePath:      "",
			apiTablePrefixes: "",
			expected: Config{
				DSN:              "clickhouse://localhost:9000/test",
				OutputDir:        "./proto",
				EnableAPI:        false, // Should be disabled
				APIBasePath:      "/api/v1",
				APITablePrefixes: []string{"fct_"},
			},
		},
		{
			name: "Override existing API settings",
			initial: Config{
				DSN:              "clickhouse://localhost:9000/test",
				OutputDir:        "./proto",
				EnableAPI:        false,
				APIBasePath:      "/old/path",
				APITablePrefixes: []string{"old_"},
			},
			dsn:              "",
			outputDir:        "",
			pkg:              "",
			goPkg:            "",
			tables:           "",
			includeComments:  false,
			enableAPI:        true,
			apiBasePath:      "/new/path",
			apiTablePrefixes: "new_,modern_",
			expected: Config{
				DSN:              "clickhouse://localhost:9000/test",
				OutputDir:        "./proto",
				EnableAPI:        true,
				APIBasePath:      "/new/path",
				APITablePrefixes: []string{"new_", "modern_"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.initial
			cfg.MergeFlags(tt.dsn, tt.outputDir, tt.pkg, tt.goPkg, tt.tables, tt.includeComments, 0, tt.enableAPI, tt.apiBasePath, tt.apiTablePrefixes, "")
			assert.Equal(t, tt.expected, cfg)
		})
	}
}

func TestConfig_PathCleaning(t *testing.T) {
	// Create a temporary directory structure for testing
	tempDir, err := os.MkdirTemp("", "config_path_test_*")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a config file
	configPath := filepath.Join(tempDir, "config.yaml")
	configContent := `
dsn: clickhouse://localhost:9000/test
tables:
  - users
`
	err = os.WriteFile(configPath, []byte(configContent), 0o600)
	require.NoError(t, err)

	tests := []struct {
		name      string
		inputPath string
		shouldErr bool
	}{
		{
			name:      "Normal path",
			inputPath: configPath,
			shouldErr: false,
		},
		{
			name:      "Path with ..",
			inputPath: filepath.Join(tempDir, "subdir", "..", "config.yaml"),
			shouldErr: false,
		},
		{
			name:      "Path with multiple ..",
			inputPath: filepath.Join(tempDir, "a", "b", "..", "..", "config.yaml"),
			shouldErr: false,
		},
		{
			name:      "Non-existent path",
			inputPath: filepath.Join(tempDir, "nonexistent.yaml"),
			shouldErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := NewConfig()
			log := logrus.New()
			log.SetLevel(logrus.WarnLevel)

			err := cfg.LoadFromFile(tt.inputPath, log)

			if tt.shouldErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, "clickhouse://localhost:9000/test", cfg.DSN)
				assert.Equal(t, []string{"users"}, cfg.Tables)
			}
		})
	}
}

func TestConfig_YAMLUnmarshalError(t *testing.T) {
	// Create a temp file with content that will cause YAML unmarshal error
	tmpFile, err := os.CreateTemp("", "bad_yaml_*.yaml")
	require.NoError(t, err)
	defer os.Remove(tmpFile.Name())

	// Write content that causes YAML unmarshal to fail
	badYAML := `
dsn: clickhouse://localhost:9000/test
tables:
  - valid_table
  invalid_key_without_value:
  : no_key_just_value
`
	_, err = tmpFile.WriteString(badYAML)
	require.NoError(t, err)
	tmpFile.Close()

	cfg := NewConfig()
	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	err = cfg.LoadFromFile(tmpFile.Name(), log)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse config file")
}

func TestConversionConfig_ShouldConvertToString(t *testing.T) {
	tests := []struct {
		name      string
		config    ConversionConfig
		tableName string
		fieldName string
		expected  bool
	}{
		// Table-scoped configuration tests
		{
			name: "table-scoped exact match",
			config: ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value", "execution_payload_value"},
				},
			},
			tableName: "fct_prepared_block",
			fieldName: "consensus_payload_value",
			expected:  true,
		},
		{
			name: "table-scoped no match - different field",
			config: ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			tableName: "fct_prepared_block",
			fieldName: "block_number",
			expected:  false,
		},
		{
			name: "table-scoped no match - different table",
			config: ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
			},
			tableName: "other_table",
			fieldName: "consensus_payload_value",
			expected:  false,
		},

		// CLI pattern tests - exact match
		{
			name: "CLI pattern - exact table.field match",
			config: ConversionConfig{
				BigIntToStringFields: []string{"fct_prepared_block.consensus_payload_value"},
			},
			tableName: "fct_prepared_block",
			fieldName: "consensus_payload_value",
			expected:  true,
		},
		{
			name: "CLI pattern - exact match no match different table",
			config: ConversionConfig{
				BigIntToStringFields: []string{"fct_prepared_block.consensus_payload_value"},
			},
			tableName: "other_table",
			fieldName: "consensus_payload_value",
			expected:  false,
		},

		// CLI pattern tests - wildcard table
		{
			name: "CLI pattern - wildcard table *.field",
			config: ConversionConfig{
				BigIntToStringFields: []string{"*.block_number"},
			},
			tableName: "fct_prepared_block",
			fieldName: "block_number",
			expected:  true,
		},
		{
			name: "CLI pattern - wildcard table *.field matches any table",
			config: ConversionConfig{
				BigIntToStringFields: []string{"*.slot"},
			},
			tableName: "any_table",
			fieldName: "slot",
			expected:  true,
		},
		{
			name: "CLI pattern - wildcard table *.field no match different field",
			config: ConversionConfig{
				BigIntToStringFields: []string{"*.block_number"},
			},
			tableName: "fct_prepared_block",
			fieldName: "slot",
			expected:  false,
		},

		// CLI pattern tests - wildcard field
		{
			name: "CLI pattern - specific table wildcard field table.*",
			config: ConversionConfig{
				BigIntToStringFields: []string{"fct_prepared_block.*"},
			},
			tableName: "fct_prepared_block",
			fieldName: "any_field",
			expected:  true,
		},
		{
			name: "CLI pattern - specific table wildcard field table.* no match different table",
			config: ConversionConfig{
				BigIntToStringFields: []string{"fct_prepared_block.*"},
			},
			tableName: "other_table",
			fieldName: "any_field",
			expected:  false,
		},

		// CLI pattern tests - full wildcard
		{
			name: "CLI pattern - full wildcard *.* matches everything",
			config: ConversionConfig{
				BigIntToStringFields: []string{"*.*"},
			},
			tableName: "any_table",
			fieldName: "any_field",
			expected:  true,
		},

		// CLI pattern tests - field only (no table prefix)
		{
			name: "CLI pattern - field only (no prefix) matches any table",
			config: ConversionConfig{
				BigIntToStringFields: []string{"block_number"},
			},
			tableName: "any_table",
			fieldName: "block_number",
			expected:  true,
		},
		{
			name: "CLI pattern - field only no match different field",
			config: ConversionConfig{
				BigIntToStringFields: []string{"block_number"},
			},
			tableName: "any_table",
			fieldName: "slot",
			expected:  false,
		},

		// Combined configuration tests
		{
			name: "combined - table-scoped match",
			config: ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
				BigIntToStringFields: []string{"*.block_number"},
			},
			tableName: "fct_prepared_block",
			fieldName: "consensus_payload_value",
			expected:  true,
		},
		{
			name: "combined - CLI pattern match",
			config: ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
				BigIntToStringFields: []string{"*.block_number"},
			},
			tableName: "other_table",
			fieldName: "block_number",
			expected:  true,
		},
		{
			name: "combined - no match",
			config: ConversionConfig{
				BigIntToString: map[string][]string{
					"fct_prepared_block": {"consensus_payload_value"},
				},
				BigIntToStringFields: []string{"*.block_number"},
			},
			tableName: "other_table",
			fieldName: "slot",
			expected:  false,
		},

		// Empty configuration tests
		{
			name:      "empty config - no match",
			config:    ConversionConfig{},
			tableName: "any_table",
			fieldName: "any_field",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.config.ShouldConvertToString(tt.tableName, tt.fieldName)
			assert.Equal(t, tt.expected, result, "ShouldConvertToString mismatch")
		})
	}
}

func TestMatchesPattern(t *testing.T) {
	tests := []struct {
		name      string
		pattern   string
		tableName string
		fieldName string
		expected  bool
	}{
		// Exact matches
		{
			name:      "exact match table.field",
			pattern:   "fct_prepared_block.consensus_payload_value",
			tableName: "fct_prepared_block",
			fieldName: "consensus_payload_value",
			expected:  true,
		},
		{
			name:      "exact match - wrong table",
			pattern:   "fct_prepared_block.consensus_payload_value",
			tableName: "other_table",
			fieldName: "consensus_payload_value",
			expected:  false,
		},
		{
			name:      "exact match - wrong field",
			pattern:   "fct_prepared_block.consensus_payload_value",
			tableName: "fct_prepared_block",
			fieldName: "other_field",
			expected:  false,
		},

		// Wildcard table patterns
		{
			name:      "wildcard table *.field",
			pattern:   "*.block_number",
			tableName: "any_table",
			fieldName: "block_number",
			expected:  true,
		},
		{
			name:      "wildcard table *.field - wrong field",
			pattern:   "*.block_number",
			tableName: "any_table",
			fieldName: "slot",
			expected:  false,
		},

		// Wildcard field patterns
		{
			name:      "wildcard field table.*",
			pattern:   "fct_prepared_block.*",
			tableName: "fct_prepared_block",
			fieldName: "any_field",
			expected:  true,
		},
		{
			name:      "wildcard field table.* - wrong table",
			pattern:   "fct_prepared_block.*",
			tableName: "other_table",
			fieldName: "any_field",
			expected:  false,
		},

		// Full wildcard
		{
			name:      "full wildcard *.* matches all",
			pattern:   "*.*",
			tableName: "any_table",
			fieldName: "any_field",
			expected:  true,
		},

		// Field only (no table prefix)
		{
			name:      "field only matches any table",
			pattern:   "block_number",
			tableName: "any_table",
			fieldName: "block_number",
			expected:  true,
		},
		{
			name:      "field only - wrong field",
			pattern:   "block_number",
			tableName: "any_table",
			fieldName: "slot",
			expected:  false,
		},

		// Edge cases
		{
			name:      "empty pattern",
			pattern:   "",
			tableName: "any_table",
			fieldName: "any_field",
			expected:  false,
		},
		{
			name:      "pattern with multiple dots - invalid",
			pattern:   "db.table.field",
			tableName: "table",
			fieldName: "field",
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := matchesPattern(tt.pattern, tt.tableName, tt.fieldName)
			assert.Equal(t, tt.expected, result, "matchesPattern result mismatch")
		})
	}
}

func TestConversionConfig_MultiplePatterns(t *testing.T) {
	config := ConversionConfig{
		BigIntToString: map[string][]string{
			"fct_prepared_block":         {"consensus_payload_value", "execution_payload_value"},
			"fct_block_native_transfers": {"value", "gas_price"},
		},
		BigIntToStringFields: []string{
			"*.block_number",
			"*.slot",
			"fct_beacon_state.*",
			"*.*",
		},
	}

	testCases := []struct {
		table    string
		field    string
		expected bool
		reason   string
	}{
		// Table-scoped matches
		{"fct_prepared_block", "consensus_payload_value", true, "table-scoped exact"},
		{"fct_prepared_block", "execution_payload_value", true, "table-scoped exact"},
		{"fct_block_native_transfers", "value", true, "table-scoped exact"},
		{"fct_block_native_transfers", "gas_price", true, "table-scoped exact"},

		// CLI wildcard field matches
		{"any_table", "block_number", true, "*.block_number pattern"},
		{"another_table", "slot", true, "*.slot pattern"},

		// CLI wildcard table matches
		{"fct_beacon_state", "any_field", true, "fct_beacon_state.* pattern"},

		// Full wildcard matches everything
		{"completely_random", "random_field", true, "*.* pattern"},
	}

	for _, tc := range testCases {
		t.Run(tc.table+"."+tc.field, func(t *testing.T) {
			result := config.ShouldConvertToString(tc.table, tc.field)
			assert.Equal(t, tc.expected, result, "Expected %v for %s.%s (%s)", tc.expected, tc.table, tc.field, tc.reason)
		})
	}
}
