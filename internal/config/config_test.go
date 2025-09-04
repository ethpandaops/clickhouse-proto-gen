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
		name            string
		initial         Config
		dsn             string
		outputDir       string
		pkg             string
		goPkg           string
		tables          string
		includeComments bool
		expected        Config
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
			dsn:             "clickhouse://new:9000/newdb",
			outputDir:       "./new",
			pkg:             "new.v1",
			goPkg:           "github.com/new/proto",
			tables:          "table1,table2,table3",
			includeComments: true,
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
			dsn:             "", // Keep old
			outputDir:       "./partial",
			pkg:             "", // Keep old
			goPkg:           "github.com/partial/proto",
			tables:          "", // Keep old
			includeComments: false,
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
			dsn:             "",
			outputDir:       "",
			pkg:             "",
			goPkg:           "",
			tables:          " table1 , table2 , table3 ",
			includeComments: true,
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
			dsn:             "",
			outputDir:       "",
			pkg:             "",
			goPkg:           "",
			tables:          "",
			includeComments: true,
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
			dsn:             "",
			outputDir:       "",
			pkg:             "",
			goPkg:           "",
			tables:          "db1.users, db2.orders, db3.products",
			includeComments: false,
			expected: Config{
				Tables:          []string{"db1.users", "db2.orders", "db3.products"},
				IncludeComments: false,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.initial
			cfg.MergeFlags(tt.dsn, tt.outputDir, tt.pkg, tt.goPkg, tt.tables, tt.includeComments)
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
