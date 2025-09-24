# clickhouse-proto-gen

A standalone CLI tool that connects to a ClickHouse cluster, introspects one or more tables, and generates corresponding Protocol Buffer (.proto) schema files.

## Features

- 🔍 **Auto-discovery**: Connects to ClickHouse and automatically extracts table schemas
- 📋 **Type mapping**: Intelligently maps ClickHouse types to appropriate Proto3 types
- 📝 **Comments preservation**: Optionally includes table and column comments in generated proto files
- 🎯 **Selective generation**: Generate proto for specific tables or all tables in a database
- ⚙️ **Configurable**: Supports both CLI flags and YAML configuration files
- 📦 **Organized Output**: Generates separate proto files for each table for better organization

## Installation

### Using Docker

```bash
docker pull ethpandaops/clickhouse-proto-gen:latest
```

### Using Go

```bash
go install github.com/ethpandaops/clickhouse-proto-gen/cmd/clickhouse-proto-gen@latest
```

Or build from source:

```bash
git clone https://github.com/ethpandaops/clickhouse-proto-gen
cd clickhouse-proto-gen
go build -o clickhouse-proto-gen ./cmd/clickhouse-proto-gen
```

## Quick Start

### Using Docker

Mount the current directory as a volume to generate proto files directly into your project:

```bash
# Basic usage - generate proto files in ./proto directory
docker run --rm -v "$(pwd):/workspace" \
  ethpandaops/clickhouse-proto-gen \
  --dsn "clickhouse://user:pass@host.docker.internal:9000/mydb" \
  --tables users,orders \
  --out /workspace/proto \
  --package myapp.v1 \
  --go-package github.com/myorg/myapp/gen/v1

# Generate for multiple tables with comments
docker run --rm -v "$(pwd):/workspace" \
  ethpandaops/clickhouse-proto-gen \
  --dsn "clickhouse://user:pass@host.docker.internal:9000/mydb" \
  --tables users,products,orders,inventory \
  --out /workspace/proto \
  --package ecommerce.v1 \
  --go-package github.com/mycompany/ecommerce/pb/v1 \
  --include-comments

# Connect to ClickHouse on custom network
docker run --rm -v "$(pwd):/workspace" --network=my_network \
  ethpandaops/clickhouse-proto-gen \
  --dsn "clickhouse://user:pass@clickhouse:9000/production" \
  --tables events,metrics \
  --out /workspace/generated/proto \
  --verbose
```

**Note for Docker users:**
- Use `host.docker.internal` instead of `localhost` to connect to ClickHouse running on your host machine
- Mount your desired output directory to `/workspace` or another path inside the container
- All output paths should be relative to the mounted volume path
- Add `--network` flag if ClickHouse is running in a Docker network

### Basic usage with CLI flags

```bash
# Generate proto for specific tables
clickhouse-proto-gen \
  --dsn "clickhouse://user:pass@localhost:9000/mydb" \
  --tables users,orders \
  --out ./proto \
  --package myapp.v1 \
  --go-package github.com/myorg/myapp/gen/v1

# Generate proto for specific database tables
clickhouse-proto-gen \
  --dsn "clickhouse://user:pass@localhost:9000/mydb" \
  --tables table1,table2,table3 \
  --out ./proto
```

### Using a configuration file

```bash
clickhouse-proto-gen --config config.yaml
```

## Configuration

### YAML Configuration File

Create a `config.yaml` file:

```yaml
# ClickHouse connection string
dsn: clickhouse://user:password@localhost:9000/mydb

# Tables to generate proto for (required)
tables:
  - users
  - orders
  - products

# Output directory for generated files
output_dir: ./proto

# Proto package name
package: myapp.clickhouse.v1

# Go package import path
go_package: github.com/myorg/myapp/gen/clickhousev1

# Include ClickHouse comments in proto files
include_comments: true

# Maximum page size for List operations (default: 10000)
max_page_size: 10000
```

See [config.example.yaml](config.example.yaml) for a complete example with all available options.

### CLI Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--dsn` | ClickHouse DSN | Required |
| `--tables` | Comma-separated list of tables | Required |
| `--out` | Output directory | `./proto` |
| `--package` | Proto package name | `clickhouse.v1` |
| `--go-package` | Go package import path | - |
| `--include-comments` | Include comments in proto | true |
| `--max-page-size` | Maximum page size for List operations | 10000 |
| `--config` | Path to YAML config file | - |
| `--verbose` | Enable verbose output | false |
| `--debug` | Enable debug output | false |

## Type Mapping

### Default Mappings

| ClickHouse Type | Proto Type | Notes |
|-----------------|------------|-------|
| `Int8`, `Int16`, `Int32` | `int32` | |
| `Int64` | `int64` | |
| `Int128`, `Int256` | `string` | No native support in protobuf |
| `UInt8`, `UInt16`, `UInt32` | `uint32` | |
| `UInt64` | `uint64` | |
| `Float32` | `float` | |
| `Float64` | `double` | |
| `Decimal*` | `string` | Preserves precision |
| `String`, `FixedString` | `string` | |
| `Date`, `DateTime` | `string` | ISO 8601 format |
| `Bool` | `bool` | |
| `UUID` | `string` | |
| `Array(T)` | `repeated T` | |
| `Nullable(T)` | Uses nullable filter types | Special handling for filtering |
| `LowCardinality(T)` | `T` | Unwraps to base type |
| `Map` | `string` | JSON representation |
| `Tuple` | `string` | JSON representation |
| `Enum8`, `Enum16` | `string` | Enum value as string |
| `IPv4`, `IPv6` | `string` | IP address as string |

## Examples

### Example 1: Generate proto for specific tables

```bash
clickhouse-proto-gen \
  --dsn "clickhouse://localhost:9000/default" \
  --tables "users,products,orders" \
  --out ./proto \
  --package ecommerce.v1
```

This generates:
- `./proto/users.proto`
- `./proto/products.proto`
- `./proto/orders.proto`

### Example 2: Cross-database generation

```bash
clickhouse-proto-gen \
  --dsn "clickhouse://localhost:9000" \
  --tables "db1.users,db2.products,db3.orders" \
  --out ./proto
```

### Example Output

For a ClickHouse table:

```sql
CREATE TABLE users (
    id UInt64,
    email String,
    name Nullable(String),
    created_at DateTime,
    tags Array(String)
) ENGINE = MergeTree()
ORDER BY id
COMMENT = 'User accounts table';
```

Generates proto:

```protobuf
syntax = "proto3";

package myapp.v1;

option go_package = "github.com/myorg/myapp/gen/v1";

// User accounts table
message Users {
  uint64 id = 11;
  string email = 12;
  optional string name = 13;
  string created_at = 14;
  repeated string tags = 15;
}
```

## Development

### Building

```bash
make build
```

### Running tests

```bash
make test
```

### Linting

```bash
make lint
```

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - see [LICENSE](LICENSE) file for details

## Support

For issues and feature requests, please use the [GitHub issue tracker](https://github.com/ethpandaops/clickhouse-proto-gen/issues).