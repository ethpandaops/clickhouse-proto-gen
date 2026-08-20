package protogen

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethpandaops/clickhouse-proto-gen/internal/clickhouse"
	"github.com/ethpandaops/clickhouse-proto-gen/internal/config"
	"github.com/sirupsen/logrus"
)

func newRowTestGenerator(t *testing.T) *Generator {
	t.Helper()

	cfg := &config.Config{
		OutputDir: t.TempDir(),
		GoPackage: "github.com/test/proto/clickhouse",
		Conversion: config.ConversionConfig{
			BigIntToString: map[string][]string{
				"events": {"big_number"},
			},
		},
	}

	return NewGenerator(cfg, logrus.New())
}

func rowTestTable() *clickhouse.Table {
	cols := []clickhouse.Column{
		{Name: "updated_date_time", Type: "DateTime", BaseType: "DateTime", Position: 1},
		{Name: "slot", Type: "UInt32", BaseType: "UInt32", Position: 2},
		{Name: "block", Type: "FixedString(66)", BaseType: "FixedString", Position: 3},
		{Name: "name", Type: "Nullable(String)", BaseType: "String", IsNullable: true, Position: 4},
		{Name: "meta_client_ip", Type: "Nullable(IPv6)", BaseType: "IPv6", IsNullable: true, Position: 5},
		{Name: "groups", Type: "Array(String)", BaseType: "String", IsArray: true, Position: 6},
		{Name: "labels", Type: "Map(String, String)", BaseType: "Map", Position: 7},
		{Name: "big_number", Type: "UInt64", BaseType: "UInt64", Position: 8},
		{Name: "seen_at", Type: "Nullable(DateTime)", BaseType: "DateTime", IsNullable: true, Position: 9},
		{Name: "small", Type: "Int16", BaseType: "Int16", Position: 10},
		{Name: "counts", Type: "Array(Nullable(UInt64))", BaseType: "UInt64", IsArray: true, Position: 11},
		{Name: "event_id", Type: "UUID", BaseType: "UUID", Position: 12},
		{Name: "kind", Type: "LowCardinality(String)", BaseType: "String", Position: 13},
		{Name: "score", Type: "Nullable(Float64)", BaseType: "Float64", IsNullable: true, Position: 14},
	}

	return &clickhouse.Table{
		Name:       "events",
		Database:   "default",
		Columns:    cols,
		SortingKey: []string{"slot"},
	}
}

func TestGenerateRowHelper(t *testing.T) {
	g := newRowTestGenerator(t)
	table := rowTestTable()

	if err := g.GenerateRowHelpers([]*clickhouse.Table{table}); err != nil {
		t.Fatalf("GenerateRowHelpers: %v", err)
	}

	data, err := os.ReadFile(filepath.Join(g.config.OutputDir, "events.row.go"))
	if err != nil {
		t.Fatalf("read generated file: %v", err)
	}

	content := string(data)

	expectations := []string{
		"type EventsRow struct {",
		"UpdatedDateTime uint32 `ch:\"updated_date_time\" json:\"updated_date_time\"`",
		"Slot uint32 `ch:\"slot\" json:\"slot\"`",
		// FixedString results are nullable via NULLIF even when the column is not.
		"Block *string `ch:\"block\" json:\"block\"`",
		"Name *string `ch:\"name\" json:\"name\"`",
		"MetaClientIp *net.IP `ch:\"meta_client_ip\" json:\"meta_client_ip\"`",
		"Groups []string `ch:\"groups\" json:\"groups\"`",
		"Labels map[string]string `ch:\"labels\" json:\"labels\"`",
		// Configured for string conversion.
		"BigNumber string `ch:\"big_number\" json:\"big_number\"`",
		"SeenAt *uint32 `ch:\"seen_at\" json:\"seen_at\"`",
		"Small int16 `ch:\"small\" json:\"small\"`",
		"Counts []uint64 `ch:\"counts\" json:\"counts\"`",
		"EventId uuid.UUID `ch:\"event_id\" json:\"event_id\"`",
		"Kind string `ch:\"kind\" json:\"kind\"`",
		"Score *float64 `ch:\"score\" json:\"score\"`",
		"func (EventsRow) TableName() string {",
		"func (r *EventsRow) ToProto() *Events {",
		// Nullable proto fields are wrapped.
		"p.Name = wrapperspb.String(*r.Name)",
		"p.MetaClientIp = wrapperspb.String(r.MetaClientIp.String())",
		"p.SeenAt = wrapperspb.UInt32(*r.SeenAt)",
		"p.Score = wrapperspb.Double(*r.Score)",
		// Non-nullable proto field from nullable scan target dereferences.
		"if r.Block != nil {",
		"p.Block = *r.Block",
		// Casts and string conversions.
		"p.Small = int32(r.Small)",
		"p.EventId = r.EventId.String()",
	}

	for _, want := range expectations {
		if !strings.Contains(content, want) {
			t.Errorf("generated file missing %q", want)
		}
	}
}

func TestGenerateRowHelperSkipsTablesWithoutSortingKey(t *testing.T) {
	g := newRowTestGenerator(t)
	table := rowTestTable()
	table.SortingKey = nil

	if err := g.GenerateRowHelpers([]*clickhouse.Table{table}); err != nil {
		t.Fatalf("GenerateRowHelpers: %v", err)
	}

	if _, err := os.Stat(filepath.Join(g.config.OutputDir, "events.row.go")); !os.IsNotExist(err) {
		t.Fatalf("expected no row file for table without sorting key")
	}
}

func TestGoCamelCase(t *testing.T) {
	cases := map[string]string{
		"updated_date_time": "UpdatedDateTime",
		"chunked_50ms":      "Chunked_50Ms",
		"slot":              "Slot",
		"_private":          "XPrivate",
		"meta_client_ip":    "MetaClientIp",
	}

	for in, want := range cases {
		if got := goCamelCase(in); got != want {
			t.Errorf("goCamelCase(%q) = %q, want %q", in, got, want)
		}
	}
}
