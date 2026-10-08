package logattrs

import (
	"encoding/json"
	"errors"
	"log/slog"
	"testing"
	"time"
)

func TestRecordHasKey(t *testing.T) {
	record := slog.NewRecord(time.Now(), slog.LevelInfo, "msg", 0)
	record.AddAttrs(slog.String("a", "1"), slog.Int("b", 2))
	if !RecordHasKey(record, "a") {
		t.Fatal("missing key a")
	}
	if RecordHasKey(record, "missing") {
		t.Fatal("unexpected key match")
	}
}

func TestCollectGroupAndFlattening(t *testing.T) {
	group := ""
	fields := map[string]any{}

	Collect(&group, fields, slog.String("group", "worker"))
	Collect(&group, fields, slog.Group("cfg",
		slog.String("addr", ":8080"),
		slog.Int("workers", 3),
	))
	Collect(&group, fields, slog.Any("!BADKEY", struct {
		Level string
		Skip  string `json:"-"`
	}{
		Level: "debug",
		Skip:  "x",
	}))

	if group != "worker" {
		t.Fatalf("group = %q", group)
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"Level":"debug","addr":":8080","workers":3}`
	if string(raw) != want {
		t.Fatalf("fields = %s\nwant   = %s", raw, want)
	}
}

func TestCollectPreservesAnySemantics(t *testing.T) {
	group := ""
	fields := map[string]any{}
	tm := time.Date(2026, 10, 8, 14, 33, 12, 123, time.UTC)

	Collect(&group, fields, slog.Duration("retry", 2*time.Second))
	Collect(&group, fields, slog.Time("at", tm))
	Collect(&group, fields, slog.Any("level", slog.LevelWarn))
	Collect(&group, fields, slog.Any("error", errors.New("failed")))

	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"at":"2026-10-08T14:33:12.000000123Z","error":"failed","level":"WARN","retry":2}`
	if string(raw) != want {
		t.Fatalf("fields = %s\nwant   = %s", raw, want)
	}
}

func TestCollectKeepsNamedStructNested(t *testing.T) {
	group := ""
	fields := map[string]any{}
	Collect(&group, fields, slog.Any("config", struct {
		HTTPAddress string
		Workers     int
	}{
		HTTPAddress: ":3000",
		Workers:     3,
	}))

	raw, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	const want = `{"config":{"HTTPAddress":":3000","Workers":3}}`
	if string(raw) != want {
		t.Fatalf("fields = %s\nwant   = %s", raw, want)
	}
}
