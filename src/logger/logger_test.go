package logger

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

var lineTime = time.Date(2026, 10, 8, 14, 33, 12, 0, time.FixedZone("CEST", 2*60*60))

func TestHandleLine(t *testing.T) {
	tests := []struct {
		name    string
		level   slog.Level
		message string
		group   string
		attrs   []slog.Attr
		bound   []slog.Attr
		want    string
	}{
		{
			name:    "group attribute and fields",
			level:   slog.LevelInfo,
			message: "service started",
			attrs: []slog.Attr{
				slog.String("group", "main"),
				slog.Group("config",
					slog.String("HTTPAddress", ":8080"),
					slog.String("LogLevel", "INFO"),
				),
			},
			want: "[INFO] 2026-10-08 14:33:12 (main)\t- Service started, {\"HTTPAddress\":\":8080\",\"LogLevel\":\"INFO\"}\n",
		},
		{
			name:    "group attribute only",
			level:   slog.LevelInfo,
			message: "service started",
			attrs:   []slog.Attr{slog.String("group", "main")},
			want:    "[INFO] 2026-10-08 14:33:12 (main)\t- Service started\n",
		},
		{
			name:    "with group",
			level:   slog.LevelInfo,
			message: "service started",
			group:   "MAIN",
			want:    "[INFO] 2026-10-08 14:33:12 (MAIN)\t- Service started\n",
		},
		{
			name:    "named struct",
			level:   slog.LevelInfo,
			message: "http server started",
			group:   "main:run",
			attrs: []slog.Attr{
				slog.String("address", ":3000"),
				slog.Any("config", struct {
					LogLevel        slog.Level
					HTTPAddress     string
					ShutdownTimeout time.Duration
					WorkerCount     int
					WorkerAttempts  int
					WorkerQueueSize int
				}{
					LogLevel:        slog.LevelDebug,
					HTTPAddress:     ":3000",
					ShutdownTimeout: 24 * time.Second,
					WorkerCount:     3,
					WorkerAttempts:  3,
					WorkerQueueSize: 128,
				}),
			},
			want: "[INFO] 2026-10-08 14:33:12 (main:run)\t- Http server started, {\"address\":\":3000\",\"config\":{\"LogLevel\":\"DEBUG\",\"HTTPAddress\":\":3000\",\"ShutdownTimeout\":24,\"WorkerCount\":3,\"WorkerAttempts\":3,\"WorkerQueueSize\":128}}\n",
		},
		{
			name:    "with group and struct",
			level:   slog.LevelInfo,
			message: "service started",
			group:   "MAIN",
			attrs: []slog.Attr{slog.Any("!BADKEY", struct {
				HTTPAddress string
				LogLevel    slog.Level
			}{HTTPAddress: ":8080", LogLevel: slog.LevelInfo})},
			want: "[INFO] 2026-10-08 14:33:12 (MAIN)\t- Service started, {\"HTTPAddress\":\":8080\",\"LogLevel\":\"INFO\"}\n",
		},
		{
			name:    "no group",
			level:   slog.LevelInfo,
			message: "service started",
			want:    "[INFO] 2026-10-08 14:33:12  \t\t- Service started\n",
		},
		{
			name:    "level and capitalization",
			level:   slog.LevelWarn,
			message: "disk full",
			group:   "storage",
			want:    "[WARN] 2026-10-08 14:33:12 (storage)\t- Disk full\n",
		},
		{
			name:    "bound attributes",
			level:   slog.LevelInfo,
			message: "ready",
			group:   "api",
			bound:   []slog.Attr{slog.Int("attempt", 2)},
			attrs:   []slog.Attr{slog.String("host", "localhost")},
			want:    "[INFO] 2026-10-08 14:33:12 (api)\t- Ready, {\"attempt\":2,\"host\":\"localhost\"}\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			h := newTestHandler(&buf)
			if tt.group != "" {
				h = h.WithGroup(tt.group)
			}
			if len(tt.bound) > 0 {
				h = h.WithAttrs(tt.bound)
			}
			record := slog.NewRecord(lineTime, tt.level, tt.message, 0)
			record.AddAttrs(tt.attrs...)

			if err := h.Handle(context.Background(), record); err != nil {
				t.Fatal(err)
			}
			if buf.String() != tt.want {
				t.Fatalf("got %q\nwant %q", buf.String(), tt.want)
			}
		})
	}
}

func TestWithGroupEmptyName(t *testing.T) {
	var buf bytes.Buffer
	h := newTestHandler(&buf).WithGroup("")
	record := slog.NewRecord(lineTime, slog.LevelInfo, "service started", 0)

	if err := h.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}

	const want = "[INFO] 2026-10-08 14:33:12  \t\t- Service started\n"
	if buf.String() != want {
		t.Fatalf("got %q\nwant %q", buf.String(), want)
	}
}

func TestNewFiltersLevelAndGroups(t *testing.T) {
	var buf bytes.Buffer
	log := New(slog.LevelWarn, &buf)
	log.WithGroup("MAIN").Info("service started")
	log.WithGroup("MAIN").Warn("disk full")

	got := buf.String()
	if strings.Contains(got, "Service started") {
		t.Fatalf("info record was written: %q", got)
	}
	if !strings.HasPrefix(got, "[WARN] ") || !strings.HasSuffix(got, " (MAIN)\t- Disk full\n") {
		t.Fatalf("got %q", got)
	}
}

func newTestHandler(w *bytes.Buffer) slog.Handler {
	return contextHandler{Handler: &lineHandler{mu: &sync.Mutex{}, w: w, level: slog.LevelDebug}}
}
