package logger

import (
	"bytes"
	"context"
	"log/slog"
	"strings"
	"testing"
)

func TestHandleLineContainsRequestID(t *testing.T) {
	var buf bytes.Buffer
	h := newTestHandler(&buf).WithGroup("main")
	ctx := WithRequestID(context.Background(), "req-42")
	record := slog.NewRecord(lineTime, slog.LevelInfo, "service started", 0)

	if err := h.Handle(ctx, record); err != nil {
		t.Fatal(err)
	}
	const want = "[INFO] 2026-10-08 14:33:12 (main)\t- Service started, {\"request_id\":\"req-42\"}\n"
	if buf.String() != want {
		t.Fatalf("got %q\nwant %q", buf.String(), want)
	}
}

func TestHandleLineWithoutRequestID(t *testing.T) {
	var buf bytes.Buffer
	h := newTestHandler(&buf)
	record := slog.NewRecord(lineTime, slog.LevelInfo, "service started", 0)

	if err := h.Handle(context.Background(), record); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), "request_id") {
		t.Fatalf("request id was added: %q", buf.String())
	}
}

func TestHandleKeepsExplicitRequestID(t *testing.T) {
	var buf bytes.Buffer
	h := newTestHandler(&buf)
	ctx := WithRequestID(context.Background(), "from-context")
	record := slog.NewRecord(lineTime, slog.LevelInfo, "ready", 0)
	record.AddAttrs(slog.String("request_id", "explicit"))

	if err := h.Handle(ctx, record); err != nil {
		t.Fatal(err)
	}
	got := buf.String()
	if !strings.Contains(got, `"request_id":"explicit"`) || strings.Contains(got, "from-context") {
		t.Fatalf("got %q", got)
	}
}

func TestLoggerInfoContextIncludesRequestID(t *testing.T) {
	var buf bytes.Buffer
	log := New(slog.LevelInfo, &buf)
	ctx := WithRequestID(context.Background(), "req-worker")

	log.InfoContext(ctx, "snapshot transition", "group", "worker")

	got := buf.String()
	if !strings.Contains(got, "req-worker") || !strings.Contains(got, "Snapshot transition") {
		t.Fatalf("got %q", got)
	}
	if RequestID(ctx) != "req-worker" {
		t.Fatalf("context id = %q", RequestID(ctx))
	}
}

func TestResolveRequestID(t *testing.T) {
	if got := ResolveRequestID("  abc-DEF_01.9  "); got != "abc-DEF_01.9" {
		t.Fatalf("safe id = %q", got)
	}
	if got := ResolveRequestID("id\n"); got != "id" {
		t.Fatalf("trimmed id = %q", got)
	}
	long := strings.Repeat("a", 128)
	if got := ResolveRequestID(long); got != long {
		t.Fatalf("128-char id was replaced")
	}

	for _, raw := range []string{"", "   ", "has space", "a/b", "id\nx", "ü", strings.Repeat("b", 129)} {
		got := ResolveRequestID(raw)
		if got == strings.TrimSpace(raw) || !validRequestID(got) {
			t.Fatalf("ResolveRequestID(%q) = %q", raw, got)
		}
	}

	first := ResolveRequestID("bad id")
	second := ResolveRequestID("bad id")
	if first == second {
		t.Fatalf("generated ids collided: %s", first)
	}
}
