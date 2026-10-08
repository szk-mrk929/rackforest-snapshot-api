// Package logger builds the process logger.
// The handler copies the request id from the context onto every record, so
// call sites can pass the request context and get correlation for free.
//
// A record is written as one line:
//
//	[INFO] 2026-10-08 14:33:12 (main)\t- Service started, {"request_id":"req-42"}
//
// WithGroup, or an attr named "group", is the parenthesized segment. The
// message's first letter is capitalized. Every other attr is JSON after the
// message. slog groups and a bare struct argument are flattened into that
// object. A struct passed with a key is nested under that key. Durations are
// emitted in seconds. With no extra attrs the line ends at the message.
package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"time"

	"rackforest-snapshot-api/src/utils/logattrs"
	"rackforest-snapshot-api/src/utils/strutil"
)

type contextHandler struct {
	slog.Handler
}

type lineHandler struct {
	mu    *sync.Mutex
	w     io.Writer
	level slog.Level
	group string
	attrs []slog.Attr
}

// New returns a logger that writes the line format described above.
func New(level slog.Level, w io.Writer) *slog.Logger {
	return slog.New(contextHandler{Handler: &lineHandler{mu: &sync.Mutex{}, w: w, level: level}})
}

func (h contextHandler) Handle(ctx context.Context, record slog.Record) error {
	if id := RequestID(ctx); id != "" && !logattrs.RecordHasKey(record, "request_id") {
		record.AddAttrs(slog.String("request_id", id))
	}
	return h.Handler.Handle(ctx, record)
}

func (h contextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return contextHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h contextHandler) WithGroup(name string) slog.Handler {
	return contextHandler{Handler: h.Handler.WithGroup(name)}
}

func (h *lineHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *lineHandler) Handle(_ context.Context, record slog.Record) error {
	group := h.group
	fields := map[string]any{}
	for _, attr := range h.attrs {
		logattrs.Collect(&group, fields, attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		logattrs.Collect(&group, fields, attr)
		return true
	})

	var buf bytes.Buffer
	buf.WriteByte('[')
	buf.WriteString(record.Level.String())
	buf.WriteByte(']')
	buf.WriteByte(' ')
	buf.WriteString(record.Time.Format(time.DateTime))
	buf.WriteByte(' ')
	if group != "" {
		buf.WriteByte('(')
		buf.WriteString(group)
		buf.WriteByte(')')
	} else {
		buf.WriteString(" \t")
	}
	buf.WriteString("\t- ")
	buf.WriteString(strutil.CapitalizeFirst(record.Message))
	if len(fields) > 0 {
		raw, err := json.Marshal(fields)
		if err != nil {
			return err
		}
		buf.WriteString(", ")
		buf.Write(raw)
	}
	buf.WriteByte('\n')

	h.mu.Lock()
	defer h.mu.Unlock()
	_, err := h.w.Write(buf.Bytes())
	return err
}

func (h *lineHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}
	next := *h
	next.attrs = make([]slog.Attr, len(h.attrs)+len(attrs))
	copy(next.attrs, h.attrs)
	copy(next.attrs[len(h.attrs):], attrs)
	return &next
}

func (h *lineHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	next := *h
	next.group = name
	return &next
}
