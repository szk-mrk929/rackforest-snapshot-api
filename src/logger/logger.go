// Package logger builds the process logger.
// The handler copies the request id from the context onto every record, so
// call sites can pass the request context and get correlation for free.
//
// A record is written as one line:
//
//	[INFO][MAIN] 2026-10-08T15:11:09.078985+02:00: Service started, {"HTTPAddress":":8080","LogLevel":"INFO"}
//
// WithGroup, or an attr named "group", is the second bracket. The message's
// first letter is capitalized. Every other attr is JSON after the message.
// slog groups and a bare struct argument are flattened into that object. A
// struct passed with a key is nested under that key. Durations are seconds.
// With no other attrs the line ends at the message.
package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"
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
		collect(&group, fields, attr)
	}
	record.Attrs(func(attr slog.Attr) bool {
		collect(&group, fields, attr)
		return true
	})

	var buf bytes.Buffer
	buf.WriteByte('[')
	buf.WriteString(record.Level.String()) // buf.WriteString(strings.ToLower(record.Level.String()))
	buf.WriteByte(']')
	buf.WriteByte(' ')
	buf.WriteString(record.Time.Format(time.DateTime)) // buf.WriteString(record.Time.Format(time.RFC3339Nano))
	buf.WriteByte(' ')
	if group != "" {
		buf.WriteByte('(')
		buf.WriteString(group)
		buf.WriteByte(')')
	} else {
		buf.WriteString(" \t")
	}
	buf.WriteString("\t- ")
	buf.WriteString(capitalize(record.Message))
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

func capitalize(message string) string {
	r, size := utf8.DecodeRuneInString(message)
	if r == utf8.RuneError {
		return message
	}
	return string(unicode.ToUpper(r)) + message[size:]
}

func collect(group *string, fields map[string]any, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	if attr.Value.Kind() == slog.KindGroup {
		for _, child := range attr.Value.Group() {
			collect(group, fields, child)
		}
		return
	}
	if attr.Key == "group" && attr.Value.Kind() == slog.KindString {
		*group = attr.Value.String()
		return
	}
	// A bare value, such as Info(msg, conf), is stored under "!BADKEY".
	// Expand a struct into the JSON object instead of printing that key.
	if attr.Key == "!BADKEY" && mergeStruct(fields, attr.Value) {
		return
	}
	fields[attr.Key] = jsonValue(attr.Value)
}

func mergeStruct(fields map[string]any, value slog.Value) bool {
	if value.Kind() != slog.KindAny {
		return false
	}
	obj, ok := structObject(value.Any())
	if !ok {
		return false
	}
	for _, field := range obj.fields {
		fields[field.key] = field.val
	}
	return true
}

func jsonFieldName(sf reflect.StructField) (string, bool) {
	tag := sf.Tag.Get("json")
	if tag == "-" {
		return "", false
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return sf.Name, true
	}
	return name, true
}

func jsonValue(value slog.Value) any {
	switch value.Kind() {
	case slog.KindString:
		return value.String()
	case slog.KindInt64:
		return value.Int64()
	case slog.KindUint64:
		return value.Uint64()
	case slog.KindFloat64:
		return value.Float64()
	case slog.KindBool:
		return value.Bool()
	case slog.KindDuration:
		return value.Duration().Seconds()
	case slog.KindTime:
		return value.Time().Format(time.RFC3339Nano)
	case slog.KindAny:
		v := value.Any()
		if level, ok := v.(slog.Level); ok {
			return level.String()
		}
		if _, ok := v.(error); ok {
			return fmt.Sprint(v)
		}
		if obj, ok := structObject(v); ok {
			return obj
		}
		return fmt.Sprint(v)
	default:
		return value.String()
	}
}

type jsonField struct {
	key string
	val any
}

type jsonObject struct {
	fields []jsonField
}

func (o jsonObject) MarshalJSON() ([]byte, error) {
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, field := range o.fields {
		if i > 0 {
			buf.WriteByte(',')
		}
		key, err := json.Marshal(field.key)
		if err != nil {
			return nil, err
		}
		val, err := json.Marshal(field.val)
		if err != nil {
			return nil, err
		}
		buf.Write(key)
		buf.WriteByte(':')
		buf.Write(val)
	}
	buf.WriteByte('}')
	return buf.Bytes(), nil
}

func structObject(v any) (jsonObject, bool) {
	if v == nil {
		return jsonObject{}, false
	}
	rv := reflect.ValueOf(v)
	if !rv.IsValid() {
		return jsonObject{}, false
	}
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return jsonObject{}, false
		}
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct || rv.Type() == reflect.TypeOf(time.Time{}) {
		return jsonObject{}, false
	}
	fields := make([]jsonField, 0, rv.NumField())
	rt := rv.Type()
	for i := 0; i < rv.NumField(); i++ {
		sf := rt.Field(i)
		if sf.PkgPath != "" {
			continue
		}
		name, ok := jsonFieldName(sf)
		if !ok {
			continue
		}
		fv := rv.Field(i)
		if !fv.CanInterface() {
			continue
		}
		fields = append(fields, jsonField{
			key: name,
			val: jsonValue(slog.AnyValue(fv.Interface())),
		})
	}
	return jsonObject{fields: fields}, true
}
