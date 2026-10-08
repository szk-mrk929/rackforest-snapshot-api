// Package logattrs normalizes slog attributes for custom line formatters.
package logattrs

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"
)

// RecordHasKey reports whether record contains key.
func RecordHasKey(record slog.Record, key string) bool {
	found := false
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == key {
			found = true
			return false
		}
		return true
	})
	return found
}

// Collect updates group and fields with one slog attribute.
// Groups are flattened into fields, and a "group" string attr overrides group.
func Collect(group *string, fields map[string]any, attr slog.Attr) {
	attr.Value = attr.Value.Resolve()
	if attr.Equal(slog.Attr{}) {
		return
	}
	if attr.Value.Kind() == slog.KindGroup {
		for _, child := range attr.Value.Group() {
			Collect(group, fields, child)
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
