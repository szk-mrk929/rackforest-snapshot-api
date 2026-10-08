package main

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const maxNameLen = 256

// record is one user or one store in log.json.
type record struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

// logFile is the whole log. Both arrays are always written, even when empty,
// so creating a user cannot drop the stores, and the other way around.
type logFile struct {
	Users  []record `json:"users"`
	Stores []record `json:"stores"`
}

func emptyLog() logFile {
	return logFile{Users: []record{}, Stores: []record{}}
}

func normalize(data logFile) logFile {
	if data.Users == nil {
		data.Users = []record{}
	}
	if data.Stores == nil {
		data.Stores = []record{}
	}
	return data
}

func load(path string) (logFile, error) {
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return emptyLog(), nil
	}
	if err != nil {
		return logFile{}, fmt.Errorf("read %s: %w", path, err)
	}
	var data logFile
	if err := json.Unmarshal(raw, &data); err != nil {
		return logFile{}, fmt.Errorf("parse %s: %w", path, err)
	}
	return normalize(data), nil
}

// save replaces path atomically: the new body is written to a temp file in the
// same directory, synced, then renamed over the destination. A crash before
// the rename leaves the previous log.json intact.
func save(path string, data logFile) error {
	data = normalize(data)
	dir := filepath.Dir(path)
	if dir == "" {
		dir = "."
	}
	tmp, err := os.CreateTemp(dir, ".log.json-*")
	if err != nil {
		return fmt.Errorf("create temp log: %w", err)
	}
	tmpName := tmp.Name()
	closed := false
	keep := false
	defer func() {
		if !closed {
			_ = tmp.Close()
		}
		if !keep {
			_ = os.Remove(tmpName)
		}
	}()

	enc := json.NewEncoder(tmp)
	enc.SetEscapeHTML(false)
	enc.SetIndent("", "  ")
	if err := enc.Encode(data); err != nil {
		return fmt.Errorf("encode log: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync log: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close log: %w", err)
	}
	closed = true
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("replace %s: %w", path, err)
	}
	keep = true
	return nil
}

// add appends one user or store. kind is "user" or "store".
// A duplicate name returns an error and does not write.
func add(path, kind, name string, now time.Time) (record, error) {
	name = strings.TrimSpace(name)
	if err := validateName(name); err != nil {
		return record{}, err
	}
	data, err := load(path)
	if err != nil {
		return record{}, err
	}
	list := listFor(data, kind)
	if list == nil {
		return record{}, fmt.Errorf("unknown kind %q", kind)
	}
	for _, item := range list {
		if item.Name == name {
			return record{}, fmt.Errorf("%s %q already exists", kind, name)
		}
	}
	id, err := newID()
	if err != nil {
		return record{}, err
	}
	rec := record{
		ID:        id,
		Name:      name,
		CreatedAt: now.UTC().Truncate(time.Second),
	}
	switch kind {
	case "user":
		data.Users = append(data.Users, rec)
	case "store":
		data.Stores = append(data.Stores, rec)
	}
	if err := save(path, data); err != nil {
		return record{}, err
	}
	return rec, nil
}

func listFor(data logFile, kind string) []record {
	switch kind {
	case "user":
		return data.Users
	case "store":
		return data.Stores
	default:
		return nil
	}
}

func validateName(name string) error {
	if name == "" {
		return errors.New("name is required")
	}
	if utf8.RuneCountInString(name) > maxNameLen {
		return fmt.Errorf("name must be at most %d characters", maxNameLen)
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return errors.New("name contains a control character")
		}
	}
	return nil
}

func newID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("generate id: %w", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

func formatLog(data logFile) string {
	var b strings.Builder
	writeSection(&b, "users", data.Users)
	writeSection(&b, "stores", data.Stores)
	return b.String()
}

func writeSection(b *strings.Builder, title string, items []record) {
	fmt.Fprintf(b, "%s:\n", title)
	if len(items) == 0 {
		fmt.Fprintf(b, "  (none)\n")
		return
	}
	for _, item := range items {
		fmt.Fprintf(b, "  %s  %s  %s\n", item.ID, item.Name, item.CreatedAt.UTC().Format(time.RFC3339))
	}
}
