package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestAddUserAndStoreWritesLog(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log.json")

	var out bytes.Buffer
	if err := run([]string{"--file", path, "user", "add", "--name", "Ada"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "created user ") || !strings.Contains(out.String(), "Ada") {
		t.Fatalf("user create output = %q", out.String())
	}

	out.Reset()
	if err := run([]string{"--file", path, "store", "add", "--name", "Central"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "created store ") || !strings.Contains(out.String(), "Central") {
		t.Fatalf("store create output = %q", out.String())
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got logFile
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Users) != 1 || got.Users[0].Name != "Ada" {
		t.Fatalf("users = %+v", got.Users)
	}
	if len(got.Stores) != 1 || got.Stores[0].Name != "Central" {
		t.Fatalf("stores = %+v", got.Stores)
	}
	if got.Users[0].ID == "" || got.Stores[0].ID == "" || got.Users[0].ID == got.Stores[0].ID {
		t.Fatalf("ids = %q %q", got.Users[0].ID, got.Stores[0].ID)
	}
	assertUTCStamp(t, got.Users[0].CreatedAt)
	assertUTCStamp(t, got.Stores[0].CreatedAt)
	if !bytes.Contains(raw, []byte(`"users"`)) || !bytes.Contains(raw, []byte(`"stores"`)) {
		t.Fatalf("log.json = %s", raw)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != "log.json" {
		t.Fatalf("temp files left behind: %v", names(entries))
	}

	out.Reset()
	if err := run([]string{"--file", path, "list"}, &out, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	list := out.String()
	if !strings.Contains(list, got.Users[0].ID) || !strings.Contains(list, "Ada") || !strings.Contains(list, "Central") {
		t.Fatalf("list = %q", list)
	}

	before := append([]byte(nil), raw...)
	err = run([]string{"--file", path, "user", "add", "--name", "Ada"}, &bytes.Buffer{}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), `user "Ada" already exists`) {
		t.Fatalf("duplicate err = %v", err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("duplicate add changed log.json")
	}

	if err := run([]string{"--file", path, "user", "add", "--name", "Grace"}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	raw, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	got = logFile{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Users) != 2 || got.Users[0].Name != "Ada" || got.Users[1].Name != "Grace" {
		t.Fatalf("users after append = %+v", got.Users)
	}
	if len(got.Stores) != 1 || got.Stores[0].Name != "Central" {
		t.Fatalf("stores after user append = %+v", got.Stores)
	}
}

func assertUTCStamp(t *testing.T, ts time.Time) {
	t.Helper()
	if ts.IsZero() || ts.Location() != time.UTC {
		t.Fatalf("created_at = %s loc %v", ts, ts.Location())
	}
	if d := time.Since(ts); d < 0 || d > time.Minute {
		t.Fatalf("created_at not recent: %s", ts)
	}
	if ts.Format(time.RFC3339) != ts.UTC().Format(time.RFC3339) {
		t.Fatalf("created_at is not UTC: %s", ts)
	}
}

func names(entries []os.DirEntry) []string {
	out := make([]string, len(entries))
	for i, entry := range entries {
		out[i] = entry.Name()
	}
	return out
}
