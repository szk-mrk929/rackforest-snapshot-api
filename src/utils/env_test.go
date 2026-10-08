package env

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	body := "" +
		"# comment\n" +
		"export RACKFOREST_TEST_ADDR=:3080\n" +
		"RACKFOREST_TEST_LEVEL=\"warn\"\n" +
		"RACKFOREST_TEST_NOTE=hello # trailing\n" +
		"RACKFOREST_TEST_EXISTING=override\n"
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RACKFOREST_TEST_EXISTING", "keep")
	t.Cleanup(func() {
		os.Unsetenv("RACKFOREST_TEST_ADDR")
		os.Unsetenv("RACKFOREST_TEST_LEVEL")
		os.Unsetenv("RACKFOREST_TEST_NOTE")
	})

	applyEnvFile(path)

	if got := os.Getenv("RACKFOREST_TEST_ADDR"); got != ":3080" {
		t.Fatalf("addr: got %q", got)
	}
	if got := os.Getenv("RACKFOREST_TEST_LEVEL"); got != "warn" {
		t.Fatalf("level: got %q", got)
	}
	if got := os.Getenv("RACKFOREST_TEST_NOTE"); got != "hello" {
		t.Fatalf("note: got %q", got)
	}
	if got := os.Getenv("RACKFOREST_TEST_EXISTING"); got != "keep" {
		t.Fatalf("existing env was overwritten: %q", got)
	}
}

func TestDuration(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "157")
	if got := Duration("SHUTDOWN_TIMEOUT", 10*time.Second); got != 157*time.Second {
		t.Fatalf("bare number: got %s", got)
	}

	t.Setenv("SHUTDOWN_TIMEOUT", "15ms")
	if got := Duration("SHUTDOWN_TIMEOUT", 10*time.Second); got != 15*time.Millisecond {
		t.Fatalf("unit: got %s", got)
	}
}
