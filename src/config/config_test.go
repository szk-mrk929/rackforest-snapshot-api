package config

import (
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	clearConfigEnv(t)

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.LogLevel != slog.LevelInfo {
		t.Errorf("LogLevel = %v", c.LogLevel)
	}
	if c.HTTPAddress != ":3000" {
		t.Errorf("HTTPAddress = %q", c.HTTPAddress)
	}
	if c.ShutdownTimeout != 24*time.Second {
		t.Errorf("ShutdownTimeout = %s", c.ShutdownTimeout)
	}
	if c.WorkerCount != 3 || c.WorkerAttempts != 3 || c.WorkerQueueSize != 128 {
		t.Errorf("workers = %d %d %d", c.WorkerCount, c.WorkerAttempts, c.WorkerQueueSize)
	}
	if c.TenantSnapshotQuota != 10 {
		t.Errorf("quota = %d", c.TenantSnapshotQuota)
	}
	if c.StorageTimeout != 30*time.Second {
		t.Errorf("StorageTimeout = %s", c.StorageTimeout)
	}
	if c.RetryBaseDelay != 200*time.Millisecond {
		t.Errorf("RetryBaseDelay = %s", c.RetryBaseDelay)
	}
	if c.StorageMinDelay != 2*time.Second || c.StorageMaxDelay != 10*time.Second {
		t.Errorf("mock delay = %s %s", c.StorageMinDelay, c.StorageMaxDelay)
	}
	if c.StorageErrorRate != 0.2 {
		t.Errorf("StorageErrorRate = %v", c.StorageErrorRate)
	}
}

func TestLoadOverrides(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("LOG_LEVEL", "warning")
	t.Setenv("HTTP_ADDR", ":9090")
	t.Setenv("SHUTDOWN_TIMEOUT", "15ms")
	t.Setenv("WORKER_COUNT", "5")
	t.Setenv("WORKER_ATTEMPTS", "2")
	t.Setenv("WORKER_QUEUE_SIZE", "16")
	t.Setenv("TENANT_SNAPSHOT_QUOTA", "4")
	t.Setenv("STORAGE_TIMEOUT", "30")
	t.Setenv("RETRY_BASE_DELAY", "200ms")
	t.Setenv("STORAGE_MIN_DELAY", "0")
	t.Setenv("STORAGE_MAX_DELAY", "5ms")
	t.Setenv("STORAGE_ERROR_RATE", "0")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.LogLevel != slog.LevelWarn || c.HTTPAddress != ":9090" {
		t.Fatalf("log/addr = %v %q", c.LogLevel, c.HTTPAddress)
	}
	if c.ShutdownTimeout != 15*time.Millisecond {
		t.Errorf("ShutdownTimeout = %s", c.ShutdownTimeout)
	}
	if c.WorkerCount != 5 || c.WorkerAttempts != 2 || c.WorkerQueueSize != 16 || c.TenantSnapshotQuota != 4 {
		t.Errorf("counts = %+v", c)
	}
	if c.StorageTimeout != 30*time.Second {
		t.Errorf("bare STORAGE_TIMEOUT = %s", c.StorageTimeout)
	}
	if c.RetryBaseDelay != 200*time.Millisecond {
		t.Errorf("RetryBaseDelay = %s", c.RetryBaseDelay)
	}
	if c.StorageMinDelay != 0 || c.StorageMaxDelay != 5*time.Millisecond || c.StorageErrorRate != 0 {
		t.Errorf("mock = %s %s %v", c.StorageMinDelay, c.StorageMaxDelay, c.StorageErrorRate)
	}
}

func TestLoadRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		key  string
		val  string
		want string
	}{
		{name: "worker count", key: "WORKER_COUNT", val: "0", want: "WORKER_COUNT"},
		{name: "attempts", key: "WORKER_ATTEMPTS", val: "0", want: "WORKER_ATTEMPTS"},
		{name: "queue", key: "WORKER_QUEUE_SIZE", val: "-1", want: "WORKER_QUEUE_SIZE"},
		{name: "quota", key: "TENANT_SNAPSHOT_QUOTA", val: "0", want: "TENANT_SNAPSHOT_QUOTA"},
		{name: "shutdown", key: "SHUTDOWN_TIMEOUT", val: "0", want: "SHUTDOWN_TIMEOUT"},
		{name: "storage timeout", key: "STORAGE_TIMEOUT", val: "-5", want: "STORAGE_TIMEOUT"},
		{name: "retry", key: "RETRY_BASE_DELAY", val: "0", want: "RETRY_BASE_DELAY"},
		{name: "error rate high", key: "STORAGE_ERROR_RATE", val: "1.5", want: "STORAGE_ERROR_RATE"},
		{name: "error rate low", key: "STORAGE_ERROR_RATE", val: "-0.1", want: "STORAGE_ERROR_RATE"},
		{name: "negative mock delay", key: "STORAGE_MIN_DELAY", val: "-1", want: "STORAGE_MIN_DELAY"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv(tt.key, tt.val)
			_, err := Load()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v", err)
			}
		})
	}
}

func TestLoadRejectsInvertedMockDelay(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("STORAGE_MIN_DELAY", "10")
	t.Setenv("STORAGE_MAX_DELAY", "2")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "STORAGE_MIN_DELAY") {
		t.Fatalf("err = %v", err)
	}
}

func TestValidateRejectsEmptyAddress(t *testing.T) {
	c := validConfig()
	c.HTTPAddress = ""
	if err := c.Validate(); err == nil {
		t.Fatal("expected error")
	}
}

func TestLoadFallsBackOnUnparseableValues(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("LOG_LEVEL", "loud")
	t.Setenv("WORKER_COUNT", "nope")
	t.Setenv("SHUTDOWN_TIMEOUT", "later")
	t.Setenv("STORAGE_ERROR_RATE", "often")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.LogLevel != slog.LevelInfo || c.WorkerCount != 3 || c.ShutdownTimeout != 24*time.Second || c.StorageErrorRate != 0.2 {
		t.Fatalf("fallback = %+v", c)
	}
}

func TestLoadAcceptsErrorRateBounds(t *testing.T) {
	for _, rate := range []string{"0", "1", "0.2"} {
		t.Run(rate, func(t *testing.T) {
			clearConfigEnv(t)
			t.Setenv("STORAGE_ERROR_RATE", rate)
			c, err := Load()
			if err != nil {
				t.Fatal(err)
			}
			want := map[string]float64{"0": 0, "1": 1, "0.2": 0.2}[rate]
			if c.StorageErrorRate != want {
				t.Fatalf("rate = %v, want %v", c.StorageErrorRate, want)
			}
		})
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"LOG_LEVEL",
		"HTTP_ADDR",
		"SHUTDOWN_TIMEOUT",
		"WORKER_COUNT",
		"WORKER_ATTEMPTS",
		"WORKER_QUEUE_SIZE",
		"TENANT_SNAPSHOT_QUOTA",
		"STORAGE_TIMEOUT",
		"RETRY_BASE_DELAY",
		"STORAGE_MIN_DELAY",
		"STORAGE_MAX_DELAY",
		"STORAGE_ERROR_RATE",
	} {
		t.Setenv(key, "")
	}
}

func validConfig() Config {
	return Config{
		LogLevel:            slog.LevelInfo,
		HTTPAddress:         ":3000",
		ShutdownTimeout:     24 * time.Second,
		WorkerCount:         3,
		WorkerAttempts:      3,
		WorkerQueueSize:     128,
		TenantSnapshotQuota: 10,
		StorageTimeout:      30 * time.Second,
		RetryBaseDelay:      200 * time.Millisecond,
		StorageMinDelay:     2 * time.Second,
		StorageMaxDelay:     10 * time.Second,
		StorageErrorRate:    0.2,
	}
}
