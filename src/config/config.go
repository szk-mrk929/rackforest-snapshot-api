package config

import (
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	envUtils "rackforest-snapshot-api/src/utils"
)

// Config is the process configuration.
// Duration variables accept a Go duration (30s, 200ms) or a bare number of seconds.
type Config struct {
	// Logging configuration
	LogLevel slog.Level

	// HTTP server configuration
	HTTPAddress string

	// Shutdown timeout configuration
	ShutdownTimeout time.Duration

	// Worker configuration
	WorkerCount     int
	WorkerAttempts  int
	WorkerQueueSize int

	// Tenant quota and storage-call policy
	TenantSnapshotQuota int
	StorageTimeout      time.Duration
	RetryBaseDelay      time.Duration

	// Random storage mock
	StorageMinDelay  time.Duration
	StorageMaxDelay  time.Duration
	StorageErrorRate float64
}

// Load reads the environment and rejects a value that is set but not valid.
// An empty or missing variable keeps its default.
func Load() (Config, error) {
	var c Config
	var err error

	if c.LogLevel, err = parseLevel("LOG_LEVEL", slog.LevelInfo); err != nil {
		return Config{}, err
	}
	c.HTTPAddress = strings.TrimSpace(envUtils.String("HTTP_ADDR", ":3000"))
	if c.ShutdownTimeout, err = parseDuration("SHUTDOWN_TIMEOUT", 24*time.Second); err != nil {
		return Config{}, err
	}
	if c.WorkerCount, err = parseInt("WORKER_COUNT", 3); err != nil {
		return Config{}, err
	}
	if c.WorkerAttempts, err = parseInt("WORKER_ATTEMPTS", 3); err != nil {
		return Config{}, err
	}
	if c.WorkerQueueSize, err = parseInt("WORKER_QUEUE_SIZE", 128); err != nil {
		return Config{}, err
	}
	if c.TenantSnapshotQuota, err = parseInt("TENANT_SNAPSHOT_QUOTA", 10); err != nil {
		return Config{}, err
	}
	if c.StorageTimeout, err = parseDuration("STORAGE_TIMEOUT", 30*time.Second); err != nil {
		return Config{}, err
	}
	if c.RetryBaseDelay, err = parseDuration("RETRY_BASE_DELAY", 200*time.Millisecond); err != nil {
		return Config{}, err
	}
	if c.StorageMinDelay, err = parseDuration("STORAGE_MIN_DELAY", 2*time.Second); err != nil {
		return Config{}, err
	}
	if c.StorageMaxDelay, err = parseDuration("STORAGE_MAX_DELAY", 10*time.Second); err != nil {
		return Config{}, err
	}
	if c.StorageErrorRate, err = parseFloat("STORAGE_ERROR_RATE", 0.2); err != nil {
		return Config{}, err
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// Validate reports a configuration that cannot run.
func (c Config) Validate() error {
	if c.HTTPAddress == "" {
		return fmt.Errorf("HTTP_ADDR is required")
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be greater than 0 (%s)", c.ShutdownTimeout)
	}
	if c.WorkerCount < 1 {
		return fmt.Errorf("WORKER_COUNT must be at least 1 (%d)", c.WorkerCount)
	}
	if c.WorkerAttempts < 1 {
		return fmt.Errorf("WORKER_ATTEMPTS must be at least 1 (%d)", c.WorkerAttempts)
	}
	if c.WorkerQueueSize < 1 {
		return fmt.Errorf("WORKER_QUEUE_SIZE must be at least 1 (%d)", c.WorkerQueueSize)
	}
	if c.TenantSnapshotQuota < 1 {
		return fmt.Errorf("TENANT_SNAPSHOT_QUOTA must be at least 1 (%d)", c.TenantSnapshotQuota)
	}
	if c.StorageTimeout <= 0 {
		return fmt.Errorf("STORAGE_TIMEOUT must be greater than 0 (%s)", c.StorageTimeout)
	}
	if c.RetryBaseDelay <= 0 {
		return fmt.Errorf("RETRY_BASE_DELAY must be greater than 0 (%s)", c.RetryBaseDelay)
	}
	if c.StorageMinDelay < 0 || c.StorageMaxDelay < c.StorageMinDelay {
		return fmt.Errorf("STORAGE_MIN_DELAY must be >= 0 and <= STORAGE_MAX_DELAY (%s, %s)", c.StorageMinDelay, c.StorageMaxDelay)
	}
	if math.IsNaN(c.StorageErrorRate) || c.StorageErrorRate < 0 || c.StorageErrorRate > 1 {
		return fmt.Errorf("STORAGE_ERROR_RATE must be between 0 and 1 (%v)", c.StorageErrorRate)
	}
	return nil
}

func parseInt(key string, fallback int) (int, error) {
	v := envUtils.String(key, "")
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func parseFloat(key string, fallback float64) (float64, error) {
	v := envUtils.String(key, "")
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return n, nil
}

func parseDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := envUtils.String(key, "")
	if v == "" {
		return fallback, nil
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d, nil
	}
	// A bare number is seconds: SHUTDOWN_TIMEOUT=24 means 24s.
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return time.Duration(n * float64(time.Second)), nil
}

func parseLevel(key string, fallback slog.Level) (slog.Level, error) {
	v := envUtils.String(key, "")
	if v == "" {
		return fallback, nil
	}
	switch strings.ToLower(v) {
	case "debug", "all":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("%s: unknown level %q", key, v)
	}
}
