package config

import (
	"fmt"
	"log/slog"
	"math"
	"time"

	envUtils "rackforest-snapshot-api/src/utils"
)

type Config struct {
	// Logging configuration
	LogLevel slog.Level // Logging level

	// HTTP server configuration
	HTTPAddress string // HTTP server address

	// Shutdown timeout configuration
	ShutdownTimeout time.Duration // Shutdown timeout

	// Worker configuration
	WorkerCount     int // Maximum number of concurrent worker operations
	WorkerAttempts  int // Maximum number of attempts to create or delete a snapshot
	WorkerQueueSize int // Maximum number of snapshots to queue for processing

	// Tenant quota and storage-call policy
	TenantSnapshotQuota int           // Non-deleted snapshots allowed per tenant
	StorageTimeout      time.Duration // Deadline of one storage call
	RetryBaseDelay      time.Duration // Base delay of the exponential backoff

	// Random storage mock
	StorageMinDelay  time.Duration // Lower bound of the mock delay
	StorageMaxDelay  time.Duration // Upper bound of the mock delay
	StorageErrorRate float64       // Mock failure probability, from 0 to 1
}

func Load() (Config, error) {
	c := Config{
		// Logging configuration
		LogLevel: envUtils.Level("LOG_LEVEL", slog.LevelInfo),

		// HTTP server configuration
		HTTPAddress: envUtils.String("HTTP_ADDR", ":3000"),

		// Shutdown timeout configuration
		ShutdownTimeout: envUtils.Duration("SHUTDOWN_TIMEOUT", 24*time.Second),

		// Worker configuration
		WorkerCount:     envUtils.Int("WORKER_COUNT", 3),
		WorkerAttempts:  envUtils.Int("WORKER_ATTEMPTS", 3),
		WorkerQueueSize: envUtils.Int("WORKER_QUEUE_SIZE", 128),

		// Tenant quota and storage-call policy
		TenantSnapshotQuota: envUtils.Int("TENANT_SNAPSHOT_QUOTA", 10),
		StorageTimeout:      envUtils.Duration("STORAGE_TIMEOUT", 30*time.Second),
		RetryBaseDelay:      envUtils.Duration("RETRY_BASE_DELAY", 200*time.Millisecond),

		// Random storage mock
		StorageMinDelay:  envUtils.Duration("STORAGE_MIN_DELAY", 2*time.Second),
		StorageMaxDelay:  envUtils.Duration("STORAGE_MAX_DELAY", 10*time.Second),
		StorageErrorRate: envUtils.Float64("STORAGE_ERROR_RATE", 0.2),
	}

	if err := c.Validate(); err != nil {
		return Config{}, err
	}

	return c, nil
}

func (c Config) Validate() error {
	if c.HTTPAddress == "" {
		return fmt.Errorf("HTTP_ADDR is required: %s", c.HTTPAddress)
	}
	if c.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be greater than 0 (%s)", c.ShutdownTimeout)
	}
	if c.WorkerCount < 1 {
		return fmt.Errorf("WORKER_COUNT must be at least 1, or more (%d)", c.WorkerCount)
	}
	if c.WorkerAttempts < 1 {
		return fmt.Errorf("WORKER_ATTEMPTS must be at least 1, or more (%d)", c.WorkerAttempts)
	}
	if c.WorkerQueueSize < 1 {
		return fmt.Errorf("WORKER_QUEUE_SIZE must be at least 1, or more (%d)", c.WorkerQueueSize)
	}
	if c.TenantSnapshotQuota < 1 {
		return fmt.Errorf("TENANT_SNAPSHOT_QUOTA must be at least 1, or more (%d)", c.TenantSnapshotQuota)
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
