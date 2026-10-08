package config

import (
	"fmt"
	"log/slog"
	envUtils "rackforest-snapshot-api/src/utils"
	"time"
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
		WorkerCount:     envUtils.Int("WORKER_COUNT", 3),        // Maximum number of concurrent workers
		WorkerAttempts:  envUtils.Int("WORKER_ATTEMPTS", 3),     // Maximum number of attempts to create or delete a snapshot
		WorkerQueueSize: envUtils.Int("WORKER_QUEUE_SIZE", 128), // Maximum number of snapshots to queue for processing
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

	return nil
}
