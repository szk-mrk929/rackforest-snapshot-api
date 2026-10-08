package main

import (
	"fmt"
	"log/slog"
	"os"
	"rackforest-snapshot-api/src/config"
	"rackforest-snapshot-api/src/logger"
)

func main() {
	fmt.Printf("\n")

	// Load configuration
	conf, err := config.Load()
	if err != nil {
		slog.Error("Environment loading failed", "error", err)
		os.Exit(1)
	}

	// Initialize logger
	log := logger.New(conf.LogLevel, os.Stdout)
	slog.SetDefault(log)

	// Validate environment variables
	if err := conf.Validate(); err != nil {
		log.WithGroup("main").Error("Environment validation failed", "error", err)
		os.Exit(1)
	}

	// DEV:
	log.WithGroup("main").Info("Service started", slog.AnyValue(conf).Any())

	// // Run service
	// if err := run(conf, log); err != nil {
	// 	log.WithGroup("main").Error("Service stopped", "error", err)
	// 	os.Exit(1)
	// }

	// DEV:
	fmt.Println("\nℹ️  Hello, World!")
}

// func run(conf config.Config, log *slog.Logger) error {
// 	return nil
// }
