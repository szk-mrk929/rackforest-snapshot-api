package env

import (
	"bufio"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

var loadEnv = sync.OnceFunc(loadDotEnv)

func String(key, fallback string) string {
	loadEnv()
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return fallback
}

// loadDotEnv reads the nearest .env file, walking up from the working
// directory and stopping at the module root. Variables already set in the
// process environment are left unchanged.
func loadDotEnv() {
	path, ok := findEnvFile()
	if !ok {
		return
	}
	applyEnvFile(path)
}

func findEnvFile() (string, bool) {
	dir, err := os.Getwd()
	if err != nil {
		return "", false
	}
	for {
		candidate := filepath.Join(dir, ".env")
		if info, err := os.Stat(candidate); err == nil && !info.IsDir() {
			return candidate, true
		}
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return "", false
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", false
		}
		dir = parent
	}
}

func applyEnvFile(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		key, val, ok := parseEnvLine(scanner.Text())
		if key == "" || val == "" || !ok {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}
		_ = os.Setenv(key, val)
	}

	if err := scanner.Err(); err != nil {
		return
	}
}

func parseEnvLine(line string) (string, string, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return "", "", false
	}
	line = strings.TrimPrefix(line, "export ")
	key, val, ok := strings.Cut(line, "=")
	if !ok {
		return "", "", false
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", "", false
	}
	return key, parseEnvValue(val), true
}

func parseEnvValue(val string) string {
	val = strings.TrimSpace(val)
	if len(val) >= 2 {
		quote := val[0]
		if (quote == '"' || quote == '\'') && val[len(val)-1] == quote {
			return val[1 : len(val)-1]
		}
	}
	if i := strings.Index(val, " #"); i >= 0 {
		val = strings.TrimSpace(val[:i])
	}
	return val
}

func Level(key string, fallback slog.Level) slog.Level {
	switch strings.ToLower(String(key, "")) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "all":
		return slog.LevelDebug
	default:
		return fallback
	}
}

func Int(key string, fallback int) int {
	v := String(key, "")
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}

func Float64(key string, fallback float64) float64 {
	v := String(key, "")
	if v == "" {
		return fallback
	}
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return n
}

func Duration(key string, fallback time.Duration) time.Duration {
	v := String(key, "")
	if v == "" {
		return fallback
	}
	if d, err := time.ParseDuration(v); err == nil {
		return d
	}
	// A bare number is seconds: SHUTDOWN_TIMEOUT=157 means 157s.
	n, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return time.Duration(n * float64(time.Second))
}

func Bool(key string, fallback bool) bool {
	v := String(key, "")
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}
