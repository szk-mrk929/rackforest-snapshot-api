package ctxutil

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestSleepReturnsOnTimer(t *testing.T) {
	start := time.Now()
	if err := Sleep(context.Background(), 5*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 3*time.Millisecond {
		t.Fatal("sleep returned too early")
	}
}

func TestSleepReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := Sleep(ctx, time.Second); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
}

func TestSleepHandlesNilContext(t *testing.T) {
	if err := Sleep(nil, 0); err != nil {
		t.Fatalf("err = %v", err)
	}
}
