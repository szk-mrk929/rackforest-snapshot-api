package backend

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMockCreateIsIdempotentAndDeleteOfMissingSucceeds(t *testing.T) {
	m := NewMock(MockConfig{ErrorRate: 0, Seed: 1})
	ctx := context.Background()
	if err := m.CreateSnapshot(ctx, "vol-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	m.errorRate = 1
	if err := m.CreateSnapshot(ctx, "vol-1", "snap-1"); err != nil {
		t.Fatalf("repeat of a successful create failed: %v", err)
	}
	m.errorRate = 0
	if err := m.DeleteSnapshot(ctx, "vol-2", "snap-1"); err != nil {
		t.Fatalf("delete of another volume failed: %v", err)
	}
	m.errorRate = 1
	if err := m.CreateSnapshot(ctx, "vol-1", "snap-1"); err != nil {
		t.Fatalf("original snapshot was forgotten: %v", err)
	}

	m.errorRate = 0
	if err := m.DeleteSnapshot(ctx, "vol-1", "snap-1"); err != nil {
		t.Fatal(err)
	}
	if err := m.DeleteSnapshot(ctx, "vol-1", "missing"); err != nil {
		t.Fatal(err)
	}
}

func TestMockErrorRateAlwaysFailsUntilSuccessIsRecorded(t *testing.T) {
	m := NewMock(MockConfig{ErrorRate: 1, Seed: 1})
	ctx := context.Background()
	if err := m.CreateSnapshot(ctx, "vol", "snap"); !errors.Is(err, ErrCreateFailed) {
		t.Fatalf("create err = %v", err)
	}
	m.errorRate = 0
	if err := m.CreateSnapshot(ctx, "vol", "snap"); err != nil {
		t.Fatalf("failed create was recorded: %v", err)
	}
	m.errorRate = 1
	if err := m.DeleteSnapshot(ctx, "vol", "snap"); !errors.Is(err, ErrDeleteFailed) {
		t.Fatalf("delete err = %v", err)
	}
}

func TestMockHonorsCancel(t *testing.T) {
	m := NewMock(MockConfig{
		MinDelay:  time.Second,
		MaxDelay:  time.Second,
		ErrorRate: 0,
		Seed:      1,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := m.CreateSnapshot(ctx, "vol", "snap"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}

	m.minDelay, m.maxDelay = 0, 0
	m.errorRate = 1
	if err := m.CreateSnapshot(context.Background(), "vol", "snap"); !errors.Is(err, ErrCreateFailed) {
		t.Fatalf("cancelled create was recorded: %v", err)
	}
}

func TestMockDelayStaysInRange(t *testing.T) {
	const min = 2 * time.Second
	const max = 10 * time.Second
	m := NewMock(MockConfig{MinDelay: min, MaxDelay: max, Seed: 7})
	for i := 0; i < 100; i++ {
		d := m.nextDelay()
		if d < min || d >= max {
			t.Fatalf("delay %s outside [%s, %s)", d, min, max)
		}
	}

	fixed := NewMock(MockConfig{MinDelay: min, MaxDelay: min, Seed: 7})
	if d := fixed.nextDelay(); d != min {
		t.Fatalf("equal bounds delay = %s", d)
	}
}
