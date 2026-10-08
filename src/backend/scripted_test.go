package backend

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScriptedFailuresThenSuccess(t *testing.T) {
	s := NewScripted()
	s.SetFailCreates(2)
	s.SetFailDeletes(1)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		if err := s.CreateSnapshot(ctx, "vol", "snap"); !errors.Is(err, ErrCreateFailed) {
			t.Fatalf("create %d err = %v", i, err)
		}
	}
	if err := s.CreateSnapshot(ctx, "vol", "snap"); err != nil {
		t.Fatal(err)
	}
	if s.CreateCalls() != 3 {
		t.Fatalf("creates = %d", s.CreateCalls())
	}

	if err := s.DeleteSnapshot(ctx, "vol", "snap"); !errors.Is(err, ErrDeleteFailed) {
		t.Fatalf("delete err = %v", err)
	}
	if err := s.DeleteSnapshot(ctx, "vol", "missing"); err != nil {
		t.Fatal(err)
	}
	if s.DeleteCalls() != 2 {
		t.Fatalf("deletes = %d", s.DeleteCalls())
	}
}

func TestScriptedCountsInflightPerVolume(t *testing.T) {
	s := NewScripted()
	gate := make(chan struct{})
	s.SetGate(gate)

	errc := make(chan error, 3)
	go func() { errc <- s.CreateSnapshot(context.Background(), "vol-a", "s1") }()
	go func() { errc <- s.CreateSnapshot(context.Background(), "vol-a", "s2") }()
	go func() { errc <- s.DeleteSnapshot(context.Background(), "vol-b", "s3") }()
	waitInflight(t, s, 3)

	if s.MaxInflight() != 3 {
		t.Fatalf("max inflight = %d", s.MaxInflight())
	}
	if s.MaxVolumeInflight() != 2 {
		t.Fatalf("max volume inflight = %d", s.MaxVolumeInflight())
	}

	close(gate)
	for i := 0; i < 3; i++ {
		if err := <-errc; err != nil {
			t.Fatal(err)
		}
	}
	if s.Inflight() != 0 {
		t.Fatalf("inflight = %d", s.Inflight())
	}
}

func TestScriptedCancelWhileBlocked(t *testing.T) {
	s := NewScripted()
	s.SetGate(make(chan struct{}))
	s.SetFailCreates(1)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.CreateSnapshot(ctx, "vol", "snap") }()
	waitInflight(t, s, 1)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("call stayed blocked after cancel")
	}
	if s.Inflight() != 0 {
		t.Fatalf("inflight = %d", s.Inflight())
	}

	// The cancelled call did not consume the scripted failure.
	s.SetGate(nil)
	if err := s.CreateSnapshot(context.Background(), "vol", "snap"); !errors.Is(err, ErrCreateFailed) {
		t.Fatalf("next create err = %v", err)
	}
}

func TestScriptedDelayHonorsDeadline(t *testing.T) {
	s := NewScripted()
	s.SetDelay(time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := s.CreateSnapshot(ctx, "vol", "snap"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v", err)
	}
}

func waitInflight(t *testing.T, s *Scripted, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if s.Inflight() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("inflight = %d, want at least %d", s.Inflight(), n)
}
