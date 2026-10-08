// Package ctxutil contains small context-aware flow helpers.
package ctxutil

import (
	"context"
	"time"
)

// Sleep blocks for d, or returns early when ctx is done.
// A non-positive duration returns immediately.
func Sleep(ctx context.Context, d time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
