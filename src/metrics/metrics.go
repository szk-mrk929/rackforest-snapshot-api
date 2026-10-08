// Package metrics records queue depth, storage-call duration, and error counts.
// The text is Prometheus exposition, written without the Prometheus client so
// the process keeps a single metrics dependency: this package.
package metrics

import (
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type sampleKey struct {
	op     string
	result string
}

// Metrics is the process recorder scraped at GET /metrics.
type Metrics struct {
	queue atomic.Int64

	mu     sync.Mutex
	count  map[sampleKey]uint64
	sum    map[sampleKey]float64
	errors map[string]uint64
}

// New returns an empty recorder.
func New() *Metrics {
	return &Metrics{
		count:  map[sampleKey]uint64{},
		sum:    map[sampleKey]float64{},
		errors: map[string]uint64{},
	}
}

// Handler serves the Prometheus text exposition. It does not require a tenant.
func (m *Metrics) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = io.WriteString(w, m.Render())
	})
}

// SetQueueDepth records jobs still waiting in the worker queue.
func (m *Metrics) SetQueueDepth(n int) {
	if m == nil {
		return
	}
	m.queue.Store(int64(n))
}

// ObserveOperation records one storage call. A failed call also increments the error counter.
// A call cancelled because the process is stopping is not observed: it is not a storage failure.
func (m *Metrics) ObserveOperation(op string, d time.Duration, failed bool) {
	if m == nil {
		return
	}
	result := "success"
	m.mu.Lock()
	defer m.mu.Unlock()
	if failed {
		result = "error"
		m.errors[op]++
	}
	key := sampleKey{op: op, result: result}
	m.count[key]++
	m.sum[key] += d.Seconds()
}

// Render returns the current exposition text.
func (m *Metrics) Render() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	var b fmtBuilder
	b.line("# HELP snapshot_queue_depth Jobs waiting in the worker queue.")
	b.line("# TYPE snapshot_queue_depth gauge")
	b.line("snapshot_queue_depth %d", m.queue.Load())

	b.line("# HELP snapshot_storage_duration_seconds Duration of one storage call, excluding backoff.")
	b.line("# TYPE snapshot_storage_duration_seconds summary")
	keys := make([]sampleKey, 0, len(m.count))
	for key := range m.count {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].op == keys[j].op {
			return keys[i].result < keys[j].result
		}
		return keys[i].op < keys[j].op
	})
	for _, key := range keys {
		b.line("snapshot_storage_duration_seconds_count{op=%q,result=%q} %d", key.op, key.result, m.count[key])
		b.line("snapshot_storage_duration_seconds_sum{op=%q,result=%q} %g", key.op, key.result, m.sum[key])
	}

	b.line("# HELP snapshot_storage_errors_total Storage calls that returned an error.")
	b.line("# TYPE snapshot_storage_errors_total counter")
	ops := make([]string, 0, len(m.errors))
	for op := range m.errors {
		ops = append(ops, op)
	}
	sort.Strings(ops)
	for _, op := range ops {
		b.line("snapshot_storage_errors_total{op=%q} %d", op, m.errors[op])
	}
	return b.String()
}

type fmtBuilder struct {
	buf []byte
}

func (b *fmtBuilder) line(format string, args ...any) {
	b.buf = fmt.Appendf(b.buf, format+"\n", args...)
}

func (b *fmtBuilder) String() string {
	return string(b.buf)
}
