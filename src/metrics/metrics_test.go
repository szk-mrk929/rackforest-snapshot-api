package metrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRenderQueueDurationAndErrors(t *testing.T) {
	m := New()
	m.SetQueueDepth(4)
	m.ObserveOperation("create", 200*time.Millisecond, false)
	m.ObserveOperation("delete", 50*time.Millisecond, true)

	body := m.Render()
	for _, want := range []string{
		"snapshot_queue_depth 4",
		`snapshot_storage_duration_seconds_count{op="create",result="success"} 1`,
		`snapshot_storage_errors_total{op="delete"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing %q in\n%s", want, body)
		}
	}
	if strings.Contains(body, `op="create",result="error"`) {
		t.Fatalf("success was counted as an error:\n%s", body)
	}

	rec := httptest.NewRecorder()
	m.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Header().Get("Content-Type"), "text/plain") {
		t.Fatalf("status %d type %q", rec.Code, rec.Header().Get("Content-Type"))
	}
	if !strings.Contains(rec.Body.String(), "snapshot_queue_depth 4") {
		t.Fatalf("handler body:\n%s", rec.Body.String())
	}
}
