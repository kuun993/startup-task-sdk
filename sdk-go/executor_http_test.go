package startuptask

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func httpSpec(serverURL string, config map[string]any) TaskSpec {
	cfg := map[string]any{
		"method": "POST",
		"url":    serverURL,
	}
	for k, v := range config {
		cfg[k] = v
	}
	return TaskSpec{
		TaskID:         "task/http",
		Version:        "v1",
		TimeoutSeconds: 10,
		BaseDir:        ".",
		Execution:      Execution{Executor: "http", Config: cfg},
	}
}

func TestHTTPExecutorSuccessWithFileBodyAndEnvHeaders(t *testing.T) {
	t.Setenv("STARTUP_TASK_TEST_TOKEN", "s3cret")

	dir := t.TempDir()
	bodyPath := filepath.Join(dir, "body.json")
	if err := os.WriteFile(bodyPath, []byte(`{"feature_flags":{"dark_mode":false}}`), 0o600); err != nil {
		t.Fatalf("write body file: %v", err)
	}

	var (
		gotMethod  string
		gotAuth    string
		gotContent string
		gotBody    string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotAuth = r.Header.Get("Authorization")
		gotContent = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	_, store := newTestStore(t)
	c := NewCoordinator(store, "pod-a")

	spec := httpSpec(server.URL+"/api/v1/apps/demo-app/settings", map[string]any{
		"method": "post", // must be upper-cased by the executor
		"headers": map[string]any{
			"Authorization": "Bearer ${STARTUP_TASK_TEST_TOKEN}",
			"Content-Type":  "application/json",
		},
		"body_source":   "file",
		"body_path":     "body.json",
		"success_check": map[string]any{"http_status": []any{200, 201}},
	})
	spec.BaseDir = dir

	result, err := c.Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	if gotMethod != "POST" {
		t.Fatalf("method = %q, want POST", gotMethod)
	}
	if gotAuth != "Bearer s3cret" {
		t.Fatalf("Authorization = %q, want Bearer s3cret", gotAuth)
	}
	if gotContent != "application/json" {
		t.Fatalf("Content-Type = %q", gotContent)
	}
	if gotBody != `{"feature_flags":{"dark_mode":false}}` {
		t.Fatalf("body = %q", gotBody)
	}
}

func TestHTTPExecutorInlineBodyEnvSubstitution(t *testing.T) {
	t.Setenv("STARTUP_TASK_TEST_NAME", "alpha")

	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	_, store := newTestStore(t)
	c := NewCoordinator(store, "pod-a")

	spec := httpSpec(server.URL, map[string]any{
		"method":      "PUT",
		"body_source": "inline",
		"body":        `{"name":"${STARTUP_TASK_TEST_NAME}"}`,
	})
	result, _ := c.Run(context.Background(), spec)
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	if gotBody != `{"name":"alpha"}` {
		t.Fatalf("body = %q", gotBody)
	}
}

func TestHTTPExecutorUnexpectedStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("boom"))
	}))
	defer server.Close()

	_, store := newTestStore(t)
	c := NewCoordinator(store, "pod-a")

	spec := httpSpec(server.URL, map[string]any{"method": "GET"})
	result, err := c.Run(context.Background(), spec)
	if err != nil {
		t.Fatalf("Run returned error: %v", err)
	}
	if result.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %s, want FAILED", result.Outcome)
	}
	const want = "http status 500 not in [200] (body: boom)"
	if result.Err == nil || result.Err.Error() != want {
		t.Fatalf("Err = %v, want %q", result.Err, want)
	}
}

func TestHTTPExecutorTransportError(t *testing.T) {
	// A server that is already closed yields a transport-level failure.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	url := server.URL
	server.Close()

	_, store := newTestStore(t)
	c := NewCoordinator(store, "pod-a")

	result, _ := c.Run(context.Background(), httpSpec(url, map[string]any{"method": "GET"}))
	if result.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %s, want FAILED", result.Outcome)
	}
	if result.Err == nil || len(result.Err.Error()) < len("http request failed: ") ||
		result.Err.Error()[:len("http request failed: ")] != "http request failed: " {
		t.Fatalf("Err = %v, want prefix \"http request failed: \"", result.Err)
	}
}

func TestHTTPExecutorMissingEnvVar(t *testing.T) {
	_, store := newTestStore(t)
	c := NewCoordinator(store, "pod-a")

	const unset = "STARTUP_TASK_TEST_DEFINITELY_UNSET"
	_ = os.Unsetenv(unset)

	spec := httpSpec("http://127.0.0.1:1/unreachable", map[string]any{
		"headers": map[string]any{"Authorization": "Bearer ${" + unset + "}"},
	})
	result, _ := c.Run(context.Background(), spec)
	if result.Outcome != OutcomeFailed {
		t.Fatalf("outcome = %s, want FAILED", result.Outcome)
	}
	want := `environment variable "` + unset + `" is not set`
	if result.Err == nil || result.Err.Error() != want {
		t.Fatalf("Err = %v, want %q", result.Err, want)
	}
}

func TestSubstituteEnv(t *testing.T) {
	t.Setenv("STARTUP_TASK_TEST_A", "1")
	cases := []struct {
		in      string
		want    string
		wantErr bool
	}{
		{in: "plain", want: "plain"},
		{in: "a${STARTUP_TASK_TEST_A}b", want: "a1b"},
		{in: "${STARTUP_TASK_TEST_A}${STARTUP_TASK_TEST_A}", want: "11"},
		{in: "$STARTUP_TASK_TEST_A", want: "$STARTUP_TASK_TEST_A"},
		{in: "${1BAD}", want: "${1BAD}"},
		{in: "${STARTUP_TASK_TEST_UNSET_X}", wantErr: true},
	}
	for _, tc := range cases {
		got, err := substituteEnv(tc.in)
		if tc.wantErr {
			if err == nil {
				t.Fatalf("substituteEnv(%q) = %q, want error", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Fatalf("substituteEnv(%q) error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Fatalf("substituteEnv(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
