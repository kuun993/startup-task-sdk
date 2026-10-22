package startuptask

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSpecFromFile(t *testing.T) {
	spec, err := LoadSpec(filepath.Join("examples", "publish_app_settings.yaml"))
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if spec.TaskID != "config/publish-app-settings" {
		t.Fatalf("TaskID = %q", spec.TaskID)
	}
	if spec.Version != "2026-10-22-v1" {
		t.Fatalf("Version = %q", spec.Version)
	}
	if spec.TimeoutSeconds != 120 {
		t.Fatalf("TimeoutSeconds = %d, want 120", spec.TimeoutSeconds)
	}
	if spec.Execution.Executor != "http" {
		t.Fatalf("Executor = %q, want http", spec.Execution.Executor)
	}
	if got := spec.Execution.Config["method"]; got != "PUT" {
		t.Fatalf("method = %v, want PUT", got)
	}
	if got := spec.Execution.Config["url"]; got != "${CONFIG_CENTER_URL}/api/v1/apps/${APP_NAME}/settings" {
		t.Fatalf("url = %v", got)
	}
	headers, ok := spec.Execution.Config["headers"].(map[string]any)
	if !ok {
		t.Fatalf("headers = %v, want a map", spec.Execution.Config["headers"])
	}
	if headers["Authorization"] != "Bearer ${CONFIG_CENTER_TOKEN}" {
		t.Fatalf("Authorization header = %v", headers["Authorization"])
	}
	// Relative body_path must resolve against the spec file's directory.
	if filepath.Base(spec.BaseDir) != "examples" {
		t.Fatalf("BaseDir = %q, want .../examples", spec.BaseDir)
	}
	resolved := filepath.Join(spec.BaseDir, "app_settings.json")
	if _, err := os.Stat(resolved); err != nil {
		t.Fatalf("body file %q not found: %v", resolved, err)
	}
}

func TestLoadSpecCustomHandler(t *testing.T) {
	spec, err := LoadSpec(filepath.Join("examples", "warm_up_cache.yaml"))
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if spec.Execution.Executor != "custom" {
		t.Fatalf("Executor = %q, want custom", spec.Execution.Executor)
	}
	if spec.Execution.Handler != "warm_up_cache" {
		t.Fatalf("Handler = %q, want warm_up_cache", spec.Execution.Handler)
	}
	if spec.TaskID != "cache/warm-up" || spec.Version != "2026-10-22-v1" {
		t.Fatalf("task/version = %q/%q, want cache/warm-up/2026-10-22-v1", spec.TaskID, spec.Version)
	}
	if spec.TimeoutSeconds != 60 {
		t.Fatalf("TimeoutSeconds = %d, want 60", spec.TimeoutSeconds)
	}
	keys, ok := spec.Execution.Config["keys"].([]any)
	if !ok || len(keys) != 3 {
		t.Fatalf("config.keys = %v, want 3 entries", spec.Execution.Config["keys"])
	}
	if spec.Execution.Config["ttl_seconds"] != 300 {
		t.Fatalf("config.ttl_seconds = %v, want 300", spec.Execution.Config["ttl_seconds"])
	}
}

func TestLoadSpecFromText(t *testing.T) {
	text := `
task_id: demo/text
version: "1"
execution:
  executor: custom
  handler: do_it
`
	spec, err := LoadSpec(text)
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if spec.TaskID != "demo/text" {
		t.Fatalf("TaskID = %q", spec.TaskID)
	}
	if spec.TimeoutSeconds != DefaultTimeoutSeconds {
		t.Fatalf("TimeoutSeconds = %d, want %d", spec.TimeoutSeconds, DefaultTimeoutSeconds)
	}
	if spec.Execution.Config == nil || len(spec.Execution.Config) != 0 {
		t.Fatalf("config = %v, want empty non-nil map", spec.Execution.Config)
	}
}

func TestLoadSpecErrors(t *testing.T) {
	cases := []struct {
		name string
		yaml string
		want string
	}{
		{
			name: "missing task_id",
			yaml: "version: v1\nexecution: {executor: custom, handler: h}\n",
			want: "task.task_id is required",
		},
		{
			name: "task_id wrong type",
			yaml: "task_id: 42\nversion: v1\nexecution: {executor: custom, handler: h}\n",
			want: "task.task_id must be a string",
		},
		{
			name: "task_id pattern",
			yaml: "task_id: \"bad id\"\nversion: v1\nexecution: {executor: custom, handler: h}\n",
			want: "task.task_id must match pattern",
		},
		{
			name: "task_id too long",
			yaml: "task_id: \"" + strings.Repeat("a", 129) + "\"\nversion: v1\nexecution: {executor: custom, handler: h}\n",
			want: "task.task_id length must be between 1 and 128",
		},
		{
			name: "missing version",
			yaml: "task_id: a\nexecution: {executor: custom, handler: h}\n",
			want: "task.version is required",
		},
		{
			name: "version too long",
			yaml: "task_id: a\nversion: \"" + strings.Repeat("v", 65) + "\"\nexecution: {executor: custom, handler: h}\n",
			want: "task.version length must be between 1 and 64",
		},
		{
			name: "timeout zero",
			yaml: "task_id: a\nversion: v1\ntimeout_seconds: 0\nexecution: {executor: custom, handler: h}\n",
			want: "task.timeout_seconds must be > 0",
		},
		{
			name: "timeout negative",
			yaml: "task_id: a\nversion: v1\ntimeout_seconds: -5\nexecution: {executor: custom, handler: h}\n",
			want: "task.timeout_seconds must be > 0",
		},
		{
			name: "timeout wrong type",
			yaml: "task_id: a\nversion: v1\ntimeout_seconds: abc\nexecution: {executor: custom, handler: h}\n",
			want: "task.timeout_seconds must be an integer",
		},
		{
			// Only integer values are accepted: "300.0" is a YAML float.
			name: "timeout float",
			yaml: "task_id: a\nversion: v1\ntimeout_seconds: 300.0\nexecution: {executor: custom, handler: h}\n",
			want: "task.timeout_seconds must be an integer",
		},
		{
			name: "unknown top level field",
			yaml: "task_id: a\nversion: v1\nnope: 1\nexecution: {executor: custom, handler: h}\n",
			want: "task.nope is not a valid field",
		},
		{
			name: "missing execution",
			yaml: "task_id: a\nversion: v1\n",
			want: "execution is required",
		},
		{
			name: "missing executor",
			yaml: "task_id: a\nversion: v1\nexecution: {handler: h}\n",
			want: "execution.executor is required",
		},
		{
			name: "bad executor",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: grpc}\n",
			want: "execution.executor must be one of [http, custom]",
		},
		{
			name: "executor case sensitive",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: HTTP}\n",
			want: "execution.executor must be one of [http, custom]",
		},
		{
			name: "custom without handler",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: custom}\n",
			want: "execution.handler is required",
		},
		{
			name: "http without config",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http}\n",
			want: "execution.config is required",
		},
		{
			name: "http without method",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {url: \"http://x\"}}\n",
			want: "execution.config.method is required",
		},
		{
			name: "http without url",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET}}\n",
			want: "execution.config.url is required",
		},
		{
			name: "http unknown config field",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", retries: 3}}\n",
			want: "execution.config.retries is not a valid field",
		},
		{
			name: "body_source without body_path",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", body_source: file}}\n",
			want: "execution.config.body_path is required when body_source is file",
		},
		{
			// An empty string is not a usable path.
			name: "body_path empty string",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", body_source: file, body_path: \"\"}}\n",
			want: "execution.config.body_path is required when body_source is file",
		},
		{
			name: "bad body_source",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", body_source: stdin}}\n",
			want: "execution.config.body_source must be one of [file, inline, none]",
		},
		{
			name: "header value not a string",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", headers: {X: 1}}}\n",
			want: "execution.config.headers.X must be a string",
		},
		{
			name: "empty status list",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", success_check: {http_status: []}}}\n",
			want: "execution.config.success_check.http_status must not be empty",
		},
		{
			name: "status not an integer",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", success_check: {http_status: ok}}}\n",
			want: "execution.config.success_check.http_status must be an integer or a list of integers",
		},
		{
			// Float statuses are rejected for the same reason as float timeouts.
			name: "status float",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", success_check: {http_status: 200.0}}}\n",
			want: "execution.config.success_check.http_status must be an integer or a list of integers",
		},
		{
			name: "unknown success_check field",
			yaml: "task_id: a\nversion: v1\nexecution: {executor: http, config: {method: GET, url: \"http://x\", success_check: {status: 200}}}\n",
			want: "execution.config.success_check.status is not a valid field",
		},
		{
			name: "root not a mapping",
			yaml: "- just\n- a list\n",
			want: "task definition must be a mapping",
		},
		{
			name: "invalid yaml",
			yaml: "task_id: [unclosed\n",
			want: "invalid yaml",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadSpec(tc.yaml)
			if err == nil {
				t.Fatalf("LoadSpec returned nil error, want %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestLoadSpecMissingFile(t *testing.T) {
	_, err := LoadSpec(filepath.Join(t.TempDir(), "nope.yaml"))
	if err == nil {
		t.Fatalf("LoadSpec returned nil error for a missing file")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("error = %q, want it to mention does not exist", err.Error())
	}
}

func TestLoadSpecTimeoutDefaultAndSuccessCheckForms(t *testing.T) {
	spec, err := LoadSpec(`
task_id: demo/http
version: v1
execution:
  executor: http
  config:
    method: get
    url: "http://example.invalid/x"
    success_check:
      http_status: [200, 201]
`)
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if spec.TimeoutSeconds != DefaultTimeoutSeconds {
		t.Fatalf("TimeoutSeconds = %d, want %d", spec.TimeoutSeconds, DefaultTimeoutSeconds)
	}
}

// Empty strings are legal for free-form fields (description, inline body) — only
// required-with-minLength fields reject "".
func TestLoadSpecAllowsEmptyDescriptionAndInlineBody(t *testing.T) {
	spec, err := LoadSpec(`
task_id: demo/empty-strings
version: v1
description: ""
execution:
  executor: http
  config:
    method: POST
    url: "http://example.invalid/x"
    body_source: inline
    body: ""
    headers: {}
`)
	if err != nil {
		t.Fatalf("LoadSpec: %v", err)
	}
	if spec.Description != "" {
		t.Fatalf("Description = %q, want empty", spec.Description)
	}
	if body, ok := spec.Execution.Config["body"].(string); !ok || body != "" {
		t.Fatalf("config.body = %v, want empty string", spec.Execution.Config["body"])
	}
	if spec.Execution.Config["body_source"] != "inline" {
		t.Fatalf("body_source = %v, want inline", spec.Execution.Config["body_source"])
	}
}
