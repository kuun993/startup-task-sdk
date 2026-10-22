package startuptask

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

const (
	// DefaultTimeoutSeconds is applied when timeout_seconds is absent.
	DefaultTimeoutSeconds = 300
	// DefaultHTTPStatus is the implicit success_check.http_status.
	DefaultHTTPStatus = 200
)

var taskIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:/-]+$`)

// TaskSpec is a validated task definition (see docs/design.md §6).
type TaskSpec struct {
	TaskID         string
	Version        string
	Description    string
	TimeoutSeconds int
	Execution      Execution
	// BaseDir is the directory relative body_path values resolve against: the
	// directory of the spec file when loaded from a path, the process working
	// directory when loaded from a string.
	BaseDir string
}

// Execution selects how the task body is executed.
type Execution struct {
	// Executor is "http" or "custom" (case sensitive).
	Executor string
	// Handler is the registered handler name; only set for executor=custom.
	Handler string
	// Config holds execution.config verbatim. For executor=custom it is passed
	// to the handler (empty map when absent).
	Config map[string]any
}

// timeoutSeconds returns the effective timeout, falling back to the default for
// hand-built specs that left the field zero.
func (s TaskSpec) timeoutSeconds() int {
	if s.TimeoutSeconds > 0 {
		return s.TimeoutSeconds
	}
	return DefaultTimeoutSeconds
}

// LoadSpec parses a task definition from YAML text or from a YAML file path.
//
// A value is treated as a file path when it contains no line break and names an
// existing regular file. Every returned error carries the offending field path.
func LoadSpec(src string) (TaskSpec, error) {
	text := src
	baseDir, err := os.Getwd()
	if err != nil {
		baseDir = "."
	}

	if path, ok := detectSpecPath(src); ok {
		data, err := os.ReadFile(path)
		if err != nil {
			return TaskSpec{}, fmt.Errorf("read spec file %q: %w", path, err)
		}
		text = string(data)
		if abs, absErr := filepath.Abs(path); absErr == nil {
			baseDir = filepath.Dir(abs)
		} else {
			baseDir = filepath.Dir(path)
		}
	} else if looksLikePath(src) {
		return TaskSpec{}, fmt.Errorf("spec file %q does not exist", src)
	}

	var root any
	if err := yaml.Unmarshal([]byte(text), &root); err != nil {
		return TaskSpec{}, fmt.Errorf("invalid yaml: %w", err)
	}
	return buildSpec(root, baseDir)
}

func detectSpecPath(src string) (string, bool) {
	if src == "" || strings.ContainsAny(src, "\r\n") {
		return "", false
	}
	info, err := os.Stat(src)
	if err != nil || info.IsDir() {
		return "", false
	}
	return src, true
}

func looksLikePath(src string) bool {
	if src == "" || strings.ContainsAny(src, "\r\n") {
		return false
	}
	lower := strings.ToLower(src)
	return strings.HasSuffix(lower, ".yaml") || strings.HasSuffix(lower, ".yml")
}

func buildSpec(root any, baseDir string) (TaskSpec, error) {
	m, ok := root.(map[string]any)
	if !ok {
		return TaskSpec{}, errors.New("task definition must be a mapping")
	}
	if err := checkKeys(m, "task", []string{"task_id", "version", "description", "timeout_seconds", "execution"}); err != nil {
		return TaskSpec{}, err
	}

	spec := TaskSpec{TimeoutSeconds: DefaultTimeoutSeconds, BaseDir: baseDir}

	taskID, err := requiredString(m, "task_id", "task.task_id")
	if err != nil {
		return TaskSpec{}, err
	}
	if n := utf8.RuneCountInString(taskID); n > 128 {
		return TaskSpec{}, errors.New("task.task_id length must be between 1 and 128")
	}
	if !taskIDPattern.MatchString(taskID) {
		return TaskSpec{}, errors.New("task.task_id must match pattern ^[A-Za-z0-9._:/-]+$")
	}
	spec.TaskID = taskID

	version, err := requiredString(m, "version", "task.version")
	if err != nil {
		return TaskSpec{}, err
	}
	if n := utf8.RuneCountInString(version); n > 64 {
		return TaskSpec{}, errors.New("task.version length must be between 1 and 64")
	}
	spec.Version = version

	if v, present := m["description"]; present && v != nil {
		s, ok := v.(string)
		if !ok {
			return TaskSpec{}, errors.New("task.description must be a string")
		}
		spec.Description = s
	}

	if v, present := m["timeout_seconds"]; present && v != nil {
		n, err := asInt(v)
		if err != nil {
			return TaskSpec{}, errors.New("task.timeout_seconds must be an integer")
		}
		if n <= 0 {
			return TaskSpec{}, errors.New("task.timeout_seconds must be > 0")
		}
		spec.TimeoutSeconds = n
	}

	execution, err := buildExecution(m)
	if err != nil {
		return TaskSpec{}, err
	}
	spec.Execution = execution

	return spec, nil
}

func buildExecution(root map[string]any) (Execution, error) {
	raw, present := root["execution"]
	if !present || raw == nil {
		return Execution{}, errors.New("execution is required")
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return Execution{}, errors.New("execution must be a mapping")
	}
	if err := checkKeys(m, "execution", []string{"executor", "handler", "config"}); err != nil {
		return Execution{}, err
	}

	executor, err := requiredString(m, "executor", "execution.executor")
	if err != nil {
		return Execution{}, err
	}

	switch executor {
	case "http":
		cfg, err := buildHTTPConfig(m)
		if err != nil {
			return Execution{}, err
		}
		return Execution{Executor: "http", Config: cfg}, nil
	case "custom":
		handler, err := requiredString(m, "handler", "execution.handler")
		if err != nil {
			return Execution{}, err
		}
		cfg := map[string]any{}
		if v, present := m["config"]; present && v != nil {
			c, ok := v.(map[string]any)
			if !ok {
				return Execution{}, errors.New("execution.config must be a mapping")
			}
			cfg = c
		}
		return Execution{Executor: "custom", Handler: handler, Config: cfg}, nil
	default:
		return Execution{}, fmt.Errorf("execution.executor must be one of [http, custom], got %q", executor)
	}
}

func buildHTTPConfig(root map[string]any) (map[string]any, error) {
	raw, present := root["config"]
	if !present || raw == nil {
		return nil, errors.New("execution.config is required")
	}
	cfg, ok := raw.(map[string]any)
	if !ok {
		return nil, errors.New("execution.config must be a mapping")
	}
	if err := checkKeys(cfg, "execution.config", []string{
		"method", "url", "headers", "body_source", "body_path", "body", "success_check",
	}); err != nil {
		return nil, err
	}

	if _, err := requiredString(cfg, "method", "execution.config.method"); err != nil {
		return nil, err
	}
	if _, err := requiredString(cfg, "url", "execution.config.url"); err != nil {
		return nil, err
	}

	if v, present := cfg["headers"]; present && v != nil {
		headers, ok := v.(map[string]any)
		if !ok {
			return nil, errors.New("execution.config.headers must be a mapping")
		}
		for _, key := range sortedKeys(headers) {
			if _, ok := headers[key].(string); !ok {
				return nil, fmt.Errorf("execution.config.headers.%s must be a string", key)
			}
		}
	}

	bodySource := "none"
	if v, present := cfg["body_source"]; present && v != nil {
		s, ok := v.(string)
		if !ok {
			return nil, errors.New("execution.config.body_source must be a string")
		}
		switch s {
		case "file", "inline", "none":
			bodySource = s
		default:
			return nil, fmt.Errorf("execution.config.body_source must be one of [file, inline, none], got %q", s)
		}
	}

	if bodySource == "file" {
		path, ok := cfg["body_path"]
		if !ok || path == nil {
			return nil, errors.New("execution.config.body_path is required when body_source is file")
		}
		s, ok := path.(string)
		if !ok {
			return nil, errors.New("execution.config.body_path must be a string")
		}
		if s == "" {
			return nil, errors.New("execution.config.body_path is required when body_source is file")
		}
	}

	if v, present := cfg["body"]; present && v != nil {
		if _, ok := v.(string); !ok {
			return nil, errors.New("execution.config.body must be a string")
		}
	}

	if v, present := cfg["success_check"]; present && v != nil {
		sc, ok := v.(map[string]any)
		if !ok {
			return nil, errors.New("execution.config.success_check must be a mapping")
		}
		if err := checkKeys(sc, "execution.config.success_check", []string{"http_status"}); err != nil {
			return nil, err
		}
		if hs, present := sc["http_status"]; present && hs != nil {
			if _, err := parseStatusList(hs, "execution.config.success_check.http_status"); err != nil {
				return nil, err
			}
		}
	}

	return cfg, nil
}

func parseStatusList(v any, path string) ([]int, error) {
	if n, err := asInt(v); err == nil {
		return []int{n}, nil
	}
	list, ok := v.([]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an integer or a list of integers", path)
	}
	if len(list) == 0 {
		return nil, fmt.Errorf("%s must not be empty", path)
	}
	out := make([]int, 0, len(list))
	for _, item := range list {
		n, err := asInt(item)
		if err != nil {
			return nil, fmt.Errorf("%s must be an integer or a list of integers", path)
		}
		out = append(out, n)
	}
	return out, nil
}

func requiredString(m map[string]any, key, path string) (string, error) {
	v, present := m[key]
	if !present || v == nil {
		return "", fmt.Errorf("%s is required", path)
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", path)
	}
	if s == "" {
		return "", fmt.Errorf("%s is required", path)
	}
	return s, nil
}

func checkKeys(m map[string]any, path string, allowed []string) error {
	for _, key := range sortedKeys(m) {
		if !slices.Contains(allowed, key) {
			if path == "" {
				return fmt.Errorf("%s is not a valid field", key)
			}
			return fmt.Errorf("%s.%s is not a valid field", path, key)
		}
	}
	return nil
}

func asInt(v any) (int, error) {
	switch n := v.(type) {
	case int:
		return n, nil
	case int8:
		return int(n), nil
	case int16:
		return int(n), nil
	case int32:
		return int(n), nil
	case int64:
		return int(n), nil
	case uint:
		return int(n), nil
	case uint8:
		return int(n), nil
	case uint16:
		return int(n), nil
	case uint32:
		return int(n), nil
	case uint64:
		return int(n), nil
	}
	return 0, fmt.Errorf("not an integer: %v", v)
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
