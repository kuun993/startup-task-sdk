package startuptask

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// envVarPattern matches ${NAME} placeholders (contract §6.3).
var envVarPattern = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// executeHTTP runs the built-in http executor.
func (c *Coordinator) executeHTTP(ctx context.Context, spec TaskSpec) error {
	cfg := spec.Execution.Config

	method := ""
	if raw, ok := cfg["method"].(string); ok {
		method = strings.ToUpper(raw)
	}
	if method == "" {
		return errors.New("execution.config.method is required")
	}

	rawURL, _ := cfg["url"].(string)
	if rawURL == "" {
		return errors.New("execution.config.url is required")
	}
	url, err := substituteEnv(rawURL)
	if err != nil {
		return err
	}

	headers, err := expandHeaders(cfg["headers"])
	if err != nil {
		return err
	}

	bodySource := "none"
	if raw, ok := cfg["body_source"].(string); ok && raw != "" {
		bodySource = raw
	}

	var body []byte
	switch bodySource {
	case "none":
		// no body
	case "inline":
		raw, _ := cfg["body"].(string)
		expanded, err := substituteEnv(raw)
		if err != nil {
			return err
		}
		body = []byte(expanded)
	case "file":
		path, _ := cfg["body_path"].(string)
		if path == "" {
			return errors.New("execution.config.body_path is required when body_source is file")
		}
		resolved := path
		if !filepath.IsAbs(resolved) && spec.BaseDir != "" {
			resolved = filepath.Join(spec.BaseDir, resolved)
		}
		data, err := os.ReadFile(resolved)
		if err != nil {
			return fmt.Errorf("read body_path %q: %v", resolved, err)
		}
		body = data
	default:
		return fmt.Errorf("execution.config.body_source must be one of [file, inline, none], got %q", bodySource)
	}

	statuses, err := successStatuses(cfg)
	if err != nil {
		return err
	}

	var reader io.Reader
	if bodySource != "none" {
		reader = bytes.NewReader(body)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		return fmt.Errorf("http request failed: %v", err)
	}
	for _, key := range sortedStringKeys(headers) {
		req.Header.Set(key, headers[key])
	}

	timeout := leaseDuration(spec.timeoutSeconds())
	client := &http.Client{Timeout: timeout}

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %v", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("http request failed: %v", err)
	}

	if !containsStatus(statuses, resp.StatusCode) {
		return fmt.Errorf("http status %d not in %s (body: %s)",
			resp.StatusCode, formatStatuses(statuses), string(responseBody))
	}
	return nil
}

func expandHeaders(raw any) (map[string]string, error) {
	out := map[string]string{}
	if raw == nil {
		return out, nil
	}

	switch typed := raw.(type) {
	case map[string]string:
		keys := make([]string, 0, len(typed))
		for k := range typed {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			expanded, err := substituteEnv(typed[k])
			if err != nil {
				return nil, err
			}
			out[k] = expanded
		}
	case map[string]any:
		for _, k := range sortedKeys(typed) {
			value, ok := typed[k].(string)
			if !ok {
				return nil, fmt.Errorf("execution.config.headers.%s must be a string", k)
			}
			expanded, err := substituteEnv(value)
			if err != nil {
				return nil, err
			}
			out[k] = expanded
		}
	default:
		return nil, errors.New("execution.config.headers must be a mapping")
	}
	return out, nil
}

func successStatuses(cfg map[string]any) ([]int, error) {
	raw, ok := cfg["success_check"]
	if !ok || raw == nil {
		return []int{DefaultHTTPStatus}, nil
	}
	check, ok := raw.(map[string]any)
	if !ok {
		return nil, errors.New("execution.config.success_check must be a mapping")
	}
	value, ok := check["http_status"]
	if !ok || value == nil {
		return []int{DefaultHTTPStatus}, nil
	}
	if n, err := asInt(value); err == nil {
		return []int{n}, nil
	}
	list, ok := value.([]any)
	if !ok || len(list) == 0 {
		return nil, errors.New("execution.config.success_check.http_status must be an integer or a list of integers")
	}
	out := make([]int, 0, len(list))
	for _, item := range list {
		n, err := asInt(item)
		if err != nil {
			return nil, errors.New("execution.config.success_check.http_status must be an integer or a list of integers")
		}
		out = append(out, n)
	}
	return out, nil
}

func containsStatus(statuses []int, code int) bool {
	for _, s := range statuses {
		if s == code {
			return true
		}
	}
	return false
}

func formatStatuses(statuses []int) string {
	parts := make([]string, len(statuses))
	for i, s := range statuses {
		parts[i] = fmt.Sprintf("%d", s)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

// substituteEnv replaces ${NAME} with the process environment. A missing
// variable yields the contract's exact message: `environment variable "NAME"
// is not set`.
func substituteEnv(input string) (string, error) {
	var missing string
	output := envVarPattern.ReplaceAllStringFunc(input, func(match string) string {
		name := envVarPattern.FindStringSubmatch(match)[1]
		value, ok := os.LookupEnv(name)
		if !ok {
			if missing == "" {
				missing = name
			}
			return ""
		}
		return value
	})
	if missing != "" {
		return "", fmt.Errorf("environment variable %q is not set", missing)
	}
	return output, nil
}

func sortedStringKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
