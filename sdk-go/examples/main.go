// Command examples runs both example task definitions against a local SQLite
// database. Run it with:
//
//	go run ./examples
//
// The configuration-center task fails gracefully (outcome FAILED, no panic) when
// the endpoint is unreachable. Delete the demo database file (path is printed at
// startup) to watch the tasks execute again.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"time"

	startuptask "github.com/kuun993/startup-task-sdk/go"

	_ "modernc.org/sqlite"
)

// Example defaults keep the run self-contained; a real deployment sets these in
// the environment instead.
var envDefaults = map[string]string{
	"CONFIG_CENTER_URL":   "http://127.0.0.1:8080",
	"CONFIG_CENTER_TOKEN": "example-token",
	"APP_NAME":            "demo-app",
}

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	dbPath := os.Getenv("TASK_DB_PATH")
	if dbPath == "" {
		dbPath = filepath.Join(os.TempDir(), "startup-task-sdk-examples.db")
	}

	db, err := sql.Open("sqlite", sqliteDSN(dbPath))
	if err != nil {
		log.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(4)

	store := startuptask.NewSQLite(db)
	if err := startuptask.EnsureSchema(ctx, store); err != nil {
		log.Fatalf("ensure schema: %v", err)
	}

	coordinator := startuptask.NewCoordinator(store, "")
	err = coordinator.RegisterHandler("warm_up_cache", func(config map[string]any) error {
		fmt.Printf("  [handler] warm_up_cache keys=%v ttl_seconds=%v\n",
			config["keys"], config["ttl_seconds"])
		return nil
	})
	if err != nil {
		log.Fatalf("register handler: %v", err)
	}

	specDir := examplesDir()
	specs := make([]startuptask.TaskSpec, 0, 2)
	for _, name := range []string{"publish_app_settings.yaml", "warm_up_cache.yaml"} {
		spec, err := startuptask.LoadSpec(filepath.Join(specDir, name))
		if err != nil {
			log.Fatalf("load %s: %v", name, err)
		}
		specs = append(specs, spec)
	}

	applied := applyEnvDefaults()
	fmt.Printf("database: %s\nowner:    %s\n", dbPath, coordinator.OwnerID())
	if len(applied) > 0 {
		fmt.Printf("env:      applied example defaults %v\n", applied)
	}
	fmt.Printf("hint:     the configuration-center task reports FAILED unless %s serves the settings API\n\n",
		os.Getenv("CONFIG_CENTER_URL"))

	for _, result := range coordinator.RunAll(ctx, specs) {
		fmt.Printf("task=%s version=%s outcome=%s duration=%.1fms\n",
			result.TaskID, result.Version, result.Outcome, result.DurationMs())
		if result.Err != nil {
			fmt.Printf("  error: %v\n", result.Err)
		}
	}
}

// applyEnvDefaults fills in the example's environment variables when the caller
// did not set them, so `go run ./examples` works out of the box.
func applyEnvDefaults() []string {
	var applied []string
	for _, key := range []string{"CONFIG_CENTER_URL", "CONFIG_CENTER_TOKEN", "APP_NAME"} {
		if os.Getenv(key) != "" {
			continue
		}
		if err := os.Setenv(key, envDefaults[key]); err == nil {
			applied = append(applied, key)
		}
	}
	return applied
}

func sqliteDSN(path string) string {
	if abs, err := filepath.Abs(path); err == nil {
		path = abs
	}
	return "file:" + filepath.ToSlash(path) +
		"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
}

// examplesDir locates the examples directory so `go run ./examples` works from
// any working directory.
func examplesDir() string {
	if _, file, _, ok := runtime.Caller(0); ok {
		return filepath.Dir(file)
	}
	return "examples"
}
