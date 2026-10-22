package startuptask

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"
)

// testDBPath returns a fresh on-disk SQLite database path inside the test's temp
// directory. Real files (not :memory:) are required so that several *sql.DB
// pools / connections observe the same database.
func testDBPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(t.TempDir(), "startup_task_test.db")
}

// openTestDB opens an independent connection pool on path, with WAL and a
// generous busy timeout as required by contract §10.
func openTestDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	dsn := "file:" + filepath.ToSlash(path) +
		"?_pragma=busy_timeout(10000)&_pragma=journal_mode(WAL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open sqlite %s: %v", path, err)
	}
	db.SetMaxOpenConns(4)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// newTestStore returns a migrated SQLite store plus its handle for assertions.
func newTestStore(t *testing.T) (*sql.DB, Store) {
	t.Helper()
	db := openTestDB(t, testDBPath(t))
	store := NewSQLite(db)
	if err := EnsureSchema(context.Background(), store); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	return db, store
}

func customSpec(taskID, version, handler string) TaskSpec {
	return TaskSpec{
		TaskID:         taskID,
		Version:        version,
		TimeoutSeconds: 30,
		Execution: Execution{
			Executor: "custom",
			Handler:  handler,
			Config:   map[string]any{},
		},
	}
}

func formatSQLiteTime(t time.Time) string { return t.UTC().Format(sqliteTimeLayout) }

func countRows(t *testing.T, db *sql.DB, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatalf("count query %q: %v", query, err)
	}
	return n
}

func lockCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	return countRows(t, db, `SELECT COUNT(*) FROM task_execution_lock`)
}

func lockOwner(t *testing.T, db *sql.DB, taskID string) (string, bool) {
	t.Helper()
	var owner string
	err := db.QueryRow(`SELECT owner_id FROM task_execution_lock WHERE task_id = ?`, taskID).Scan(&owner)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("select lock owner: %v", err)
	}
	return owner, true
}

func versionStatus(t *testing.T, db *sql.DB, taskID, version string) (string, bool) {
	t.Helper()
	var status string
	err := db.QueryRow(
		`SELECT status FROM task_version_record WHERE task_id = ? AND version = ?`,
		taskID, version).Scan(&status)
	if err == sql.ErrNoRows {
		return "", false
	}
	if err != nil {
		t.Fatalf("select version status: %v", err)
	}
	return status, true
}
