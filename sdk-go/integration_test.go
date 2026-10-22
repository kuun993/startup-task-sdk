package startuptask

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Real-database integration tests (PostgreSQL / MySQL).
//
// They are opt-in: when STARTUP_TASK_PG_DSN / STARTUP_TASK_MYSQL_DSN is absent
// the tests skip, so `go test ./...` stays green on machines without databases.
//
// Connection strings are read from the environment and never printed. Every
// test creates its own rows under a unique task_id prefix and deletes them in
// t.Cleanup; no database or table is ever dropped.
const (
	pgDSNEnvVar    = "STARTUP_TASK_PG_DSN"
	mysqlDSNEnvVar = "STARTUP_TASK_MYSQL_DSN"

	integrationPingTimeout = 15 * time.Second
)

// integrationTarget describes one real database backend.
type integrationTarget struct {
	name     string
	envVar   string
	driver   string
	dsn      string
	dialect  dialect
	newStore func(*sql.DB) Store
}

func pgTarget(t *testing.T) *integrationTarget {
	t.Helper()
	raw := os.Getenv(pgDSNEnvVar)
	if raw == "" {
		t.Skipf("%s is not set; skipping PostgreSQL integration tests", pgDSNEnvVar)
	}
	return &integrationTarget{
		name:     "postgres",
		envVar:   pgDSNEnvVar,
		driver:   "postgres",
		dsn:      pgDSNWithSSLDisabled(raw),
		dialect:  postgresDialect{},
		newStore: NewPostgres,
	}
}

// pgDSNWithSSLDisabled derives a test DSN that keeps the caller's settings but
// falls back to sslmode=disable, because the self-test server does not speak
// TLS. An explicit sslmode in the DSN always wins.
func pgDSNWithSSLDisabled(raw string) string {
	if strings.Contains(raw, "sslmode=") {
		return raw
	}
	if strings.Contains(raw, "://") {
		separator := "?"
		if strings.Contains(raw, "?") {
			separator = "&"
		}
		return raw + separator + "sslmode=disable"
	}
	return raw + " sslmode=disable" // keyword/value DSN form
}

func mysqlTarget(t *testing.T) *integrationTarget {
	t.Helper()
	raw := os.Getenv(mysqlDSNEnvVar)
	if raw == "" {
		t.Skipf("%s is not set; skipping MySQL integration tests", mysqlDSNEnvVar)
	}
	dsn, err := mysqlDriverDSN(raw)
	if err != nil {
		t.Fatalf("cannot build a MySQL driver DSN from %s: %v", mysqlDSNEnvVar, err)
	}
	return &integrationTarget{
		name:     "mysql",
		envVar:   mysqlDSNEnvVar,
		driver:   "mysql",
		dsn:      dsn,
		dialect:  mysqlDialect{},
		newStore: NewMySQL,
	}
}

// mysqlDriverDSN converts a mysql:// URL into the go-sql-driver DSN form. Time
// columns are parsed back as UTC so DATETIME(6) round-trips stay timezone-free.
//
// The URL is parsed by hand: net/url rejects userinfo characters that occur in
// real passwords, and its error text would echo the credentials.
func mysqlDriverDSN(raw string) (string, error) {
	rest, ok := strings.CutPrefix(raw, "mysql://")
	if !ok {
		if strings.Contains(raw, "://") {
			return "", errors.New("unsupported scheme, want mysql://")
		}
		return raw, nil // already in driver form
	}

	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return "", errors.New("missing user:password@ in the DSN")
	}
	userinfo, hostAndDB := rest[:at], rest[at+1:]

	user, password, _ := strings.Cut(userinfo, ":")

	dbName, query := hostAndDB, ""
	if idx := strings.Index(hostAndDB, "/"); idx >= 0 {
		dbName = hostAndDB[idx+1:]
		hostAndDB = hostAndDB[:idx]
	}
	if idx := strings.Index(dbName, "?"); idx >= 0 {
		query = dbName[idx+1:]
		dbName = dbName[:idx]
	}

	cfg := mysql.NewConfig()
	cfg.User = pathUnescape(user)
	cfg.Passwd = pathUnescape(password)
	cfg.Net = "tcp"
	cfg.Addr = hostAndDB
	cfg.DBName = pathUnescape(dbName)
	cfg.ParseTime = true
	cfg.Loc = time.UTC
	cfg.Params = map[string]string{}
	if query != "" {
		values, err := url.ParseQuery(query)
		if err != nil {
			return "", errors.New("invalid query string in the DSN")
		}
		for key, value := range values {
			if len(value) == 0 || key == "parseTime" || key == "loc" {
				continue
			}
			cfg.Params[key] = value[len(value)-1]
		}
	}
	return cfg.FormatDSN(), nil
}

func pathUnescape(s string) string {
	if decoded, err := url.PathUnescape(s); err == nil {
		return decoded
	}
	return s
}

// open returns an independent connection pool on the target database.
func (tg *integrationTarget) open(t *testing.T) *sql.DB {
	t.Helper()
	return tg.openWithMaxConns(t, tg.dsn, 8)
}

func (tg *integrationTarget) openWithMaxConns(t *testing.T, dsn string, maxConns int) *sql.DB {
	t.Helper()
	db, err := sql.Open(tg.driver, dsn)
	if err != nil {
		t.Fatalf("sql.Open %s: %v", tg.name, err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	ctx, cancel := context.WithTimeout(context.Background(), integrationPingTimeout)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("connect to %s (%s): %v", tg.name, tg.envVar, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// sql rewrites ? placeholders into the target's syntax.
func (tg *integrationTarget) sql(query string) string {
	if tg.name != "postgres" {
		return query
	}
	var b strings.Builder
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			fmt.Fprintf(&b, "$%d", n)
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (tg *integrationTarget) uniquePrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("itsdk-%s-%d", tg.name, time.Now().UnixNano())
}

// cleanupRows deletes only the rows this test created.
func cleanupRows(t *testing.T, db *sql.DB, tg *integrationTarget, prefix string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), integrationPingTimeout)
		defer cancel()
		for _, table := range []string{"task_execution_lock", "task_version_record"} {
			if _, err := db.ExecContext(ctx, tg.sql(`DELETE FROM `+table+` WHERE task_id LIKE ?`), prefix+"%"); err != nil {
				t.Logf("cleanup %s for %s: %v", table, prefix, err)
			}
		}
	})
}

func queryInt(t *testing.T, db *sql.DB, tg *integrationTarget, query string, args ...any) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(), tg.sql(query), args...).Scan(&n); err != nil {
		t.Fatalf("query %q: %v", query, err)
	}
	return n
}

func lockRows(t *testing.T, db *sql.DB, tg *integrationTarget, taskID string) int {
	t.Helper()
	return queryInt(t, db, tg, `SELECT COUNT(*) FROM task_execution_lock WHERE task_id = ?`, taskID)
}

func lockOwnerInDB(t *testing.T, db *sql.DB, tg *integrationTarget, taskID string) (string, bool) {
	t.Helper()
	var owner string
	err := db.QueryRowContext(context.Background(),
		tg.sql(`SELECT owner_id FROM task_execution_lock WHERE task_id = ?`), taskID).Scan(&owner)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false
	}
	if err != nil {
		t.Fatalf("select lock owner: %v", err)
	}
	return owner, true
}

func versionRecord(t *testing.T, db *sql.DB, tg *integrationTarget, taskID, version string) (status, owner string, ok bool) {
	t.Helper()
	err := db.QueryRowContext(context.Background(),
		tg.sql(`SELECT status, owner_id FROM task_version_record WHERE task_id = ? AND version = ?`),
		taskID, version).Scan(&status, &owner)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", false
	}
	if err != nil {
		t.Fatalf("select version record: %v", err)
	}
	return status, owner, true
}

// truncMicro is the common precision of PostgreSQL timestamp and MySQL
// DATETIME(6); truncating the inputs keeps round-trip equality exact.
func truncMicro(t time.Time) time.Time { return t.UTC().Truncate(time.Microsecond) }

func assertSameInstant(t *testing.T, label string, got, want time.Time) {
	t.Helper()
	if !got.UTC().Equal(want) {
		t.Fatalf("%s = %s (unix %d), want %s (unix %d)",
			label, got.UTC().Format(time.RFC3339Nano), got.UnixNano(),
			want.UTC().Format(time.RFC3339Nano), want.UnixNano())
	}
	if _, offset := got.Zone(); offset != 0 {
		t.Fatalf("%s carried a non-UTC zone offset of %d seconds (%s)", label, offset, got)
	}
}

// ---------------------------------------------------------------- shared cases

func runCommonScenarios(t *testing.T, tg *integrationTarget) {
	t.Run("ensure_schema", func(t *testing.T) {
		ctx := context.Background()
		db := tg.open(t)
		store := tg.newStore(db)
		for i := 0; i < 2; i++ { // idempotent
			if err := EnsureSchema(ctx, store); err != nil {
				t.Fatalf("EnsureSchema #%d: %v", i+1, err)
			}
		}
		for _, table := range []string{"task_execution_lock", "task_version_record"} {
			var n int
			if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+table).Scan(&n); err != nil {
				t.Fatalf("table %s missing after EnsureSchema: %v", table, err)
			}
		}
	})

	t.Run("first_run_executed", func(t *testing.T) {
		ctx := context.Background()
		db := tg.open(t)
		store := tg.newStore(db)
		prefix := tg.uniquePrefix(t)
		cleanupRows(t, db, tg, prefix)
		taskID := prefix + "-first"

		var calls atomic.Int32
		c := NewCoordinator(store, "pod-first")
		if err := c.RegisterHandler("noop", func(map[string]any) error {
			calls.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("register handler: %v", err)
		}

		result, err := c.Run(ctx, customSpec(taskID, "v1", "noop"))
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		if result.Outcome != OutcomeExecuted {
			t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1", got)
		}
		status, owner, ok := versionRecord(t, db, tg, taskID, "v1")
		if !ok || status != statusSuccess || owner != "pod-first" {
			t.Fatalf("version record = (%q, %q, %v), want (success, pod-first, true)", status, owner, ok)
		}
		if n := lockRows(t, db, tg, taskID); n != 0 {
			t.Fatalf("lock rows = %d, want 0", n)
		}
	})

	t.Run("second_run_skipped_already_done", func(t *testing.T) {
		ctx := context.Background()
		db := tg.open(t)
		store := tg.newStore(db)
		prefix := tg.uniquePrefix(t)
		cleanupRows(t, db, tg, prefix)
		taskID := prefix + "-second"

		var calls atomic.Int32
		c := NewCoordinator(store, "pod-second")
		if err := c.RegisterHandler("noop", func(map[string]any) error {
			calls.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("register handler: %v", err)
		}

		spec := customSpec(taskID, "v1", "noop")
		if first, _ := c.Run(ctx, spec); first.Outcome != OutcomeExecuted {
			t.Fatalf("first outcome = %s, want EXECUTED (err: %v)", first.Outcome, first.Err)
		}
		if n := lockRows(t, db, tg, taskID); n != 0 {
			t.Fatalf("lock rows before second run = %d, want 0", n)
		}

		second, err := c.Run(ctx, spec)
		if err != nil {
			t.Fatalf("second Run returned error: %v", err)
		}
		if second.Outcome != OutcomeSkippedAlreadyDone {
			t.Fatalf("second outcome = %s, want SKIPPED_ALREADY_DONE (err: %v)", second.Outcome, second.Err)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1", got)
		}
		if n := lockRows(t, db, tg, taskID); n != 0 {
			t.Fatalf("lock rows after second run = %d, want 0", n)
		}
	})

	t.Run("concurrent_only_one_executes", func(t *testing.T) {
		ctx := context.Background()

		setup := tg.open(t)
		if err := EnsureSchema(ctx, tg.newStore(setup)); err != nil {
			t.Fatalf("EnsureSchema: %v", err)
		}
		prefix := tg.uniquePrefix(t)
		cleanupRows(t, setup, tg, prefix)
		taskID := prefix + "-concurrent"

		const pods = 8
		var calls atomic.Int32
		handler := func(map[string]any) error {
			calls.Add(1)
			time.Sleep(300 * time.Millisecond)
			return nil
		}

		spec := customSpec(taskID, "v1", "counting")
		results := make([]RunResult, pods)
		start := make(chan struct{})
		var wg sync.WaitGroup

		for i := 0; i < pods; i++ {
			// One pool per pod => one real connection per pod.
			db := tg.open(t)
			coordinator := NewCoordinator(tg.newStore(db), fmt.Sprintf("pod-%d", i))
			if err := coordinator.RegisterHandler("counting", handler); err != nil {
				t.Fatalf("register handler: %v", err)
			}
			wg.Add(1)
			go func(index int, c *Coordinator) {
				defer wg.Done()
				<-start
				result, err := c.Run(ctx, spec)
				if err != nil {
					t.Errorf("Run returned error: %v", err)
				}
				results[index] = result
			}(i, coordinator)
		}

		close(start)
		wg.Wait()

		var executed, skippedLocked int
		for i, result := range results {
			switch result.Outcome {
			case OutcomeExecuted:
				executed++
			case OutcomeSkippedLocked:
				skippedLocked++
			default:
				t.Errorf("pod-%d outcome = %s (err: %v), want EXECUTED or SKIPPED_LOCKED",
					i, result.Outcome, result.Err)
			}
		}
		if executed != 1 {
			t.Fatalf("EXECUTED count = %d, want 1", executed)
		}
		if skippedLocked != pods-1 {
			t.Fatalf("SKIPPED_LOCKED count = %d, want %d", skippedLocked, pods-1)
		}
		if got := calls.Load(); got != 1 {
			t.Fatalf("handler calls = %d, want 1", got)
		}
		if n := lockRows(t, setup, tg, taskID); n != 0 {
			t.Fatalf("lock rows = %d, want 0", n)
		}
		if status, _, ok := versionRecord(t, setup, tg, taskID, "v1"); !ok || status != statusSuccess {
			t.Fatalf("version record = (%q, %v), want (success, true)", status, ok)
		}
	})

	t.Run("expired_lock_takeover", func(t *testing.T) {
		ctx := context.Background()
		db := tg.open(t)
		store := tg.newStore(db)
		prefix := tg.uniquePrefix(t)
		cleanupRows(t, db, tg, prefix)
		taskID := prefix + "-takeover"

		now := truncMicro(time.Now().UTC())
		// Q2's own SQL + the dialect's time binding keeps this portable.
		if _, err := db.ExecContext(ctx, tg.dialect.acquireSQL(), taskID, "old-pod",
			tg.dialect.bindTime(now.Add(-30*time.Minute)),
			tg.dialect.bindTime(now.Add(-20*time.Minute))); err != nil {
			t.Fatalf("insert expired lock: %v", err)
		}

		ownerDuringRun := ""
		c := NewCoordinator(store, "new-pod")
		if err := c.RegisterHandler("inspect", func(map[string]any) error {
			owner, ok := lockOwnerInDB(t, db, tg, taskID)
			if !ok {
				return errors.New("lock row missing while executing after takeover")
			}
			ownerDuringRun = owner
			return nil
		}); err != nil {
			t.Fatalf("register handler: %v", err)
		}

		result, err := c.Run(ctx, customSpec(taskID, "v1", "inspect"))
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		if result.Outcome != OutcomeExecuted {
			t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
		}
		if ownerDuringRun != "new-pod" {
			t.Fatalf("lock owner during execution = %q, want new-pod", ownerDuringRun)
		}
		if n := lockRows(t, db, tg, taskID); n != 0 {
			t.Fatalf("lock rows after run = %d, want 0", n)
		}
		status, owner, ok := versionRecord(t, db, tg, taskID, "v1")
		if !ok || status != statusSuccess || owner != "new-pod" {
			t.Fatalf("version record = (%q, %q, %v), want (success, new-pod, true)", status, owner, ok)
		}
	})

	t.Run("failure_releases_lock_and_no_success", func(t *testing.T) {
		ctx := context.Background()
		db := tg.open(t)
		store := tg.newStore(db)
		prefix := tg.uniquePrefix(t)
		cleanupRows(t, db, tg, prefix)
		taskID := prefix + "-failing"

		c := NewCoordinator(store, "pod-failing")
		if err := c.RegisterHandler("boom", func(map[string]any) error {
			return errors.New("boom")
		}); err != nil {
			t.Fatalf("register handler: %v", err)
		}

		result, err := c.Run(ctx, customSpec(taskID, "v1", "boom"))
		if err != nil {
			t.Fatalf("Run returned error: %v", err)
		}
		if result.Outcome != OutcomeFailed {
			t.Fatalf("outcome = %s, want FAILED", result.Outcome)
		}
		if result.Err == nil || result.Err.Error() != "boom" {
			t.Fatalf("Err = %v, want boom", result.Err)
		}
		if n := lockRows(t, db, tg, taskID); n != 0 {
			t.Fatalf("lock rows = %d, want 0 (release must run on failure)", n)
		}
		status, _, ok := versionRecord(t, db, tg, taskID, "v1")
		if !ok || status != statusFailed {
			t.Fatalf("version record = (%q, %v), want (failed, true)", status, ok)
		}
	})

	t.Run("release_does_not_delete_other_owner_lock", func(t *testing.T) {
		ctx := context.Background()
		db := tg.open(t)
		store := tg.newStore(db)
		prefix := tg.uniquePrefix(t)
		cleanupRows(t, db, tg, prefix)
		taskID := prefix + "-ownership"

		now := truncMicro(time.Now().UTC())
		acquired, err := store.TryAcquire(ctx, taskID, "pod-a", now, now.Add(5*time.Minute))
		if err != nil || !acquired {
			t.Fatalf("TryAcquire = (%v, %v), want (true, nil)", acquired, err)
		}
		if _, err := db.ExecContext(ctx,
			tg.sql(`UPDATE task_execution_lock SET owner_id = ? WHERE task_id = ?`),
			"pod-b", taskID); err != nil {
			t.Fatalf("reassign lock to pod-b: %v", err)
		}

		if err := releaseLock(ctx, store, taskID, "pod-a"); err != nil {
			t.Fatalf("releaseLock: %v", err)
		}
		owner, ok := lockOwnerInDB(t, db, tg, taskID)
		if !ok {
			t.Fatalf("lock row was deleted although pod-b owned it")
		}
		if owner != "pod-b" {
			t.Fatalf("lock owner = %q, want pod-b", owner)
		}
	})
}

// ----------------------------------------------------------------- entrypoints

func TestIntegrationPostgres(t *testing.T) {
	tg := pgTarget(t)
	runCommonScenarios(t, tg)
	t.Run("utc_time_binding", func(t *testing.T) { pgTimeBindingIsUTC(t, tg) })
	t.Run("timezone_session_keeps_lease_semantics", func(t *testing.T) { pgTimezoneSession(t, tg) })
}

func TestIntegrationMySQL(t *testing.T) {
	tg := mysqlTarget(t)
	runCommonScenarios(t, tg)
	t.Run("duplicate_key_1062", func(t *testing.T) { mysqlDuplicateKeyDetection(t, tg) })
	t.Run("upsert_replaces_failed_with_success", func(t *testing.T) { mysqlUpsert(t, tg) })
	t.Run("datetime6_round_trips_as_utc", func(t *testing.T) { mysqlDatetimeRoundTrip(t, tg) })
	t.Run("stored_wall_clock_is_utc", func(t *testing.T) { mysqlStoredWallClockIsUTC(t, tg) })
	t.Run("lease_boundary", func(t *testing.T) { mysqlLeaseBoundary(t, tg) })
}

// ------------------------------------------------------------------- Postgres

// Timestamps must be stored as the UTC instant that was bound, and read back
// with no zone offset.
func pgTimeBindingIsUTC(t *testing.T, tg *integrationTarget) {
	ctx := context.Background()
	db := tg.open(t)
	store := tg.newStore(db)
	prefix := tg.uniquePrefix(t)
	cleanupRows(t, db, tg, prefix)
	taskID := prefix + "-utc"

	now := truncMicro(time.Now().UTC())
	expires := now.Add(5 * time.Minute)
	acquired, err := store.TryAcquire(ctx, taskID, "pod-utc", now, expires)
	if err != nil || !acquired {
		t.Fatalf("TryAcquire = (%v, %v), want (true, nil)", acquired, err)
	}

	var gotAcquired, gotExpires time.Time
	if err := db.QueryRowContext(ctx,
		tg.sql(`SELECT acquired_at, expires_at FROM task_execution_lock WHERE task_id = ?`),
		taskID).Scan(&gotAcquired, &gotExpires); err != nil {
		t.Fatalf("select timestamps: %v", err)
	}
	assertSameInstant(t, "acquired_at", gotAcquired, now)
	assertSameInstant(t, "expires_at", gotExpires, expires)

	// A full Run must record executed_at through the same UTC path.
	runAt := truncMicro(time.Now().UTC())
	c := NewCoordinatorWithClock(store, "pod-utc-run", newFakeClock(runAt))
	if err := c.RegisterHandler("noop", func(map[string]any) error { return nil }); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	runTaskID := prefix + "-utc-run"
	result, _ := c.Run(ctx, customSpec(runTaskID, "v1", "noop"))
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	var executedAt time.Time
	if err := db.QueryRowContext(ctx,
		tg.sql(`SELECT executed_at FROM task_version_record WHERE task_id = ? AND version = ?`),
		runTaskID, "v1").Scan(&executedAt); err != nil {
		t.Fatalf("select executed_at: %v", err)
	}
	assertSameInstant(t, "executed_at", executedAt, runAt)
}

// A session timezone far from UTC must not shift what we write, read back, or
// how the strict `expires_at < now` takeover predicate behaves.
func pgTimezoneSession(t *testing.T, tg *integrationTarget) {
	ctx := context.Background()
	// MaxOpenConns(1): SET TIME ZONE then applies to every statement below.
	db := tg.openWithMaxConns(t, tg.dsn, 1)
	if _, err := db.ExecContext(ctx, `SET TIME ZONE 'Asia/Shanghai'`); err != nil {
		t.Fatalf("set session timezone: %v", err)
	}
	var sessionTZ string
	if err := db.QueryRowContext(ctx, `SHOW timezone`).Scan(&sessionTZ); err != nil {
		t.Fatalf("show timezone: %v", err)
	}
	if sessionTZ != "Asia/Shanghai" {
		t.Fatalf("session timezone = %q, want Asia/Shanghai", sessionTZ)
	}

	store := tg.newStore(db)
	prefix := tg.uniquePrefix(t)
	cleanupRows(t, db, tg, prefix)

	// 1. write/read round-trip stays the same instant.
	taskID := prefix + "-tz"
	now := truncMicro(time.Now().UTC())
	expires := now.Add(10 * time.Minute)
	acquired, err := store.TryAcquire(ctx, taskID, "pod-tz", now, expires)
	if err != nil || !acquired {
		t.Fatalf("TryAcquire = (%v, %v), want (true, nil)", acquired, err)
	}
	var gotAcquired, gotExpires time.Time
	if err := db.QueryRowContext(ctx,
		tg.sql(`SELECT acquired_at, expires_at FROM task_execution_lock WHERE task_id = ?`),
		taskID).Scan(&gotAcquired, &gotExpires); err != nil {
		t.Fatalf("select timestamps: %v", err)
	}
	assertSameInstant(t, "acquired_at (Asia/Shanghai session)", gotAcquired, now)
	assertSameInstant(t, "expires_at (Asia/Shanghai session)", gotExpires, expires)

	// 2. strict boundary: a lock expiring exactly now is still held.
	equalTaskID := prefix + "-tz-equal"
	if _, err := db.ExecContext(ctx, tg.dialect.acquireSQL(), equalTaskID, "old-pod",
		tg.dialect.bindTime(now.Add(-time.Minute)), tg.dialect.bindTime(now)); err != nil {
		t.Fatalf("insert lock expiring now: %v", err)
	}
	if taken, err := store.TryAcquire(ctx, equalTaskID, "new-pod", now, now.Add(time.Minute)); err != nil || taken {
		t.Fatalf("TryAcquire at expires_at == now = (%v, %v), want (false, nil)", taken, err)
	}

	// 3. one microsecond earlier is taken over.
	pastTaskID := prefix + "-tz-past"
	if _, err := db.ExecContext(ctx, tg.dialect.acquireSQL(), pastTaskID, "old-pod",
		tg.dialect.bindTime(now.Add(-time.Minute)), tg.dialect.bindTime(now.Add(-time.Microsecond))); err != nil {
		t.Fatalf("insert expired lock: %v", err)
	}
	if taken, err := store.TryAcquire(ctx, pastTaskID, "new-pod", now, now.Add(time.Minute)); err != nil || !taken {
		t.Fatalf("TryAcquire at expires_at == now-1µs = (%v, %v), want (true, nil)", taken, err)
	}

	// 4. a full Run under this session writes UTC timestamps and releases.
	runAt := truncMicro(time.Now().UTC())
	c := NewCoordinatorWithClock(store, "pod-tz-run", newFakeClock(runAt))
	var observedAcquired, observedExpires time.Time
	runTaskID := prefix + "-tz-run"
	if err := c.RegisterHandler("inspect", func(map[string]any) error {
		return db.QueryRowContext(ctx,
			tg.sql(`SELECT acquired_at, expires_at FROM task_execution_lock WHERE task_id = ?`),
			runTaskID).Scan(&observedAcquired, &observedExpires)
	}); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	spec := customSpec(runTaskID, "v1", "inspect") // TimeoutSeconds = 30
	result, _ := c.Run(ctx, spec)
	if result.Outcome != OutcomeExecuted {
		t.Fatalf("outcome = %s, want EXECUTED (err: %v)", result.Outcome, result.Err)
	}
	assertSameInstant(t, "acquired_at during run", observedAcquired, runAt)
	assertSameInstant(t, "expires_at during run", observedExpires, runAt.Add(30*time.Second))
	if n := lockRows(t, db, tg, runTaskID); n != 0 {
		t.Fatalf("lock rows = %d, want 0", n)
	}

	// Session setting is still in force, i.e. all statements above used it.
	if err := db.QueryRowContext(ctx, `SHOW timezone`).Scan(&sessionTZ); err != nil {
		t.Fatalf("show timezone: %v", err)
	}
	if sessionTZ != "Asia/Shanghai" {
		t.Fatalf("session timezone changed to %q during the test", sessionTZ)
	}
}

// ---------------------------------------------------------------------- MySQL

// The literal time binding must keep the strict `expires_at < now` boundary
// intact: a lock expiring exactly now is still held, one microsecond earlier is
// taken over.
func mysqlLeaseBoundary(t *testing.T, tg *integrationTarget) {
	ctx := context.Background()
	db := tg.open(t)
	store := tg.newStore(db)
	prefix := tg.uniquePrefix(t)
	cleanupRows(t, db, tg, prefix)

	now := truncMicro(time.Now().UTC())

	equalTaskID := prefix + "-boundary-equal"
	if _, err := db.ExecContext(ctx, tg.dialect.acquireSQL(), equalTaskID, "old-pod",
		tg.dialect.bindTime(now.Add(-time.Minute)), tg.dialect.bindTime(now)); err != nil {
		t.Fatalf("insert lock expiring now: %v", err)
	}
	if taken, err := store.TryAcquire(ctx, equalTaskID, "new-pod", now, now.Add(time.Minute)); err != nil || taken {
		t.Fatalf("TryAcquire at expires_at == now = (%v, %v), want (false, nil)", taken, err)
	}

	pastTaskID := prefix + "-boundary-past"
	if _, err := db.ExecContext(ctx, tg.dialect.acquireSQL(), pastTaskID, "old-pod",
		tg.dialect.bindTime(now.Add(-time.Minute)), tg.dialect.bindTime(now.Add(-time.Microsecond))); err != nil {
		t.Fatalf("insert expired lock: %v", err)
	}
	if taken, err := store.TryAcquire(ctx, pastTaskID, "new-pod", now, now.Add(time.Minute)); err != nil || !taken {
		t.Fatalf("TryAcquire at expires_at == now-1µs = (%v, %v), want (true, nil)", taken, err)
	}
	if owner, ok := lockOwnerInDB(t, db, tg, pastTaskID); !ok || owner != "new-pod" {
		t.Fatalf("lock = (%q, %v), want (new-pod, true)", owner, ok)
	}
}

// The driver's 1062 (ER_DUP_ENTRY) must be classified as "lock held", not as a
// failure.
func mysqlDuplicateKeyDetection(t *testing.T, tg *integrationTarget) {
	ctx := context.Background()
	db := tg.open(t)
	store := tg.newStore(db)
	prefix := tg.uniquePrefix(t)
	cleanupRows(t, db, tg, prefix)
	taskID := prefix + "-dup"

	now := truncMicro(time.Now().UTC())
	acquired, err := store.TryAcquire(ctx, taskID, "pod-a", now, now.Add(5*time.Minute))
	if err != nil || !acquired {
		t.Fatalf("first TryAcquire = (%v, %v), want (true, nil)", acquired, err)
	}

	// Raw driver error: the duplicate must surface as *mysql.MySQLError 1062.
	_, rawErr := db.ExecContext(ctx, tg.dialect.acquireSQL(), taskID, "pod-raw",
		tg.dialect.bindTime(now), tg.dialect.bindTime(now.Add(5*time.Minute)))
	if rawErr == nil {
		t.Fatalf("duplicate INSERT succeeded, want a 1062 error")
	}
	var mysqlErr *mysql.MySQLError
	if !errors.As(rawErr, &mysqlErr) {
		t.Fatalf("duplicate INSERT error type = %T, want *mysql.MySQLError", rawErr)
	}
	if mysqlErr.Number != 1062 {
		t.Fatalf("vendor code = %d, want 1062", mysqlErr.Number)
	}
	if !tg.dialect.isDuplicateKey(rawErr) {
		t.Fatalf("mysqlDialect.isDuplicateKey did not recognize %v", rawErr)
	}

	// Second acquisition attempt: held, and importantly not an error.
	acquired, err = store.TryAcquire(ctx, taskID, "pod-b", now, now.Add(5*time.Minute))
	if err != nil {
		t.Fatalf("second TryAcquire returned error %v, want (false, nil)", err)
	}
	if acquired {
		t.Fatalf("second TryAcquire acquired a live lock")
	}

	// And through the coordinator: SKIPPED_LOCKED, not FAILED.
	c := NewCoordinator(store, "pod-c")
	if err := c.RegisterHandler("noop", func(map[string]any) error { return nil }); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	result, _ := c.Run(ctx, customSpec(taskID, "v1", "noop"))
	if result.Outcome != OutcomeSkippedLocked {
		t.Fatalf("outcome = %s, want SKIPPED_LOCKED (err: %v)", result.Outcome, result.Err)
	}
}

// ON DUPLICATE KEY UPDATE must replace a failed record with success in place.
func mysqlUpsert(t *testing.T, tg *integrationTarget) {
	ctx := context.Background()
	db := tg.open(t)
	store := tg.newStore(db)
	prefix := tg.uniquePrefix(t)
	cleanupRows(t, db, tg, prefix)
	taskID := prefix + "-upsert"

	at := truncMicro(time.Now().UTC())
	if err := store.RecordVersion(ctx, taskID, "v1", statusFailed, "pod-a", at); err != nil {
		t.Fatalf("RecordVersion(failed): %v", err)
	}
	if err := store.RecordVersion(ctx, taskID, "v1", statusSuccess, "pod-b", at.Add(time.Second)); err != nil {
		t.Fatalf("RecordVersion(success): %v", err)
	}
	if n := queryInt(t, db, tg,
		`SELECT COUNT(*) FROM task_version_record WHERE task_id = ? AND version = ?`,
		taskID, "v1"); n != 1 {
		t.Fatalf("version rows = %d, want 1 (upsert, not insert)", n)
	}
	status, owner, ok := versionRecord(t, db, tg, taskID, "v1")
	if !ok || status != statusSuccess || owner != "pod-b" {
		t.Fatalf("version record = (%q, %q, %v), want (success, pod-b, true)", status, owner, ok)
	}
}

// DATETIME(6) must round-trip the UTC instant that was bound.
func mysqlDatetimeRoundTrip(t *testing.T, tg *integrationTarget) {
	ctx := context.Background()
	db := tg.open(t)
	store := tg.newStore(db)
	prefix := tg.uniquePrefix(t)
	cleanupRows(t, db, tg, prefix)

	at := truncMicro(time.Now().UTC())
	versionTaskID := prefix + "-datetime"
	if err := store.RecordVersion(ctx, versionTaskID, "v1", statusSuccess, "pod-dt", at); err != nil {
		t.Fatalf("RecordVersion: %v", err)
	}
	var executedAt time.Time
	if err := db.QueryRowContext(ctx,
		tg.sql(`SELECT executed_at FROM task_version_record WHERE task_id = ? AND version = ?`),
		versionTaskID, "v1").Scan(&executedAt); err != nil {
		t.Fatalf("select executed_at: %v", err)
	}
	assertSameInstant(t, "executed_at", executedAt, at)

	lockTaskID := prefix + "-datetime-lock"
	expires := at.Add(15 * time.Minute)
	acquired, err := store.TryAcquire(ctx, lockTaskID, "pod-dt", at, expires)
	if err != nil || !acquired {
		t.Fatalf("TryAcquire = (%v, %v), want (true, nil)", acquired, err)
	}
	var gotAcquired, gotExpires time.Time
	if err := db.QueryRowContext(ctx,
		tg.sql(`SELECT acquired_at, expires_at FROM task_execution_lock WHERE task_id = ?`),
		lockTaskID).Scan(&gotAcquired, &gotExpires); err != nil {
		t.Fatalf("select lock timestamps: %v", err)
	}
	assertSameInstant(t, "acquired_at", gotAcquired, at)
	assertSameInstant(t, "expires_at", gotExpires, expires)
}

// The stored wall clock must be UTC no matter how the caller configured the
// driver's loc: another language writes UTC into DATETIME(6), and a Go pod with
// loc=Local must agree with it.
func mysqlStoredWallClockIsUTC(t *testing.T, tg *integrationTarget) {
	ctx := context.Background()

	writeCfg, err := mysql.ParseDSN(tg.dsn)
	if err != nil {
		t.Fatalf("parse the MySQL driver DSN: %v", err)
	}
	shanghai, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		shanghai = time.FixedZone("CST", 8*3600)
	}
	writeCfg.Loc = shanghai // a very common real-world DSN setting
	writer := tg.openWithMaxConns(t, writeCfg.FormatDSN(), 2)
	// loc=UTC reader: what a Python/Java/Rust peer would see.
	reader := tg.open(t)

	store := tg.newStore(writer)
	prefix := tg.uniquePrefix(t)
	cleanupRows(t, reader, tg, prefix)

	now := truncMicro(time.Now().UTC())
	expires := now.Add(20 * time.Minute)
	lockTaskID := prefix + "-wallclock"
	acquired, err := store.TryAcquire(ctx, lockTaskID, "pod-loc", now, expires)
	if err != nil || !acquired {
		t.Fatalf("TryAcquire = (%v, %v), want (true, nil)", acquired, err)
	}

	// 1. The literal stored in DATETIME(6) is the UTC wall clock.
	var acquiredText, expiresText string
	if err := reader.QueryRowContext(ctx,
		tg.sql(`SELECT DATE_FORMAT(acquired_at, '%Y-%m-%d %H:%i:%s.%f'), DATE_FORMAT(expires_at, '%Y-%m-%d %H:%i:%s.%f') FROM task_execution_lock WHERE task_id = ?`),
		lockTaskID).Scan(&acquiredText, &expiresText); err != nil {
		t.Fatalf("select formatted timestamps: %v", err)
	}
	if got, want := acquiredText, now.UTC().Format("2006-01-02 15:04:05.000000"); got != want {
		t.Fatalf("stored acquired_at literal = %q, want the UTC wall clock %q (offset from UTC: %v)",
			got, want, now.Sub(mustParseDateTime(t, got)))
	}
	if got, want := expiresText, expires.UTC().Format("2006-01-02 15:04:05.000000"); got != want {
		t.Fatalf("stored expires_at literal = %q, want the UTC wall clock %q", got, want)
	}

	// 2. A loc=UTC reader sees the same instant, so peers agree.
	var gotAcquired, gotExpires time.Time
	if err := reader.QueryRowContext(ctx,
		tg.sql(`SELECT acquired_at, expires_at FROM task_execution_lock WHERE task_id = ?`),
		lockTaskID).Scan(&gotAcquired, &gotExpires); err != nil {
		t.Fatalf("select lock timestamps: %v", err)
	}
	assertSameInstant(t, "acquired_at (written with loc=Asia/Shanghai)", gotAcquired, now)
	assertSameInstant(t, "expires_at (written with loc=Asia/Shanghai)", gotExpires, expires)

	// 3. Version records take the same path.
	versionTaskID := prefix + "-wallclock-version"
	if err := store.RecordVersion(ctx, versionTaskID, "v1", statusSuccess, "pod-loc", now); err != nil {
		t.Fatalf("RecordVersion: %v", err)
	}
	var executedText string
	if err := reader.QueryRowContext(ctx,
		tg.sql(`SELECT DATE_FORMAT(executed_at, '%Y-%m-%d %H:%i:%s.%f') FROM task_version_record WHERE task_id = ? AND version = ?`),
		versionTaskID, "v1").Scan(&executedText); err != nil {
		t.Fatalf("select formatted executed_at: %v", err)
	}
	if got, want := executedText, now.UTC().Format("2006-01-02 15:04:05.000000"); got != want {
		t.Fatalf("stored executed_at literal = %q, want the UTC wall clock %q", got, want)
	}
}

func mustParseDateTime(t *testing.T, value string) time.Time {
	t.Helper()
	parsed, err := time.Parse("2006-01-02 15:04:05.000000", value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}
	return parsed
}
