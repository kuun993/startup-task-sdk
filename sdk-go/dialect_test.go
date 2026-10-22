package startuptask

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
)

func TestPostgresDuplicateKeyDetection(t *testing.T) {
	d := postgresDialect{}
	if !d.isDuplicateKey(&pq.Error{Code: "23505"}) {
		t.Fatalf("23505 not detected as duplicate key")
	}
	if d.isDuplicateKey(&pq.Error{Code: "23503"}) {
		t.Fatalf("23503 (foreign_key_violation) wrongly detected as duplicate key")
	}
	if !d.isDuplicateKey(fmt.Errorf("wrapped: %w", &pq.Error{Code: "23505"})) {
		t.Fatalf("wrapped 23505 not detected")
	}
	if d.isDuplicateKey(errors.New("connection refused")) {
		t.Fatalf("plain error wrongly detected as duplicate key")
	}
}

// fakeMySQLError mirrors github.com/go-sql-driver/mysql's MySQLError layout,
// which is how the dialect reads vendor codes without importing the driver.
type fakeMySQLError struct {
	Number   uint16
	SQLState [5]byte
	Message  string
}

func (e *fakeMySQLError) Error() string {
	return fmt.Sprintf("Error %d (%s): %s", e.Number, string(e.SQLState[:]), e.Message)
}

func TestMySQLDuplicateKeyDetection(t *testing.T) {
	d := mysqlDialect{}
	dup := &fakeMySQLError{Number: 1062, Message: "Duplicate entry 'a' for key 'PRIMARY'"}
	copy(dup.SQLState[:], "23000")
	if !d.isDuplicateKey(dup) {
		t.Fatalf("vendor code 1062 not detected as duplicate key")
	}
	if !d.isDuplicateKey(fmt.Errorf("wrapped: %w", dup)) {
		t.Fatalf("wrapped 1062 not detected")
	}
	if d.isDuplicateKey(&fakeMySQLError{Number: 1452, Message: "Cannot add or update a child row"}) {
		t.Fatalf("vendor code 1452 wrongly detected as duplicate key")
	}
	if d.isDuplicateKey(errors.New("dial tcp: connection refused")) {
		t.Fatalf("plain error wrongly detected as duplicate key")
	}
}

func TestSQLiteDuplicateKeyDetection(t *testing.T) {
	d := sqliteDialect{}
	// Real driver error: hit the actual primary key twice.
	db := openTestDB(t, testDBPath(t))
	if err := EnsureSchema(t.Context(), NewSQLite(db)); err != nil {
		t.Fatalf("ensure schema: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)`,
		"t", "a", "2026-01-01 00:00:00.000000", "2026-01-01 00:05:00.000000"); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, err := db.Exec(
		`INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) VALUES (?, ?, ?, ?)`,
		"t", "b", "2026-01-01 00:00:00.000000", "2026-01-01 00:05:00.000000")
	if err == nil {
		t.Fatalf("second insert succeeded, want unique violation")
	}
	if !d.isDuplicateKey(err) {
		t.Fatalf("driver error %v (type %T) not detected as duplicate key", err, err)
	}

	// Message-based fallbacks and SQLState, per contract §4.3.
	cases := []struct {
		err  error
		want bool
	}{
		{errors.New("UNIQUE constraint failed: task_execution_lock.task_id"), true},
		{errors.New("PRIMARY KEY constraint failed"), true},
		{errors.New("database is locked"), false},
		{&fakeSQLStateError{state: "23505"}, true},
		{&fakeSQLStateError{state: "23000"}, false},
	}
	for _, tc := range cases {
		if got := d.isDuplicateKey(tc.err); got != tc.want {
			t.Fatalf("isDuplicateKey(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

type fakeSQLStateError struct{ state string }

func (e *fakeSQLStateError) Error() string    { return "sqlstate " + e.state }
func (e *fakeSQLStateError) SQLState() string { return e.state }

func TestSQLiteTimeBinding(t *testing.T) {
	d := sqliteDialect{}
	bound, ok := d.bindTime(parseUTC("2026-10-22T08:30:01.123456789Z")).(string)
	if !ok {
		t.Fatalf("bindTime returned %T, want string", bound)
	}
	const want = "2026-10-22 08:30:01.123456"
	if bound != want {
		t.Fatalf("bindTime = %q, want %q", bound, want)
	}
	if len(bound) != len(want) {
		t.Fatalf("bound time length = %d, want %d", len(bound), len(want))
	}
}

func TestQueriesUseBindParametersOnly(t *testing.T) {
	// Guard against regressions that reintroduce database clock functions.
	for _, d := range []dialect{postgresDialect{}, mysqlDialect{}, sqliteDialect{}} {
		queries := []string{
			d.versionSucceededSQL(), d.acquireSQL(), d.takeoverSQL(),
			d.recordVersionSQL(), d.releaseSQL(),
		}
		for _, q := range queries {
			for _, forbidden := range []string{"now()", "NOW()", "CURRENT_TIMESTAMP", "current_timestamp"} {
				if containsFold(q, forbidden) {
					t.Fatalf("%s query contains database time function %q", d.name(), forbidden)
				}
			}
		}
	}
}

func TestEnsureSchemaIsIdempotent(t *testing.T) {
	ctx := t.Context()
	db := openTestDB(t, testDBPath(t))
	store := NewSQLite(db)
	for i := 0; i < 2; i++ {
		if err := EnsureSchema(ctx, store); err != nil {
			t.Fatalf("EnsureSchema #%d: %v", i, err)
		}
	}
	if _, err := db.ExecContext(ctx, `SELECT 1 FROM task_execution_lock`); err != nil {
		t.Fatalf("lock table missing: %v", err)
	}
	if _, err := db.ExecContext(ctx, `SELECT 1 FROM task_version_record`); err != nil {
		t.Fatalf("version table missing: %v", err)
	}
}

func parseUTC(value string) (t2 time.Time) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return parsed
}

func containsFold(haystack, needle string) bool {
	return strings.Contains(strings.ToLower(haystack), strings.ToLower(needle))
}

// MySQL binds a canonical UTC literal (driver loc must not leak into the stored
// wall clock); PostgreSQL binds a time.Time, already converted to UTC.
func TestDialectTimeBindingTypes(t *testing.T) {
	input := parseUTC("2026-10-22T08:30:01.123456789Z")

	mysqlBound, ok := mysqlDialect{}.bindTime(input).(string)
	if !ok {
		t.Fatalf("mysql bindTime returned %T, want string", mysqlBound)
	}
	if want := "2026-10-22 08:30:01.123456"; mysqlBound != want {
		t.Fatalf("mysql bindTime = %q, want %q", mysqlBound, want)
	}

	// A time already in another zone must still be written as UTC.
	inShanghai := input.In(time.FixedZone("CST", 8*3600))
	if got := (mysqlDialect{}).bindTime(inShanghai); got != mysqlBound {
		t.Fatalf("mysql bindTime for a non-UTC value = %v, want %q", got, mysqlBound)
	}

	pgBound, ok := (postgresDialect{}).bindTime(input).(time.Time)
	if !ok {
		t.Fatalf("postgres bindTime returned %T, want time.Time", pgBound)
	}
	if !pgBound.Equal(input) || pgBound.Location() != time.UTC {
		t.Fatalf("postgres bindTime = %v (%v), want the same instant in UTC", pgBound, pgBound.Location())
	}
}
