package startuptask

import (
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/lib/pq"
)

// Version record statuses.
const (
	statusSuccess = "success"
	statusFailed  = "failed"
)

// sqliteTimeLayout is the contract-mandated TEXT representation for SQLite:
// UTC, 6 fractional digits, lexicographic order equals chronological order.
const sqliteTimeLayout = "2006-01-02 15:04:05.000000"

// mysqlTimeLayout is the canonical UTC literal bound for MySQL DATETIME(6).
const mysqlTimeLayout = "2006-01-02 15:04:05.000000"

// dialect encapsulates everything that differs between the three supported
// databases: placeholder syntax, upsert flavour, time binding and duplicate-key
// detection. Queries never contain database time functions (now() /
// CURRENT_TIMESTAMP); all timestamps are bound parameters.
type dialect interface {
	name() string
	versionSucceededSQL() string
	acquireSQL() string
	takeoverSQL() string
	recordVersionSQL() string
	releaseSQL() string
	bindTime(t time.Time) any
	isDuplicateKey(err error) bool
	ddl() []string
}

// baseDialect holds the ?-placeholder variants shared by MySQL and SQLite
// (and the PG/SQLite upsert flavour).
type baseDialect struct{}

func (baseDialect) versionSucceededSQL() string {
	return `SELECT 1
FROM task_version_record
WHERE task_id = ? AND version = ? AND status = 'success'`
}

func (baseDialect) acquireSQL() string {
	return `INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at)
VALUES (?, ?, ?, ?)`
}

func (baseDialect) takeoverSQL() string {
	return `UPDATE task_execution_lock
SET owner_id = ?, acquired_at = ?, expires_at = ?
WHERE task_id = ? AND expires_at < ?`
}

func (baseDialect) recordVersionSQL() string {
	return `INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)
VALUES (?, ?, ?, ?, ?)
ON CONFLICT (task_id, version) DO UPDATE
SET status = excluded.status,
    executed_at = excluded.executed_at,
    owner_id = excluded.owner_id`
}

func (baseDialect) releaseSQL() string {
	return `DELETE FROM task_execution_lock WHERE task_id = ? AND owner_id = ?`
}

func (baseDialect) bindTime(t time.Time) any { return t.UTC() }

func (baseDialect) isDuplicateKey(error) bool { return false }

// ---------------------------------------------------------------- PostgreSQL

type postgresDialect struct{ baseDialect }

func (postgresDialect) name() string { return "postgres" }

func (postgresDialect) versionSucceededSQL() string {
	return `SELECT 1
FROM task_version_record
WHERE task_id = $1 AND version = $2 AND status = 'success'`
}

func (postgresDialect) acquireSQL() string {
	return `INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at)
VALUES ($1, $2, $3, $4)`
}

func (postgresDialect) takeoverSQL() string {
	return `UPDATE task_execution_lock
SET owner_id = $1, acquired_at = $2, expires_at = $3
WHERE task_id = $4 AND expires_at < $5`
}

func (postgresDialect) recordVersionSQL() string {
	return `INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (task_id, version) DO UPDATE
SET status = excluded.status,
    executed_at = excluded.executed_at,
    owner_id = excluded.owner_id`
}

func (postgresDialect) releaseSQL() string {
	return `DELETE FROM task_execution_lock WHERE task_id = $1 AND owner_id = $2`
}

// isDuplicateKey reports PostgreSQL SQLState 23505 (unique_violation).
func (postgresDialect) isDuplicateKey(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return string(pqErr.Code) == "23505"
	}
	return false
}

func (postgresDialect) ddl() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS task_execution_lock (
    task_id      VARCHAR(128) PRIMARY KEY,
    owner_id     VARCHAR(128) NOT NULL,
    acquired_at  TIMESTAMP    NOT NULL,
    expires_at   TIMESTAMP    NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS task_version_record (
    task_id      VARCHAR(128) NOT NULL,
    version      VARCHAR(64)  NOT NULL,
    status       VARCHAR(16)  NOT NULL,
    executed_at  TIMESTAMP    NOT NULL,
    owner_id     VARCHAR(128) NOT NULL,
    PRIMARY KEY (task_id, version)
)`,
	}
}

// --------------------------------------------------------------------- MySQL

type mysqlDialect struct{ baseDialect }

func (mysqlDialect) name() string { return "mysql" }

// bindTime sends a canonical UTC literal instead of a time.Time. The driver
// would convert time.Time into the DSN's loc and store *local* wall clock, which
// breaks contract §4.4 (MySQL DATETIME(6) holds UTC) and makes a Go pod disagree
// with Python/Java/Rust peers by the zone offset. Literals keep writes and the
// `expires_at < ?` comparisons independent of any session/DSN timezone.
func (mysqlDialect) bindTime(t time.Time) any {
	return t.UTC().Format(mysqlTimeLayout)
}

func (mysqlDialect) recordVersionSQL() string {
	return `INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id)
VALUES (?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
    status = VALUES(status), executed_at = VALUES(executed_at), owner_id = VALUES(owner_id)`
}

// isDuplicateKey reports MySQL vendor code 1062 (ER_DUP_ENTRY); SQLState 23000
// is accepted as a corroborating signal because the MySQL driver does not
// expose an error interface we can depend on without importing it.
func (mysqlDialect) isDuplicateKey(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if code, ok := mysqlVendorCode(e); ok && code == 1062 {
			return true
		}
		if state, ok := sqlStateOf(e); ok && state == "23000" &&
			strings.Contains(strings.ToLower(e.Error()), "duplicate entry") {
			return true
		}
	}
	return false
}

func (mysqlDialect) ddl() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS task_execution_lock (
    task_id      VARCHAR(128) NOT NULL,
    owner_id     VARCHAR(128) NOT NULL,
    acquired_at  DATETIME(6)  NOT NULL,
    expires_at   DATETIME(6)  NOT NULL,
    PRIMARY KEY (task_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4`,
		`CREATE TABLE IF NOT EXISTS task_version_record (
    task_id      VARCHAR(128) NOT NULL,
    version      VARCHAR(64)  NOT NULL,
    status       VARCHAR(16)  NOT NULL,
    executed_at  DATETIME(6)  NOT NULL,
    owner_id     VARCHAR(128) NOT NULL,
    PRIMARY KEY (task_id, version)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4`,
	}
}

// -------------------------------------------------------------------- SQLite

type sqliteDialect struct{ baseDialect }

func (sqliteDialect) name() string { return "sqlite" }

func (sqliteDialect) bindTime(t time.Time) any {
	return t.UTC().Format(sqliteTimeLayout)
}

// isDuplicateKey reports extended result codes 1555 (SQLITE_CONSTRAINT_PRIMARYKEY)
// and 2067 (SQLITE_CONSTRAINT_UNIQUE), SQLState 23505, or the driver's message
// text for unique/primary-key violations.
func (sqliteDialect) isDuplicateKey(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if code, ok := sqliteExtendedCode(e); ok && (code == 1555 || code == 2067) {
			return true
		}
		if state, ok := sqlStateOf(e); ok && state == "23505" {
			return true
		}
		msg := e.Error()
		if strings.Contains(msg, "UNIQUE constraint failed") ||
			strings.Contains(msg, "PRIMARY KEY constraint failed") {
			return true
		}
	}
	return false
}

func (sqliteDialect) ddl() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS task_execution_lock (
    task_id      TEXT PRIMARY KEY,
    owner_id     TEXT NOT NULL,
    acquired_at  TEXT NOT NULL,
    expires_at   TEXT NOT NULL
)`,
		`CREATE TABLE IF NOT EXISTS task_version_record (
    task_id      TEXT NOT NULL,
    version      TEXT NOT NULL,
    status       TEXT NOT NULL,
    executed_at  TEXT NOT NULL,
    owner_id     TEXT NOT NULL,
    PRIMARY KEY (task_id, version)
)`,
	}
}

// ------------------------------------------------------- driver error probing

// sqliteExtendedCode reads the extended result code from drivers that expose
// `Code() int` (modernc.org/sqlite).
func sqliteExtendedCode(err error) (int, bool) {
	var coder interface{ Code() int }
	if errors.As(err, &coder) {
		return coder.Code(), true
	}
	if v := reflect.ValueOf(err); v.IsValid() {
		if code, ok := intField(v, "Code"); ok {
			return code, true
		}
	}
	return 0, false
}

// mysqlVendorCode reads the vendor error number from drivers that expose a
// `Number uint16` field (github.com/go-sql-driver/mysql).
func mysqlVendorCode(err error) (int, bool) {
	if v := reflect.ValueOf(err); v.IsValid() {
		if code, ok := uintField(v, "Number"); ok {
			return code, true
		}
	}
	return 0, false
}

func intField(v reflect.Value, name string) (int, bool) {
	f := structField(v, name)
	if !f.IsValid() {
		return 0, false
	}
	switch f.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(f.Int()), true
	}
	return 0, false
}

func uintField(v reflect.Value, name string) (int, bool) {
	f := structField(v, name)
	if !f.IsValid() {
		return 0, false
	}
	switch f.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int(f.Uint()), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int(f.Int()), true
	}
	return 0, false
}

func structField(v reflect.Value, name string) reflect.Value {
	if v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return reflect.Value{}
		}
		v = v.Elem()
	}
	if v.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	f := v.FieldByName(name)
	if !f.IsValid() || !f.CanInterface() {
		return reflect.Value{}
	}
	return f
}

// sqlStateOf reads the five-character SQLState from drivers that expose it
// either as a method or as a struct field.
func sqlStateOf(err error) (string, bool) {
	var stringer interface{ SQLState() string }
	if errors.As(err, &stringer) {
		return stringer.SQLState(), true
	}
	var byteser interface{ SQLState() [5]byte }
	if errors.As(err, &byteser) {
		state := byteser.SQLState()
		return strings.TrimRight(string(state[:]), "\x00"), true
	}
	v := reflect.ValueOf(err)
	if v.IsValid() {
		f := structField(v, "SQLState")
		if f.IsValid() {
			switch f.Kind() {
			case reflect.Array:
				if f.Type().Elem().Kind() == reflect.Uint8 {
					buf := make([]byte, f.Len())
					for i := 0; i < f.Len(); i++ {
						buf[i] = byte(f.Index(i).Uint())
					}
					return strings.TrimRight(string(buf), "\x00"), true
				}
			case reflect.String:
				return f.String(), true
			}
		}
	}
	return "", false
}
