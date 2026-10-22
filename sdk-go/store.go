package startuptask

import (
	"context"
	"database/sql"
	"log/slog"
	"time"
)

// Store is the persistence port used by Coordinator. All methods take explicit
// UTC timestamps; no implementation may read the database clock.
type Store interface {
	// EnsureSchema creates the two tables when they do not exist yet.
	EnsureSchema(ctx context.Context) error
	// VersionSucceeded answers Q1: does a status='success' record exist for
	// (taskID, version)?
	VersionSucceeded(ctx context.Context, taskID, version string) (bool, error)
	// TryAcquire runs Q2 (atomic INSERT) and, on a duplicate-key conflict,
	// Q3 (conditional takeover of an expired lock). It reports whether the
	// caller now owns the lock. Non-conflict SQL errors are returned as-is.
	TryAcquire(ctx context.Context, taskID, ownerID string, now, expires time.Time) (bool, error)
	// RecordVersion runs Q4 (upsert) with status "success" or "failed".
	RecordVersion(ctx context.Context, taskID, version, status, ownerID string, at time.Time) error
	// Release runs Q5, deleting the lock only when it still belongs to ownerID.
	Release(ctx context.Context, taskID, ownerID string) error
}

// NewPostgres builds a Store backed by a PostgreSQL *sql.DB.
func NewPostgres(db *sql.DB) Store { return &sqlStore{db: db, d: postgresDialect{}} }

// NewMySQL builds a Store backed by a MySQL *sql.DB.
func NewMySQL(db *sql.DB) Store { return &sqlStore{db: db, d: mysqlDialect{}} }

// NewSQLite builds a Store backed by a SQLite *sql.DB (modernc.org/sqlite).
func NewSQLite(db *sql.DB) Store { return &sqlStore{db: db, d: sqliteDialect{}} }

type sqlStore struct {
	db *sql.DB
	d  dialect
}

// DialectName reports the store's dialect ("postgres", "mysql" or "sqlite").
func (s *sqlStore) DialectName() string { return s.d.name() }

func (s *sqlStore) EnsureSchema(ctx context.Context) error {
	for _, stmt := range s.d.ddl() {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *sqlStore) VersionSucceeded(ctx context.Context, taskID, version string) (bool, error) {
	rows, err := s.db.QueryContext(ctx, s.d.versionSucceededSQL(), taskID, version)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	if rows.Next() {
		return true, nil
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (s *sqlStore) TryAcquire(ctx context.Context, taskID, ownerID string, now, expires time.Time) (bool, error) {
	_, err := s.db.ExecContext(ctx, s.d.acquireSQL(),
		taskID, ownerID, s.d.bindTime(now), s.d.bindTime(expires))
	if err == nil {
		return true, nil
	}
	if !s.d.isDuplicateKey(err) {
		return false, err
	}

	// The lock row exists: take over only when the previous owner's lease has
	// expired. The predicate makes the update atomic.
	res, err := s.db.ExecContext(ctx, s.d.takeoverSQL(),
		ownerID, s.d.bindTime(now), s.d.bindTime(expires), taskID, s.d.bindTime(now))
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 1 {
		slog.Warn("lock takeover",
			"task_id", taskID, "owner_id", ownerID, "dialect", s.d.name())
		return true, nil
	}
	return false, nil
}

func (s *sqlStore) RecordVersion(ctx context.Context, taskID, version, status, ownerID string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, s.d.recordVersionSQL(),
		taskID, version, status, s.d.bindTime(at), ownerID)
	return err
}

func (s *sqlStore) Release(ctx context.Context, taskID, ownerID string) error {
	_, err := s.db.ExecContext(ctx, s.d.releaseSQL(), taskID, ownerID)
	return err
}

// releaseLock is the single release path used by Coordinator (via defer) and by
// tests. Q5 always carries the owner_id predicate so a lock taken over by
// another owner is never deleted by a stale release.
func releaseLock(ctx context.Context, store Store, taskID, ownerID string) error {
	return store.Release(ctx, taskID, ownerID)
}
