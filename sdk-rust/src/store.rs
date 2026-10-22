//! Storage layer: the `Store` trait plus the PostgreSQL, MySQL and SQLite implementations.
//!
//! The SQL mirrors contract §4.2 (Q1..Q5) statement by statement, is fully parameterised and **never** uses database time functions:
//! the current time comes from the SDK clock and is passed in as a bound parameter.

use async_trait::async_trait;
use chrono::{DateTime, NaiveDateTime, Utc};
use sqlx::{MySqlPool, PgPool, SqlitePool};

/// SQL dialect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    Mysql,
    Sqlite,
}

impl Dialect {
    pub fn as_str(&self) -> &'static str {
        match self {
            Dialect::Postgres => "postgres",
            Dialect::Mysql => "mysql",
            Dialect::Sqlite => "sqlite",
        }
    }
}

/// Storage layer error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoreError {
    message: String,
}

impl StoreError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl std::fmt::Display for StoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for StoreError {}

impl From<sqlx::Error> for StoreError {
    fn from(err: sqlx::Error) -> Self {
        Self::new(format!("sql error: {}", err))
    }
}

/// Coordination state store. Implementations own the dialect differences (SQL text, time binding, duplicate detection).
#[async_trait]
pub trait Store: Send + Sync {
    /// Q1: does `(task_id, version)` already have a `status = 'success'` record?
    async fn version_succeeded(&self, task_id: &str, version: &str) -> Result<bool, StoreError>;

    /// Q2 + Q3: acquire the lock atomically. `true` means we own it (fresh insert or expired-lock takeover).
    async fn try_acquire(
        &self,
        task_id: &str,
        owner: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, StoreError>;

    /// Q4: write the version record (upsert).
    async fn record_version(
        &self,
        task_id: &str,
        version: &str,
        status: &str,
        executed_at: DateTime<Utc>,
        owner: &str,
    ) -> Result<(), StoreError>;

    /// Q5: release the lock, deleting only the row we own.
    async fn release(&self, task_id: &str, owner: &str) -> Result<(), StoreError>;

    /// Create the tables (DDL in `ddl/{postgres,mysql,sqlite}.sql`).
    async fn ensure_schema(&self) -> Result<(), StoreError>;

    fn dialect(&self) -> Dialect;
}

// ---------------------------------------------------------------------------
// Dialect helpers
// ---------------------------------------------------------------------------

/// SQLite time binding format: `YYYY-MM-DD HH:MM:SS.ffffff` (UTC, six fractional digits, so lexicographic order equals chronological order).
pub(crate) fn sqlite_time(value: DateTime<Utc>) -> String {
    value.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// PostgreSQL/MySQL time binding: native timestamp / DATETIME(6), UTC.
fn native_time(value: DateTime<Utc>) -> NaiveDateTime {
    value.naive_utc()
}

/// Read the driver's structured error code (SQLState for PostgreSQL/MySQL, decimal extended code for SQLite).
fn error_code(err: &sqlx::Error) -> Option<String> {
    match err {
        sqlx::Error::Database(db_err) => db_err.code().map(|code| code.into_owned()),
        _ => None,
    }
}

/// PostgreSQL duplicate key: only SQLState `23505` counts (contract §4.3).
fn is_postgres_unique_violation(err: &sqlx::Error) -> bool {
    error_code(err).as_deref() == Some("23505")
}

/// SQLite duplicate key: extended codes `1555` (PRIMARY KEY) and `2067` (UNIQUE), with SQLState `23505` as a fallback.
fn is_sqlite_unique_violation(err: &sqlx::Error) -> bool {
    matches!(
        error_code(err).as_deref(),
        Some("1555") | Some("2067") | Some("23505")
    )
}

/// MySQL duplicate key: only vendor error code `1062` (ER_DUP_ENTRY, contract §4.3).
fn is_mysql_duplicate_entry(err: &sqlx::Error) -> bool {
    match err {
        sqlx::Error::Database(db_err) => db_err
            .try_downcast_ref::<sqlx::mysql::MySqlDatabaseError>()
            .map(|mysql_err| mysql_err.number() == 1062)
            .unwrap_or(false),
        _ => false,
    }
}

/// Split a DDL file into single statements (skipping `--` comment and blank lines) and execute them one by one.
///
/// Avoids relying on the driver's multi-statement support, so all three dialects share one code path. Statements come from compile-time constants in `ddl/*.sql`,
/// contain no external input, hence `AssertSqlSafe` is safe here.
fn ddl_statements(sql: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut buf = String::new();

    for line in sql.split_inclusive('\n') {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("--") {
            continue;
        }
        buf.push_str(line);
        if trimmed.ends_with(';') {
            let text = buf.trim();
            let text = text.strip_suffix(';').unwrap_or(text).trim();
            if !text.is_empty() {
                statements.push(text.to_string());
            }
            buf.clear();
        }
    }

    let rest = buf.trim();
    if !rest.is_empty() {
        statements.push(rest.to_string());
    }

    statements
}

// ---------------------------------------------------------------------------
// SQLite
// ---------------------------------------------------------------------------

/// SQLite implementation. `SQLITE` shares the `POSTGRES` SQL family (`ON CONFLICT`); only the time binding differs.
#[derive(Debug, Clone)]
pub struct SqliteStore {
    pool: SqlitePool,
}

impl SqliteStore {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Connect with a DSN (`sqlite://path` or `sqlite::memory:`).
    pub async fn connect(dsn: &str) -> Result<Self, StoreError> {
        let pool = SqlitePool::connect(dsn).await?;
        Ok(Self::new(pool))
    }

    /// Connect with custom options (WAL, busy_timeout, create_if_missing, ...).
    pub async fn connect_with_options(
        options: sqlx::sqlite::SqliteConnectOptions,
    ) -> Result<Self, StoreError> {
        let pool = SqlitePool::connect_with(options).await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }
}

#[async_trait]
impl Store for SqliteStore {
    async fn version_succeeded(&self, task_id: &str, version: &str) -> Result<bool, StoreError> {
        let row = sqlx::query(
            "SELECT 1 FROM task_version_record \
             WHERE task_id = ? AND version = ? AND status = 'success' LIMIT 1",
        )
        .bind(task_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    async fn try_acquire(
        &self,
        task_id: &str,
        owner: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let acquired = sqlx::query(
            "INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(task_id)
        .bind(owner)
        .bind(sqlite_time(now))
        .bind(sqlite_time(expires_at))
        .execute(&self.pool)
        .await;

        match acquired {
            Ok(_) => Ok(true),
            Err(err) if is_sqlite_unique_violation(&err) => {
                // Lock already taken: takeover is only allowed once it expired (the conditional UPDATE is atomic).
                let updated = sqlx::query(
                    "UPDATE task_execution_lock \
                        SET owner_id = ?, acquired_at = ?, expires_at = ? \
                      WHERE task_id = ? AND expires_at < ?",
                )
                .bind(owner)
                .bind(sqlite_time(now))
                .bind(sqlite_time(expires_at))
                .bind(task_id)
                .bind(sqlite_time(now))
                .execute(&self.pool)
                .await?;

                if updated.rows_affected() == 1 {
                    tracing::info!(task_id = task_id, owner_id = owner, "lock takeover");
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(err) => Err(StoreError::from(err)),
        }
    }

    async fn record_version(
        &self,
        task_id: &str,
        version: &str,
        status: &str,
        executed_at: DateTime<Utc>,
        owner: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id) \
             VALUES (?, ?, ?, ?, ?) \
             ON CONFLICT (task_id, version) DO UPDATE \
                SET status = excluded.status, \
                    executed_at = excluded.executed_at, \
                    owner_id = excluded.owner_id",
        )
        .bind(task_id)
        .bind(version)
        .bind(status)
        .bind(sqlite_time(executed_at))
        .bind(owner)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn release(&self, task_id: &str, owner: &str) -> Result<(), StoreError> {
        let deleted =
            sqlx::query("DELETE FROM task_execution_lock WHERE task_id = ? AND owner_id = ?")
                .bind(task_id)
                .bind(owner)
                .execute(&self.pool)
                .await?;
        if deleted.rows_affected() > 1 {
            return Err(StoreError::new(format!(
                "release deleted {} rows for task \"{}\"",
                deleted.rows_affected(),
                task_id
            )));
        }
        Ok(())
    }

    async fn ensure_schema(&self) -> Result<(), StoreError> {
        for statement in ddl_statements(SQLITE_DDL) {
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    fn dialect(&self) -> Dialect {
        Dialect::Sqlite
    }
}

// ---------------------------------------------------------------------------
// PostgreSQL
// ---------------------------------------------------------------------------

/// PostgreSQL implementation.
#[derive(Debug, Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(dsn: &str) -> Result<Self, StoreError> {
        let pool = PgPool::connect(dsn).await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }
}

#[async_trait]
impl Store for PostgresStore {
    async fn version_succeeded(&self, task_id: &str, version: &str) -> Result<bool, StoreError> {
        let row = sqlx::query(
            "SELECT 1 FROM task_version_record \
             WHERE task_id = $1 AND version = $2 AND status = 'success' LIMIT 1",
        )
        .bind(task_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    async fn try_acquire(
        &self,
        task_id: &str,
        owner: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let acquired = sqlx::query(
            "INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(task_id)
        .bind(owner)
        .bind(native_time(now))
        .bind(native_time(expires_at))
        .execute(&self.pool)
        .await;

        match acquired {
            Ok(_) => Ok(true),
            Err(err) if is_postgres_unique_violation(&err) => {
                let updated = sqlx::query(
                    "UPDATE task_execution_lock \
                        SET owner_id = $1, acquired_at = $2, expires_at = $3 \
                      WHERE task_id = $4 AND expires_at < $5",
                )
                .bind(owner)
                .bind(native_time(now))
                .bind(native_time(expires_at))
                .bind(task_id)
                .bind(native_time(now))
                .execute(&self.pool)
                .await?;

                if updated.rows_affected() == 1 {
                    tracing::info!(task_id = task_id, owner_id = owner, "lock takeover");
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(err) => Err(StoreError::from(err)),
        }
    }

    async fn record_version(
        &self,
        task_id: &str,
        version: &str,
        status: &str,
        executed_at: DateTime<Utc>,
        owner: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (task_id, version) DO UPDATE \
                SET status = excluded.status, \
                    executed_at = excluded.executed_at, \
                    owner_id = excluded.owner_id",
        )
        .bind(task_id)
        .bind(version)
        .bind(status)
        .bind(native_time(executed_at))
        .bind(owner)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn release(&self, task_id: &str, owner: &str) -> Result<(), StoreError> {
        let deleted =
            sqlx::query("DELETE FROM task_execution_lock WHERE task_id = $1 AND owner_id = $2")
                .bind(task_id)
                .bind(owner)
                .execute(&self.pool)
                .await?;
        if deleted.rows_affected() > 1 {
            return Err(StoreError::new(format!(
                "release deleted {} rows for task \"{}\"",
                deleted.rows_affected(),
                task_id
            )));
        }
        Ok(())
    }

    async fn ensure_schema(&self) -> Result<(), StoreError> {
        for statement in ddl_statements(POSTGRES_DDL) {
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    fn dialect(&self) -> Dialect {
        Dialect::Postgres
    }
}

// ---------------------------------------------------------------------------
// MySQL
// ---------------------------------------------------------------------------

/// MySQL implementation. The SQL mirrors `PostgresStore`; the differences are the `?` placeholders, `ON DUPLICATE KEY UPDATE`
/// and duplicate detection (vendor code `1062`).
#[derive(Debug, Clone)]
pub struct MysqlStore {
    pool: MySqlPool,
}

impl MysqlStore {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn connect(dsn: &str) -> Result<Self, StoreError> {
        let pool = MySqlPool::connect(dsn).await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }
}

#[async_trait]
impl Store for MysqlStore {
    async fn version_succeeded(&self, task_id: &str, version: &str) -> Result<bool, StoreError> {
        let row = sqlx::query(
            "SELECT 1 FROM task_version_record \
             WHERE task_id = ? AND version = ? AND status = 'success' LIMIT 1",
        )
        .bind(task_id)
        .bind(version)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    async fn try_acquire(
        &self,
        task_id: &str,
        owner: &str,
        now: DateTime<Utc>,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, StoreError> {
        let acquired = sqlx::query(
            "INSERT INTO task_execution_lock (task_id, owner_id, acquired_at, expires_at) \
             VALUES (?, ?, ?, ?)",
        )
        .bind(task_id)
        .bind(owner)
        .bind(native_time(now))
        .bind(native_time(expires_at))
        .execute(&self.pool)
        .await;

        match acquired {
            Ok(_) => Ok(true),
            Err(err) if is_mysql_duplicate_entry(&err) => {
                let updated = sqlx::query(
                    "UPDATE task_execution_lock \
                        SET owner_id = ?, acquired_at = ?, expires_at = ? \
                      WHERE task_id = ? AND expires_at < ?",
                )
                .bind(owner)
                .bind(native_time(now))
                .bind(native_time(expires_at))
                .bind(task_id)
                .bind(native_time(now))
                .execute(&self.pool)
                .await?;

                if updated.rows_affected() == 1 {
                    tracing::info!(task_id = task_id, owner_id = owner, "lock takeover");
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Err(err) => Err(StoreError::from(err)),
        }
    }

    async fn record_version(
        &self,
        task_id: &str,
        version: &str,
        status: &str,
        executed_at: DateTime<Utc>,
        owner: &str,
    ) -> Result<(), StoreError> {
        sqlx::query(
            "INSERT INTO task_version_record (task_id, version, status, executed_at, owner_id) \
             VALUES (?, ?, ?, ?, ?) \
             ON DUPLICATE KEY UPDATE \
                 status = VALUES(status), \
                 executed_at = VALUES(executed_at), \
                 owner_id = VALUES(owner_id)",
        )
        .bind(task_id)
        .bind(version)
        .bind(status)
        .bind(native_time(executed_at))
        .bind(owner)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn release(&self, task_id: &str, owner: &str) -> Result<(), StoreError> {
        let deleted =
            sqlx::query("DELETE FROM task_execution_lock WHERE task_id = ? AND owner_id = ?")
                .bind(task_id)
                .bind(owner)
                .execute(&self.pool)
                .await?;
        if deleted.rows_affected() > 1 {
            return Err(StoreError::new(format!(
                "release deleted {} rows for task \"{}\"",
                deleted.rows_affected(),
                task_id
            )));
        }
        Ok(())
    }

    async fn ensure_schema(&self) -> Result<(), StoreError> {
        for statement in ddl_statements(MYSQL_DDL) {
            sqlx::query(sqlx::AssertSqlSafe(statement))
                .execute(&self.pool)
                .await?;
        }
        Ok(())
    }

    fn dialect(&self) -> Dialect {
        Dialect::Mysql
    }
}

// ---------------------------------------------------------------------------
// DDL (byte-for-byte copies of spec/ddl/{postgres,mysql,sqlite}.sql)
// ---------------------------------------------------------------------------

const SQLITE_DDL: &str = include_str!("../ddl/sqlite.sql");
const POSTGRES_DDL: &str = include_str!("../ddl/postgres.sql");
const MYSQL_DDL: &str = include_str!("../ddl/mysql.sql");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ddl_files_split_into_two_create_statements() {
        for (dialect, ddl) in [
            ("postgres", POSTGRES_DDL),
            ("mysql", MYSQL_DDL),
            ("sqlite", SQLITE_DDL),
        ] {
            let statements = ddl_statements(ddl);
            assert_eq!(statements.len(), 2, "{} ddl: {:?}", dialect, statements);
            assert!(
                statements[0].starts_with("CREATE TABLE IF NOT EXISTS task_execution_lock"),
                "{} ddl first statement: {}",
                dialect,
                statements[0]
            );
            assert!(
                statements[1].starts_with("CREATE TABLE IF NOT EXISTS task_version_record"),
                "{} ddl second statement: {}",
                dialect,
                statements[1]
            );
            for statement in &statements {
                assert!(!statement.contains("--"), "comments must be stripped: {}", statement);
                assert!(!statement.ends_with(';'), "statements must not keep the trailing semicolon: {}", statement);
            }
        }
    }

    #[test]
    fn sqlite_time_is_fixed_width_utc_with_microseconds() {
        let value = DateTime::parse_from_rfc3339("2026-01-02T03:04:05.123456Z")
            .expect("parse")
            .with_timezone(&Utc);

        assert_eq!(sqlite_time(value), "2026-01-02 03:04:05.123456");
        // lexicographic order equals chronological order
        let later = value + chrono::Duration::seconds(1);
        assert!(sqlite_time(value) < sqlite_time(later));
    }
}
