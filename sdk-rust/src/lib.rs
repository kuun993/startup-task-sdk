//! Cross-language startup task coordination SDK — Rust implementation.
//!
//! The normative behaviour is `docs/design.md`: a given `(task_id, version)` must succeed at most once.
//! See [`coordinator::Coordinator::run`] for the algorithm and [`store`] for the SQL.
//!
//! ```no_run
//! use std::sync::Arc;
//! use startup_task_sdk::{load_spec, ensure_schema, Coordinator, SqliteStore, Store};
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let store = SqliteStore::connect("sqlite://startup-task.db?mode=rwc").await?;
//! ensure_schema(&store).await?;
//!
//! let mut coordinator = Coordinator::with_generated_owner(Arc::new(store) as Arc<dyn Store>);
//! coordinator.register_handler("warm_up_cache", Box::new(|_config| Ok(())))?;
//!
//! let spec = load_spec("examples/warm_up_cache.yaml")?;
//! let result = coordinator.run(&spec).await;
//! println!("{}", result);
//! # Ok(())
//! # }
//! ```
//!
//! Field reference and trade-offs live in README.en.md.

pub mod coordinator;
pub mod executor;
pub mod outcome;
pub mod schema;
pub mod spec;
pub mod store;

pub use coordinator::{
    default_owner_id, Clock, Coordinator, Handler, HandlerError, RegisterHandlerError, SystemClock,
};
pub use executor::{substitute_env, ExecError};
pub use outcome::{Outcome, RunResult};
pub use schema::ensure_schema;
pub use spec::{
    load_spec, BodySource, CustomConfig, Execution, HttpConfig, SpecError, SpecErrorKind, TaskSpec,
};
pub use store::{Dialect, MysqlStore, PostgresStore, SqliteStore, Store, StoreError};
