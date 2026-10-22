//! Schema entry point (contract §2 `ensure_schema`).

use crate::store::{Store, StoreError};

/// Create the coordination tables on the given store (idempotent; DDL in `ddl/*.sql`).
pub async fn ensure_schema(store: &dyn Store) -> Result<(), StoreError> {
    store.ensure_schema().await
}
