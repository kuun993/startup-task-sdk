//! Runnable example: create a SQLite database, register a custom handler, then `run_all` two tasks and print the results.
//!
//! ```text
//! cd sdk-rust
//! cargo run --example run_examples
//! ```
//!
//! When the config center is unreachable the http task reports `FAILED` (no panic) and the custom task reports `EXECUTED`;
//! running the example a second time turns the custom task into `SKIPPED_ALREADY_DONE`.

use std::path::Path;
use std::sync::Arc;

use startup_task_sdk::{ensure_schema, load_spec, Coordinator, SqliteStore, Store};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Demo fallbacks for environment variables; in production let the environment inject them.
    fill_demo_env("CONFIG_CENTER_URL", "http://127.0.0.1:8080");
    fill_demo_env("CONFIG_CENTER_TOKEN", "example-token");
    fill_demo_env("APP_NAME", "demo-app");

    let db_path = std::env::var("STARTUP_TASK_DB").unwrap_or_else(|_| {
        std::env::temp_dir()
            .join("startup-task-sdk-example.db")
            .to_string_lossy()
            .into_owned()
    });
    let dsn = format!("sqlite://{}?mode=rwc", db_path.replace('\\', "/"));
    println!("store      : {}", dsn);

    let store = SqliteStore::connect(&dsn).await?;
    ensure_schema(&store).await?;
    let store: Arc<dyn Store> = Arc::new(store);

    let mut coordinator = Coordinator::new(store, "example-pod/00000001");

    // custom executor: the caller registers the handler; its argument is execution.config.
    coordinator.register_handler(
        "warm_up_cache",
        Box::new(|config| {
            let keys = config["keys"]
                .as_array()
                .map(|items| {
                    items
                        .iter()
                        .filter_map(|item| item.as_str())
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let ttl_seconds = config["ttl_seconds"].as_u64().unwrap_or(0);
            println!(
                "    warm_up_cache: warming up {} keys {:?}, ttl_seconds={}",
                keys.len(),
                keys,
                ttl_seconds
            );
            Ok(())
        }),
    )?;

    let base = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    let specs = [
        load_spec(&base.join("publish_app_settings.yaml").to_string_lossy())?,
        load_spec(&base.join("warm_up_cache.yaml").to_string_lossy())?,
    ];

    println!("\n== run_all ({} tasks) ==", specs.len());
    for result in coordinator.run_all(&specs).await {
        println!("  {}", result);
    }

    println!(
        "\nNote: run this example again and the cache/warm-up task returns SKIPPED_ALREADY_DONE (a given (task_id, version) succeeds only once)."
    );
    Ok(())
}

fn fill_demo_env(name: &str, default_value: &str) {
    if std::env::var_os(name).is_none() {
        std::env::set_var(name, default_value);
    }
}
