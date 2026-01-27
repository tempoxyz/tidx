mod duckdb;
mod pool;
mod schema;

pub use duckdb::DuckDbPgEngine;
pub use pool::{create_pool, create_pool_with_size, BackfillConnection, ThrottledPool};
pub use schema::run_migrations;

pub type Pool = deadpool_postgres::Pool;
