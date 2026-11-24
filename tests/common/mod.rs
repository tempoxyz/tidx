#![allow(dead_code)]

pub mod clickhouse;
pub mod seed;
pub mod tempo;
pub mod testdb;

/// Skips a test whose service is unavailable, or fails it when
/// `TIDX_TEST_REQUIRE_SERVICES` is set (as CI does), so a broken service or
/// bootstrap cannot pass as a skip.
pub fn skip(reason: &str) {
    assert!(
        std::env::var_os("TIDX_TEST_REQUIRE_SERVICES").is_none(),
        "{reason}; TIDX_TEST_REQUIRE_SERVICES is set, so this fails instead of skipping"
    );
    println!("{reason}, skipping test");
}
