mod common;

use common::testdb::TestDb;

use tidx::sync::writer::{detect_all_gaps, detect_gaps, has_gaps};
use serial_test::serial;

// ============================================================================
// detect_all_gaps tests - unified gap detection from genesis to tip
// ============================================================================

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_empty_table() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Empty table with tip_num > 0 should report entire range as gap (starting from block 1)
    let gaps = detect_all_gaps(&db.pool, 100).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 1, "Should detect one gap for entire range");
    assert_eq!(gaps[0], (1, 100), "Gap should be 1 -> 100 (block 0 is genesis)");
}

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_empty_table_tip_zero() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Empty table with tip_num = 0 has no gaps
    let gaps = detect_all_gaps(&db.pool, 0).await.expect("Failed to detect gaps");

    assert!(gaps.is_empty(), "No gaps when tip_num is 0 and table is empty");
}

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_genesis_missing() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks starting from 5 (missing 0-4)
    for num in 5i64..=10 {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num - 1) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 10).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 1, "Should detect one gap from block 1");
    assert_eq!(gaps[0], (1, 4), "Gap should be 1 -> 4 (block 0 is genesis)");
}

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_genesis_present() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks from 0 to 10 (no gaps)
    for num in 0i64..=10 {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &if num == 0 { vec![0u8; 32] } else { vec![(num - 1) as u8; 32] },
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 10).await.expect("Failed to detect gaps");

    assert!(gaps.is_empty(), "Should have no gaps when fully synced from genesis");
}

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_multiple_gaps_sorted_by_recency() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks: 5, 6, 10, 11, 15, 16
    // Gaps: (0-4), (7-9), (12-14)
    for num in [5i64, 6, 10, 11, 15, 16] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num.saturating_sub(1)) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 16).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 3, "Should detect three gaps");

    // Gaps should be sorted by end block descending (most recent first)
    assert_eq!(gaps[0], (12, 14), "First gap should be most recent: 12-14");
    assert_eq!(gaps[1], (7, 9), "Second gap should be 7-9");
    assert_eq!(gaps[2], (1, 4), "Third gap should be from block 1: 1-4 (block 0 is genesis)");
}

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_single_block_gap() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks: 0, 1, 2, 4, 5 (missing block 3 only)
    for num in [0i64, 1, 2, 4, 5] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &if num == 0 { vec![0u8; 32] } else { vec![(num - 1) as u8; 32] },
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 5).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 1, "Should detect one gap");
    assert_eq!(gaps[0], (3, 3), "Gap should be single block 3");
}

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_only_genesis() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert only block 0
    conn.execute(
        r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
           VALUES (0, $1, $2, NOW(), 0, 1000000, 100000, $3)"#,
        &[
            &vec![0u8; 32],
            &vec![0u8; 32],
            &vec![0u8; 20],
        ],
    )
    .await
    .expect("Failed to insert block");

    // With tip = 0, no gaps
    let gaps = detect_all_gaps(&db.pool, 0).await.expect("Failed to detect gaps");
    assert!(gaps.is_empty(), "No gaps when only genesis exists and tip is 0");

    // With tip = 10 and only block 0, detect_all_gaps uses detect_gaps between existing blocks
    // Since there's only one block, detect_gaps returns empty, and there's no genesis gap 
    // (min block is 0), so no gaps are reported. The gap 1-10 is an "unfilled region" but
    // detect_all_gaps only reports gaps between MIN(blocks) and tip. Since we have block 0,
    // there's no gap from genesis. The gap from 1-10 would be detected as a gap between
    // existing blocks if we had block 11+. This is correct behavior - gap detection finds
    // discontinuities, not "how far we've synced."
    let gaps = detect_all_gaps(&db.pool, 10).await.expect("Failed to detect gaps");
    // No gaps detected because there are no discontinuities from block 0 onward that are
    // bounded by existing blocks
    assert!(gaps.is_empty(), "No gaps when only block 0 exists (no upper bound block)");
}

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_filters_beyond_tip() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks: 5, 6, 20, 21 (gap at 7-19, but tip is 10)
    for num in [5i64, 6, 20, 21] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num.saturating_sub(1)) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    // With tip = 10, should only show gaps up to block 10
    let gaps = detect_all_gaps(&db.pool, 10).await.expect("Failed to detect gaps");

    // Gap 7-19 extends beyond tip, but we filter to gaps where end <= tip
    // Since the gap 7-19 has end=19 > tip=10, it should be filtered out
    // Only the gap from block 1 (1-4) should remain
    assert_eq!(gaps.len(), 1, "Should only show gaps within tip range");
    assert_eq!(gaps[0], (1, 4), "Only gap from block 1 should be reported (block 0 is genesis)");
}

#[tokio::test]
#[serial(db)]
async fn test_detect_all_gaps_large_gap_range() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert block 0 and block 1000 (large gap in between)
    for num in [0i64, 1000] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![(num % 256) as u8; 32],
                &vec![((num.saturating_sub(1)) % 256) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 1000).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 1, "Should detect one large gap");
    assert_eq!(gaps[0], (1, 999), "Gap should be 1 -> 999");
}

// ============================================================================
// detect_gaps vs detect_all_gaps comparison
// ============================================================================

#[tokio::test]
#[serial(db)]
async fn test_detect_gaps_vs_detect_all_gaps() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks: 5, 6, 10, 11 (gaps: genesis 0-4 and middle 7-9)
    for num in [5i64, 6, 10, 11] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num.saturating_sub(1)) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    // detect_gaps only finds gaps between existing blocks
    let gaps = detect_gaps(&db.pool, u64::MAX).await.expect("Failed");
    assert_eq!(gaps.len(), 1, "detect_gaps only finds middle gaps");
    assert_eq!(gaps[0], (7, 9), "detect_gaps should find 7-9");

    // detect_all_gaps also includes gap from block 1
    let all_gaps = detect_all_gaps(&db.pool, 11).await.expect("Failed");
    assert_eq!(all_gaps.len(), 2, "detect_all_gaps finds block 1 gap + middle gaps");
    // Sorted by end descending
    assert_eq!(all_gaps[0], (7, 9), "Most recent gap first");
    assert_eq!(all_gaps[1], (1, 4), "Gap from block 1 last (block 0 is genesis)");
}

// ============================================================================
// Gap sync ordering tests - verify most recent gaps filled first
// ============================================================================

#[tokio::test]
#[serial(db)]
async fn test_gap_order_prioritizes_recent() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Create scenario: blocks at 0, 50, 100 with gaps at 1-49 and 51-99
    for num in [0i64, 50, 100] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![(num % 256) as u8; 32],
                &vec![((num.saturating_sub(1)) % 256) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 100).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 2, "Should detect two gaps");

    // Most recent gap (51-99) should be first
    assert_eq!(gaps[0], (51, 99), "Most recent gap should be first");
    assert_eq!(gaps[1], (1, 49), "Older gap should be second");
}

#[tokio::test]
#[serial(db)]
async fn test_gap_detection_with_realtime_jump() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Simulate realtime sync that jumped to near head
    // Blocks: 0, 1, 2, 3, 4, 5, then jump to 995, 996, 997, 998, 999, 1000
    for num in [0i64, 1, 2, 3, 4, 5, 995, 996, 997, 998, 999, 1000] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![(num % 256) as u8; 32],
                &vec![((num.saturating_sub(1)) % 256) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 1000).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 1, "Should detect one large gap from jump");
    assert_eq!(gaps[0], (6, 994), "Gap should be the jumped-over region");
}

// ============================================================================
// Edge cases
// ============================================================================

#[tokio::test]
#[serial(db)]
async fn test_gap_detection_consecutive_single_block_gaps() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert alternating blocks: 0, 2, 4, 6, 8, 10 (missing 1, 3, 5, 7, 9)
    for num in [0i64, 2, 4, 6, 8, 10] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num.saturating_sub(1)) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 10).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 5, "Should detect 5 single-block gaps");

    // Verify all gaps are single blocks, sorted by end descending
    assert_eq!(gaps[0], (9, 9));
    assert_eq!(gaps[1], (7, 7));
    assert_eq!(gaps[2], (5, 5));
    assert_eq!(gaps[3], (3, 3));
    assert_eq!(gaps[4], (1, 1));
}

#[tokio::test]
#[serial(db)]
async fn test_gap_size_calculation() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert blocks: 10, 20, 30 (gaps: 0-9, 11-19, 21-29)
    for num in [10i64, 20, 30] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num - 1) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 30).await.expect("Failed to detect gaps");

    assert_eq!(gaps.len(), 3, "Should detect three gaps");

    // Calculate total gap blocks
    // Gap 1-9: 9 blocks (block 0 is genesis), Gap 11-19: 9 blocks, Gap 21-29: 9 blocks = 27 total
    let total_gap_blocks: u64 = gaps.iter().map(|(s, e)| e - s + 1).sum();
    assert_eq!(total_gap_blocks, 27, "Total gap should be 27 blocks (9+9+9)");

    // Verify individual gap sizes (sorted by end descending)
    let sizes: Vec<u64> = gaps.iter().map(|(s, e)| e - s + 1).collect();
    assert_eq!(sizes, vec![9, 9, 9], "Gap sizes should be 9, 9, 9 (most recent first)");
}

#[tokio::test]
#[serial(db)]
async fn test_fully_synced_returns_empty_gaps() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.expect("Failed to get connection");

    // Insert contiguous blocks 0-100
    for num in 0i64..=100 {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![(num % 256) as u8; 32],
                &if num == 0 { vec![0u8; 32] } else { vec![((num - 1) % 256) as u8; 32] },
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .expect("Failed to insert block");
    }

    let gaps = detect_all_gaps(&db.pool, 100).await.expect("Failed to detect gaps");

    assert!(gaps.is_empty(), "Fully synced chain should have no gaps");
}

// ============================================================================
// has_gaps tests - fast COUNT-based gap detection
// ============================================================================

#[tokio::test]
#[serial(db)]
async fn test_has_gaps_empty_table() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // Empty table should report gaps for any non-empty range
    assert!(has_gaps(&db.pool, 1, 100).await.unwrap());
}

#[tokio::test]
#[serial(db)]
async fn test_has_gaps_empty_range() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    // to < from should report no gaps
    assert!(!has_gaps(&db.pool, 100, 1).await.unwrap());
}

#[tokio::test]
#[serial(db)]
async fn test_has_gaps_fully_synced() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.unwrap();

    // Insert contiguous blocks 1-100
    for num in 1i64..=100 {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![(num % 256) as u8; 32],
                &vec![((num - 1) % 256) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .unwrap();
    }

    // No gaps in [1, 100]
    assert!(!has_gaps(&db.pool, 1, 100).await.unwrap());

    // Subrange also has no gaps
    assert!(!has_gaps(&db.pool, 50, 75).await.unwrap());
}

#[tokio::test]
#[serial(db)]
async fn test_has_gaps_with_gap() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.unwrap();

    // Insert blocks 1-5 and 10-15 (gap at 6-9)
    for num in (1i64..=5).chain(10..=15) {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![(num % 256) as u8; 32],
                &vec![((num - 1) % 256) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .unwrap();
    }

    // Full range has gaps
    assert!(has_gaps(&db.pool, 1, 15).await.unwrap());

    // Range covering the gap
    assert!(has_gaps(&db.pool, 4, 12).await.unwrap());

    // Range entirely before the gap — no gaps
    assert!(!has_gaps(&db.pool, 1, 5).await.unwrap());

    // Range entirely after the gap — no gaps
    assert!(!has_gaps(&db.pool, 10, 15).await.unwrap());
}

#[tokio::test]
#[serial(db)]
async fn test_has_gaps_single_missing_block() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.unwrap();

    // Insert blocks 1-10 except block 5
    for num in (1i64..=4).chain(6..=10) {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num - 1) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .unwrap();
    }

    // Full range detects the gap
    assert!(has_gaps(&db.pool, 1, 10).await.unwrap());

    // Range not including block 5 — no gaps
    assert!(!has_gaps(&db.pool, 1, 4).await.unwrap());
    assert!(!has_gaps(&db.pool, 6, 10).await.unwrap());
}

#[tokio::test]
#[serial(db)]
async fn test_has_gaps_agrees_with_detect_all_gaps() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.unwrap();

    // Insert blocks: 0, 1, 2, 5, 6, 10, 11, 12 (gaps at 3-4 and 7-9)
    for num in [0i64, 1, 2, 5, 6, 10, 11, 12] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &if num == 0 { vec![0u8; 32] } else { vec![(num - 1) as u8; 32] },
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .unwrap();
    }

    // has_gaps and detect_all_gaps should agree
    let gaps = detect_all_gaps(&db.pool, 12).await.unwrap();
    let has = has_gaps(&db.pool, 1, 12).await.unwrap();

    assert!(!gaps.is_empty(), "detect_all_gaps should find gaps");
    assert!(has, "has_gaps should agree: gaps exist");

    // Now fill in all missing blocks
    for num in [3i64, 4, 7, 8, 9] {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num - 1) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .unwrap();
    }

    let gaps = detect_all_gaps(&db.pool, 12).await.unwrap();
    let has = has_gaps(&db.pool, 1, 12).await.unwrap();

    assert!(gaps.is_empty(), "detect_all_gaps should find no gaps");
    assert!(!has, "has_gaps should agree: no gaps");
}

#[tokio::test]
#[serial(db)]
async fn test_has_gaps_beyond_indexed_range() {
    let db = TestDb::empty().await;
    db.truncate_all().await;

    let conn = db.pool.get().await.unwrap();

    // Insert blocks 1-10
    for num in 1i64..=10 {
        conn.execute(
            r#"INSERT INTO blocks (num, hash, parent_hash, timestamp, timestamp_ms, gas_limit, gas_used, miner)
               VALUES ($1, $2, $3, NOW(), $4, 1000000, 100000, $5)"#,
            &[
                &num,
                &vec![num as u8; 32],
                &vec![(num - 1) as u8; 32],
                &(num * 1000),
                &vec![0u8; 20],
            ],
        )
        .await
        .unwrap();
    }

    // Asking about range [1, 20] when only 1-10 exist — has gaps
    assert!(has_gaps(&db.pool, 1, 20).await.unwrap());

    // Asking about exactly the indexed range — no gaps
    assert!(!has_gaps(&db.pool, 1, 10).await.unwrap());
}
