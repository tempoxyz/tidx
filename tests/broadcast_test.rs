mod common;

use std::sync::Arc;
use std::time::Duration;

use tidx::broadcast::{BlockUpdate, Broadcaster};

#[tokio::test]
async fn test_broadcaster_send_receive() {
    let broadcaster = Broadcaster::new();
    let mut rx = broadcaster.subscribe();

    let update = BlockUpdate {
        chain_id: 4217,
        block_num: 100,
        block_hash: "0xabc123".to_string(),
        tx_count: 5,
        log_count: 10,
        timestamp: 1234567890,
    };

    broadcaster.send(update.clone());

    let received = tokio::time::timeout(Duration::from_millis(100), rx.recv())
        .await
        .expect("timeout waiting for message")
        .expect("channel closed");

    assert_eq!(received.chain_id, 4217);
    assert_eq!(received.block_num, 100);
    assert_eq!(received.tx_count, 5);
}

#[tokio::test]
async fn test_broadcaster_multiple_subscribers() {
    let broadcaster = Arc::new(Broadcaster::new());
    let mut rx1 = broadcaster.subscribe();
    let mut rx2 = broadcaster.subscribe();

    assert_eq!(broadcaster.receiver_count(), 2);

    let update = BlockUpdate {
        chain_id: 4217,
        block_num: 200,
        block_hash: "0xdef456".to_string(),
        tx_count: 3,
        log_count: 7,
        timestamp: 1234567891,
    };

    broadcaster.send(update);

    let recv1 = rx1.recv().await.expect("rx1 should receive");
    let recv2 = rx2.recv().await.expect("rx2 should receive");

    assert_eq!(recv1.block_num, 200);
    assert_eq!(recv2.block_num, 200);
}

#[tokio::test]
async fn test_broadcaster_lagged_receiver() {
    let broadcaster = Broadcaster::new();
    let mut rx = broadcaster.subscribe();

    // Send more messages than channel capacity (256)
    for i in 0..300 {
        broadcaster.send(BlockUpdate {
            chain_id: 4217,
            block_num: i,
            block_hash: format!("0x{i:064x}"),
            tx_count: 1,
            log_count: 1,
            timestamp: 1234567890 + i as i64,
        });
    }

    // First recv should report lagged
    match rx.recv().await {
        Err(tokio::sync::broadcast::error::RecvError::Lagged(n)) => {
            assert!(n > 0, "should report skipped messages");
        }
        Ok(_) => panic!("expected lagged error"),
        Err(e) => panic!("unexpected error: {e:?}"),
    }
}

#[tokio::test]
async fn test_broadcaster_no_receivers() {
    let broadcaster = Broadcaster::new();

    // Sending without receivers should not panic
    broadcaster.send(BlockUpdate {
        chain_id: 4217,
        block_num: 1,
        block_hash: "0x123".to_string(),
        tx_count: 0,
        log_count: 0,
        timestamp: 0,
    });

    assert_eq!(broadcaster.receiver_count(), 0);
}
