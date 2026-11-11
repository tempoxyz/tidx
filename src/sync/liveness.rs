//! Realtime sync liveness, for the `/health` probe.
//!
//! The realtime loop records progress every time it completes a tick or
//! commits a batch, including ticks that find no new block, so a halted chain
//! still counts as live. When a chain records nothing for [`STALL_AFTER`], the
//! loop is stuck (for example on a database connection that will never
//! answer) and `/health` fails so the orchestrator restarts the process.
//!
//! A process that never runs realtime sync (`tidx serve`, or `tidx up` still
//! in its backfill-first phase) records nothing and always reports healthy.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

/// How long a chain may go without realtime progress before `/health` fails.
pub const STALL_AFTER: Duration = Duration::from_secs(120);

static GLOBAL: LazyLock<Liveness> = LazyLock::new(Liveness::default);

/// The process-wide liveness tracker.
pub fn global() -> &'static Liveness {
    &GLOBAL
}

/// Records realtime progress for `chain_id` in the process-wide tracker.
pub fn record_progress(chain_id: u64) {
    GLOBAL.record_at(chain_id, Instant::now());
}

/// Last realtime progress per chain.
#[derive(Debug, Default)]
pub struct Liveness {
    last_progress: Mutex<HashMap<u64, Instant>>,
}

impl Liveness {
    /// Records progress for `chain_id` at `at`.
    pub fn record_at(&self, chain_id: u64, at: Instant) {
        self.lock().insert(chain_id, at);
    }

    /// Chains whose last progress is older than `threshold` at `now`, with
    /// how long they have been silent, sorted by chain id.
    pub fn stalled_at(&self, now: Instant, threshold: Duration) -> Vec<(u64, Duration)> {
        let mut stalled: Vec<_> = self
            .lock()
            .iter()
            .filter_map(|(&chain_id, &at)| {
                let silent = now.saturating_duration_since(at);
                (silent > threshold).then_some((chain_id, silent))
            })
            .collect();
        stalled.sort_unstable_by_key(|(chain_id, _)| *chain_id);
        stalled
    }

    /// Chains silent for longer than [`STALL_AFTER`] now.
    pub fn stalled(&self) -> Vec<(u64, Duration)> {
        self.stalled_at(Instant::now(), STALL_AFTER)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Instant>> {
        // The map holds plain timestamps, so a poisoned lock is still usable.
        self.last_progress
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_untracked_process_is_live() {
        let liveness = Liveness::default();
        assert_eq!(liveness.stalled_at(Instant::now(), STALL_AFTER), vec![]);
    }

    #[test]
    fn test_recent_progress_is_live() {
        let liveness = Liveness::default();
        let start = Instant::now();
        liveness.record_at(4217, start);
        assert_eq!(
            liveness.stalled_at(start + Duration::from_secs(119), STALL_AFTER),
            vec![]
        );
    }

    #[test]
    fn test_silent_chain_is_stalled() {
        let liveness = Liveness::default();
        let start = Instant::now();
        liveness.record_at(4217, start);
        liveness.record_at(42431, start + Duration::from_secs(100));
        assert_eq!(
            liveness.stalled_at(start + Duration::from_secs(150), STALL_AFTER),
            vec![(4217, Duration::from_secs(150))]
        );
    }

    #[test]
    fn test_progress_after_stall_recovers() {
        let liveness = Liveness::default();
        let start = Instant::now();
        liveness.record_at(4217, start);
        let later = start + Duration::from_secs(300);
        assert_eq!(liveness.stalled_at(later, STALL_AFTER).len(), 1);
        liveness.record_at(4217, later);
        assert_eq!(liveness.stalled_at(later, STALL_AFTER), vec![]);
    }
}
