//! Point-in-time read snapshots.
//!
//! # What a snapshot is
//!
//! A [`Snapshot`] is a sequence number and a registration. The sequence number
//! is captured at the instant the snapshot is taken and never moves, so every
//! read through it resolves against the same point in time — see the visibility
//! rule in the crate docs.
//!
//! # Why it needs a registry
//!
//! Reading old versions only works while those versions still exist, and the one
//! thing that deletes them is compaction. So the store has to know which parts
//! of the past are still spoken for.
//!
//! [`SnapshotRegistry`] is that bookkeeping: a multiset of live sequence
//! numbers. Compaction asks it for the [`oldest`](SnapshotRegistry::oldest) one
//! and keeps every version at or above it, plus one version beneath it per key —
//! enough for the oldest reader to resolve any key. A snapshot deregisters on
//! `Drop`, so the floor rises again as readers finish.
//!
//! Several snapshots can share a sequence number (nothing was written between
//! them), which is why the registry counts rather than just storing a set.
//!
//! # The cost, stated plainly
//!
//! A snapshot that is held forever pins every version written after it: space
//! amplification grows without bound and compaction can reclaim nothing beneath
//! it. That is inherent to MVCC, not a defect here, but it does mean a snapshot
//! is a resource and should be dropped promptly.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

/// Live snapshot sequence numbers, counted.
///
/// Cheap to share: compaction only ever reads [`oldest`](Self::oldest), and
/// acquire/release touch one `BTreeMap` entry.
#[derive(Debug, Default)]
pub struct SnapshotRegistry {
    /// Sequence number → number of live snapshots holding it.
    live: Mutex<BTreeMap<u64, usize>>,
}

impl SnapshotRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers interest in `seq`.
    fn acquire(&self, seq: u64) {
        *self.lock().entry(seq).or_insert(0) += 1;
    }

    /// Releases one registration of `seq`.
    fn release(&self, seq: u64) {
        let mut live = self.lock();
        if let Some(count) = live.get_mut(&seq) {
            *count -= 1;
            if *count == 0 {
                live.remove(&seq);
            }
        }
    }

    /// The lowest sequence number any live snapshot is pinned to.
    ///
    /// `None` when nothing is pinned, which lets compaction collect every
    /// superseded version.
    pub fn oldest(&self) -> Option<u64> {
        self.lock().keys().next().copied()
    }

    /// Number of live snapshots.
    pub fn live_count(&self) -> usize {
        self.lock().values().sum()
    }

    /// Locks the map, recovering from poisoning.
    ///
    /// A panic elsewhere must not make the store permanently unreadable. The
    /// map's invariants do not span operations — every mutation is a single
    /// entry update — so the state behind a poisoned lock is still coherent.
    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<u64, usize>> {
        self.live.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// A pinned point in time.
///
/// Reads taken through this handle see exactly the writes acknowledged before it
/// was created. Dropping it releases the versions it was holding back.
#[derive(Debug)]
pub struct Snapshot {
    seq: u64,
    registry: Arc<SnapshotRegistry>,
}

impl Snapshot {
    /// Registers a snapshot at `seq`.
    pub fn acquire(registry: Arc<SnapshotRegistry>, seq: u64) -> Self {
        registry.acquire(seq);
        Self { seq, registry }
    }

    /// The sequence number this snapshot reads at.
    pub fn seq(&self) -> u64 {
        self.seq
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        self.registry.release(self.seq);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_registry_pins_nothing() {
        let registry = SnapshotRegistry::new();
        assert_eq!(registry.oldest(), None);
        assert_eq!(registry.live_count(), 0);
    }

    #[test]
    fn the_oldest_live_snapshot_is_the_floor() {
        let registry = Arc::new(SnapshotRegistry::new());
        let a = Snapshot::acquire(Arc::clone(&registry), 10);
        let b = Snapshot::acquire(Arc::clone(&registry), 3);
        let c = Snapshot::acquire(Arc::clone(&registry), 7);

        assert_eq!(registry.oldest(), Some(3));
        assert_eq!(registry.live_count(), 3);

        drop(b);
        assert_eq!(registry.oldest(), Some(7), "the floor rises as readers end");
        drop(c);
        assert_eq!(registry.oldest(), Some(10));
        drop(a);
        assert_eq!(registry.oldest(), None);
    }

    #[test]
    fn snapshots_sharing_a_sequence_number_are_counted_not_deduplicated() {
        let registry = Arc::new(SnapshotRegistry::new());
        let a = Snapshot::acquire(Arc::clone(&registry), 5);
        let b = Snapshot::acquire(Arc::clone(&registry), 5);
        assert_eq!(registry.live_count(), 2);

        drop(a);
        assert_eq!(
            registry.oldest(),
            Some(5),
            "one reader remains at this sequence"
        );
        drop(b);
        assert_eq!(registry.oldest(), None);
    }

    #[test]
    fn a_snapshot_reports_the_sequence_it_was_taken_at() {
        let registry = Arc::new(SnapshotRegistry::new());
        let snap = Snapshot::acquire(registry, 42);
        assert_eq!(snap.seq(), 42);
    }

    #[test]
    fn the_registry_survives_a_panicking_holder() {
        let registry = Arc::new(SnapshotRegistry::new());
        let clone = Arc::clone(&registry);
        let _ = std::thread::spawn(move || {
            let _snap = Snapshot::acquire(clone, 1);
            panic!("holder died");
        })
        .join();

        // The snapshot's Drop still ran during unwinding, and the lock is usable.
        assert_eq!(registry.oldest(), None);
        assert_eq!(registry.live_count(), 0);
    }
}
