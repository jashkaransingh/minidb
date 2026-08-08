//! MVCC and snapshot isolation.
//!
//! The visibility rule under test, stated once (it is also in the crate docs):
//!
//! > A read at snapshot `S` resolves `key` to the version of `key` with the
//! > greatest sequence number `<= S`, searching the memtable first and then the
//! > on-disk tables newest-first. A tombstone found that way means absent and
//! > stops the search. Versions with sequence number `> S` are invisible.
//!
//! The cases that matter are the ones where a version crosses a *boundary*:
//! from the memtable into an SSTable on flush, and from several SSTables into
//! one on compaction. A snapshot must be unmoved by either.

use std::fs;
use std::path::{Path, PathBuf};

use minidb::{CompactionConfig, Db, DbOptions, Entry, SsTable};

/// A scratch store directory that removes itself on drop.
struct TempStore(PathBuf);

impl TempStore {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("minidb-mvcc-{tag}"));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Options that flush often, so tests cross the memtable→SSTable boundary.
fn eager_flush(threshold: usize) -> DbOptions {
    DbOptions {
        flush_threshold_bytes: threshold,
        ..DbOptions::default()
    }
}

#[test]
fn a_snapshot_sees_none_of_the_writes_that_land_after_it() {
    let mut db = Db::new();
    for i in 0..50u32 {
        db.put(format!("k{i:03}").as_bytes(), b"original").unwrap();
    }

    let snap = db.snapshot();

    // Hammer every key with new values while the snapshot is open.
    for round in 0..5u32 {
        for i in 0..50u32 {
            let value = format!("round-{round}");
            db.put(format!("k{i:03}").as_bytes(), value.as_bytes())
                .unwrap();
        }
    }

    for i in 0..50u32 {
        let key = format!("k{i:03}");
        assert_eq!(
            db.get_at(&snap, key.as_bytes()).unwrap(),
            Some(b"original".to_vec()),
            "{key} moved under an open snapshot"
        );
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(b"round-4".to_vec()),
            "{key} should be current outside the snapshot"
        );
    }
}

#[test]
fn a_snapshot_survives_the_memtable_being_flushed_to_disk() {
    let store = TempStore::new("flush-boundary");
    let mut db = Db::open_with_options(store.path(), eager_flush(4 * 1024 * 1024)).unwrap();

    db.put(b"k", b"v1").unwrap();
    let snap = db.snapshot();
    db.put(b"k", b"v2").unwrap();

    // Both versions move from the memtable into one SSTable together.
    db.flush().unwrap();
    assert_eq!(db.sstable_count(), 1);
    assert!(db.memtable().is_empty(), "the memtable was drained");

    assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn a_snapshot_survives_versions_being_split_across_two_tables() {
    let store = TempStore::new("split-across-tables");
    let mut db = Db::open_with_options(store.path(), eager_flush(4 * 1024 * 1024)).unwrap();

    db.put(b"k", b"v1").unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();

    db.put(b"k", b"v2").unwrap();
    db.flush().unwrap();
    assert_eq!(
        db.sstable_count(),
        2,
        "the versions live in separate tables"
    );

    // The newest table holds only v2, which is invisible to the snapshot; the
    // search has to fall through to the older table rather than report absent.
    assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v1".to_vec()));
    assert_eq!(db.get(b"k").unwrap(), Some(b"v2".to_vec()));
}

#[test]
fn an_open_snapshot_stops_compaction_from_collecting_what_it_reads() {
    let store = TempStore::new("compaction-pin");
    let options = DbOptions {
        flush_threshold_bytes: 4 * 1024 * 1024,
        compaction: CompactionConfig {
            min_merge_width: 2,
            ..CompactionConfig::default()
        },
        auto_compact: false,
        ..DbOptions::default()
    };
    let mut db = Db::open_with_options(store.path(), options).unwrap();

    db.put(b"k", b"old").unwrap();
    db.flush().unwrap();

    let snap = db.snapshot();

    for round in 0..4u32 {
        db.put(b"k", format!("new-{round}").as_bytes()).unwrap();
        db.flush().unwrap();
    }

    // Merge everything. The snapshot pins "old", so it must survive the merge.
    let rounds = db.compact_all().unwrap();
    assert!(rounds > 0, "something should have been merged");

    assert_eq!(
        db.get_at(&snap, b"k").unwrap(),
        Some(b"old".to_vec()),
        "compaction collected a version an open snapshot could still read"
    );
    assert_eq!(db.get(b"k").unwrap(), Some(b"new-3".to_vec()));
}

#[test]
fn compaction_collects_old_versions_once_no_snapshot_holds_them() {
    let store = TempStore::new("collect-versions");
    let options = DbOptions {
        flush_threshold_bytes: 4 * 1024 * 1024,
        compaction: CompactionConfig {
            min_merge_width: 2,
            ..CompactionConfig::default()
        },
        auto_compact: false,
        ..DbOptions::default()
    };
    let mut db = Db::open_with_options(store.path(), options).unwrap();

    // Ten versions of one key, each in its own table.
    for round in 0..10u32 {
        db.put(b"k", format!("v{round}").as_bytes()).unwrap();
        db.flush().unwrap();
    }
    assert_eq!(db.sstable_count(), 10);

    db.compact_all().unwrap();

    let total: u64 = db.tables().iter().map(|t| t.len()).sum();
    assert_eq!(
        total, 1,
        "with no snapshot open only the newest version is reachable, \
         so exactly one should survive (found {total})"
    );
    assert_eq!(db.get(b"k").unwrap(), Some(b"v9".to_vec()));
}

#[test]
fn a_deleted_key_is_still_readable_at_a_snapshot_taken_before_the_delete() {
    let store = TempStore::new("delete-boundary");
    let mut db = Db::open_with_options(store.path(), eager_flush(4 * 1024 * 1024)).unwrap();

    db.put(b"k", b"alive").unwrap();
    let snap = db.snapshot();
    db.delete(b"k").unwrap();
    db.flush().unwrap();

    assert_eq!(db.get(b"k").unwrap(), None, "deleted for current readers");
    assert_eq!(
        db.get_at(&snap, b"k").unwrap(),
        Some(b"alive".to_vec()),
        "the snapshot predates the tombstone"
    );
}

#[test]
fn a_tombstone_stops_the_search_rather_than_falling_through_to_an_older_table() {
    // The classic LSM resurrection bug, checked directly: the value is in an
    // older table, the tombstone in a newer one.
    let store = TempStore::new("tombstone-stops");
    let mut db = Db::open_with_options(store.path(), eager_flush(4 * 1024 * 1024)).unwrap();

    db.put(b"k", b"v").unwrap();
    db.flush().unwrap();
    db.delete(b"k").unwrap();
    db.flush().unwrap();
    assert_eq!(db.sstable_count(), 2);

    assert_eq!(db.get(b"k").unwrap(), None);

    // And across a reopen, which rebuilds the table list from scratch.
    drop(db);
    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), None);
}

#[test]
fn sequence_numbers_continue_from_where_a_reopen_found_them() {
    let store = TempStore::new("seq-recovery");

    let last_seq = {
        let mut db = Db::open_with_options(store.path(), eager_flush(4 * 1024 * 1024)).unwrap();
        for i in 0..20u32 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        // Flush so the log is empty and the tables are the only record of how
        // far the counter got.
        db.flush().unwrap();
        assert_eq!(db.wal_size_bytes(), 0, "the log was rotated");
        db.current_seq()
    };
    assert_eq!(last_seq, 20);

    let mut db = Db::open(store.path()).unwrap();
    assert_eq!(
        db.current_seq(),
        last_seq,
        "the counter must resume from the tables, not restart at zero"
    );

    // A restart that reused sequence numbers would write a version that sorts
    // *above* an existing one for the same key, silently shadowing it.
    db.put(b"k0", b"after-restart").unwrap();
    assert!(db.current_seq() > last_seq);
    assert_eq!(db.get(b"k0").unwrap(), Some(b"after-restart".to_vec()));
}

#[test]
fn sequence_numbers_survive_a_reopen_that_replays_the_log() {
    let store = TempStore::new("seq-replay");

    {
        let mut db = Db::open(store.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();
        assert_eq!(db.current_seq(), 2);
    }

    let mut db = Db::open(store.path()).unwrap();
    assert_eq!(db.current_seq(), 2, "recovered from the log");
    db.put(b"c", b"3").unwrap();
    assert_eq!(db.current_seq(), 3);
}

#[test]
fn every_version_reaches_disk_in_internal_key_order() {
    // The flush path writes user key ascending, sequence descending. Anything
    // else and the reader's binary search silently returns the wrong version.
    let store = TempStore::new("on-disk-order");
    let mut db = Db::open_with_options(store.path(), eager_flush(4 * 1024 * 1024)).unwrap();

    for round in 0..3u32 {
        for key in ["a", "b", "c"] {
            db.put(key.as_bytes(), format!("r{round}").as_bytes())
                .unwrap();
        }
    }
    let path = db.flush().unwrap().expect("a table");

    let table = SsTable::open(&path).unwrap();
    let entries: Vec<(String, u64)> = table
        .iter()
        .unwrap()
        .map(|r| r.unwrap())
        .map(|(k, seq, _)| (String::from_utf8(k).unwrap(), seq))
        .collect();

    assert_eq!(entries.len(), 9);
    for pair in entries.windows(2) {
        let ((ka, sa), (kb, sb)) = (&pair[0], &pair[1]);
        assert!(
            ka < kb || (ka == kb && sa > sb),
            "out of internal-key order: ({ka}, {sa}) then ({kb}, {sb})"
        );
    }
}

#[test]
fn an_empty_value_is_not_a_tombstone_at_any_snapshot() {
    let store = TempStore::new("empty-value");
    let mut db = Db::open_with_options(store.path(), eager_flush(4 * 1024 * 1024)).unwrap();

    db.put(b"k", b"").unwrap();
    let snap = db.snapshot();
    db.flush().unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(Vec::new()));
    assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(Vec::new()));

    let table = SsTable::open(db.tables()[0].path()).unwrap();
    assert_eq!(
        table.get_latest(b"k").unwrap(),
        Some(Entry::Value(Vec::new())),
        "an empty value must not be stored as a deletion"
    );
}

#[test]
fn many_snapshots_at_different_points_each_read_their_own_past() {
    let mut db = Db::new();
    let mut snaps = Vec::new();

    for round in 0..10u32 {
        db.put(b"k", format!("v{round}").as_bytes()).unwrap();
        snaps.push((round, db.snapshot()));
    }

    for (round, snap) in &snaps {
        assert_eq!(
            db.get_at(snap, b"k").unwrap(),
            Some(format!("v{round}").into_bytes()),
            "snapshot {round} drifted"
        );
    }
}

#[test]
fn dropping_a_snapshot_lets_compaction_reclaim_again() {
    let store = TempStore::new("snapshot-release");
    let options = DbOptions {
        flush_threshold_bytes: 4 * 1024 * 1024,
        compaction: CompactionConfig {
            min_merge_width: 2,
            ..CompactionConfig::default()
        },
        auto_compact: false,
        ..DbOptions::default()
    };
    let mut db = Db::open_with_options(store.path(), options).unwrap();

    db.put(b"k", b"pinned").unwrap();
    db.flush().unwrap();
    let snap = db.snapshot();

    for round in 0..4u32 {
        db.put(b"k", format!("v{round}").as_bytes()).unwrap();
        db.flush().unwrap();
    }
    db.compact_all().unwrap();

    let held: u64 = db.tables().iter().map(|t| t.len()).sum();
    assert!(held > 1, "the open snapshot should hold versions back");

    drop(snap);
    assert_eq!(db.snapshots().oldest(), None);

    // Force another merge now that nothing is pinned.
    for round in 4..8u32 {
        db.put(b"k", format!("v{round}").as_bytes()).unwrap();
        db.flush().unwrap();
    }
    db.compact_all().unwrap();

    let after: u64 = db.tables().iter().map(|t| t.len()).sum();
    assert_eq!(
        after, 1,
        "with the snapshot dropped, only the newest version should remain \
         (found {after})"
    );
}

#[test]
fn a_mixed_workload_is_correct_at_both_a_snapshot_and_the_present() {
    let store = TempStore::new("mixed");
    let mut db = Db::open_with_options(store.path(), eager_flush(2 * 1024)).unwrap();

    // 300 keys written, then a third overwritten and a fifth deleted, with a
    // snapshot taken in between. Small flush threshold, so this spans many
    // tables and several compactions.
    for i in 0..300u32 {
        db.put(format!("key:{i:04}").as_bytes(), b"first").unwrap();
    }

    let snap = db.snapshot();

    for i in (0..300u32).step_by(3) {
        db.put(format!("key:{i:04}").as_bytes(), b"second").unwrap();
    }
    for i in (0..300u32).step_by(5) {
        db.delete(format!("key:{i:04}").as_bytes()).unwrap();
    }

    for i in 0..300u32 {
        let key = format!("key:{i:04}");
        assert_eq!(
            db.get_at(&snap, key.as_bytes()).unwrap(),
            Some(b"first".to_vec()),
            "{key} at the snapshot"
        );

        let expected = if i % 5 == 0 {
            None
        } else if i % 3 == 0 {
            Some(b"second".to_vec())
        } else {
            Some(b"first".to_vec())
        };
        assert_eq!(db.get(key.as_bytes()).unwrap(), expected, "{key} now");
    }

    // And the same after a reopen, which rebuilds everything from disk.
    let snapshot_view = db.scan_at(&snap).unwrap();
    assert_eq!(snapshot_view.len(), 300);
    drop(db);

    let db = Db::open(store.path()).unwrap();
    for i in 0..300u32 {
        let key = format!("key:{i:04}");
        let expected = if i % 5 == 0 {
            None
        } else if i % 3 == 0 {
            Some(b"second".to_vec())
        } else {
            Some(b"first".to_vec())
        };
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            expected,
            "{key} after reopen"
        );
    }
}
