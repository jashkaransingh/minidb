//! Integration tests for size-tiered compaction.
//!
//! The properties under test, in rough order of how badly they fail when wrong:
//!
//! 1. **No resurrection** — a deleted key must never come back, even after the
//!    tombstone that hid it has been dropped.
//! 2. **No reversion** — a merged table must not shadow newer tables outside
//!    the merge and revert their values.
//! 3. **Crash safety** — an interrupted swap must leave the store consistent.
//! 4. Space and table count actually go down.

use std::fs;
use std::path::{Path, PathBuf};

use minidb::compaction::{CompactionTask, Marker, TableInfo, plan};
use minidb::{COMPACTION_MARKER, CompactionConfig, Db, DbOptions, FaultPlan, SyncPolicy};

struct TempStore(PathBuf);

impl TempStore {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("minidb-compaction-{tag}"));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    /// Opens with compaction disabled, so tables can be staged deliberately.
    fn open_manual(&self) -> Db {
        self.open_with(false, CompactionConfig::default())
    }

    fn open_auto(&self) -> Db {
        self.open_with(true, CompactionConfig::default())
    }

    fn open_with(&self, auto_compact: bool, compaction: CompactionConfig) -> Db {
        Db::open_with_options(
            &self.0,
            DbOptions {
                sync_policy: SyncPolicy::EveryWrite,
                flush_threshold_bytes: usize::MAX, // flush only when asked
                compaction,
                auto_compact,
                fault: FaultPlan::none(),
            },
        )
        .unwrap()
    }

    fn sst_files(&self) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(&self.0)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".sst"))
            .collect();
        names.sort();
        names
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn compaction_merges_tables_and_preserves_every_live_value() {
    let store = TempStore::new("basic");
    let db = store.open_manual();

    for table in 0..4u32 {
        for i in 0..50u32 {
            let key = format!("key:{:04}", table * 50 + i);
            db.put(key.as_bytes(), format!("v{}", table * 50 + i).as_bytes())
                .unwrap();
        }
        db.flush().unwrap();
    }
    assert_eq!(db.sstable_count(), 4);

    assert!(db.compact().unwrap(), "four equal tables should merge");
    assert_eq!(db.sstable_count(), 1, "four tables became one");

    for i in 0..200u32 {
        let key = format!("key:{i:04}");
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(format!("v{i}").into_bytes()),
            "{key} lost across compaction"
        );
    }
    assert_eq!(db.len().unwrap(), 200);
}

#[test]
fn a_deleted_key_never_comes_back_after_compaction() {
    // The resurrection bug: if a tombstone is dropped while an older table
    // still holds the value it was hiding, the delete silently un-happens.
    let store = TempStore::new("no-resurrection");
    let db = store.open_manual();

    db.put(b"victim", b"original").unwrap();
    db.put(b"bystander", b"kept").unwrap();
    db.flush().unwrap();

    db.delete(b"victim").unwrap();
    db.flush().unwrap();

    db.put(b"filler-1", b"x").unwrap();
    db.flush().unwrap();
    db.put(b"filler-2", b"x").unwrap();
    db.flush().unwrap();

    assert_eq!(
        db.get(b"victim").unwrap(),
        None,
        "deleted before compaction"
    );

    db.compact_all().unwrap();
    assert_eq!(
        db.get(b"victim").unwrap(),
        None,
        "the delete must survive compaction"
    );

    // And after a reopen, reading purely from the merged table.
    drop(db);
    let db = store.open_manual();
    assert_eq!(
        db.get(b"victim").unwrap(),
        None,
        "still deleted after reopen"
    );
    assert_eq!(db.get(b"bystander").unwrap(), Some(b"kept".to_vec()));
}

#[test]
fn tombstones_survive_a_merge_that_excludes_older_tables() {
    // Table 0 is large and stays out of the merge, so it may still hold the
    // value the tombstone hides. Dropping the tombstone here would resurrect it.
    let store = TempStore::new("tombstone-kept");
    let db = store.open_manual();

    db.put(b"victim", b"original").unwrap();
    for i in 0..400u32 {
        db.put(format!("bulk:{i:04}").as_bytes(), &[b'x'; 200])
            .unwrap();
    }
    db.flush().unwrap(); // a big table 0

    db.delete(b"victim").unwrap();
    db.flush().unwrap();
    for i in 0..3u32 {
        db.put(format!("small:{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }

    let infos: Vec<TableInfo> = db
        .tables()
        .iter()
        .zip(db.table_slots())
        .map(|(t, (seq, generation))| TableInfo {
            path: t.path().to_path_buf(),
            seq,
            generation,
            size_bytes: t.size_bytes(),
        })
        .collect();

    let task = plan(&infos, &CompactionConfig::default()).expect("small tables should merge");
    assert!(
        !task.drop_tombstones,
        "table 0 is older and excluded, so tombstones must be preserved"
    );

    db.compact_all().unwrap();
    assert_eq!(
        db.get(b"victim").unwrap(),
        None,
        "victim must stay deleted; the big table still holds its old value"
    );
}

#[test]
fn compaction_keeps_the_newest_value_for_a_repeatedly_written_key() {
    let store = TempStore::new("newest-wins");
    let db = store.open_manual();

    for v in 1..=4u32 {
        db.put(b"hot", format!("v{v}").as_bytes()).unwrap();
        db.flush().unwrap();
    }

    assert_eq!(db.get(b"hot").unwrap(), Some(b"v4".to_vec()));
    db.compact_all().unwrap();
    assert_eq!(
        db.get(b"hot").unwrap(),
        Some(b"v4".to_vec()),
        "the merge must keep the newest version, not an arbitrary one"
    );
    assert_eq!(db.len().unwrap(), 1, "older versions are reclaimed");
}

#[test]
fn a_merged_table_does_not_shadow_newer_tables_outside_the_merge() {
    // The reversion bug: if the output took a recency slot above tables that
    // were not merged, it would revert their newer values.
    let store = TempStore::new("no-reversion");
    let db = store.open_manual();

    // Four small tables that will merge, all writing key "k".
    for v in 1..=4u32 {
        db.put(b"k", format!("old{v}").as_bytes()).unwrap();
        db.flush().unwrap();
    }
    // A newer table with the current value, large enough to sit in its own tier.
    db.put(b"k", b"NEWEST").unwrap();
    for i in 0..400u32 {
        db.put(format!("bulk:{i:04}").as_bytes(), &[b'y'; 200])
            .unwrap();
    }
    db.flush().unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(b"NEWEST".to_vec()));

    db.compact_all().unwrap();
    assert_eq!(
        db.get(b"k").unwrap(),
        Some(b"NEWEST".to_vec()),
        "merging the old tables must not revert k"
    );

    drop(db);
    let db = store.open_manual();
    assert_eq!(
        db.get(b"k").unwrap(),
        Some(b"NEWEST".to_vec()),
        "after reopen"
    );
}

#[test]
fn compaction_reclaims_space_from_superseded_values() {
    let store = TempStore::new("reclaim");
    let db = store.open_manual();

    // The same 100 keys rewritten across four tables: 400 entries, 100 live.
    for round in 0..4u32 {
        for i in 0..100u32 {
            db.put(
                format!("key:{i:04}").as_bytes(),
                format!("round{round}-{:0>200}", i).as_bytes(),
            )
            .unwrap();
        }
        db.flush().unwrap();
    }

    let before: u64 = db.tables().iter().map(|t| t.size_bytes()).sum();
    db.compact_all().unwrap();
    let after: u64 = db.tables().iter().map(|t| t.size_bytes()).sum();

    assert_eq!(db.sstable_count(), 1);
    assert!(
        after * 2 < before,
        "expected superseded copies to be reclaimed: {before} -> {after}"
    );
    assert_eq!(db.len().unwrap(), 100);
    assert_eq!(db.tables()[0].meta().num_entries, 100);
}

#[test]
fn tombstones_are_dropped_when_the_oldest_table_participates() {
    let store = TempStore::new("drop-tombstones");
    let db = store.open_manual();

    for i in 0..4u32 {
        db.put(format!("key:{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }
    // Delete them all, in tables that will merge together with the oldest.
    for i in 0..4u32 {
        db.delete(format!("key:{i}").as_bytes()).unwrap();
    }
    db.flush().unwrap();

    db.compact_all().unwrap();

    assert_eq!(db.len().unwrap(), 0);
    assert_eq!(db.sstable_count(), 1);
    assert_eq!(
        db.tables()[0].meta().num_tombstones,
        0,
        "with the oldest table merged, tombstones have nothing left to hide"
    );
}

#[test]
fn auto_compaction_bounds_the_table_count() {
    let store = TempStore::new("auto");
    let db = store.open_auto();

    for round in 0..30u32 {
        for i in 0..20u32 {
            db.put(format!("key:{round:03}:{i:03}").as_bytes(), b"value")
                .unwrap();
        }
        db.flush().unwrap();
    }

    assert!(
        db.sstable_count() < 10,
        "auto compaction should keep the table count bounded, got {}",
        db.sstable_count()
    );

    // Everything is still readable.
    for round in 0..30u32 {
        for i in 0..20u32 {
            let key = format!("key:{round:03}:{i:03}");
            assert_eq!(
                db.get(key.as_bytes()).unwrap(),
                Some(b"value".to_vec()),
                "{key}"
            );
        }
    }
    assert_eq!(db.len().unwrap(), 600);
}

#[test]
fn nothing_is_compacted_below_the_minimum_width() {
    let store = TempStore::new("below-threshold");
    let db = store.open_manual();

    for i in 0..3u32 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }
    assert!(
        !db.compact().unwrap(),
        "three tables is below min_merge_width"
    );
    assert_eq!(db.sstable_count(), 3);
}

#[test]
fn an_interrupted_compaction_with_a_published_output_is_completed_on_open() {
    let store = TempStore::new("recover-forward");
    let db = store.open_manual();

    for i in 0..4u32 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }
    db.compact_all().unwrap();
    let merged = store.sst_files();
    assert_eq!(merged.len(), 1);
    drop(db);

    // Simulate a crash after the output was published but before the inputs
    // were deleted: recreate the inputs and re-plant the marker.
    let output = store.path().join(&merged[0]);
    let mut inputs = Vec::new();
    for i in 0..4u32 {
        let stale = store.path().join(format!("{i:010}-0000.sst"));
        fs::copy(&output, &stale).unwrap();
        inputs.push(stale);
    }
    Marker {
        output: output.clone(),
        inputs: inputs.clone(),
    }
    .write(store.path())
    .unwrap();

    let db = store.open_manual();
    for input in &inputs {
        assert!(!input.exists(), "stale inputs must be deleted on recovery");
    }
    assert!(
        !store.path().join(COMPACTION_MARKER).exists(),
        "marker cleared"
    );
    assert_eq!(db.sstable_count(), 1);
    for i in 0..4u32 {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).unwrap(),
            Some(b"v".to_vec())
        );
    }
}

#[test]
fn an_interrupted_compaction_with_no_output_rolls_back_on_open() {
    let store = TempStore::new("recover-back");
    let db = store.open_manual();

    for i in 0..4u32 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }
    let inputs: Vec<PathBuf> = db.tables().iter().map(|t| t.path().to_path_buf()).collect();
    drop(db);

    // A marker naming an output that was never published, plus a partial temp.
    let output = store.path().join("0000000003-0001.sst");
    let tmp = store.path().join("0000000003-0001.sst.tmp");
    fs::write(&tmp, b"half-written garbage").unwrap();
    Marker {
        output: output.clone(),
        inputs: inputs.clone(),
    }
    .write(store.path())
    .unwrap();

    let db = store.open_manual();
    assert!(!tmp.exists(), "partial output discarded");
    assert!(!output.exists(), "output was never published");
    assert!(
        !store.path().join(COMPACTION_MARKER).exists(),
        "marker cleared"
    );
    assert_eq!(db.sstable_count(), 4, "inputs kept");
    for i in 0..4u32 {
        assert_eq!(
            db.get(format!("k{i}").as_bytes()).unwrap(),
            Some(b"v".to_vec()),
            "no data lost by the rollback"
        );
    }
}

#[test]
fn a_workload_of_writes_overwrites_and_deletes_stays_correct_through_compaction() {
    let store = TempStore::new("workload");
    let db = store.open_auto();

    // Round 1: write 300 keys across several tables.
    for i in 0..300u32 {
        db.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
        if i % 50 == 49 {
            db.flush().unwrap();
        }
    }
    // Round 2: overwrite every third, delete every fifth.
    for i in (0..300u32).step_by(3) {
        db.put(format!("k{i:04}").as_bytes(), b"updated").unwrap();
    }
    for i in (0..300u32).step_by(5) {
        db.delete(format!("k{i:04}").as_bytes()).unwrap();
    }
    db.flush().unwrap();
    db.compact_all().unwrap();
    drop(db);

    let db = store.open_auto();
    let mut expected_live = 0;
    for i in 0..300u32 {
        let key = format!("k{i:04}");
        let got = db.get(key.as_bytes()).unwrap();
        if i % 5 == 0 {
            assert_eq!(got, None, "{key} was deleted");
        } else if i % 3 == 0 {
            assert_eq!(got, Some(b"updated".to_vec()), "{key}");
            expected_live += 1;
        } else {
            assert_eq!(got, Some(format!("v{i}").into_bytes()), "{key}");
            expected_live += 1;
        }
    }
    assert_eq!(db.len().unwrap(), expected_live);
}

#[test]
fn repeated_compaction_converges_and_stops() {
    let store = TempStore::new("converge");
    let db = store.open_manual();

    for i in 0..8u32 {
        db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        db.flush().unwrap();
    }

    let rounds = db.compact_all().unwrap();
    assert!(rounds >= 1);
    assert!(
        !db.compact().unwrap(),
        "compact_all must leave nothing further to do"
    );
    assert_eq!(db.len().unwrap(), 8);
}

#[test]
fn the_planner_never_proposes_a_non_contiguous_run() {
    // A direct check on the invariant, independent of any store.
    let sizes = [100u64, 100, 9_000_000, 100, 100, 100, 100];
    let tables: Vec<TableInfo> = sizes
        .iter()
        .enumerate()
        .map(|(i, &size_bytes)| TableInfo {
            path: PathBuf::from(format!("{i:010}-0000.sst")),
            seq: i as u64,
            generation: 0,
            size_bytes,
        })
        .collect();

    let task: CompactionTask = plan(&tables, &CompactionConfig::default()).expect("a run exists");
    let seqs: Vec<u64> = task.inputs.iter().map(|t| t.seq).collect();
    assert_eq!(
        seqs,
        vec![3, 4, 5, 6],
        "must skip the large table, not span it"
    );

    for pair in seqs.windows(2) {
        assert_eq!(pair[1], pair[0] + 1, "inputs must be contiguous");
    }
}
