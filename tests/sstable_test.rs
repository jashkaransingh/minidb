//! Integration tests for memtable → SSTable flush and multi-level reads.
//!
//! These drive a real store directory with a small flush threshold so that
//! ordinary writes spill to disk, then assert that reads still see a single
//! coherent view across the memtable and every table beneath it.

use std::fs;
use std::path::{Path, PathBuf};

use minidb::{
    CompactionConfig, Db, DbOptions, Entry, FaultPlan, SsTable, SsTableWriter, SyncPolicy,
};

/// A scratch store directory that removes itself on drop.
struct TempStore(PathBuf);

impl TempStore {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("minidb-sstable-it-{tag}"));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn open(&self, threshold: usize) -> Db {
        Db::open_with_options(
            &self.0,
            DbOptions {
                sync_policy: SyncPolicy::EveryWrite,
                flush_threshold_bytes: threshold,
                compaction: CompactionConfig::default(),
                // These tests exercise flush and shadowing in isolation;
                // compaction has its own suite.
                auto_compact: false,
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
fn an_explicit_flush_writes_a_table_and_empties_the_memtable() {
    let store = TempStore::new("explicit");
    let mut db = store.open(usize::MAX); // never auto-flush

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    assert_eq!(db.sstable_count(), 0);

    let path = db.flush().unwrap().expect("a flush should have happened");
    assert!(path.exists());
    assert_eq!(db.sstable_count(), 1);
    assert_eq!(db.size_bytes(), 0, "memtable is emptied by the flush");

    // Data is still readable — now from disk.
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn flushing_an_empty_memtable_is_a_no_op() {
    let store = TempStore::new("empty-flush");
    let mut db = store.open(usize::MAX);
    assert_eq!(db.flush().unwrap(), None);
    assert_eq!(db.sstable_count(), 0);
}

#[test]
fn the_wal_is_rotated_once_its_data_is_safely_in_a_table() {
    let store = TempStore::new("rotate");
    let mut db = store.open(usize::MAX);

    db.put(b"a", b"1").unwrap();
    assert!(db.wal_size_bytes() > 0);

    db.flush().unwrap();
    assert_eq!(db.wal_size_bytes(), 0, "log is discarded after the flush");

    // And the data is still there after a reopen, now from the table.
    drop(db);
    let db = store.open(usize::MAX);
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.sstable_count(), 1);
}

#[test]
fn writes_past_the_threshold_flush_automatically() {
    let store = TempStore::new("auto");
    let mut db = store.open(64); // tiny threshold

    for i in 0..100u32 {
        db.put(format!("key:{i:04}").as_bytes(), b"payload-value")
            .unwrap();
    }

    assert!(
        db.sstable_count() > 1,
        "expected several automatic flushes, got {}",
        db.sstable_count()
    );

    // Every write is still readable across all the tables produced.
    for i in 0..100u32 {
        let key = format!("key:{i:04}");
        assert_eq!(
            db.get(key.as_bytes()).unwrap(),
            Some(b"payload-value".to_vec()),
            "{key} went missing across a flush"
        );
    }
    assert_eq!(db.len().unwrap(), 100);
}

#[test]
fn a_newer_table_shadows_an_older_one() {
    let store = TempStore::new("shadow");
    let mut db = store.open(usize::MAX);

    db.put(b"k", b"old").unwrap();
    db.flush().unwrap();

    db.put(b"k", b"new").unwrap();
    db.flush().unwrap();

    assert_eq!(db.sstable_count(), 2);
    assert_eq!(db.get(b"k").unwrap(), Some(b"new".to_vec()));
    assert_eq!(db.len().unwrap(), 1);
}

#[test]
fn a_tombstone_in_a_newer_table_hides_a_value_in_an_older_one() {
    let store = TempStore::new("tombstone-shadow");
    let mut db = store.open(usize::MAX);

    db.put(b"doomed", b"value").unwrap();
    db.put(b"kept", b"value").unwrap();
    db.flush().unwrap();

    db.delete(b"doomed").unwrap();
    db.flush().unwrap();

    assert_eq!(db.sstable_count(), 2);
    assert_eq!(
        db.get(b"doomed").unwrap(),
        None,
        "the tombstone must stop the search, not fall through to the old value"
    );
    assert_eq!(db.get(b"kept").unwrap(), Some(b"value".to_vec()));
    assert_eq!(db.len().unwrap(), 1);
}

#[test]
fn a_delete_then_rewrite_across_flushes_resolves_to_the_newest_value() {
    let store = TempStore::new("resurrect");
    let mut db = store.open(usize::MAX);

    db.put(b"phoenix", b"first").unwrap();
    db.flush().unwrap();
    db.delete(b"phoenix").unwrap();
    db.flush().unwrap();
    db.put(b"phoenix", b"second").unwrap();
    db.flush().unwrap();

    assert_eq!(db.sstable_count(), 3);
    assert_eq!(db.get(b"phoenix").unwrap(), Some(b"second".to_vec()));
}

#[test]
fn the_memtable_shadows_every_table_beneath_it() {
    let store = TempStore::new("memtable-wins");
    let mut db = store.open(usize::MAX);

    db.put(b"k", b"on-disk").unwrap();
    db.flush().unwrap();
    db.put(b"k", b"in-memory").unwrap();

    assert_eq!(db.get(b"k").unwrap(), Some(b"in-memory".to_vec()));

    db.delete(b"k").unwrap();
    assert_eq!(
        db.get(b"k").unwrap(),
        None,
        "an unflushed tombstone must hide the on-disk value"
    );
}

#[test]
fn tables_are_rediscovered_when_the_store_is_reopened() {
    let store = TempStore::new("rediscover");
    {
        let mut db = store.open(usize::MAX);
        db.put(b"a", b"1").unwrap();
        db.flush().unwrap();
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
        db.put(b"c", b"3").unwrap(); // stays in the memtable and the log
    }

    let db = store.open(usize::MAX);
    assert_eq!(db.sstable_count(), 2);
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"b").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.get(b"c").unwrap(), Some(b"3".to_vec()), "from the log");
    assert_eq!(db.len().unwrap(), 3);
}

#[test]
fn table_shadowing_order_survives_a_reopen() {
    let store = TempStore::new("order-survives");
    {
        let mut db = store.open(usize::MAX);
        db.put(b"k", b"v1").unwrap();
        db.flush().unwrap();
        db.put(b"k", b"v2").unwrap();
        db.flush().unwrap();
        db.put(b"k", b"v3").unwrap();
        db.flush().unwrap();
    }

    let db = store.open(usize::MAX);
    assert_eq!(db.sstable_count(), 3);
    assert_eq!(
        db.get(b"k").unwrap(),
        Some(b"v3".to_vec()),
        "sequence numbering must preserve which table is newest"
    );
}

#[test]
fn sequence_numbers_continue_after_a_reopen() {
    let store = TempStore::new("seq");
    {
        let mut db = store.open(usize::MAX);
        db.put(b"a", b"1").unwrap();
        db.flush().unwrap();
    }
    {
        let mut db = store.open(usize::MAX);
        db.put(b"b", b"2").unwrap();
        db.flush().unwrap();
    }

    let files = store.sst_files();
    assert_eq!(files, vec!["0000000000-0000.sst", "0000000001-0000.sst"]);
}

#[test]
fn a_stale_temp_file_is_cleaned_up_on_open() {
    let store = TempStore::new("stale-tmp");
    {
        let mut db = store.open(usize::MAX);
        db.put(b"a", b"1").unwrap();
        db.flush().unwrap();
    }

    // Simulate a crash midway through writing the next table.
    let junk = store.path().join("0000000001-0000.sst.tmp");
    fs::write(&junk, b"half-written garbage").unwrap();

    let db = store.open(usize::MAX);
    assert!(!junk.exists(), "stale temp files must be removed on open");
    assert_eq!(db.get(b"a").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.sstable_count(), 1);
}

#[test]
fn data_written_before_a_flush_survives_a_simulated_crash_after_it() {
    let store = TempStore::new("crash-after-flush");
    {
        let mut db = store.open(usize::MAX);
        db.put(b"flushed", b"1").unwrap();
        db.flush().unwrap();
        // Written after the flush: lives only in the log.
        db.put(b"logged", b"2").unwrap();
        // Handle dropped without another flush — like a crash.
    }

    let db = store.open(usize::MAX);
    assert_eq!(db.get(b"flushed").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"logged").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn a_bulk_workload_stays_consistent_across_many_flushes() {
    let store = TempStore::new("bulk");
    let mut db = store.open(256);

    // Write 600 keys, then overwrite a third and delete another third.
    for i in 0..600u32 {
        db.put(format!("k{i:04}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }
    for i in (0..600u32).step_by(3) {
        db.put(format!("k{i:04}").as_bytes(), b"overwritten")
            .unwrap();
    }
    for i in (1..600u32).step_by(3) {
        db.delete(format!("k{i:04}").as_bytes()).unwrap();
    }
    drop(db);

    // Reopen so everything is served from disk plus the recovered log.
    let db = store.open(256);
    assert!(db.sstable_count() > 1);

    for i in 0..600u32 {
        let key = format!("k{i:04}");
        let got = db.get(key.as_bytes()).unwrap();
        if i % 3 == 0 {
            assert_eq!(got, Some(b"overwritten".to_vec()), "{key}");
        } else if i % 3 == 1 {
            assert_eq!(got, None, "{key} was deleted");
        } else {
            assert_eq!(got, Some(format!("v{i}").into_bytes()), "{key}");
        }
    }
    assert_eq!(db.len().unwrap(), 400);
}

#[test]
fn scan_returns_the_merged_live_view() {
    let store = TempStore::new("scan");
    let mut db = store.open(usize::MAX);

    db.put(b"a", b"1").unwrap();
    db.put(b"b", b"2").unwrap();
    db.flush().unwrap();
    db.put(b"b", b"2-new").unwrap();
    db.delete(b"a").unwrap();
    db.put(b"c", b"3").unwrap();

    let live = db.scan().unwrap();
    let as_pairs: Vec<(String, String)> = live
        .into_iter()
        .map(|(k, v)| (String::from_utf8(k).unwrap(), String::from_utf8(v).unwrap()))
        .collect();

    assert_eq!(
        as_pairs,
        vec![
            ("b".to_string(), "2-new".to_string()),
            ("c".to_string(), "3".to_string()),
        ]
    );
}

#[test]
fn flushed_tables_carry_usable_metadata() {
    let store = TempStore::new("meta");
    let mut db = store.open(usize::MAX);

    db.put(b"apple", b"1").unwrap();
    db.put(b"mango", b"2").unwrap();
    db.delete(b"zebra").unwrap();
    db.flush().unwrap();

    let table = &db.tables()[0];
    assert_eq!(table.meta().num_entries, 3);
    assert_eq!(table.meta().num_tombstones, 1);
    assert_eq!(table.meta().min_key, b"apple".to_vec());
    assert_eq!(table.meta().max_key, b"zebra".to_vec());
    assert!(table.verify().unwrap(), "data checksum must validate");
}

#[test]
fn a_table_written_directly_can_be_read_back() {
    // Exercises the writer/reader pair without going through Db.
    let store = TempStore::new("direct");
    fs::create_dir_all(store.path()).unwrap();
    let path = store.path().join("direct.sst");

    let mut w = SsTableWriter::create(&path).unwrap();
    w.append(b"one", &Entry::Value(b"1".to_vec())).unwrap();
    w.append(b"two", &Entry::Tombstone).unwrap();
    let meta = w.finish().unwrap();
    assert_eq!(meta.num_entries, 2);

    let table = SsTable::open(&path).unwrap();
    assert_eq!(
        table.get(b"one").unwrap(),
        Some(Entry::Value(b"1".to_vec()))
    );
    assert_eq!(table.get(b"two").unwrap(), Some(Entry::Tombstone));
    assert_eq!(table.get(b"three").unwrap(), None);
}
