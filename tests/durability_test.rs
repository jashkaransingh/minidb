//! Integration tests for the write-ahead log and crash recovery.
//!
//! These drive a real store directory on disk, then reopen it to assert that
//! acknowledged writes came back. "Crashes" are simulated by dropping the handle
//! and, where a torn write is needed, by truncating or corrupting the log file
//! directly — the same damage a power failure leaves behind.

use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use minidb::wal::TailDefect;
use minidb::{Db, Record, SyncPolicy, WAL_FILENAME, Wal};

/// A scratch store directory that removes itself on drop.
struct TempStore(PathBuf);

impl TempStore {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("minidb-durability-{tag}"));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }

    fn wal_path(&self) -> PathBuf {
        self.0.join(WAL_FILENAME)
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn writes_survive_reopening_the_store() {
    let store = TempStore::new("reopen");
    {
        let mut db = Db::open(store.path()).unwrap();
        db.put(b"alpha", b"1").unwrap();
        db.put(b"beta", b"2").unwrap();
    }

    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.get(b"alpha").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"beta").unwrap(), Some(b"2".to_vec()));
    assert_eq!(db.len().unwrap(), 2);
}

#[test]
fn deletes_survive_reopening_and_stay_deleted() {
    let store = TempStore::new("delete-survives");
    {
        let mut db = Db::open(store.path()).unwrap();
        db.put(b"doomed", b"value").unwrap();
        db.put(b"kept", b"value").unwrap();
        db.delete(b"doomed").unwrap();
    }

    let db = Db::open(store.path()).unwrap();
    assert_eq!(
        db.get(b"doomed").unwrap(),
        None,
        "a replayed delete must stay applied"
    );
    assert_eq!(db.get(b"kept").unwrap(), Some(b"value".to_vec()));
    assert_eq!(db.len().unwrap(), 1);
}

#[test]
fn overwrites_replay_in_order_so_the_last_write_wins() {
    let store = TempStore::new("last-write-wins");
    {
        let mut db = Db::open(store.path()).unwrap();
        for v in ["v1", "v2", "v3"] {
            db.put(b"key", v.as_bytes()).unwrap();
        }
    }

    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.get(b"key").unwrap(), Some(b"v3".to_vec()));
}

#[test]
fn a_key_rewritten_after_deletion_replays_as_live() {
    let store = TempStore::new("resurrect");
    {
        let mut db = Db::open(store.path()).unwrap();
        db.put(b"phoenix", b"first").unwrap();
        db.delete(b"phoenix").unwrap();
        db.put(b"phoenix", b"second").unwrap();
    }

    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.get(b"phoenix").unwrap(), Some(b"second".to_vec()));
}

#[test]
fn opening_a_fresh_directory_yields_an_empty_store() {
    let store = TempStore::new("fresh");
    let db = Db::open(store.path()).unwrap();
    assert!(db.is_empty().unwrap());
    assert!(db.is_durable());
    assert_eq!(db.wal_size_bytes(), 0);
}

#[test]
fn many_writes_all_survive_recovery() {
    let store = TempStore::new("bulk");
    {
        let mut db = Db::open(store.path()).unwrap();
        for i in 0..500u32 {
            db.put(format!("key:{i:04}").as_bytes(), &i.to_be_bytes())
                .unwrap();
        }
        // Delete every tenth key.
        for i in (0..500u32).step_by(10) {
            db.delete(format!("key:{i:04}").as_bytes()).unwrap();
        }
    }

    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.len().unwrap(), 450);
    for i in 0..500u32 {
        let key = format!("key:{i:04}");
        let got = db.get(key.as_bytes()).unwrap();
        if i % 10 == 0 {
            assert_eq!(got, None, "{key} was deleted before the reopen");
        } else {
            assert_eq!(got, Some(i.to_be_bytes().to_vec()), "{key} lost its value");
        }
    }
}

#[test]
fn a_torn_tail_costs_only_the_unacknowledged_write() {
    let store = TempStore::new("torn-tail");
    {
        let mut db = Db::open(store.path()).unwrap();
        db.put(b"acked-1", b"a").unwrap();
        db.put(b"acked-2", b"b").unwrap();
    }

    // Simulate a crash midway through a third append that was never acked.
    let mut file = OpenOptions::new()
        .append(true)
        .open(store.wal_path())
        .unwrap();
    file.write_all(&[0xab; 9]).unwrap();
    drop(file);

    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.get(b"acked-1").unwrap(), Some(b"a".to_vec()));
    assert_eq!(db.get(b"acked-2").unwrap(), Some(b"b".to_vec()));
    assert_eq!(db.len().unwrap(), 2);
}

#[test]
fn recovery_repairs_the_log_so_the_store_keeps_working() {
    let store = TempStore::new("repair");
    {
        let mut db = Db::open(store.path()).unwrap();
        db.put(b"before", b"1").unwrap();
    }

    // Damage the tail.
    let mut file = OpenOptions::new()
        .append(true)
        .open(store.wal_path())
        .unwrap();
    file.write_all(&[0xff; 11]).unwrap();
    drop(file);

    // Reopen, write more, and reopen again: the repaired log must be appendable.
    {
        let mut db = Db::open(store.path()).unwrap();
        assert_eq!(db.get(b"before").unwrap(), Some(b"1".to_vec()));
        db.put(b"after", b"2").unwrap();
    }

    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.get(b"before").unwrap(), Some(b"1".to_vec()));
    assert_eq!(db.get(b"after").unwrap(), Some(b"2".to_vec()));
}

#[test]
fn corruption_in_the_log_is_caught_by_the_checksum() {
    let store = TempStore::new("checksum");
    {
        let mut db = Db::open(store.path()).unwrap();
        db.put(b"first", b"aaaa").unwrap();
        db.put(b"second", b"bbbb").unwrap();
    }

    // Flip a bit in the final record's value.
    let mut bytes = fs::read(store.wal_path()).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(store.wal_path(), &bytes).unwrap();

    let recovery = Wal::replay(store.wal_path()).unwrap();
    assert_eq!(recovery.defect, Some(TailDefect::BadChecksum));
    assert_eq!(recovery.records.len(), 1);

    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.get(b"first").unwrap(), Some(b"aaaa".to_vec()));
    assert_eq!(
        db.get(b"second").unwrap(),
        None,
        "corrupt record must not be applied"
    );
}

#[test]
fn the_log_records_exactly_the_mutations_issued() {
    let store = TempStore::new("record-stream");
    {
        let mut db = Db::open(store.path()).unwrap();
        db.put(b"a", b"1").unwrap();
        db.delete(b"a").unwrap();
        db.put(b"b", b"2").unwrap();
    }

    let recovery = Wal::replay(store.wal_path()).unwrap();
    assert_eq!(
        recovery.records,
        vec![
            Record::put(1, b"a".to_vec(), b"1".to_vec()),
            Record::delete(2, b"a".to_vec()),
            Record::put(3, b"b".to_vec(), b"2".to_vec()),
        ],
        "each mutation carries the sequence number it was assigned"
    );
    assert!(!recovery.truncated());
}

#[test]
fn buffered_policy_survives_a_clean_close() {
    let store = TempStore::new("buffered");
    {
        let mut db = Db::open_with_policy(store.path(), SyncPolicy::OsBuffered).unwrap();
        db.put(b"k", b"v").unwrap();
        db.sync().unwrap();
    }

    let db = Db::open(store.path()).unwrap();
    assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
}

#[test]
fn an_in_memory_store_writes_nothing_to_disk() {
    let mut db = Db::new();
    db.put(b"k", b"v").unwrap();
    assert!(!db.is_durable());
    assert_eq!(db.wal_size_bytes(), 0);
    assert!(db.dir().is_none());
}
