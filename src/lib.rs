//! `minidb` — an embedded log-structured merge-tree key/value store.
//!
//! # Status
//!
//! The write path is durable: mutations are appended to a write-ahead log and
//! fsynced before they are acknowledged, and a store reopened after a crash
//! replays that log to rebuild its in-memory state. The on-disk levels —
//! SSTables, bloom filters, and compaction — are still scaffolded; see the
//! module docs for [`sstable`], [`bloom`], and [`compaction`].
//!
//! # Design
//!
//! LSM trees trade read cost for write cost. Every mutation is an append — to
//! the log, then to a sorted in-memory buffer — so writes never seek and never
//! read-modify-write a page. Reads pay for that by searching newest-to-oldest
//! across the memtable and then each on-disk level, which is why bloom filters
//! and a sparse per-table index matter so much once the disk levels exist.
//!
//! # Examples
//!
//! In memory, with no durability:
//!
//! ```
//! use minidb::Db;
//!
//! let mut db = Db::new();
//! db.put(b"lang", b"rust").unwrap();
//! assert_eq!(db.get(b"lang"), Some(b"rust".to_vec()));
//!
//! db.delete(b"lang").unwrap();
//! assert_eq!(db.get(b"lang"), None);
//! ```
//!
//! Durable, backed by a directory on disk:
//!
//! ```
//! # use std::fs;
//! use minidb::Db;
//!
//! # let dir = std::env::temp_dir().join("minidb-doctest-open");
//! # let _ = fs::remove_dir_all(&dir);
//! let mut db = Db::open(&dir)?;
//! db.put(b"key", b"value")?;
//! drop(db); // or crash here — the write is already fsynced
//!
//! let recovered = Db::open(&dir)?;
//! assert_eq!(recovered.get(b"key"), Some(b"value".to_vec()));
//! # let _ = fs::remove_dir_all(&dir);
//! # Ok::<(), std::io::Error>(())
//! ```

pub mod bloom;
pub mod compaction;
pub mod memtable;
pub mod sstable;
pub mod wal;

use std::io;
use std::path::{Path, PathBuf};

pub use memtable::{Entry, MemTable};
pub use wal::{Record, Recovery, SyncPolicy, Wal};

/// Size at which a memtable is frozen and flushed to an SSTable.
///
/// Not yet enforced — the flush path lands with the SSTable milestone.
pub const MEMTABLE_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// Filename of the write-ahead log within a store directory.
pub const WAL_FILENAME: &str = "wal.log";

/// The store's public handle.
///
/// A `Db` is either **in-memory** ([`Db::new`]) or **durable** ([`Db::open`]).
/// The durable form appends every mutation to a write-ahead log before applying
/// it to the memtable, so an acknowledged write survives a crash.
#[derive(Debug, Default)]
pub struct Db {
    memtable: MemTable,
    wal: Option<Wal>,
    dir: Option<PathBuf>,
}

impl Db {
    /// Opens a purely in-memory store.
    ///
    /// Nothing is written to disk and nothing survives process exit. Useful for
    /// tests and for callers that want the data structure without the I/O.
    pub fn new() -> Self {
        Self {
            memtable: MemTable::new(),
            wal: None,
            dir: None,
        }
    }

    /// Opens a durable store in `dir`, creating the directory if needed.
    ///
    /// Any existing write-ahead log is replayed to rebuild the memtable. If the
    /// log has a damaged tail — the normal result of a crash mid-append — the
    /// damaged records are discarded and the log is truncated to its last
    /// intact record. Everything acknowledged before the crash is recovered.
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        let wal_path = dir.join(WAL_FILENAME);
        let recovery = Wal::replay(&wal_path)?;

        let mut memtable = MemTable::new();
        for record in &recovery.records {
            match record {
                Record::Put { key, value } => memtable.put(key.clone(), value.clone()),
                Record::Delete { key } => {
                    memtable.delete(key.clone());
                }
            }
        }

        let wal = Wal::open(&wal_path, SyncPolicy::EveryWrite)?;

        Ok(Self {
            memtable,
            wal: Some(wal),
            dir: Some(dir),
        })
    }

    /// Opens a durable store with an explicit fsync policy.
    ///
    /// [`SyncPolicy::OsBuffered`] trades crash-durability for throughput: writes
    /// are logged but not fsynced, so a power failure can lose recent
    /// acknowledged writes. Process-level crashes are still survived, since the
    /// bytes reach the OS.
    pub fn open_with_policy<P: AsRef<Path>>(dir: P, policy: SyncPolicy) -> io::Result<Self> {
        let mut db = Self::open(dir)?;
        if let Some(path) = db.wal.as_ref().map(|w| w.path().to_path_buf()) {
            db.wal = Some(Wal::open(path, policy)?);
        }
        Ok(db)
    }

    /// Writes `value` at `key`, replacing any existing value.
    ///
    /// On a durable store this returns only after the mutation is in the log
    /// (and fsynced, under the default policy). If it returns `Ok`, the write
    /// will survive a crash.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        if let Some(wal) = self.wal.as_mut() {
            wal.append(&Record::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            })?;
        }
        self.memtable.put(key.to_vec(), value.to_vec());
        Ok(())
    }

    /// Reads the value at `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.memtable.get(key).map(|v| v.to_vec())
    }

    /// Deletes `key`, returning `true` if a live value was removed.
    ///
    /// Recorded as a tombstone rather than an erasure — see [`Entry`].
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        if let Some(wal) = self.wal.as_mut() {
            wal.append(&Record::Delete { key: key.to_vec() })?;
        }
        Ok(self.memtable.delete(key.to_vec()))
    }

    /// Forces any buffered log data to stable storage.
    ///
    /// A no-op under the default [`SyncPolicy::EveryWrite`], which has already
    /// synced. Meaningful under [`SyncPolicy::OsBuffered`].
    pub fn sync(&mut self) -> io::Result<()> {
        match self.wal.as_mut() {
            Some(wal) => wal.sync(),
            None => Ok(()),
        }
    }

    /// Returns `true` if `key` currently resolves to a value.
    pub fn contains(&self, key: &[u8]) -> bool {
        self.memtable.get(key).is_some()
    }

    /// Returns the number of live key/value pairs.
    pub fn len(&self) -> usize {
        self.memtable.iter_values().count()
    }

    /// Returns `true` if the store holds no live values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate resident size of the write buffer, in bytes.
    pub fn size_bytes(&self) -> usize {
        self.memtable.size_bytes()
    }

    /// Returns `true` if this store is backed by a write-ahead log.
    pub fn is_durable(&self) -> bool {
        self.wal.is_some()
    }

    /// Returns the store's directory, or `None` for an in-memory store.
    pub fn dir(&self) -> Option<&Path> {
        self.dir.as_deref()
    }

    /// Returns the size of the write-ahead log in bytes, or 0 if in-memory.
    pub fn wal_size_bytes(&self) -> u64 {
        self.wal.as_ref().map_or(0, |w| w.size_bytes())
    }

    /// Borrows the underlying memtable.
    pub fn memtable(&self) -> &MemTable {
        &self.memtable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_put_get_delete_cycle() {
        let mut db = Db::new();
        assert!(db.is_empty());

        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
        assert!(db.contains(b"k"));
        assert_eq!(db.len(), 1);

        assert!(db.delete(b"k").unwrap());
        assert_eq!(db.get(b"k"), None);
        assert!(!db.contains(b"k"));
        assert!(db.is_empty());
    }

    #[test]
    fn deleting_an_absent_key_is_a_no_op_for_callers() {
        let mut db = Db::new();
        assert!(!db.delete(b"nope").unwrap());
    }

    #[test]
    fn an_in_memory_store_writes_no_log() {
        let db = Db::new();
        assert!(!db.is_durable());
        assert_eq!(db.wal_size_bytes(), 0);
        assert!(db.dir().is_none());
    }
}
