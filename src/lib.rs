//! `minidb` — an embedded log-structured merge-tree key/value store.
//!
//! # Status
//!
//! The in-memory write path is real and tested. Durability and the on-disk
//! levels are scaffolded with documented signatures but not yet implemented —
//! see the module docs for [`wal`], [`sstable`], [`bloom`], and [`compaction`].
//! **Nothing written through this API currently survives process exit.**
//!
//! # Design
//!
//! LSM trees trade read cost for write cost. Every mutation is an append — to
//! the log, then to a sorted in-memory buffer — so writes never seek and never
//! read-modify-write a page. Reads pay for that by searching newest-to-oldest
//! across the memtable and then each on-disk level, which is why bloom filters
//! and a sparse per-table index matter so much once the disk levels exist.
//!
//! ```
//! use minidb::Db;
//!
//! let mut db = Db::new();
//! db.put(b"lang", b"rust");
//! assert_eq!(db.get(b"lang"), Some(b"rust".to_vec()));
//!
//! db.delete(b"lang");
//! assert_eq!(db.get(b"lang"), None);
//! ```

pub mod bloom;
pub mod compaction;
pub mod memtable;
pub mod sstable;
pub mod wal;

pub use memtable::{Entry, MemTable};

/// Size at which a memtable would be frozen and flushed to an SSTable.
///
/// Unused until the flush path exists; recorded here so the threshold lives with
/// the rest of the store configuration.
pub const MEMTABLE_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// The store's public handle.
///
/// Today this is a thin wrapper over a single [`MemTable`]. As the on-disk
/// levels land it grows a WAL, a frozen-memtable queue, and a level manifest —
/// the API surface here is meant to stay put while that happens underneath.
#[derive(Debug, Default)]
pub struct Db {
    memtable: MemTable,
}

impl Db {
    /// Opens an in-memory store.
    ///
    /// There is no `open(path)` yet — that arrives with the WAL, since opening a
    /// store means replaying one.
    pub fn new() -> Self {
        Self {
            memtable: MemTable::new(),
        }
    }

    /// Writes `value` at `key`, replacing any existing value.
    ///
    /// Not durable: once the WAL exists this will append and fsync before
    /// returning, and the signature will become fallible.
    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.memtable.put(key.to_vec(), value.to_vec());
    }

    /// Reads the value at `key`, or `None` if absent or deleted.
    ///
    /// Once SSTables exist this searches newest-to-oldest and stops at the first
    /// table holding either a value or a tombstone for the key.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.memtable.get(key).map(|v| v.to_vec())
    }

    /// Deletes `key`, returning `true` if a live value was removed.
    ///
    /// Recorded as a tombstone rather than an erasure — see [`Entry`].
    pub fn delete(&mut self, key: &[u8]) -> bool {
        self.memtable.delete(key.to_vec())
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

        db.put(b"k", b"v");
        assert_eq!(db.get(b"k"), Some(b"v".to_vec()));
        assert!(db.contains(b"k"));
        assert_eq!(db.len(), 1);

        assert!(db.delete(b"k"));
        assert_eq!(db.get(b"k"), None);
        assert!(!db.contains(b"k"));
        assert!(db.is_empty());
    }

    #[test]
    fn deleting_an_absent_key_is_a_no_op_for_callers() {
        let mut db = Db::new();
        assert!(!db.delete(b"nope"));
    }
}
