//! Thread-safe access to a store.
//!
//! # This type is now a thin wrapper, and that is the news
//!
//! `SharedDb` used to be the whole concurrency story: an `Arc<RwLock<Db>>`
//! wrapping a `Db` whose write methods took `&mut self`. Readers shared the
//! lock, writers took it exclusively — which meant **a write blocked every
//! reader for its duration**, and under fsync-per-write that duration is a disk
//! sync.
//!
//! That is fixed, and not by changing the lock. [`Db`] itself is now a cheap
//! cloneable handle — `Clone + Send + Sync`, every method taking `&self` — whose
//! readers take no lock across any I/O at all. The mechanism is documented on
//! `Db`: readers clone an immutable view of the levels, the memtable is an
//! insert-only lock-free structure, and structural changes swap a pointer
//! instead of mutating shared state.
//!
//! So `SharedDb` is now just `Db` with a different name. It is kept because it
//! is a published API and existing code uses it, and because "share a store
//! across threads" is a reasonable thing to go looking for by name. New code can
//! use `Db` directly and lose nothing.
//!
//! # What changed for callers
//!
//! The `read()` / `write()` guard accessors are gone. They returned
//! `RwLockReadGuard<Db>` / `RwLockWriteGuard<Db>`, and there is no longer a
//! `RwLock<Db>` for them to guard. Their purpose was to run several operations
//! against one consistent view, and [`Db::snapshot`] now does that properly —
//! it pins a point in time rather than merely excluding other threads, and it
//! does so without blocking anyone.
//!
//! # Example
//!
//! ```
//! # use std::fs;
//! use minidb::SharedDb;
//!
//! # let dir = std::env::temp_dir().join("minidb-doctest-shared");
//! # let _ = fs::remove_dir_all(&dir);
//! let db = SharedDb::open(&dir)?;
//! db.put(b"key", b"value")?;
//!
//! // Handles are cheap to clone and can move to other threads.
//! let reader = db.clone();
//! let found = std::thread::spawn(move || reader.get(b"key").unwrap())
//!     .join()
//!     .unwrap();
//! assert_eq!(found, Some(b"value".to_vec()));
//! # let _ = fs::remove_dir_all(&dir);
//! # Ok::<(), std::io::Error>(())
//! ```

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use crate::{Db, DbOptions, Snapshot, SyncPolicy};

/// A cloneable, thread-safe handle to a store.
///
/// Equivalent to [`Db`], which is itself `Clone + Send + Sync`. See the module
/// docs for why this still exists.
#[derive(Debug, Clone)]
pub struct SharedDb {
    inner: Db,
}

impl SharedDb {
    /// Wraps an existing store.
    pub fn from_db(db: Db) -> Self {
        Self { inner: db }
    }

    /// Opens an in-memory store.
    pub fn new() -> Self {
        Self::from_db(Db::new())
    }

    /// Opens a durable store in `dir` with default options.
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        Ok(Self::from_db(Db::open(dir)?))
    }

    /// Opens a durable store with an explicit fsync policy.
    pub fn open_with_policy<P: AsRef<Path>>(dir: P, policy: SyncPolicy) -> io::Result<Self> {
        Ok(Self::from_db(Db::open_with_policy(dir, policy)?))
    }

    /// Opens a durable store with explicit options.
    pub fn open_with_options<P: AsRef<Path>>(dir: P, options: DbOptions) -> io::Result<Self> {
        Ok(Self::from_db(Db::open_with_options(dir, options)?))
    }

    /// Borrows the underlying store.
    pub fn db(&self) -> &Db {
        &self.inner
    }

    /// Takes a point-in-time snapshot.
    ///
    /// Replaces the old `read()` guard: several reads through one snapshot see
    /// one consistent view, and unlike a lock it blocks nothing while held.
    pub fn snapshot(&self) -> Snapshot {
        self.inner.snapshot()
    }

    /// Writes `value` at `key`, replacing any existing value.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.inner.put(key, value)
    }

    /// Reads the value at `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.inner.get(key)
    }

    /// Reads the value at `key` as of `snapshot`.
    pub fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.inner.get_at(snapshot, key)
    }

    /// Deletes `key`, returning `true` if a live value was visible beforehand.
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
        self.inner.delete(key)
    }

    /// Returns `true` if `key` currently resolves to a value.
    pub fn contains(&self, key: &[u8]) -> io::Result<bool> {
        self.inner.contains(key)
    }

    /// Returns every live key/value pair, merged across all levels.
    pub fn scan(&self) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        self.inner.scan()
    }

    /// Returns the number of live key/value pairs.
    pub fn len(&self) -> io::Result<usize> {
        self.inner.len()
    }

    /// Returns `true` if the store holds no live values.
    pub fn is_empty(&self) -> io::Result<bool> {
        self.inner.is_empty()
    }

    /// Freezes the memtable and writes it to a new SSTable.
    pub fn flush(&self) -> io::Result<Option<PathBuf>> {
        self.inner.flush()
    }

    /// Runs one compaction if any size tier is over its threshold.
    pub fn compact(&self) -> io::Result<bool> {
        self.inner.compact()
    }

    /// Compacts repeatedly until no tier is over its threshold.
    pub fn compact_all(&self) -> io::Result<usize> {
        self.inner.compact_all()
    }

    /// Forces any buffered log data to stable storage.
    pub fn sync(&self) -> io::Result<()> {
        self.inner.sync()
    }

    /// Returns the number of SSTables currently on disk.
    pub fn sstable_count(&self) -> io::Result<usize> {
        Ok(self.inner.sstable_count())
    }

    /// Returns `true` if this store is backed by a write-ahead log.
    pub fn is_durable(&self) -> io::Result<bool> {
        Ok(self.inner.is_durable())
    }

    /// Returns the number of live handles to this store.
    pub fn handle_count(&self) -> usize {
        self.inner.handle_count()
    }
}

impl Default for SharedDb {
    fn default() -> Self {
        Self::new()
    }
}

impl From<Db> for SharedDb {
    fn from(db: Db) -> Self {
        Self::from_db(db)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both handles are only useful if they can cross a thread boundary.
    #[test]
    fn the_handles_are_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SharedDb>();
        assert_send_sync::<Db>();
    }

    #[test]
    fn an_in_memory_shared_store_round_trips() {
        let db = SharedDb::new();
        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
        assert!(db.delete(b"k").unwrap());
        assert_eq!(db.get(b"k").unwrap(), None);
        assert!(db.is_empty().unwrap());
    }

    #[test]
    fn clones_share_one_underlying_store() {
        let a = SharedDb::new();
        let b = a.clone();

        a.put(b"from-a", b"1").unwrap();
        b.put(b"from-b", b"2").unwrap();

        assert_eq!(a.get(b"from-b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(b.get(b"from-a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(a.handle_count(), 2);
    }

    #[test]
    fn a_snapshot_gives_a_consistent_multi_read_view() {
        let db = SharedDb::new();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();

        let snap = db.snapshot();
        // Writes landing here are invisible through `snap` — which is what the
        // old `read()` guard was reaching for, without blocking the writer.
        db.put(b"a", b"changed").unwrap();
        db.put(b"c", b"3").unwrap();

        assert_eq!(db.get_at(&snap, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(db.get_at(&snap, b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(db.get_at(&snap, b"c").unwrap(), None);
    }

    #[test]
    fn a_borrowed_db_is_the_same_store() {
        let shared = SharedDb::new();
        shared.put(b"k", b"v").unwrap();
        assert_eq!(shared.db().get(b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn many_mutations_land_through_one_handle() {
        let db = SharedDb::new();
        for i in 0..10u32 {
            db.put(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        assert_eq!(db.len().unwrap(), 10);
    }
}
