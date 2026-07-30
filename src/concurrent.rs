//! Thread-safe access to a store, via a reader–writer lock.
//!
//! # Why a separate type
//!
//! [`Db`] takes `&mut self` to write. That is the right signature for a
//! single-threaded embedded store: it makes exclusive access a compile-time
//! fact and costs nothing at runtime. Baking a lock into `Db` itself would make
//! every single-threaded user pay for synchronization they never asked for.
//!
//! [`SharedDb`] is the opt-in wrapper: an `Arc<RwLock<Db>>` with an `&self` API,
//! cloneable into as many handles as there are threads.
//!
//! # What the lock buys, and what it does not
//!
//! Readers share the lock, so any number of `get`/`scan`/`len` calls proceed in
//! parallel. Writers take it exclusively, so `put`/`delete`/`flush`/`compact`
//! are serialized against everything else.
//!
//! That is the honest limit of this design: **a write blocks every reader for
//! its duration**, and under the default fsync-per-write policy a write is
//! dominated by a disk sync. A write-heavy workload will see readers stall.
//!
//! Removing that stall is a bigger change than a different lock. The standard
//! approach exploits the fact that SSTables are immutable: readers take a cheap
//! snapshot of the table list and read outside the lock entirely, while writers
//! touch only the memtable and the log. Doing that safely needs the memtable to
//! be a concurrent structure (or double-buffered behind an atomic swap), which
//! is a real redesign rather than a wrapper. It is not implemented here, and the
//! benchmark to justify it does not exist yet either.
//!
//! # Poisoning
//!
//! If a thread panics while holding the lock, `std`'s `RwLock` marks it
//! poisoned. Every method here surfaces that as an [`io::Error`] rather than
//! panicking, so one thread's failure degrades the store instead of cascading
//! into every other thread.
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
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::{Db, DbOptions, SyncPolicy};

/// A cloneable, thread-safe handle to a store.
///
/// Clones share one underlying [`Db`]; the type is `Send + Sync` and is intended
/// to be cloned once per thread.
#[derive(Debug, Clone)]
pub struct SharedDb {
    inner: Arc<RwLock<Db>>,
}

impl SharedDb {
    /// Wraps an existing store.
    pub fn from_db(db: Db) -> Self {
        Self {
            inner: Arc::new(RwLock::new(db)),
        }
    }

    /// Opens an in-memory store behind a lock.
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

    /// Acquires shared read access.
    ///
    /// Use this to run several reads against one consistent view; calling
    /// [`get`](Self::get) twice takes the lock twice and a write may interleave.
    pub fn read(&self) -> io::Result<RwLockReadGuard<'_, Db>> {
        self.inner.read().map_err(poisoned)
    }

    /// Acquires exclusive write access.
    ///
    /// Use this to run several mutations under one lock acquisition.
    pub fn write(&self) -> io::Result<RwLockWriteGuard<'_, Db>> {
        self.inner.write().map_err(poisoned)
    }

    /// Writes `value` at `key`, replacing any existing value.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.write()?.put(key, value)
    }

    /// Reads the value at `key`, or `None` if absent or deleted.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.read()?.get(key)
    }

    /// Deletes `key`, returning `true` if a live value was visible in the
    /// memtable beforehand.
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
        self.write()?.delete(key)
    }

    /// Returns `true` if `key` currently resolves to a value.
    pub fn contains(&self, key: &[u8]) -> io::Result<bool> {
        self.read()?.contains(key)
    }

    /// Returns every live key/value pair, merged across all levels.
    pub fn scan(&self) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        self.read()?.scan()
    }

    /// Returns the number of live key/value pairs.
    pub fn len(&self) -> io::Result<usize> {
        self.read()?.len()
    }

    /// Returns `true` if the store holds no live values.
    pub fn is_empty(&self) -> io::Result<bool> {
        self.read()?.is_empty()
    }

    /// Freezes the memtable and writes it to a new SSTable.
    pub fn flush(&self) -> io::Result<Option<PathBuf>> {
        self.write()?.flush()
    }

    /// Runs one compaction if any size tier is over its threshold.
    pub fn compact(&self) -> io::Result<bool> {
        self.write()?.compact()
    }

    /// Compacts repeatedly until no tier is over its threshold.
    pub fn compact_all(&self) -> io::Result<usize> {
        self.write()?.compact_all()
    }

    /// Forces any buffered log data to stable storage.
    pub fn sync(&self) -> io::Result<()> {
        self.write()?.sync()
    }

    /// Returns the number of SSTables currently on disk.
    pub fn sstable_count(&self) -> io::Result<usize> {
        Ok(self.read()?.sstable_count())
    }

    /// Returns `true` if this store is backed by a write-ahead log.
    pub fn is_durable(&self) -> io::Result<bool> {
        Ok(self.read()?.is_durable())
    }

    /// Returns the number of live handles to this store.
    pub fn handle_count(&self) -> usize {
        Arc::strong_count(&self.inner)
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

/// Converts a poisoned-lock error into an `io::Error`.
///
/// A poisoned lock means another thread panicked mid-mutation, so the in-memory
/// state may be inconsistent. Reporting it is better than either panicking in
/// every subsequent thread or silently handing out possibly-torn state.
fn poisoned<T>(_: std::sync::PoisonError<T>) -> io::Error {
    io::Error::other("minidb lock poisoned: a thread panicked while holding it")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `SharedDb` is only useful if it can actually cross a thread boundary.
    #[test]
    fn the_handle_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<SharedDb>();
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
    fn a_held_guard_gives_a_consistent_multi_read_view() {
        let db = SharedDb::new();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();

        let guard = db.read().unwrap();
        assert_eq!(guard.get(b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(guard.get(b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(guard.len().unwrap(), 2);
    }

    #[test]
    fn a_write_guard_batches_mutations_under_one_acquisition() {
        let db = SharedDb::new();
        {
            let mut guard = db.write().unwrap();
            for i in 0..10u32 {
                guard.put(format!("k{i}").as_bytes(), b"v").unwrap();
            }
        }
        assert_eq!(db.len().unwrap(), 10);
    }
}
