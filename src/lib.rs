//! `minidb` — an embedded log-structured merge-tree key/value store.
//!
//! # Status
//!
//! The write path is durable and reaches disk: mutations are appended to a
//! write-ahead log and fsynced before they are acknowledged, and when the
//! memtable exceeds its threshold it is flushed to an immutable SSTable. Reads
//! search the memtable and then each table, newest first. Bloom filters, the
//! sparse block index, and compaction are still scaffolded — see [`bloom`] and
//! [`compaction`].
//!
//! # Design
//!
//! LSM trees trade read cost for write cost. Every mutation is an append — to
//! the log, then to a sorted in-memory buffer — so writes never seek and never
//! read-modify-write a page. Reads pay for that by searching newest-to-oldest
//! across the memtable and then each on-disk table, which is why bloom filters
//! and a sparse per-table index matter so much.
//!
//! # Examples
//!
//! In memory, with no durability:
//!
//! ```
//! use minidb::Db;
//!
//! let mut db = Db::new();
//! db.put(b"lang", b"rust")?;
//! assert_eq!(db.get(b"lang")?, Some(b"rust".to_vec()));
//!
//! db.delete(b"lang")?;
//! assert_eq!(db.get(b"lang")?, None);
//! # Ok::<(), std::io::Error>(())
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
//! assert_eq!(recovered.get(b"key")?, Some(b"value".to_vec()));
//! # let _ = fs::remove_dir_all(&dir);
//! # Ok::<(), std::io::Error>(())
//! ```

pub mod bloom;
pub mod compaction;
pub mod memtable;
pub mod sstable;
pub mod wal;

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

pub use memtable::{Entry, MemTable};
pub use sstable::{SsTable, SsTableWriter, TableMeta};
pub use wal::{Record, Recovery, SyncPolicy, Wal};

/// Default size at which a memtable is frozen and flushed to an SSTable.
pub const MEMTABLE_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// Filename of the write-ahead log within a store directory.
pub const WAL_FILENAME: &str = "wal.log";

/// Extension given to SSTable files.
pub const SSTABLE_EXTENSION: &str = "sst";

/// Tunables for a durable store.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DbOptions {
    /// How aggressively the write-ahead log is fsynced.
    pub sync_policy: SyncPolicy,
    /// Memtable size that triggers a flush to a new SSTable.
    pub flush_threshold_bytes: usize,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            sync_policy: SyncPolicy::EveryWrite,
            flush_threshold_bytes: MEMTABLE_FLUSH_THRESHOLD_BYTES,
        }
    }
}

/// The store's public handle.
///
/// A `Db` is either **in-memory** ([`Db::new`]) or **durable** ([`Db::open`]).
/// The durable form appends every mutation to a write-ahead log before applying
/// it to the memtable, and flushes the memtable to an immutable SSTable once it
/// grows past [`DbOptions::flush_threshold_bytes`].
#[derive(Debug)]
pub struct Db {
    memtable: MemTable,
    wal: Option<Wal>,
    dir: Option<PathBuf>,
    /// Tables ordered oldest first; later entries shadow earlier ones.
    tables: Vec<SsTable>,
    next_seq: u64,
    options: DbOptions,
}

impl Default for Db {
    fn default() -> Self {
        Self::new()
    }
}

impl Db {
    /// Opens a purely in-memory store.
    ///
    /// Nothing is written to disk and nothing survives process exit. The
    /// memtable is never flushed, so the whole dataset stays resident.
    pub fn new() -> Self {
        Self {
            memtable: MemTable::new(),
            wal: None,
            dir: None,
            tables: Vec::new(),
            next_seq: 0,
            options: DbOptions::default(),
        }
    }

    /// Opens a durable store in `dir` with default options.
    pub fn open<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        Self::open_with_options(dir, DbOptions::default())
    }

    /// Opens a durable store with an explicit fsync policy.
    pub fn open_with_policy<P: AsRef<Path>>(dir: P, policy: SyncPolicy) -> io::Result<Self> {
        Self::open_with_options(
            dir,
            DbOptions {
                sync_policy: policy,
                ..DbOptions::default()
            },
        )
    }

    /// Opens a durable store in `dir`, creating the directory if needed.
    ///
    /// Recovery does three things, in order: discard any `.tmp` files left by a
    /// crashed SSTable write, open every published table, then replay the
    /// write-ahead log into a fresh memtable.
    ///
    /// A crash *between* publishing a table and rotating the log leaves both
    /// copies of that data present. That is the safe direction to fail: replay
    /// puts the same entries back into the memtable, where they shadow the
    /// identical entries in the table, so reads are unaffected.
    pub fn open_with_options<P: AsRef<Path>>(dir: P, options: DbOptions) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        remove_stale_temp_files(&dir)?;
        let (tables, next_seq) = discover_tables(&dir)?;

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

        let wal = Wal::open(&wal_path, options.sync_policy)?;

        Ok(Self {
            memtable,
            wal: Some(wal),
            dir: Some(dir),
            tables,
            next_seq,
            options,
        })
    }

    /// Writes `value` at `key`, replacing any existing value.
    ///
    /// On a durable store this returns only after the mutation is in the log
    /// (and fsynced, under the default policy). If it returns `Ok`, the write
    /// will survive a crash. May trigger a memtable flush.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        if let Some(wal) = self.wal.as_mut() {
            wal.append(&Record::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            })?;
        }
        self.memtable.put(key.to_vec(), value.to_vec());
        self.maybe_flush()
    }

    /// Deletes `key`, returning `true` if a live value was visible in the
    /// memtable beforehand.
    ///
    /// The return value reflects only the memtable, not the on-disk tables —
    /// answering it accurately across every level would require a full lookup
    /// on each delete, which defeats the point of an append-only write path.
    /// Recorded as a tombstone rather than an erasure; see [`Entry`].
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        if let Some(wal) = self.wal.as_mut() {
            wal.append(&Record::Delete { key: key.to_vec() })?;
        }
        let existed = self.memtable.delete(key.to_vec());
        self.maybe_flush()?;
        Ok(existed)
    }

    /// Reads the value at `key`, or `None` if absent or deleted.
    ///
    /// Searches newest to oldest — memtable first, then each table in reverse
    /// order of creation — and stops at the first entry found, whether that is
    /// a value or a tombstone.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        match self.memtable.get_entry(key) {
            Some(Entry::Value(v)) => return Ok(Some(v.clone())),
            Some(Entry::Tombstone) => return Ok(None),
            None => {}
        }

        for table in self.tables.iter().rev() {
            match table.get(key)? {
                Some(Entry::Value(v)) => return Ok(Some(v)),
                Some(Entry::Tombstone) => return Ok(None),
                None => {}
            }
        }
        Ok(None)
    }

    /// Returns `true` if `key` currently resolves to a value.
    pub fn contains(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Freezes the memtable and writes it to a new SSTable.
    ///
    /// Returns the new table's path, or `None` if there was nothing to flush or
    /// the store is in-memory.
    ///
    /// The ordering here is the whole correctness argument: the table is fully
    /// written and fsynced, and its directory entry synced, *before* the log is
    /// rotated. Rotating first would open a window in which a crash loses every
    /// write the table was supposed to contain.
    pub fn flush(&mut self) -> io::Result<Option<PathBuf>> {
        let Some(dir) = self.dir.clone() else {
            return Ok(None);
        };
        if self.memtable.is_empty() {
            return Ok(None);
        }

        let path = dir.join(table_filename(self.next_seq));
        let mut writer = SsTableWriter::create(&path)?;
        for (key, entry) in self.memtable.iter() {
            writer.append(key, entry)?;
        }
        writer.finish()?; // fsyncs the table and its directory entry

        // Only now is it safe to discard the log.
        if let Some(wal) = self.wal.as_mut() {
            wal.rotate()?;
        }

        self.tables.push(SsTable::open(&path)?);
        self.next_seq += 1;
        self.memtable.clear();
        Ok(Some(path))
    }

    /// Flushes if the memtable has grown past the configured threshold.
    fn maybe_flush(&mut self) -> io::Result<()> {
        if self.dir.is_some() && self.memtable.size_bytes() >= self.options.flush_threshold_bytes {
            self.flush()?;
        }
        Ok(())
    }

    /// Returns every live key/value pair, merged across all levels.
    ///
    /// Newer entries shadow older ones and tombstones remove keys entirely.
    ///
    /// This materializes the whole dataset in memory and reads every table end
    /// to end. It is a diagnostic and test helper, not a hot path — a streaming
    /// merge iterator arrives with compaction.
    pub fn scan(&self) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let mut merged: BTreeMap<Vec<u8>, Entry> = BTreeMap::new();

        // Oldest first, so newer writes overwrite older ones as we go.
        for table in &self.tables {
            for item in table.iter()? {
                let (key, entry) = item?;
                merged.insert(key, entry);
            }
        }
        for (key, entry) in self.memtable.iter() {
            merged.insert(key.clone(), entry.clone());
        }

        Ok(merged
            .into_iter()
            .filter_map(|(k, e)| match e {
                Entry::Value(v) => Some((k, v)),
                Entry::Tombstone => None,
            })
            .collect())
    }

    /// Returns the number of live key/value pairs across all levels.
    ///
    /// Runs a full [`scan`](Self::scan); see the cost note there.
    pub fn len(&self) -> io::Result<usize> {
        Ok(self.scan()?.len())
    }

    /// Returns `true` if the store holds no live values.
    pub fn is_empty(&self) -> io::Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Forces any buffered log data to stable storage.
    pub fn sync(&mut self) -> io::Result<()> {
        match self.wal.as_mut() {
            Some(wal) => wal.sync(),
            None => Ok(()),
        }
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

    /// Approximate resident size of the write buffer, in bytes.
    pub fn size_bytes(&self) -> usize {
        self.memtable.size_bytes()
    }

    /// Returns the number of SSTables currently on disk.
    pub fn sstable_count(&self) -> usize {
        self.tables.len()
    }

    /// Borrows the on-disk tables, oldest first.
    pub fn tables(&self) -> &[SsTable] {
        &self.tables
    }

    /// Borrows the underlying memtable.
    pub fn memtable(&self) -> &MemTable {
        &self.memtable
    }

    /// Returns the store's configured options.
    pub fn options(&self) -> DbOptions {
        self.options
    }
}

/// Returns the filename for the table with sequence number `seq`.
///
/// Zero-padded so lexical and numeric order agree, which keeps directory
/// listings readable and makes sorting cheap.
pub fn table_filename(seq: u64) -> String {
    format!("{seq:010}.{SSTABLE_EXTENSION}")
}

/// Opens every published table in `dir`, oldest first, and returns the next
/// free sequence number.
fn discover_tables(dir: &Path) -> io::Result<(Vec<SsTable>, u64)> {
    let mut found: Vec<(u64, PathBuf)> = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some(SSTABLE_EXTENSION) {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Ok(seq) = stem.parse::<u64>() else {
            continue;
        };
        found.push((seq, path));
    }

    found.sort_by_key(|(seq, _)| *seq);
    let next_seq = found.last().map_or(0, |(seq, _)| seq + 1);

    let mut tables = Vec::with_capacity(found.len());
    for (_, path) in found {
        tables.push(SsTable::open(&path)?);
    }
    Ok((tables, next_seq))
}

/// Deletes `.tmp` files left behind by an SSTable write that never finished.
///
/// These are always garbage: a table only becomes visible via an atomic rename
/// after its contents are durable, so a surviving temp file means the write was
/// interrupted and its data is still in the write-ahead log.
fn remove_stale_temp_files(dir: &Path) -> io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("tmp") {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_put_get_delete_cycle() {
        let mut db = Db::new();
        assert!(db.is_empty().unwrap());

        db.put(b"k", b"v").unwrap();
        assert_eq!(db.get(b"k").unwrap(), Some(b"v".to_vec()));
        assert!(db.contains(b"k").unwrap());
        assert_eq!(db.len().unwrap(), 1);

        assert!(db.delete(b"k").unwrap());
        assert_eq!(db.get(b"k").unwrap(), None);
        assert!(!db.contains(b"k").unwrap());
        assert!(db.is_empty().unwrap());
    }

    #[test]
    fn deleting_an_absent_key_is_a_no_op_for_callers() {
        let mut db = Db::new();
        assert!(!db.delete(b"nope").unwrap());
    }

    #[test]
    fn an_in_memory_store_writes_no_log_and_never_flushes() {
        let mut db = Db::new();
        db.put(b"k", b"v").unwrap();
        assert!(!db.is_durable());
        assert_eq!(db.wal_size_bytes(), 0);
        assert!(db.dir().is_none());
        assert_eq!(db.flush().unwrap(), None);
        assert_eq!(db.sstable_count(), 0);
    }

    #[test]
    fn table_filenames_sort_lexically_in_numeric_order() {
        let mut names = vec![table_filename(10), table_filename(2), table_filename(1)];
        names.sort();
        assert_eq!(
            names,
            vec![table_filename(1), table_filename(2), table_filename(10)]
        );
    }
}
