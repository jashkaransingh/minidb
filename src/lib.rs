//! `minidb` — an embedded log-structured merge-tree key/value store.
//!
//! # Status
//!
//! Mutations are appended to a write-ahead log and fsynced before they are
//! acknowledged; the memtable is flushed to an immutable SSTable once it passes
//! its threshold; and size-tiered [`compaction`] merges tables to bound read
//! cost and reclaim space. Reads search the memtable and then each table
//! newest-first, filtered by key range, a [`bloom`] filter, and a sparse block
//! index.
//!
//! Every write carries a **sequence number**, and reads resolve against a
//! **snapshot** of that counter, so a reader sees a consistent point-in-time
//! view even while writes land underneath it. See [`Db::snapshot`].
//!
//! [`Db`] itself is single-threaded — it takes `&mut self` to write, so
//! exclusive access is a compile-time fact and costs nothing at runtime. For
//! multi-threaded use, [`SharedDb`] wraps it in a reader–writer lock.
//!
//! Not yet present: a streaming range-scan iterator, group-commit batching, and
//! background (rather than inline) compaction.
//!
//! # Design
//!
//! LSM trees trade read cost for write cost. Every mutation is an append — to
//! the log, then to a sorted in-memory buffer — so writes never seek and never
//! read-modify-write a page. Reads pay for that by searching newest-to-oldest
//! across the memtable and then each on-disk table, which is why bloom filters
//! and a sparse per-table index matter so much.
//!
//! # MVCC visibility rule
//!
//! This is the invariant every layer below has to agree on, stated once:
//!
//! > A read at snapshot `S` resolves `key` to the version of `key` with the
//! > **greatest sequence number `≤ S`**, searching the memtable first and then
//! > the on-disk tables newest-first. If that version is a tombstone the key
//! > reads as absent; if no such version exists in any level, the key reads as
//! > absent. Versions with sequence number `> S` are invisible and are never
//! > consulted.
//!
//! Three consequences worth spelling out, because each is a place the rule is
//! easy to break:
//!
//! - **An overwrite adds a version, it does not replace one.** `put` at seq 9
//!   leaves the seq 4 version in place; a snapshot at 5 still reads it. Storage
//!   is keyed by [`memtable::InternalKey`] — `(user_key, seq)` ordered *user key
//!   ascending, seq descending* — so all versions of a key are adjacent and run
//!   newest-first, and the visible version is one seek away.
//! - **A tombstone is a version, not an erasure.** A delete at seq 7 makes the
//!   key absent for snapshots ≥ 7 and leaves it readable for snapshots < 7. A
//!   search that finds a tombstone at or below its snapshot must *stop* — it
//!   must not fall through to an older level, or the delete reverts.
//! - **The first level that has any visible version wins outright.** Levels are
//!   searched newest-first, so a hit stops the search even if an older level
//!   holds a version with a *higher* sequence number — which cannot happen,
//!   because compaction only ever moves versions downward and never reorders
//!   them.
//!
//! Old versions are reclaimed only by [`compaction::merge_into`], and only once
//! no live snapshot can reach them.
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
//! A snapshot pins a point in time:
//!
//! ```
//! use minidb::Db;
//!
//! let mut db = Db::new();
//! db.put(b"k", b"before")?;
//!
//! let snap = db.snapshot();
//! db.put(b"k", b"after")?;
//!
//! assert_eq!(db.get(b"k")?, Some(b"after".to_vec()));
//! assert_eq!(db.get_at(&snap, b"k")?, Some(b"before".to_vec()));
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
pub mod concurrent;
pub mod fault;
pub mod memtable;
pub mod snapshot;
pub mod sstable;
pub mod wal;

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use compaction::{Marker, merge_into, plan};

pub use compaction::{COMPACTION_MARKER, CompactionConfig, CompactionTask, TableInfo};
pub use concurrent::SharedDb;
pub use fault::FaultPlan;
pub use memtable::{Entry, InternalKey, MemTable};
pub use snapshot::{Snapshot, SnapshotRegistry};
pub use sstable::{SsTable, SsTableWriter, TableMeta};
pub use wal::{Record, Recovery, SyncPolicy, Wal};

/// Default size at which a memtable is frozen and flushed to an SSTable.
pub const MEMTABLE_FLUSH_THRESHOLD_BYTES: usize = 4 * 1024 * 1024;

/// Filename of the write-ahead log within a store directory.
pub const WAL_FILENAME: &str = "wal.log";

/// Extension given to SSTable files.
pub const SSTABLE_EXTENSION: &str = "sst";

/// Tunables for a durable store.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct DbOptions {
    /// How aggressively the write-ahead log is fsynced.
    pub sync_policy: SyncPolicy,
    /// Memtable size that triggers a flush to a new SSTable.
    pub flush_threshold_bytes: usize,
    /// Thresholds governing when tables are merged.
    pub compaction: CompactionConfig,
    /// Whether a flush automatically triggers any compaction it makes due.
    pub auto_compact: bool,
    /// Scripted failure point, for crash testing. See [`fault`].
    pub fault: FaultPlan,
}

impl Default for DbOptions {
    fn default() -> Self {
        Self {
            sync_policy: SyncPolicy::EveryWrite,
            flush_threshold_bytes: MEMTABLE_FLUSH_THRESHOLD_BYTES,
            compaction: CompactionConfig::default(),
            auto_compact: true,
            fault: FaultPlan::none(),
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
    tables: Vec<OpenTable>,
    /// Next free recency slot for a table filename. Unrelated to write
    /// sequence numbers, which version the *data*.
    next_table_seq: u64,
    /// Sequence number the next mutation will be assigned.
    next_write_seq: u64,
    /// Highest sequence number that is durable and applied. Every read resolves
    /// against this unless given an explicit snapshot.
    visible_seq: u64,
    snapshots: Arc<SnapshotRegistry>,
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
            next_table_seq: 0,
            next_write_seq: 1,
            visible_seq: 0,
            snapshots: Arc::new(SnapshotRegistry::new()),
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
    /// Recovery runs in order: finish or roll back any interrupted compaction,
    /// discard `.tmp` files left by a crashed SSTable write, open every
    /// published table, then replay the write-ahead log into a fresh memtable.
    ///
    /// A crash *between* publishing a table and rotating the log leaves both
    /// copies of that data present. That is the safe direction to fail: replay
    /// puts the same entries back into the memtable, where they shadow the
    /// identical entries in the table, so reads are unaffected.
    ///
    /// The write sequence counter is rebuilt from the highest sequence number
    /// found anywhere — in the log *or* in any table's metadata. Tables are the
    /// binding half: after a flush the log is empty, so they are the only record
    /// of how far the counter had advanced.
    pub fn open_with_options<P: AsRef<Path>>(dir: P, options: DbOptions) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;

        // Finish or roll back a compaction interrupted by a crash, before any
        // table is opened — otherwise a merge that dropped tombstones could be
        // read alongside the inputs it was replacing.
        compaction::recover(&dir)?;
        remove_stale_temp_files(&dir)?;
        let (tables, next_table_seq) = discover_tables(&dir)?;

        let wal_path = dir.join(WAL_FILENAME);
        let recovery = Wal::replay(&wal_path)?;

        let mut memtable = MemTable::new();
        for record in &recovery.records {
            memtable.insert(&record.key, record.seq, record.entry.clone());
        }

        let table_max_seq = tables
            .iter()
            .map(|t| t.table.meta().max_seq)
            .max()
            .unwrap_or(0);
        let max_seq = table_max_seq.max(recovery.max_seq());

        let mut wal = Wal::open(&wal_path, options.sync_policy)?;
        wal.set_fault_plan(options.fault);

        Ok(Self {
            memtable,
            wal: Some(wal),
            dir: Some(dir),
            tables,
            next_table_seq,
            next_write_seq: max_seq + 1,
            visible_seq: max_seq,
            snapshots: Arc::new(SnapshotRegistry::new()),
            options,
        })
    }

    /// Writes `value` at `key`, replacing any existing value.
    ///
    /// On a durable store this returns only after the mutation is in the log
    /// (and fsynced, under the default policy). If it returns `Ok`, the write
    /// will survive a crash. May trigger a memtable flush.
    ///
    /// Under MVCC this *adds a version*: readers holding an older snapshot keep
    /// seeing what they saw.
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.apply(key, Entry::Value(value.to_vec()))
    }

    /// Deletes `key`, returning `true` if a live value was visible beforehand.
    ///
    /// Recorded as a tombstone rather than an erasure: older tables on disk are
    /// immutable and may still hold a value, and older snapshots must keep
    /// reading it. See [`Entry`].
    pub fn delete(&mut self, key: &[u8]) -> io::Result<bool> {
        let existed = self.get(key)?.is_some();
        self.apply(key, Entry::Tombstone)?;
        Ok(existed)
    }

    /// Assigns a sequence number, logs the mutation, then applies it.
    ///
    /// The ordering is the durability argument: nothing becomes *visible* until
    /// it is durable. `visible_seq` is advanced only after both the log append
    /// and the memtable insert have succeeded, so a reader can never observe a
    /// write that a crash would then lose.
    fn apply(&mut self, key: &[u8], entry: Entry) -> io::Result<()> {
        let seq = self.next_write_seq;
        self.next_write_seq += 1;

        if let Some(wal) = self.wal.as_mut() {
            wal.append(&Record {
                seq,
                key: key.to_vec(),
                entry: entry.clone(),
            })?;
        }
        self.memtable.insert(key, seq, entry);
        self.visible_seq = seq;
        self.maybe_flush()
    }

    /// Takes a snapshot of the current sequence number.
    ///
    /// Reads through the returned handle see exactly the writes that had been
    /// acknowledged when it was taken, and none of the ones that land later —
    /// for as long as the handle is alive. Holding one also pins the versions it
    /// can reach against collection by compaction, so a long-lived snapshot
    /// costs space; drop it when done.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::acquire(Arc::clone(&self.snapshots), self.visible_seq)
    }

    /// Reads the value at `key` as of the latest acknowledged write.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_at_seq(self.visible_seq, key)
    }

    /// Reads the value at `key` as of `snapshot`.
    pub fn get_at(&self, snapshot: &Snapshot, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_at_seq(snapshot.seq(), key)
    }

    /// The visibility rule, implemented once.
    ///
    /// Searches newest level to oldest and stops at the first version at or
    /// below `snapshot` — value or tombstone. Stopping on a tombstone is the
    /// part that matters: continuing would find the value the tombstone exists
    /// to hide and silently un-delete it.
    pub fn get_at_seq(&self, snapshot: u64, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        if let Some(entry) = self.memtable.get(key, snapshot) {
            return Ok(entry.value().map(<[u8]>::to_vec));
        }
        for open in self.tables.iter().rev() {
            if let Some((_, entry)) = open.table.get(key, snapshot)? {
                return Ok(entry.value().map(|v| v.to_vec()));
            }
        }
        Ok(None)
    }

    /// Returns `true` if `key` currently resolves to a value.
    pub fn contains(&self, key: &[u8]) -> io::Result<bool> {
        Ok(self.get(key)?.is_some())
    }

    /// Returns the sequence number of the latest acknowledged write.
    pub fn current_seq(&self) -> u64 {
        self.visible_seq
    }

    /// Borrows the registry of live snapshots.
    pub fn snapshots(&self) -> &Arc<SnapshotRegistry> {
        &self.snapshots
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
    ///
    /// Every version in the memtable is written, not just the newest per key —
    /// the memtable's iteration order *is* the table's required order, and
    /// dropping versions here would break snapshots that are still open.
    pub fn flush(&mut self) -> io::Result<Option<PathBuf>> {
        let Some(dir) = self.dir.clone() else {
            return Ok(None);
        };
        if self.memtable.is_empty() {
            return Ok(None);
        }

        let seq = self.next_table_seq;
        let path = dir.join(table_filename(seq, 0));
        let mut writer = SsTableWriter::create(&path)?;
        for (key, entry) in self.memtable.iter() {
            writer.append(&key.user_key, key.seq, entry)?;
        }
        writer.finish()?; // fsyncs the table and its directory entry

        // Only now is it safe to discard the log.
        if let Some(wal) = self.wal.as_mut() {
            wal.rotate()?;
        }

        self.tables.push(OpenTable {
            table: SsTable::open(&path)?,
            seq,
            generation: 0,
        });
        self.next_table_seq += 1;
        self.memtable.clear();

        if self.options.auto_compact {
            self.compact_all()?;
        }
        Ok(Some(path))
    }

    /// Runs one compaction if any size tier is over its threshold.
    ///
    /// Returns `true` if tables were merged. The swap is journalled, so a crash
    /// at any point leaves the store consistent — see [`compaction`].
    pub fn compact(&mut self) -> io::Result<bool> {
        let Some(dir) = self.dir.clone() else {
            return Ok(false);
        };

        let infos: Vec<TableInfo> = self
            .tables
            .iter()
            .map(|open| TableInfo {
                path: open.table.path().to_path_buf(),
                seq: open.seq,
                generation: open.generation,
                size_bytes: open.table.size_bytes(),
            })
            .collect();

        let Some(task) = plan(&infos, &self.options.compaction) else {
            return Ok(false);
        };

        let (seq, generation) = task.output_slot();
        let output = dir.join(table_filename(seq, generation));
        let inputs = task.input_paths();

        // Journal the swap before publishing anything, so recovery can finish
        // whichever half a crash interrupts.
        Marker {
            output: output.clone(),
            inputs: inputs.clone(),
        }
        .write(&dir)?;

        let input_tables = inputs
            .iter()
            .map(SsTable::open)
            .collect::<io::Result<Vec<_>>>()?;
        merge_into(
            &input_tables,
            &output,
            task.drop_tombstones,
            self.oldest_snapshot(),
        )?;
        drop(input_tables);

        // The output is durable now; retiring the inputs is safe.
        for input in &inputs {
            if input.exists() {
                std::fs::remove_file(input)?;
            }
        }
        wal::sync_parent_dir(&output)?;
        Marker::clear(&dir)?;

        self.reload_tables()?;
        Ok(true)
    }

    /// The sequence number below which versions are unreachable.
    ///
    /// The oldest live snapshot if there is one, otherwise the current sequence
    /// — with no readers pinned to the past, only the newest version of each key
    /// is reachable. Taking the *minimum* is what makes this safe against a
    /// snapshot created later: any future snapshot has a sequence number at
    /// least this high, so anything kept for this one covers it too.
    fn oldest_snapshot(&self) -> u64 {
        self.snapshots.oldest().unwrap_or(self.visible_seq)
    }

    /// Compacts repeatedly until no tier is over its threshold.
    ///
    /// Returns how many compactions ran.
    pub fn compact_all(&mut self) -> io::Result<usize> {
        let mut rounds = 0;
        // Each round strictly reduces the table count, so this terminates; the
        // bound is a backstop against a planner bug turning into a hang.
        while self.compact()? {
            rounds += 1;
            if rounds > 1_000 {
                break;
            }
        }
        Ok(rounds)
    }

    /// Re-reads the set of tables on disk.
    fn reload_tables(&mut self) -> io::Result<()> {
        if let Some(dir) = self.dir.clone() {
            let (tables, next_table_seq) = discover_tables(&dir)?;
            self.tables = tables;
            self.next_table_seq = self.next_table_seq.max(next_table_seq);
        }
        Ok(())
    }

    /// Flushes if the memtable has grown past the configured threshold.
    fn maybe_flush(&mut self) -> io::Result<()> {
        if self.dir.is_some() && self.memtable.size_bytes() >= self.options.flush_threshold_bytes {
            self.flush()?;
        }
        Ok(())
    }

    /// Returns every live key/value pair as of the latest acknowledged write.
    ///
    /// This materializes the whole dataset in memory and reads every table end
    /// to end. It is a diagnostic and test helper, not a hot path — a streaming
    /// merge iterator is the next milestone.
    pub fn scan(&self) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        self.scan_at_seq(self.visible_seq)
    }

    /// Returns every live key/value pair as of `snapshot`.
    pub fn scan_at(&self, snapshot: &Snapshot) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        self.scan_at_seq(snapshot.seq())
    }

    fn scan_at_seq(&self, snapshot: u64) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        // Newest level first, and the first version seen for a key wins — the
        // same rule `get_at_seq` follows, applied to a whole level at a time.
        let mut winners: BTreeMap<Vec<u8>, (u64, Entry)> = BTreeMap::new();

        let mut consider = |key: Vec<u8>, seq: u64, entry: Entry| {
            if seq > snapshot {
                return;
            }
            match winners.get(&key) {
                Some((existing, _)) if *existing >= seq => {}
                _ => {
                    winners.insert(key, (seq, entry));
                }
            }
        };

        for (key, entry) in self.memtable.iter() {
            consider(key.user_key.clone(), key.seq, entry.clone());
        }
        for open in self.tables.iter().rev() {
            for item in open.table.iter()? {
                let (key, seq, entry) = item?;
                consider(key, seq, entry);
            }
        }

        Ok(winners
            .into_iter()
            .filter_map(|(k, (_, e))| match e {
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
    pub fn tables(&self) -> Vec<&SsTable> {
        self.tables.iter().map(|open| &open.table).collect()
    }

    /// Returns the recency slots `(seq, generation)` of the tables on disk,
    /// oldest first.
    pub fn table_slots(&self) -> Vec<(u64, u32)> {
        self.tables.iter().map(|o| (o.seq, o.generation)).collect()
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

/// A table on disk together with the recency slot it occupies.
#[derive(Debug)]
struct OpenTable {
    table: SsTable,
    /// Recency rank. Higher is newer.
    seq: u64,
    /// Generation within a `seq`, bumped each time compaction rewrites the slot.
    generation: u32,
}

/// Returns the filename for the table in recency slot `(seq, generation)`.
///
/// Both components are zero-padded so lexical and numeric order agree, which
/// keeps directory listings readable and sorting cheap.
///
/// `generation` exists because compaction must give its output the recency position of
/// its newest input — otherwise the merged table would leapfrog tables that were
/// not part of the merge and silently shadow their newer values. The output
/// therefore reuses that input's `seq` and takes the next `generation`, which keeps the
/// ordering right while still producing a distinct filename.
pub fn table_filename(seq: u64, generation: u32) -> String {
    format!("{seq:010}-{generation:04}.{SSTABLE_EXTENSION}")
}

/// Parses a table filename stem back into its recency slot.
fn parse_table_stem(stem: &str) -> Option<(u64, u32)> {
    let (seq, generation) = stem.split_once('-')?;
    Some((seq.parse().ok()?, generation.parse().ok()?))
}

/// Opens every published table in `dir`, oldest first, and returns the next
/// free sequence number.
fn discover_tables(dir: &Path) -> io::Result<(Vec<OpenTable>, u64)> {
    let mut found: Vec<(u64, u32, PathBuf)> = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some(SSTABLE_EXTENSION) {
            continue;
        }
        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some((seq, generation)) = parse_table_stem(stem) else {
            continue;
        };
        found.push((seq, generation, path));
    }

    // Oldest first: by recency slot, generation breaking ties within a slot.
    found.sort_by_key(|(seq, generation, _)| (*seq, *generation));
    let next_table_seq = found.last().map_or(0, |(seq, _, _)| seq + 1);

    let mut tables = Vec::with_capacity(found.len());
    for (seq, generation, path) in found {
        tables.push(OpenTable {
            table: SsTable::open(&path)?,
            seq,
            generation,
        });
    }
    Ok((tables, next_table_seq))
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
    fn sequence_numbers_increase_by_one_per_mutation() {
        let mut db = Db::new();
        assert_eq!(db.current_seq(), 0);
        db.put(b"a", b"1").unwrap();
        assert_eq!(db.current_seq(), 1);
        db.put(b"b", b"2").unwrap();
        assert_eq!(db.current_seq(), 2);
        db.delete(b"a").unwrap();
        assert_eq!(db.current_seq(), 3);
    }

    #[test]
    fn a_snapshot_is_unaffected_by_later_writes() {
        let mut db = Db::new();
        db.put(b"k", b"v1").unwrap();

        let snap = db.snapshot();
        db.put(b"k", b"v2").unwrap();
        db.put(b"fresh", b"new").unwrap();
        db.delete(b"k").unwrap();

        assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v1".to_vec()));
        assert_eq!(db.get_at(&snap, b"fresh").unwrap(), None);
        assert_eq!(db.get(b"k").unwrap(), None);
        assert_eq!(db.get(b"fresh").unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn a_snapshot_taken_before_a_delete_still_reads_the_value() {
        let mut db = Db::new();
        db.put(b"k", b"v").unwrap();
        let snap = db.snapshot();
        db.delete(b"k").unwrap();

        assert_eq!(db.get(b"k").unwrap(), None);
        assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn scan_at_a_snapshot_sees_the_state_of_that_moment() {
        let mut db = Db::new();
        db.put(b"a", b"1").unwrap();
        db.put(b"b", b"2").unwrap();

        let snap = db.snapshot();
        db.put(b"c", b"3").unwrap();
        db.delete(b"a").unwrap();

        let then = db.scan_at(&snap).unwrap();
        assert_eq!(then.len(), 2);
        assert_eq!(then.get(b"a".as_slice()), Some(&b"1".to_vec()));
        assert!(!then.contains_key(b"c".as_slice()));

        let now = db.scan().unwrap();
        assert_eq!(now.len(), 2);
        assert!(!now.contains_key(b"a".as_slice()));
        assert!(now.contains_key(b"c".as_slice()));
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
    fn table_filenames_sort_lexically_in_recency_order() {
        let mut names = vec![
            table_filename(10, 0),
            table_filename(2, 3),
            table_filename(2, 0),
            table_filename(1, 0),
        ];
        names.sort();
        assert_eq!(
            names,
            vec![
                table_filename(1, 0),
                table_filename(2, 0),
                table_filename(2, 3),
                table_filename(10, 0),
            ]
        );
    }

    #[test]
    fn table_filenames_round_trip_through_parsing() {
        for (seq, generation) in [(0u64, 0u32), (7, 2), (123_456, 9_999)] {
            let name = table_filename(seq, generation);
            let stem = name.strip_suffix(".sst").unwrap();
            assert_eq!(parse_table_stem(stem), Some((seq, generation)));
        }
        assert_eq!(parse_table_stem("not-a-number"), None);
        assert_eq!(parse_table_stem("0000000001"), None);
    }
}
