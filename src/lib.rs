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
//! [`Db`] is a cheap cloneable handle — `Clone + Send + Sync`, every method
//! `&self` — and **reads never block behind writes**: a reader clones an
//! immutable view of the levels and then searches it holding no lock at all.
//! See [`Db`] and the `skiplist` module for how, and for what it costs.
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
//! let db = Db::new();
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
//! let db = Db::new();
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
//! let db = Db::open(&dir)?;
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
pub mod skiplist;
pub mod snapshot;
pub mod sstable;
pub mod wal;

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

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
///
/// # Concurrency
///
/// `Db` is a cheap cloneable handle: `Clone`, `Send`, and `Sync`, with every
/// method taking `&self`. Clones share one store. The design goal is stated as
/// an invariant:
///
/// > **A read never waits on a write, and a read never makes a write wait.**
///
/// How that is achieved, and what it costs, is documented on [`Versions`].
#[derive(Debug, Clone)]
pub struct Db {
    core: Arc<Core>,
}

/// The shared state behind every [`Db`] handle.
#[derive(Debug)]
struct Core {
    dir: Option<PathBuf>,
    options: DbOptions,
    /// The current immutable view of everything a reader must search.
    ///
    /// The lock guards only the *pointer*. A reader takes it, clones one `Arc`,
    /// and releases it — a handful of nanoseconds, never held across I/O.
    versions: RwLock<Arc<Versions>>,
    /// Serializes writers and owns the log. Never taken by a read path.
    writer: Mutex<Writer>,
    /// Sequence number the next mutation will be assigned.
    next_write_seq: AtomicU64,
    /// Highest sequence number that is durable *and* applied. This is the
    /// commit point: a write becomes visible when, and only when, this advances
    /// past it.
    visible_seq: AtomicU64,
    snapshots: Arc<SnapshotRegistry>,
}

/// An immutable snapshot of the levels a read has to search.
///
/// # Why this shape
///
/// The old design put the whole store behind one `RwLock`, so a write held that
/// lock across its `fsync` and every reader queued behind the disk. Fixing that
/// is not a matter of choosing a different lock — it needs the reader to stop
/// sharing mutable state with the writer at all.
///
/// So the reader's view is an immutable value, swapped rather than mutated:
///
/// - **SSTables are already immutable.** Holding `Arc`s to them is free.
/// - **The memtable is made effectively immutable** by using an insert-only
///   structure ([`skiplist`]) whose reads are lock-free and whose nodes are
///   never removed or rewritten. A writer appending to it cannot invalidate
///   anything a reader is looking at.
/// - **Structural changes — flush, compaction — build a new `Versions` and swap
///   the pointer.** A reader that already cloned the old one keeps using it,
///   consistently, until it finishes.
///
/// A read therefore holds a lock for exactly as long as it takes to clone an
/// `Arc`, and then does all its work — memtable probes, block reads, bloom
/// checks — holding nothing.
///
/// # The trade-offs, stated honestly
///
/// - **Writers are still serialized with each other.** One log, one memtable
///   writer. That is a deliberate limit, not an oversight: it is what makes the
///   memtable a *single-writer* structure, which is dramatically simpler to get
///   right than a multi-writer lock-free one, and it is what group commit will
///   turn into an advantage rather than a bottleneck.
/// - **Flush and compaction hold the writer mutex**, so a large merge stalls
///   *writes*. It does not stall reads, which is the claim being made.
/// - **Compaction unlinks input files while readers may still hold them.** Safe
///   on Unix, where an open descriptor keeps an unlinked file readable — which
///   is why [`SsTable`] holds its descriptor open. Not safe on Windows; see the
///   note on that type.
/// - **A reader holding an old `Versions` can read stale-but-consistent data**
///   for the duration of one operation. That is exactly snapshot semantics, so
///   it is the intended behaviour rather than a compromise.
#[derive(Debug)]
struct Versions {
    /// The memtable currently accepting writes.
    mem: Arc<MemTable>,
    /// Frozen memtables not yet written to disk, oldest first. Still fully
    /// readable — a flush must not make data disappear mid-flight.
    imm: Vec<Arc<MemTable>>,
    /// Tables ordered oldest first; later entries shadow earlier ones.
    tables: Vec<OpenTable>,
    /// Next free recency slot for a table filename. Unrelated to write
    /// sequence numbers, which version the *data*.
    next_table_seq: u64,
}

impl Versions {
    /// A view with an empty memtable and the given tables.
    fn initial(tables: Vec<OpenTable>, next_table_seq: u64, mem: MemTable) -> Self {
        Self {
            mem: Arc::new(mem),
            imm: Vec::new(),
            tables,
            next_table_seq,
        }
    }
}

/// State only a writer touches.
#[derive(Debug)]
struct Writer {
    wal: Option<Wal>,
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
        Self::from_parts(
            None,
            DbOptions::default(),
            Versions::initial(Vec::new(), 0, MemTable::new()),
            Writer { wal: None },
            0,
        )
    }

    fn from_parts(
        dir: Option<PathBuf>,
        options: DbOptions,
        versions: Versions,
        writer: Writer,
        max_seq: u64,
    ) -> Self {
        Self {
            core: Arc::new(Core {
                dir,
                options,
                versions: RwLock::new(Arc::new(versions)),
                writer: Mutex::new(writer),
                next_write_seq: AtomicU64::new(max_seq + 1),
                visible_seq: AtomicU64::new(max_seq),
                snapshots: Arc::new(SnapshotRegistry::new()),
            }),
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

        Ok(Self::from_parts(
            Some(dir),
            options,
            Versions::initial(tables, next_table_seq, memtable),
            Writer { wal: Some(wal) },
            max_seq,
        ))
    }

    /// Clones the current view. Holds the lock only long enough to bump a
    /// refcount — never across I/O.
    fn versions(&self) -> Arc<Versions> {
        Arc::clone(&self.core.versions.read().unwrap_or_else(|e| e.into_inner()))
    }

    /// Takes the writer mutex.
    ///
    /// Poisoning is recovered from rather than propagated. The state this guards
    /// is a log handle and an insert-only memtable; neither has an invariant
    /// that spans operations, so a panicking writer cannot leave a half-updated
    /// structure behind.
    fn writer(&self) -> std::sync::MutexGuard<'_, Writer> {
        self.core.writer.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Installs a new view.
    fn publish(&self, versions: Versions) {
        let mut guard = self
            .core
            .versions
            .write()
            .unwrap_or_else(|e| e.into_inner());
        *guard = Arc::new(versions);
    }

    /// Writes `value` at `key`, replacing any existing value.
    ///
    /// On a durable store this returns only after the mutation is in the log
    /// (and fsynced, under the default policy). If it returns `Ok`, the write
    /// will survive a crash. May trigger a memtable flush.
    ///
    /// Under MVCC this *adds a version*: readers holding an older snapshot keep
    /// seeing what they saw.
    pub fn put(&self, key: &[u8], value: &[u8]) -> io::Result<()> {
        self.apply(key, Entry::Value(value.to_vec()))
    }

    /// Deletes `key`, returning `true` if a live value was visible beforehand.
    ///
    /// Recorded as a tombstone rather than an erasure: older tables on disk are
    /// immutable and may still hold a value, and older snapshots must keep
    /// reading it. See [`Entry`].
    pub fn delete(&self, key: &[u8]) -> io::Result<bool> {
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
    fn apply(&self, key: &[u8], entry: Entry) -> io::Result<()> {
        let mut writer = self.writer();
        let seq = self.core.next_write_seq.fetch_add(1, Ordering::SeqCst);

        if let Some(wal) = writer.wal.as_mut() {
            wal.append(&Record {
                seq,
                key: key.to_vec(),
                entry: entry.clone(),
            })?;
        }

        let versions = self.versions();
        // SAFETY: the memtable requires a single writer at a time, and the
        // writer mutex held above is exactly that guarantee. Readers running
        // concurrently are fine and are the point of the structure.
        unsafe { versions.mem.insert_shared(key, seq, entry) };

        // The commit point. Release pairs with the Acquire in `current_seq`, so
        // a reader that sees this sequence necessarily sees the inserted node.
        self.core.visible_seq.store(seq, Ordering::Release);

        let full = self.core.dir.is_some()
            && versions.mem.size_bytes() >= self.core.options.flush_threshold_bytes;
        drop(versions);

        if full {
            self.flush_locked(&mut writer)?;
        }
        Ok(())
    }

    /// Takes a snapshot of the current sequence number.
    ///
    /// Reads through the returned handle see exactly the writes that had been
    /// acknowledged when it was taken, and none of the ones that land later —
    /// for as long as the handle is alive. Holding one also pins the versions it
    /// can reach against collection by compaction, so a long-lived snapshot
    /// costs space; drop it when done.
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::acquire(Arc::clone(&self.core.snapshots), self.current_seq())
    }

    /// Reads the value at `key` as of the latest acknowledged write.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        self.get_at_seq(self.current_seq(), key)
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
    ///
    /// No lock is held for any of it. The view is cloned up front and the search
    /// runs against that fixed set of levels, so a flush or compaction landing
    /// mid-read changes nothing about the answer.
    pub fn get_at_seq(&self, snapshot: u64, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        let versions = self.versions();

        if let Some(entry) = versions.mem.get(key, snapshot) {
            return Ok(entry.value().map(<[u8]>::to_vec));
        }
        // Frozen memtables, newest first — they sit between the active memtable
        // and the tables in recency order.
        for mem in versions.imm.iter().rev() {
            if let Some(entry) = mem.get(key, snapshot) {
                return Ok(entry.value().map(<[u8]>::to_vec));
            }
        }
        for open in versions.tables.iter().rev() {
            if let Some((_, entry)) = open.table.get(key, snapshot)? {
                return Ok(entry.value().map(<[u8]>::to_vec));
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
        self.core.visible_seq.load(Ordering::Acquire)
    }

    /// Borrows the registry of live snapshots.
    pub fn snapshots(&self) -> &Arc<SnapshotRegistry> {
        &self.core.snapshots
    }

    /// Freezes the memtable and writes it to a new SSTable.
    ///
    /// Returns the path of the last table written, or `None` if there was
    /// nothing to flush or the store is in-memory.
    pub fn flush(&self) -> io::Result<Option<PathBuf>> {
        let mut writer = self.writer();
        self.flush_locked(&mut writer)
    }

    /// Flush, given the writer mutex is already held.
    ///
    /// Three phases, and the split is the whole point:
    ///
    /// 1. **Freeze** — under a momentary write lock, move the active memtable
    ///    into `imm` and install a fresh one. Readers keep finding the frozen
    ///    data because `imm` is searched; nothing disappears mid-flight.
    /// 2. **Write** — serialize each frozen memtable to a table, holding *no*
    ///    lock on the view. This is the slow part, and readers run through it.
    /// 3. **Publish** — under another momentary write lock, drop the frozen
    ///    memtables and add the new tables.
    ///
    /// The log is rotated between 2 and 3, and only once every frozen memtable
    /// is durable on disk. Rotating earlier would open a window where a crash
    /// loses data that is in neither place.
    fn flush_locked(&self, writer: &mut Writer) -> io::Result<Option<PathBuf>> {
        let Some(dir) = self.core.dir.clone() else {
            return Ok(None);
        };

        // Phase 1: freeze, if there is anything to freeze.
        {
            let current = self.versions();
            if !current.mem.is_empty() {
                let mut imm = current.imm.clone();
                imm.push(Arc::clone(&current.mem));
                self.publish(Versions {
                    mem: Arc::new(MemTable::new()),
                    imm,
                    tables: current.tables.clone(),
                    next_table_seq: current.next_table_seq,
                });
            }
        }

        let pending = self.versions().imm.clone();
        if pending.is_empty() {
            return Ok(None);
        }

        // Phase 2: write each frozen memtable out, holding no view lock. A
        // failure here leaves the memtables in `imm` — still readable, still in
        // the log — and the next flush retries them.
        let mut written = Vec::with_capacity(pending.len());
        let mut next_seq = self.versions().next_table_seq;
        for frozen in &pending {
            let path = dir.join(table_filename(next_seq, 0));
            let mut table_writer = SsTableWriter::create(&path)?;
            for (key, entry) in frozen.iter() {
                table_writer.append(&key.user_key, key.seq, entry)?;
            }
            table_writer.finish()?; // fsyncs the table and its directory entry
            written.push(OpenTable {
                table: Arc::new(SsTable::open(&path)?),
                seq: next_seq,
                generation: 0,
            });
            next_seq += 1;
        }

        // Every frozen memtable is now durable on disk, so the log may go. No
        // write can have slipped in: this thread holds the writer mutex.
        if let Some(wal) = writer.wal.as_mut() {
            wal.rotate()?;
        }

        // Phase 3: publish the tables and retire the frozen memtables.
        let last_path = written.last().map(|t| t.table.path().to_path_buf());
        {
            let current = self.versions();
            let mut tables = current.tables.clone();
            tables.extend(written);
            self.publish(Versions {
                mem: Arc::clone(&current.mem),
                imm: current
                    .imm
                    .iter()
                    .filter(|m| !pending.iter().any(|p| Arc::ptr_eq(p, m)))
                    .cloned()
                    .collect(),
                tables,
                next_table_seq: next_seq,
            });
        }

        if self.core.options.auto_compact {
            self.compact_all_locked(writer)?;
        }
        Ok(last_path)
    }

    /// Runs one compaction if any size tier is over its threshold.
    ///
    /// Returns `true` if tables were merged. The swap is journalled, so a crash
    /// at any point leaves the store consistent — see [`compaction`].
    pub fn compact(&self) -> io::Result<bool> {
        let mut writer = self.writer();
        self.compact_locked(&mut writer)
    }

    /// Compaction, given the writer mutex is already held.
    ///
    /// The merge itself runs with no view lock held, so readers are untouched by
    /// it. Only the final table-list swap takes one, for the duration of a
    /// pointer store.
    ///
    /// Retiring the inputs *unlinks files a reader may still be reading*. That
    /// is safe because [`SsTable`] holds its descriptor open and an unlinked
    /// file stays readable through it on Unix — see that type's docs for the
    /// Windows caveat.
    fn compact_locked(&self, _writer: &mut Writer) -> io::Result<bool> {
        let Some(dir) = self.core.dir.clone() else {
            return Ok(false);
        };

        let current = self.versions();
        let infos: Vec<TableInfo> = current
            .tables
            .iter()
            .map(|open| TableInfo {
                path: open.table.path().to_path_buf(),
                seq: open.seq,
                generation: open.generation,
                size_bytes: open.table.size_bytes(),
            })
            .collect();

        let Some(task) = plan(&infos, &self.core.options.compaction) else {
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

        let merged = OpenTable {
            table: Arc::new(SsTable::open(&output)?),
            seq,
            generation,
        };

        // Swap the merged table in for its inputs. They are contiguous in
        // recency order, so replacing the run in place preserves every other
        // table's position — which is the invariant that stops a merge output
        // from shadowing tables it never merged.
        {
            let current = self.versions();
            let mut tables = Vec::with_capacity(current.tables.len());
            let mut placed = false;
            for open in &current.tables {
                if inputs.iter().any(|p| p == open.table.path()) {
                    if !placed {
                        tables.push(merged.clone());
                        placed = true;
                    }
                } else {
                    tables.push(open.clone());
                }
            }
            self.publish(Versions {
                mem: Arc::clone(&current.mem),
                imm: current.imm.clone(),
                tables,
                next_table_seq: current.next_table_seq,
            });
        }

        // The output is durable and published; retiring the inputs is safe.
        // Readers still holding the previous view keep their open descriptors.
        for input in &inputs {
            if input.exists() {
                std::fs::remove_file(input)?;
            }
        }
        wal::sync_parent_dir(&output)?;
        Marker::clear(&dir)?;

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
        self.core.snapshots.oldest().unwrap_or(self.current_seq())
    }

    /// Compacts repeatedly until no tier is over its threshold.
    ///
    /// Returns how many compactions ran.
    pub fn compact_all(&self) -> io::Result<usize> {
        let mut writer = self.writer();
        self.compact_all_locked(&mut writer)
    }

    fn compact_all_locked(&self, writer: &mut Writer) -> io::Result<usize> {
        let mut rounds = 0;
        // Each round strictly reduces the table count, so this terminates; the
        // bound is a backstop against a planner bug turning into a hang.
        while self.compact_locked(writer)? {
            rounds += 1;
            if rounds > 1_000 {
                break;
            }
        }
        Ok(rounds)
    }

    /// Returns every live key/value pair as of the latest acknowledged write.
    ///
    /// This materializes the whole dataset in memory and reads every table end
    /// to end. It is a diagnostic and test helper, not a hot path — a streaming
    /// merge iterator is the next milestone.
    pub fn scan(&self) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        self.scan_at_seq(self.current_seq())
    }

    /// Returns every live key/value pair as of `snapshot`.
    pub fn scan_at(&self, snapshot: &Snapshot) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        self.scan_at_seq(snapshot.seq())
    }

    fn scan_at_seq(&self, snapshot: u64) -> io::Result<BTreeMap<Vec<u8>, Vec<u8>>> {
        let versions = self.versions();

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

        for (key, entry) in versions.mem.iter() {
            consider(key.user_key.clone(), key.seq, entry.clone());
        }
        for mem in versions.imm.iter().rev() {
            for (key, entry) in mem.iter() {
                consider(key.user_key.clone(), key.seq, entry.clone());
            }
        }
        for open in versions.tables.iter().rev() {
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
    pub fn sync(&self) -> io::Result<()> {
        match self.writer().wal.as_mut() {
            Some(wal) => wal.sync(),
            None => Ok(()),
        }
    }

    /// Returns `true` if this store is backed by a write-ahead log.
    pub fn is_durable(&self) -> bool {
        self.core.dir.is_some()
    }

    /// Returns the store's directory, or `None` for an in-memory store.
    pub fn dir(&self) -> Option<&Path> {
        self.core.dir.as_deref()
    }

    /// Returns the size of the write-ahead log in bytes, or 0 if in-memory.
    pub fn wal_size_bytes(&self) -> u64 {
        self.writer().wal.as_ref().map_or(0, |w| w.size_bytes())
    }

    /// Returns the number of fsyncs the log has issued.
    pub fn wal_syncs(&self) -> u64 {
        self.writer().wal.as_ref().map_or(0, |w| w.syncs())
    }

    /// Approximate resident size of the write buffer, in bytes.
    pub fn size_bytes(&self) -> usize {
        let versions = self.versions();
        versions.mem.size_bytes() + versions.imm.iter().map(|m| m.size_bytes()).sum::<usize>()
    }

    /// Returns the number of SSTables currently on disk.
    pub fn sstable_count(&self) -> usize {
        self.versions().tables.len()
    }

    /// Returns handles to the on-disk tables, oldest first.
    pub fn tables(&self) -> Vec<Arc<SsTable>> {
        self.versions()
            .tables
            .iter()
            .map(|open| Arc::clone(&open.table))
            .collect()
    }

    /// Returns the recency slots `(seq, generation)` of the tables on disk,
    /// oldest first.
    pub fn table_slots(&self) -> Vec<(u64, u32)> {
        self.versions()
            .tables
            .iter()
            .map(|o| (o.seq, o.generation))
            .collect()
    }

    /// Returns the memtable currently accepting writes.
    pub fn memtable(&self) -> Arc<MemTable> {
        Arc::clone(&self.versions().mem)
    }

    /// Returns the store's configured options.
    pub fn options(&self) -> DbOptions {
        self.core.options
    }

    /// Returns the number of live handles to this store.
    pub fn handle_count(&self) -> usize {
        Arc::strong_count(&self.core)
    }
}

/// A table on disk together with the recency slot it occupies.
///
/// Cheap to clone — the table itself is shared — because publishing a new view
/// copies the list.
#[derive(Debug, Clone)]
struct OpenTable {
    table: Arc<SsTable>,
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
            table: Arc::new(SsTable::open(&path)?),
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
        let db = Db::new();
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
        let db = Db::new();
        assert!(!db.delete(b"nope").unwrap());
    }

    #[test]
    fn sequence_numbers_increase_by_one_per_mutation() {
        let db = Db::new();
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
        let db = Db::new();
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
        let db = Db::new();
        db.put(b"k", b"v").unwrap();
        let snap = db.snapshot();
        db.delete(b"k").unwrap();

        assert_eq!(db.get(b"k").unwrap(), None);
        assert_eq!(db.get_at(&snap, b"k").unwrap(), Some(b"v".to_vec()));
    }

    #[test]
    fn scan_at_a_snapshot_sees_the_state_of_that_moment() {
        let db = Db::new();
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
        let db = Db::new();
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
