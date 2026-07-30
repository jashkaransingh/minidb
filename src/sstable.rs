//! Sorted String Tables — **not yet implemented**.
//!
//! # Why this exists
//!
//! An SSTable is an immutable, sorted, on-disk run of key/value pairs. When a
//! memtable fills up it is written out as one of these in a single sequential
//! pass. Immutability is what makes the rest of the design tractable: readers
//! never take locks against writers, files can be cached aggressively, and
//! compaction can rewrite data by producing new files rather than mutating
//! existing ones.
//!
//! # Intended file layout
//!
//! ```text
//! ┌─────────────────────────────────────────┐
//! │ Data blocks    (~4 KiB, prefix-compressed)
//! ├─────────────────────────────────────────┤
//! │ Bloom filter   (see crate::bloom)        │
//! ├─────────────────────────────────────────┤
//! │ Sparse index   (first key → block offset)│
//! ├─────────────────────────────────────────┤
//! │ Footer         (offsets, magic, version) │
//! └─────────────────────────────────────────┘
//! ```
//!
//! The index is *sparse* — one entry per block, not per key — so it stays small
//! enough to hold in memory for every open table. A lookup binary-searches the
//! index to find the one block that could contain the key, reads that block, and
//! scans it. That is one disk read per table probe.

use std::io;
use std::path::Path;

use crate::memtable::Entry;

/// Statistics recorded in the footer, used by the compaction planner.
#[derive(Debug, Clone, Default)]
pub struct TableMeta {
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub size_bytes: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

/// Streams a sorted key/value run into a new SSTable file.
#[derive(Debug)]
pub struct SsTableWriter {
    _private: (),
}

impl SsTableWriter {
    /// Creates a new table file at `path`.
    ///
    /// TODO: write to a `.tmp` sibling and rename into place at finish time, so
    /// a crash mid-write never leaves a half-built table visible to recovery.
    pub fn create<P: AsRef<Path>>(_path: P) -> io::Result<Self> {
        todo!("create the output file and initialize block buffers")
    }

    /// Appends one entry. Callers must supply keys in ascending order.
    ///
    /// TODO: buffer into the current data block; when the block exceeds the
    /// target size, flush it, record `(first_key, offset)` in the sparse index,
    /// and feed the key to the bloom filter builder.
    pub fn append(&mut self, _key: &[u8], _entry: &Entry) -> io::Result<()> {
        todo!("append to the current block, rolling over at the block size limit")
    }

    /// Flushes the tail block, writes the bloom filter, index, and footer.
    ///
    /// TODO: must fsync the file *and* its parent directory before returning —
    /// the table is not safely on disk until both are done, and the WAL cannot
    /// be rotated until this returns.
    pub fn finish(self) -> io::Result<TableMeta> {
        todo!("flush tail block, write bloom + index + footer, fsync, rename")
    }
}

/// A read handle onto an immutable table file.
#[derive(Debug)]
pub struct SsTable {
    _private: (),
}

impl SsTable {
    /// Opens a table, loading its footer, sparse index, and bloom filter.
    ///
    /// TODO: validate the footer magic and version before trusting any offsets.
    pub fn open<P: AsRef<Path>>(_path: P) -> io::Result<Self> {
        todo!("read footer, load index and bloom filter into memory")
    }

    /// Looks up `key` in this table.
    ///
    /// Returns `Ok(None)` when the key is absent *from this table* — the caller
    /// must continue searching older tables. A `Some(Entry::Tombstone)` result
    /// means the key was deleted and the search stops here.
    ///
    /// TODO: probe the bloom filter first and return early on a negative — that
    /// is the whole point of carrying one. Then binary-search the sparse index,
    /// read the single candidate block, and scan it.
    pub fn get(&self, _key: &[u8]) -> io::Result<Option<Entry>> {
        todo!("bloom probe, index binary search, single block read, scan")
    }

    /// Iterates over every entry in ascending key order, tombstones included.
    ///
    /// TODO: the merge iterator that compaction and range scans are built on.
    pub fn iter(&self) -> impl Iterator<Item = io::Result<(Vec<u8>, Entry)>> {
        std::iter::empty()
    }

    /// Returns the metadata recorded in the footer.
    pub fn meta(&self) -> &TableMeta {
        todo!("return the parsed footer metadata")
    }

    /// Returns `true` if `key` falls within this table's key range.
    ///
    /// A cheap pre-filter: skips the bloom probe entirely for out-of-range keys.
    pub fn may_contain(&self, _key: &[u8]) -> bool {
        todo!("compare against min_key/max_key from the footer")
    }
}
