//! Write-ahead log — **not yet implemented**.
//!
//! # Why this exists
//!
//! The memtable lives in RAM, so an unclean shutdown loses every write that has
//! not yet been flushed to an SSTable. The WAL closes that window: each mutation
//! is appended to a log file and fsynced *before* it is acknowledged. On startup
//! the log is replayed to rebuild the memtable exactly as it was.
//!
//! # Intended record format
//!
//! Records are appended sequentially, each length-prefixed and checksummed so a
//! torn write at the tail can be detected and truncated rather than silently
//! deserialized as garbage:
//!
//! ```text
//! ┌──────────┬────────┬──────────┬────────────┬─────────┬───────────┐
//! │ crc32    │ kind   │ key_len  │ value_len  │ key     │ value     │
//! │ 4 bytes  │ 1 byte │ varint   │ varint     │ n bytes │ m bytes   │
//! └──────────┴────────┴──────────┴────────────┴─────────┴───────────┘
//! ```
//!
//! `kind` is 0 for a put and 1 for a delete; deletes carry no value bytes.

use std::io;
use std::path::Path;

/// A single decoded log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// How aggressively the log is flushed to stable storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncPolicy {
    /// fsync on every append. Durable, slow.
    EveryWrite,
    /// Let the OS page cache decide. Fast, loses recent writes on power failure.
    OsBuffered,
}

/// Append-only durability log guarding the memtable.
#[derive(Debug)]
pub struct Wal {
    _private: (),
}

impl Wal {
    /// Opens the log at `path`, creating it if absent, positioned for appends.
    ///
    /// TODO: open with `OpenOptions::new().create(true).append(true)`, and fsync
    /// the *parent directory* as well so the file's directory entry survives a
    /// crash — creating a file is not durable until the directory is synced.
    pub fn open<P: AsRef<Path>>(_path: P, _policy: SyncPolicy) -> io::Result<Self> {
        todo!("open or create the log file and seek to the end")
    }

    /// Appends a record and, under [`SyncPolicy::EveryWrite`], fsyncs before returning.
    ///
    /// TODO: encode the frame described in the module docs, compute the crc32
    /// over `kind..value`, write it, then honor the sync policy. This must not
    /// return `Ok` until the caller may safely consider the write durable.
    pub fn append(&mut self, _record: &Record) -> io::Result<()> {
        todo!("encode, checksum, append, conditionally fsync")
    }

    /// Replays every intact record in the log, in write order.
    ///
    /// TODO: decode frames until EOF. A record whose crc32 fails, or that is
    /// truncated mid-frame, marks the end of the durable prefix: stop there,
    /// truncate the file to that offset, and return what was recovered. A crash
    /// during append is expected and must not be treated as corruption.
    pub fn replay<P: AsRef<Path>>(_path: P) -> io::Result<Vec<Record>> {
        todo!("decode frames, stop at the first bad or partial record, truncate")
    }

    /// Forces buffered data to stable storage.
    ///
    /// TODO: `File::sync_data`.
    pub fn sync(&mut self) -> io::Result<()> {
        todo!("fsync the underlying file")
    }

    /// Discards the log after its contents have been flushed to an SSTable.
    ///
    /// TODO: only safe once the corresponding SSTable is itself durable —
    /// otherwise a crash between the two steps loses the data entirely. Order
    /// is: write SSTable, fsync it, fsync its directory, *then* rotate the WAL.
    pub fn rotate(&mut self) -> io::Result<()> {
        todo!("truncate or replace the log once its data is safely on disk")
    }

    /// Returns the current size of the log in bytes.
    pub fn size_bytes(&self) -> u64 {
        todo!("stat the file or track bytes written")
    }
}
