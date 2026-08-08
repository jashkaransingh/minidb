//! Write-ahead log — durability for the in-memory write buffer.
//!
//! # Why this exists
//!
//! The memtable lives in RAM, so an unclean shutdown loses every write that has
//! not yet been flushed to an SSTable. The WAL closes that window: each mutation
//! is appended to a log file and (under [`SyncPolicy::EveryWrite`]) fsynced
//! *before* the write is acknowledged. On startup the log is replayed to rebuild
//! the memtable exactly as it was.
//!
//! # Frames, not records
//!
//! The unit of both writing and recovery is a **frame**: a length-prefixed,
//! checksummed group of one or more mutations.
//!
//! ```text
//! ┌──────────┬─────────┬─────────────┬───────────────────────────┐
//! │ crc32    │ count   │ payload_len │ payload                   │
//! │ 4 bytes  │ 4 bytes │ 4 bytes     │ payload_len bytes         │
//! └──────────┴─────────┴─────────────┴───────────────────────────┘
//! ```
//!
//! and the payload is `count` mutations laid end to end:
//!
//! ```text
//! ┌────────┬─────────┬──────────┬────────────┬─────────┬───────────┐
//! │ kind   │ seq     │ key_len  │ value_len  │ key     │ value     │
//! │ 1 byte │ 8 bytes │ 4 bytes  │ 4 bytes    │ n bytes │ m bytes   │
//! └────────┴─────────┴──────────┴────────────┴─────────┴───────────┘
//! ```
//!
//! All integers are little-endian. `kind` is 0 for a put and 1 for a delete;
//! deletes carry no value bytes and store `value_len = 0`. `seq` is the
//! mutation's MVCC sequence number, which is what makes recovery reconstruct the
//! *versions* a snapshot read depends on rather than just the final state.
//!
//! **The checksum covers the whole frame**, which is the property group commit
//! is built on: a frame is recovered in full or not at all. A crash that tears a
//! frame — anywhere in it — fails the frame's crc or its length check, and every
//! mutation in it is discarded together. There is no state in which half a
//! batch survives. See [`Wal::append_batch`].
//!
//! Fixed-width lengths are used rather than varints. A varint would save a few
//! bytes per record, but the log is short-lived (rotated on every memtable
//! flush) and fixed headers make the "is there a whole header left?" check at
//! replay trivial, which is the part that has to be exactly right.
//!
//! # Crash semantics
//!
//! A crash can tear the log mid-frame: the tail may be a partial header, a
//! partial payload, or a complete-looking frame whose checksum fails because
//! only some sectors reached the platter. All three are *expected* and mean the
//! same thing — the durable prefix ends here. [`Wal::replay`] stops at the first
//! bad frame, truncates the file to that offset, and returns what was recovered.
//! Corruption in the *middle* of the log is indistinguishable from a torn tail
//! with this format, so replay is deliberately conservative: it never tries to
//! resynchronize and skip ahead to later frames.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::{fmt, io};

use crate::fault::{FaultPlan, simulated_crash};
use crate::memtable::Entry;

/// Bytes of fixed header preceding each frame's payload.
const FRAME_HEADER_LEN: usize = 4 + 4 + 4;

/// Bytes of fixed header preceding each mutation's key and value.
const MUTATION_HEADER_LEN: usize = 1 + 8 + 4 + 4;

const KIND_PUT: u8 = 0;
const KIND_DELETE: u8 = 1;

/// A single logged mutation.
///
/// Carries the MVCC sequence number assigned when the write was accepted, so
/// replay rebuilds the same versioned view the store had before the crash.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub key: Vec<u8>,
    pub entry: Entry,
}

impl Record {
    /// A write of `value` at `key`, at sequence number `seq`.
    pub fn put(seq: u64, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            seq,
            key: key.into(),
            entry: Entry::Value(value.into()),
        }
    }

    /// A deletion of `key`, at sequence number `seq`.
    pub fn delete(seq: u64, key: impl Into<Vec<u8>>) -> Self {
        Self {
            seq,
            key: key.into(),
            entry: Entry::Tombstone,
        }
    }

    /// Returns `true` if this record is a deletion marker.
    pub fn is_delete(&self) -> bool {
        self.entry.is_tombstone()
    }

    /// Appends this mutation's encoding to `buf`.
    fn encode_into(&self, buf: &mut Vec<u8>) {
        let (kind, value): (u8, &[u8]) = match &self.entry {
            Entry::Value(v) => (KIND_PUT, v),
            Entry::Tombstone => (KIND_DELETE, &[]),
        };
        buf.push(kind);
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(value);
    }

    /// Bytes this mutation occupies inside a frame payload.
    pub fn encoded_len(&self) -> usize {
        MUTATION_HEADER_LEN + self.key.len() + self.entry.len()
    }
}

/// Encodes `records` as one self-checksummed frame.
///
/// Exposed within the crate so the group-commit path can measure a batch before
/// committing to it.
pub(crate) fn encode_frame(records: &[Record]) -> Vec<u8> {
    let payload_len: usize = records.iter().map(Record::encoded_len).sum();
    let mut buf = Vec::with_capacity(FRAME_HEADER_LEN + payload_len);
    buf.extend_from_slice(&[0; 4]); // checksum patched in below
    buf.extend_from_slice(&(records.len() as u32).to_le_bytes());
    buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
    for record in records {
        record.encode_into(&mut buf);
    }
    let checksum = crc32fast::hash(&buf[4..]);
    buf[..4].copy_from_slice(&checksum.to_le_bytes());
    buf
}

/// Why replay stopped before the end of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailDefect {
    /// Fewer than [`FRAME_HEADER_LEN`] bytes remained.
    PartialHeader,
    /// The header was complete but the payload was cut short.
    PartialPayload,
    /// The frame was fully present but its checksum did not match.
    BadChecksum,
    /// The frame passed its checksum but its payload did not decode.
    MalformedPayload,
}

impl fmt::Display for TailDefect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TailDefect::PartialHeader => "truncated frame header",
            TailDefect::PartialPayload => "truncated frame payload",
            TailDefect::BadChecksum => "checksum mismatch",
            TailDefect::MalformedPayload => "malformed frame payload",
        };
        f.write_str(s)
    }
}

/// Outcome of replaying a log.
#[derive(Debug)]
pub struct Recovery {
    /// Records recovered from the durable prefix, in write order.
    pub records: Vec<Record>,
    /// Byte offset where the durable prefix ended.
    pub valid_bytes: u64,
    /// What stopped the replay, if it stopped before EOF.
    pub defect: Option<TailDefect>,
    /// Number of whole frames recovered.
    pub frames: usize,
}

impl Recovery {
    /// Returns `true` if a damaged tail was discarded during replay.
    pub fn truncated(&self) -> bool {
        self.defect.is_some()
    }

    /// Highest sequence number recovered, or 0 if the log was empty.
    pub fn max_seq(&self) -> u64 {
        self.records.iter().map(|r| r.seq).max().unwrap_or(0)
    }
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
    file: BufWriter<File>,
    path: PathBuf,
    policy: SyncPolicy,
    size_bytes: u64,
    /// Bytes appended over this log's lifetime, unaffected by rotation. Fault
    /// plans are expressed against this, so a plan survives a memtable flush.
    total_appended: u64,
    /// fsyncs issued over this log's lifetime. Group commit is measured against
    /// this: it is the count that batching is supposed to drive down.
    syncs: u64,
    fault: FaultPlan,
    /// Set once an injected fault has fired; every later operation fails.
    crashed: bool,
}

impl Wal {
    /// Opens the log at `path`, creating it if absent, positioned for appends.
    ///
    /// Creating the file is not durable until its *parent directory* is fsynced
    /// — the file's directory entry is metadata, and a crash can lose it even
    /// though the file's own contents were flushed.
    pub fn open<P: AsRef<Path>>(path: P, policy: SyncPolicy) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let existed = path.exists();

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        let size_bytes = file.metadata()?.len();

        if !existed {
            sync_parent_dir(&path)?;
        }

        Ok(Self {
            file: BufWriter::new(file),
            path,
            policy,
            size_bytes,
            total_appended: 0,
            syncs: 0,
            fault: FaultPlan::none(),
            crashed: false,
        })
    }

    /// Appends one record as a frame of one, fsyncing before returning under
    /// [`SyncPolicy::EveryWrite`].
    ///
    /// When this returns `Ok` under that policy, the write is durable: it will
    /// be recovered by [`replay`](Self::replay) even if the process dies in the
    /// next instant.
    pub fn append(&mut self, record: &Record) -> io::Result<()> {
        self.append_batch(std::slice::from_ref(record))
    }

    /// Appends several records as **one frame**, paying one fsync for the batch.
    ///
    /// This is the durability primitive group commit is built on. The frame's
    /// checksum spans every record in it, so recovery is all-or-nothing:
    ///
    /// - A crash **after** the fsync recovers every record in the batch.
    /// - A crash **before** it recovers none of them — a torn frame fails its
    ///   length check or its checksum, and replay stops at the frame's start
    ///   offset, discarding the partial bytes.
    ///
    /// There is deliberately no intermediate state where some records in a
    /// batch survive and others do not.
    pub fn append_batch(&mut self, records: &[Record]) -> io::Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        self.write_frame(&encode_frame(records))?;
        if self.policy == SyncPolicy::EveryWrite {
            self.sync()?;
        }
        Ok(())
    }

    /// Writes one encoded frame, honouring any armed fault plan.
    fn write_frame(&mut self, bytes: &[u8]) -> io::Result<()> {
        if self.crashed {
            return Err(simulated_crash());
        }

        if let Some(limit) = self.fault.crash_after_wal_bytes
            && self.total_appended + bytes.len() as u64 > limit
        {
            // Write only what fits below the fault point and flush it to the OS,
            // then fail *without* fsyncing — exactly the torn, unacknowledged
            // tail a crash mid-append leaves behind. Because the checksum covers
            // the whole frame, everything in this batch is lost together.
            let partial = limit.saturating_sub(self.total_appended) as usize;
            self.file.write_all(&bytes[..partial])?;
            self.file.flush()?;
            self.size_bytes += partial as u64;
            self.total_appended += partial as u64;
            self.crashed = true;
            return Err(simulated_crash());
        }

        self.file.write_all(bytes)?;
        self.size_bytes += bytes.len() as u64;
        self.total_appended += bytes.len() as u64;
        Ok(())
    }

    /// Arms a fault plan on this log. See [`crate::fault`].
    pub fn set_fault_plan(&mut self, plan: FaultPlan) {
        self.fault = plan;
    }

    /// Returns `true` once an injected fault has fired.
    pub fn has_crashed(&self) -> bool {
        self.crashed
    }

    /// Returns the number of bytes appended over this log's lifetime.
    pub fn total_appended(&self) -> u64 {
        self.total_appended
    }

    /// Returns the number of fsyncs issued over this log's lifetime.
    pub fn syncs(&self) -> u64 {
        self.syncs
    }

    /// Replays every intact frame in the log, in write order.
    ///
    /// Stops at the first damaged frame and truncates the file to that offset,
    /// so the log is left in a state that can be appended to safely. A missing
    /// file replays as an empty log rather than an error — that is simply a
    /// store that has never been written to.
    pub fn replay<P: AsRef<Path>>(path: P) -> io::Result<Recovery> {
        let path = path.as_ref();
        if !path.exists() {
            return Ok(Recovery {
                records: Vec::new(),
                valid_bytes: 0,
                defect: None,
                frames: 0,
            });
        }

        let mut buf = Vec::new();
        File::open(path)?.read_to_end(&mut buf)?;

        let recovery = decode_all(&buf);

        if recovery.defect.is_some() {
            let file = OpenOptions::new().write(true).open(path)?;
            file.set_len(recovery.valid_bytes)?;
            file.sync_all()?;
        }

        Ok(recovery)
    }

    /// Forces buffered data to stable storage.
    pub fn sync(&mut self) -> io::Result<()> {
        if self.crashed {
            return Err(simulated_crash());
        }
        self.file.flush()?;
        self.file.get_ref().sync_data()?;
        self.syncs += 1;
        Ok(())
    }

    /// Discards the log's contents after they have been flushed to an SSTable.
    ///
    /// **Only safe once the corresponding SSTable is itself durable.** A crash
    /// between writing the table and rotating the log must lose nothing, so the
    /// required order is: write the SSTable, fsync it, fsync its directory,
    /// *then* call this.
    pub fn rotate(&mut self) -> io::Result<()> {
        if self.crashed {
            return Err(simulated_crash());
        }
        self.file.flush()?;
        let file = OpenOptions::new().write(true).open(&self.path)?;
        file.set_len(0)?;
        file.sync_all()?;

        let reopened = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.path)?;
        self.file = BufWriter::new(reopened);
        self.size_bytes = 0;
        Ok(())
    }

    /// Returns the number of bytes appended to the log.
    pub fn size_bytes(&self) -> u64 {
        self.size_bytes
    }

    /// Returns the log's path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

/// Decodes as many whole, checksum-valid frames as the buffer contains.
fn decode_all(buf: &[u8]) -> Recovery {
    let mut records = Vec::new();
    let mut frames = 0usize;
    let mut offset = 0usize;

    macro_rules! stop {
        ($defect:expr) => {
            return Recovery {
                records,
                valid_bytes: offset as u64,
                defect: $defect,
                frames,
            }
        };
    }

    loop {
        if offset == buf.len() {
            stop!(None);
        }
        if buf.len() - offset < FRAME_HEADER_LEN {
            stop!(Some(TailDefect::PartialHeader));
        }

        let header = &buf[offset..offset + FRAME_HEADER_LEN];
        let expected_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let count = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let payload_len = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;

        let payload_start = offset + FRAME_HEADER_LEN;
        // Checked arithmetic: a corrupt length field could otherwise overflow
        // and wrap to a small in-bounds value.
        let Some(frame_end) = payload_start.checked_add(payload_len) else {
            stop!(Some(TailDefect::BadChecksum));
        };
        if frame_end > buf.len() {
            stop!(Some(TailDefect::PartialPayload));
        }

        // Verify before trusting any of the decoded fields.
        if crc32fast::hash(&buf[offset + 4..frame_end]) != expected_crc {
            stop!(Some(TailDefect::BadChecksum));
        }

        // The frame is intact, so its records are recovered as a unit.
        let mut cursor = payload_start;
        let mut decoded = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            match decode_mutation(buf, cursor, frame_end) {
                Some((record, next)) => {
                    decoded.push(record);
                    cursor = next;
                }
                None => stop!(Some(TailDefect::MalformedPayload)),
            }
        }
        if cursor != frame_end {
            stop!(Some(TailDefect::MalformedPayload));
        }

        records.extend(decoded);
        frames += 1;
        offset = frame_end;
    }
}

/// Decodes one mutation starting at `cursor`, bounded by `end`.
fn decode_mutation(buf: &[u8], cursor: usize, end: usize) -> Option<(Record, usize)> {
    if end.checked_sub(cursor)? < MUTATION_HEADER_LEN {
        return None;
    }
    let kind = buf[cursor];
    let seq = u64::from_le_bytes(buf[cursor + 1..cursor + 9].try_into().ok()?);
    let key_len = u32::from_le_bytes(buf[cursor + 9..cursor + 13].try_into().ok()?) as usize;
    let value_len = u32::from_le_bytes(buf[cursor + 13..cursor + 17].try_into().ok()?) as usize;

    let key_start = cursor + MUTATION_HEADER_LEN;
    let value_start = key_start.checked_add(key_len)?;
    let next = value_start.checked_add(value_len)?;
    if next > end {
        return None;
    }

    let key = buf[key_start..value_start].to_vec();
    let entry = match kind {
        KIND_PUT => Entry::Value(buf[value_start..next].to_vec()),
        KIND_DELETE if value_len == 0 => Entry::Tombstone,
        _ => return None,
    };
    Some((Record { seq, key, entry }, next))
}

/// fsyncs the directory containing `path`, making the file's presence durable.
pub(crate) fn sync_parent_dir(path: &Path) -> io::Result<()> {
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    let dir = if dir.as_os_str().is_empty() {
        Path::new(".")
    } else {
        dir
    };
    // Opening a directory read-only and syncing it is the portable-on-Unix way
    // to durably commit a directory entry. Not meaningful on Windows, where the
    // call fails and is ignored.
    match File::open(dir) {
        Ok(handle) => match handle.sync_all() {
            Ok(()) => Ok(()),
            Err(_) if cfg!(windows) => Ok(()),
            Err(e) => Err(e),
        },
        Err(_) if cfg!(windows) => Ok(()),
        Err(e) => Err(e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    /// A scratch directory that cleans itself up.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new(tag: &str) -> Self {
            let mut path = std::env::temp_dir();
            path.push(format!("minidb-wal-{tag}"));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
        fn file(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn put(seq: u64, k: &str, v: &str) -> Record {
        Record::put(seq, k.as_bytes(), v.as_bytes())
    }

    fn del(seq: u64, k: &str) -> Record {
        Record::delete(seq, k.as_bytes())
    }

    #[test]
    fn replay_of_a_missing_log_is_empty_not_an_error() {
        let dir = TempDir::new("missing");
        let recovery = Wal::replay(dir.file("absent.wal")).unwrap();
        assert!(recovery.records.is_empty());
        assert!(!recovery.truncated());
    }

    #[test]
    fn appended_records_replay_in_write_order_with_their_sequence_numbers() {
        let dir = TempDir::new("order");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "a", "1")).unwrap();
        wal.append(&put(2, "b", "2")).unwrap();
        wal.append(&del(3, "a")).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(
            recovery.records,
            vec![put(1, "a", "1"), put(2, "b", "2"), del(3, "a")]
        );
        assert_eq!(recovery.max_seq(), 3);
        assert_eq!(recovery.frames, 3);
        assert!(!recovery.truncated());
    }

    #[test]
    fn empty_keys_and_values_round_trip() {
        let dir = TempDir::new("empty-kv");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&Record::put(1, Vec::new(), Vec::new())).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records.len(), 1);
        assert_eq!(recovery.records[0].entry, Entry::Value(Vec::new()));
        assert!(!recovery.truncated());
    }

    #[test]
    fn an_empty_value_stays_distinct_from_a_tombstone_across_replay() {
        let dir = TempDir::new("empty-vs-tombstone");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&Record::put(1, b"k".to_vec(), Vec::new()))
            .unwrap();
        wal.append(&Record::delete(2, b"k".to_vec())).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records[0].entry, Entry::Value(Vec::new()));
        assert_eq!(recovery.records[1].entry, Entry::Tombstone);
    }

    #[test]
    fn binary_payloads_survive_a_round_trip() {
        let dir = TempDir::new("binary");
        let path = dir.file("a.wal");

        let record = Record::put(
            7,
            vec![0x00, 0xff, 0x7f],
            vec![0xde, 0xad, 0x00, 0xbe, 0xef],
        );
        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&record).unwrap();
        drop(wal);

        assert_eq!(Wal::replay(&path).unwrap().records, vec![record]);
    }

    #[test]
    fn reopening_appends_rather_than_overwrites() {
        let dir = TempDir::new("reopen");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "first", "1")).unwrap();
        drop(wal);

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(2, "second", "2")).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(
            recovery.records,
            vec![put(1, "first", "1"), put(2, "second", "2")]
        );
    }

    #[test]
    fn a_torn_payload_is_discarded_and_earlier_frames_survive() {
        let dir = TempDir::new("torn-payload");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "keep", "yes")).unwrap();
        let good_len = wal.size_bytes();
        wal.append(&put(2, "lose", "this-is-a-longer-value"))
            .unwrap();
        drop(wal);

        // Simulate a crash partway through the second frame's payload.
        let full = fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full - 5).unwrap();
        drop(file);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put(1, "keep", "yes")]);
        assert_eq!(recovery.defect, Some(TailDefect::PartialPayload));
        assert_eq!(recovery.valid_bytes, good_len);
        // Replay truncated the damaged tail away.
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);
    }

    #[test]
    fn a_torn_header_is_discarded() {
        let dir = TempDir::new("torn-header");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "keep", "yes")).unwrap();
        let good_len = wal.size_bytes();
        drop(wal);

        // Append a stub shorter than a full frame header.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0u8; 6]).unwrap();
        drop(file);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put(1, "keep", "yes")]);
        assert_eq!(recovery.defect, Some(TailDefect::PartialHeader));
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);
    }

    #[test]
    fn a_flipped_bit_is_caught_by_the_checksum() {
        let dir = TempDir::new("bitflip");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "first", "aaaa")).unwrap();
        let good_len = wal.size_bytes();
        wal.append(&put(2, "second", "bbbb")).unwrap();
        drop(wal);

        // Corrupt one byte inside the second frame's value.
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put(1, "first", "aaaa")]);
        assert_eq!(recovery.defect, Some(TailDefect::BadChecksum));
        assert_eq!(recovery.valid_bytes, good_len);
    }

    #[test]
    fn a_corrupt_length_field_cannot_cause_a_huge_read() {
        let dir = TempDir::new("bad-len");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "k", "v")).unwrap();
        drop(wal);

        // Rewrite payload_len as u32::MAX without fixing the checksum.
        let mut bytes = fs::read(&path).unwrap();
        bytes[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let recovery = Wal::replay(&path).unwrap();
        assert!(recovery.records.is_empty());
        assert_eq!(recovery.defect, Some(TailDefect::PartialPayload));
    }

    #[test]
    fn an_unknown_kind_byte_stops_replay() {
        let dir = TempDir::new("bad-kind");
        let path = dir.file("a.wal");

        // Hand-build a well-checksummed frame carrying an invalid kind.
        let mut payload = Vec::new();
        payload.push(9u8);
        payload.extend_from_slice(&1u64.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.push(b'k');

        let mut buf = vec![0u8; 4];
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload);
        let crc = crc32fast::hash(&buf[4..]);
        buf[..4].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &buf).unwrap();

        let recovery = Wal::replay(&path).unwrap();
        assert!(recovery.records.is_empty());
        assert_eq!(recovery.defect, Some(TailDefect::MalformedPayload));
    }

    #[test]
    fn a_frame_whose_count_disagrees_with_its_payload_is_rejected() {
        let dir = TempDir::new("bad-count");
        let path = dir.file("a.wal");

        // One real mutation, but the frame claims two.
        let record = put(1, "k", "v");
        let mut payload = Vec::new();
        record.encode_into(&mut payload);

        let mut buf = vec![0u8; 4];
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&payload);
        let crc = crc32fast::hash(&buf[4..]);
        buf[..4].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &buf).unwrap();

        let recovery = Wal::replay(&path).unwrap();
        assert!(recovery.records.is_empty(), "no partial frame is accepted");
        assert_eq!(recovery.defect, Some(TailDefect::MalformedPayload));
    }

    #[test]
    fn replay_leaves_an_appendable_log_after_truncation() {
        let dir = TempDir::new("append-after-truncate");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "a", "1")).unwrap();
        drop(wal);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xffu8; 7]).unwrap();
        drop(file);

        assert!(Wal::replay(&path).unwrap().truncated());

        // The recovered log must accept new writes and replay cleanly.
        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(2, "b", "2")).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put(1, "a", "1"), put(2, "b", "2")]);
        assert!(!recovery.truncated());
    }

    #[test]
    fn rotate_empties_the_log_but_keeps_it_usable() {
        let dir = TempDir::new("rotate");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "old", "data")).unwrap();
        assert!(wal.size_bytes() > 0);

        wal.rotate().unwrap();
        assert_eq!(wal.size_bytes(), 0);

        wal.append(&put(2, "new", "data")).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put(2, "new", "data")]);
    }

    #[test]
    fn buffered_policy_still_replays_after_an_explicit_sync() {
        let dir = TempDir::new("buffered");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::OsBuffered).unwrap();
        wal.append(&put(1, "a", "1")).unwrap();
        wal.sync().unwrap();
        drop(wal);

        assert_eq!(Wal::replay(&path).unwrap().records, vec![put(1, "a", "1")]);
    }

    #[test]
    fn a_batch_is_one_frame_and_one_fsync() {
        let dir = TempDir::new("batch");
        let path = dir.file("a.wal");

        let batch = vec![put(1, "a", "1"), del(2, "b"), put(3, "c", "3")];
        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append_batch(&batch).unwrap();
        assert_eq!(wal.syncs(), 1, "three records, one fsync");
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, batch);
        assert_eq!(recovery.frames, 1);
    }

    #[test]
    fn a_batch_torn_anywhere_loses_every_record_in_it() {
        let dir = TempDir::new("batch-atomic");

        let batch = vec![put(1, "a", "1"), put(2, "b", "2"), put(3, "c", "3")];
        let frame_len = encode_frame(&batch).len();

        // Tear the frame at every byte offset inside it and confirm that no
        // prefix of the batch is ever recovered.
        for cut in 1..frame_len {
            let path = dir.file(&format!("cut-{cut}.wal"));
            let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
            wal.append(&put(0, "before", "kept")).unwrap();
            let before_len = wal.size_bytes();
            wal.append_batch(&batch).unwrap();
            drop(wal);

            let file = OpenOptions::new().write(true).open(&path).unwrap();
            file.set_len(before_len + cut as u64).unwrap();
            drop(file);

            let recovery = Wal::replay(&path).unwrap();
            assert_eq!(
                recovery.records,
                vec![put(0, "before", "kept")],
                "a batch torn {cut} bytes in must recover no part of itself"
            );
            assert_eq!(recovery.valid_bytes, before_len);
        }
    }

    #[test]
    fn an_empty_batch_writes_nothing() {
        let dir = TempDir::new("empty-batch");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append_batch(&[]).unwrap();
        assert_eq!(wal.size_bytes(), 0);
        assert_eq!(wal.syncs(), 0);
    }

    #[test]
    fn size_bytes_tracks_the_file_length() {
        let dir = TempDir::new("size");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put(1, "abc", "de")).unwrap();
        wal.sync().unwrap();

        assert_eq!(
            wal.size_bytes(),
            (FRAME_HEADER_LEN + MUTATION_HEADER_LEN + 3 + 2) as u64
        );
        assert_eq!(fs::metadata(&path).unwrap().len(), wal.size_bytes());
    }
}
