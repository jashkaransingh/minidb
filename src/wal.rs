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
//! # Record format
//!
//! Records are appended sequentially, each length-prefixed and checksummed so a
//! torn write at the tail can be detected and truncated rather than silently
//! deserialized as garbage:
//!
//! ```text
//! ┌──────────┬────────┬──────────┬────────────┬─────────┬───────────┐
//! │ crc32    │ kind   │ key_len  │ value_len  │ key     │ value     │
//! │ 4 bytes  │ 1 byte │ 4 bytes  │ 4 bytes    │ n bytes │ m bytes   │
//! └──────────┴────────┴──────────┴────────────┴─────────┴───────────┘
//! ```
//!
//! All integers are little-endian. `kind` is 0 for a put and 1 for a delete;
//! deletes carry no value bytes and store `value_len = 0`. The crc32 covers
//! everything after itself — `kind`, both lengths, and the payload — so a
//! corrupt length field is caught before it is used to size an allocation.
//!
//! Fixed-width lengths are used rather than varints. A varint would save a few
//! bytes per record, but the log is short-lived (rotated on every memtable
//! flush) and a fixed 13-byte header makes the "is there a whole header left?"
//! check at replay trivial, which is the part that has to be exactly right.
//!
//! # Crash semantics
//!
//! A crash can tear the log mid-record: the tail may be a partial header, a
//! partial payload, or a complete-looking record whose checksum fails because
//! only some sectors reached the platter. All three are *expected* and mean the
//! same thing — the durable prefix ends here. [`Wal::replay`] stops at the first
//! bad record, truncates the file to that offset, and returns what was
//! recovered. Corruption in the *middle* of the log is indistinguishable from a
//! torn tail with this format, so replay is deliberately conservative: it never
//! tries to resynchronize and skip ahead to later records.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::{fmt, io};

/// Bytes of fixed header preceding each record's payload.
const HEADER_LEN: usize = 4 + 1 + 4 + 4;

const KIND_PUT: u8 = 0;
const KIND_DELETE: u8 = 1;

/// A single decoded log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Record {
    /// Returns the key this record applies to.
    pub fn key(&self) -> &[u8] {
        match self {
            Record::Put { key, .. } | Record::Delete { key } => key,
        }
    }

    /// Serializes this record, including its header and checksum.
    fn encode(&self) -> Vec<u8> {
        let (kind, key, value): (u8, &[u8], &[u8]) = match self {
            Record::Put { key, value } => (KIND_PUT, key, value),
            Record::Delete { key } => (KIND_DELETE, key, &[]),
        };

        let mut buf = Vec::with_capacity(HEADER_LEN + key.len() + value.len());
        buf.extend_from_slice(&[0; 4]); // checksum patched in below
        buf.push(kind);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);

        let checksum = crc32fast::hash(&buf[4..]);
        buf[..4].copy_from_slice(&checksum.to_le_bytes());
        buf
    }
}

/// Why replay stopped before the end of the file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TailDefect {
    /// Fewer than [`HEADER_LEN`] bytes remained.
    PartialHeader,
    /// The header was complete but the payload was cut short.
    PartialPayload,
    /// The record was fully present but its checksum did not match.
    BadChecksum,
    /// The `kind` byte was neither put nor delete.
    UnknownKind,
}

impl fmt::Display for TailDefect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            TailDefect::PartialHeader => "truncated record header",
            TailDefect::PartialPayload => "truncated record payload",
            TailDefect::BadChecksum => "checksum mismatch",
            TailDefect::UnknownKind => "unrecognized record kind",
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
}

impl Recovery {
    /// Returns `true` if a damaged tail was discarded during replay.
    pub fn truncated(&self) -> bool {
        self.defect.is_some()
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
        })
    }

    /// Appends a record, fsyncing before returning under [`SyncPolicy::EveryWrite`].
    ///
    /// When this returns `Ok` under that policy, the write is durable: it will
    /// be recovered by [`replay`](Self::replay) even if the process dies in the
    /// next instant.
    pub fn append(&mut self, record: &Record) -> io::Result<()> {
        let bytes = record.encode();
        self.file.write_all(&bytes)?;
        self.size_bytes += bytes.len() as u64;

        if self.policy == SyncPolicy::EveryWrite {
            self.sync()?;
        }
        Ok(())
    }

    /// Appends several records, paying at most one fsync for the batch.
    pub fn append_batch(&mut self, records: &[Record]) -> io::Result<()> {
        for record in records {
            let bytes = record.encode();
            self.file.write_all(&bytes)?;
            self.size_bytes += bytes.len() as u64;
        }
        if self.policy == SyncPolicy::EveryWrite {
            self.sync()?;
        }
        Ok(())
    }

    /// Replays every intact record in the log, in write order.
    ///
    /// Stops at the first damaged record and truncates the file to that offset,
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
        self.file.flush()?;
        self.file.get_ref().sync_data()
    }

    /// Discards the log's contents after they have been flushed to an SSTable.
    ///
    /// **Only safe once the corresponding SSTable is itself durable.** A crash
    /// between writing the table and rotating the log must lose nothing, so the
    /// required order is: write the SSTable, fsync it, fsync its directory,
    /// *then* call this.
    pub fn rotate(&mut self) -> io::Result<()> {
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

/// Decodes as many whole, checksum-valid records as the buffer contains.
fn decode_all(buf: &[u8]) -> Recovery {
    let mut records = Vec::new();
    let mut offset = 0usize;

    loop {
        if offset == buf.len() {
            return Recovery {
                records,
                valid_bytes: offset as u64,
                defect: None,
            };
        }
        if buf.len() - offset < HEADER_LEN {
            return Recovery {
                records,
                valid_bytes: offset as u64,
                defect: Some(TailDefect::PartialHeader),
            };
        }

        let header = &buf[offset..offset + HEADER_LEN];
        let expected_crc = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let kind = header[4];
        let key_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(header[9..13].try_into().unwrap()) as usize;

        let payload_start = offset + HEADER_LEN;
        // Checked arithmetic: a corrupt length field could otherwise overflow
        // and wrap to a small in-bounds value.
        let record_end = match payload_start
            .checked_add(key_len)
            .and_then(|n| n.checked_add(value_len))
        {
            Some(end) => end,
            None => {
                return Recovery {
                    records,
                    valid_bytes: offset as u64,
                    defect: Some(TailDefect::BadChecksum),
                };
            }
        };

        if record_end > buf.len() {
            return Recovery {
                records,
                valid_bytes: offset as u64,
                defect: Some(TailDefect::PartialPayload),
            };
        }

        // Verify before trusting any of the decoded fields.
        let actual_crc = crc32fast::hash(&buf[offset + 4..record_end]);
        if actual_crc != expected_crc {
            return Recovery {
                records,
                valid_bytes: offset as u64,
                defect: Some(TailDefect::BadChecksum),
            };
        }

        let key = buf[payload_start..payload_start + key_len].to_vec();
        let record = match kind {
            KIND_PUT => Record::Put {
                key,
                value: buf[payload_start + key_len..record_end].to_vec(),
            },
            KIND_DELETE => Record::Delete { key },
            _ => {
                return Recovery {
                    records,
                    valid_bytes: offset as u64,
                    defect: Some(TailDefect::UnknownKind),
                };
            }
        };

        records.push(record);
        offset = record_end;
    }
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

    fn put(k: &str, v: &str) -> Record {
        Record::Put {
            key: k.as_bytes().to_vec(),
            value: v.as_bytes().to_vec(),
        }
    }

    fn del(k: &str) -> Record {
        Record::Delete {
            key: k.as_bytes().to_vec(),
        }
    }

    #[test]
    fn replay_of_a_missing_log_is_empty_not_an_error() {
        let dir = TempDir::new("missing");
        let recovery = Wal::replay(dir.file("absent.wal")).unwrap();
        assert!(recovery.records.is_empty());
        assert!(!recovery.truncated());
    }

    #[test]
    fn appended_records_replay_in_write_order() {
        let dir = TempDir::new("order");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("a", "1")).unwrap();
        wal.append(&put("b", "2")).unwrap();
        wal.append(&del("a")).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(
            recovery.records,
            vec![put("a", "1"), put("b", "2"), del("a")]
        );
        assert!(!recovery.truncated());
    }

    #[test]
    fn empty_keys_and_values_round_trip() {
        let dir = TempDir::new("empty-kv");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&Record::Put {
            key: Vec::new(),
            value: Vec::new(),
        })
        .unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records.len(), 1);
        assert!(!recovery.truncated());
    }

    #[test]
    fn binary_payloads_survive_a_round_trip() {
        let dir = TempDir::new("binary");
        let path = dir.file("a.wal");

        let record = Record::Put {
            key: vec![0x00, 0xff, 0x7f],
            value: vec![0xde, 0xad, 0x00, 0xbe, 0xef],
        };
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
        wal.append(&put("first", "1")).unwrap();
        drop(wal);

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("second", "2")).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(
            recovery.records,
            vec![put("first", "1"), put("second", "2")]
        );
    }

    #[test]
    fn a_torn_payload_is_discarded_and_earlier_records_survive() {
        let dir = TempDir::new("torn-payload");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("keep", "yes")).unwrap();
        let good_len = wal.size_bytes();
        wal.append(&put("lose", "this-is-a-longer-value")).unwrap();
        drop(wal);

        // Simulate a crash partway through the second record's payload.
        let full = fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full - 5).unwrap();
        drop(file);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put("keep", "yes")]);
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
        wal.append(&put("keep", "yes")).unwrap();
        let good_len = wal.size_bytes();
        drop(wal);

        // Append a stub shorter than a full header.
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0u8; 6]).unwrap();
        drop(file);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put("keep", "yes")]);
        assert_eq!(recovery.defect, Some(TailDefect::PartialHeader));
        assert_eq!(fs::metadata(&path).unwrap().len(), good_len);
    }

    #[test]
    fn a_flipped_bit_is_caught_by_the_checksum() {
        let dir = TempDir::new("bitflip");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("first", "aaaa")).unwrap();
        let good_len = wal.size_bytes();
        wal.append(&put("second", "bbbb")).unwrap();
        drop(wal);

        // Corrupt one byte inside the second record's value.
        let mut bytes = fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put("first", "aaaa")]);
        assert_eq!(recovery.defect, Some(TailDefect::BadChecksum));
        assert_eq!(recovery.valid_bytes, good_len);
    }

    #[test]
    fn a_corrupt_length_field_cannot_cause_a_huge_read() {
        let dir = TempDir::new("bad-len");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("k", "v")).unwrap();
        drop(wal);

        // Rewrite key_len as u32::MAX without fixing the checksum.
        let mut bytes = fs::read(&path).unwrap();
        bytes[5..9].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&path, &bytes).unwrap();

        let recovery = Wal::replay(&path).unwrap();
        assert!(recovery.records.is_empty());
        assert_eq!(recovery.defect, Some(TailDefect::PartialPayload));
    }

    #[test]
    fn an_unknown_kind_byte_stops_replay() {
        let dir = TempDir::new("bad-kind");
        let path = dir.file("a.wal");

        // Hand-build a well-checksummed record with an invalid kind.
        let mut buf = vec![0u8; 4];
        buf.push(9);
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0u32.to_le_bytes());
        buf.push(b'k');
        let crc = crc32fast::hash(&buf[4..]);
        buf[..4].copy_from_slice(&crc.to_le_bytes());
        fs::write(&path, &buf).unwrap();

        let recovery = Wal::replay(&path).unwrap();
        assert!(recovery.records.is_empty());
        assert_eq!(recovery.defect, Some(TailDefect::UnknownKind));
    }

    #[test]
    fn replay_leaves_an_appendable_log_after_truncation() {
        let dir = TempDir::new("append-after-truncate");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("a", "1")).unwrap();
        drop(wal);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(&[0xffu8; 7]).unwrap();
        drop(file);

        assert!(Wal::replay(&path).unwrap().truncated());

        // The recovered log must accept new writes and replay cleanly.
        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("b", "2")).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put("a", "1"), put("b", "2")]);
        assert!(!recovery.truncated());
    }

    #[test]
    fn rotate_empties_the_log_but_keeps_it_usable() {
        let dir = TempDir::new("rotate");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("old", "data")).unwrap();
        assert!(wal.size_bytes() > 0);

        wal.rotate().unwrap();
        assert_eq!(wal.size_bytes(), 0);

        wal.append(&put("new", "data")).unwrap();
        drop(wal);

        let recovery = Wal::replay(&path).unwrap();
        assert_eq!(recovery.records, vec![put("new", "data")]);
    }

    #[test]
    fn buffered_policy_still_replays_after_an_explicit_sync() {
        let dir = TempDir::new("buffered");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::OsBuffered).unwrap();
        wal.append(&put("a", "1")).unwrap();
        wal.sync().unwrap();
        drop(wal);

        assert_eq!(Wal::replay(&path).unwrap().records, vec![put("a", "1")]);
    }

    #[test]
    fn batch_append_records_every_entry() {
        let dir = TempDir::new("batch");
        let path = dir.file("a.wal");

        let batch = vec![put("a", "1"), del("b"), put("c", "3")];
        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append_batch(&batch).unwrap();
        drop(wal);

        assert_eq!(Wal::replay(&path).unwrap().records, batch);
    }

    #[test]
    fn size_bytes_tracks_the_file_length() {
        let dir = TempDir::new("size");
        let path = dir.file("a.wal");

        let mut wal = Wal::open(&path, SyncPolicy::EveryWrite).unwrap();
        wal.append(&put("abc", "de")).unwrap();
        wal.sync().unwrap();

        assert_eq!(wal.size_bytes(), (HEADER_LEN + 3 + 2) as u64);
        assert_eq!(fs::metadata(&path).unwrap().len(), wal.size_bytes());
    }
}
