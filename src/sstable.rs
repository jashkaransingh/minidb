//! Sorted String Tables — immutable on-disk runs of key/value pairs.
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
//! # File layout
//!
//! ```text
//! ┌─────────────────────────────────────────┐ offset 0
//! │ Data section    entries, ascending keys  │
//! ├─────────────────────────────────────────┤
//! │ Bloom section   membership filter        │
//! ├─────────────────────────────────────────┤
//! │ Index section   (empty until milestone 4)│
//! ├─────────────────────────────────────────┤
//! │ Meta section    counts, min/max key      │
//! ├─────────────────────────────────────────┤
//! │ Footer          76 bytes, fixed          │
//! └─────────────────────────────────────────┘
//! ```
//!
//! Every section's offset and length live in the fixed-size footer at the end of
//! the file, which is why the footer is read first. The footer carries its own
//! crc32 and a magic number, so a truncated or unrelated file is rejected before
//! any of its offsets are trusted.
//!
//! Each data entry is:
//!
//! ```text
//! kind (1B) │ key_len (4B) │ value_len (4B) │ key │ value
//! ```
//!
//! `kind` is 0 for a value and 1 for a tombstone; tombstones store no value
//! bytes. A crc32 over the whole data section is recorded in the footer.
//!
//! # Lookup path, and its current limitation
//!
//! [`SsTable::get`] filters in three stages, cheapest first:
//!
//! 1. **Key range** — `min_key`/`max_key` from the meta section rule out keys
//!    outside the table's span with two comparisons.
//! 2. **Bloom filter** — an in-memory probe that ends the lookup for almost
//!    every remaining miss without touching the data section.
//! 3. **Data scan** — still a sequential walk, so a *hit* costs O(n).
//!
//! Stage 3 is the remaining gap. The sparse index (milestone 4) will replace it
//! with a binary search to a single block; the index section is already reserved
//! in the layout, so that milestone fills it in rather than changing the format.
//!
//! # Durability
//!
//! [`SsTableWriter`] writes to a `.tmp` sibling and renames into place only
//! after the contents are fsynced. A crash mid-write therefore leaves a stray
//! temp file, never a half-built table that recovery might mistake for a
//! complete one. The rename is followed by a directory fsync, without which the
//! new name itself is not durable.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::bloom::{BloomFilter, DEFAULT_FP_RATE, hash_pair};
use crate::memtable::Entry;
use crate::wal::sync_parent_dir;

/// Identifies a minidb SSTable. ASCII "MINIDBST".
const MAGIC: u64 = 0x4D49_4E49_4442_5354;

/// Format version, bumped whenever the layout changes incompatibly.
pub const FORMAT_VERSION: u32 = 1;

/// Size of the fixed footer in bytes.
const FOOTER_LEN: usize = 76;

const KIND_VALUE: u8 = 0;
const KIND_TOMBSTONE: u8 = 1;

/// Bytes of fixed header preceding each data entry's payload.
const ENTRY_HEADER_LEN: usize = 1 + 4 + 4;

/// Statistics recorded in the meta section, used by the compaction planner.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableMeta {
    pub num_entries: u64,
    pub num_tombstones: u64,
    pub size_bytes: u64,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

impl TableMeta {
    /// Returns the number of non-tombstone entries.
    pub fn num_values(&self) -> u64 {
        self.num_entries - self.num_tombstones
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&self.num_entries.to_le_bytes());
        buf.extend_from_slice(&self.num_tombstones.to_le_bytes());
        buf.extend_from_slice(&(self.min_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.min_key);
        buf.extend_from_slice(&(self.max_key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.max_key);
        buf
    }

    fn decode(buf: &[u8]) -> io::Result<Self> {
        let mut cursor = 0usize;
        let num_entries = read_u64(buf, &mut cursor)?;
        let num_tombstones = read_u64(buf, &mut cursor)?;
        let min_key = read_blob(buf, &mut cursor)?;
        let max_key = read_blob(buf, &mut cursor)?;
        Ok(Self {
            num_entries,
            num_tombstones,
            size_bytes: 0,
            min_key,
            max_key,
        })
    }
}

/// Section offsets and integrity fields, stored at the end of every table.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Footer {
    data_len: u64,
    data_crc: u32,
    bloom_off: u64,
    bloom_len: u64,
    index_off: u64,
    index_len: u64,
    meta_off: u64,
    meta_len: u64,
    version: u32,
}

impl Footer {
    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(FOOTER_LEN);
        buf.extend_from_slice(&self.data_len.to_le_bytes());
        buf.extend_from_slice(&self.data_crc.to_le_bytes());
        buf.extend_from_slice(&self.bloom_off.to_le_bytes());
        buf.extend_from_slice(&self.bloom_len.to_le_bytes());
        buf.extend_from_slice(&self.index_off.to_le_bytes());
        buf.extend_from_slice(&self.index_len.to_le_bytes());
        buf.extend_from_slice(&self.meta_off.to_le_bytes());
        buf.extend_from_slice(&self.meta_len.to_le_bytes());
        buf.extend_from_slice(&self.version.to_le_bytes());
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_le_bytes());
        buf.extend_from_slice(&MAGIC.to_le_bytes());
        debug_assert_eq!(buf.len(), FOOTER_LEN);
        buf
    }

    fn decode(buf: &[u8]) -> io::Result<Self> {
        if buf.len() != FOOTER_LEN {
            return Err(corrupt("footer has the wrong length"));
        }
        let magic = u64::from_le_bytes(buf[68..76].try_into().unwrap());
        if magic != MAGIC {
            return Err(corrupt("not a minidb SSTable (bad magic)"));
        }
        let stored_crc = u32::from_le_bytes(buf[64..68].try_into().unwrap());
        if crc32fast::hash(&buf[..64]) != stored_crc {
            return Err(corrupt("footer checksum mismatch"));
        }
        let version = u32::from_le_bytes(buf[60..64].try_into().unwrap());
        if version != FORMAT_VERSION {
            return Err(corrupt(&format!(
                "unsupported SSTable format version {version}"
            )));
        }
        Ok(Self {
            data_len: u64::from_le_bytes(buf[0..8].try_into().unwrap()),
            data_crc: u32::from_le_bytes(buf[8..12].try_into().unwrap()),
            bloom_off: u64::from_le_bytes(buf[12..20].try_into().unwrap()),
            bloom_len: u64::from_le_bytes(buf[20..28].try_into().unwrap()),
            index_off: u64::from_le_bytes(buf[28..36].try_into().unwrap()),
            index_len: u64::from_le_bytes(buf[36..44].try_into().unwrap()),
            meta_off: u64::from_le_bytes(buf[44..52].try_into().unwrap()),
            meta_len: u64::from_le_bytes(buf[52..60].try_into().unwrap()),
            version,
        })
    }
}

/// Streams a sorted key/value run into a new SSTable file.
///
/// Keys must be appended in ascending order; [`append`](Self::append) rejects
/// anything else rather than silently producing an unsearchable table.
#[derive(Debug)]
pub struct SsTableWriter {
    /// `None` once [`SsTableWriter::finish`] has closed and published the file.
    file: Option<BufWriter<File>>,
    final_path: PathBuf,
    tmp_path: PathBuf,
    data_len: u64,
    hasher: crc32fast::Hasher,
    num_entries: u64,
    num_tombstones: u64,
    min_key: Option<Vec<u8>>,
    last_key: Option<Vec<u8>>,
    /// One hash pair per appended key, used to size and fill the bloom filter
    /// at `finish`, when the exact key count is finally known.
    key_hashes: Vec<(u64, u64)>,
    finished: bool,
}

impl SsTableWriter {
    /// Creates a new table, staged at `<path>.tmp` until [`finish`](Self::finish).
    pub fn create<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let final_path = path.as_ref().to_path_buf();
        let tmp_path = tmp_path_for(&final_path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;

        Ok(Self {
            file: Some(BufWriter::new(file)),
            final_path,
            tmp_path,
            data_len: 0,
            hasher: crc32fast::Hasher::new(),
            num_entries: 0,
            num_tombstones: 0,
            min_key: None,
            last_key: None,
            key_hashes: Vec::new(),
            finished: false,
        })
    }

    /// Appends one entry. Keys must arrive in strictly ascending order.
    pub fn append(&mut self, key: &[u8], entry: &Entry) -> io::Result<()> {
        if let Some(last) = &self.last_key
            && key <= last.as_slice()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "SSTable keys must be appended in strictly ascending order",
            ));
        }

        let (kind, value): (u8, &[u8]) = match entry {
            Entry::Value(v) => (KIND_VALUE, v),
            Entry::Tombstone => (KIND_TOMBSTONE, &[]),
        };

        let mut buf = Vec::with_capacity(ENTRY_HEADER_LEN + key.len() + value.len());
        buf.push(kind);
        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(value);

        self.writer()?.write_all(&buf)?;
        self.hasher.update(&buf);
        self.data_len += buf.len() as u64;

        self.num_entries += 1;
        if matches!(entry, Entry::Tombstone) {
            self.num_tombstones += 1;
        }
        if self.min_key.is_none() {
            self.min_key = Some(key.to_vec());
        }
        self.last_key = Some(key.to_vec());
        self.key_hashes.push(hash_pair(key));
        Ok(())
    }

    /// Writes the remaining sections, fsyncs, and renames the table into place.
    ///
    /// Returns the table's metadata. After this returns `Ok`, the table is
    /// durable and it is safe to rotate the write-ahead log that fed it.
    pub fn finish(mut self) -> io::Result<TableMeta> {
        let data_crc = self.hasher.clone().finalize();
        let data_len = self.data_len;

        // Build the filter now: the exact key count is known, so it can be
        // sized correctly instead of guessed at when the writer was created.
        let bloom_bytes = if self.key_hashes.is_empty() {
            Vec::new()
        } else {
            let mut filter = BloomFilter::new(self.key_hashes.len(), DEFAULT_FP_RATE);
            for &(h1, h2) in &self.key_hashes {
                filter.insert_hashed(h1, h2);
            }
            filter.encode()
        };
        self.writer()?.write_all(&bloom_bytes)?;

        let bloom_off = data_len;
        let bloom_len = bloom_bytes.len() as u64;

        // Index section is still reserved but empty; filled in by milestone 4.
        let index_off = bloom_off + bloom_len;
        let index_len = 0u64;

        let meta = TableMeta {
            num_entries: self.num_entries,
            num_tombstones: self.num_tombstones,
            size_bytes: 0,
            min_key: self.min_key.clone().unwrap_or_default(),
            max_key: self.last_key.clone().unwrap_or_default(),
        };
        let meta_bytes = meta.encode();
        let meta_off = index_off + index_len;
        let meta_len = meta_bytes.len() as u64;

        self.writer()?.write_all(&meta_bytes)?;

        let footer = Footer {
            data_len,
            data_crc,
            bloom_off,
            bloom_len,
            index_off,
            index_len,
            meta_off,
            meta_len,
            version: FORMAT_VERSION,
        };
        self.writer()?.write_all(&footer.encode())?;

        // Durability order: flush userspace buffers, fsync the file contents,
        // rename into place, then fsync the directory so the new name survives.
        let mut file = self
            .file
            .take()
            .ok_or_else(|| corrupt("SSTable writer already finished"))?;
        file.flush()?;
        file.get_ref().sync_all()?;
        drop(file); // close before renaming — Windows will not rename an open file

        fs::rename(&self.tmp_path, &self.final_path)?;
        sync_parent_dir(&self.final_path)?;
        self.finished = true;

        let size_bytes = fs::metadata(&self.final_path)?.len();
        Ok(TableMeta { size_bytes, ..meta })
    }

    /// Borrows the open file, erroring if the writer was already finished.
    fn writer(&mut self) -> io::Result<&mut BufWriter<File>> {
        self.file
            .as_mut()
            .ok_or_else(|| corrupt("SSTable writer already finished"))
    }

    /// Bytes of key/value data written so far, excluding trailing sections.
    pub fn data_len(&self) -> u64 {
        self.data_len
    }

    /// Number of entries appended so far.
    pub fn num_entries(&self) -> u64 {
        self.num_entries
    }
}

impl Drop for SsTableWriter {
    fn drop(&mut self) {
        // An abandoned writer must not leave a temp file behind.
        if !self.finished {
            let _ = fs::remove_file(&self.tmp_path);
        }
    }
}

/// A read handle onto an immutable table file.
#[derive(Debug)]
pub struct SsTable {
    path: PathBuf,
    footer: Footer,
    meta: TableMeta,
    /// `None` for tables written before filters existed, or when the stored
    /// filter fails its checksum. Either way the table stays fully readable —
    /// a missing filter costs a scan, never a wrong answer.
    bloom: Option<BloomFilter>,
}

impl SsTable {
    /// Opens a table, validating its footer and loading its metadata.
    pub fn open<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let mut file = File::open(&path)?;
        let file_len = file.metadata()?.len();

        if (file_len as usize) < FOOTER_LEN {
            return Err(corrupt("file is smaller than an SSTable footer"));
        }

        let mut footer_bytes = vec![0u8; FOOTER_LEN];
        file.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        file.read_exact(&mut footer_bytes)?;
        let footer = Footer::decode(&footer_bytes)?;

        if footer.meta_off + footer.meta_len > file_len {
            return Err(corrupt("meta section extends past end of file"));
        }

        let mut meta_bytes = vec![0u8; footer.meta_len as usize];
        file.seek(SeekFrom::Start(footer.meta_off))?;
        file.read_exact(&mut meta_bytes)?;
        let mut meta = TableMeta::decode(&meta_bytes)?;
        meta.size_bytes = file_len;

        let bloom = if footer.bloom_len > 0 {
            if footer.bloom_off + footer.bloom_len > file_len {
                return Err(corrupt("bloom section extends past end of file"));
            }
            let mut bloom_bytes = vec![0u8; footer.bloom_len as usize];
            file.seek(SeekFrom::Start(footer.bloom_off))?;
            file.read_exact(&mut bloom_bytes)?;
            // A corrupt filter degrades to no filter rather than failing the
            // open: the data section is independently checksummed and still
            // authoritative.
            BloomFilter::decode(&bloom_bytes)
        } else {
            None
        };

        Ok(Self {
            path,
            footer,
            meta,
            bloom,
        })
    }

    /// Looks up `key` in this table.
    ///
    /// `Ok(None)` means the key is absent *from this table* — the caller must
    /// keep searching older tables. `Ok(Some(Entry::Tombstone))` means the key
    /// was deleted here and the search must stop.
    ///
    /// Currently a sequential scan of the data section; see the module docs.
    pub fn get(&self, key: &[u8]) -> io::Result<Option<Entry>> {
        if !self.may_contain(key) {
            return Ok(None);
        }
        // The whole point of carrying a filter: a negative answer here means the
        // key is definitely absent, so the data section is never touched.
        if let Some(bloom) = &self.bloom {
            let (h1, h2) = hash_pair(key);
            if !bloom.contains_hashed(h1, h2) {
                return Ok(None);
            }
        }
        for item in self.iter()? {
            let (k, entry) = item?;
            match k.as_slice().cmp(key) {
                std::cmp::Ordering::Less => continue,
                // Keys are sorted, so passing the target means it is absent.
                std::cmp::Ordering::Greater => return Ok(None),
                std::cmp::Ordering::Equal => return Ok(Some(entry)),
            }
        }
        Ok(None)
    }

    /// Iterates over every entry in ascending key order, tombstones included.
    pub fn iter(&self) -> io::Result<SsTableIter> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(SsTableIter {
            reader: BufReader::new(file),
            remaining: self.footer.data_len,
            done: false,
        })
    }

    /// Verifies the data section against the checksum recorded in the footer.
    ///
    /// Not run on open — that would make opening O(file size). Exposed so tests
    /// and a future repair tool can validate a table on demand.
    pub fn verify(&self) -> io::Result<bool> {
        let mut file = File::open(&self.path)?;
        let mut hasher = crc32fast::Hasher::new();
        let mut remaining = self.footer.data_len;
        let mut buf = vec![0u8; 64 * 1024];

        while remaining > 0 {
            let want = buf.len().min(remaining as usize);
            file.read_exact(&mut buf[..want])?;
            hasher.update(&buf[..want]);
            remaining -= want as u64;
        }
        Ok(hasher.finalize() == self.footer.data_crc)
    }

    /// Returns the metadata recorded in the meta section.
    pub fn meta(&self) -> &TableMeta {
        &self.meta
    }

    /// Returns `true` if `key` falls within this table's key range.
    ///
    /// A cheap pre-filter that avoids opening the data section at all for keys
    /// outside the table's span.
    pub fn may_contain(&self, key: &[u8]) -> bool {
        if self.meta.num_entries == 0 {
            return false;
        }
        key >= self.meta.min_key.as_slice() && key <= self.meta.max_key.as_slice()
    }

    /// Borrows this table's bloom filter, if it has a usable one.
    pub fn bloom(&self) -> Option<&BloomFilter> {
        self.bloom.as_ref()
    }

    /// Returns `true` if this table carries a usable bloom filter.
    pub fn has_bloom(&self) -> bool {
        self.bloom.is_some()
    }

    /// Returns the table's path on disk.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Returns the table's total size in bytes.
    pub fn size_bytes(&self) -> u64 {
        self.meta.size_bytes
    }

    /// Returns the number of entries, tombstones included.
    pub fn len(&self) -> u64 {
        self.meta.num_entries
    }

    /// Returns `true` if the table holds no entries.
    pub fn is_empty(&self) -> bool {
        self.meta.num_entries == 0
    }
}

/// Sequential iterator over a table's data section.
#[derive(Debug)]
pub struct SsTableIter {
    reader: BufReader<File>,
    remaining: u64,
    done: bool,
}

impl Iterator for SsTableIter {
    type Item = io::Result<(Vec<u8>, Entry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done || self.remaining == 0 {
            return None;
        }

        match read_entry(&mut self.reader) {
            Ok((consumed, key, entry)) => {
                self.remaining = self.remaining.saturating_sub(consumed);
                Some(Ok((key, entry)))
            }
            Err(e) => {
                self.done = true;
                Some(Err(e))
            }
        }
    }
}

/// Reads one data entry, returning the bytes consumed alongside the entry.
fn read_entry<R: Read>(reader: &mut R) -> io::Result<(u64, Vec<u8>, Entry)> {
    let mut header = [0u8; ENTRY_HEADER_LEN];
    reader.read_exact(&mut header)?;

    let kind = header[0];
    let key_len = u32::from_le_bytes(header[1..5].try_into().unwrap()) as usize;
    let value_len = u32::from_le_bytes(header[5..9].try_into().unwrap()) as usize;

    let mut key = vec![0u8; key_len];
    reader.read_exact(&mut key)?;

    let entry = match kind {
        KIND_VALUE => {
            let mut value = vec![0u8; value_len];
            reader.read_exact(&mut value)?;
            Entry::Value(value)
        }
        KIND_TOMBSTONE => Entry::Tombstone,
        other => return Err(corrupt(&format!("unknown SSTable entry kind {other}"))),
    };

    let consumed = (ENTRY_HEADER_LEN + key_len + value_len) as u64;
    Ok((consumed, key, entry))
}

fn tmp_path_for(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

fn read_u64(buf: &[u8], cursor: &mut usize) -> io::Result<u64> {
    if buf.len() < *cursor + 8 {
        return Err(corrupt("meta section truncated"));
    }
    let v = u64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(v)
}

fn read_blob(buf: &[u8], cursor: &mut usize) -> io::Result<Vec<u8>> {
    if buf.len() < *cursor + 4 {
        return Err(corrupt("meta section truncated"));
    }
    let len = u32::from_le_bytes(buf[*cursor..*cursor + 4].try_into().unwrap()) as usize;
    *cursor += 4;
    if buf.len() < *cursor + len {
        return Err(corrupt("meta section truncated"));
    }
    let blob = buf[*cursor..*cursor + len].to_vec();
    *cursor += len;
    Ok(blob)
}

pub(crate) fn corrupt(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.to_string())
}

/// Helpers shared by this file's test modules.
#[cfg(test)]
pub(crate) mod tests_support {
    use super::*;

    /// A scratch directory that removes itself on drop.
    pub(crate) struct TempDir(pub(crate) PathBuf);

    impl TempDir {
        pub(crate) fn new(tag: &str) -> Self {
            let path = std::env::temp_dir().join(format!("minidb-sst-{tag}"));
            let _ = fs::remove_dir_all(&path);
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }

        pub(crate) fn file(&self, name: &str) -> PathBuf {
            self.0.join(name)
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    /// Writes a table from `(key, Some(value) | None)` pairs; `None` is a tombstone.
    pub(crate) fn write_table(path: &Path, entries: &[(&str, Option<&str>)]) -> TableMeta {
        let mut w = SsTableWriter::create(path).unwrap();
        for (k, v) in entries {
            let entry = match v {
                Some(v) => Entry::Value(v.as_bytes().to_vec()),
                None => Entry::Tombstone,
            };
            w.append(k.as_bytes(), &entry).unwrap();
        }
        w.finish().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::tests_support::*;
    use super::*;

    #[test]
    fn a_written_table_reads_back_every_entry() {
        let dir = TempDir::new("roundtrip");
        let path = dir.file("t.sst");
        write_table(
            &path,
            &[
                ("alpha", Some("1")),
                ("bravo", Some("2")),
                ("delta", Some("4")),
            ],
        );

        let table = SsTable::open(&path).unwrap();
        assert_eq!(
            table.get(b"alpha").unwrap(),
            Some(Entry::Value(b"1".to_vec()))
        );
        assert_eq!(
            table.get(b"bravo").unwrap(),
            Some(Entry::Value(b"2".to_vec()))
        );
        assert_eq!(
            table.get(b"delta").unwrap(),
            Some(Entry::Value(b"4".to_vec()))
        );
    }

    #[test]
    fn absent_keys_return_none() {
        let dir = TempDir::new("absent");
        let path = dir.file("t.sst");
        write_table(&path, &[("b", Some("1")), ("d", Some("2"))]);

        let table = SsTable::open(&path).unwrap();
        assert_eq!(table.get(b"a").unwrap(), None, "before min key");
        assert_eq!(table.get(b"c").unwrap(), None, "between keys");
        assert_eq!(table.get(b"z").unwrap(), None, "after max key");
    }

    #[test]
    fn tombstones_round_trip_and_are_distinguishable_from_absence() {
        let dir = TempDir::new("tombstone");
        let path = dir.file("t.sst");
        write_table(&path, &[("gone", None), ("here", Some("v"))]);

        let table = SsTable::open(&path).unwrap();
        assert_eq!(table.get(b"gone").unwrap(), Some(Entry::Tombstone));
        assert_eq!(
            table.get(b"here").unwrap(),
            Some(Entry::Value(b"v".to_vec()))
        );
        assert_eq!(table.get(b"never").unwrap(), None);
    }

    #[test]
    fn metadata_records_counts_and_key_range() {
        let dir = TempDir::new("meta");
        let path = dir.file("t.sst");
        let meta = write_table(&path, &[("a", Some("1")), ("m", None), ("z", Some("3"))]);

        assert_eq!(meta.num_entries, 3);
        assert_eq!(meta.num_tombstones, 1);
        assert_eq!(meta.num_values(), 2);
        assert_eq!(meta.min_key, b"a".to_vec());
        assert_eq!(meta.max_key, b"z".to_vec());
        assert!(meta.size_bytes > 0);

        let table = SsTable::open(&path).unwrap();
        assert_eq!(table.meta().num_entries, 3);
        assert_eq!(table.meta().min_key, b"a".to_vec());
        assert_eq!(table.meta().max_key, b"z".to_vec());
    }

    #[test]
    fn iteration_yields_entries_in_ascending_key_order() {
        let dir = TempDir::new("iter");
        let path = dir.file("t.sst");
        write_table(&path, &[("a", Some("1")), ("b", None), ("c", Some("3"))]);

        let table = SsTable::open(&path).unwrap();
        let got: Vec<_> = table
            .iter()
            .unwrap()
            .map(|r| r.unwrap())
            .map(|(k, e)| (String::from_utf8(k).unwrap(), e))
            .collect();

        assert_eq!(
            got,
            vec![
                ("a".to_string(), Entry::Value(b"1".to_vec())),
                ("b".to_string(), Entry::Tombstone),
                ("c".to_string(), Entry::Value(b"3".to_vec())),
            ]
        );
    }

    #[test]
    fn out_of_order_appends_are_rejected() {
        let dir = TempDir::new("order");
        let path = dir.file("t.sst");
        let mut w = SsTableWriter::create(&path).unwrap();
        w.append(b"b", &Entry::Value(b"1".to_vec())).unwrap();

        let err = w.append(b"a", &Entry::Value(b"2".to_vec())).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        // Duplicates are also out of order — strictly ascending is required.
        let err = w.append(b"b", &Entry::Value(b"3".to_vec())).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn an_empty_table_is_valid_and_contains_nothing() {
        let dir = TempDir::new("empty");
        let path = dir.file("t.sst");
        let meta = write_table(&path, &[]);
        assert_eq!(meta.num_entries, 0);

        let table = SsTable::open(&path).unwrap();
        assert!(table.is_empty());
        assert_eq!(table.get(b"anything").unwrap(), None);
        assert!(!table.may_contain(b"anything"));
        assert_eq!(table.iter().unwrap().count(), 0);
    }

    #[test]
    fn binary_keys_and_values_survive_a_round_trip() {
        let dir = TempDir::new("binary");
        let path = dir.file("t.sst");

        let mut w = SsTableWriter::create(&path).unwrap();
        w.append(&[0x00, 0x01], &Entry::Value(vec![0xde, 0xad]))
            .unwrap();
        w.append(&[0xff, 0xfe], &Entry::Value(vec![0x00, 0xbe, 0xef]))
            .unwrap();
        w.finish().unwrap();

        let table = SsTable::open(&path).unwrap();
        assert_eq!(
            table.get(&[0x00, 0x01]).unwrap(),
            Some(Entry::Value(vec![0xde, 0xad]))
        );
        assert_eq!(
            table.get(&[0xff, 0xfe]).unwrap(),
            Some(Entry::Value(vec![0x00, 0xbe, 0xef]))
        );
    }

    #[test]
    fn empty_values_are_distinct_from_tombstones() {
        let dir = TempDir::new("empty-value");
        let path = dir.file("t.sst");
        write_table(&path, &[("dead", None), ("empty", Some(""))]);

        let table = SsTable::open(&path).unwrap();
        assert_eq!(table.get(b"dead").unwrap(), Some(Entry::Tombstone));
        assert_eq!(table.get(b"empty").unwrap(), Some(Entry::Value(Vec::new())));
    }

    #[test]
    fn a_large_table_reads_back_correctly() {
        let dir = TempDir::new("large");
        let path = dir.file("t.sst");

        let mut w = SsTableWriter::create(&path).unwrap();
        for i in 0..2_000u32 {
            let key = format!("key:{i:06}");
            w.append(key.as_bytes(), &Entry::Value(i.to_be_bytes().to_vec()))
                .unwrap();
        }
        let meta = w.finish().unwrap();
        assert_eq!(meta.num_entries, 2_000);

        let table = SsTable::open(&path).unwrap();
        assert_eq!(table.iter().unwrap().count(), 2_000);
        for i in [0u32, 1, 999, 1_500, 1_999] {
            let key = format!("key:{i:06}");
            assert_eq!(
                table.get(key.as_bytes()).unwrap(),
                Some(Entry::Value(i.to_be_bytes().to_vec()))
            );
        }
        assert_eq!(table.get(b"key:002000").unwrap(), None);
    }

    #[test]
    fn the_data_section_checksum_verifies() {
        let dir = TempDir::new("verify");
        let path = dir.file("t.sst");
        write_table(&path, &[("a", Some("1")), ("b", Some("2"))]);

        assert!(SsTable::open(&path).unwrap().verify().unwrap());
    }

    #[test]
    fn corrupting_the_data_section_fails_verification() {
        let dir = TempDir::new("corrupt-data");
        let path = dir.file("t.sst");
        write_table(&path, &[("a", Some("1111")), ("b", Some("2222"))]);

        let mut bytes = fs::read(&path).unwrap();
        bytes[12] ^= 0xff; // inside the first entry's payload
        fs::write(&path, &bytes).unwrap();

        assert!(!SsTable::open(&path).unwrap().verify().unwrap());
    }

    #[test]
    fn a_file_with_a_bad_magic_is_rejected() {
        let dir = TempDir::new("bad-magic");
        let path = dir.file("t.sst");
        fs::write(&path, vec![0u8; FOOTER_LEN + 10]).unwrap();

        let err = SsTable::open(&path).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_truncated_file_is_rejected_rather_than_misread() {
        let dir = TempDir::new("truncated");
        let path = dir.file("t.sst");
        write_table(&path, &[("a", Some("1"))]);

        let full = fs::metadata(&path).unwrap().len();
        let file = OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(full - 20).unwrap();
        drop(file);

        assert!(SsTable::open(&path).is_err());
    }

    #[test]
    fn a_corrupt_footer_is_caught_by_its_checksum() {
        let dir = TempDir::new("bad-footer");
        let path = dir.file("t.sst");
        write_table(&path, &[("a", Some("1"))]);

        let mut bytes = fs::read(&path).unwrap();
        let len = bytes.len();
        // Corrupt data_len inside the footer, leaving magic intact.
        bytes[len - FOOTER_LEN] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let err = SsTable::open(&path).unwrap_err();
        assert!(err.to_string().contains("checksum"));
    }

    #[test]
    fn an_abandoned_writer_leaves_no_temp_file() {
        let dir = TempDir::new("abandoned");
        let path = dir.file("t.sst");

        {
            let mut w = SsTableWriter::create(&path).unwrap();
            w.append(b"a", &Entry::Value(b"1".to_vec())).unwrap();
            // dropped without finish()
        }

        assert!(!path.exists(), "no table should be published");
        assert!(
            !tmp_path_for(&path).exists(),
            "temp file should be cleaned up"
        );
    }

    #[test]
    fn the_table_is_only_published_on_finish() {
        let dir = TempDir::new("publish");
        let path = dir.file("t.sst");

        let mut w = SsTableWriter::create(&path).unwrap();
        w.append(b"a", &Entry::Value(b"1".to_vec())).unwrap();
        assert!(!path.exists(), "not visible before finish");
        assert!(tmp_path_for(&path).exists(), "staged as a temp file");

        w.finish().unwrap();
        assert!(path.exists());
        assert!(!tmp_path_for(&path).exists());
    }

    #[test]
    fn may_contain_bounds_the_key_range() {
        let dir = TempDir::new("range");
        let path = dir.file("t.sst");
        write_table(
            &path,
            &[("d", Some("1")), ("m", Some("2")), ("s", Some("3"))],
        );

        let table = SsTable::open(&path).unwrap();
        assert!(!table.may_contain(b"a"));
        assert!(table.may_contain(b"d"));
        assert!(table.may_contain(b"j"), "within range, even if absent");
        assert!(table.may_contain(b"s"));
        assert!(!table.may_contain(b"z"));
    }
}

#[cfg(test)]
mod bloom_integration_tests {
    use super::tests_support::*;
    use super::*;

    #[test]
    fn a_written_table_carries_a_bloom_filter() {
        let dir = TempDir::new("has-bloom");
        let path = dir.file("t.sst");
        write_table(&path, &[("a", Some("1")), ("b", Some("2"))]);

        let table = SsTable::open(&path).unwrap();
        assert!(table.has_bloom(), "tables should ship a filter");
        let bloom = table.bloom().unwrap();
        assert!(bloom.num_bits() > 0);
        assert!(bloom.num_hashes() > 0);
    }

    #[test]
    fn the_filter_reports_every_stored_key_as_present() {
        let dir = TempDir::new("no-false-negatives");
        let path = dir.file("t.sst");

        let mut w = SsTableWriter::create(&path).unwrap();
        for i in 0..1_000u32 {
            let key = format!("key:{i:05}");
            w.append(key.as_bytes(), &Entry::Value(b"v".to_vec()))
                .unwrap();
        }
        w.finish().unwrap();

        let table = SsTable::open(&path).unwrap();
        let bloom = table.bloom().unwrap();
        for i in 0..1_000u32 {
            let key = format!("key:{i:05}");
            assert!(
                bloom.contains(key.as_bytes()),
                "false negative would make {key} unreadable"
            );
        }
    }

    #[test]
    fn lookups_still_return_correct_answers_with_a_filter_in_place() {
        let dir = TempDir::new("correctness");
        let path = dir.file("t.sst");

        let mut w = SsTableWriter::create(&path).unwrap();
        for i in (0..1_000u32).step_by(2) {
            let key = format!("key:{i:05}");
            w.append(key.as_bytes(), &Entry::Value(i.to_be_bytes().to_vec()))
                .unwrap();
        }
        w.finish().unwrap();

        let table = SsTable::open(&path).unwrap();
        // Present keys must be found; absent keys must not, filter or no filter.
        for i in 0..1_000u32 {
            let key = format!("key:{i:05}");
            let got = table.get(key.as_bytes()).unwrap();
            if i % 2 == 0 {
                assert_eq!(got, Some(Entry::Value(i.to_be_bytes().to_vec())), "{key}");
            } else {
                assert_eq!(got, None, "{key} is not in the table");
            }
        }
    }

    #[test]
    fn tombstoned_keys_are_in_the_filter_too() {
        // A tombstone must be findable, or a delete would stop shadowing the
        // older value beneath it.
        let dir = TempDir::new("tombstone-bloom");
        let path = dir.file("t.sst");
        write_table(&path, &[("dead", None), ("live", Some("v"))]);

        let table = SsTable::open(&path).unwrap();
        assert!(table.bloom().unwrap().contains(b"dead"));
        assert_eq!(table.get(b"dead").unwrap(), Some(Entry::Tombstone));
    }

    #[test]
    fn an_empty_table_carries_no_filter() {
        let dir = TempDir::new("empty-bloom");
        let path = dir.file("t.sst");
        write_table(&path, &[]);

        let table = SsTable::open(&path).unwrap();
        assert!(!table.has_bloom(), "no keys means no filter worth writing");
        assert_eq!(table.get(b"anything").unwrap(), None);
    }

    #[test]
    fn a_corrupt_filter_degrades_to_a_scan_rather_than_a_wrong_answer() {
        let dir = TempDir::new("corrupt-bloom");
        let path = dir.file("t.sst");
        write_table(&path, &[("a", Some("1")), ("b", Some("2"))]);

        // Corrupt a byte inside the bloom section, which begins right after the
        // data section.
        let table = SsTable::open(&path).unwrap();
        let bloom_off = table.footer.bloom_off as usize;
        drop(table);

        let mut bytes = fs::read(&path).unwrap();
        bytes[bloom_off + 6] ^= 0xff;
        fs::write(&path, &bytes).unwrap();

        let table = SsTable::open(&path).unwrap();
        assert!(!table.has_bloom(), "corrupt filter must be discarded");
        // Reads still work, just without the filter's help.
        assert_eq!(table.get(b"a").unwrap(), Some(Entry::Value(b"1".to_vec())));
        assert_eq!(table.get(b"b").unwrap(), Some(Entry::Value(b"2".to_vec())));
        assert_eq!(table.get(b"zzz").unwrap(), None);
    }

    #[test]
    fn the_filter_rejects_most_absent_keys_without_a_data_read() {
        let dir = TempDir::new("effective");
        let path = dir.file("t.sst");

        let mut w = SsTableWriter::create(&path).unwrap();
        for i in 0..2_000u32 {
            let key = format!("present:{i:05}");
            w.append(key.as_bytes(), &Entry::Value(b"v".to_vec()))
                .unwrap();
        }
        w.finish().unwrap();

        let table = SsTable::open(&path).unwrap();
        let bloom = table.bloom().unwrap();

        let trials = 5_000u32;
        let rejected = (0..trials)
            .filter(|i| !bloom.contains(format!("absent:{i:05}").as_bytes()))
            .count();

        let reject_rate = rejected as f64 / trials as f64;
        assert!(
            reject_rate > 0.90,
            "filter only rejected {reject_rate:.3} of absent keys; it is not earning its bytes"
        );
    }

    #[test]
    fn the_filter_is_small_relative_to_the_data() {
        let dir = TempDir::new("size");
        let path = dir.file("t.sst");

        let mut w = SsTableWriter::create(&path).unwrap();
        for i in 0..5_000u32 {
            let key = format!("key:{i:06}");
            w.append(key.as_bytes(), &Entry::Value(vec![0u8; 64]))
                .unwrap();
        }
        let meta = w.finish().unwrap();

        let table = SsTable::open(&path).unwrap();
        let bloom_bytes = table.bloom().unwrap().size_bytes() as u64;
        // ~9.6 bits/key against ~80 bytes/entry of payload.
        assert!(
            bloom_bytes * 20 < meta.size_bytes,
            "filter is {bloom_bytes} bytes against a {} byte table",
            meta.size_bytes
        );
    }
}
