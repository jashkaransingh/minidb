//! In-memory sorted table, keyed by *internal key*.
//!
//! The memtable is the write buffer at the top of the LSM tree. All writes land
//! here first (after being appended to the WAL for durability). Once it exceeds
//! a size threshold it is frozen and flushed to disk as an immutable SSTable.
//!
//! # Internal keys and MVCC
//!
//! Every mutation carries a monotonically increasing **sequence number**, and the
//! memtable is keyed by [`InternalKey`] — the pair `(user_key, seq)` — rather
//! than by the user key alone. An overwrite therefore *adds* a version instead
//! of replacing one, which is what makes point-in-time snapshot reads possible.
//!
//! The ordering is the whole trick:
//!
//! ```text
//! user_key ascending, then seq DESCENDING
//! ```
//!
//! so all versions of one key are adjacent and run newest-first:
//!
//! ```text
//! ("apple", 9) ("apple", 4) ("apple", 1) ("banana", 7) ("banana", 2) …
//! ```
//!
//! A read at snapshot `S` seeks to the synthetic key `(user_key, S)`. Because
//! seq sorts descending, every version *newer* than `S` sorts strictly before
//! that probe and every version at or older than `S` sorts at or after it — so
//! the very first entry the seek lands on is the newest version visible to the
//! snapshot. One seek, no scanning past invisible versions. See
//! [`MemTable::get`].
//!
//! # Why a skiplist and not a `BTreeMap`
//!
//! Keys must stay sorted — the flush is a single sequential write and range
//! scans are a straight walk — but a `BTreeMap` also forces every reader to take
//! a lock, and in this store a writer holds its lock across an fsync. Readers
//! would stall on the disk.
//!
//! [`crate::skiplist::SkipList`] is an insert-only, single-writer ordered list
//! whose reads are lock-free pointer chasing. Insert-only is not a restriction
//! here: under MVCC a delete is a tombstone *insert* and an overwrite is a new
//! version *insert*, so nothing is ever removed or mutated in place, which is
//! exactly the case a lock-free structure handles without reclamation
//! machinery. The single-writer restriction is discharged by the store's write
//! mutex, which mutations already hold to serialize log appends.

use crate::skiplist::SkipList;

/// A value slot in the memtable.
///
/// Deletes are recorded as `Tombstone` rather than removing the key outright.
/// An LSM tree cannot delete in place: older SSTables on disk may still hold a
/// value for this key, so the tombstone must shadow them until compaction drops
/// both. The memtable follows the same rule to stay consistent with the levels
/// below it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Entry {
    Value(Vec<u8>),
    Tombstone,
}

impl Entry {
    /// Returns the value, or `None` for a tombstone.
    pub fn value(&self) -> Option<&[u8]> {
        match self {
            Entry::Value(v) => Some(v),
            Entry::Tombstone => None,
        }
    }

    /// Returns `true` if this entry is a deletion marker.
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Entry::Tombstone)
    }

    /// Bytes of payload this entry holds.
    pub fn len(&self) -> usize {
        match self {
            Entry::Value(v) => v.len(),
            Entry::Tombstone => 0,
        }
    }

    /// Returns `true` if this entry carries no payload bytes.
    ///
    /// Note that an empty *value* is a real value, distinct from a tombstone —
    /// this reports only the byte count.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// A user key paired with the sequence number of the write that produced it.
///
/// The `Ord` implementation is load-bearing: **user key ascending, sequence
/// number descending**. Versions of one key are therefore contiguous and ordered
/// newest-first, which turns "find the newest version visible at snapshot `S`"
/// into a single seek to `(user_key, S)`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: u64,
}

impl InternalKey {
    /// Builds an internal key.
    pub fn new(user_key: impl Into<Vec<u8>>, seq: u64) -> Self {
        Self {
            user_key: user_key.into(),
            seq,
        }
    }

    /// The probe key for reading `user_key` at snapshot `snapshot`.
    ///
    /// Sorts at or before every version of `user_key` with `seq <= snapshot`,
    /// and strictly after every version with `seq > snapshot`.
    pub fn probe(user_key: &[u8], snapshot: u64) -> Self {
        Self {
            user_key: user_key.to_vec(),
            seq: snapshot,
        }
    }

    /// Approximate heap footprint, in bytes.
    pub fn size_bytes(&self) -> usize {
        self.user_key.len() + 8
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // User key ascending, then sequence number DESCENDING so the newest
        // version of a key sorts first.
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.seq.cmp(&self.seq))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Compares two internal keys given as their parts, without allocating.
pub fn compare_internal(a_key: &[u8], a_seq: u64, b_key: &[u8], b_seq: u64) -> std::cmp::Ordering {
    a_key.cmp(b_key).then_with(|| b_seq.cmp(&a_seq))
}

/// Sorted in-memory write buffer, holding every version written since the last
/// flush.
///
/// Reads take no lock. Writes require exclusive access — either statically, via
/// `&mut self` on [`insert`](Self::insert), or dynamically, via the `unsafe`
/// [`insert_shared`](Self::insert_shared) for callers that hold the store's
/// write mutex and reach the memtable through an `Arc`.
#[derive(Debug, Default)]
pub struct MemTable {
    list: SkipList,
}

impl MemTable {
    /// Creates an empty memtable.
    pub fn new() -> Self {
        Self {
            list: SkipList::new(),
        }
    }

    /// Records `entry` for `key` at sequence number `seq`.
    ///
    /// This never replaces an existing entry: sequence numbers are unique per
    /// mutation, so each call adds a new version. That is what an overwrite
    /// *is* under MVCC — the old version stays readable by older snapshots
    /// until compaction collects it.
    pub fn insert(&mut self, key: &[u8], seq: u64, entry: Entry) {
        self.list
            .insert_exclusive(InternalKey::new(key.to_vec(), seq), entry);
    }

    /// Records `entry` through a shared reference.
    ///
    /// # Safety
    ///
    /// **At most one thread may be inside this function at a time.** Concurrent
    /// readers are fine — that is the point of the structure — but two
    /// concurrent writers would corrupt it.
    ///
    /// In minidb this is discharged by the store's write mutex, which every
    /// mutation holds anyway to serialize log appends. Reaching the memtable
    /// through an `Arc` is what lets readers hold it without a lock, so the
    /// exclusivity cannot be proven by the borrow checker and has to be an
    /// obligation instead.
    pub unsafe fn insert_shared(&self, key: &[u8], seq: u64, entry: Entry) {
        // SAFETY: forwarded to the caller.
        unsafe { self.list.insert(InternalKey::new(key.to_vec(), seq), entry) }
    }

    /// Convenience wrapper: writes a value at `seq`.
    pub fn put(&mut self, key: &[u8], seq: u64, value: Vec<u8>) {
        self.insert(key, seq, Entry::Value(value));
    }

    /// Convenience wrapper: writes a tombstone at `seq`.
    pub fn delete(&mut self, key: &[u8], seq: u64) {
        self.insert(key, seq, Entry::Tombstone);
    }

    /// Returns the newest version of `key` visible at `snapshot`.
    ///
    /// `None` means this memtable holds no version of `key` at or below
    /// `snapshot` — the caller must keep searching older levels.
    /// `Some(Entry::Tombstone)` means the key was deleted at or below the
    /// snapshot, and the search must **stop** rather than fall through to a
    /// stale value on disk.
    ///
    /// Implemented as one seek: `(key, snapshot)` sorts immediately before the
    /// newest visible version, so the first entry at or after it is the answer —
    /// provided it still belongs to `key`.
    pub fn get(&self, key: &[u8], snapshot: u64) -> Option<&Entry> {
        let (found, entry) = self.list.seek(&InternalKey::probe(key, snapshot))?;
        if found.user_key == key {
            Some(entry)
        } else {
            // The seek ran off the end of this key's versions, so every version
            // of `key` here is newer than the snapshot.
            None
        }
    }

    /// Returns the number of stored versions, tombstones included.
    pub fn len(&self) -> usize {
        self.list.len()
    }

    /// Returns `true` if the memtable holds no entries at all.
    pub fn is_empty(&self) -> bool {
        self.list.is_empty()
    }

    /// Returns the approximate heap footprint of the stored data, in bytes.
    ///
    /// Used to decide when to freeze this memtable and flush it to an SSTable.
    pub fn size_bytes(&self) -> usize {
        self.list.size_bytes()
    }

    /// Returns the highest sequence number stored, or 0 if empty.
    pub fn max_seq(&self) -> u64 {
        self.list.max_seq()
    }

    /// Iterates over every version in internal-key order — user key ascending,
    /// sequence number descending — tombstones included.
    ///
    /// This is the flush path: an SSTable writer consumes this stream directly,
    /// which is why the on-disk ordering has to match this one exactly.
    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &Entry)> {
        self.list.iter()
    }

    /// Iterates over versions whose user key falls in `[start, end)`.
    ///
    /// `end` of `None` means unbounded. Ordering is the same internal-key order
    /// as [`iter`](Self::iter), so all versions of a key arrive together,
    /// newest first.
    pub fn range<'a>(
        &'a self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> impl Iterator<Item = (&'a InternalKey, &'a Entry)> {
        // Both bounds use seq = u64::MAX because that sorts before every real
        // version of a key (sequence numbers descend): the lower bound therefore
        // skips no version of `start`, and the upper bound excludes `end`
        // entirely rather than admitting its newest version.
        let lower = InternalKey::new(start.to_vec(), u64::MAX);
        let upper = end.map(|e| InternalKey::new(e.to_vec(), u64::MAX));
        self.list
            .iter_from(&lower)
            .take_while(move |(k, _)| match &upper {
                Some(limit) => *k < limit,
                None => true,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(t: &mut MemTable, k: &str, seq: u64, v: &str) {
        t.put(k.as_bytes(), seq, v.as_bytes().to_vec());
    }

    fn get(t: &MemTable, k: &str, snapshot: u64) -> Option<String> {
        match t.get(k.as_bytes(), snapshot) {
            Some(Entry::Value(v)) => Some(String::from_utf8(v.clone()).unwrap()),
            _ => None,
        }
    }

    #[test]
    fn get_on_empty_table_returns_none() {
        let t = MemTable::new();
        assert_eq!(t.get(b"absent", u64::MAX), None);
        assert!(t.is_empty());
    }

    #[test]
    fn put_then_get_round_trips() {
        let mut t = MemTable::new();
        put(&mut t, "alpha", 1, "one");
        assert_eq!(get(&t, "alpha", u64::MAX).as_deref(), Some("one"));
    }

    #[test]
    fn an_overwrite_adds_a_version_rather_than_replacing_one() {
        let mut t = MemTable::new();
        put(&mut t, "k", 1, "first");
        put(&mut t, "k", 2, "second");

        assert_eq!(t.len(), 2, "both versions are retained");
        assert_eq!(get(&t, "k", u64::MAX).as_deref(), Some("second"));
    }

    #[test]
    fn a_snapshot_does_not_see_versions_written_after_it() {
        let mut t = MemTable::new();
        put(&mut t, "k", 1, "v1");
        put(&mut t, "k", 5, "v5");
        put(&mut t, "k", 9, "v9");

        assert_eq!(get(&t, "k", 0), None, "before any version exists");
        assert_eq!(get(&t, "k", 1).as_deref(), Some("v1"));
        assert_eq!(get(&t, "k", 4).as_deref(), Some("v1"), "between versions");
        assert_eq!(get(&t, "k", 5).as_deref(), Some("v5"), "exactly at a write");
        assert_eq!(get(&t, "k", 8).as_deref(), Some("v5"));
        assert_eq!(get(&t, "k", 9).as_deref(), Some("v9"));
        assert_eq!(get(&t, "k", u64::MAX).as_deref(), Some("v9"));
    }

    #[test]
    fn a_tombstone_hides_older_versions_but_not_from_older_snapshots() {
        let mut t = MemTable::new();
        put(&mut t, "k", 1, "v");
        t.delete(b"k", 2);

        assert_eq!(t.get(b"k", 2), Some(&Entry::Tombstone));
        assert_eq!(get(&t, "k", 2), None);
        // A snapshot taken before the delete still sees the value.
        assert_eq!(get(&t, "k", 1).as_deref(), Some("v"));
    }

    #[test]
    fn a_write_after_a_delete_resurrects_the_key() {
        let mut t = MemTable::new();
        put(&mut t, "k", 1, "v1");
        t.delete(b"k", 2);
        put(&mut t, "k", 3, "v2");

        assert_eq!(get(&t, "k", 3).as_deref(), Some("v2"));
        assert_eq!(t.get(b"k", 2), Some(&Entry::Tombstone));
        assert_eq!(get(&t, "k", 1).as_deref(), Some("v1"));
    }

    #[test]
    fn a_seek_past_a_keys_versions_does_not_leak_the_next_key() {
        let mut t = MemTable::new();
        put(&mut t, "a", 10, "a-value");
        put(&mut t, "b", 1, "b-value");

        // Snapshot 5 is older than every version of "a", and the seek would
        // otherwise land on ("b", 1).
        assert_eq!(t.get(b"a", 5), None);
    }

    #[test]
    fn internal_keys_order_by_key_then_descending_sequence() {
        let mut keys = vec![
            InternalKey::new(b"b".to_vec(), 1),
            InternalKey::new(b"a".to_vec(), 1),
            InternalKey::new(b"a".to_vec(), 9),
            InternalKey::new(b"a".to_vec(), 5),
        ];
        keys.sort();
        assert_eq!(
            keys,
            vec![
                InternalKey::new(b"a".to_vec(), 9),
                InternalKey::new(b"a".to_vec(), 5),
                InternalKey::new(b"a".to_vec(), 1),
                InternalKey::new(b"b".to_vec(), 1),
            ]
        );
    }

    #[test]
    fn compare_internal_matches_the_internal_key_ordering() {
        use std::cmp::Ordering;
        assert_eq!(compare_internal(b"a", 1, b"b", 1), Ordering::Less);
        assert_eq!(compare_internal(b"a", 1, b"a", 9), Ordering::Greater);
        assert_eq!(compare_internal(b"a", 9, b"a", 1), Ordering::Less);
        assert_eq!(compare_internal(b"a", 5, b"a", 5), Ordering::Equal);
    }

    #[test]
    fn iteration_is_in_internal_key_order() {
        let mut t = MemTable::new();
        put(&mut t, "delta", 1, "x");
        put(&mut t, "alpha", 2, "x");
        put(&mut t, "alpha", 7, "x");
        put(&mut t, "charlie", 3, "x");

        let seen: Vec<_> = t
            .iter()
            .map(|(k, _)| (String::from_utf8(k.user_key.clone()).unwrap(), k.seq))
            .collect();
        assert_eq!(
            seen,
            vec![
                ("alpha".into(), 7),
                ("alpha".into(), 2),
                ("charlie".into(), 3),
                ("delta".into(), 1),
            ]
        );
    }

    #[test]
    fn range_is_half_open_and_keeps_every_version_of_the_start_key() {
        let mut t = MemTable::new();
        for (k, seq) in [("a", 1), ("b", 2), ("b", 8), ("c", 3), ("d", 4)] {
            put(&mut t, k, seq, "x");
        }

        let seen: Vec<_> = t
            .range(b"b", Some(b"d"))
            .map(|(k, _)| (String::from_utf8(k.user_key.clone()).unwrap(), k.seq))
            .collect();
        assert_eq!(
            seen,
            vec![("b".into(), 8), ("b".into(), 2), ("c".into(), 3)],
            "start inclusive with all its versions, end exclusive"
        );
    }

    #[test]
    fn an_unbounded_range_runs_to_the_end() {
        let mut t = MemTable::new();
        for (k, seq) in [("a", 1), ("b", 2), ("c", 3)] {
            put(&mut t, k, seq, "x");
        }
        assert_eq!(t.range(b"b", None).count(), 2);
        assert_eq!(t.range(b"", None).count(), 3);
    }

    #[test]
    fn size_tracking_grows_with_every_version() {
        let mut t = MemTable::new();
        put(&mut t, "key", 1, "value"); // 3 + 8 + 5
        assert_eq!(t.size_bytes(), 16);
        put(&mut t, "key", 2, "v"); // 3 + 8 + 1
        assert_eq!(t.size_bytes(), 28, "an overwrite adds, never subtracts");
        t.delete(b"key", 3); // 3 + 8 + 0
        assert_eq!(t.size_bytes(), 39);
    }

    #[test]
    fn max_seq_tracks_the_highest_sequence_inserted() {
        let mut t = MemTable::new();
        assert_eq!(t.max_seq(), 0);
        put(&mut t, "a", 4, "x");
        put(&mut t, "b", 2, "x");
        assert_eq!(t.max_seq(), 4);
    }

    #[test]
    fn binary_keys_and_values_are_supported() {
        let mut t = MemTable::new();
        t.put(&[0x00, 0xff], 1, vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(
            t.get(&[0x00, 0xff], 1),
            Some(&Entry::Value(vec![0xde, 0xad, 0xbe, 0xef]))
        );
    }

    #[test]
    fn an_empty_value_is_distinct_from_a_tombstone() {
        let mut t = MemTable::new();
        t.put(b"k", 1, Vec::new());
        assert_eq!(t.get(b"k", 1), Some(&Entry::Value(Vec::new())));
        assert!(!t.get(b"k", 1).unwrap().is_tombstone());
    }

    #[test]
    fn a_fresh_memtable_starts_empty() {
        // A flush installs a new memtable rather than clearing the old one:
        // readers may still be holding the old one through an `Arc`, and
        // mutating it under them is exactly what this design rules out.
        let t = MemTable::new();
        assert!(t.is_empty());
        assert_eq!(t.size_bytes(), 0);
        assert_eq!(t.max_seq(), 0);
    }
}
