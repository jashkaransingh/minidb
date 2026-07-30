//! In-memory sorted table.
//!
//! The memtable is the write buffer at the top of the LSM tree. All writes land
//! here first (after being appended to the WAL for durability). Once it exceeds
//! a size threshold it is frozen and flushed to disk as an immutable SSTable.
//!
//! Backed by a `BTreeMap` so keys stay in sorted order, which makes the eventual
//! flush a single sequential write and makes range scans cheap.

use std::collections::BTreeMap;

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

/// Sorted in-memory write buffer.
#[derive(Debug, Default)]
pub struct MemTable {
    map: BTreeMap<Vec<u8>, Entry>,
    /// Approximate heap footprint of live keys and values, in bytes.
    size_bytes: usize,
}

impl MemTable {
    /// Creates an empty memtable.
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
            size_bytes: 0,
        }
    }

    /// Inserts or overwrites `key` with `value`.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>) {
        let added = key.len() + value.len();
        if let Some(old) = self.map.insert(key.clone(), Entry::Value(value)) {
            self.size_bytes -= key.len() + entry_len(&old);
        }
        self.size_bytes += added;
    }

    /// Returns the value for `key`, or `None` if it is absent or deleted.
    pub fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.map.get(key) {
            Some(Entry::Value(v)) => Some(v),
            Some(Entry::Tombstone) | None => None,
        }
    }

    /// Marks `key` as deleted by writing a tombstone.
    ///
    /// Returns `true` if a live value was shadowed by this call.
    pub fn delete(&mut self, key: Vec<u8>) -> bool {
        let existed = matches!(self.map.get(&key), Some(Entry::Value(_)));
        let key_len = key.len();
        if let Some(old) = self.map.insert(key, Entry::Tombstone) {
            self.size_bytes -= entry_len(&old);
        } else {
            self.size_bytes += key_len;
        }
        existed
    }

    /// Returns the raw entry for `key`, including tombstones.
    ///
    /// Reads that fall through to lower levels need to distinguish "deleted
    /// here" from "not present here" — the first stops the search, the second
    /// continues it. [`get`](Self::get) collapses both to `None`.
    pub fn get_entry(&self, key: &[u8]) -> Option<&Entry> {
        self.map.get(key)
    }

    /// Returns the number of entries, counting tombstones.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Returns `true` if the memtable holds no entries at all.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Returns the approximate heap footprint of the stored data, in bytes.
    ///
    /// Used to decide when to freeze this memtable and flush it to an SSTable.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    /// Iterates over every entry in ascending key order, tombstones included.
    ///
    /// This is the flush path: an SSTable writer consumes this stream directly.
    pub fn iter(&self) -> impl Iterator<Item = (&Vec<u8>, &Entry)> {
        self.map.iter()
    }

    /// Iterates over live key/value pairs in ascending key order.
    pub fn iter_values(&self) -> impl Iterator<Item = (&[u8], &[u8])> {
        self.map.iter().filter_map(|(k, e)| match e {
            Entry::Value(v) => Some((k.as_slice(), v.as_slice())),
            Entry::Tombstone => None,
        })
    }

    /// Removes all entries.
    pub fn clear(&mut self) {
        self.map.clear();
        self.size_bytes = 0;
    }
}

fn entry_len(entry: &Entry) -> usize {
    match entry {
        Entry::Value(v) => v.len(),
        Entry::Tombstone => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn put(t: &mut MemTable, k: &str, v: &str) {
        t.put(k.as_bytes().to_vec(), v.as_bytes().to_vec());
    }

    fn get(t: &MemTable, k: &str) -> Option<String> {
        t.get(k.as_bytes())
            .map(|v| String::from_utf8(v.to_vec()).unwrap())
    }

    #[test]
    fn get_on_empty_table_returns_none() {
        let t = MemTable::new();
        assert_eq!(t.get(b"absent"), None);
        assert!(t.is_empty());
    }

    #[test]
    fn put_then_get_round_trips() {
        let mut t = MemTable::new();
        put(&mut t, "alpha", "one");
        assert_eq!(get(&t, "alpha").as_deref(), Some("one"));
    }

    #[test]
    fn put_overwrites_existing_value() {
        let mut t = MemTable::new();
        put(&mut t, "k", "first");
        put(&mut t, "k", "second");
        assert_eq!(get(&t, "k").as_deref(), Some("second"));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn delete_hides_value_but_leaves_tombstone() {
        let mut t = MemTable::new();
        put(&mut t, "k", "v");
        assert!(t.delete(b"k".to_vec()));
        assert_eq!(t.get(b"k"), None);
        assert_eq!(t.get_entry(b"k"), Some(&Entry::Tombstone));
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn delete_of_absent_key_reports_no_prior_value() {
        let mut t = MemTable::new();
        assert!(!t.delete(b"ghost".to_vec()));
        assert_eq!(t.get_entry(b"ghost"), Some(&Entry::Tombstone));
    }

    #[test]
    fn put_after_delete_resurrects_key() {
        let mut t = MemTable::new();
        put(&mut t, "k", "v1");
        t.delete(b"k".to_vec());
        put(&mut t, "k", "v2");
        assert_eq!(get(&t, "k").as_deref(), Some("v2"));
    }

    #[test]
    fn iteration_is_in_sorted_key_order() {
        let mut t = MemTable::new();
        for k in ["delta", "alpha", "charlie", "bravo"] {
            put(&mut t, k, "x");
        }
        let keys: Vec<_> = t
            .iter_values()
            .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
            .collect();
        assert_eq!(keys, ["alpha", "bravo", "charlie", "delta"]);
    }

    #[test]
    fn iter_values_skips_tombstones() {
        let mut t = MemTable::new();
        put(&mut t, "a", "1");
        put(&mut t, "b", "2");
        t.delete(b"a".to_vec());
        let live: Vec<_> = t.iter_values().map(|(k, _)| k.to_vec()).collect();
        assert_eq!(live, [b"b".to_vec()]);
    }

    #[test]
    fn size_tracking_accounts_for_overwrites_and_deletes() {
        let mut t = MemTable::new();
        put(&mut t, "key", "value"); // 3 + 5
        assert_eq!(t.size_bytes(), 8);
        put(&mut t, "key", "v"); // 3 + 1
        assert_eq!(t.size_bytes(), 4);
        t.delete(b"key".to_vec()); // 3 + 0
        assert_eq!(t.size_bytes(), 3);
    }

    #[test]
    fn binary_keys_and_values_are_supported() {
        let mut t = MemTable::new();
        t.put(vec![0x00, 0xff], vec![0xde, 0xad, 0xbe, 0xef]);
        assert_eq!(t.get(&[0x00, 0xff]), Some(&[0xde, 0xad, 0xbe, 0xef][..]));
    }

    #[test]
    fn clear_empties_the_table() {
        let mut t = MemTable::new();
        put(&mut t, "a", "1");
        t.clear();
        assert!(t.is_empty());
        assert_eq!(t.size_bytes(), 0);
    }
}
