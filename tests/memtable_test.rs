//! Integration tests driving the memtable through the public API.
//!
//! These use `minidb` exactly as an external crate would, so they cover the
//! re-exports and the `Db` wrapper as well as the memtable itself.

use minidb::{Db, Entry, MemTable};

#[test]
fn writes_are_visible_to_subsequent_reads() {
    let mut db = Db::new();
    db.put(b"user:1", b"ada");
    db.put(b"user:2", b"grace");

    assert_eq!(db.get(b"user:1"), Some(b"ada".to_vec()));
    assert_eq!(db.get(b"user:2"), Some(b"grace".to_vec()));
    assert_eq!(db.get(b"user:3"), None);
    assert_eq!(db.len(), 2);
}

#[test]
fn last_write_wins_for_a_repeated_key() {
    let mut db = Db::new();
    for value in ["v1", "v2", "v3"] {
        db.put(b"key", value.as_bytes());
    }
    assert_eq!(db.get(b"key"), Some(b"v3".to_vec()));
    assert_eq!(db.len(), 1);
}

#[test]
fn delete_removes_a_key_from_reads() {
    let mut db = Db::new();
    db.put(b"doomed", b"value");

    assert!(db.delete(b"doomed"));
    assert_eq!(db.get(b"doomed"), None);
    assert!(!db.contains(b"doomed"));
    assert!(db.is_empty());
}

#[test]
fn delete_of_an_absent_key_reports_false() {
    let mut db = Db::new();
    assert!(!db.delete(b"never-existed"));
    assert!(db.is_empty());
}

#[test]
fn a_key_can_be_rewritten_after_deletion() {
    let mut db = Db::new();
    db.put(b"phoenix", b"first");
    db.delete(b"phoenix");
    db.put(b"phoenix", b"second");

    assert_eq!(db.get(b"phoenix"), Some(b"second".to_vec()));
    assert_eq!(db.len(), 1);
}

#[test]
fn deletes_leave_tombstones_rather_than_erasing_keys() {
    // Tombstones matter because older SSTables may still hold a value for the
    // key; the delete has to shadow them until compaction drops both.
    let mut table = MemTable::new();
    table.put(b"k".to_vec(), b"v".to_vec());
    table.delete(b"k".to_vec());

    assert_eq!(table.get(b"k"), None);
    assert_eq!(table.get_entry(b"k"), Some(&Entry::Tombstone));
    assert_eq!(table.len(), 1, "the tombstone still occupies a slot");
}

#[test]
fn entries_iterate_in_sorted_key_order() {
    let mut db = Db::new();
    for key in ["zeta", "alpha", "mike", "bravo"] {
        db.put(key.as_bytes(), b"x");
    }

    let keys: Vec<String> = db
        .memtable()
        .iter_values()
        .map(|(k, _)| String::from_utf8(k.to_vec()).unwrap())
        .collect();

    assert_eq!(keys, ["alpha", "bravo", "mike", "zeta"]);
}

#[test]
fn arbitrary_binary_keys_and_values_round_trip() {
    let mut db = Db::new();
    let key = vec![0x00, 0x01, 0xfe, 0xff];
    let value = vec![0xde, 0xad, 0xbe, 0xef, 0x00];

    db.put(&key, &value);
    assert_eq!(db.get(&key), Some(value));
}

#[test]
fn many_keys_are_all_retrievable() {
    let mut db = Db::new();
    for i in 0..1_000u32 {
        db.put(format!("key:{i:04}").as_bytes(), i.to_be_bytes().as_slice());
    }

    assert_eq!(db.len(), 1_000);
    assert_eq!(db.get(b"key:0000"), Some(0u32.to_be_bytes().to_vec()));
    assert_eq!(db.get(b"key:0999"), Some(999u32.to_be_bytes().to_vec()));
    assert_eq!(db.get(b"key:1000"), None);
}

#[test]
fn buffered_size_grows_with_writes_and_shrinks_on_delete() {
    let mut db = Db::new();
    assert_eq!(db.size_bytes(), 0);

    db.put(b"abc", b"12345"); // 3 + 5
    assert_eq!(db.size_bytes(), 8);

    db.delete(b"abc"); // key retained, value dropped
    assert_eq!(db.size_bytes(), 3);
}
