//! Integration tests driving the memtable through the public API.
//!
//! These use `minidb` exactly as an external crate would, so they cover the
//! re-exports and the `Db` wrapper as well as the memtable itself.

use minidb::{Db, Entry, MemTable};

#[test]
fn writes_are_visible_to_subsequent_reads() {
    let db = Db::new();
    db.put(b"user:1", b"ada").unwrap();
    db.put(b"user:2", b"grace").unwrap();

    assert_eq!(db.get(b"user:1").unwrap(), Some(b"ada".to_vec()));
    assert_eq!(db.get(b"user:2").unwrap(), Some(b"grace".to_vec()));
    assert_eq!(db.get(b"user:3").unwrap(), None);
    assert_eq!(db.len().unwrap(), 2);
}

#[test]
fn last_write_wins_for_a_repeated_key() {
    let db = Db::new();
    for value in ["v1", "v2", "v3"] {
        db.put(b"key", value.as_bytes()).unwrap();
    }
    assert_eq!(db.get(b"key").unwrap(), Some(b"v3".to_vec()));
    assert_eq!(db.len().unwrap(), 1);
}

#[test]
fn delete_removes_a_key_from_reads() {
    let db = Db::new();
    db.put(b"doomed", b"value").unwrap();

    assert!(db.delete(b"doomed").unwrap());
    assert_eq!(db.get(b"doomed").unwrap(), None);
    assert!(!db.contains(b"doomed").unwrap());
    assert!(db.is_empty().unwrap());
}

#[test]
fn delete_of_an_absent_key_reports_false() {
    let db = Db::new();
    assert!(!db.delete(b"never-existed").unwrap());
    assert!(db.is_empty().unwrap());
}

#[test]
fn a_key_can_be_rewritten_after_deletion() {
    let db = Db::new();
    db.put(b"phoenix", b"first").unwrap();
    db.delete(b"phoenix").unwrap();
    db.put(b"phoenix", b"second").unwrap();

    assert_eq!(db.get(b"phoenix").unwrap(), Some(b"second".to_vec()));
    assert_eq!(db.len().unwrap(), 1);
}

#[test]
fn deletes_leave_tombstones_rather_than_erasing_keys() {
    // Tombstones matter because older SSTables may still hold a value for the
    // key; the delete has to shadow them until compaction drops both.
    let mut table = MemTable::new();
    table.put(b"k", 1, b"v".to_vec());
    table.delete(b"k", 2);

    assert_eq!(table.get(b"k", u64::MAX), Some(&Entry::Tombstone));
    assert_eq!(
        table.len(),
        2,
        "the tombstone is a version in its own right, above the value it hides"
    );
}

#[test]
fn entries_iterate_in_sorted_key_order() {
    let db = Db::new();
    for key in ["zeta", "alpha", "mike", "bravo"] {
        db.put(key.as_bytes(), b"x").unwrap();
    }

    let keys: Vec<String> = db
        .scan()
        .unwrap()
        .into_keys()
        .map(|k| String::from_utf8(k).unwrap())
        .collect();

    assert_eq!(keys, ["alpha", "bravo", "mike", "zeta"]);
}

#[test]
fn arbitrary_binary_keys_and_values_round_trip() {
    let db = Db::new();
    let key = vec![0x00, 0x01, 0xfe, 0xff];
    let value = vec![0xde, 0xad, 0xbe, 0xef, 0x00];

    db.put(&key, &value).unwrap();
    assert_eq!(db.get(&key).unwrap(), Some(value));
}

#[test]
fn many_keys_are_all_retrievable() {
    let db = Db::new();
    for i in 0..1_000u32 {
        db.put(format!("key:{i:04}").as_bytes(), i.to_be_bytes().as_slice())
            .unwrap();
    }

    assert_eq!(db.len().unwrap(), 1_000);
    assert_eq!(
        db.get(b"key:0000").unwrap(),
        Some(0u32.to_be_bytes().to_vec())
    );
    assert_eq!(
        db.get(b"key:0999").unwrap(),
        Some(999u32.to_be_bytes().to_vec())
    );
    assert_eq!(db.get(b"key:1000").unwrap(), None);
}

#[test]
fn buffered_size_grows_with_every_version_written() {
    // Under MVCC the buffer only ever grows: an overwrite or a delete *adds* a
    // version rather than replacing one, because older snapshots may still need
    // to read what came before. Each version costs key + 8-byte sequence number
    // + payload.
    let db = Db::new();
    assert_eq!(db.size_bytes(), 0);

    db.put(b"abc", b"12345").unwrap(); // 3 + 8 + 5
    assert_eq!(db.size_bytes(), 16);

    db.put(b"abc", b"9").unwrap(); // 3 + 8 + 1
    assert_eq!(db.size_bytes(), 28);

    db.delete(b"abc").unwrap(); // 3 + 8 + 0
    assert_eq!(db.size_bytes(), 39);
}
