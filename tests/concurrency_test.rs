//! Multi-threaded tests for `SharedDb`.
//!
//! Concurrency bugs do not reproduce reliably, so these tests aim for volume and
//! for assertions that are *specific*: not just "nothing panicked", but "every
//! value a reader observed was one that was actually written".
//!
//! A torn read — a value that is a mixture of two writes, or bytes that were
//! never written at all — is the failure these are built to catch.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;

use minidb::{CompactionConfig, DbOptions, FaultPlan, SharedDb, SyncPolicy};

struct TempStore(PathBuf);

impl TempStore {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("minidb-concurrency-{tag}"));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn open(&self, flush_threshold_bytes: usize) -> SharedDb {
        SharedDb::open_with_options(
            &self.0,
            DbOptions {
                sync_policy: SyncPolicy::OsBuffered, // keep these tests fast
                flush_threshold_bytes,
                compaction: CompactionConfig::default(),
                auto_compact: true,
                fault: FaultPlan::none(),
            },
        )
        .unwrap()
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn many_readers_run_against_a_stable_store() {
    let store = TempStore::new("many-readers");
    let db = store.open(4_096);

    for i in 0..500u32 {
        db.put(format!("key:{i:04}").as_bytes(), format!("v{i}").as_bytes())
            .unwrap();
    }

    let mut handles = Vec::new();
    for _ in 0..8 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for round in 0..200u32 {
                let i = round % 500;
                let got = db.get(format!("key:{i:04}").as_bytes()).unwrap();
                assert_eq!(got, Some(format!("v{i}").into_bytes()));
            }
        }));
    }
    for handle in handles {
        handle.join().expect("reader thread panicked");
    }
}

#[test]
fn readers_never_observe_a_value_that_was_never_written() {
    // One writer cycling a small key space through known values, many readers
    // checking that whatever they see is one of those values — never a torn or
    // partial one.
    let store = TempStore::new("no-torn-reads");
    let db = store.open(2_048);

    const KEYS: u32 = 20;
    const VALUES: u32 = 50;

    // Seed every key so readers always find something.
    for k in 0..KEYS {
        db.put(format!("k{k:02}").as_bytes(), b"v000").unwrap();
    }

    let valid: HashSet<Vec<u8>> = (0..VALUES)
        .map(|v| format!("v{v:03}").into_bytes())
        .collect();
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));

    let mut readers = Vec::new();
    for _ in 0..6 {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        let reads = Arc::clone(&reads);
        let valid = valid.clone();
        readers.push(thread::spawn(move || {
            let mut seen = 0usize;
            while !stop.load(Ordering::Relaxed) {
                for k in 0..KEYS {
                    let got = db
                        .get(format!("k{k:02}").as_bytes())
                        .expect("read failed")
                        .expect("seeded key must always be present");
                    assert!(
                        valid.contains(&got),
                        "observed a value that was never written: {:?}",
                        String::from_utf8_lossy(&got)
                    );
                    seen += 1;
                }
            }
            reads.fetch_add(seen, Ordering::Relaxed);
        }));
    }

    let writer = {
        let db = db.clone();
        thread::spawn(move || {
            for v in 1..VALUES {
                for k in 0..KEYS {
                    db.put(format!("k{k:02}").as_bytes(), format!("v{v:03}").as_bytes())
                        .expect("write failed");
                }
            }
        })
    };

    writer.join().expect("writer thread panicked");
    stop.store(true, Ordering::Relaxed);
    for reader in readers {
        reader.join().expect("reader thread panicked");
    }

    assert!(
        reads.load(Ordering::Relaxed) > 1_000,
        "readers barely ran; the test proved little"
    );

    // Final state is the last value the writer wrote.
    for k in 0..KEYS {
        assert_eq!(
            db.get(format!("k{k:02}").as_bytes()).unwrap(),
            Some(format!("v{:03}", VALUES - 1).into_bytes())
        );
    }
}

#[test]
fn concurrent_writers_to_disjoint_keys_all_land() {
    let store = TempStore::new("disjoint-writers");
    let db = store.open(2_048);

    const THREADS: u32 = 6;
    const PER_THREAD: u32 = 150;

    let mut handles = Vec::new();
    for t in 0..THREADS {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..PER_THREAD {
                db.put(
                    format!("t{t}:k{i:04}").as_bytes(),
                    format!("t{t}-v{i}").as_bytes(),
                )
                .expect("write failed");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("writer thread panicked");
    }

    for t in 0..THREADS {
        for i in 0..PER_THREAD {
            assert_eq!(
                db.get(format!("t{t}:k{i:04}").as_bytes()).unwrap(),
                Some(format!("t{t}-v{i}").into_bytes()),
                "t{t}:k{i:04} was lost"
            );
        }
    }
    assert_eq!(db.len().unwrap(), (THREADS * PER_THREAD) as usize);
}

#[test]
fn concurrent_writers_to_the_same_key_leave_one_of_their_values() {
    // Last-write-wins is not deterministic under contention, but the surviving
    // value must be exactly one of the values actually written — not a blend.
    let store = TempStore::new("same-key");
    let db = store.open(1_024);

    const THREADS: u32 = 8;
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for _ in 0..100 {
                db.put(b"contended", format!("writer-{t}").as_bytes())
                    .expect("write failed");
            }
        }));
    }
    for handle in handles {
        handle.join().expect("writer thread panicked");
    }

    let final_value = db.get(b"contended").unwrap().expect("key must exist");
    let expected: HashSet<Vec<u8>> = (0..THREADS)
        .map(|t| format!("writer-{t}").into_bytes())
        .collect();
    assert!(
        expected.contains(&final_value),
        "final value {:?} was never written by any thread",
        String::from_utf8_lossy(&final_value)
    );
}

#[test]
fn reads_and_writes_interleave_safely_with_flushes_and_compaction() {
    // The stress case: writers pushing the memtable past its threshold (so
    // flushes and compactions fire mid-run) while readers hammer the store.
    let store = TempStore::new("stress");
    let db = store.open(512); // tiny, so flushes happen constantly

    let stop = Arc::new(AtomicBool::new(false));
    let mut handles = Vec::new();

    for t in 0..3u32 {
        let db = db.clone();
        handles.push(thread::spawn(move || {
            for i in 0..400u32 {
                let key = format!("w{t}:{i:04}");
                db.put(key.as_bytes(), format!("value-{t}-{i}").as_bytes())
                    .expect("write failed");
                if i % 7 == 0 {
                    db.delete(format!("w{t}:{:04}", i.saturating_sub(3)).as_bytes())
                        .expect("delete failed");
                }
            }
        }));
    }

    for _ in 0..4 {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        handles.push(thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                // Reads must never error, whatever flush or compaction is running.
                let _ = db.get(b"w0:0100").expect("read failed");
                let _ = db.get(b"absent-key").expect("read failed");
                let _ = db.sstable_count().expect("stat failed");
            }
        }));
    }

    // Join writers first, then release the readers.
    for handle in handles.drain(..3) {
        handle.join().expect("writer thread panicked");
    }
    stop.store(true, Ordering::Relaxed);
    for handle in handles {
        handle.join().expect("reader thread panicked");
    }

    // Verify the full expected final state. Each writer touched a disjoint key
    // range, so replaying one writer's op sequence gives the exact expectation —
    // safer than re-deriving which indices the delete stride hit.
    for t in 0..3u32 {
        let mut expected: BTreeMap<String, Option<String>> = BTreeMap::new();
        for i in 0..400u32 {
            expected.insert(format!("w{t}:{i:04}"), Some(format!("value-{t}-{i}")));
            if i % 7 == 0 {
                expected.insert(format!("w{t}:{:04}", i.saturating_sub(3)), None);
            }
        }

        for (key, want) in expected {
            let got = db.get(key.as_bytes()).unwrap();
            match want {
                Some(value) => assert_eq!(got, Some(value.into_bytes()), "{key} is wrong"),
                None => assert_eq!(got, None, "{key} should have been deleted"),
            }
        }
    }
}

#[test]
fn a_shared_store_recovers_correctly_after_all_handles_are_dropped() {
    let store = TempStore::new("durable");
    {
        let db = store.open(1_024);
        let mut handles = Vec::new();
        for t in 0..4u32 {
            let db = db.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100u32 {
                    db.put(format!("t{t}:{i:03}").as_bytes(), b"v")
                        .expect("write failed");
                }
            }));
        }
        for handle in handles {
            handle.join().unwrap();
        }
        db.sync().unwrap();
    }

    let db = store.open(1_024);
    assert_eq!(db.len().unwrap(), 400);
    for t in 0..4u32 {
        for i in 0..100u32 {
            assert_eq!(
                db.get(format!("t{t}:{i:03}").as_bytes()).unwrap(),
                Some(b"v".to_vec())
            );
        }
    }
}

#[test]
fn a_panic_in_one_thread_poisons_the_lock_without_crashing_the_others() {
    // std's RwLock poisons on panic. The wrapper must surface that as an error
    // rather than letting every other thread panic in turn.
    let db = SharedDb::new();
    db.put(b"k", b"v").unwrap();

    let poisoner = {
        let db = db.clone();
        thread::spawn(move || {
            let _guard = db.write().unwrap();
            panic!("deliberate panic while holding the write lock");
        })
    };
    assert!(poisoner.join().is_err(), "the thread should have panicked");

    // Other threads get a clean error, not a panic.
    let err = db.get(b"k").expect_err("the lock should be poisoned");
    assert!(
        err.to_string().contains("poisoned"),
        "unexpected error: {err}"
    );
    assert!(db.put(b"k2", b"v").is_err());
}

#[test]
fn the_number_of_handles_is_reported_accurately() {
    let db = SharedDb::new();
    assert_eq!(db.handle_count(), 1);
    {
        let _a = db.clone();
        let _b = db.clone();
        assert_eq!(db.handle_count(), 3);
    }
    assert_eq!(db.handle_count(), 1);
}
