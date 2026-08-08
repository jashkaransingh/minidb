//! Reads must not block behind writes, and must not block writes.
//!
//! # What "non-blocking" means here, precisely
//!
//! A reader clones an immutable view of the levels — one `Arc` bump under a lock
//! held for nanoseconds — and then does all of its work holding nothing: no
//! memtable lock, no table-list lock, no writer mutex. A writer appends to the
//! log (fsync included) and inserts into an insert-only lock-free memtable,
//! taking no lock a reader ever waits on.
//!
//! # Why these tests are shaped the way they are
//!
//! "It didn't crash" is not evidence. Two things are asserted instead:
//!
//! 1. **Reads complete continuously.** The run is divided into time buckets and
//!    every bucket must contain reads. A reader that stalls behind an fsync
//!    leaves an empty bucket.
//! 2. **Reads massively out-rate writes.** Under `SyncPolicy::EveryWrite` a
//!    write costs a disk sync. If reads queued behind writes, the two rates
//!    would be within a small factor of each other. They are not, and the
//!    ratio is asserted with a wide margin so the test is a structural check
//!    rather than a performance measurement.
//!
//! Both are also checked *while flushes and compactions are running*, since
//! those are the operations that used to hold the store's lock the longest.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use minidb::{CompactionConfig, Db, DbOptions, SyncPolicy};

/// How long the concurrent phases run. Long enough for many buckets, short
/// enough to keep `cargo test` quick.
const RUN: Duration = Duration::from_millis(2_000);

/// Width of a "reads must have happened in here" bucket.
const BUCKET: Duration = Duration::from_millis(100);

const BUCKETS: usize = (RUN.as_millis() / BUCKET.as_millis()) as usize;

/// Keys pre-loaded before the concurrent phase, so readers always have hits.
const PRELOADED: u32 = 2_000;

struct TempStore(PathBuf);

impl TempStore {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("minidb-nonblocking-{tag}"));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn key_of(i: u32) -> String {
    format!("key:{i:06}")
}

/// Per-bucket read counters, so a stall shows up as a hole rather than as a
/// slightly lower total.
#[derive(Debug)]
struct Buckets {
    started: Instant,
    counts: Vec<AtomicU64>,
}

impl Buckets {
    fn new() -> Self {
        Self {
            started: Instant::now(),
            counts: (0..BUCKETS + 2).map(|_| AtomicU64::new(0)).collect(),
        }
    }

    fn record(&self) {
        let index = (self.started.elapsed().as_millis() / BUCKET.as_millis()) as usize;
        if let Some(counter) = self.counts.get(index) {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn total(&self) -> u64 {
        self.counts.iter().map(|c| c.load(Ordering::Relaxed)).sum()
    }

    /// Asserts every bucket the run actually spanned saw reads.
    ///
    /// The first and last buckets are excluded: threads are still starting in
    /// one and already joining in the other, so a hole there means nothing.
    fn assert_no_stalls(&self, label: &str) {
        let counts: Vec<u64> = self
            .counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect();
        let empty: Vec<usize> = (1..BUCKETS - 1).filter(|&i| counts[i] == 0).collect();
        assert!(
            empty.is_empty(),
            "{label}: no reads completed during {}ms window(s) {empty:?} — \
             a reader was blocked. Per-bucket counts: {counts:?}",
            BUCKET.as_millis()
        );
    }
}

/// Opens a store with `count` keys already written and flushed to disk.
fn preloaded(store: &TempStore, options: DbOptions, count: u32) -> Db {
    let db = Db::open_with_options(store.path(), options).unwrap();
    for i in 0..count {
        db.put(key_of(i).as_bytes(), b"preloaded").unwrap();
    }
    db.flush().unwrap();
    db
}

#[test]
fn point_reads_keep_completing_while_a_writer_hammers_the_store() {
    let store = TempStore::new("point-reads");
    let options = DbOptions {
        sync_policy: SyncPolicy::EveryWrite,
        flush_threshold_bytes: 64 * 1024,
        ..DbOptions::default()
    };
    let db = preloaded(&store, options, PRELOADED);

    let stop = Arc::new(AtomicBool::new(false));
    let buckets = Arc::new(Buckets::new());
    let writes = Arc::new(AtomicU64::new(0));

    // One writer, going as fast as fsync-per-write allows.
    let writer = {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        let writes = Arc::clone(&writes);
        thread::spawn(move || {
            let mut round = 0u64;
            while !stop.load(Ordering::Relaxed) {
                for i in 0..PRELOADED {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let value = format!("round-{round}");
                    db.put(key_of(i).as_bytes(), value.as_bytes()).unwrap();
                    writes.fetch_add(1, Ordering::Relaxed);
                }
                round += 1;
            }
        })
    };

    // Four readers, each checking that what it reads was really written.
    let readers: Vec<_> = (0..4u32)
        .map(|thread_id| {
            let db = db.clone();
            let stop = Arc::clone(&stop);
            let buckets = Arc::clone(&buckets);
            thread::spawn(move || {
                let mut i = thread_id;
                while !stop.load(Ordering::Relaxed) {
                    i = (i + 7) % PRELOADED;
                    let value = db
                        .get(key_of(i).as_bytes())
                        .unwrap()
                        .expect("a preloaded key must always resolve");
                    let text = String::from_utf8(value).unwrap();
                    assert!(
                        text == "preloaded" || text.starts_with("round-"),
                        "read a value that was never written: {text:?}"
                    );
                    buckets.record();
                }
            })
        })
        .collect();

    thread::sleep(RUN);
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }

    let reads = buckets.total();
    let writes = writes.load(Ordering::Relaxed);

    // Printed so `cargo test -- --nocapture` shows the real ratio rather than
    // only the fact that it cleared the bar.
    println!(
        "point reads: {reads} reads and {writes} fsynced writes in {}ms \
         (ratio {:.1}x)",
        RUN.as_millis(),
        reads as f64 / writes.max(1) as f64
    );

    buckets.assert_no_stalls("point reads under a concurrent writer");
    assert!(
        writes > 100,
        "the writer barely progressed ({writes} writes) — readers may be \
         blocking it, or the test machine is unusable"
    );
    // Every write pays an fsync; every read is memory or one block. If reads
    // were serialized behind writes these would be comparable. The margin is
    // wide on purpose — this asserts a structural property, not a speed.
    assert!(
        reads > writes * 5,
        "reads ({reads}) should vastly out-rate fsynced writes ({writes}); \
         a ratio this low suggests readers are queueing behind the write path"
    );
}

#[test]
fn reads_keep_completing_across_flushes_and_compactions() {
    // Flush and compaction are the operations that used to hold the store's
    // lock longest. A tiny flush threshold makes them fire constantly.
    let store = TempStore::new("flush-compact");
    let options = DbOptions {
        sync_policy: SyncPolicy::EveryWrite,
        flush_threshold_bytes: 8 * 1024,
        compaction: CompactionConfig {
            min_merge_width: 2,
            ..CompactionConfig::default()
        },
        auto_compact: true,
        ..DbOptions::default()
    };
    let db = preloaded(&store, options, PRELOADED);

    let stop = Arc::new(AtomicBool::new(false));
    let buckets = Arc::new(Buckets::new());
    let flushes = Arc::new(AtomicU64::new(0));

    let writer = {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        let flushes = Arc::clone(&flushes);
        thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                // Values large enough to cross the flush threshold often, and
                // an explicit flush every 50 writes so the slow path is
                // exercised many times within the run rather than once.
                let value = format!("round-{n}{}", "-pad".repeat(16));
                db.put(
                    key_of((n % PRELOADED as u64) as u32).as_bytes(),
                    value.as_bytes(),
                )
                .unwrap();
                n += 1;
                if n.is_multiple_of(50) {
                    db.flush().unwrap();
                    flushes.fetch_add(1, Ordering::Relaxed);
                }
            }
        })
    };

    let readers: Vec<_> = (0..4u32)
        .map(|thread_id| {
            let db = db.clone();
            let stop = Arc::clone(&stop);
            let buckets = Arc::clone(&buckets);
            thread::spawn(move || {
                let mut i = thread_id;
                while !stop.load(Ordering::Relaxed) {
                    i = (i + 13) % PRELOADED;
                    assert!(
                        db.get(key_of(i).as_bytes()).unwrap().is_some(),
                        "key:{i:06} vanished during a flush or compaction"
                    );
                    buckets.record();
                }
            })
        })
        .collect();

    thread::sleep(RUN);
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }

    println!(
        "under flush/compaction: {} reads across {} flush cycles, {} tables left",
        buckets.total(),
        flushes.load(Ordering::Relaxed),
        db.sstable_count()
    );

    buckets.assert_no_stalls("point reads across flushes and compactions");
    assert!(
        flushes.load(Ordering::Relaxed) > 3,
        "only {} flushes ran; this test is meant to cover the slow path \
         many times over",
        flushes.load(Ordering::Relaxed)
    );
    assert!(
        buckets.total() > 1_000,
        "only {} reads completed; readers were not running freely",
        buckets.total()
    );
}

#[test]
fn full_scans_keep_completing_while_a_writer_hammers_the_store() {
    // A scan is the longest-running read there is, so it is where a
    // reader-blocks-writer design shows up worst — and where a
    // writer-blocks-reader design stalls the most obviously.
    let store = TempStore::new("scans");
    let options = DbOptions {
        sync_policy: SyncPolicy::EveryWrite,
        flush_threshold_bytes: 64 * 1024,
        ..DbOptions::default()
    };
    let db = preloaded(&store, options, 500);

    let stop = Arc::new(AtomicBool::new(false));
    let buckets = Arc::new(Buckets::new());
    let writes = Arc::new(AtomicU64::new(0));

    let writer = {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        let writes = Arc::clone(&writes);
        thread::spawn(move || {
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                db.put(key_of((n % 500) as u32).as_bytes(), b"rewritten")
                    .unwrap();
                writes.fetch_add(1, Ordering::Relaxed);
                n += 1;
            }
        })
    };

    let readers: Vec<_> = (0..2)
        .map(|_| {
            let db = db.clone();
            let stop = Arc::clone(&stop);
            let buckets = Arc::clone(&buckets);
            thread::spawn(move || {
                while !stop.load(Ordering::Relaxed) {
                    let live = db.scan().unwrap();
                    assert_eq!(
                        live.len(),
                        500,
                        "a scan saw a partial dataset while writes were landing"
                    );
                    buckets.record();
                }
            })
        })
        .collect();

    thread::sleep(RUN);
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for reader in readers {
        reader.join().unwrap();
    }

    println!(
        "full scans: {} scans and {} fsynced writes in {}ms",
        buckets.total(),
        writes.load(Ordering::Relaxed),
        RUN.as_millis()
    );

    buckets.assert_no_stalls("full scans under a concurrent writer");
    assert!(
        writes.load(Ordering::Relaxed) > 100,
        "long-running scans blocked the writer: only {} writes landed",
        writes.load(Ordering::Relaxed)
    );
}

#[test]
fn a_reader_never_observes_a_value_that_was_never_written() {
    // Correctness, not liveness: with many writers cycling a small key space
    // through a known set of values, every read must land on one of them.
    let store = TempStore::new("no-torn-reads");
    let options = DbOptions {
        sync_policy: SyncPolicy::EveryWrite,
        flush_threshold_bytes: 4 * 1024,
        ..DbOptions::default()
    };
    let db = preloaded(&store, options, 50);

    let known: Vec<String> = (0..64).map(|v| format!("value-{v:03}")).collect();
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));

    let writers: Vec<_> = (0..3u64)
        .map(|w| {
            let db = db.clone();
            let stop = Arc::clone(&stop);
            let known = known.clone();
            thread::spawn(move || {
                let mut n = w;
                while !stop.load(Ordering::Relaxed) {
                    let key = key_of((n % 50) as u32);
                    let value = &known[(n % known.len() as u64) as usize];
                    db.put(key.as_bytes(), value.as_bytes()).unwrap();
                    n += 1;
                }
            })
        })
        .collect();

    let readers: Vec<_> = (0..4u32)
        .map(|thread_id| {
            let db = db.clone();
            let stop = Arc::clone(&stop);
            let reads = Arc::clone(&reads);
            let known = known.clone();
            thread::spawn(move || {
                let mut i = thread_id;
                while !stop.load(Ordering::Relaxed) {
                    i = (i + 1) % 50;
                    let got = db.get(key_of(i).as_bytes()).unwrap().expect("key vanished");
                    let text = String::from_utf8(got).unwrap();
                    assert!(
                        text == "preloaded" || known.contains(&text),
                        "read {text:?}, which no writer ever wrote"
                    );
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(1_000));
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.join().unwrap();
    }
    for r in readers {
        r.join().unwrap();
    }

    assert!(
        reads.load(Ordering::Relaxed) > 1_000,
        "too few reads ({}) for this to mean anything",
        reads.load(Ordering::Relaxed)
    );
}

#[test]
fn a_snapshot_stays_readable_and_stable_through_flushes_and_compactions() {
    // Snapshot isolation and non-blocking reads have to hold *together*: an
    // open snapshot must keep resolving correctly while the levels it reads
    // through are being rewritten underneath it.
    let store = TempStore::new("snapshot-under-load");
    let options = DbOptions {
        sync_policy: SyncPolicy::EveryWrite,
        flush_threshold_bytes: 8 * 1024,
        compaction: CompactionConfig {
            min_merge_width: 2,
            ..CompactionConfig::default()
        },
        auto_compact: true,
        ..DbOptions::default()
    };
    let db = preloaded(&store, options, 500);

    let snapshot = Arc::new(db.snapshot());
    let stop = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicU64::new(0));

    let writer = {
        let db = db.clone();
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            let mut round = 0u64;
            while !stop.load(Ordering::Relaxed) {
                for i in 0..500u32 {
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    let value = format!("round-{round}{}", "-pad".repeat(8));
                    db.put(key_of(i).as_bytes(), value.as_bytes()).unwrap();
                }
                round += 1;
            }
        })
    };

    let readers: Vec<_> = (0..3u32)
        .map(|thread_id| {
            let db = db.clone();
            let stop = Arc::clone(&stop);
            let reads = Arc::clone(&reads);
            let snapshot = Arc::clone(&snapshot);
            thread::spawn(move || {
                let mut i = thread_id;
                while !stop.load(Ordering::Relaxed) {
                    i = (i + 11) % 500;
                    let got = db.get_at(&snapshot, key_of(i).as_bytes()).unwrap();
                    assert_eq!(
                        got.as_deref(),
                        Some(&b"preloaded"[..]),
                        "the snapshot moved while the store was being rewritten"
                    );
                    reads.fetch_add(1, Ordering::Relaxed);
                }
            })
        })
        .collect();

    thread::sleep(Duration::from_millis(1_500));
    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }

    assert!(
        reads.load(Ordering::Relaxed) > 1_000,
        "too few snapshot reads ({}) for this to mean anything",
        reads.load(Ordering::Relaxed)
    );
    assert!(db.get(b"key:000000").unwrap().is_some());
}
