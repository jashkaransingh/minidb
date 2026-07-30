//! Randomized in-process crash testing.
//!
//! # The property under test
//!
//! **If `put` or `delete` returned `Ok`, that mutation survives a crash.**
//!
//! Each run builds a model of every *acknowledged* mutation, crashes the store
//! at a pseudo-random byte offset partway through a record, reopens it, and
//! checks the recovered store against the model. A mutation whose call returned
//! `Err` is the one interrupted by the crash; it is allowed to be present or
//! absent, and the harness asserts nothing about it either way.
//!
//! # Why in-process
//!
//! No forking, no signals. The fault is injected inside the write path (see
//! `minidb::fault`), which makes every run reproducible from its seed: a failure
//! at seed 73 can be replayed at seed 73 under a debugger. A `kill -9` harness
//! crashes wherever the signal lands and cannot do that.
//!
//! # Determinism
//!
//! Workloads come from a seeded xorshift64* generator written out here rather
//! than pulled from a crate, so the sequence is fixed by this file and cannot
//! shift under a dependency update — which would quietly change what is being
//! tested.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use minidb::fault::is_simulated_crash;
use minidb::{CompactionConfig, Db, DbOptions, FaultPlan, SyncPolicy};

/// Seeded xorshift64* — small, deterministic, and good enough to pick offsets.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Any non-zero state works; xorshift is degenerate at zero.
        Self(seed.wrapping_mul(2_685_821_657_736_338_717).max(1))
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(2_685_821_657_736_338_717)
    }

    fn below(&mut self, bound: u64) -> u64 {
        if bound == 0 {
            0
        } else {
            self.next_u64() % bound
        }
    }
}

/// One mutation in a generated workload.
#[derive(Debug, Clone)]
enum Op {
    Put(String, String),
    Delete(String),
    Flush,
}

/// Builds a workload mixing writes, overwrites, deletes, and flushes.
fn workload(rng: &mut Rng, len: usize) -> Vec<Op> {
    (0..len)
        .map(|_| match rng.below(100) {
            // Overwrites and deletes concentrate on a small key space so that
            // shadowing, tombstones, and compaction all get exercised.
            0..=59 => {
                let k = rng.below(40);
                let v = rng.below(1_000_000);
                Op::Put(format!("key:{k:03}"), format!("value:{v}"))
            }
            60..=84 => Op::Delete(format!("key:{:03}", rng.below(40))),
            85..=94 => {
                let k = rng.below(500);
                Op::Put(
                    format!("cold:{k:03}"),
                    "x".repeat(1 + rng.below(200) as usize),
                )
            }
            _ => Op::Flush,
        })
        .collect()
}

struct TempStore(PathBuf);

impl TempStore {
    fn new(tag: &str) -> Self {
        let path = std::env::temp_dir().join(format!("minidb-crash-{tag}"));
        let _ = fs::remove_dir_all(&path);
        Self(path)
    }
}

impl Drop for TempStore {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn options(fault: FaultPlan) -> DbOptions {
    DbOptions {
        sync_policy: SyncPolicy::EveryWrite,
        // Small enough that a run produces several tables and some compaction.
        flush_threshold_bytes: 2_048,
        compaction: CompactionConfig::default(),
        auto_compact: true,
        fault,
    }
}

/// Result of driving a workload until the injected crash fires.
struct CrashRun {
    /// Live key/value pairs implied by the acknowledged mutations.
    acknowledged: HashMap<String, String>,
    /// Keys whose acknowledged final state is "deleted".
    deleted: Vec<String>,
    /// Whether the injected fault actually fired this run.
    crashed: bool,
    /// How many mutations were acknowledged before the crash.
    applied: usize,
}

/// Applies `ops` to a store that will crash at `crash_at` bytes.
///
/// Stops at the first error, which the fault plan guarantees is the simulated
/// crash. The returned model reflects only mutations whose call returned `Ok`.
fn drive_until_crash(dir: &PathBuf, ops: &[Op], crash_at: u64) -> CrashRun {
    let mut db = Db::open_with_options(dir, options(FaultPlan::crash_after_wal_bytes(crash_at)))
        .expect("open should succeed");

    let mut model: HashMap<String, String> = HashMap::new();
    let mut deleted: Vec<String> = Vec::new();
    let mut crashed = false;
    let mut applied = 0usize;

    for op in ops {
        let outcome = match op {
            Op::Put(k, v) => db.put(k.as_bytes(), v.as_bytes()),
            Op::Delete(k) => db.delete(k.as_bytes()).map(|_| ()),
            Op::Flush => db.flush().map(|_| ()),
        };

        match outcome {
            Ok(()) => {
                // Only now is the mutation acknowledged, and only now does the
                // durability claim attach to it.
                match op {
                    Op::Put(k, v) => {
                        model.insert(k.clone(), v.clone());
                        deleted.retain(|d| d != k);
                    }
                    Op::Delete(k) => {
                        if model.remove(k).is_some() || !deleted.contains(k) {
                            deleted.push(k.clone());
                        }
                    }
                    Op::Flush => {}
                }
                applied += 1;
            }
            Err(e) => {
                assert!(
                    is_simulated_crash(&e),
                    "expected the injected fault, got a real I/O error: {e}"
                );
                crashed = true;
                break;
            }
        }
    }

    // Drop without any clean shutdown — this is the "process died" moment.
    drop(db);

    CrashRun {
        acknowledged: model,
        deleted,
        crashed,
        applied,
    }
}

/// Reopens the store and checks it against the model.
fn verify_recovery(dir: &PathBuf, run: &CrashRun, seed: u64) {
    let db = Db::open(dir).unwrap_or_else(|e| panic!("seed {seed}: reopen failed: {e}"));

    for (key, value) in &run.acknowledged {
        let got = db
            .get(key.as_bytes())
            .unwrap_or_else(|e| panic!("seed {seed}: read of {key} failed: {e}"));
        assert_eq!(
            got.as_deref(),
            Some(value.as_bytes()),
            "seed {seed}: acknowledged write to {key} was lost across the crash"
        );
    }

    for key in &run.deleted {
        let got = db
            .get(key.as_bytes())
            .unwrap_or_else(|e| panic!("seed {seed}: read of {key} failed: {e}"));
        assert_eq!(
            got, None,
            "seed {seed}: {key} was acknowledged as deleted but came back"
        );
    }
}

#[test]
fn acknowledged_writes_survive_150_randomized_crashes() {
    const RUNS: u64 = 150;

    let mut crashes = 0;
    let mut total_applied = 0usize;

    for seed in 0..RUNS {
        let store = TempStore::new(&format!("randomized-{seed}"));
        let mut rng = Rng::new(seed);
        let ops = workload(&mut rng, 200);

        // Crash somewhere inside the bytes this workload will write. The range
        // spans early crashes (before anything is durable) and late ones.
        let crash_at = 1 + rng.below(6_000);

        let run = drive_until_crash(&store.0, &ops, crash_at);
        verify_recovery(&store.0, &run, seed);

        if run.crashed {
            crashes += 1;
        }
        total_applied += run.applied;
    }

    // The harness is worthless if the fault never actually fires.
    assert!(
        crashes as f64 / RUNS as f64 > 0.8,
        "the injected crash only fired in {crashes}/{RUNS} runs; the harness is not testing much"
    );
    assert!(
        total_applied > 1_000,
        "only {total_applied} mutations were acknowledged across all runs"
    );
}

#[test]
fn the_store_stays_usable_after_recovering_from_a_crash() {
    const RUNS: u64 = 30;

    for seed in 0..RUNS {
        let store = TempStore::new(&format!("usable-{seed}"));
        let mut rng = Rng::new(seed ^ 0xabcd);
        let ops = workload(&mut rng, 120);
        let crash_at = 1 + rng.below(4_000);

        let run = drive_until_crash(&store.0, &ops, crash_at);

        // Reopen, keep writing, reopen again: recovery must leave a store that
        // works, not merely one that reads.
        {
            let mut db = Db::open(&store.0).unwrap();
            for i in 0..20u32 {
                db.put(format!("post:{i:02}").as_bytes(), b"after-crash")
                    .unwrap_or_else(|e| panic!("seed {seed}: write after recovery failed: {e}"));
            }
            db.flush().unwrap();
        }

        let db = Db::open(&store.0).unwrap();
        for i in 0..20u32 {
            assert_eq!(
                db.get(format!("post:{i:02}").as_bytes()).unwrap(),
                Some(b"after-crash".to_vec()),
                "seed {seed}: post-recovery write was lost"
            );
        }
        // The pre-crash acknowledged state must still be intact too.
        verify_recovery(&store.0, &run, seed);
    }
}

#[test]
fn a_crash_partway_through_a_record_never_applies_that_record() {
    // The torn-tail case in isolation: a half-written record must not be
    // decoded into a mutation.
    let store = TempStore::new("torn-record");

    let mut db =
        Db::open_with_options(&store.0, options(FaultPlan::crash_after_wal_bytes(40))).unwrap();

    db.put(b"first", b"durable").unwrap();
    let err = loop {
        match db.put(b"doomed", b"this record will be cut in half") {
            Ok(()) => continue,
            Err(e) => break e,
        }
    };
    assert!(is_simulated_crash(&err));
    drop(db);

    let db = Db::open(&store.0).unwrap();
    assert_eq!(
        db.get(b"first").unwrap(),
        Some(b"durable".to_vec()),
        "the acknowledged write must survive"
    );
    assert_eq!(
        db.get(b"doomed").unwrap(),
        None,
        "a torn record must not be applied"
    );
}

#[test]
fn every_operation_fails_after_the_crash_fires() {
    // Once the fault fires the process is notionally dead; nothing should
    // silently keep working and appear durable.
    let store = TempStore::new("dead-after");

    let mut db =
        Db::open_with_options(&store.0, options(FaultPlan::crash_after_wal_bytes(30))).unwrap();

    let mut fired = false;
    for i in 0..50u32 {
        if db.put(format!("k{i}").as_bytes(), b"v").is_err() {
            fired = true;
            break;
        }
    }
    assert!(fired, "the fault should have fired within 50 writes");

    for i in 0..10u32 {
        assert!(
            db.put(format!("after{i}").as_bytes(), b"v").is_err(),
            "writes after the crash must keep failing"
        );
    }
    assert!(db.sync().is_err(), "sync after the crash must fail");
}

#[test]
fn an_unarmed_fault_plan_changes_nothing() {
    let store = TempStore::new("unarmed");

    let mut db = Db::open_with_options(&store.0, options(FaultPlan::none())).unwrap();
    for i in 0..200u32 {
        db.put(format!("k{i:03}").as_bytes(), b"value").unwrap();
    }
    drop(db);

    let db = Db::open(&store.0).unwrap();
    assert_eq!(db.len().unwrap(), 200);
}

#[test]
fn crash_runs_are_reproducible_from_their_seed() {
    // Determinism is what makes a failing run debuggable, so it is worth an
    // explicit test rather than an assumption.
    let ops_a = workload(&mut Rng::new(1234), 60);
    let ops_b = workload(&mut Rng::new(1234), 60);

    let render = |ops: &[Op]| {
        ops.iter()
            .map(|op| match op {
                Op::Put(k, v) => format!("P{k}={v}"),
                Op::Delete(k) => format!("D{k}"),
                Op::Flush => "F".to_string(),
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(render(&ops_a), render(&ops_b));

    // And the same seed must produce the same recovered state.
    let recovered = |tag: &str| {
        let store = TempStore::new(tag);
        let mut rng = Rng::new(99);
        let ops = workload(&mut rng, 150);
        let crash_at = 1 + rng.below(5_000);
        let run = drive_until_crash(&store.0, &ops, crash_at);
        let db = Db::open(&store.0).unwrap();
        let scan = db.scan().unwrap();
        (run.applied, run.crashed, scan)
    };

    assert_eq!(recovered("repro-a"), recovered("repro-b"));
}
