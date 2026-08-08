//! A guided tour of the storage engine.
//!
//! Run with `cargo run`. Each section exercises one layer of the LSM tree
//! against a real store directory under the system temp dir, which is removed
//! on exit.

use std::io;
use std::path::Path;

use minidb::{CompactionConfig, Db, DbOptions, FaultPlan, SharedDb, SyncPolicy};

fn main() -> io::Result<()> {
    println!("minidb — an embedded LSM-tree key/value store");

    let dir = std::env::temp_dir().join("minidb-demo");
    let _ = std::fs::remove_dir_all(&dir);

    basics()?;
    durability(&dir.join("durability"))?;
    flush_and_lookup(&dir.join("flush"))?;
    compaction(&dir.join("compaction"))?;
    concurrency(&dir.join("concurrent"))?;

    let _ = std::fs::remove_dir_all(&dir);
    println!("\ndone — every store directory above has been cleaned up");
    Ok(())
}

fn heading(title: &str) {
    println!("\n{title}");
    println!("{}", "─".repeat(title.chars().count()));
}

/// The basic key/value API, entirely in memory.
fn basics() -> io::Result<()> {
    heading("1. in-memory basics");

    let db = Db::new();
    for (key, value) in [("lang", "rust"), ("kind", "lsm-tree"), ("scratch", "temp")] {
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("  put    {key:<8} = {value}");
    }

    println!("  get    lang     -> {}", show(db.get(b"lang")?));
    println!("  get    missing  -> {}", show(db.get(b"missing")?));

    db.delete(b"scratch")?;
    println!("  delete scratch");
    println!("  get    scratch  -> {}", show(db.get(b"scratch")?));

    println!("\n  keys in sorted order, tombstones skipped:");
    for (key, value) in db.scan()? {
        println!(
            "    {:<8} = {}",
            String::from_utf8_lossy(&key),
            String::from_utf8_lossy(&value)
        );
    }
    Ok(())
}

/// Writes are fsynced to the log before they are acknowledged.
fn durability(dir: &Path) -> io::Result<()> {
    heading("2. durability — write, crash, recover");

    // A fault plan cuts the log off partway through a record, exactly as a
    // crash mid-append would.
    let options = DbOptions {
        fault: FaultPlan::crash_after_wal_bytes(120),
        ..DbOptions::default()
    };

    let db = Db::open_with_options(dir, options)?;
    let mut acknowledged = 0;
    for i in 0..20u32 {
        match db.put(format!("key:{i:02}").as_bytes(), b"acknowledged") {
            Ok(()) => acknowledged += 1,
            Err(e) => {
                println!("  simulated crash on write {i}: {e}");
                break;
            }
        }
    }
    println!("  {acknowledged} writes were acknowledged before the crash");
    drop(db); // no clean shutdown — the process is notionally gone

    let db = Db::open(dir)?;
    let recovered = (0..20u32)
        .filter(|i| {
            db.get(format!("key:{i:02}").as_bytes())
                .ok()
                .flatten()
                .is_some()
        })
        .count();
    println!("  {recovered} keys recovered after reopening the store");
    println!(
        "  every acknowledged write survived: {}",
        recovered >= acknowledged
    );
    Ok(())
}

/// Memtable flushes to an SSTable; lookups use the bloom filter and index.
fn flush_and_lookup(dir: &Path) -> io::Result<()> {
    heading("3. flush to an SSTable, and how a lookup narrows down");

    let db = Db::open_with_options(
        dir,
        DbOptions {
            flush_threshold_bytes: usize::MAX, // flush only when asked
            auto_compact: false,
            sync_policy: SyncPolicy::OsBuffered,
            ..DbOptions::default()
        },
    )?;

    for i in 0..5_000u32 {
        db.put(format!("key:{i:06}").as_bytes(), &[b'v'; 80])?;
    }
    println!("  wrote 5,000 keys into the memtable");
    println!("  wal is now {} KiB", db.wal_size_bytes() / 1024);

    db.flush()?;
    let tables = db.tables();
    let table = &tables[0];
    println!("\n  flushed to {}", file_name(table.path()));
    println!("    entries      {}", table.meta().num_entries);
    println!("    size         {} KiB", table.size_bytes() / 1024);
    println!(
        "    key range    {}..={}",
        show_bytes(&table.meta().min_key),
        show_bytes(&table.meta().max_key)
    );
    println!(
        "    wal after    {} bytes (rotated once the table was durable)",
        db.wal_size_bytes()
    );

    if let Some(bloom) = table.bloom() {
        println!(
            "    bloom        {} bytes, {} probes/key, ~{:.2}% false positives",
            bloom.size_bytes(),
            bloom.num_hashes(),
            bloom.estimated_fp_rate() * 100.0
        );
    }
    println!(
        "    index        {} blocks for {} keys (sparse: one entry per block)",
        table.num_blocks(),
        table.len()
    );

    println!("\n  a lookup filters in four stages, cheapest first:");
    println!("    1. key range   two comparisons against min/max");
    println!("    2. bloom       in-memory probe, ends most misses here");
    println!("    3. index       binary search to one block");
    println!("    4. block read  scan ~4 KiB, not the whole table");

    println!(
        "\n  get key:002500 -> {} byte value (hit: one block read)",
        db.get(b"key:002500")?.map_or(0, |v| v.len())
    );
    println!(
        "  get key:999999 -> {} (miss: rejected by the key range, no disk read)",
        show(db.get(b"key:999999")?)
    );
    Ok(())
}

/// Size-tiered compaction merges tables and reclaims superseded data.
fn compaction(dir: &Path) -> io::Result<()> {
    heading("4. size-tiered compaction");

    let db = Db::open_with_options(
        dir,
        DbOptions {
            flush_threshold_bytes: usize::MAX,
            auto_compact: false,
            sync_policy: SyncPolicy::OsBuffered,
            compaction: CompactionConfig::default(),
            ..DbOptions::default()
        },
    )?;

    // The same 500 keys rewritten four times: 2,000 entries, 500 of them live.
    for round in 0..4u32 {
        for i in 0..500u32 {
            db.put(
                format!("key:{i:04}").as_bytes(),
                format!("round-{round}{}", "-pad".repeat(20)).as_bytes(),
            )?;
        }
        db.flush()?;
    }
    // Delete a tenth of them.
    for i in (0..500u32).step_by(10) {
        db.delete(format!("key:{i:04}").as_bytes())?;
    }
    db.flush()?;

    let before_tables = db.sstable_count();
    let before_bytes: u64 = db.tables().iter().map(|t| t.size_bytes()).sum();
    let before_entries: u64 = db.tables().iter().map(|t| t.meta().num_entries).sum();
    println!(
        "  before:  {before_tables} tables, {before_entries} entries, {} KiB",
        before_bytes / 1024
    );

    let rounds = db.compact_all()?;
    let after_bytes: u64 = db.tables().iter().map(|t| t.size_bytes()).sum();
    let after_entries: u64 = db.tables().iter().map(|t| t.meta().num_entries).sum();
    println!(
        "  after:   {} table(s), {after_entries} entries, {} KiB  ({rounds} round(s))",
        db.sstable_count(),
        after_bytes / 1024
    );
    println!(
        "  reclaimed {}% of the bytes by dropping superseded values",
        100 - (after_bytes * 100 / before_bytes.max(1))
    );
    println!("  live keys: {} (50 were deleted)", db.len()?);
    // The merge took the four equal-sized tables, which includes the oldest, so
    // it was allowed to drop tombstones. These survivors are in the newest
    // table, which sat in its own size tier and was left alone.
    println!(
        "  tombstones left: {} — they live in the one table the merge did not cover",
        db.tables()
            .iter()
            .map(|t| t.meta().num_tombstones)
            .sum::<u64>()
    );
    Ok(())
}

/// `SharedDb` gives parallel readers and exclusive writers.
fn concurrency(dir: &Path) -> io::Result<()> {
    heading("5. concurrent access");

    let db = SharedDb::open_with_options(
        dir,
        DbOptions {
            sync_policy: SyncPolicy::OsBuffered,
            flush_threshold_bytes: 4_096,
            ..DbOptions::default()
        },
    )?;

    let mut handles = Vec::new();
    for t in 0..4u32 {
        let db = db.clone();
        handles.push(std::thread::spawn(move || -> io::Result<()> {
            for i in 0..250u32 {
                db.put(format!("t{t}:{i:03}").as_bytes(), b"written")?;
            }
            Ok(())
        }));
    }
    // Readers run in parallel with the writers above.
    for _ in 0..4 {
        let db = db.clone();
        handles.push(std::thread::spawn(move || -> io::Result<()> {
            for _ in 0..500 {
                let _ = db.get(b"t0:100")?;
            }
            Ok(())
        }));
    }

    println!("  4 writer threads and 4 reader threads sharing one store");
    for handle in handles {
        handle.join().expect("thread panicked")?;
    }

    println!("  {} keys written, all readable", db.len()?);
    println!("  {} SSTables on disk", db.sstable_count()?);
    println!("  handles outstanding: {}", db.handle_count());
    Ok(())
}

fn show(value: Option<Vec<u8>>) -> String {
    match value {
        Some(v) => String::from_utf8_lossy(&v).into_owned(),
        None => "<none>".to_string(),
    }
}

fn show_bytes(value: &[u8]) -> String {
    String::from_utf8_lossy(value).into_owned()
}

fn file_name(path: &Path) -> String {
    path.file_name()
        .unwrap_or_default()
        .to_string_lossy()
        .into_owned()
}
