//! A short demo of the working write path.
//!
//! Run with `cargo run`. This exercises the parts of `minidb` that work today:
//! the durable write-ahead log and the in-memory memtable. The on-disk SSTable
//! levels below them are still scaffolding.

use std::io;

use minidb::Db;

fn main() -> io::Result<()> {
    println!("minidb — embedded LSM-tree key/value store");

    let dir = std::env::temp_dir().join("minidb-demo");
    let _ = std::fs::remove_dir_all(&dir);

    demo_in_memory()?;
    demo_durability(&dir)?;

    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

fn demo_in_memory() -> io::Result<()> {
    println!("\n=== in-memory store ===\n");
    let mut db = Db::new();

    println!("── put ──");
    for (key, value) in [
        ("lang", "rust"),
        ("structure", "lsm-tree"),
        ("buffer", "btreemap"),
        ("durable", "not yet"),
    ] {
        db.put(key.as_bytes(), value.as_bytes())?;
        println!("  put {key:<10} = {value}");
    }

    println!("\n── get ──");
    for key in ["lang", "structure", "missing"] {
        match db.get(key.as_bytes())? {
            Some(v) => println!("  get {key:<10} -> {}", String::from_utf8_lossy(&v)),
            None => println!("  get {key:<10} -> <none>"),
        }
    }

    println!("\n── overwrite ──");
    db.put(b"durable", b"via wal")?;
    println!(
        "  put durable    = {}",
        String::from_utf8_lossy(&db.get(b"durable")?.unwrap())
    );

    println!("\n── delete ──");
    println!(
        "  delete buffer  -> removed existing value: {}",
        db.delete(b"buffer")?
    );
    println!(
        "  delete ghost   -> removed existing value: {}",
        db.delete(b"ghost")?
    );

    println!("\n── scan (sorted, tombstones skipped) ──");
    for (key, value) in db.memtable().iter_values() {
        println!(
            "  {:<10} = {}",
            String::from_utf8_lossy(key),
            String::from_utf8_lossy(value)
        );
    }

    println!(
        "\n{} live keys, ~{} bytes buffered",
        db.len()?,
        db.size_bytes()
    );
    Ok(())
}

fn demo_durability(dir: &std::path::Path) -> io::Result<()> {
    println!("\n=== durability: write, drop, reopen ===\n");

    {
        let mut db = Db::open(dir)?;
        db.put(b"alpha", b"first")?;
        db.put(b"beta", b"second")?;
        db.delete(b"alpha")?;
        db.put(b"gamma", b"third")?;
        println!("  wrote 3 keys and 1 delete");
        println!("  wal is {} bytes on disk", db.wal_size_bytes());
        println!("  dropping the handle (simulating process exit)");
    }

    let db = Db::open(dir)?;
    println!("\n  reopened — replayed state:");
    for key in ["alpha", "beta", "gamma"] {
        match db.get(key.as_bytes())? {
            Some(v) => println!("    {key:<6} -> {}", String::from_utf8_lossy(&v)),
            None => println!("    {key:<6} -> <deleted>"),
        }
    }
    println!("\n  {} live keys recovered from the log", db.len()?);
    Ok(())
}
