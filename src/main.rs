//! A short demo of the working in-memory write path.
//!
//! Run with `cargo run`. This exercises the parts of `minidb` that actually
//! work today — everything below the memtable is still scaffolding.

use minidb::Db;

fn main() {
    println!("minidb — embedded LSM-tree key/value store");
    println!("in-memory memtable only; nothing here is persisted yet\n");

    let mut db = Db::new();

    println!("── put ──");
    for (key, value) in [
        ("lang", "rust"),
        ("structure", "lsm-tree"),
        ("buffer", "btreemap"),
        ("durable", "not yet"),
    ] {
        db.put(key.as_bytes(), value.as_bytes());
        println!("  put {key:<10} = {value}");
    }

    println!("\n── get ──");
    for key in ["lang", "structure", "missing"] {
        match db.get(key.as_bytes()) {
            Some(v) => println!("  get {key:<10} -> {}", String::from_utf8_lossy(&v)),
            None => println!("  get {key:<10} -> <none>"),
        }
    }

    println!("\n── overwrite ──");
    db.put(b"durable", b"planned via wal");
    println!(
        "  put durable    = {}",
        String::from_utf8_lossy(&db.get(b"durable").unwrap())
    );

    println!("\n── delete ──");
    println!(
        "  delete buffer  -> removed existing value: {}",
        db.delete(b"buffer")
    );
    println!(
        "  delete ghost   -> removed existing value: {}",
        db.delete(b"ghost")
    );
    println!(
        "  get    buffer  -> {:?}",
        db.get(b"buffer")
            .map(|v| String::from_utf8_lossy(&v).into_owned())
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
        db.len(),
        db.size_bytes()
    );
}
