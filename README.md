# minidb

An embedded log-structured merge-tree (LSM) key/value store, written in Rust with no dependencies.

Built to understand storage engines from the bottom up — the same family of design as RocksDB,
LevelDB, and Cassandra's write path, implemented small enough to read in one sitting.

## Why an LSM tree is interesting

A B-tree updates data in place: to write one key you find its page, read it, modify it, and write it
back. That is a random I/O per write, and it gets worse as the tree grows.

An LSM tree makes every write an **append** instead. A mutation goes to a log and then to a sorted
in-memory buffer — no seeking, no read-modify-write, no page splits. When the buffer fills, it is
written out in one sequential pass as an immutable sorted file.

The cost is moved to reads. A key may live in the buffer or in any file on disk, so a lookup searches
newest-to-oldest until it finds a value *or a deletion marker*. That single trade-off is what makes
the rest of the design necessary rather than decorative:

- **Bloom filters** exist because a miss would otherwise touch every file on disk.
- **Sparse per-file indexes** exist so a lookup costs one block read, not a scan.
- **Compaction** exists because appends alone never reclaim overwritten or deleted data.
- **Tombstones** exist because you cannot delete in place from an immutable file — and dropping one
  too early silently resurrects deleted data.

## Status

This is a work in progress, and the split is sharp:

| Component | State | Notes |
|---|---|---|
| `memtable` | **Working** | `BTreeMap`-backed `put`/`get`/`delete`, tombstones, sorted iteration, size accounting |
| `lib` (`Db` API) | **Working** | Public `put`/`get`/`delete`/`contains`/`len` surface over the memtable |
| `main` (CLI demo) | **Working** | `cargo run` exercises the write path end to end |
| `wal` | Scaffolded | Signatures + record format documented; bodies are `todo!()` |
| `sstable` | Scaffolded | Signatures + file layout documented; bodies are `todo!()` |
| `bloom` | Scaffolded | Signatures + sizing math documented; bodies are `todo!()` |
| `compaction` | Scaffolded | Signatures + leveled strategy documented; bodies are `todo!()` |

**Nothing is persisted yet.** The store is in-memory only, and data does not survive process exit.
The scaffolded modules compile and document their intended design, but their functions will panic if
called. Durability is the next milestone.

24 tests currently pass (`cargo test`).

## Architecture

The intended full design. Only the memtable box is implemented today.

```
                 WRITE PATH                              READ PATH
                 ──────────                              ─────────

  put(k, v)                                    get(k)
     │                                            │
     ▼                                            ▼
  ┌─────────────────┐   append + fsync      ┌──────────────┐  found value
  │  Write-Ahead    │  ◄──── durability      │   MemTable   │  or tombstone
  │  Log (WAL)      │        boundary        │  (BTreeMap)  │ ────────────► return
  └─────────────────┘                        └──────────────┘
     │                                            │ not found
     ▼                                            ▼
  ┌─────────────────┐                        ┌──────────────┐
  │    MemTable     │  full? freeze          │  L0 SSTables │  bloom filter
  │   (BTreeMap)    │ ──────────┐            │  (may overlap│  rejects most
  └─────────────────┘           │            │   each other)│  misses in RAM
                                ▼            └──────────────┘
                        ┌──────────────┐          │ not found
                        │  flush as an │          ▼
                        │  immutable   │     ┌──────────────┐
                        │  L0 SSTable  │     │  L1 SSTables │  non-overlapping:
                        └──────────────┘     │              │  at most one file
                                │            └──────────────┘  checked per level
                                ▼                  │
                        ┌──────────────┐           ▼
                        │  Compaction  │      ┌──────────────┐
                        │  merges into │      │  L2 … Ln     │  each level ~10×
                        │  L1, L2, …   │      │              │  the size of the
                        └──────────────┘      └──────────────┘  one above
```

**Write path.** A mutation is appended to the WAL and fsynced before it is acknowledged — that fsync
is the durability boundary. It then lands in the memtable, a sorted `BTreeMap`. When the memtable
exceeds its size threshold it is frozen and flushed to a new immutable L0 SSTable in a single
sequential write, after which the WAL can be rotated. Ordering matters: the SSTable must be durable
*before* the log is discarded, or a crash between the two loses the data outright.

**Read path.** A lookup walks newest to oldest — memtable, then each L0 file, then one file per level
below. It stops at the first entry it finds, whether that is a value or a tombstone; a tombstone
means "deleted" and must halt the search rather than fall through to a stale value underneath.

**Deletes.** A delete writes a tombstone rather than erasing the key, because older files on disk are
immutable and may still hold a value. The tombstone shadows them until compaction reaches the
bottom-most level and can safely drop both.

**On-disk table layout.** Each SSTable is self-describing:

```
┌──────────────────────────────────────────────┐
│ Data blocks       ~4 KiB, prefix-compressed   │
├──────────────────────────────────────────────┤
│ Bloom filter      "definitely not here?"      │
├──────────────────────────────────────────────┤
│ Sparse index      first key → block offset    │
├──────────────────────────────────────────────┤
│ Footer            offsets, magic, version     │
└──────────────────────────────────────────────┘
```

The index is sparse — one entry per block rather than per key — so it stays small enough to keep
resident for every open table. A lookup binary-searches it, reads the single candidate block, and
scans it: one disk read per table probe, and usually zero, because the bloom filter answered first.

**Compaction.** L0 holds freshly flushed memtables, which may overlap each other. L1 and below hold
non-overlapping runs, each level roughly 10× the last. When a level exceeds its budget, a table is
merged with the overlapping tables one level down and rewritten. Because the inputs are sorted and
immutable, this is a k-way sequential merge — cheap, restartable, and safe to run in the background.

## Build, run, test

```bash
cargo build     # compile
cargo run       # demo: put / get / overwrite / delete / sorted scan
cargo test      # 24 tests, unit + integration + doctest
cargo clippy --all-targets -- -D warnings
```

`cargo run` prints a walkthrough of the working in-memory path:

```
── put ──
  put lang       = rust
  put structure  = lsm-tree
  ...
── scan (sorted, tombstones skipped) ──
  durable    = planned via wal
  lang       = rust
  structure  = lsm-tree

3 live keys, ~58 bytes buffered
```

## Usage

```rust
use minidb::Db;

let mut db = Db::new();
db.put(b"lang", b"rust");
assert_eq!(db.get(b"lang"), Some(b"rust".to_vec()));

db.delete(b"lang");
assert_eq!(db.get(b"lang"), None);
```

## Roadmap

- [x] **Memtable** — sorted in-memory buffer with tombstones
- [ ] **Durability (WAL)** — checksummed append-only log, fsync policy, crash-recovery replay with
      truncation at the first torn record
- [ ] **SSTables + bloom filters** — block-based immutable tables, sparse index, footer;
      Kirsch–Mitzenmacher double hashing for the filter
- [ ] **Compaction** — leveled strategy, k-way merge, correct tombstone lifetime
- [ ] **Concurrent readers/writers** — lock-free reads against immutable tables, single writer
- [ ] **MVCC / snapshot isolation** — sequence-numbered keys, point-in-time consistent reads
- [ ] **Range-scan iterator** — merged ordered iteration across memtable and all levels
- [ ] **TCP wire protocol** — network server, so it is a database and not just a library
- [ ] **Benchmarks** — throughput and latency against `sled` and RocksDB

## License

MIT — see [LICENSE](LICENSE).
