# minidb

An embedded log-structured merge-tree (LSM) key/value store, written in Rust. One dependency
(`crc32fast`, for checksums); everything else — the log format, the on-disk tables, the recovery
logic — is written from scratch.

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

Every module is implemented and tested; the gaps are features not yet started, listed below the
table and tracked in the roadmap.

| Component | State | Notes |
|---|---|---|
| `memtable` | **Working** | Keyed by internal key `(user_key, seq)`, ordered key-ascending/seq-descending; tombstones, sorted iteration, size accounting |
| `snapshot` | **Working** | Point-in-time read snapshots; registry of live sequence numbers that compaction reads before collecting versions |
| `wal` | **Working** | crc32-checksummed frames carrying sequence numbers, fsync-per-frame, crash recovery with torn-tail truncation |
| `lib` (`Db` API) | **Working** | In-memory or durable; writes logged + fsynced, memtable flushed to SSTables, reads resolved against a snapshot sequence |
| `main` (CLI demo) | **Working** | `cargo run` tours every layer: basics, crash recovery, flush, compaction, threads |
| `sstable` | **Working** | Immutable sorted tables (format v2, versioned entries), 4 KiB blocks, sparse index, crc32-checked sections, atomic publish |
| `bloom` | **Working** | Sized from the textbook formula, double hashing, crc32-checked, one per table |
| `compaction` | **Working** | Size-tiered merging, correct tombstone lifetime, MVCC version collection, journalled crash-safe table swap |
| `fault` | **Working** | Deterministic in-process crash injection, used by the randomized crash suite |
| `concurrent` | **Working** | `SharedDb`: `Arc<RwLock<Db>>` with an `&self` API, poison-safe |

The storage engine is functionally complete. Writes are logged and fsynced, flushed to immutable
tables, and merged by size-tiered compaction. Reads filter through four stages, cheapest first: key
range → bloom filter → binary search over the sparse index → scan one 4 KiB block. A **miss** is
usually resolved entirely in memory; a **hit** costs a single block read.

Every write carries a monotonically increasing **sequence number**, and reads resolve against a
**snapshot** of that counter, so a reader sees a consistent point in time even while writes land
underneath it. The rule is stated exactly once, in the crate docs, and every layer honours it:

> A read at snapshot `S` resolves `key` to the version with the greatest sequence number `<= S`,
> searching the memtable first and then the tables newest-first. A tombstone found that way means
> absent and stops the search. Versions above `S` are never consulted.

Compaction is the only thing that reclaims old versions, and it will not collect anything an open
snapshot can still reach.

`Db` itself is single-threaded by design — `&mut self` makes exclusive access a compile-time fact and
costs nothing at runtime. `SharedDb` is the opt-in wrapper for multi-threaded use: any number of
concurrent readers, exclusive writers.

The honest gaps: **a write blocks every reader for its duration**, and under fsync-per-write that
means a disk sync. Removing that stall means letting readers snapshot the immutable table list and
read outside the lock, which needs a concurrent or double-buffered memtable — a redesign, not a
different lock. Compaction runs **inline on the flushing thread**, so a large merge stalls writes.
There is **no group-commit batching** (the log format supports it; the machinery is not built) and
**no range-scan iterator** — `scan()` materializes the whole live dataset in memory and is a test
helper, not a query path. There are **no benchmarks**: no throughput, latency, or write-amplification
number has been measured, so none is claimed.

207 tests currently pass (`cargo test`), including a randomized crash suite that injects a
deterministic failure partway through a log frame across 150 seeded runs and verifies that every
acknowledged write survives, multi-threaded stress tests asserting that no reader ever observes a
value that was never written, and 14 MVCC tests covering snapshot visibility across memtable
flushes and compactions.

## Architecture

Every box below is implemented. What remains is listed in the roadmap: lock-free reads, group
commit, a streaming range-scan iterator, background compaction, and a network layer.

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
│ Data blocks       ~4 KiB, ascending keys      │  implemented
├──────────────────────────────────────────────┤
│ Bloom filter      "definitely not here?"      │  implemented
├──────────────────────────────────────────────┤
│ Sparse index      first key → block offset    │  implemented
├──────────────────────────────────────────────┤
│ Meta              counts, min/max key         │  implemented
├──────────────────────────────────────────────┤
│ Footer            offsets, crc32, magic, ver  │  implemented
└──────────────────────────────────────────────┘
```

Section offsets live in the fixed 76-byte footer, which carries its own checksum and a magic number
so a truncated or foreign file is rejected before any offset is trusted. Reserving the bloom and
index sections up front means the next two milestones extend the format instead of rewriting it.

The index is sparse — one entry per block rather than per key — so it stays small enough to keep
resident for every open table. A lookup binary-searches it, reads the single candidate block, and
scans it: one disk read per table probe, and usually zero, because the bloom filter answered first.

**Compaction.** minidb uses a **size-tiered** strategy: tables of similar size are merged into one
larger table, so each flush's output is rewritten O(log n) times overall rather than once per level
crossing. Two rules make it correct, and both are easy to get silently wrong:

- **Inputs must be contiguous in recency order.** A merged table takes the recency slot of its newest
  input. Merging tables 1 and 3 but not 2 would put the output above table 2 and revert every key
  table 2 had updated.
- **A tombstone may only be dropped when no older table can still hold a value for that key.** Here
  that means tombstones survive unless the merge includes the oldest table in the store. Dropping one
  early resurrects deleted data — silently.

Replacing N tables with 1 is not atomic, so the swap is journalled: a marker file naming the inputs
and the output is fsynced before the output is published. On open, recovery finishes whichever half a
crash interrupted — deleting the inputs if the output exists, discarding the partial output if it
does not.

## Build, run, test

```bash
cargo build     # compile
cargo run       # guided tour: basics, crash recovery, flush, compaction, threads
cargo test      # 207 tests (the crash suite takes ~45s)
cargo clippy --all-targets -- -D warnings
```

`cargo run` walks through every layer against real store directories (cleaned up on exit):

```
2. durability — write, crash, recover
─────────────────────────────────────
  simulated crash on write 3: simulated crash: injected fault point reached
  3 writes were acknowledged before the crash
  3 keys recovered after reopening the store
  every acknowledged write survived: true

3. flush to an SSTable, and how a lookup narrows down
─────────────────────────────────────────────────────
  flushed to 0000000000-0000.sst
    entries      5000
    size         492 KiB
    bloom        5992 bytes, 7 probes/key, ~1.03% false positives
    index        120 blocks for 5000 keys (sparse: one entry per block)

4. size-tiered compaction
─────────────────────────
  before:  5 tables, 2050 entries, 208 KiB
  after:   2 table(s), 550 entries, 52 KiB  (1 round(s))
  reclaimed 75% of the bytes by dropping superseded values
```

## Usage

In-memory, with no durability:

```rust
use minidb::Db;

let mut db = Db::new();
db.put(b"lang", b"rust")?;
assert_eq!(db.get(b"lang")?, Some(b"rust".to_vec()));

db.delete(b"lang")?;
assert_eq!(db.get(b"lang")?, None);
```

Reads return `io::Result` because they may touch the disk.

Durable, backed by a directory. `put` returns only once the mutation is fsynced to the log, so an
`Ok` return means the write survives a crash:

```rust
use minidb::Db;

let mut db = Db::open("/tmp/my-store")?;
db.put(b"key", b"value")?;
drop(db); // or crash here — the write is already durable

let recovered = Db::open("/tmp/my-store")?;
assert_eq!(recovered.get(b"key")?, Some(b"value".to_vec()));
```

A snapshot pins a point in time. Reads through it see exactly the writes acknowledged before it was
taken, however much lands afterwards:

```rust
use minidb::Db;

let mut db = Db::new();
db.put(b"k", b"before")?;

let snap = db.snapshot();
db.put(b"k", b"after")?;
db.delete(b"gone")?;

assert_eq!(db.get(b"k")?, Some(b"after".to_vec()));
assert_eq!(db.get_at(&snap, b"k")?, Some(b"before".to_vec()));
```

Holding a snapshot also stops compaction reclaiming the versions it can reach, so it is a resource:
drop it when done.

Across threads. `SharedDb` is a cloneable handle over an `RwLock`: readers run in parallel, writers
are exclusive:

```rust
use minidb::SharedDb;

let db = SharedDb::open("/tmp/my-store")?;
db.put(b"key", b"value")?;

let reader = db.clone();
std::thread::spawn(move || {
    assert_eq!(reader.get(b"key").unwrap(), Some(b"value".to_vec()));
})
.join()
.unwrap();
```

Tuning is via `DbOptions` — fsync policy, memtable flush threshold, compaction thresholds, and
whether compaction runs automatically after a flush.

## Roadmap

- [x] **Memtable** — sorted in-memory buffer with tombstones
- [x] **Durability (WAL)** — checksummed append-only log, fsync policy, crash-recovery replay with
      truncation at the first torn record
- [x] **SSTable flush** — immutable sorted tables, checksummed sections, atomic publish by rename,
      WAL rotated only once the table is durable
- [x] **Bloom filters** — one per table, Kirsch–Mitzenmacher double hashing, so misses skip the read
- [x] **Sparse block index** — binary search to a single block instead of scanning the table
- [x] **Compaction** — size-tiered merging, k-way merge, correct tombstone lifetime, journalled swap
- [x] **Crash testing** — deterministic in-process fault injection, 150 seeded randomized runs
- [x] **Concurrent readers/writers** — `SharedDb`, an `RwLock` wrapper: parallel reads, exclusive writes
- [x] **MVCC / snapshot isolation** — sequence-numbered internal keys, point-in-time consistent
      reads, version collection gated on the oldest live snapshot
- [ ] **Lock-free reads** — snapshot the immutable table list and read outside the lock, so a write
      no longer stalls readers
- [ ] **Group-commit WAL batching** — one fsync per batch of concurrent writes instead of per write
- [ ] **Range-scan iterator** — merged ordered iteration across memtable and all levels
- [ ] **Background compaction** — move merges off the writing thread, so a large merge stops
      stalling writes
- [ ] **Write-amplification measurement** — disk bytes vs. logical bytes on a real workload
- [ ] **Benchmarks** — throughput and latency, against a baseline
- [ ] **TCP wire protocol** — network server, so it is a database and not just a library

## License

MIT — see [LICENSE](LICENSE).
