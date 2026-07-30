# Progress log

A running record of what has been built, why it was built that way, and what is next.
Newest milestone last.

> **Note on the roadmap source:** the task referred to a roadmap in `CLAUDE.md`, but no such file
> exists in this repo. The roadmap lives in `README.md`, and its ordering matches the six milestones
> given in the task, so I worked from that list. No decision was blocked by the discrepancy.

---

## Milestone 1 — Write-ahead log (durability)

**Status:** complete. Build, test, clippy (`-D warnings`), and fmt all clean. 53 tests pass.

### What was built

`src/wal.rs`, previously a `todo!()` stub, is now a working append-only log:

- **Record framing.** `crc32 (4B) | kind (1B) | key_len (4B) | value_len (4B) | key | value`, all
  little-endian. 13-byte fixed header.
- **`Wal::open`** — creates or reopens for append, and fsyncs the *parent directory* when the file is
  newly created.
- **`Wal::append` / `append_batch`** — encode, write, and (under `SyncPolicy::EveryWrite`) fsync
  before returning. `append_batch` amortizes to one fsync per batch.
- **`Wal::replay`** — decodes the durable prefix, stops at the first damaged record, truncates the
  file to that offset, and reports what it found via a `Recovery { records, valid_bytes, defect }`.
- **`Wal::rotate` / `sync` / `size_bytes` / `path`.**

`src/lib.rs` grew a durable mode. `Db::new()` is still purely in-memory; `Db::open(dir)` replays the
log into a fresh memtable and then appends every subsequent mutation to it.

### Key design decisions, and why

**Fixed-width lengths instead of varints.** The original stub doc specified varints. I switched to
fixed `u32`s: the saving is a couple of bytes on a log that gets rotated on every memtable flush, and
in exchange the "do I have a whole header left?" check at replay becomes a single comparison. That
check is the part that has to be exactly right for crash recovery, so I optimized for it being
obviously correct rather than for bytes on disk. The module docs were updated to match — docs
describing a format the code doesn't implement are worse than no docs.

**Checksum covers the length fields, and is verified before they are used.** A corrupt `key_len`
would otherwise size an allocation. `decode_all` also uses `checked_add` when computing the record
end, so a corrupt length can't overflow and wrap to a small in-bounds value.

**Replay never resynchronizes.** With this format, corruption in the middle of the log is
indistinguishable from a torn tail. Trying to skip ahead and salvage later records risks applying
mutations out of order around a gap, which silently produces a *wrong* database rather than a
smaller one. Stopping at the first defect is the conservative choice, and it's documented as such.

**`put`/`delete` became fallible (`io::Result`).** This was a breaking API change to `Db`, and I
made it deliberately: durability that cannot report failure is not durability. An `Ok(())` from
`put` on a durable store now carries a real guarantee. All call sites — tests, `main.rs`, the
doctests, the README — were updated. No existing logic in `memtable.rs` or `lib.rs` was removed.

**Directory fsync on file creation.** A new file's directory entry is metadata; without an fsync on
the parent directory a crash can lose the file even though its contents were flushed. Handled in
`sync_parent_dir`, which no-ops on Windows where the call isn't meaningful.

### What is tested

15 unit tests in `wal.rs` and 12 integration tests in `tests/durability_test.rs`, covering:

- Round-trip of puts, deletes, empty keys/values, and arbitrary binary payloads.
- Replay ordering; last-write-wins; delete-then-rewrite.
- Reopen-and-append (the log is not clobbered on reopen).
- **Torn payload** — truncate mid-record; earlier records survive, file is repaired.
- **Torn header** — a stub shorter than 13 bytes; same.
- **Bit flip** — caught by crc32, reported as `BadChecksum`.
- **Corrupt length field** set to `u32::MAX` — rejected without a huge allocation.
- **Unknown kind byte** with a *valid* checksum — rejected.
- A repaired log accepts new writes and replays cleanly afterwards.
- Bulk: 500 writes + 50 deletes, all correct after recovery.
- Missing log replays as empty rather than erroring.

### Known limitations (deliberate, not oversights)

- The log is **never rotated**, because there is nowhere to flush the memtable to yet. The log grows
  without bound and startup replays all of it. Milestone 2 fixes this.
- The whole dataset lives in RAM. `MEMTABLE_FLUSH_THRESHOLD_BYTES` is defined but not enforced.
- `SyncPolicy::OsBuffered` survives process death but not power loss. Documented on the enum.

### Next

Milestone 2 — SSTable flush: write a frozen memtable to an immutable sorted file, then rotate the
WAL. Ordering is the whole game: table written → table fsynced → directory fsynced → *then* log
rotated. Any other order loses data on a crash between the steps.

---

## Milestone 2 — SSTable flush

**Status:** complete. Build, test, clippy (`-D warnings`), and fmt all clean. 89 tests pass.

### What was built

`src/sstable.rs`, previously a `todo!()` stub, now reads and writes real immutable tables.

**File layout** — sections, with all offsets recorded in a fixed 76-byte footer:

```
[data section][bloom: empty][index: empty][meta section][footer]
```

The footer holds `data_len`, `data_crc`, the offset/length of every other section, a format version,
its own crc32, and a magic number. Data entries are `kind | key_len | value_len | key | value`.

- **`SsTableWriter`** — `create` / `append` / `finish`. Rejects out-of-order or duplicate keys rather
  than producing a table whose binary search would be wrong later.
- **`SsTable`** — `open` (validates footer, loads meta), `get`, `iter`, `verify`, `may_contain`.
- **`Db::flush`** — freezes the memtable into a new table, then rotates the WAL.
- Auto-flush when the memtable passes `DbOptions::flush_threshold_bytes`.
- `Db::get` now searches memtable → tables newest-first, stopping at the first value *or tombstone*.
- `Db::scan` returns the merged live view across all levels.

### Key design decisions, and why

**Reserved-but-empty bloom and index sections.** Milestones 3 and 4 add a bloom filter and a sparse
index. Rather than write a format now that would need replacing twice, the footer already carries
offset/length slots for both, currently zero. Those milestones fill in sections; they do not change
the layout. The cost is 32 wasted footer bytes per table.

**Atomic publish via `.tmp` + rename.** A table is written to `<name>.sst.tmp`, fsynced, then renamed
into place, then the directory is fsynced. A crash mid-write leaves a stray temp file, never a
half-built table that recovery would mistake for complete. `Db::open` deletes stray `.tmp` files, and
the writer's `Drop` cleans up if it is abandoned without `finish`.

**Flush ordering is the correctness argument.** Table written → table fsynced → directory fsynced →
*then* WAL rotated. A crash before the rotation leaves the data in *both* the table and the log;
replay puts it back in the memtable where it shadows the identical table entries, so reads are
unaffected. That is the safe direction. Rotating first would lose everything the table held.

**Sequence-numbered filenames, no manifest yet.** Tables are `{seq:010}.sst`, discovered by listing
the directory and sorting numerically. Zero-padding keeps lexical and numeric order in agreement.
This is enough while every table is a peer at L0; compaction (milestone 5) needs to know which level
a table belongs to, which is where a real manifest becomes necessary.

**`get` scans sequentially — deliberately, for now.** No index exists yet, so `SsTable::get` walks
the data section and stops early once it passes the target key. It is O(n) and honestly documented as
such in both the module docs and the README. Milestone 4 replaces the scan with a binary search.
`may_contain` (min/max key check) already skips tables that cannot hold the key.

**Read API became fallible.** `get`, `contains`, `len`, `is_empty`, and `scan` now return
`io::Result`, because reads touch the disk and disk reads fail. Mechanical churn across all tests;
no logic removed from `memtable.rs`.

**`delete` returns memtable-only truth.** `Db::delete` reports whether a live value was visible *in
the memtable*. Answering it across every level would require a full lookup on each delete, which
defeats the append-only write path. Documented on the method rather than quietly redefined.

### What is tested

19 unit tests in `sstable.rs` and 17 integration tests in `tests/sstable_test.rs`:

- Round-trip of values, tombstones, empty values (distinct from tombstones), binary keys, 2 000-entry
  tables, and empty tables.
- Out-of-order and duplicate appends rejected.
- Metadata: counts, tombstone count, min/max key.
- Corruption: bad magic, corrupt footer (caught by footer crc), truncated file, corrupted data
  section (caught by `verify`).
- Publication: nothing visible before `finish`; abandoned writer leaves no temp file; stale `.tmp`
  removed on open.
- Shadowing: newer table beats older; tombstone in a newer table hides an older value; memtable beats
  every table; delete-then-rewrite across three flushes.
- Ordering survives a reopen; sequence numbers continue after a reopen.
- WAL rotated on flush, and data still readable afterwards.
- Bulk: 600 keys with a third overwritten and a third deleted, verified after a reopen.

### Known limitations (deliberate)

- **Lookups are O(n) per table** until milestones 3 and 4.
- **Tables accumulate without bound.** Nothing merges them; a store that is written to forever grows
  an unbounded number of tables, and every miss touches all of them. Milestone 5.
- `scan`/`len` materialize the whole dataset in memory. Fine for tests, wrong for production; a
  streaming merge iterator comes with compaction.
- No manifest file — level membership is implied by filename order.

### Next

Milestone 3 — bloom filters: one per table, written into the reserved section, probed before any data
read so a miss costs a few in-memory bit tests instead of a full scan.

---

## Milestone 3 — Bloom filters

**Status:** complete. Build, test, clippy (`-D warnings`), and fmt all clean. 111 tests pass.

### What was built

`src/bloom.rs`, previously a `todo!()` stub, is now a working filter, and every SSTable carries one
in the section reserved for it in milestone 2.

- **`BloomFilter::new(expected_keys, fp_rate)`** — sizes `m` and `k` from the standard formulas
  (`m = -n·ln p / (ln 2)²`, `k = (m/n)·ln 2`), clamped against degenerate inputs.
- **`insert` / `contains`**, plus `insert_hashed` / `contains_hashed` taking a precomputed hash pair.
- **`encode` / `decode`** — `crc32 | k | num_bits | words…`, self-checksummed.
- **Diagnostics** — `fill_ratio`, `estimated_fp_rate`, `num_bits`, `num_hashes`, `size_bytes`.
- `SsTableWriter` hashes each key during `append` and builds the filter at `finish`.
- `SsTable::get` probes range → bloom → data scan, cheapest first.

### Key design decisions, and why

**The hash function is pinned in this crate, not taken from `std`.** This is the most important
decision in the milestone. Filters are serialized into SSTables and read back by whatever binary
opens the store later. `DefaultHasher` explicitly does not guarantee a stable algorithm across Rust
releases — so a store written by one build and read by another could probe *different bits*, and the
filter would start reporting **false negatives**. A false negative means a read returns "not found"
for data that is really on disk: silent, permanent-looking data loss that no checksum would catch.
So the hash is FNV-1a with two offset bases, each finalized through MurmurHash3's `fmix64`, entirely
defined in `bloom.rs` and stable by construction.

**FNV-1a alone was not good enough.** It avalanches poorly in the high bits, and the probe index is
`(h1 + i·h2) % num_bits` — so weak high bits translate directly into clustered probes and a worse
false-positive rate. The `fmix64` finalizer fixes the distribution for a few ns per key.

**`h2` is forced odd.** Without it, an `h2` sharing factors with `num_bits` collapses the probe
sequence onto a short cycle, so `k` probes can hit far fewer than `k` distinct bits. There is a test
asserting this property directly.

**The filter is built at `finish`, not at `create`.** Sizing needs `n`, and `n` is not known until
the last key has been appended. The writer therefore stores one `(h1, h2)` pair per key — 16 bytes,
bounded by the memtable size — and builds an exactly-sized filter at the end. Guessing at `create`
time would mean either a bloated filter or one far off its target rate.

**A corrupt filter degrades to no filter.** `BloomFilter::decode` returns `None` on a checksum
failure, and `SsTable::open` treats that as "no filter available" rather than failing the open. The
data section is independently checksummed and remains authoritative, so the cost of corruption here
is a slower read, never a wrong answer. Trusting a corrupt filter would risk false negatives.

**Empty tables get no filter.** `bloom_len = 0`, and readers handle that already — which also means
tables written in milestone 2, before filters existed, still open and read correctly. No format
version bump was needed; the change is purely additive.

### What is tested

15 unit tests in `bloom.rs` and 8 integration tests in `sstable.rs`:

- **No false negatives** across 1 000 keys — the property everything else depends on.
- Measured false-positive rate stays under 3% against a 1% target over 20 000 trials.
- Sizing matches the formula (~9.6 bits/key, k=7 at 1%); tighter targets produce larger filters.
- Encode/decode round-trip preserves *identical* answers, including on absent keys.
- Corrupt and truncated encodings are rejected.
- `h2` is always odd; hashing is deterministic.
- Degenerate parameters (0 keys, `fp_rate` 0.0 and 1.0) still produce usable filters.
- In-table: tombstones are in the filter (or deletes would stop shadowing); empty tables carry none;
  a corrupted filter section still yields correct reads; the filter rejects >90% of absent keys; the
  filter is <5% of table size.

### Known limitations (deliberate)

- **Hits are still O(n) per table** — the data scan is unchanged. Milestone 4.
- `num_inserted` is not serialized, so a decoded filter reports 0 for it. `estimated_fp_rate` uses
  the observed fill ratio instead, which works either way.
- Tables still accumulate without bound. Milestone 5.

### Next

Milestone 4 — sparse block index: group entries into blocks, record `(first_key, offset)` per block
in the reserved index section, and binary-search it so a hit reads one block instead of scanning.

---

## Milestone 4 — Sparse block index

**Status:** complete. Build, test, clippy (`-D warnings`), and fmt all clean. 124 tests pass.

### What was built

The data section is now divided into ~4 KiB **blocks**, and the index section reserved in milestone 2
holds one entry per block: `(first_key, offset, len)`.

- **Writer** — opens a block lazily on its first entry, closes it once it passes
  `BLOCK_TARGET_BYTES`, and always closes on a whole-entry boundary. The trailing short block is
  closed at `finish`.
- **Index encoding** — `crc32 | count | (key_len, key, offset, len)…`, self-checksummed.
- **Reader** — loads the index on open; `find_block` binary-searches it, `get_in_block` reads exactly
  that block and scans it.

`SsTable::get` is now four stages: key range → bloom → binary search → one block read.

### Key design decisions, and why

**Blocks close on entry boundaries, never mid-entry.** A block is read and parsed in isolation, so an
entry split across two blocks would be unparseable without stitching. Closing only after a complete
entry means `block.len` slightly overshoots the 4 KiB target rather than truncating — the right
trade, since the alternative is a format that cannot be read one block at a time.

**The index records each block's *first* key, and lookup takes the last block whose first key is
`<= key`.** This is the subtle part. A key that falls in a *gap* between two blocks' key ranges
sorts after block N's first key and before block N+1's, so the search lands on block N and finds
nothing there — the correct answer. Taking the *first* block with `first_key >= key` instead would
skip past the block that actually contains the key. `Err(0)` from the binary search means the key
sorts before every block, so the table cannot contain it.

**Blocks are opened lazily, on their first entry.** The index needs the block's real first key, and
that is not known until an entry arrives. Recording it eagerly at block-close time would mean storing
the *previous* block's last key, which is a different and wrong thing.

**A corrupt index degrades to a full scan, exactly like the bloom filter.** `decode_index` returns
`None` on a checksum failure and the reader falls back to `get_by_scan`. Both optional structures
follow the same rule: they may make reads faster, never make them wrong. This also means milestone 2
tables, which have `index_len = 0`, still open and read correctly — no format version bump was
needed.

**Index entries are validated against the data section before use.** `get_in_block` rejects an entry
whose `offset + len` exceeds `data_len` rather than issuing a wild read.

### What is tested

13 tests in `src/sstable.rs`:

- A 1 000-key table produces many blocks, and the index is genuinely sparse (blocks < keys/5).
- **Every key findable** through the index across 1 000 keys.
- **Gap keys return `None`** — a table of only even keys, probed with all the odd ones.
- Keys sorting before all blocks and after all blocks return `None`.
- **Block boundaries**: every block's own first key is findable (the case a binary search most easily
  gets wrong).
- The index tiles the data section exactly — contiguous, non-overlapping, ascending, summing to
  `data_len`.
- First index entry matches `meta.min_key` at offset 0.
- Small tables get exactly one block; empty tables get no index.
- Tombstones remain findable through the index.
- A corrupted index section is discarded and reads still return correct answers via the fallback.
- Index encoding round-trips; corrupt and truncated encodings are rejected.
- A lookup's candidate block is <2× the block target and <10% of the data section.

### Known limitations (deliberate)

- **Tables accumulate without bound.** This is now the dominant problem: every flush adds a table,
  reads may consult all of them, and superseded values and tombstones are never reclaimed. Milestone
  5.
- No block-level checksums — integrity is a single crc32 over the whole data section, so `verify()`
  is all-or-nothing rather than per-block.
- No prefix compression within blocks; keys are stored whole.
- `get_in_block` opens the file per lookup. A file-handle or block cache would help, but that is a
  performance concern, not a correctness one.

### Next

Milestone 5 — compaction: size-tiered merging of overlapping tables, with the tombstone-lifetime rule
handled correctly (a tombstone may only be dropped when no older table can still hold a value for
that key).

---

## Milestone 5 — Compaction (size-tiered)

**Status:** complete. Build, test, clippy (`-D warnings`), and fmt all clean. 150 tests pass.

### What was built

`src/compaction.rs`, previously a `todo!()` stub, now plans and executes merges.

- **`plan(tables, config)`** — size-tiered planner. Walks contiguous runs of tables whose sizes are
  within `size_ratio` of the run's mean, and returns the widest run of at least `min_merge_width`
  (default 4, capped at 10).
- **`MergeIter`** — k-way merge across input tables, newest input winning on duplicate keys.
- **`merge_into`** — drives the merge into a new table, optionally dropping tombstones.
- **`Marker` / `recover`** — the crash-safety journal for the table swap.
- **`Db::compact` / `Db::compact_all`**, plus `auto_compact` (on by default) after each flush.

**Strategy note:** the stub's docs described *leveled* compaction. The task specified **size-tiered**,
so the implementation and the docs are both size-tiered now. Leaving leveled docs over a size-tiered
implementation would have been worse than no docs.

### Key design decisions, and why

**Inputs must be contiguous in recency order.** This is the decision the whole milestone hinges on. A
merged table has to occupy *some* position in the newest-first read order. It takes the position of
its newest input — which is only sound if nothing outside the merge sits between the inputs. If the
planner picked tables 1 and 3 but skipped 2, the output would land at position 3 and shadow table 2,
silently reverting every key table 2 had updated. So `plan` only ever proposes an unbroken run, and
there are two tests pinning that: one on the planner directly, one end-to-end
(`a_merged_table_does_not_shadow_newer_tables_outside_the_merge`).

**A `(seq, generation)` recency slot instead of a bare sequence number.** The output needs the
newest input's recency position *and* a filename of its own. Filenames are now
`{seq:010}-{generation:04}.sst`, ordered by `(seq, generation)`. The output reuses the newest input's
`seq` and takes the next `generation`. Only one table ever holds a given `seq`, so collisions cannot
occur. The alternative — giving the output a fresh, higher `seq` — is exactly the reversion bug.

**Tombstones are dropped only when the oldest table is in the merge.** A tombstone exists to shadow
values in older tables. If any older table survives the merge, dropping the tombstone un-deletes
whatever it was hiding. The conservative rule (`drop_tombstones = start == 0`) is easy to verify and
costs only some retained tombstones in partial merges. Two tests cover both directions: dropped when
the oldest participates, kept when it does not.

**The swap is journalled.** Replacing N tables with 1 means publishing the output and deleting the
inputs — two steps, not one. A crash in between leaves both, and if tombstones were dropped, the
surviving inputs resurrect deleted keys. So a marker file naming inputs and output is fsynced *before*
the output is published, and `recover` runs on open before any table is read:

- Output exists → the merge finished; delete the inputs, clear the marker.
- Output missing → the merge did not finish; keep the inputs, discard the partial `.tmp`, clear the
  marker.

Both branches are tested by planting a marker and reopening.

**Linear-scan merge rather than a binary heap.** At most `max_merge_width` (10) tables merge at once,
so finding the minimum key is a scan of ≤10 cursors per output entry. A heap would be asymptotically
better and has an ordering invariant that is easy to get subtly wrong; at this width the scan is
faster to reason about and no slower in practice. Documented on `MergeIter`.

**Every copy of a duplicated key is consumed, not just the winner.** The merge advances *all* cursors
sitting on the minimum key. Advancing only the winner would re-emit the older copies on later
iterations, producing a table with duplicate keys — which `SsTableWriter` would then reject.

### What is tested

11 unit tests in `compaction.rs` and 14 integration tests in `tests/compaction_test.rs`:

- **No resurrection**: a deleted key stays deleted through compaction and a reopen.
- **Tombstones preserved** when an older table is excluded from the merge.
- **Tombstones dropped** when the oldest table participates (asserted on `num_tombstones == 0`).
- **No reversion**: merging four old tables does not revert a newer table's value for the same key.
- Newest value wins for a key rewritten across four tables.
- Space reclaimed: 400 entries over 100 keys compacts to 100 entries at less than half the bytes.
- Planner: min width respected, size tiers separated, max width capped, widest run preferred,
  contiguity enforced across a large intervening table.
- **Crash recovery both ways**: marker + published output → inputs deleted; marker + no output →
  inputs kept, partial temp discarded.
- Auto-compaction keeps the table count under 10 across 30 flushes.
- A mixed workload (300 writes, a third overwritten, a fifth deleted) verified after compaction and
  reopen.
- `compact_all` converges and leaves nothing further to do.

### Known limitations (deliberate)

- **Compaction is synchronous**, running inline on the thread that flushed. A large merge stalls
  writes. Moving it to a background thread needs the concurrency work first.
- **No concurrency at all** — `Db` takes `&mut self`; there is no locking and no `Send`/`Sync` story.
- The planner reads sizes only; it does not consider tombstone density, so a table that is mostly
  tombstones is not prioritized for merging.
- Space amplification is inherent to size-tiered: several large tables may each hold a copy of the
  same key. Leveled compaction would bound that at the cost of more write amplification.

### Next

Milestone 6 — in-process fault-injection crash testing: a deterministic, seeded failure point
injected into the write path, asserting across ≥100 randomized runs that every write acknowledged
before the simulated crash is still readable afterwards.
