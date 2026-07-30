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
