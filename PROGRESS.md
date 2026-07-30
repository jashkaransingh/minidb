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
