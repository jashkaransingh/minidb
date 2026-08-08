//! Compaction — merging overlapping SSTables to bound read cost and reclaim space.
//!
//! # Why this exists
//!
//! Flushing memtables produces an ever-growing pile of SSTables. Left alone,
//! that pile costs read amplification (a miss probes every table), space
//! amplification (superseded values and tombstones are never reclaimed), and
//! eventually file-descriptor exhaustion.
//!
//! Compaction merges several tables into one. Because the inputs are sorted and
//! immutable, a merge is a k-way sequential scan — cheap, restartable, and safe
//! to run without blocking readers of the old files.
//!
//! # Strategy: size-tiered
//!
//! Tables of *similar size* are merged together. Each flush produces a small
//! table; once several small tables accumulate they become one medium table,
//! several medium tables become one large table, and so on. Compared to leveled
//! compaction this writes far less (each byte is rewritten O(log n) times rather
//! than on every level crossing) at the cost of higher space amplification,
//! since several large tables can hold copies of the same key.
//!
//! # The two rules that make it correct
//!
//! **Inputs must be contiguous in recency order.** A merged table takes the
//! recency position of its newest input. If the planner picked tables 1 and 3
//! but not 2, the output would sit at position 3 and shadow table 2 — silently
//! reverting every key that table 2 had updated. [`plan`] therefore only ever
//! proposes an unbroken run.
//!
//! **A tombstone may only be dropped when no older table can still hold a value
//! for that key.** Dropping one early resurrects deleted data, which is the
//! classic LSM correctness bug and is completely silent when it happens. Here
//! that means tombstones survive unless the merge includes the *oldest* table in
//! the store — see [`CompactionTask::drop_tombstones`].
//!
//! # The third rule, added by MVCC
//!
//! **An old version may only be collected once no open snapshot can read it.**
//! Compaction is the only place versions are ever reclaimed, so it takes the
//! oldest live snapshot's sequence number and keeps everything at or above it,
//! plus one version beneath it per key. See [`merge_into`].
//!
//! # Crash safety
//!
//! Replacing N tables with 1 is not a single atomic operation: the output must
//! be published and the inputs removed. A crash in between would leave both, and
//! if tombstones had been dropped, the surviving old tables would resurrect
//! deleted keys.
//!
//! So the swap is journalled. Before the output is published, a marker file
//! naming the inputs and the output is written and fsynced. Recovery
//! ([`recover`]) finishes whichever half was interrupted: if the output exists,
//! the inputs are deleted; if it does not, the inputs are kept and the partial
//! output discarded. Either way the store lands in a consistent state, and the
//! marker is only removed once it is there.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::memtable::{Entry, compare_internal};
use crate::sstable::{SsTable, SsTableIter, SsTableWriter, TableMeta};
use crate::wal::sync_parent_dir;

/// Name of the journal file describing an in-flight table swap.
pub const COMPACTION_MARKER: &str = "COMPACTION";

/// Size thresholds governing when compaction is triggered.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CompactionConfig {
    /// Fewest tables in a size tier before they are worth merging.
    pub min_merge_width: usize,
    /// Most tables to merge at once, bounding the work of a single compaction.
    pub max_merge_width: usize,
    /// How far table sizes may differ and still count as the same tier.
    ///
    /// A table joins a run if its size is within this factor of the run's mean.
    pub size_ratio: f64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            min_merge_width: 4,
            max_merge_width: 10,
            size_ratio: 1.5,
        }
    }
}

/// What the planner knows about one table on disk.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableInfo {
    pub path: PathBuf,
    /// Recency rank. Higher is newer; ties broken by `generation`.
    pub seq: u64,
    /// Generation within a `seq`, incremented each time that slot is rewritten.
    pub generation: u32,
    pub size_bytes: u64,
}

/// One unit of compaction work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompactionTask {
    /// Tables to merge, oldest first. Always contiguous in recency order.
    pub inputs: Vec<TableInfo>,
    /// Whether tombstones may be discarded rather than carried into the output.
    ///
    /// Only ever true when the merge includes the oldest table in the store, so
    /// that no surviving table can still hold a value the tombstone was hiding.
    pub drop_tombstones: bool,
}

impl CompactionTask {
    /// Returns the recency slot the merged output will occupy.
    ///
    /// The newest input's `seq`, with a bumped `generation` so the output sorts above
    /// the table it replaces and gets a filename of its own.
    pub fn output_slot(&self) -> (u64, u32) {
        let newest = self.inputs.last().expect("a task always has inputs");
        (newest.seq, newest.generation + 1)
    }

    /// Total size of the input tables in bytes.
    pub fn input_bytes(&self) -> u64 {
        self.inputs.iter().map(|t| t.size_bytes).sum()
    }

    /// Returns the input paths.
    pub fn input_paths(&self) -> Vec<PathBuf> {
        self.inputs.iter().map(|t| t.path.clone()).collect()
    }
}

/// Chooses the next compaction, or `None` if no tier is over its threshold.
///
/// `tables` must be ordered oldest first. The returned task is always a
/// contiguous run — see the module docs for why that is not optional.
pub fn plan(tables: &[TableInfo], config: &CompactionConfig) -> Option<CompactionTask> {
    if tables.len() < config.min_merge_width {
        return None;
    }

    let mut best: Option<(usize, usize)> = None; // (start, width)

    for start in 0..tables.len() {
        let mut sum = 0u64;
        let mut width = 0usize;

        for candidate in tables.iter().skip(start) {
            let size = candidate.size_bytes.max(1);

            if width > 0 {
                // Compare against the mean of the run so far: a table joins only
                // if it is the same rough magnitude as its neighbours.
                let mean = sum as f64 / width as f64;
                let ratio = (size as f64 / mean).max(mean / size as f64);
                if ratio > config.size_ratio {
                    break;
                }
            }

            sum += size;
            width += 1;
            if width == config.max_merge_width {
                break;
            }
        }

        if width >= config.min_merge_width {
            // Prefer the widest run; on a tie prefer the oldest, so the bottom
            // of the store keeps getting consolidated rather than starved.
            let better = match best {
                None => true,
                Some((_, best_width)) => width > best_width,
            };
            if better {
                best = Some((start, width));
            }
        }
    }

    let (start, width) = best?;
    let inputs: Vec<TableInfo> = tables[start..start + width].to_vec();

    Some(CompactionTask {
        // Safe only when the oldest table in the store is part of the merge.
        drop_tombstones: start == 0,
        inputs,
    })
}

/// Merges `inputs` into a single new table at `output`.
///
/// `inputs` must be ordered oldest first. Returns the metadata of the table
/// produced.
///
/// # What gets collected, and why
///
/// Under MVCC a compaction is the only place old versions are ever reclaimed, so
/// it has to know which ones are still reachable. Two rules, both conservative:
///
/// - **Version collection.** For each user key, every version newer than
///   `oldest_snapshot` is kept — some open snapshot may still read it — plus the
///   newest version at or below `oldest_snapshot`, which is what the oldest
///   reader sees. Everything older than that is unreachable by any present or
///   future reader and is dropped. Passing `oldest_snapshot = u64::MAX` collapses
///   this to "keep only the newest version of each key".
/// - **Tombstone lifetime.** A tombstone may only be dropped when no older table
///   can still hold a value for that key, which here means the merge includes the
///   oldest table in the store (`drop_tombstones`). Even then only *trailing*
///   tombstones go: a tombstone that still shadows a kept older version has to
///   stay, or the delete silently reverts.
pub fn merge_into(
    inputs: &[SsTable],
    output: &Path,
    drop_tombstones: bool,
    oldest_snapshot: u64,
) -> io::Result<TableMeta> {
    let mut writer = SsTableWriter::create(output)?;
    let mut collector = VersionCollector::new(drop_tombstones, oldest_snapshot);

    for item in MergeIter::new(inputs)? {
        let (key, seq, entry) = item?;
        for (key, seq, entry) in collector.accept(key, seq, entry) {
            writer.append(&key, seq, &entry)?;
        }
    }
    for (key, seq, entry) in collector.finish() {
        writer.append(&key, seq, &entry)?;
    }
    writer.finish()
}

/// Decides which versions of the merged stream survive into the output table.
///
/// Kept separate from [`merge_into`] because the rules are the part worth
/// testing directly — the I/O around them is not where the subtlety lives.
struct VersionCollector {
    drop_tombstones: bool,
    oldest_snapshot: u64,
    /// User key currently being processed.
    current: Option<Vec<u8>>,
    /// Set once a version at or below `oldest_snapshot` has been kept for the
    /// current key; every older version of it is then unreachable.
    anchored: bool,
    /// Tombstones held back because they might turn out to be trailing.
    ///
    /// A tombstone is only droppable if nothing older survives beneath it, and
    /// that is not known until either an older kept version arrives (flush them)
    /// or the key ends (discard them).
    pending_tombstones: Vec<(Vec<u8>, u64)>,
}

impl VersionCollector {
    fn new(drop_tombstones: bool, oldest_snapshot: u64) -> Self {
        Self {
            drop_tombstones,
            oldest_snapshot,
            current: None,
            anchored: false,
            pending_tombstones: Vec::new(),
        }
    }

    /// Feeds one version in and returns whatever is now known to survive.
    fn accept(&mut self, key: Vec<u8>, seq: u64, entry: Entry) -> Vec<(Vec<u8>, u64, Entry)> {
        let mut out = Vec::new();

        if self.current.as_deref() != Some(key.as_slice()) {
            // A new key: anything still pending belonged to the previous key and
            // had nothing older beneath it, so it can go.
            out.extend(self.drain_pending_as_dropped());
            self.current = Some(key.clone());
            self.anchored = false;
        }

        // Unreachable: an older version than the one the oldest snapshot reads.
        if self.anchored {
            return out;
        }
        if seq <= self.oldest_snapshot {
            self.anchored = true;
        }

        if entry.is_tombstone() && self.drop_tombstones {
            // Hold it: droppable only if nothing older survives under it.
            self.pending_tombstones.push((key, seq));
            return out;
        }

        // A surviving non-tombstone means every held tombstone above it is still
        // shadowing something and must be written out too.
        out.extend(
            self.pending_tombstones
                .drain(..)
                .map(|(k, s)| (k, s, Entry::Tombstone)),
        );
        out.push((key, seq, entry));
        out
    }

    /// Flushes whatever remains once the stream ends.
    fn finish(&mut self) -> Vec<(Vec<u8>, u64, Entry)> {
        self.drain_pending_as_dropped()
    }

    /// Discards held tombstones when they are droppable, or emits them when not.
    fn drain_pending_as_dropped(&mut self) -> Vec<(Vec<u8>, u64, Entry)> {
        // `drop_tombstones` is the only reason anything is ever held, so reaching
        // here with a non-empty queue means these are trailing tombstones with
        // nothing older beneath them anywhere in the store.
        self.pending_tombstones.clear();
        Vec::new()
    }
}

/// Merges sorted table iterators into one ascending stream in internal-key
/// order — user key ascending, sequence number descending.
///
/// Every version is emitted; deciding which ones survive is
/// [`VersionCollector`]'s job, not this one's. Sequence numbers are globally
/// unique per mutation, so two inputs never carry the same internal key; if one
/// somehow did, the newest input wins, matching the read path.
///
/// Implemented as a linear scan across the input cursors rather than a binary
/// heap. A compaction merges at most `max_merge_width` tables — ten by default —
/// so the scan compares a handful of keys per output entry, and the code has no
/// heap-ordering invariant to get subtly wrong.
pub struct MergeIter {
    /// One cursor per input, in the same order as the inputs (oldest first).
    cursors: Vec<Cursor>,
    failed: bool,
}

struct Cursor {
    iter: SsTableIter,
    /// The next entry from this input, or `None` once it is exhausted.
    head: Option<(Vec<u8>, u64, Entry)>,
}

impl MergeIter {
    /// Builds a merge over `inputs`, which must be ordered oldest first.
    pub fn new(inputs: &[SsTable]) -> io::Result<Self> {
        let mut cursors = Vec::with_capacity(inputs.len());
        for table in inputs {
            let mut cursor = Cursor {
                iter: table.iter()?,
                head: None,
            };
            cursor.advance()?;
            cursors.push(cursor);
        }
        Ok(Self {
            cursors,
            failed: false,
        })
    }
}

impl Cursor {
    fn advance(&mut self) -> io::Result<()> {
        self.head = match self.iter.next() {
            Some(Ok(item)) => Some(item),
            Some(Err(e)) => return Err(e),
            None => None,
        };
        Ok(())
    }
}

impl Iterator for MergeIter {
    type Item = io::Result<(Vec<u8>, u64, Entry)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.failed {
            return None;
        }

        // Smallest internal key across all cursors.
        let (min_key, min_seq) = self
            .cursors
            .iter()
            .filter_map(|c| c.head.as_ref().map(|(k, s, _)| (k.clone(), *s)))
            .min_by(|(ak, as_), (bk, bs)| compare_internal(ak, *as_, bk, *bs))?;

        // Among the cursors sitting on that internal key, the newest input wins.
        // Cursors are in oldest-first order, so the last match is the newest.
        let mut winner: Option<Entry> = None;
        for cursor in self.cursors.iter_mut() {
            let matches = cursor
                .head
                .as_ref()
                .is_some_and(|(k, s, _)| k.as_slice() == min_key.as_slice() && *s == min_seq);
            if !matches {
                continue;
            }
            // Every copy of this internal key is consumed, not just the winner.
            winner = cursor.head.take().map(|(_, _, entry)| entry);
            if let Err(e) = cursor.advance() {
                self.failed = true;
                return Some(Err(e));
            }
        }

        winner.map(|entry| Ok((min_key, min_seq, entry)))
    }
}

/// A journalled table swap, describing work that must be finished or undone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Marker {
    pub output: PathBuf,
    pub inputs: Vec<PathBuf>,
}

impl Marker {
    /// Serializes as one path per line: the output first, then the inputs.
    fn encode(&self) -> String {
        let mut out = String::new();
        out.push_str(&self.output.to_string_lossy());
        out.push('\n');
        for input in &self.inputs {
            out.push_str(&input.to_string_lossy());
            out.push('\n');
        }
        out
    }

    fn decode(text: &str) -> Option<Self> {
        let mut lines = text.lines().filter(|l| !l.is_empty());
        let output = PathBuf::from(lines.next()?);
        let inputs = lines.map(PathBuf::from).collect();
        Some(Self { output, inputs })
    }

    /// Writes the marker durably, so recovery can see it after a crash.
    pub fn write(&self, dir: &Path) -> io::Result<()> {
        let path = dir.join(COMPACTION_MARKER);
        fs::write(&path, self.encode())?;
        // The marker is worthless if it does not survive the crash it exists for.
        fs::File::open(&path)?.sync_all()?;
        sync_parent_dir(&path)?;
        Ok(())
    }

    /// Reads the marker in `dir`, if one is present.
    pub fn read(dir: &Path) -> io::Result<Option<Self>> {
        let path = dir.join(COMPACTION_MARKER);
        if !path.exists() {
            return Ok(None);
        }
        let text = fs::read_to_string(&path)?;
        Ok(Marker::decode(&text))
    }

    /// Removes the marker, once the swap it describes is complete.
    pub fn clear(dir: &Path) -> io::Result<()> {
        let path = dir.join(COMPACTION_MARKER);
        if path.exists() {
            fs::remove_file(&path)?;
            sync_parent_dir(&path)?;
        }
        Ok(())
    }
}

/// Finishes or rolls back a compaction interrupted by a crash.
///
/// Called on open, before any table is read. The rule is decided by whether the
/// output was published:
///
/// - **Output present** — the merge completed. Delete the inputs, which are now
///   redundant, and clear the marker. This is the branch that matters: leaving
///   the inputs would resurrect keys whose tombstones the merge dropped.
/// - **Output absent** — the merge never finished. Keep the inputs, discard any
///   partial temp file, and clear the marker.
pub fn recover(dir: &Path) -> io::Result<()> {
    let Some(marker) = Marker::read(dir)? else {
        return Ok(());
    };

    if marker.output.exists() {
        for input in &marker.inputs {
            if input.exists() {
                fs::remove_file(input)?;
            }
        }
    } else {
        let mut tmp = marker.output.clone().into_os_string();
        tmp.push(".tmp");
        let tmp = PathBuf::from(tmp);
        if tmp.exists() {
            fs::remove_file(&tmp)?;
        }
    }

    sync_parent_dir(dir)?;
    Marker::clear(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(seq: u64, size_bytes: u64) -> TableInfo {
        TableInfo {
            path: PathBuf::from(format!("{seq:010}-0000.sst")),
            seq,
            generation: 0,
            size_bytes,
        }
    }

    #[test]
    fn nothing_to_do_below_the_minimum_width() {
        let config = CompactionConfig::default();
        let tables = vec![info(0, 100), info(1, 100), info(2, 100)];
        assert_eq!(plan(&tables, &config), None);
    }

    #[test]
    fn four_similar_tables_are_merged() {
        let config = CompactionConfig::default();
        let tables = vec![info(0, 100), info(1, 100), info(2, 100), info(3, 100)];

        let task = plan(&tables, &config).expect("should plan a merge");
        assert_eq!(task.inputs.len(), 4);
        assert!(task.drop_tombstones, "the oldest table is included");
        assert_eq!(task.output_slot(), (3, 1));
    }

    #[test]
    fn tables_of_very_different_sizes_are_not_merged_together() {
        let config = CompactionConfig::default();
        // Sizes escalate far beyond the ratio, so no run of 4 forms.
        let tables = vec![
            info(0, 100),
            info(1, 1_000),
            info(2, 10_000),
            info(3, 100_000),
        ];
        assert_eq!(plan(&tables, &config), None);
    }

    #[test]
    fn a_size_tier_is_selected_out_of_a_mixed_store() {
        let config = CompactionConfig::default();
        let tables = vec![
            info(0, 100_000), // big, old
            info(1, 100),
            info(2, 100),
            info(3, 100),
            info(4, 100),
        ];

        let task = plan(&tables, &config).expect("should plan a merge");
        assert_eq!(
            task.inputs.iter().map(|t| t.seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert!(
            !task.drop_tombstones,
            "table 0 is older and not in the merge, so tombstones must survive"
        );
    }

    #[test]
    fn planned_inputs_are_always_contiguous_in_recency_order() {
        let config = CompactionConfig::default();
        // A big table sits between two groups of small ones.
        let tables = vec![
            info(0, 100),
            info(1, 100),
            info(2, 5_000_000),
            info(3, 100),
            info(4, 100),
        ];
        // No contiguous run of 4 similar tables exists, so nothing is planned —
        // rather than jumping the gap and merging 0,1,3,4.
        assert_eq!(plan(&tables, &config), None);
    }

    #[test]
    fn a_merge_is_capped_at_the_maximum_width() {
        let config = CompactionConfig {
            max_merge_width: 5,
            ..CompactionConfig::default()
        };
        let tables: Vec<_> = (0..20).map(|i| info(i, 100)).collect();

        let task = plan(&tables, &config).expect("should plan a merge");
        assert_eq!(task.inputs.len(), 5);
    }

    #[test]
    fn the_widest_run_is_preferred() {
        let config = CompactionConfig::default();
        let tables = vec![
            info(0, 100),
            info(1, 100),
            info(2, 100),
            info(3, 100),
            info(4, 100),
            info(5, 100),
        ];
        let task = plan(&tables, &config).expect("should plan a merge");
        assert_eq!(task.inputs.len(), 6, "all six are one tier");
    }

    #[test]
    fn tombstones_are_only_droppable_when_the_oldest_table_participates() {
        let config = CompactionConfig::default();

        let all_old = vec![info(0, 100), info(1, 100), info(2, 100), info(3, 100)];
        assert!(plan(&all_old, &config).unwrap().drop_tombstones);

        let with_older_survivor = vec![
            info(0, 9_000_000),
            info(1, 100),
            info(2, 100),
            info(3, 100),
            info(4, 100),
        ];
        assert!(!plan(&with_older_survivor, &config).unwrap().drop_tombstones);
    }

    #[test]
    fn the_output_slot_takes_the_newest_inputs_position() {
        let config = CompactionConfig::default();
        let tables: Vec<_> = (0..4).map(|i| info(i, 100)).collect();
        let task = plan(&tables, &config).unwrap();

        // Newest input is seq 3 generation 0, so the output is seq 3 generation 1: it
        // replaces that slot without leapfrogging any table outside the merge.
        assert_eq!(task.output_slot(), (3, 1));
        assert_eq!(task.input_bytes(), 400);
    }

    #[test]
    fn a_marker_round_trips_through_its_encoding() {
        let marker = Marker {
            output: PathBuf::from("/store/0000000003-0001.sst"),
            inputs: vec![
                PathBuf::from("/store/0000000002-0000.sst"),
                PathBuf::from("/store/0000000003-0000.sst"),
            ],
        };
        assert_eq!(Marker::decode(&marker.encode()), Some(marker));
    }

    #[test]
    fn an_empty_marker_decodes_to_nothing() {
        assert_eq!(Marker::decode(""), None);
    }
}
