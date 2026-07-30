//! Compaction — **not yet implemented**.
//!
//! # Why this exists
//!
//! Flushing memtables produces an ever-growing pile of SSTables. Left alone,
//! that pile costs read amplification (every miss probes every table), space
//! amplification (superseded values and tombstones are never reclaimed), and
//! eventually file-descriptor exhaustion.
//!
//! Compaction merges overlapping tables into fewer, larger ones. Because the
//! inputs are sorted and immutable, a merge is a k-way sequential scan — cheap
//! and safely restartable. It is also the only place where obsolete data is
//! actually reclaimed.
//!
//! # Leveled strategy
//!
//! - **L0** holds freshly flushed memtables. These *may overlap* each other, so
//!   a read must check every L0 table.
//! - **L1+** hold non-overlapping runs, each level roughly 10× the previous. A
//!   read touches at most one table per level.
//!
//! When a level exceeds its budget, pick a table from it, find every overlapping
//! table in the next level down, merge them, and write the result to that lower
//! level.
//!
//! # The tombstone rule
//!
//! A tombstone may only be dropped once no older table can still hold a value
//! for that key — that means it survives until it reaches the bottom-most level
//! participating in the merge. Dropping one early resurrects deleted data, which
//! is the classic LSM correctness bug and is silent when it happens.

use std::io;
use std::path::PathBuf;

use crate::sstable::SsTable;

/// One unit of compaction work: inputs to merge and where the output belongs.
#[derive(Debug, Clone)]
pub struct CompactionTask {
    /// Tables from the source level.
    pub inputs: Vec<PathBuf>,
    /// Overlapping tables from the destination level.
    pub overlaps: Vec<PathBuf>,
    pub source_level: usize,
    pub target_level: usize,
}

/// Size thresholds governing when compaction is triggered.
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// L0 table count that triggers a merge into L1.
    pub l0_trigger: usize,
    /// Byte budget for L1; each deeper level gets `size_multiplier`× the last.
    pub l1_budget_bytes: u64,
    pub size_multiplier: u64,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_trigger: 4,
            l1_budget_bytes: 10 * 1024 * 1024,
            size_multiplier: 10,
        }
    }
}

/// Decides what to compact next.
#[derive(Debug)]
pub struct CompactionPlanner {
    _private: (),
}

impl CompactionPlanner {
    pub fn new(_config: CompactionConfig) -> Self {
        todo!("store the config and the current level manifest")
    }

    /// Returns the highest-priority task, or `None` if every level is in budget.
    ///
    /// TODO: check the L0 file count first, then walk L1+ looking for the level
    /// most over its byte budget. Prefer whichever is furthest out of bounds so
    /// a single hot level cannot starve the others.
    pub fn plan(&self) -> Option<CompactionTask> {
        todo!("check L0 trigger, then find the most over-budget level")
    }

    /// Records a completed compaction so the next plan sees current state.
    ///
    /// TODO: this manifest update is the commit point. Write it durably before
    /// deleting any input file, or a crash between the two leaves the store
    /// referencing tables that no longer exist.
    pub fn apply_result(&mut self, _task: &CompactionTask, _outputs: Vec<PathBuf>) {
        todo!("update the level manifest, then retire the input tables")
    }
}

/// Merges the tables named by `task` into new tables at the target level.
///
/// TODO: k-way merge the input iterators through a binary heap keyed on
/// `(key, table_recency)`. On duplicate keys keep only the newest version. Drop
/// tombstones **only** when `task.target_level` is the bottom-most level — see
/// the tombstone rule in the module docs. Roll the output over into a new file
/// each time it reaches the target table size.
pub fn compact(_task: &CompactionTask) -> io::Result<Vec<PathBuf>> {
    todo!("k-way merge inputs, dedupe by recency, conditionally drop tombstones")
}

/// Merges sorted table iterators into one ascending, deduplicated stream.
///
/// TODO: the shared primitive behind both compaction and range scans.
pub fn merge_iter(_tables: &[SsTable]) -> impl Iterator<Item = io::Result<(Vec<u8>, Vec<u8>)>> {
    std::iter::empty()
}
