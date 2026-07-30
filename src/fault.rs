//! Deterministic fault injection for crash testing.
//!
//! # Why this exists
//!
//! Durability claims are only worth what they have been tested against. The
//! claim minidb makes is narrow and checkable: *if `put` returned `Ok`, the
//! write survives a crash.* Verifying it needs a crash at an arbitrary point in
//! the write path — including partway through a single record's bytes, before
//! the fsync that would have made it durable.
//!
//! # Why in-process, not `kill -9`
//!
//! Killing a real process is the more faithful simulation, but it is a poor
//! test: the crash point is whenever the signal happens to land, so runs are not
//! reproducible, a failure cannot be replayed under a debugger, and the harness
//! has to marshal state across a process boundary to know what was acknowledged.
//!
//! Injecting the fault in-process trades a little fidelity for full determinism.
//! A [`FaultPlan`] names an exact byte offset; the same seed produces the same
//! crash every time, so a failure found on run 73 can be re-run on run 73.
//!
//! # What the injected crash actually does
//!
//! At the chosen offset, [`crate::wal::Wal`] writes only the bytes that fit
//! *below* it, flushes them to the OS, and then returns an error **without
//! fsyncing** — leaving exactly the torn tail a real crash mid-append leaves.
//! Every subsequent operation fails, standing in for a process that is gone.
//!
//! This models power loss between the write and the fsync. It does not model a
//! disk that reorders or silently drops sectors, or a filesystem that lies about
//! fsync — those need a different tool.

use std::io;

/// A scripted failure point in the write path.
///
/// Offsets count **total bytes appended to the log over its lifetime**, not the
/// log's current size, so a plan stays meaningful across the rotations that
/// happen when the memtable is flushed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct FaultPlan {
    /// Simulate a crash once this many bytes have been appended to the log.
    ///
    /// The record straddling the offset is truncated at it and never fsynced.
    pub crash_after_wal_bytes: Option<u64>,
}

impl FaultPlan {
    /// A plan that never fires.
    pub const fn none() -> Self {
        Self {
            crash_after_wal_bytes: None,
        }
    }

    /// A plan that crashes once `bytes` have been appended to the log.
    pub const fn crash_after_wal_bytes(bytes: u64) -> Self {
        Self {
            crash_after_wal_bytes: Some(bytes),
        }
    }

    /// Returns `true` if this plan can fire at all.
    pub fn is_armed(&self) -> bool {
        self.crash_after_wal_bytes.is_some()
    }
}

/// The error returned by an operation interrupted by an injected fault.
///
/// Distinguishable from a real I/O error so a harness can tell "the simulated
/// crash fired" from "the test environment broke".
pub fn simulated_crash() -> io::Error {
    io::Error::new(
        io::ErrorKind::Interrupted,
        "simulated crash: injected fault point reached",
    )
}

/// Returns `true` if `error` is an injected crash rather than a real failure.
pub fn is_simulated_crash(error: &io::Error) -> bool {
    error.kind() == io::ErrorKind::Interrupted && error.to_string().starts_with("simulated crash")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_unarmed_plan_never_fires() {
        assert!(!FaultPlan::none().is_armed());
        assert!(!FaultPlan::default().is_armed());
    }

    #[test]
    fn an_armed_plan_records_its_offset() {
        let plan = FaultPlan::crash_after_wal_bytes(4_096);
        assert!(plan.is_armed());
        assert_eq!(plan.crash_after_wal_bytes, Some(4_096));
    }

    #[test]
    fn a_simulated_crash_is_distinguishable_from_a_real_error() {
        assert!(is_simulated_crash(&simulated_crash()));
        assert!(!is_simulated_crash(&io::Error::other("disk on fire")));
        assert!(!is_simulated_crash(&io::Error::new(
            io::ErrorKind::Interrupted,
            "a genuine EINTR"
        )));
    }
}
