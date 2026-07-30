//! Bloom filters — **not yet implemented**.
//!
//! # Why this exists
//!
//! A read that misses has to check every level of the tree. With a dozen
//! SSTables on disk that is a dozen block reads to conclude "not found", which
//! makes misses far more expensive than hits — the wrong way round for most
//! workloads.
//!
//! A bloom filter is a compact probabilistic set that answers "definitely not
//! present" or "probably present". Each SSTable carries one, so a miss usually
//! costs a few bit tests in memory instead of a disk read. False positives cost
//! one wasted block read; false negatives cannot occur, which is what makes the
//! filter safe to consult before the real index.
//!
//! # Sizing
//!
//! For `n` keys at a target false-positive rate `p`:
//!
//! ```text
//! m = -n · ln(p) / (ln 2)²      bits
//! k = (m / n) · ln 2            hash functions
//! ```
//!
//! At `p = 1%` that is ~9.6 bits and 7 probes per key — small enough to keep
//! every table's filter resident in memory.
//!
//! # Hashing
//!
//! Rather than evaluating `k` independent hashes, use Kirsch–Mitzenmacher double
//! hashing: take one 128-bit hash, split it into `h1`/`h2`, and derive probe `i`
//! as `h1 + i·h2`. The false-positive rate is asymptotically unchanged and the
//! cost drops to a single hash per key.

/// A fixed-size probabilistic set of keys.
#[derive(Debug, Clone)]
pub struct BloomFilter {
    _private: (),
}

impl BloomFilter {
    /// Builds an empty filter sized for `expected_keys` at false-positive rate `fp_rate`.
    ///
    /// TODO: derive `m` and `k` from the formulas above, round `m` up to a whole
    /// number of `u64` words, and allocate the bit vector.
    pub fn new(_expected_keys: usize, _fp_rate: f64) -> Self {
        todo!("compute m and k, allocate the bit vector")
    }

    /// Records `key` as a member.
    ///
    /// TODO: hash once, derive `k` probe positions by double hashing, set those bits.
    pub fn insert(&mut self, _key: &[u8]) {
        todo!("set the k derived bit positions")
    }

    /// Returns `false` if `key` is definitely absent, `true` if it may be present.
    ///
    /// Never returns `false` for a key that was inserted — that guarantee is what
    /// lets [`crate::sstable::SsTable::get`] skip a disk read on a negative.
    pub fn contains(&self, _key: &[u8]) -> bool {
        todo!("test the k derived bit positions; false if any is clear")
    }

    /// Serializes the filter for embedding in an SSTable.
    ///
    /// TODO: emit `k` and the bit count alongside the words, so a reader can
    /// reconstruct the probe sequence without recomputing the sizing formulas.
    pub fn encode(&self) -> Vec<u8> {
        todo!("serialize k, bit length, and the backing words")
    }

    /// Reconstructs a filter from its serialized form.
    pub fn decode(_bytes: &[u8]) -> Option<Self> {
        todo!("parse header and words; return None on malformed input")
    }

    /// Returns the size of the bit vector in bytes.
    pub fn size_bytes(&self) -> usize {
        todo!("return the backing storage length")
    }

    /// Estimates the current false-positive rate given how many keys were inserted.
    ///
    /// `(1 - e^(-k·n/m))^k`. Useful as a test assertion and for tuning.
    pub fn estimated_fp_rate(&self) -> f64 {
        todo!("apply the false-positive formula to the current fill")
    }
}
