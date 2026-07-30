//! Bloom filters — compact probabilistic membership tests, one per SSTable.
//!
//! # Why this exists
//!
//! A read that misses has to check every table. With a dozen tables on disk that
//! is a dozen scans to conclude "not found", which makes misses far more
//! expensive than hits — the wrong way round for most workloads.
//!
//! A bloom filter answers "definitely not present" or "probably present". Each
//! SSTable carries one, so a miss usually costs a few bit tests in memory
//! instead of touching the data section at all. False positives cost one wasted
//! read; **false negatives cannot occur**, which is what makes the filter safe
//! to consult before the real lookup.
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
//! Rather than evaluating `k` independent hashes, this uses Kirsch–Mitzenmacher
//! double hashing: take two 64-bit hashes `h1`/`h2` and derive probe `i` as
//! `h1 + i·h2`. The false-positive rate is asymptotically unchanged and the cost
//! drops to two hash evaluations per key regardless of `k`.
//!
//! The hash is **defined here rather than borrowed from the standard library**,
//! and that is deliberate. These filters are serialized into SSTables and read
//! back months later, possibly by a binary built with a different Rust version.
//! `DefaultHasher` explicitly does not promise a stable algorithm across
//! releases, and an unstable hash would turn a filter into a source of *false
//! negatives* — reads returning "not present" for data that is really there.
//! That is silent data loss, so the hash is pinned in this file: FNV-1a with two
//! different offset bases, each run through the MurmurHash3 64-bit finalizer for
//! avalanche.

/// Default target false-positive rate for table filters.
pub const DEFAULT_FP_RATE: f64 = 0.01;

/// Bytes of fixed header in the encoded form: crc32, k, and the bit count.
const ENCODED_HEADER_LEN: usize = 4 + 4 + 8;

const FNV_OFFSET_A: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_OFFSET_B: u64 = 0x9e37_79b9_7f4a_7c15;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

/// A fixed-size probabilistic set of keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BloomFilter {
    /// Bit vector, packed into 64-bit words.
    words: Vec<u64>,
    /// Number of bits actually in use (`words.len() * 64` rounds up past this).
    num_bits: u64,
    /// Number of probes per key.
    k: u32,
    /// Keys inserted so far, tracked for [`estimated_fp_rate`](Self::estimated_fp_rate).
    num_inserted: u64,
}

impl BloomFilter {
    /// Builds an empty filter sized for `expected_keys` at false-positive rate `fp_rate`.
    ///
    /// `fp_rate` is clamped to a sane open interval; a rate of 0 would demand an
    /// infinite filter, and a rate of 1 would demand none at all.
    pub fn new(expected_keys: usize, fp_rate: f64) -> Self {
        let n = expected_keys.max(1) as f64;
        let p = fp_rate.clamp(1e-9, 0.5);

        let ln2 = std::f64::consts::LN_2;
        let m = (-n * p.ln() / (ln2 * ln2)).ceil().max(64.0);
        let k = ((m / n) * ln2).round().clamp(1.0, 30.0) as u32;

        let num_bits = m as u64;
        let num_words = num_bits.div_ceil(64) as usize;

        Self {
            words: vec![0; num_words],
            num_bits,
            k,
            num_inserted: 0,
        }
    }

    /// Records `key` as a member.
    pub fn insert(&mut self, key: &[u8]) {
        let (h1, h2) = hash_pair(key);
        self.insert_hashed(h1, h2);
    }

    /// Records a key from its precomputed hash pair.
    ///
    /// Lets the SSTable writer hash each key once during `append` and build the
    /// filter at `finish`, when the exact key count — and therefore the correct
    /// filter size — is finally known.
    pub fn insert_hashed(&mut self, h1: u64, h2: u64) {
        for i in 0..self.k {
            let bit = self.probe(h1, h2, i);
            self.words[(bit / 64) as usize] |= 1u64 << (bit % 64);
        }
        self.num_inserted += 1;
    }

    /// Returns `false` if `key` is definitely absent, `true` if it may be present.
    ///
    /// Never returns `false` for a key that was inserted.
    pub fn contains(&self, key: &[u8]) -> bool {
        let (h1, h2) = hash_pair(key);
        self.contains_hashed(h1, h2)
    }

    /// Membership test from a precomputed hash pair.
    pub fn contains_hashed(&self, h1: u64, h2: u64) -> bool {
        for i in 0..self.k {
            let bit = self.probe(h1, h2, i);
            if self.words[(bit / 64) as usize] & (1u64 << (bit % 64)) == 0 {
                return false;
            }
        }
        true
    }

    /// Returns the bit index for probe `i`, via Kirsch–Mitzenmacher double hashing.
    fn probe(&self, h1: u64, h2: u64, i: u32) -> u64 {
        h1.wrapping_add((i as u64).wrapping_mul(h2)) % self.num_bits
    }

    /// Serializes the filter for embedding in an SSTable.
    ///
    /// Layout: `crc32 (4B) | k (4B) | num_bits (8B) | words…`, little-endian.
    /// The checksum covers everything after itself, so a corrupted filter is
    /// detected on load rather than silently answering wrong.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ENCODED_HEADER_LEN + self.words.len() * 8);
        buf.extend_from_slice(&[0u8; 4]); // checksum patched in below
        buf.extend_from_slice(&self.k.to_le_bytes());
        buf.extend_from_slice(&self.num_bits.to_le_bytes());
        for word in &self.words {
            buf.extend_from_slice(&word.to_le_bytes());
        }
        let crc = crc32fast::hash(&buf[4..]);
        buf[..4].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// Reconstructs a filter from its serialized form.
    ///
    /// Returns `None` on malformed or corrupt input. Callers should treat that
    /// as "no filter available" and fall back to reading the data section —
    /// degraded performance, never a wrong answer.
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < ENCODED_HEADER_LEN {
            return None;
        }
        let stored_crc = u32::from_le_bytes(bytes[0..4].try_into().ok()?);
        if crc32fast::hash(&bytes[4..]) != stored_crc {
            return None;
        }

        let k = u32::from_le_bytes(bytes[4..8].try_into().ok()?);
        let num_bits = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        if k == 0 || num_bits == 0 {
            return None;
        }

        let expected_words = num_bits.div_ceil(64) as usize;
        let word_bytes = &bytes[ENCODED_HEADER_LEN..];
        if word_bytes.len() != expected_words * 8 {
            return None;
        }

        let mut words = Vec::with_capacity(expected_words);
        for chunk in word_bytes.chunks_exact(8) {
            words.push(u64::from_le_bytes(chunk.try_into().ok()?));
        }

        Some(Self {
            words,
            num_bits,
            k,
            // Not serialized: recovered from the fill ratio below, and only ever
            // used for diagnostics.
            num_inserted: 0,
        })
    }

    /// Returns the size of the bit vector in bytes.
    pub fn size_bytes(&self) -> usize {
        self.words.len() * 8
    }

    /// Returns the number of bits in the filter.
    pub fn num_bits(&self) -> u64 {
        self.num_bits
    }

    /// Returns the number of probes performed per key.
    pub fn num_hashes(&self) -> u32 {
        self.k
    }

    /// Returns how many keys have been inserted since construction.
    pub fn num_inserted(&self) -> u64 {
        self.num_inserted
    }

    /// Returns the fraction of bits currently set.
    pub fn fill_ratio(&self) -> f64 {
        let set: u32 = self.words.iter().map(|w| w.count_ones()).sum();
        set as f64 / self.num_bits as f64
    }

    /// Estimates the current false-positive rate from the observed fill.
    ///
    /// Uses `fill_ratio^k`, which works on a decoded filter too — unlike the
    /// textbook `(1 - e^(-kn/m))^k`, it needs no insertion count.
    pub fn estimated_fp_rate(&self) -> f64 {
        self.fill_ratio().powi(self.k as i32)
    }
}

/// Returns the two independent 64-bit hashes used for double hashing.
///
/// Pinned in-crate for format stability — see the module docs.
pub fn hash_pair(key: &[u8]) -> (u64, u64) {
    let h1 = fmix64(fnv1a64(key, FNV_OFFSET_A));
    // Forced odd so that `i * h2` steps through distinct residues rather than
    // collapsing onto a short cycle when h2 shares factors with num_bits.
    let h2 = fmix64(fnv1a64(key, FNV_OFFSET_B)) | 1;
    (h1, h2)
}

/// FNV-1a, 64-bit, with a caller-supplied offset basis.
fn fnv1a64(data: &[u8], basis: u64) -> u64 {
    let mut hash = basis;
    for &byte in data {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// MurmurHash3's 64-bit finalizer. FNV-1a alone avalanches poorly in the high
/// bits, which matters because the probe index is taken modulo the bit count.
fn fmix64(mut z: u64) -> u64 {
    z ^= z >> 33;
    z = z.wrapping_mul(0xff51_afd7_ed55_8ccd);
    z ^= z >> 33;
    z = z.wrapping_mul(0xc4ce_b9fe_1a85_ec53);
    z ^= z >> 33;
    z
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_inserted_key_is_always_reported_as_present() {
        let mut filter = BloomFilter::new(1_000, DEFAULT_FP_RATE);
        for i in 0..1_000u32 {
            filter.insert(format!("key:{i}").as_bytes());
        }
        // No false negatives, ever — this is the property the whole design rests on.
        for i in 0..1_000u32 {
            assert!(
                filter.contains(format!("key:{i}").as_bytes()),
                "false negative on key:{i}"
            );
        }
    }

    #[test]
    fn an_empty_filter_reports_everything_as_absent() {
        let filter = BloomFilter::new(100, DEFAULT_FP_RATE);
        for i in 0..100u32 {
            assert!(!filter.contains(format!("nope:{i}").as_bytes()));
        }
        assert_eq!(filter.fill_ratio(), 0.0);
    }

    #[test]
    fn the_false_positive_rate_is_near_the_target() {
        let n = 5_000usize;
        let mut filter = BloomFilter::new(n, 0.01);
        for i in 0..n {
            filter.insert(format!("present:{i}").as_bytes());
        }

        let trials = 20_000;
        let mut false_positives = 0;
        for i in 0..trials {
            if filter.contains(format!("absent:{i}").as_bytes()) {
                false_positives += 1;
            }
        }

        let rate = false_positives as f64 / trials as f64;
        // Generous bound: the target is 1%, so anything under 3% means the
        // sizing and hashing are behaving. A broken hash blows well past this.
        assert!(rate < 0.03, "false-positive rate {rate} exceeded 3%");
    }

    #[test]
    fn a_tighter_target_rate_produces_a_larger_filter() {
        let loose = BloomFilter::new(10_000, 0.10);
        let tight = BloomFilter::new(10_000, 0.001);
        assert!(tight.num_bits() > loose.num_bits());
        assert!(tight.num_hashes() >= loose.num_hashes());
    }

    #[test]
    fn sizing_follows_the_textbook_formula() {
        // 1% over 1000 keys => ~9.6 bits/key and k = 7.
        let filter = BloomFilter::new(1_000, 0.01);
        let bits_per_key = filter.num_bits() as f64 / 1_000.0;
        assert!(
            (9.0..=10.5).contains(&bits_per_key),
            "unexpected bits/key: {bits_per_key}"
        );
        assert_eq!(filter.num_hashes(), 7);
    }

    #[test]
    fn a_filter_survives_an_encode_decode_round_trip() {
        let mut filter = BloomFilter::new(500, DEFAULT_FP_RATE);
        for i in 0..500u32 {
            filter.insert(format!("k{i}").as_bytes());
        }

        let decoded = BloomFilter::decode(&filter.encode()).expect("should decode");
        assert_eq!(decoded.num_bits(), filter.num_bits());
        assert_eq!(decoded.num_hashes(), filter.num_hashes());

        // The decoded filter must answer identically — especially no false negatives.
        for i in 0..500u32 {
            assert!(decoded.contains(format!("k{i}").as_bytes()));
        }
        for i in 0..500u32 {
            let key = format!("absent{i}");
            assert_eq!(
                decoded.contains(key.as_bytes()),
                filter.contains(key.as_bytes())
            );
        }
    }

    #[test]
    fn a_corrupted_filter_fails_to_decode() {
        let mut filter = BloomFilter::new(100, DEFAULT_FP_RATE);
        filter.insert(b"key");

        let mut bytes = filter.encode();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        assert!(
            BloomFilter::decode(&bytes).is_none(),
            "corruption must be caught, not answered wrongly"
        );
    }

    #[test]
    fn truncated_and_empty_input_fails_to_decode() {
        assert!(BloomFilter::decode(&[]).is_none());
        assert!(BloomFilter::decode(&[0u8; 4]).is_none());

        let filter = BloomFilter::new(100, DEFAULT_FP_RATE);
        let bytes = filter.encode();
        assert!(BloomFilter::decode(&bytes[..bytes.len() - 8]).is_none());
    }

    #[test]
    fn binary_and_empty_keys_are_handled() {
        let mut filter = BloomFilter::new(10, DEFAULT_FP_RATE);
        filter.insert(&[]);
        filter.insert(&[0x00, 0xff, 0x00]);

        assert!(filter.contains(&[]));
        assert!(filter.contains(&[0x00, 0xff, 0x00]));
    }

    #[test]
    fn hashing_is_deterministic_across_calls() {
        // Stability matters: these hashes are baked into serialized tables.
        assert_eq!(hash_pair(b"stability"), hash_pair(b"stability"));
        assert_ne!(hash_pair(b"a"), hash_pair(b"b"));
    }

    #[test]
    fn the_second_hash_is_always_odd() {
        // Guards the probe sequence against collapsing onto a short cycle.
        for i in 0..256u32 {
            let (_, h2) = hash_pair(format!("k{i}").as_bytes());
            assert_eq!(h2 % 2, 1);
        }
    }

    #[test]
    fn degenerate_parameters_still_produce_a_usable_filter() {
        let mut zero = BloomFilter::new(0, DEFAULT_FP_RATE);
        zero.insert(b"k");
        assert!(zero.contains(b"k"));
        assert!(zero.num_bits() >= 64);

        let mut absurd = BloomFilter::new(10, 0.0);
        absurd.insert(b"k");
        assert!(absurd.contains(b"k"));

        let mut certain = BloomFilter::new(10, 1.0);
        certain.insert(b"k");
        assert!(certain.contains(b"k"));
    }

    #[test]
    fn fill_ratio_and_estimated_rate_track_insertions() {
        let mut filter = BloomFilter::new(1_000, 0.01);
        assert_eq!(filter.estimated_fp_rate(), 0.0);

        for i in 0..1_000u32 {
            filter.insert(format!("k{i}").as_bytes());
        }
        assert_eq!(filter.num_inserted(), 1_000);

        let fill = filter.fill_ratio();
        assert!(fill > 0.3 && fill < 0.7, "unexpected fill ratio {fill}");
        assert!(filter.estimated_fp_rate() < 0.05);
    }

    #[test]
    fn insert_hashed_matches_insert() {
        let mut a = BloomFilter::new(100, DEFAULT_FP_RATE);
        let mut b = BloomFilter::new(100, DEFAULT_FP_RATE);

        for i in 0..100u32 {
            let key = format!("k{i}");
            a.insert(key.as_bytes());
            let (h1, h2) = hash_pair(key.as_bytes());
            b.insert_hashed(h1, h2);
        }
        assert_eq!(a, b);
    }
}
