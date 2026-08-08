//! A single-writer, many-reader ordered skiplist.
//!
//! # Why this exists
//!
//! The memtable used to be a `BTreeMap`, which means every read and every write
//! has to hold a lock — and because a write also fsyncs the log, that lock is
//! held across a disk sync. Readers stall behind writers for milliseconds.
//!
//! This structure removes the reader's lock entirely. A read is pointer-chasing
//! with atomic loads: it takes no lock, blocks nothing, and cannot be blocked.
//!
//! # The concurrency contract
//!
//! - **Readers: unlimited, wait-free, no synchronization.** Any number of
//!   threads may call [`seek`](SkipList::seek), [`iter`](SkipList::iter), or the
//!   accessors concurrently with each other *and* with a writer.
//! - **Writers: exactly one at a time.** [`insert`](SkipList::insert) is
//!   `unsafe` precisely because that is a contract the type cannot check. In
//!   minidb it is discharged by the store's write mutex, which every mutation
//!   already holds in order to serialize log appends.
//!
//! Single-writer is not a limitation worked around — it is what makes this
//! tractable. A multi-writer lock-free skiplist needs a CAS retry loop at every
//! level and gets memory reclamation wrong in interesting ways. With one writer,
//! linking a node is a plain store, and the only thing that must be right is
//! *ordering*.
//!
//! # Why no memory reclamation problem
//!
//! Nodes are **never removed and never mutated after publication**. A delete is
//! a tombstone *insert*; an overwrite is a new version *insert* — both add
//! nodes. So a pointer a reader is holding can never dangle while the list is
//! alive, and the whole list is freed at once in `Drop`.
//!
//! Lifetime is then just `Arc`: readers hold an `Arc<MemTable>`, and the
//! allocation outlives the last of them. This is the reason the classic
//! epoch/hazard-pointer machinery is absent — it is not needed, not skipped.
//!
//! # Why the ordering is correct
//!
//! A node is fully initialized *before* any pointer to it is published, and each
//! `next` pointer is published with a `Release` store that readers load with
//! `Acquire`. So a reader that observes the pointer necessarily observes the
//! node's key and value.
//!
//! Nodes are linked **bottom level first**. A reader scanning an upper level may
//! therefore miss a node that is not linked there yet — and that is harmless,
//! because it descends and finds it at level 0, which is linked first. The
//! reverse order would be the bug: a node reachable at level 3 but not level 0
//! would break the iteration order.
//!
//! The list height is raised before the new levels are linked. A reader that
//! reads the new height sees a null pointer at the new top level and simply
//! drops down a level. Also harmless.

use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize, Ordering};

use crate::memtable::{Entry, InternalKey};

/// Maximum number of levels. 12 levels at a branching factor of 4 indexes about
/// 4^12 ≈ 16M entries, far beyond any memtable that would be flushed.
const MAX_HEIGHT: usize = 12;

/// One in `BRANCHING` nodes is promoted to each successive level.
const BRANCHING: u64 = 4;

/// A published list node.
///
/// `key` and `entry` are immutable for the node's whole life; only `next` is
/// ever written, and only by the single writer.
struct Node {
    key: InternalKey,
    entry: Entry,
    next: Box<[AtomicPtr<Node>]>,
}

/// An ordered map from [`InternalKey`] to [`Entry`], insert-only.
pub struct SkipList {
    /// Level-0..MAX_HEIGHT head pointers. The list has no head *node*, just
    /// these slots, so a node never has to special-case being first.
    head: Box<[AtomicPtr<Node>]>,
    /// Highest level currently in use. Only grows.
    height: AtomicUsize,
    len: AtomicUsize,
    size_bytes: AtomicUsize,
    max_seq: AtomicU64,
    /// xorshift64* state for level selection. Writer-only; an atomic purely so
    /// `insert` can take `&self`.
    rng: AtomicU64,
}

// SAFETY: the raw pointers are into nodes owned by this list and freed only in
// `Drop`, which takes `&mut self` and therefore cannot race a reader. `Node` is
// `Send + Sync` in substance — `InternalKey` and `Entry` are plain owned data,
// and every field but `next` is immutable after publication.
unsafe impl Send for SkipList {}
unsafe impl Sync for SkipList {}

impl SkipList {
    /// Creates an empty list.
    pub fn new() -> Self {
        Self {
            head: (0..MAX_HEIGHT)
                .map(|_| AtomicPtr::new(ptr::null_mut()))
                .collect(),
            height: AtomicUsize::new(1),
            len: AtomicUsize::new(0),
            size_bytes: AtomicUsize::new(0),
            max_seq: AtomicU64::new(0),
            // Any odd non-zero seed works; a fixed one keeps level selection
            // reproducible, which makes failures reproducible too.
            rng: AtomicU64::new(0x2545_F491_4F6C_DD1D),
        }
    }

    /// Inserts `entry` at `key`.
    ///
    /// # Safety
    ///
    /// **At most one thread may be inside this function at a time.** Concurrent
    /// readers are fine and are the entire point; concurrent *writers* would
    /// interleave their pointer updates and corrupt the list.
    ///
    /// Callers discharge this by holding an exclusive lock across the call.
    /// [`insert_exclusive`](Self::insert_exclusive) is the safe form for callers
    /// that hold `&mut self`, where the borrow checker proves it.
    ///
    /// Keys must be unique. Sequence numbers are unique per mutation, so this
    /// holds by construction; inserting a duplicate would leave an unreachable
    /// node rather than corrupting anything.
    pub unsafe fn insert(&self, key: InternalKey, entry: Entry) {
        let bytes = key.size_bytes() + entry.len();
        let seq = key.seq;

        // Find, for every level, the node the new one will be spliced after.
        // `None` means "the head slot", i.e. the new node goes first.
        let mut prev: [Option<*mut Node>; MAX_HEIGHT] = [None; MAX_HEIGHT];
        let height = self.height.load(Ordering::Acquire);
        let mut cursor: Option<*mut Node> = None;

        for level in (0..height).rev() {
            cursor = unsafe { self.scan_level(cursor, level, &key) };
            prev[level] = cursor;
        }
        // Levels above the current height stay `None`: nothing is linked there,
        // so the new node goes directly after the head slot.

        let new_height = self.random_height();
        if new_height > height {
            // Raise the ceiling before linking into the new levels. A reader
            // that sees the new height finds null there and drops down a level.
            self.height.store(new_height, Ordering::Release);
        }

        let node = Box::into_raw(Box::new(Node {
            key,
            entry,
            next: (0..new_height)
                .map(|_| AtomicPtr::new(ptr::null_mut()))
                .collect(),
        }));

        // Link bottom-up. Level 0 defines the iteration order, so it must be
        // linked first: a node reachable from an upper level but not from level
        // 0 would be skipped by every scan that descends.
        for (level, slot) in prev.iter().enumerate().take(new_height) {
            let slot = unsafe { self.next_slot(*slot, level) };
            // The new node's forward pointer is set while it is still private,
            // so a plain store suffices.
            unsafe { &*node }.next[level].store(slot.load(Ordering::Acquire), Ordering::Relaxed);
            // Release: everything written above happens-before any reader that
            // acquires this pointer.
            slot.store(node, Ordering::Release);
        }

        self.len.fetch_add(1, Ordering::Relaxed);
        self.size_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.max_seq.fetch_max(seq, Ordering::Relaxed);
    }

    /// Inserts through an exclusive borrow.
    ///
    /// The safe form of [`insert`](Self::insert): `&mut self` is proof that no
    /// other writer exists.
    pub fn insert_exclusive(&mut self, key: InternalKey, entry: Entry) {
        // SAFETY: `&mut self` guarantees no concurrent writer.
        unsafe { self.insert(key, entry) }
    }

    /// Walks `level` forward from `cursor` while the next key is `< key`.
    ///
    /// # Safety
    ///
    /// `cursor` must be null or a node belonging to this list.
    unsafe fn scan_level(
        &self,
        mut cursor: Option<*mut Node>,
        level: usize,
        key: &InternalKey,
    ) -> Option<*mut Node> {
        loop {
            let next = unsafe { self.next_slot(cursor, level) }.load(Ordering::Acquire);
            if next.is_null() {
                return cursor;
            }
            // Acquire above pairs with the Release publish, so the key is fully
            // visible here.
            if unsafe { &*next }.key < *key {
                cursor = Some(next);
            } else {
                return cursor;
            }
        }
    }

    /// The forward-pointer slot at `level` for `node`, or the head slot.
    ///
    /// # Safety
    ///
    /// `node` must be null or a node of this list with at least `level + 1`
    /// levels.
    unsafe fn next_slot(&self, node: Option<*mut Node>, level: usize) -> &AtomicPtr<Node> {
        match node {
            None => &self.head[level],
            Some(p) => &unsafe { &*p }.next[level],
        }
    }

    /// Returns the first entry whose key is `>= key`.
    ///
    /// Wait-free and lock-free: no allocation, no synchronization, no blocking.
    pub fn seek(&self, key: &InternalKey) -> Option<(&InternalKey, &Entry)> {
        let node = self.seek_node(key)?;
        // SAFETY: nodes live as long as the list, and this borrow is tied to
        // `&self`.
        let node = unsafe { &*node };
        Some((&node.key, &node.entry))
    }

    /// Returns the first node whose key is `>= key`, as a raw pointer.
    fn seek_node(&self, key: &InternalKey) -> Option<*mut Node> {
        let mut cursor: Option<*mut Node> = None;
        for level in (0..self.height.load(Ordering::Acquire)).rev() {
            // SAFETY: `cursor` is always null or a node of this list.
            cursor = unsafe { self.scan_level(cursor, level, key) };
        }
        // SAFETY: as above.
        let next = unsafe { self.next_slot(cursor, 0) }.load(Ordering::Acquire);
        (!next.is_null()).then_some(next)
    }

    /// Iterates over every entry in ascending key order.
    pub fn iter(&self) -> SkipIter<'_> {
        SkipIter {
            next: self.head[0].load(Ordering::Acquire),
            _list: PhantomData,
        }
    }

    /// Iterates from the first entry whose key is `>= start`.
    pub fn iter_from(&self, start: &InternalKey) -> SkipIter<'_> {
        SkipIter {
            next: self.seek_node(start).unwrap_or(ptr::null_mut()),
            _list: PhantomData,
        }
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.len.load(Ordering::Relaxed)
    }

    /// Whether the list holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Approximate heap footprint of stored keys and values, in bytes.
    pub fn size_bytes(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// Highest sequence number inserted, or 0 if empty.
    pub fn max_seq(&self) -> u64 {
        self.max_seq.load(Ordering::Relaxed)
    }

    /// Number of levels currently in use.
    pub fn height(&self) -> usize {
        self.height.load(Ordering::Relaxed)
    }

    /// Picks a level count: 1, plus one more with probability `1/BRANCHING` each.
    fn random_height(&self) -> usize {
        // xorshift64*, written out rather than pulled from a crate so the
        // structure's shape is fixed by this file.
        let mut x = self.rng.load(Ordering::Relaxed);
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.rng.store(x, Ordering::Relaxed);
        let mut r = x.wrapping_mul(0x2545_F491_4F6C_DD1D);

        let mut height = 1;
        while height < MAX_HEIGHT && r.is_multiple_of(BRANCHING) {
            height += 1;
            r /= BRANCHING;
        }
        height
    }
}

impl Default for SkipList {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for SkipList {
    fn drop(&mut self) {
        // `&mut self` proves no reader holds a borrow, so freeing the whole
        // chain at once is sound. Walking level 0 reaches every node.
        let mut current = self.head[0].load(Ordering::Relaxed);
        while !current.is_null() {
            // SAFETY: every node was created by `Box::into_raw` in `insert` and
            // is reachable from level 0 exactly once.
            let owned = unsafe { Box::from_raw(current) };
            current = owned.next[0].load(Ordering::Relaxed);
        }
    }
}

impl std::fmt::Debug for SkipList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SkipList")
            .field("len", &self.len())
            .field("height", &self.height())
            .field("size_bytes", &self.size_bytes())
            .finish()
    }
}

/// Forward iterator over a [`SkipList`].
///
/// Safe to hold while a writer inserts: it walks level-0 pointers, and a node
/// linked after the iterator has passed its position is simply not seen. Nodes
/// are never removed, so what it does see is always valid.
pub struct SkipIter<'a> {
    next: *mut Node,
    _list: PhantomData<&'a SkipList>,
}

impl<'a> Iterator for SkipIter<'a> {
    type Item = (&'a InternalKey, &'a Entry);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next.is_null() {
            return None;
        }
        // SAFETY: nodes outlive the borrow of the list this iterator carries.
        let node = unsafe { &*self.next };
        self.next = node.next[0].load(Ordering::Acquire);
        Some((&node.key, &node.entry))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;

    fn key(k: &str, seq: u64) -> InternalKey {
        InternalKey::new(k.as_bytes().to_vec(), seq)
    }

    fn value(v: &str) -> Entry {
        Entry::Value(v.as_bytes().to_vec())
    }

    fn keys_of(list: &SkipList) -> Vec<(String, u64)> {
        list.iter()
            .map(|(k, _)| (String::from_utf8(k.user_key.clone()).unwrap(), k.seq))
            .collect()
    }

    #[test]
    fn an_empty_list_finds_nothing() {
        let list = SkipList::new();
        assert!(list.is_empty());
        assert_eq!(list.len(), 0);
        assert!(list.seek(&key("anything", 1)).is_none());
        assert_eq!(list.iter().count(), 0);
    }

    #[test]
    fn entries_iterate_in_internal_key_order_regardless_of_insertion_order() {
        let mut list = SkipList::new();
        for (k, seq) in [("delta", 1), ("alpha", 3), ("charlie", 2), ("alpha", 9)] {
            list.insert_exclusive(key(k, seq), value("x"));
        }
        assert_eq!(
            keys_of(&list),
            vec![
                ("alpha".into(), 9),
                ("alpha".into(), 3),
                ("charlie".into(), 2),
                ("delta".into(), 1),
            ],
            "user key ascending, sequence descending"
        );
    }

    #[test]
    fn seek_lands_on_the_first_key_at_or_after_the_probe() {
        let mut list = SkipList::new();
        for (k, seq) in [("a", 10), ("a", 5), ("c", 7)] {
            list.insert_exclusive(key(k, seq), value(k));
        }

        // Sequence descends, so probing ("a", 7) skips ("a", 10) and lands on
        // ("a", 5) — the newest version at or below the probe.
        let (found, _) = list.seek(&key("a", 7)).unwrap();
        assert_eq!((found.user_key.as_slice(), found.seq), (&b"a"[..], 5));

        // A probe below every version of "a" runs off into the next key.
        let (found, _) = list.seek(&key("a", 1)).unwrap();
        assert_eq!(found.user_key.as_slice(), b"c");

        // A probe past the end finds nothing.
        assert!(list.seek(&key("z", 0)).is_none());
    }

    #[test]
    fn seek_finds_an_exact_key() {
        let mut list = SkipList::new();
        for i in 0..500u32 {
            list.insert_exclusive(key(&format!("k{i:04}"), i as u64 + 1), value("v"));
        }
        for i in 0..500u32 {
            let probe = key(&format!("k{i:04}"), i as u64 + 1);
            let (found, entry) = list.seek(&probe).expect("every key must be findable");
            assert_eq!(*found, probe);
            assert_eq!(*entry, value("v"));
        }
    }

    #[test]
    fn iter_from_starts_at_the_probe_position() {
        let mut list = SkipList::new();
        for k in ["a", "b", "c", "d"] {
            list.insert_exclusive(key(k, 1), value(k));
        }
        let seen: Vec<_> = list
            .iter_from(&key("c", u64::MAX))
            .map(|(k, _)| String::from_utf8(k.user_key.clone()).unwrap())
            .collect();
        assert_eq!(seen, vec!["c", "d"]);

        assert_eq!(list.iter_from(&key("zz", 0)).count(), 0);
    }

    #[test]
    fn a_thousand_shuffled_inserts_come_back_sorted() {
        let mut list = SkipList::new();
        // Deterministic scatter: a stride coprime with 1000 visits every slot.
        let mut i = 0u64;
        for _ in 0..1_000 {
            i = (i + 337) % 1_000;
            list.insert_exclusive(key(&format!("k{i:04}"), 1), value("v"));
        }
        assert_eq!(list.len(), 1_000);

        let keys = keys_of(&list);
        assert_eq!(keys.len(), 1_000);
        assert!(
            keys.windows(2).all(|w| w[0] < w[1]),
            "iteration must be strictly ascending"
        );
    }

    #[test]
    fn the_list_grows_past_one_level() {
        let mut list = SkipList::new();
        for i in 0..2_000u32 {
            list.insert_exclusive(key(&format!("k{i:05}"), 1), value("v"));
        }
        assert!(
            list.height() > 1,
            "2000 entries should promote some nodes above level 0 (height {})",
            list.height()
        );
        assert!(list.height() <= MAX_HEIGHT);
    }

    #[test]
    fn accounting_tracks_size_and_the_highest_sequence() {
        let mut list = SkipList::new();
        list.insert_exclusive(key("abc", 4), value("de")); // 3 + 8 + 2
        assert_eq!(list.size_bytes(), 13);
        assert_eq!(list.max_seq(), 4);

        list.insert_exclusive(key("abc", 2), Entry::Tombstone); // 3 + 8 + 0
        assert_eq!(list.size_bytes(), 24);
        assert_eq!(list.max_seq(), 4, "max, not last");
    }

    #[test]
    fn readers_see_a_consistent_list_while_a_writer_inserts() {
        // The property this whole module exists for: reads complete correctly
        // and without blocking while writes are landing.
        let list = Arc::new(SkipList::new());
        let stop = Arc::new(AtomicBool::new(false));

        // Pre-populate, so readers have something to find from the start.
        // SAFETY: no other thread exists yet, so this is trivially the only
        // writer.
        for i in 0..100u32 {
            unsafe { list.insert(key(&format!("k{i:05}"), 1), value("seed")) };
        }

        let readers: Vec<_> = (0..4)
            .map(|_| {
                let list = Arc::clone(&list);
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    let mut reads = 0u64;
                    while !stop.load(Ordering::Relaxed) {
                        for i in 0..100u32 {
                            let probe = key(&format!("k{i:05}"), 1);
                            let (found, entry) =
                                list.seek(&probe).expect("a seeded key vanished mid-write");
                            assert_eq!(*found, probe);
                            assert_eq!(*entry, value("seed"));
                            reads += 1;
                        }
                        // Iteration must stay sorted even mid-insert.
                        let mut last: Option<InternalKey> = None;
                        for (k, _) in list.iter() {
                            if let Some(prev) = &last {
                                assert!(prev < k, "iteration order broke under a concurrent write");
                            }
                            last = Some(k.clone());
                        }
                    }
                    reads
                })
            })
            .collect();

        // The single writer. `insert` is unsafe because only one thread may be
        // in it; that is satisfied here by there being exactly one writer.
        let writer = {
            let list = Arc::clone(&list);
            std::thread::spawn(move || {
                for i in 0..20_000u32 {
                    unsafe { list.insert(key(&format!("w{i:06}"), i as u64 + 2), value("new")) };
                }
            })
        };

        writer.join().unwrap();
        stop.store(true, Ordering::Relaxed);

        let total: u64 = readers.into_iter().map(|h| h.join().unwrap()).sum();
        assert!(
            total > 10_000,
            "readers should have completed many reads during the writes, got {total}"
        );
        assert_eq!(list.len(), 100 + 20_000);
    }

    #[test]
    fn dropping_a_populated_list_frees_every_node() {
        // Nothing to assert directly; this exists so the leak/UB checkers in CI
        // and `cargo test` under Miri have a case that exercises Drop.
        let mut list = SkipList::new();
        for i in 0..5_000u32 {
            list.insert_exclusive(key(&format!("k{i:05}"), i as u64), value("payload"));
        }
        assert_eq!(list.len(), 5_000);
        drop(list);
    }
}
