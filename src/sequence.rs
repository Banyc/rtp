//! Wrapping sequence numbers.
//!
//! A wrapping `u64` has no global total order.  Every live protocol window
//! is bounded to fewer than `2^63` packets, so all ordering decisions must
//! be made as *forward offsets from a window anchor*; raw integer order is
//! permitted only as a private `BTreeMap` storage detail inside
//! [`SequenceMap`].
//!
//! This module is the only place allowed to inspect sequence integers:
//! [`SequenceNumber`] exposes no `Ord`, and every other module of the crate
//! compares sequences through the helpers here (`lt`/`le`/`gt`/`ge`,
//! `max`/`min`) or through [`SequenceWindow::classify`].

use std::collections::{BTreeMap, VecDeque};
use std::fmt;
use std::ops::Bound::{Excluded, Included, Unbounded};

/// Half the sequence space.  Forward distances at or above this value are
/// ambiguous or stale and must never be treated as "ahead".
pub(crate) const HALF_SEQUENCE_SPACE: u64 = 1 << 63;

/// An opaque wrapping packet sequence number.
///
/// Deliberately without `Ord`/`PartialOrd`: a wrapping sequence space has
/// no total order, and exposing one would let callers misclassify the
/// `u64::MAX → 0` transition.  All ordering goes through the window-aware
/// helpers in this module.
#[repr(transparent)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub struct SequenceNumber(u64);

impl SequenceNumber {
    pub(crate) const ZERO: Self = Self(0);

    /// Recover the sequence from its wire (on-the-wire big-endian u64) form.
    pub(crate) fn from_wire(v: u64) -> Self {
        Self(v)
    }

    /// The raw value as it appears on the wire.
    pub(crate) fn to_wire(self) -> u64 {
        self.0
    }

    /// Advance by `n` with wrapping arithmetic.
    pub(crate) fn advance(self, n: u64) -> Self {
        Self(self.0.wrapping_add(n))
    }

    /// Retreat by `n` with wrapping arithmetic.
    pub(crate) fn retreat(self, n: u64) -> Self {
        Self(self.0.wrapping_sub(n))
    }

    /// Forward distance from `self` to `later`, computed with wrapping
    /// arithmetic.  The result is a raw offset, meaningful only relative to
    /// a live window anchor.
    pub(crate) fn forward_distance_to(self, later: Self) -> u64 {
        later.0.wrapping_sub(self.0)
    }
}

impl fmt::Display for SequenceNumber {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Directional initial sequences for a connection, derived from the opening
/// handshake nonce (or zero for connections opened without the handshake).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct InitialSequences {
    pub(crate) send: SequenceNumber,
    pub(crate) recv: SequenceNumber,
}

impl InitialSequences {
    pub(crate) const ZERO: Self = Self {
        send: SequenceNumber::ZERO,
        recv: SequenceNumber::ZERO,
    };

    /// Client view: sends `client_to_server` sequences and expects to
    /// receive `server_to_client` sequences.
    pub(crate) fn client(
        client_to_server: SequenceNumber,
        server_to_client: SequenceNumber,
    ) -> Self {
        Self {
            send: client_to_server,
            recv: server_to_client,
        }
    }

    /// Server view: role-inverted — sends `server_to_client` sequences and
    /// expects to receive `client_to_server` sequences.
    pub(crate) fn server(
        client_to_server: SequenceNumber,
        server_to_client: SequenceNumber,
    ) -> Self {
        Self {
            send: server_to_client,
            recv: client_to_server,
        }
    }
}

/// Classification of a sequence number relative to a [`SequenceWindow`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SequencePosition {
    /// Inside the live window; carries the forward offset from the anchor.
    InWindow(u64),
    /// Logically behind the window (more than half a space behind the
    /// anchor): already passed.
    Stale,
    /// More than half a space ahead of the anchor: ambiguous at best.
    Ambiguous,
    /// Ahead of the live window but within the forward half space.
    TooFarAhead,
}

/// A bounded live window over the wrapping sequence space: `[anchor,
/// anchor + limit)` with `0 < limit < HALF_SEQUENCE_SPACE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct SequenceWindow {
    anchor: SequenceNumber,
    limit: u64,
}

impl SequenceWindow {
    pub(crate) fn new(anchor: SequenceNumber, limit: u64) -> Self {
        assert!(
            0 < limit && limit < HALF_SEQUENCE_SPACE,
            "a live sequence window must be nonempty and smaller than half the space"
        );
        Self { anchor, limit }
    }

    pub(crate) fn anchor(&self) -> SequenceNumber {
        self.anchor
    }

    pub(crate) fn limit(&self) -> u64 {
        self.limit
    }

    /// Classify `seq` by its forward distance from the anchor:
    /// - `< limit` → live (`InWindow(offset)`),
    /// - `== HALF_SEQUENCE_SPACE` → ambiguous,
    /// - `> HALF_SEQUENCE_SPACE` → stale,
    /// - the remaining forward half (`limit..HALF_SEQUENCE_SPACE`) → too far
    ///   ahead.
    pub(crate) fn classify(&self, seq: SequenceNumber) -> SequencePosition {
        let forward = self.anchor.forward_distance_to(seq);
        if forward < self.limit {
            SequencePosition::InWindow(forward)
        } else if forward == HALF_SEQUENCE_SPACE {
            SequencePosition::Ambiguous
        } else if forward > HALF_SEQUENCE_SPACE {
            SequencePosition::Stale
        } else {
            SequencePosition::TooFarAhead
        }
    }

    pub(crate) fn contains(&self, seq: SequenceNumber) -> bool {
        matches!(self.classify(seq), SequencePosition::InWindow(_))
    }
}

/// Raw storage key: the only place raw integer order is meaningful (a
/// private `BTreeMap` detail).  Alone derives `Ord`.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
struct RawSequenceKey(u64);

/// A map of sequence-number → value, keyed by raw `u64` but ordered
/// logically (wrapping) through the window anchor.
///
/// Every retained key is inside the live window; insertion outside the
/// window is rejected.
#[derive(Debug, Clone)]
pub(crate) struct SequenceMap<V> {
    window: SequenceWindow,
    inner: BTreeMap<RawSequenceKey, V>,
}

impl<V> SequenceMap<V> {
    pub(crate) fn new(anchor: SequenceNumber, limit: u64) -> Self {
        Self {
            window: SequenceWindow::new(anchor, limit),
            inner: BTreeMap::new(),
        }
    }

    pub(crate) fn window(&self) -> &SequenceWindow {
        &self.window
    }

    pub(crate) fn len(&self) -> usize {
        self.inner.len()
    }

    /// Insert `value` at `seq`.  Returns the previous value when the key was
    /// already present; `None` when the key is outside the live window (the
    /// insertion is rejected) or when the key was absent.
    pub(crate) fn insert(&mut self, seq: SequenceNumber, value: V) -> Option<V> {
        if !self.window.contains(seq) {
            return None;
        }
        self.inner.insert(RawSequenceKey(seq.to_wire()), value)
    }

    pub(crate) fn remove(&mut self, seq: &SequenceNumber) -> Option<V> {
        self.inner.remove(&RawSequenceKey(seq.to_wire()))
    }

    pub(crate) fn get(&self, seq: &SequenceNumber) -> Option<&V> {
        self.inner.get(&RawSequenceKey(seq.to_wire()))
    }

    pub(crate) fn contains_key(&self, seq: &SequenceNumber) -> bool {
        self.inner.contains_key(&RawSequenceKey(seq.to_wire()))
    }

    /// Logical iteration: chain `[anchor..]` then `[..anchor)` exactly once.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (SequenceNumber, &V)> + '_ {
        let anchor = RawSequenceKey(self.window.anchor().to_wire());
        self.inner
            .range(anchor..)
            .chain(self.inner.range(..anchor))
            .map(|(k, v)| (SequenceNumber::from_wire(k.0), v))
    }

    /// Logical iteration starting at `start`: only the suffix at/after
    /// `start`, never circling back.
    pub(crate) fn iter_from(
        &self,
        start: SequenceNumber,
    ) -> impl Iterator<Item = (SequenceNumber, &V)> + '_ {
        let start_raw = RawSequenceKey(start.to_wire());
        let anchor_raw = RawSequenceKey(self.window.anchor().to_wire());
        // The wrapped low segment `[..anchor)` is only part of the suffix
        // when `start` sits at/after the anchor physically.
        let wrapped_tail = (start_raw >= anchor_raw).then(|| self.inner.range(..anchor_raw));
        let head = if start_raw >= anchor_raw {
            // `start` is at/after the anchor physically: the suffix is the
            // physical tail `[start..]` followed by the wrapped low segment.
            self.inner.range(start_raw..)
        } else {
            // `start` is inside the wrapped low segment: the suffix is the
            // physical range `[start..anchor)` — never circles back.
            self.inner.range(start_raw..anchor_raw)
        };
        head.chain(wrapped_tail.into_iter().flatten())
            .map(|(k, v)| (SequenceNumber::from_wire(k.0), v))
    }

    /// Greatest retained key logically before `seq`, crossing the physical
    /// zero boundary when required.
    pub(crate) fn predecessor(&self, seq: SequenceNumber) -> Option<(SequenceNumber, &V)> {
        let anchor = RawSequenceKey(self.window.anchor().0);
        let seq = RawSequenceKey(seq.0);
        let found = if seq >= anchor {
            // Keys before `seq` are the high keys in `[anchor, seq)`; the
            // wrapped low segment `[..anchor)` is logically *after* `seq`.
            self.inner
                .range((Included(anchor), Excluded(seq)))
                .next_back()
        } else {
            self.inner
                .range((Unbounded, Excluded(seq)))
                .next_back()
                .or_else(|| self.inner.range((Included(anchor), Unbounded)).next_back())
        }?;
        Some((SequenceNumber(found.0.0), found.1))
    }

    /// Smallest retained key logically after `seq`, crossing the physical
    /// zero boundary when required.
    pub(crate) fn successor(&self, seq: SequenceNumber) -> Option<(SequenceNumber, &V)> {
        let anchor = RawSequenceKey(self.window.anchor().0);
        let seq = RawSequenceKey(seq.0);
        let found = if seq >= anchor {
            self.inner
                .range((Excluded(seq), Unbounded))
                .next()
                .or_else(|| self.inner.range((Unbounded, Excluded(anchor))).next())
        } else {
            self.inner.range((Excluded(seq), Excluded(anchor))).next()
        }?;
        Some((SequenceNumber(found.0.0), found.1))
    }

    /// Move the window anchor, retaining only keys that stay inside the new
    /// live window.
    pub(crate) fn move_anchor(&mut self, new_anchor: SequenceNumber) {
        let new_window = SequenceWindow::new(new_anchor, self.window.limit());
        self.inner
            .retain(|k, _| new_window.contains(SequenceNumber::from_wire(k.0)));
        self.window = new_window;
    }
}

/// A send window: a contiguous sequence-number range backed by a
/// `VecDeque`, where the sequence of queue entry `i` is
/// `start.advance(i)`.
#[derive(Debug, Clone)]
pub(crate) struct SendWindow<V> {
    start: SequenceNumber,
    next: SequenceNumber,
    queue: VecDeque<V>,
}

impl<V> SendWindow<V> {
    pub(crate) fn new(start: SequenceNumber) -> Self {
        Self {
            start,
            next: start,
            queue: VecDeque::new(),
        }
    }

    pub(crate) fn start(&self) -> SequenceNumber {
        self.start
    }

    pub(crate) fn next(&self) -> SequenceNumber {
        self.next
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    pub(crate) fn len(&self) -> usize {
        self.queue.len()
    }

    /// Assign the next sequence number (advancing `next` with wrapping
    /// arithmetic) and enqueue `value` at that sequence.
    pub(crate) fn push(&mut self, value: V) {
        self.queue.push_back(value);
        self.next = self.next.advance(1);
    }

    /// Map `seq` to its `VecDeque` index via the forward distance from
    /// `start`.  Sequences outside the window (stale or too far ahead) map
    /// to an out-of-range index and yield `None`.
    pub(crate) fn get(&self, seq: &SequenceNumber) -> Option<&V> {
        let idx = self.start.forward_distance_to(*seq);
        if idx >= self.len() as u64 {
            return None;
        }
        self.queue.get(idx as usize)
    }

    pub(crate) fn get_mut(&mut self, seq: &SequenceNumber) -> Option<&mut V> {
        let idx = self.start.forward_distance_to(*seq);
        if idx >= self.queue.len() as u64 {
            return None;
        }
        self.queue.get_mut(idx as usize)
    }

    /// Pop the front entry, advancing `start` with wrapping arithmetic.
    pub(crate) fn pop(&mut self) -> Option<V> {
        let v = self.queue.pop_front()?;
        self.start = self.start.advance(1);
        Some(v)
    }

    /// Iterate by total local offsets: entry `i` carries sequence
    /// `start.advance(i)`.
    pub(crate) fn iter(&self) -> impl Iterator<Item = (SequenceNumber, &V)> + '_ {
        self.queue
            .iter()
            .enumerate()
            .map(|(i, v)| (self.start.advance(i as u64), v))
    }

    pub(crate) fn iter_mut(&mut self) -> impl Iterator<Item = (SequenceNumber, &mut V)> + '_ {
        self.queue
            .iter_mut()
            .enumerate()
            .map(|(i, v)| (self.start.advance(i as u64), v))
    }
}

impl<V> SendWindow<Option<V>> {
    /// Remove the acknowledged empty prefix (`None` entries at the head) and
    /// return the number of entries removed.  Liveness accounting must use
    /// this count rather than comparing wrapped numeric values.
    pub(crate) fn pop_none(&mut self) -> usize {
        let mut removed = 0;
        while self.queue.front().is_some_and(Option::is_none) {
            self.pop();
            removed += 1;
        }
        removed
    }
}

/// Strictly-less-than within a live window (all compared values within
/// `HALF_SEQUENCE_SPACE` of one another).
pub(crate) fn lt(a: SequenceNumber, b: SequenceNumber) -> bool {
    a != b && a.forward_distance_to(b) < HALF_SEQUENCE_SPACE
}

/// Less-than-or-equal within a live window.
pub(crate) fn le(a: SequenceNumber, b: SequenceNumber) -> bool {
    a == b || a.forward_distance_to(b) < HALF_SEQUENCE_SPACE
}

/// The lesser of two in-window sequences.
pub(crate) fn min(a: SequenceNumber, b: SequenceNumber) -> SequenceNumber {
    if le(a, b) { a } else { b }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seq(n: u64) -> SequenceNumber {
        SequenceNumber::from_wire(n)
    }

    #[test]
    fn forward_distance_is_wrapping() {
        let a = seq(u64::MAX - 1);
        let b = seq(1);
        assert_eq!(a.forward_distance_to(b), 3);
        assert_eq!(b.forward_distance_to(a), u64::MAX - 2);
    }

    #[test]
    fn advance_and_retreat_wrap() {
        let a = seq(u64::MAX);
        assert_eq!(a.advance(1), seq(0));
        assert_eq!(seq(0).retreat(1), seq(u64::MAX));
        assert_eq!(a.advance(2), seq(1));
    }

    #[test]
    fn position_distinguishes_live_stale_and_future_values() {
        let window = SequenceWindow::new(seq(100), 10);
        assert_eq!(window.classify(seq(100)), SequencePosition::InWindow(0));
        assert_eq!(window.classify(seq(109)), SequencePosition::InWindow(9));
        // Too far ahead: within the forward half, past the window.
        assert!(matches!(
            window.classify(seq(120)),
            SequencePosition::TooFarAhead
        ));
        // Exactly half a space ahead: ambiguous.
        assert_eq!(
            window.classify(seq(100).advance(HALF_SEQUENCE_SPACE)),
            SequencePosition::Ambiguous
        );
        // More than half a space behind: stale.
        assert_eq!(window.classify(seq(99)), SequencePosition::Stale);
        // Wrapped-ahead values are live when they land inside the window.
        // (anchor = u64::MAX - 1, so seq 0 sits at forward offset 2.)
        let wrapped = SequenceWindow::new(seq(u64::MAX - 1), 10);
        assert_eq!(wrapped.classify(seq(0)), SequencePosition::InWindow(2));
        assert_eq!(wrapped.classify(seq(7)), SequencePosition::InWindow(9));
        assert!(matches!(
            wrapped.classify(seq(8)),
            SequencePosition::TooFarAhead
        ));
        assert_eq!(wrapped.classify(seq(u64::MAX - 2)), SequencePosition::Stale);
    }

    #[test]
    fn sequence_map_rejects_insertion_outside_the_window() {
        let mut map: SequenceMap<u32> = SequenceMap::new(seq(10), 5);
        assert!(map.insert(seq(12), 1).is_none());
        assert_eq!(map.get(&seq(12)), Some(&1));
        assert!(
            map.insert(seq(5), 2).is_none(),
            "stale insert must be rejected"
        );
        assert!(
            map.insert(seq(16), 3).is_none(),
            "too-far insert must be rejected"
        );
        assert_eq!(map.len(), 1);
        // Replacing an existing key returns the previous value.
        assert_eq!(map.insert(seq(12), 4), Some(1));
        assert_eq!(map.get(&seq(12)), Some(&4));
    }

    #[test]
    fn sequence_map_iterates_the_high_then_low_storage_segments_once() {
        // Anchor in the high segment; keys straddle the physical zero.
        let mut map: SequenceMap<u32> = SequenceMap::new(seq(u64::MAX - 2), 8);
        for (s, v) in [
            (u64::MAX - 2, 1),
            (u64::MAX - 1, 2),
            (u64::MAX, 3),
            (0, 4),
            (1, 5),
        ] {
            assert!(map.insert(seq(s), v).is_none());
        }
        let logical: Vec<u64> = map.iter().map(|(k, _)| k.to_wire()).collect();
        assert_eq!(
            logical,
            vec![u64::MAX - 2, u64::MAX - 1, u64::MAX, 0, 1],
            "logical iteration must chain [anchor..] then [..anchor) exactly once"
        );
        // Predecessor/successor cross the physical zero boundary.
        assert_eq!(
            map.predecessor(seq(0)).map(|(sequence, _)| sequence),
            Some(seq(u64::MAX))
        );
        assert_eq!(
            map.predecessor(seq(1)).map(|(sequence, _)| sequence),
            Some(seq(0))
        );
        assert_eq!(
            map.successor(seq(u64::MAX)).map(|(sequence, _)| sequence),
            Some(seq(0))
        );
        assert_eq!(map.predecessor(seq(u64::MAX - 2)), None);
        assert_eq!(map.successor(seq(1)), None);
    }

    #[test]
    fn sequence_map_iter_from_returns_only_the_logical_suffix() {
        let mut map: SequenceMap<u32> = SequenceMap::new(seq(u64::MAX - 2), 8);
        for (s, v) in [
            (u64::MAX - 2, 1),
            (u64::MAX - 1, 2),
            (u64::MAX, 3),
            (0, 4),
            (1, 5),
        ] {
            map.insert(seq(s), v);
        }
        let from_mid: Vec<u64> = map
            .iter_from(seq(u64::MAX))
            .map(|(k, _)| k.to_wire())
            .collect();
        assert_eq!(from_mid, vec![u64::MAX, 0, 1]);
        let from_low: Vec<u64> = map.iter_from(seq(0)).map(|(k, _)| k.to_wire()).collect();
        assert_eq!(from_low, vec![0, 1], "the suffix must never circle back");
        let from_anchor: Vec<u64> = map
            .iter_from(seq(u64::MAX - 2))
            .map(|(k, _)| k.to_wire())
            .collect();
        assert_eq!(from_anchor.len(), 5);
    }

    #[test]
    fn move_anchor_retains_only_in_window_keys() {
        let mut map: SequenceMap<u32> = SequenceMap::new(seq(10), 100);
        for s in 20..30 {
            map.insert(seq(s), s as u32);
        }
        map.move_anchor(seq(25));
        let keys: Vec<u64> = map.iter().map(|(k, _)| k.to_wire()).collect();
        assert_eq!(keys, (25..30).collect::<Vec<_>>());
        assert_eq!(map.window().anchor(), seq(25));
    }

    #[test]
    fn send_window_wraps_without_losing_its_total_local_offsets() {
        let mut w: SendWindow<u32> = SendWindow::new(seq(u64::MAX - 1));
        w.push(1);
        w.push(2);
        w.push(3);
        assert_eq!(w.next(), seq(1));
        assert_eq!(w.start(), seq(u64::MAX - 1));
        assert_eq!(*w.get(&seq(u64::MAX)).unwrap(), 2);
        assert_eq!(*w.get(&seq(0)).unwrap(), 3);
        assert!(w.get(&seq(1)).is_none());
        let entries: Vec<(u64, u32)> = w.iter().map(|(k, v)| (k.to_wire(), *v)).collect();
        assert_eq!(
            entries,
            vec![(u64::MAX - 1, 1), (u64::MAX, 2), (0, 3)],
            "iteration must use total local offsets across the wrap"
        );
        assert_eq!(w.pop(), Some(1));
        assert_eq!(w.start(), seq(u64::MAX));
        assert_eq!(*w.get(&seq(0)).unwrap(), 3);
        assert_eq!(w.pop(), Some(2));
        assert_eq!(w.pop(), Some(3));
        assert!(w.is_empty());
        // The empty window's start equals its next.
        assert_eq!(w.start(), w.next());
    }

    #[test]
    fn pop_none_counts_removed_prefix() {
        let mut w: SendWindow<Option<u32>> = SendWindow::new(seq(u64::MAX - 1));
        w.push(Some(1));
        w.push(None);
        w.push(None);
        w.push(Some(4));
        assert_eq!(w.pop_none(), 0);
        assert_eq!(w.pop(), Some(Some(1)));
        assert_eq!(w.pop_none(), 2, "both None entries must be removed");
        assert_eq!(w.start(), seq(u64::MAX - 1).advance(3));
        assert_eq!(*w.get(&seq(u64::MAX - 1).advance(3)).unwrap(), Some(4));
    }
}
