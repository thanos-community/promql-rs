//! Resumable replica deduplication: the penalty merge and chain merge of
//! Thanos `pkg/dedup/iter.go`, adapted to merge block by block as the
//! query's time range is split into overlapping blocks.
//!
//! The penalty merge is a literal port of the nested iterators, not a flat
//! fold over replicas. `dedupSeries.Iterator` wraps replica 0 and folds the
//! rest in as `newDedupSeriesIterator(it, wrap(replica))`, so with three
//! replicas the outer iterator's `a` is itself a dedup iterator with its own
//! `lastT`, penalties and `useA`, and `adjustAtValue` on a switch propagates
//! down into it. Collapsing that into one loop over N replicas changes which
//! sample is picked as soon as a third replica has gaps, so the nesting is
//! kept: node 0 is the leaf of replica 0, node k is `dedupSeriesIterator`
//! over node k-1 and the leaf of replica k.
//!
//! Resuming is what the Go code never needed. A block ends at a checkpoint,
//! and every replica's next sample is then unknown but known to lie past it.
//! A step whose outcome does not depend on those samples (some side has a
//! sample at or before the checkpoint, so it wins) is taken; its side effects
//! on a side that is still unknown (`Seek`, `adjustAtValue`) are queued on
//! that side and replayed, in order, once its samples arrive. A step whose
//! outcome does depend on them stays suspended and is retried with the next
//! call. The state at a checkpoint is therefore the whole tree, queues
//! included, and the next call continues as the unbroken run would have.
//!
//! One thing Go reads that no checkpoint can wait for: a level's first
//! `Next` lifts the replica it switches to by the value of the sample the
//! levels below picked first, wherever in the query that sample lies. The
//! caller therefore names every replica's first sample up front
//! ([`ReplicaMerge::new`]), the source knowing every chunk of the query
//! before it decodes any block, and the fold is built with all its levels
//! from the start, as Go builds it.

use std::collections::{BTreeMap, VecDeque};

/// `initialPenalty`: before any interval is known, timestamps being in
/// milliseconds and scrapes seconds apart.
const INITIAL_PENALTY: i64 = 5000;

/// Thanos `dedup.AlgorithmPenalty` or `dedup.AlgorithmChain`.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Algorithm {
    /// Follow one replica and penalise switching (the default).
    Penalty,
    /// Union the samples one by one, first replica winning a timestamp.
    Chain,
}

/// `chunkenc.ValueType` restricted to floats, plus the one state a resumable
/// iterator needs: the next sample exists but lies past the checkpoint, so it
/// cannot be read yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Val {
    Float,
    None,
    Unknown,
}

/// A call made on an iterator that could not be answered yet.
#[derive(Debug, Clone)]
enum Op {
    Seek(i64),
    Adjust(f64),
}

/// Consecutive seeks collapse to the largest target: seeking is monotone, so
/// the queue stays bounded for a replica that is silent for a long stretch.
/// Adjusts are not collapsed; their float rounding is order dependent.
fn push_op(ops: &mut VecDeque<Op>, op: Op) {
    if let (Op::Seek(t), Some(Op::Seek(last))) = (&op, ops.back_mut()) {
        *last = (*last).max(*t);
        return;
    }
    ops.push_back(op);
}

/// One replica's samples behind `counterErrAdjustSeriesIterator` (counters)
/// or `noopAdjustableSeriesIterator`.
///
/// `samples[pos]` is the current sample. `Seek` only moves forward, as
/// `chunkenc.Iterator.Seek` does; Thanos' test mock searches the whole slice
/// instead, which differs only for a seek target behind the current sample.
#[derive(Debug, Clone)]
struct Leaf {
    samples: Vec<(i64, f64)>,
    pos: usize,
    /// `counterErrAdjustSeriesIterator.errAdjust`.
    err_adjust: f64,
    counter: bool,
    /// Samples past the end of `samples` may still arrive.
    more: bool,
    ops: VecDeque<Op>,
}

impl Leaf {
    fn new(counter: bool) -> Self {
        Self {
            samples: Vec::new(),
            pos: 0,
            err_adjust: 0.0,
            counter,
            more: true,
            ops: VecDeque::new(),
        }
    }

    fn val(&self) -> Val {
        if self.pos < self.samples.len() {
            Val::Float
        } else if self.more {
            Val::Unknown
        } else {
            Val::None
        }
    }

    fn at_t(&self) -> i64 {
        self.samples[self.pos].0
    }

    fn value(&self) -> Option<f64> {
        let (_, v) = *self.samples.get(self.pos)?;
        // `counterErrAdjustSeriesIterator.At` adds errAdjust to everything.
        // A NaN is left alone because adding to a signalling NaN quiets it
        // and a staleness marker is identified by its exact bits.
        Some(if self.counter && !v.is_nan() {
            v + self.err_adjust
        } else {
            v
        })
    }

    fn seek(&mut self, t: i64) -> Val {
        if self.ops.is_empty() {
            while self.pos < self.samples.len() && self.samples[self.pos].0 < t {
                self.pos += 1;
            }
        }
        let v = self.val();
        if v == Val::Unknown {
            push_op(&mut self.ops, Op::Seek(t));
        }
        v
    }

    /// `counterErrAdjustSeriesIterator.adjustAtValue`.
    fn adjust(&mut self, last: f64) {
        if !self.counter {
            return;
        }
        match self.val() {
            Val::Float => {
                if let Some(v) = self.value() {
                    if last > v {
                        // This replica has an obsolete value: it did not see
                        // the end of the counter before an app restart.
                        self.err_adjust += last - v;
                    }
                }
            }
            Val::Unknown => push_op(&mut self.ops, Op::Adjust(last)),
            Val::None => {}
        }
    }

    /// Runs the queued calls against samples that arrived since.
    fn replay(&mut self) {
        let mut ops = std::mem::take(&mut self.ops).into_iter();
        while let Some(op) = ops.next() {
            if self.pos >= self.samples.len() {
                if self.more {
                    self.ops.push_back(op);
                    self.ops.extend(ops);
                }
                return;
            }
            match op {
                Op::Seek(t) => {
                    while self.pos < self.samples.len() && self.samples[self.pos].0 < t {
                        self.pos += 1;
                    }
                    if self.pos >= self.samples.len() {
                        if self.more {
                            self.ops.push_back(Op::Seek(t));
                            self.ops.extend(ops);
                        }
                        return;
                    }
                }
                Op::Adjust(last) => self.adjust(last),
            }
        }
    }
}

/// A `dedupSeriesIterator` whose `Next` may be suspended.
#[derive(Debug, Clone)]
struct Dedup {
    /// `aval` and `bval`.
    a_val: Val,
    b_val: Val,
    last_t: i64,
    /// `penA` and `penB`.
    pen_a: i64,
    pen_b: i64,
    use_a: bool,
    pending: Pending,
    /// Calls its parent made while `pending`; they run after the `Next`.
    ops: VecDeque<Op>,
    /// The last `Next` returned `ValNone`.
    done: bool,
}

impl Dedup {
    fn new() -> Self {
        Self {
            a_val: Val::Unknown,
            b_val: Val::Unknown,
            last_t: i64::MIN,
            pen_a: 0,
            pen_b: 0,
            use_a: true,
            pending: Pending::Start,
            ops: VecDeque::new(),
            done: false,
        }
    }
}

/// How far a `Next` has got.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Pending {
    No,
    /// Not started: Go runs a level's first `Next` when the level above it is
    /// built, which here is whenever something first asks for it.
    Start,
    /// Stopped after the seeks: `lastFloatVal` as read at the start, so the
    /// retry does not read it from an iterator the seeks already advanced.
    Late(Option<f64>),
}

enum Pick {
    A { exhausted: bool },
    B { exhausted: bool },
    Done,
    Blocked,
}

/// The nested iterators of one series.
#[derive(Debug, Clone)]
struct Fold {
    /// `leaves[k]` is replica `ids[k]`, in fold order.
    leaves: Vec<Leaf>,
    /// `levels[k - 1]` is the dedup iterator of node k.
    levels: Vec<Dedup>,
    counter: bool,
    /// `firsts[k]` is the value node `k` held after its first `Next`, which
    /// is what the level above reads as `lastFloatVal` on its own first
    /// `Next`; empty unless `counter`.
    firsts: Vec<f64>,
}

impl Fold {
    /// `AtT` of node `k`, which must hold a sample.
    fn at_t(&self, k: usize) -> i64 {
        if k == 0 || !self.levels[k - 1].use_a {
            self.leaves[k].at_t()
        } else {
            self.at_t(k - 1)
        }
    }

    /// `At` of node `k`: the value of whichever replica was last used.
    fn value(&self, k: usize) -> Option<f64> {
        if k == 0 {
            return self.leaves[0].value();
        }
        let d = &self.levels[k - 1];
        if d.pending != Pending::No {
            None
        } else if d.use_a {
            self.value(k - 1)
        } else {
            self.leaves[k].value()
        }
    }

    fn node_val(&self, k: usize) -> Val {
        if k == 0 {
            return self.leaves[0].val();
        }
        let d = &self.levels[k - 1];
        if d.done {
            Val::None
        } else if d.pending != Pending::No {
            Val::Unknown
        } else {
            Val::Float
        }
    }

    /// `Seek` on node `k`.
    fn seek(&mut self, k: usize, t: i64) -> Val {
        if k == 0 {
            return self.leaves[0].seek(t);
        }
        let j = k - 1;
        if self.levels[j].done {
            return Val::None;
        }
        if self.levels[j].pending != Pending::No {
            match self.settle(k) {
                Val::None => return Val::None,
                Val::Unknown => {
                    push_op(&mut self.levels[j].ops, Op::Seek(t));
                    return Val::Unknown;
                }
                Val::Float => {}
            }
            if self.levels[j].pending != Pending::No {
                push_op(&mut self.levels[j].ops, Op::Seek(t));
                return Val::Unknown;
            }
            if self.levels[j].done {
                return Val::None;
            }
        }
        // Don't use underlying Seek, but iterate over Next to not miss gaps.
        loop {
            if self.at_t(k) >= t {
                return Val::Float;
            }
            match self.next(k) {
                Val::Float => {}
                Val::None => return Val::None,
                Val::Unknown => {
                    push_op(&mut self.levels[j].ops, Op::Seek(t));
                    return Val::Unknown;
                }
            }
        }
    }

    /// `adjustAtValue` on node `k`.
    fn adjust(&mut self, k: usize, last: f64) {
        if k == 0 {
            self.leaves[0].adjust(last);
        } else if self.levels[k - 1].pending != Pending::No {
            push_op(&mut self.levels[k - 1].ops, Op::Adjust(last));
        } else {
            self.adjust_children(k, last);
        }
    }

    /// `dedupSeriesIterator.adjustAtValue`: both sides are adjusted, not only
    /// the one switched to.
    fn adjust_children(&mut self, k: usize, last: f64) {
        let d = &self.levels[k - 1];
        let (a_alive, b_alive) = (d.a_val != Val::None, d.b_val != Val::None);
        if a_alive {
            self.adjust(k - 1, last);
        }
        if b_alive {
            self.leaves[k].adjust(last);
        }
    }

    /// Retries a suspended `Next` of node `k` and, if it completes, the calls
    /// its parent queued meanwhile.
    fn settle(&mut self, k: usize) -> Val {
        let v = self.next(k);
        match v {
            Val::Float => {
                let ops = std::mem::take(&mut self.levels[k - 1].ops);
                for op in ops {
                    match op {
                        Op::Seek(t) => {
                            self.seek(k, t);
                        }
                        Op::Adjust(last) => self.adjust(k, last),
                    }
                }
            }
            Val::None => self.levels[k - 1].ops.clear(),
            Val::Unknown => {}
        }
        v
    }

    /// `lastFloatVal` of the level of node `k`.
    fn last_float_val(&self, k: usize) -> Option<f64> {
        let j = k - 1;
        // Adjusting is a no-op for a non-counter, so the value is never used.
        if !self.counter {
            return None;
        }
        let d = &self.levels[j];
        if d.last_t == i64::MIN {
            // The first `Next` reads what the levels below picked first, a
            // sample that may lie past everything this call has seen.
            return Some(self.firsts[k - 1]);
        }
        let (alive, value) = if d.use_a {
            (d.a_val != Val::None, self.value(k - 1))
        } else {
            (d.b_val != Val::None, self.leaves[k].value())
        };
        // A level that has picked has a readable current sample.
        value.filter(|_| alive)
    }

    /// `dedupSeriesIterator.Next` of node `k`.
    fn next(&mut self, k: usize) -> Val {
        let j = k - 1;
        // A side whose seek was queued is unknown only until its samples
        // arrive; the seek below is what finds out.
        if self.levels[j].a_val == Val::Unknown {
            self.levels[j].a_val = self.node_val(k - 1);
        }
        if self.levels[j].b_val == Val::Unknown {
            self.levels[j].b_val = self.leaves[k].val();
        }
        let last_float = match self.levels[j].pending {
            Pending::Late(captured) => captured,
            _ => self.last_float_val(k),
        };
        let last_use_a = self.levels[j].use_a;

        // Advance both iterators to at least the next highest timestamp plus
        // the potential penalty.
        if self.levels[j].a_val != Val::None {
            let t = self.levels[j]
                .last_t
                .wrapping_add(1)
                .wrapping_add(self.levels[j].pen_a);
            self.levels[j].a_val = self.seek(k - 1, t);
        }
        if self.levels[j].b_val != Val::None {
            let t = self.levels[j]
                .last_t
                .wrapping_add(1)
                .wrapping_add(self.levels[j].pen_b);
            self.levels[j].b_val = self.leaves[k].seek(t);
        }

        // A side that is unknown holds samples past the checkpoint, later
        // than any known one, so it never wins against a known side.
        let (av, bv) = (self.levels[j].a_val, self.levels[j].b_val);
        let pick = match (av, bv) {
            (Val::None, Val::None) => Pick::Done,
            (Val::None, Val::Float) => Pick::B { exhausted: true },
            (Val::Float, Val::None) => Pick::A { exhausted: true },
            (Val::None, Val::Unknown) | (Val::Unknown, Val::None) => Pick::Blocked,
            (Val::Unknown, Val::Unknown) => Pick::Blocked,
            (Val::Float, Val::Unknown) => Pick::A { exhausted: false },
            (Val::Unknown, Val::Float) => Pick::B { exhausted: false },
            (Val::Float, Val::Float) => {
                if self.at_t(k - 1) <= self.leaves[k].at_t() {
                    Pick::A { exhausted: false }
                } else {
                    Pick::B { exhausted: false }
                }
            }
        };

        let ret = match pick {
            Pick::Blocked => {
                self.levels[j].pending = Pending::Late(last_float);
                return Val::Unknown;
            }
            Pick::Done => {
                self.levels[j].use_a = false;
                self.levels[j].done = true;
                Val::None
            }
            Pick::A { exhausted } => {
                let ta = self.at_t(k - 1);
                let d = &mut self.levels[j];
                d.use_a = true;
                if !exhausted {
                    // For the series we didn't pick, add a penalty twice as
                    // high as the delta of the last two samples to the next
                    // seek against it. If we don't know a delta yet, use a
                    // constant.
                    d.pen_b = if d.last_t != i64::MIN {
                        2i64.wrapping_mul(ta.wrapping_sub(d.last_t))
                    } else {
                        INITIAL_PENALTY
                    };
                }
                d.pen_a = 0;
                d.last_t = ta;
                Val::Float
            }
            Pick::B { exhausted } => {
                let tb = self.leaves[k].at_t();
                let d = &mut self.levels[j];
                d.use_a = false;
                if !exhausted {
                    d.pen_a = if d.last_t != i64::MIN {
                        2i64.wrapping_mul(tb.wrapping_sub(d.last_t))
                    } else {
                        INITIAL_PENALTY
                    };
                }
                d.pen_b = 0;
                d.last_t = tb;
                Val::Float
            }
        };
        self.levels[j].pending = Pending::No;
        if self.levels[j].use_a != last_use_a {
            // We switched replicas. Ensure values are correct based on the
            // value before At.
            if let Some(last) = last_float {
                self.adjust_children(k, last);
            }
        }
        ret
    }
}

/// Resumable merge of one series' replicas across block calls.
///
/// State carried between calls is the nested iterators of the penalty
/// algorithm, so a call that starts past a checkpoint produces what an
/// unbroken run over every sample would have.
#[derive(Debug, Clone)]
pub(crate) struct ReplicaMerge {
    algorithm: Algorithm,
    /// Replica ids in fold order.
    ids: Vec<usize>,
    fold: Fold,
}

impl ReplicaMerge {
    /// The merge of the replicas `firsts` names, each with the first sample
    /// Go's iterator for it would yield: inside the query's bounds, and for
    /// a counter with the resets applied. A replica with no such sample is
    /// left out, as Go's iterator over it is exhausted from the start and
    /// the levels around it then pass the other side through.
    ///
    /// Ids are the fold order, ascending; they only have to be stable
    /// across calls.
    pub(crate) fn new(
        algorithm: Algorithm,
        is_counter: bool,
        firsts: &[(usize, (i64, f64))],
    ) -> Self {
        let mut firsts = firsts.to_vec();
        firsts.sort_by_key(|(id, _)| *id);
        let n = firsts.len();
        let mut fold = Fold {
            leaves: (0..n).map(|_| Leaf::new(is_counter)).collect(),
            levels: (1..n).map(|_| Dedup::new()).collect(),
            counter: is_counter,
            firsts: Vec::new(),
        };
        if is_counter && n > 1 {
            fold.firsts = Self::first_values(&firsts);
        }
        Self {
            algorithm,
            ids: firsts.iter().map(|(id, _)| *id).collect(),
            fold,
        }
    }

    /// What each node holds after its first `Next`, by running those
    /// `Next`s bottom-up as Go does while it builds the nested iterators,
    /// over replicas that have no sample but their first.
    fn first_values(firsts: &[(usize, (i64, f64))]) -> Vec<f64> {
        let n = firsts.len();
        let mut sim = Fold {
            leaves: firsts
                .iter()
                .map(|(_, first)| Leaf {
                    samples: vec![*first],
                    more: false,
                    ..Leaf::new(true)
                })
                .collect(),
            levels: (1..n).map(|_| Dedup::new()).collect(),
            counter: true,
            firsts: vec![firsts[0].1 .1],
        };
        for k in 1..n {
            let picked = sim.next(k);
            debug_assert_eq!(picked, Val::Float, "a replica with a first sample has one");
            sim.firsts.push(sim.value(k).unwrap_or(f64::NAN));
        }
        sim.firsts
    }

    /// Whether replica `id` was named in [`Self::new`].
    pub(crate) fn knows(&self, id: usize) -> bool {
        self.ids.binary_search(&id).is_ok()
    }

    /// Folds this call's samples up to `checkpoint` into the state and
    /// returns what became final.
    ///
    /// `replicas`: (replica id, samples) in fold order, ascending id; ids
    /// not named in [`Self::new`] are a caller bug. Samples are
    /// ts-ascending. Afterwards the state is exactly that after consuming
    /// every input sample with ts <= checkpoint, so the next call must pass
    /// only samples past it; `i64::MAX` ends the series. Samples past the
    /// checkpoint are not kept.
    ///
    /// What is returned is at or before the checkpoint, and every sample
    /// of it: a step whose pick depends on samples past the checkpoint is
    /// not taken, and whatever it would pick lies past it too.
    pub(crate) fn advance(
        &mut self,
        replicas: &[(usize, &[(i64, f64)])],
        checkpoint: i64,
    ) -> Vec<(i64, f64)> {
        if matches!(self.algorithm, Algorithm::Chain) {
            return merge_chain(replicas);
        }
        let more = checkpoint != i64::MAX;
        for (id, samples) in replicas {
            debug_assert!(samples.windows(2).all(|w| w[0].0 <= w[1].0));
            let Ok(i) = self.ids.binary_search(id) else {
                debug_assert!(samples.is_empty(), "replica {id} was not named in new");
                continue;
            };
            let seen = samples.iter().take_while(|(t, _)| *t <= checkpoint);
            self.fold.leaves[i].samples.extend(seen);
        }
        self.fold.set_more(more);

        let mut out = Vec::new();
        self.drain(&mut out);
        self.fold.compact();
        out
    }

    /// The samples past `checkpoint` that `replicas` give, as if the series
    /// ended with them; none of it is kept. A block returns this tail after
    /// [`Self::advance`]'s samples, since its reach runs past the
    /// checkpoint into the next block's, which gets to see the same samples
    /// again and, with the data beyond them, may pick differently.
    pub(crate) fn preview(
        &self,
        replicas: &[(usize, &[(i64, f64)])],
        checkpoint: i64,
    ) -> Vec<(i64, f64)> {
        if matches!(self.algorithm, Algorithm::Chain) {
            // The union of a block's samples is all there is to return.
            return Vec::new();
        }
        let mut tail = self.clone();
        for (id, samples) in replicas {
            let Ok(i) = tail.ids.binary_search(id) else {
                debug_assert!(samples.is_empty(), "replica {id} was not named in new");
                continue;
            };
            let past = samples.iter().filter(|(t, _)| *t > checkpoint);
            tail.fold.leaves[i].samples.extend(past);
        }
        tail.fold.set_more(false);
        let mut out = Vec::new();
        tail.drain(&mut out);
        out.retain(|(t, _)| *t > checkpoint);
        out
    }

    /// [`Self::advance`] and [`Self::preview`] in one, over replicas given
    /// whole: what a series with one block gives.
    #[cfg(test)]
    pub(crate) fn merge(
        &mut self,
        replicas: &[(usize, &[(i64, f64)])],
        checkpoint: i64,
    ) -> Vec<(i64, f64)> {
        let mut out = self.advance(replicas, checkpoint);
        if checkpoint != i64::MAX {
            out.extend(self.preview(replicas, checkpoint));
        }
        out
    }

    /// Calls `Next` on the root iterator until it is exhausted or suspended.
    fn drain(&mut self, out: &mut Vec<(i64, f64)>) {
        let fold = &mut self.fold;
        if fold.leaves.is_empty() {
            return;
        }
        if fold.leaves.len() == 1 {
            let leaf = &mut fold.leaves[0];
            out.extend_from_slice(&leaf.samples[leaf.pos..]);
            leaf.pos = leaf.samples.len();
            return;
        }
        let root = fold.leaves.len() - 1;
        while fold.next(root) == Val::Float {
            let t = fold.at_t(root);
            let v = fold.value(root).expect("a picked sample is readable");
            out.push((t, v));
        }
    }
}

impl Fold {
    fn set_more(&mut self, more: bool) {
        for leaf in &mut self.leaves {
            leaf.more = more;
            leaf.replay();
        }
    }

    /// Drops samples every iterator has moved past; the current sample of a
    /// leaf is kept because `lastFloatVal` of its level reads it.
    fn compact(&mut self) {
        for leaf in &mut self.leaves {
            leaf.samples.drain(..leaf.pos);
            leaf.pos = 0;
        }
    }
}

/// `storage.ChainedSeriesMerge`: the union of the replicas' samples, one per
/// timestamp.
///
/// Prometheus' `chainSampleIterator` keeps one iterator current and pops a
/// `container/heap` when another has the smaller timestamp, so the winner of
/// a timestamp two replicas share is whichever the heap surfaces, which is
/// not specified. The lowest replica id wins here instead; replicas of one
/// series that share a timestamp share its value in practice, and a stable
/// rule is what lets a block merge resume without keeping a heap.
fn merge_chain(replicas: &[(usize, &[(i64, f64)])]) -> Vec<(i64, f64)> {
    let mut merged = BTreeMap::new();
    for (_, samples) in replicas {
        for &(t, v) in *samples {
            merged.entry(t).or_insert(v);
        }
    }
    merged.into_iter().collect()
}

/// `isCounter`: the functions whose input must never go backwards.
pub(crate) fn is_counter(func: Option<&str>) -> bool {
    matches!(
        func,
        Some("increase" | "rate" | "irate" | "resets" | "xincrease" | "xrate")
    )
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) const STALE_NAN_BITS: u64 = 0x7ff0000000000002;

    type Samples = Vec<(i64, f64)>;

    const REAL_A: &str = "\
    1587690005791:461968 1587690020791:462151 1587690035797:462336 1587690050791:462650 \
    1587690065791:462813 1587690080791:462987 1587690095791:463095 1587690110791:463247 \
    1587690125791:463440 1587690140791:463642 1587690155791:463811 1587690170791:464027 \
    1587690185791:464308 1587690200791:464514 1587690215791:464798 1587690230791:465018 \
    1587690245791:465215 1587690260813:465431 1587690275791:465651 1587690290791:465870 \
    1587690305791:466070 1587690320792:466248 1587690335791:466506 1587690350791:466766 \
    1587690365791:466970 1587690380791:467123 1587690395791:467265 1587690410791:467383 \
    1587690425791:467629 1587690440791:467931 1587690455791:468097 1587690470791:468281 \
    1587690485791:468477 1587690500791:468649 1587690515791:468867 1587690530791:469150 \
    1587690545791:469268 1587690560791:469488 1587690575791:469742 1587690590791:469951 \
    1587690605791:470131 1587690620791:470337 1587690635791:470631 1587690650791:470832 \
    1587690665791:471077 1587690680791:471311 1587690695791:471473 1587690710791:471728 \
    1587690725791:472002 1587690740791:472158 1587690755791:472329 1587690770791:472722 \
    1587690785791:472925 1587690800791:473220 1587690815791:473460 1587690830791:473748 \
    1587690845791:473968 1587690860791:474261 1587690875791:474418 1587690890791:474726 \
    1587690905791:474913 1587690920791:475031 1587690935791:475284 1587690950791:475563 \
    1587690965791:475762 1587690980791:475945 1587690995791:476302 1587691010791:476501 \
    1587691025791:476849 1587691040800:477020 1587691055791:477280 1587691070791:477549 \
    1587691085791:477758 1587691100817:477960 1587691115791:478261 1587691130791:478559 \
    1587691145791:478704 1587691160804:478950 1587691175791:479173 1587691190791:479368 \
    1587691205791:479625 1587691220805:479866 1587691235791:480008 1587691250791:480155 \
    1587691265791:480472 1587691280811:480598 1587691295791:480771 1587691310791:480996 \
    1587691325791:481200 1587691340803:481381 1587691355791:481584 1587691370791:481759 \
    1587691385791:482003 1587691400803:482189 1587691415791:482457 1587691430791:482623 \
    1587691445791:482768 1587691460804:483036 1587691475791:483322 1587691490791:483566 \
    1587691505791:483709 1587691520807:483838 1587691535791:484091 1587691550791:484236 \
    1587691565791:484454 1587691580816:484710 1587691595791:484978 1587691610791:485271 \
    1587691625791:485476 1587691640792:485640 1587691655791:485921 1587691670791:486201 \
    1587691685791:486555 1587691700791:486691 1587691715791:486831 1587691730791:487033 \
    1587691745791:487268 1587691760803:487370 1587691775791:487571 1587691790791:487787 \
    1587691805791:488036 1587691820791:488241 1587691835791:488411 1587691850791:488625 \
    1587691865791:488868 1587691880791:489005 1587691895791:489237 1587691910791:489545 \
    1587691925791:489750 1587691940791:489899 1587691955791:490048 1587691970791:490364 \
    1587691985791:490485 1587692000791:490722 1587692015791:490866 1587692030791:491025 \
    1587692045791:491286 1587692060816:491543 1587692075791:491787 1587692090791:492065 \
    1587692105791:492223 1587692120816:492501 1587692135791:492767 1587692150791:492955 \
    1587692165791:493194 1587692180792:493402 1587692195791:493647 1587692210791:493897 \
    1587692225791:494117 1587692240805:494356 1587692255791:494620 1587692270791:494762 \
    1587692285791:495001 1587692300805:495222 1587692315791:495393 1587692330791:495662 \
    1587692345791:495875 1587692360801:496082 1587692375791:496196 1587692390791:496245 \
    1587692405791:496295 1587692420791:496365 1587692435791:496401 1587692450791:496452 \
    1587692465791:496491 1587692480791:496544 1587692555791:496619 1587692570791:496852 \
    1587692585791:497052 1587692600791:497245 1587692615791:497529 1587692630791:497697 \
    1587692645791:497909 1587692660791:498156 1587692675803:498466 1587692690791:498647 \
    1587692705791:498805 1587692720791:499013 1587692735805:499169 1587692750791:499345 \
    1587692765791:499499 1587692780791:499731 1587692795806:499972 1587692810791:500201 \
    1587692825791:500354 1587692840791:500512 1587692855791:500739 1587692870791:500958 \
    1587692885791:501190 1587692900791:501233 1587692915791:501391 1587692930791:501649 \
    1587692945791:501853 1587692960791:502065 1587692975791:502239 1587692990810:502554 \
    1587693005791:502754 1587693020791:502938 1587693035791:503141 1587693050791:503416 \
    1587693065791:503642 1587693080791:503873 1587693095791:504014 1587693110791:504178 \
    1587693125821:504374 1587693140791:504578 1587693155791:504753 1587693170791:505043 \
    1587693185791:505232 1587693200791:505437 1587693215791:505596 1587693230791:505923 \
    1587693245791:506088 1587693260791:506307 1587693275791:506518 1587693290791:506786 \
    1587693305791:507008 1587693320803:507260 1587693335791:507519 1587693350791:507776 \
    1587693365791:508003 1587693380791:508322 1587693395804:508551 1587693410791:508750 \
    1587693425791:508994 1587693440791:509237 1587693455791:509452 1587693470791:509702 \
    1587693485791:509971 1587693500791:510147 1587693515791:510471 1587693530816:510666 \
    1587693545791:510871 1587693560791:511123 1587693575791:511303 1587693590791:511500 ";
    const REAL_B: &str = "\
    1587690007139:461993 1587690022139:462164 1587690037139:462409 1587690052139:462662 \
    1587690067139:462824 1587690082139:462987 1587690097155:463108 1587690112139:463261 \
    1587690127139:463465 1587690142139:463642 1587690157139:463823 1587690172139:464065 \
    1587690187139:464333 1587690202139:464566 1587690217139:464811 1587690232140:465032 \
    1587690247139:465229 1587690262139:465445 1587690277139:465700 1587690292139:465884 \
    1587690307139:466083 1587690322139:466250 1587690337150:466534 1587690352139:466791 \
    1587690367139:466970 1587690382139:467149 1587690397139:467265 1587690412139:467383 \
    1587690427139:467647 1587690442139:467943 1587690457139:468121 1587690472139:468294 \
    1587690487139:468545 1587690502139:468676 1587690517139:468879 1587690532139:469154 \
    1587690547139:469281 1587690562139:469512 1587690577139:469783 1587690592139:469964 \
    1587690607139:470171 1587690622139:470355 1587690637139:470656 1587690652139:470845 \
    1587690667139:471077 1587690682139:471315 1587690697139:471535 1587690712139:471766 \
    1587690727139:472002 1587690742139:472171 1587690757139:472354 1587690772139:472736 \
    1587690787139:472948 1587690802139:473259 1587690817139:473460 1587690832139:473753 \
    1587690847139:474007 1587690862139:474286 1587690877139:474423 1587690892139:474788 \
    1587690907139:474925 1587690922139:475031 1587690937139:475316 1587690952139:475573 \
    1587690967139:475784 1587690982139:475992 1587690997139:476341 1587691012139:476541 \
    1587691027139:476890 1587691042139:477033 1587691057139:477305 1587691072139:477577 \
    1587691087139:477771 1587691102139:478012 1587691117139:478296 1587691132139:478559 \
    1587691147139:478744 1587691162139:478950 1587691177139:479201 1587691192139:479388 \
    1587691207139:479638 1587691222154:479907 1587691237139:480008 1587691252139:480167 \
    1587691267139:480472 1587691282157:480615 1587691297139:480771 1587691312139:481027 \
    1587691327139:481212 1587691342159:481395 1587691357139:481598 1587691372139:481786 \
    1587691387139:482003 1587691402141:482236 1587691417139:482508 1587691432139:482636 \
    1587691447139:482780 1587691462139:483059 1587691477139:483357 1587691492139:483566 \
    1587691507139:483711 1587691522139:483838 1587691537139:484091 1587691552139:484254 \
    1587691567139:484479 1587691582139:484748 1587691597139:484978 1587691612139:485271 \
    1587691627139:485488 1587691642139:485700 1587691657139:485945 1587691672139:486228 \
    1587691687139:486588 1587691702139:486691 1587691717139:486881 1587691732139:487046 \
    1587691747139:487291 1587691762177:487410 1587691777139:487571 1587691792139:487799 \
    1587691807139:488050 1587691822139:488241 1587691837139:488424 1587691852139:488629 \
    1587691867139:488875 1587691882139:489017 1587691897139:489254 1587691912139:489545 \
    1587691927139:489778 1587691942139:489912 1587691957139:490084 1587691972139:490364 \
    1587691987139:490510 1587692002139:490744 1587692017139:490880 1587692032139:491025 \
    1587692047139:491297 1587692062155:491557 1587692077139:491839 1587692092139:492065 \
    1587692107139:492234 1587692122139:492526 1587692137139:492767 1587692152139:492967 \
    1587692167139:493218 1587692182139:493442 1587692197139:493647 1587692212139:493920 \
    1587692227139:494170 1587692242139:494358 1587692257139:494632 1587692272139:494800 \
    1587692287139:495026 1587692302139:495222 1587692317139:495433 1587692332139:495677 \
    1587692347139:495901 1587692362139:496107 1587692377139:496196 1587692392139:496245 \
    1587692407139:496300 1587692422159:496365 1587692437139:496401 1587692452139:496452 \
    1587692467139:496532 1587692542149:496537 1587692557139:496633 1587692572139:496844 \
    1587692587139:497040 1587692602144:497257 1587692617139:497522 1587692632139:497710 \
    1587692647139:497938 1587692662154:498172 1587692677139:498459 1587692692139:498635 \
    1587692707139:498832 1587692722139:499014 1587692737139:499170 1587692752139:499338 \
    1587692767139:499511 1587692782149:499719 1587692797139:499973 1587692812139:500189 \
    1587692827139:500359 1587692842139:500517 1587692857139:500727 1587692872139:500959 \
    1587692887139:501178 1587692902139:501246 1587692917153:501404 1587692932139:501663 \
    1587692947139:501850 1587692962139:502103 1587692977155:502280 1587692992139:502562 \
    1587693007139:502742 1587693022139:502931 1587693037139:503190 1587693052139:503428 \
    1587693067139:503630 1587693082139:503873 1587693097139:504027 1587693112139:504179 \
    1587693127139:504362 1587693142139:504590 1587693157139:504741 1587693172139:505056 \
    1587693187139:505244 1587693202139:505436 1587693217139:505635 1587693232139:505936 \
    1587693247155:506088 1587693262139:506309 1587693277139:506524 1587693292139:506800 \
    1587693307139:507010 1587693322139:507286 1587693337139:507530 1587693352139:507781 \
    1587693367139:507991 1587693382139:508310 1587693397139:508570 1587693412139:508770 \
    1587693427139:508982 1587693442163:509274 1587693457139:509477 1587693472139:509713 \
    1587693487139:509972 1587693502139:510182 1587693517139:510498 1587693532139:510654 \
    1587693547139:510859 1587693562139:511124 1587693577139:511314 1587693592139:511488 ";
    const REAL_EXP: &str = "\
    1587690005791:461968 1587690020791:462151 1587690035797:462336 1587690050791:462650 \
    1587690065791:462813 1587690080791:462987 1587690095791:463095 1587690110791:463247 \
    1587690125791:463440 1587690140791:463642 1587690155791:463811 1587690170791:464027 \
    1587690185791:464308 1587690200791:464514 1587690215791:464798 1587690230791:465018 \
    1587690245791:465215 1587690260813:465431 1587690275791:465651 1587690290791:465870 \
    1587690305791:466070 1587690320792:466248 1587690335791:466506 1587690350791:466766 \
    1587690365791:466970 1587690380791:467123 1587690395791:467265 1587690410791:467383 \
    1587690425791:467629 1587690440791:467931 1587690455791:468097 1587690470791:468281 \
    1587690485791:468477 1587690500791:468649 1587690515791:468867 1587690530791:469150 \
    1587690545791:469268 1587690560791:469488 1587690575791:469742 1587690590791:469951 \
    1587690605791:470131 1587690620791:470337 1587690635791:470631 1587690650791:470832 \
    1587690665791:471077 1587690680791:471311 1587690695791:471473 1587690710791:471728 \
    1587690725791:472002 1587690740791:472158 1587690755791:472329 1587690770791:472722 \
    1587690785791:472925 1587690800791:473220 1587690815791:473460 1587690830791:473748 \
    1587690845791:473968 1587690860791:474261 1587690875791:474418 1587690890791:474726 \
    1587690905791:474913 1587690920791:475031 1587690935791:475284 1587690950791:475563 \
    1587690965791:475762 1587690980791:475945 1587690995791:476302 1587691010791:476501 \
    1587691025791:476849 1587691040800:477020 1587691055791:477280 1587691070791:477549 \
    1587691085791:477758 1587691100817:477960 1587691115791:478261 1587691130791:478559 \
    1587691145791:478704 1587691160804:478950 1587691175791:479173 1587691190791:479368 \
    1587691205791:479625 1587691220805:479866 1587691235791:480008 1587691250791:480155 \
    1587691265791:480472 1587691280811:480598 1587691295791:480771 1587691310791:480996 \
    1587691325791:481200 1587691340803:481381 1587691355791:481584 1587691370791:481759 \
    1587691385791:482003 1587691400803:482189 1587691415791:482457 1587691430791:482623 \
    1587691445791:482768 1587691460804:483036 1587691475791:483322 1587691490791:483566 \
    1587691505791:483709 1587691520807:483838 1587691535791:484091 1587691550791:484236 \
    1587691565791:484454 1587691580816:484710 1587691595791:484978 1587691610791:485271 \
    1587691625791:485476 1587691640792:485640 1587691655791:485921 1587691670791:486201 \
    1587691685791:486555 1587691700791:486691 1587691715791:486831 1587691730791:487033 \
    1587691745791:487268 1587691760803:487370 1587691775791:487571 1587691790791:487787 \
    1587691805791:488036 1587691820791:488241 1587691835791:488411 1587691850791:488625 \
    1587691865791:488868 1587691880791:489005 1587691895791:489237 1587691910791:489545 \
    1587691925791:489750 1587691940791:489899 1587691955791:490048 1587691970791:490364 \
    1587691985791:490485 1587692000791:490722 1587692015791:490866 1587692030791:491025 \
    1587692045791:491286 1587692060816:491543 1587692075791:491787 1587692090791:492065 \
    1587692105791:492223 1587692120816:492501 1587692135791:492767 1587692150791:492955 \
    1587692165791:493194 1587692180792:493402 1587692195791:493647 1587692210791:493897 \
    1587692225791:494117 1587692240805:494356 1587692255791:494620 1587692270791:494762 \
    1587692285791:495001 1587692300805:495222 1587692315791:495393 1587692330791:495662 \
    1587692345791:495875 1587692360801:496082 1587692375791:496196 1587692390791:496245 \
    1587692405791:496295 1587692420791:496365 1587692435791:496401 1587692450791:496452 \
    1587692465791:496491 1587692480791:496544 1587692542149:496544 1587692557139:496640 \
    1587692572139:496851 1587692587139:497047 1587692602144:497264 1587692617139:497529 \
    1587692632139:497717 1587692647139:497945 1587692662154:498179 1587692677139:498466 \
    1587692692139:498642 1587692707139:498839 1587692722139:499021 1587692737139:499177 \
    1587692752139:499345 1587692767139:499518 1587692782149:499726 1587692797139:499980 \
    1587692812139:500196 1587692827139:500366 1587692842139:500524 1587692857139:500734 \
    1587692872139:500966 1587692887139:501185 1587692902139:501253 1587692917153:501411 \
    1587692932139:501670 1587692947139:501857 1587692962139:502110 1587692977155:502287 \
    1587692992139:502569 1587693007139:502749 1587693022139:502938 1587693037139:503197 \
    1587693052139:503435 1587693067139:503637 1587693082139:503880 1587693097139:504034 \
    1587693112139:504186 1587693127139:504369 1587693142139:504597 1587693157139:504748 \
    1587693172139:505063 1587693187139:505251 1587693202139:505443 1587693217139:505642 \
    1587693232139:505943 1587693247155:506095 1587693262139:506316 1587693277139:506531 \
    1587693292139:506807 1587693307139:507017 1587693322139:507293 1587693337139:507537 \
    1587693352139:507788 1587693367139:507998 1587693382139:508317 1587693397139:508577 \
    1587693412139:508777 1587693427139:508989 1587693442163:509281 1587693457139:509484 \
    1587693472139:509720 1587693487139:509979 1587693502139:510189 1587693517139:510505 \
    1587693532139:510661 1587693547139:510866 1587693562139:511131 1587693577139:511321 \
    1587693592139:511495 ";

    /// The first sample of every replica that has one, as the source names
    /// them.
    fn firsts_of(replicas: &[(usize, &[(i64, f64)])]) -> Vec<(usize, (i64, f64))> {
        replicas
            .iter()
            .filter_map(|(id, s)| s.first().map(|f| (*id, *f)))
            .collect()
    }

    fn merge_over(
        alg: Algorithm,
        counter: bool,
        replicas: &[(usize, &[(i64, f64)])],
    ) -> ReplicaMerge {
        ReplicaMerge::new(alg, counter, &firsts_of(replicas))
    }

    fn merged(alg: Algorithm, counter: bool, replicas: &[&[(i64, f64)]]) -> Samples {
        let input: Vec<_> = replicas.iter().enumerate().map(|(i, s)| (i, *s)).collect();
        merge_over(alg, counter, &input).merge(&input, i64::MAX)
    }

    /// NaN-safe equality: a staleness marker is told apart by its bits.
    fn bits(samples: &[(i64, f64)]) -> Vec<(i64, u64)> {
        samples.iter().map(|&(t, v)| (t, v.to_bits())).collect()
    }

    /// Samples every `step` seconds from `start` seconds, values 1, 2, 3…
    fn scrapes(start: i64, step: i64, n: i64) -> Samples {
        (0..n)
            .map(|i| ((start + i * step) * 1000, (i + 1) as f64))
            .collect()
    }

    fn seconds(samples: &[(i64, f64)]) -> Vec<i64> {
        samples.iter().map(|(t, _)| t / 1000).collect()
    }

    // A literal port of the Go iterators over complete series, the oracle the
    // resumable fold is compared to. It shares no code with the fold above:
    // nested `Box<dyn>` iterators with their own state, as in Go.

    trait Iter {
        fn next(&mut self) -> bool;
        fn seek(&mut self, t: i64) -> bool;
        fn at(&self) -> (i64, f64);
        fn at_t(&self) -> i64;
        fn adjust(&mut self, last: f64);
    }

    struct RefLeaf {
        samples: Samples,
        cur: Option<usize>,
        counter: bool,
        err_adjust: f64,
        /// Seek like the mock iterator of Go's tests, `sort.Search` over the
        /// whole slice, which can move backwards. Real chunk iterators and
        /// the fold never do.
        rewind: bool,
    }

    impl Iter for RefLeaf {
        fn next(&mut self) -> bool {
            let i = self.cur.map_or(0, |c| c + 1);
            self.cur = Some(i);
            i < self.samples.len()
        }
        fn seek(&mut self, t: i64) -> bool {
            let mut i = if self.rewind {
                0
            } else {
                self.cur.unwrap_or(0)
            };
            while i < self.samples.len() && self.samples[i].0 < t {
                i += 1;
            }
            self.cur = Some(i);
            i < self.samples.len()
        }
        fn at(&self) -> (i64, f64) {
            let (t, v) = self.samples[self.cur.unwrap()];
            (
                t,
                if self.counter && !v.is_nan() {
                    v + self.err_adjust
                } else {
                    v
                },
            )
        }
        fn at_t(&self) -> i64 {
            self.samples[self.cur.unwrap()].0
        }
        fn adjust(&mut self, last: f64) {
            if self.counter {
                let (_, v) = self.at();
                if last > v {
                    self.err_adjust += last - v;
                }
            }
        }
    }

    struct RefDedup {
        a: Box<dyn Iter>,
        b: Box<dyn Iter>,
        aval: bool,
        bval: bool,
        last_t: i64,
        last_a: bool,
        pen_a: i64,
        pen_b: i64,
        use_a: bool,
    }

    impl RefDedup {
        fn new(mut a: Box<dyn Iter>, mut b: Box<dyn Iter>) -> Self {
            let (aval, bval) = (a.next(), b.next());
            Self {
                a,
                b,
                aval,
                bval,
                last_t: i64::MIN,
                last_a: true,
                pen_a: 0,
                pen_b: 0,
                use_a: true,
            }
        }
        fn last_float_val(&self) -> Option<f64> {
            if (self.use_a && self.aval) || (!self.use_a && self.bval) {
                let it = if self.last_a { &self.a } else { &self.b };
                return Some(it.at().1);
            }
            None
        }
        fn adjust_at_value(&mut self, last: f64) {
            if self.aval {
                self.a.adjust(last);
            }
            if self.bval {
                self.b.adjust(last);
            }
        }
    }

    impl Iter for RefDedup {
        fn next(&mut self) -> bool {
            let last_float = self.last_float_val();
            let last_use_a = self.use_a;
            let ret = self.next_inner();
            if self.use_a != last_use_a {
                if let Some(l) = last_float {
                    self.adjust_at_value(l);
                }
            }
            ret
        }
        fn seek(&mut self, t: i64) -> bool {
            loop {
                let ts = self.at_t();
                if ts >= t {
                    return true;
                }
                if !self.next() {
                    return false;
                }
            }
        }
        fn at(&self) -> (i64, f64) {
            if self.last_a {
                self.a.at()
            } else {
                self.b.at()
            }
        }
        fn at_t(&self) -> i64 {
            if self.use_a {
                self.a.at_t()
            } else {
                self.b.at_t()
            }
        }
        fn adjust(&mut self, last: f64) {
            self.adjust_at_value(last);
        }
    }

    impl RefDedup {
        fn next_inner(&mut self) -> bool {
            if self.aval {
                self.aval = self
                    .a
                    .seek(self.last_t.wrapping_add(1).wrapping_add(self.pen_a));
            }
            if self.bval {
                self.bval = self
                    .b
                    .seek(self.last_t.wrapping_add(1).wrapping_add(self.pen_b));
            }
            if !self.aval {
                self.use_a = false;
                if self.bval {
                    self.last_t = self.b.at_t();
                    self.last_a = false;
                    self.pen_b = 0;
                }
                return self.bval;
            }
            if !self.bval {
                self.use_a = true;
                self.last_t = self.a.at_t();
                self.last_a = true;
                self.pen_a = 0;
                return self.aval;
            }
            let (ta, tb) = (self.a.at_t(), self.b.at_t());
            self.use_a = ta <= tb;
            let delta = |t: i64, last: i64| {
                if last != i64::MIN {
                    2 * (t - last)
                } else {
                    INITIAL_PENALTY
                }
            };
            if self.use_a {
                self.pen_b = delta(ta, self.last_t);
                self.pen_a = 0;
                self.last_t = ta;
                self.last_a = true;
            } else {
                self.pen_a = delta(tb, self.last_t);
                self.pen_b = 0;
                self.last_t = tb;
                self.last_a = false;
            }
            true
        }
    }

    /// `dedupSeries.Iterator` over complete series.
    fn reference(counter: bool, rewind: bool, replicas: &[Samples]) -> Samples {
        let leaf = |s: &Samples| -> Box<dyn Iter> {
            Box::new(RefLeaf {
                samples: s.clone(),
                cur: None,
                counter,
                err_adjust: 0.0,
                rewind,
            })
        };
        let mut it = leaf(&replicas[0]);
        for r in &replicas[1..] {
            it = Box::new(RefDedup::new(it, leaf(r)));
        }
        let mut out = Vec::new();
        if replicas.len() == 1 {
            return replicas[0].clone();
        }
        while it.next() {
            out.push(it.at());
        }
        out
    }

    // Ports of Go's tests in pkg/dedup/iter_test.go at 1d2b2ae7.

    #[test]
    fn dedup_series_iterator_cases() {
        // TestDedupSeriesIterator.
        let s = |v: &[(i64, f64)]| v.to_vec();
        let cases = [
            // Generally prefer the first series.
            (
                s(&[(10000, 10.), (20000, 11.), (30000, 12.), (40000, 13.)]),
                s(&[(10000, 20.), (20000, 21.), (30000, 22.), (40000, 23.)]),
                s(&[(10000, 10.), (20000, 11.), (30000, 12.), (40000, 13.)]),
            ),
            // Prefer b if it starts earlier.
            (
                s(&[(10100, 1.), (20100, 1.), (30100, 1.), (40100, 1.)]),
                s(&[(10000, 2.), (20000, 2.), (30000, 2.), (40000, 2.)]),
                s(&[(10000, 2.), (20000, 2.), (30000, 2.), (40000, 2.)]),
            ),
            // Don't switch series on a single delta sized gap.
            (
                s(&[(10000, 1.), (20000, 1.), (40000, 1.)]),
                s(&[(10000, 2.), (20000, 2.), (30000, 2.), (40000, 2.)]),
                s(&[(10000, 1.), (20000, 1.), (40000, 1.)]),
            ),
            (
                s(&[(10000, 1.), (20000, 1.), (40000, 1.)]),
                s(&[(15000, 2.), (25000, 2.), (35000, 2.), (45000, 2.)]),
                s(&[(10000, 1.), (20000, 1.), (40000, 1.)]),
            ),
            // Once the gap gets bigger than 2 deltas, switch and stay with the new series.
            (
                s(&[
                    (10000, 1.),
                    (20000, 1.),
                    (30000, 1.),
                    (60000, 1.),
                    (70000, 1.),
                ]),
                s(&[
                    (10100, 2.),
                    (20100, 2.),
                    (30100, 2.),
                    (40100, 2.),
                    (50100, 2.),
                    (60100, 2.),
                ]),
                s(&[
                    (10000, 1.),
                    (20000, 1.),
                    (30000, 1.),
                    (50100, 2.),
                    (60100, 2.),
                ]),
            ),
        ];
        for (i, (a, b, exp)) in cases.iter().enumerate() {
            assert_eq!(merged(Algorithm::Penalty, false, &[a, b]), *exp, "case {i}");
        }
    }

    const REGRESSION_2401_A: [(i64, f64); 8] = [
        (10000, 8.0),
        (20000, 9.0),
        (50001, 10.0),
        (60000, 11.0),
        (70000, 12.0),
        (80000, 13.0),
        (90000, 14.0),
        (100000, 15.0),
    ];
    const REGRESSION_2401_B: [(i64, f64); 4] =
        [(10001, 8.0), (45001, 8.5), (55001, 9.5), (65001, 10.5)];

    /// Go's expected output for the two 2401 cases, which holds only under the
    /// mock iterator of its tests: it passes at 55001 to seek a to 95004
    /// (a's penalty is twice the 25s gap that made b win), and the mock's
    /// seek walks back to 90000 where a real forward-only iterator has
    /// already skipped it. The fold and `reference(.., false, ..)` agree
    /// with that iterator, so their output lacks the (90000, 14) sample.
    fn regression_2401(counter: bool) -> (Samples, Samples) {
        let (a, b) = (REGRESSION_2401_A.to_vec(), REGRESSION_2401_B.to_vec());
        let go = reference(counter, true, &[a.clone(), b.clone()]);
        let forward = merged(Algorithm::Penalty, counter, &[&a, &b]);
        assert_eq!(bits(&forward), bits(&reference(counter, false, &[a, b])));
        (go, forward)
    }

    #[test]
    fn regression_against_2401_adjusts_the_lagging_counter() {
        // TestDedupSeriesSet "Regression test against 2401".
        let (go, forward) = regression_2401(true);
        let exp = [
            (10000, 8.),
            (20000, 9.),
            (45001, 9.),
            (55001, 10.),
            (65001, 11.),
            (90000, 14.),
            (100000, 15.),
        ];
        assert_eq!(go, exp);
        let without_90000: Samples = exp.iter().copied().filter(|s| s.0 != 90000).collect();
        assert_eq!(forward, without_90000);
    }

    #[test]
    fn regression_against_2401_without_counter_adjusts_nothing() {
        // TestDedupSeriesSet "Regression test with no counter adjustment".
        let (go, forward) = regression_2401(false);
        let exp = [
            (10000, 8.),
            (20000, 9.),
            (45001, 8.5),
            (55001, 9.5),
            (65001, 10.5),
            (90000, 14.),
            (100000, 15.),
        ];
        assert_eq!(go, exp);
        let without_90000: Samples = exp.iter().copied().filter(|s| s.0 != 90000).collect();
        assert_eq!(forward, without_90000);
    }

    #[test]
    fn x_functions_deduplicate_as_counters() {
        // TestDedupSeriesSet_XFunctions: every counter function gives the
        // result of `rate`.
        let exp = merged(
            Algorithm::Penalty,
            true,
            &[&REGRESSION_2401_A, &REGRESSION_2401_B],
        );
        for f in ["rate", "xrate", "xincrease", "increase", "irate", "resets"] {
            let got = merged(
                Algorithm::Penalty,
                is_counter(Some(f)),
                &[&REGRESSION_2401_A, &REGRESSION_2401_B],
            );
            assert_eq!(got, exp, "{f}");
        }
    }

    fn parse(s: &str) -> Samples {
        s.split_whitespace()
            .map(|p| {
                let (t, v) = p.split_once(':').unwrap();
                (t.parse().unwrap(), v.parse().unwrap())
            })
            .collect()
    }

    #[test]
    fn regression_on_real_data_against_2401() {
        // TestDedupSeriesSet "Regression test on real data against 2401";
        // expectedRealSeriesWithStaleMarkerDeduplicatedForRate.
        let got = merged(Algorithm::Penalty, true, &[&parse(REAL_A), &parse(REAL_B)]);
        assert_eq!(got, parse(REAL_EXP));
    }

    #[test]
    fn four_replicas_with_disjoint_ranges_concatenate() {
        // TestDedupSeriesSet "Single dedup label": three disjoint replicas
        // and a duplicate of the first.
        let r = [
            vec![(10000, 1.), (20000, 2.)],
            vec![(60000, 3.), (70000, 4.)],
            vec![(200000, 5.), (210000, 6.)],
            vec![(10000, 1.), (20000, 2.)],
        ];
        let views: Vec<&[(i64, f64)]> = r.iter().map(|s| s.as_slice()).collect();
        let exp = [
            (10000, 1.),
            (20000, 2.),
            (60000, 3.),
            (70000, 4.),
            (200000, 5.),
            (210000, 6.),
        ];
        assert_eq!(merged(Algorithm::Penalty, false, &views), exp);
    }

    #[test]
    fn chain_unions_replicas_with_gaps() {
        // TestDedupSeriesSet_Chain, "gap in one series", "gaps in two
        // series", and the multi label case with gaps.
        let full = [(10000, 1.), (20000, 2.), (30000, 3.)];
        let exp = full.to_vec();
        let c = |a: &[(i64, f64)], b: &[(i64, f64)]| merged(Algorithm::Chain, false, &[a, b]);
        assert_eq!(c(&full, &full), exp);
        assert_eq!(c(&[(10000, 1.), (30000, 3.)], &full), exp);
        assert_eq!(
            c(&[(10000, 1.), (30000, 3.)], &[(10000, 1.), (20000, 2.)]),
            exp
        );
        assert_eq!(
            c(&[(20000, 2.), (30000, 3.)], &[(10000, 1.), (20000, 2.)]),
            exp
        );
        let exp = [(10000, 101.), (20000, 102.), (30000, 103.)];
        assert_eq!(
            c(
                &[(10000, 101.), (20000, 102.)],
                &[(10000, 101.), (30000, 103.)]
            ),
            exp
        );
    }

    #[test]
    fn is_counter_matches_go() {
        // TestIsCounter.
        for f in ["rate", "increase", "irate", "resets", "xrate", "xincrease"] {
            assert!(is_counter(Some(f)), "{f}");
        }
        for f in ["", "sum", "avg", "count"] {
            assert!(!is_counter(Some(f)), "{f}");
        }
        assert!(!is_counter(None));
    }

    // Behaviour specific to the nested fold.

    #[test]
    fn one_replica_passes_through() {
        let samples = scrapes(0, 10, 4);
        assert_eq!(merged(Algorithm::Penalty, true, &[&samples]), samples);
        assert_eq!(merged(Algorithm::Penalty, false, &[]), vec![]);
    }

    #[test]
    fn a_gap_switches_replicas_after_the_penalty() {
        // a stops after 20s and returns at 60s; b keeps scraping. After a's
        // 20s sample, b is held back by twice the interval, so the switch
        // lands at 41s and a's 60s sample is then out-penalised.
        let a: Samples = [0, 10, 20, 60]
            .iter()
            .map(|s| (s * 1000, *s as f64))
            .collect();
        let b = scrapes(1, 10, 7);
        let got = merged(Algorithm::Penalty, false, &[&a, &b]);
        assert_eq!(seconds(&got), [0, 10, 20, 41, 51, 61]);
    }

    #[test]
    fn three_replicas_fold_pairwise() {
        // (a, b) is one iterator and c is folded over it. b's 3s lag is
        // inside the initial 5s penalty, so the pair follows a; c's 6s is
        // past it, so c is switched to after the first sample and followed,
        // the pair being held back by twice the interval each time.
        let a = scrapes(0, 10, 3);
        let b = scrapes(3, 10, 3);
        let c = scrapes(6, 10, 3);
        let got = merged(Algorithm::Penalty, false, &[&a, &b, &c]);
        assert_eq!(seconds(&got), [0, 6, 16, 26]);
    }

    #[test]
    fn a_switch_inside_the_inner_pair_lifts_the_outer_leaf_too() {
        // adjustAtValue on the outer iterator reaches both its sides, and the
        // inner iterator forwards it to both of its own. Replica 2 lags the
        // counter by 30 and is switched to once the pair below it has a gap.
        let a = vec![(0, 100.), (10_000, 110.), (20_000, 120.)];
        let b = vec![(500, 100.), (10_500, 110.)];
        let c = vec![
            (1_000, 70.),
            (11_000, 80.),
            (21_000, 90.),
            (31_000, 100.),
            (41_000, 110.),
        ];
        let got = merged(Algorithm::Penalty, true, &[&a, &b, &c]);
        let exp = reference(true, false, &[a.clone(), b.clone(), c.clone()]);
        assert_eq!(bits(&got), bits(&exp));
        // Whatever is picked, the counter never goes backwards.
        assert!(got.windows(2).all(|w| w[0].1 <= w[1].1), "{got:?}");
    }

    #[test]
    fn staleness_marker_keeps_its_bits_through_counter_lift() {
        let stale = f64::from_bits(STALE_NAN_BITS);
        // b never saw the counter reach 110, so it is lifted by 5 on the
        // switch; its staleness marker must not turn into another NaN.
        let a = vec![(0, 100.0), (10_000, 110.0)];
        let b = vec![(35_000, 105.0), (45_000, stale), (55_000, 106.0)];
        let got = merged(Algorithm::Penalty, true, &[&a, &b]);
        assert_eq!(
            bits(&got),
            bits(&[
                (0, 100.0),
                (10_000, 110.0),
                (35_000, 110.0),
                (45_000, stale),
                (55_000, 111.0)
            ])
        );
    }

    #[test]
    fn a_replica_silent_in_the_first_call_keeps_its_place_and_its_penalty() {
        // Replica 3 sorts before replica 5 and has nothing in the first
        // call, but Go has it from the start: it is `a` of the pair, and the
        // pick of 5 it is silent through already penalises it.
        let three = [(30_000, 100.), (40_000, 101.), (50_000, 102.)];
        let five = [(0, 1.), (10_000, 2.), (20_000, 3.)];
        let five_later = [(30_000, 3.5), (40_000, 4.)];
        let whole: Samples = five.iter().chain(&five_later).copied().collect();
        let all: [(usize, &[(i64, f64)]); 2] = [(3, &three), (5, &whole)];
        let mut merge = merge_over(Algorithm::Penalty, false, &all);
        let mut got = merge.merge(&[(5, &five)], 20_000);
        got.extend(merge.merge(&[(3, &three), (5, &five_later)], i64::MAX));
        let exp = reference(false, false, &[three.to_vec(), whole.clone()]);
        assert_eq!(got, exp);
    }

    // The resumable fold against the oracle and against itself.

    pub(crate) struct Rng(pub u64);
    impl Rng {
        pub(crate) fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        pub(crate) fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    /// Replicas scraping a counter every 10s with clock offsets and jitter,
    /// dropped samples, silent stretches that start with a staleness marker,
    /// replicas starting late or ending early, and counter resets seen by a
    /// replica only if it scraped at the time.
    pub(crate) fn generate(rng: &mut Rng, n: usize) -> Vec<Samples> {
        let ticks = 40 + rng.below(30) as i64;
        let mut base = 0.0;
        let mut counter_at = Vec::new();
        for _ in 0..ticks {
            base = if rng.below(25) == 0 {
                0.0
            } else {
                base + 1.0 + rng.below(9) as f64
            };
            counter_at.push(base);
        }
        (0..n)
            .map(|_| {
                let offset = rng.below(4_000) as i64;
                let lag = rng.below(15) as f64;
                let start = if rng.below(4) == 0 {
                    rng.below(ticks as u64 / 2)
                } else {
                    0
                } as i64;
                let end = if rng.below(4) == 0 {
                    ticks - rng.below(ticks as u64 / 2) as i64
                } else {
                    ticks
                };
                let mut out = Vec::new();
                let mut silent = 0;
                for i in start..end {
                    let t = i * 10_000 + offset + rng.below(300) as i64;
                    if silent > 0 {
                        silent -= 1;
                        continue;
                    }
                    if rng.below(12) == 0 {
                        silent = 2 + rng.below(7);
                        if rng.below(2) == 0 {
                            out.push((t, f64::from_bits(STALE_NAN_BITS)));
                        }
                        continue;
                    }
                    if rng.below(10) == 0 {
                        continue;
                    }
                    out.push((t, (counter_at[i as usize] - lag).max(0.0)));
                }
                out
            })
            .collect()
    }

    fn views<'a>(ids: &[usize], replicas: &'a [Samples]) -> Vec<(usize, &'a [(i64, f64)])> {
        ids.iter()
            .copied()
            .zip(replicas.iter().map(|s| s.as_slice()))
            .collect()
    }

    fn cut(replicas: &[Samples], keep: impl Fn(i64) -> bool) -> Vec<Samples> {
        replicas
            .iter()
            .map(|s| s.iter().copied().filter(|(t, _)| keep(*t)).collect())
            .collect()
    }

    #[test]
    fn the_fold_matches_the_nested_iterators_of_go() {
        let mut rng = Rng(0x9e3779b97f4a7c15);
        for round in 0..600 {
            let n = 2 + round % 3;
            let counter = round % 2 == 0;
            let replicas = generate(&mut rng, n);
            let ids: Vec<usize> = (0..n).collect();
            let got = merge_over(Algorithm::Penalty, counter, &views(&ids, &replicas))
                .merge(&views(&ids, &replicas), i64::MAX);
            let exp = reference(counter, false, &replicas);
            assert_eq!(
                bits(&got),
                bits(&exp),
                "round {round}, {n} replicas, counter {counter}"
            );
        }
    }

    /// One merge over everything equals a merge up to `cp` followed by one
    /// over the samples past it, whatever `cp` is.
    fn assert_resumes(alg: Algorithm, counter: bool, rounds: u64, seed: u64) {
        let mut rng = Rng(seed);
        for round in 0..rounds {
            let n = 2 + (round % 2) as usize;
            let replicas = generate(&mut rng, n);
            // Ids need not be dense; only their order is the fold order.
            let ids: Vec<usize> = (0..n).map(|i| i * 3 + 1).collect();
            let all = views(&ids, &replicas);
            let full = merge_over(alg, counter, &all).merge(&all, i64::MAX);
            let end = replicas
                .iter()
                .flatten()
                .map(|(t, _)| *t)
                .max()
                .unwrap_or(0);
            for k in 0..24 {
                let cp = if k < 12 {
                    rng.below(end as u64 + 1) as i64
                } else {
                    (k - 12) * 37_000
                };
                for give_all_first in [false, true] {
                    let first = if give_all_first {
                        cut(&replicas, |_| true)
                    } else {
                        cut(&replicas, |t| t <= cp)
                    };
                    let rest = cut(&replicas, |t| t > cp);
                    let mut merge = merge_over(alg, counter, &all);
                    let mut got = merge.merge(&views(&ids, &first), cp);
                    got.retain(|(t, _)| *t <= cp);
                    got.extend(merge.merge(&views(&ids, &rest), i64::MAX));
                    assert_eq!(
                        bits(&got),
                        bits(&full),
                        "round {round}, cp {cp}, first call sees everything: {give_all_first}"
                    );
                }
            }
        }
    }

    #[test]
    fn penalty_resumes_at_any_checkpoint() {
        assert_resumes(Algorithm::Penalty, false, 60, 0xdeadbeef);
    }

    #[test]
    fn penalty_with_counter_resumes_at_any_checkpoint() {
        assert_resumes(Algorithm::Penalty, true, 60, 0xfeedface);
    }

    #[test]
    fn chain_resumes_at_any_checkpoint() {
        assert_resumes(Algorithm::Chain, false, 40, 0xc0ffee);
    }

    #[test]
    fn penalty_with_counter_resumes_over_many_blocks() {
        // Several checkpoints in a row, so a suspension can span calls and a
        // replica can stay silent through some of them.
        let mut rng = Rng(0xabad1dea);
        for round in 0..80 {
            let n = 2 + round % 2;
            let counter = round % 4 < 2;
            let replicas = generate(&mut rng, n);
            let ids: Vec<usize> = (0..n).collect();
            let all = views(&ids, &replicas);
            let full = merge_over(Algorithm::Penalty, counter, &all).merge(&all, i64::MAX);
            let step = 20_000 + rng.below(90_000) as i64;
            let mut merge = merge_over(Algorithm::Penalty, counter, &all);
            let mut got = Vec::new();
            let mut from = i64::MIN;
            let mut cp = step;
            loop {
                let last = cp
                    > replicas
                        .iter()
                        .flatten()
                        .map(|(t, _)| *t)
                        .max()
                        .unwrap_or(0);
                let block = cut(&replicas, |t| t > from && t <= cp);
                let checkpoint = if last { i64::MAX } else { cp };
                got.extend(merge.merge(&views(&ids, &block), checkpoint));
                if last {
                    break;
                }
                from = cp;
                cp += step;
            }
            assert_eq!(bits(&got), bits(&full), "round {round}, step {step}");
        }
    }

    #[test]
    fn a_first_pick_is_lifted_by_a_sample_past_the_checkpoint() {
        // Replica 0 has nothing before the checkpoint, but its first sample
        // is earlier than replica 1's second one, and Go lifts replica 1 to
        // it on the first pick. Naming the first sample up front is what
        // lets the pick at 10s go out in the block that holds it, valued 500.
        let a = vec![(15_000, 500.)];
        let b = vec![(10_000, 5.), (20_000, 6.)];
        let want = reference(true, false, &[a.clone(), b.clone()]);
        let all: [(usize, &[(i64, f64)]); 2] = [(0, &a), (1, &b)];
        let mut merge = merge_over(Algorithm::Penalty, true, &all);
        let first = merge.advance(&[(0, &[]), (1, &b[..1])], 12_000);
        assert_eq!(first, [(10_000, 500.)]);
        let second = merge.advance(&[(0, &a), (1, &b[1..])], i64::MAX);
        assert_eq!([first, second].concat(), want);
    }

    #[test]
    fn the_preview_lifts_by_a_first_sample_beyond_its_samples() {
        // The block's own data holds replica 1 only; replica 0's first
        // sample is later still. The tail must be what the unbroken run
        // gives, which lifts replica 1 to it.
        let a = vec![(80_000, 500.), (90_000, 501.)];
        let b: Samples = (0..8).map(|i| (1_000 + i * 10_000, i as f64)).collect();
        let want = reference(true, false, &[a.clone(), b.clone()]);
        let all: [(usize, &[(i64, f64)]); 2] = [(0, &a), (1, &b)];
        let mut merge = merge_over(Algorithm::Penalty, true, &all);
        let block = merge.merge(&[(0, &[]), (1, &b)], 30_000);
        let in_reach: Samples = want
            .iter()
            .copied()
            .filter(|(t, _)| *t <= b.last().unwrap().0)
            .collect();
        assert_eq!(block, in_reach);
        assert_eq!(block[0], (1_000, 500.));
    }
}
