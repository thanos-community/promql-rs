//! Resumable replica deduplication, Thanos's `pkg/dedup`, as a kernel for
//! merging replicated series across overlapping blocks.

mod exec;
mod iter;
mod node;

pub(crate) use node::DedupNode;

/// The label the source tags every row with once it strips the replica
/// labels from the stores' answers. With the replica labels gone the
/// overlap split (`NewOverlapSplit` in Thanos `pkg/dedup`) is the only
/// replica identity there is, and it has to run over whole chunks before
/// the per-block decode clips them: split on clipped samples, a replica's
/// chunk could fall to another slot in every block and the merge would see
/// a different replica each time.
///
/// The value is the row's slot as a decimal index, then, for every slot of
/// the series in order, `;` and that replica's first sample as Go's
/// iterator for it yields it, `<ms>:<f64 bits in hex>`, or nothing for a
/// slot with none. Go's merge reads those wherever they lie in the query,
/// which a block-by-block merge cannot see from its own rows; the source
/// knows every chunk of the select before it decodes the first block.
pub(crate) const REPLICA_SLOT_LABEL: &str = "__replica_slot__";

/// A slot's first sample: timestamp and value.
pub(crate) type First = (i64, f64);

/// The `;`-led part of a [`REPLICA_SLOT_LABEL`] value that all rows of a
/// series share, for `firsts[slot]`.
pub(crate) fn encode_firsts(firsts: &[Option<First>]) -> String {
    let mut out = String::new();
    for first in firsts {
        out.push(';');
        if let Some((t, v)) = first {
            out.push_str(&format!("{t}:{:x}", v.to_bits()));
        }
    }
    out
}

/// The row's slot in a [`REPLICA_SLOT_LABEL`] value.
pub(crate) fn slot_index(value: &str) -> Result<usize, String> {
    let slot = value.split_once(';').map_or(value, |(slot, _)| slot);
    slot.parse()
        .map_err(|_| format!("replica slot {value:?} does not start with an index"))
}

/// A [`REPLICA_SLOT_LABEL`] value taken apart: the row's slot, and the
/// `(slot, first sample)` of every slot that has one.
pub(crate) fn decode_slot(value: &str) -> Result<(usize, Vec<(usize, First)>), String> {
    let bad = || format!("replica slot {value:?} has a malformed first sample");
    let slot = slot_index(value)?;
    let mut firsts = Vec::new();
    let Some((_, rest)) = value.split_once(';') else {
        return Ok((slot, firsts));
    };
    for (i, part) in rest.split(';').enumerate() {
        if part.is_empty() {
            continue;
        }
        let (t, bits) = part.split_once(':').ok_or_else(bad)?;
        let t = t.parse().map_err(|_| bad())?;
        let bits = u64::from_str_radix(bits, 16).map_err(|_| bad())?;
        firsts.push((i, (t, f64::from_bits(bits))));
    }
    Ok((slot, firsts))
}

pub(crate) use iter::{is_counter, Algorithm, ReplicaMerge};

/// `--query-deduplication-func`: how the samples of replicas are merged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum DeduplicationFunc {
    /// Follow one replica and penalise switching: `dedup.AlgorithmPenalty`.
    #[default]
    Penalty,
    /// Union the samples one to one, the first replica winning a shared
    /// timestamp: `dedup.AlgorithmChain`, Prometheus's `ChainedSeriesMerge`.
    Chain,
}

impl DeduplicationFunc {
    pub(crate) fn as_algorithm(self) -> Algorithm {
        match self {
            Self::Penalty => Algorithm::Penalty,
            Self::Chain => Algorithm::Chain,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_value_round_trips_with_its_first_samples() {
        let stale = f64::from_bits(0x7ff0_0000_0000_0002);
        let firsts = [None, Some((1_791_000_000_123, 0.1)), Some((-5, stale))];
        let value = format!("2{}", encode_firsts(&firsts));
        let (slot, got) = decode_slot(&value).unwrap();
        assert_eq!(slot, 2);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0], (1, (1_791_000_000_123, 0.1)));
        assert_eq!((got[1].0, (got[1].1).0), (2, -5));
        assert_eq!((got[1].1).1.to_bits(), stale.to_bits());
        assert_eq!(decode_slot("3").unwrap(), (3, Vec::new()));
        assert!(decode_slot("x;1:0").is_err());
        assert!(decode_slot("0;1").is_err());
    }
}
