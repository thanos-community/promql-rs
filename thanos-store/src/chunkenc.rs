//! Prometheus's XOR chunk codec, a port of `tsdb/chunkenc/{bstream,xor}.go`.
//!
//! `storepb.Chunk.data` with `Encoding::Xor` is the whole chunk as
//! Prometheus stores it, header included:
//!
//! ```text
//! u16 big-endian sample count
//! sample 0:  zigzag varint t0, then the 64 raw bits of the f64
//! sample 1:  unsigned varint (t1 - t0), then an XOR-coded value
//! sample n:  delta-of-delta with prefix 0 | 10 + 14 bits | 110 + 17
//!            | 1110 + 20 | 1111 + 64, then an XOR-coded value
//! ```
//!
//! Bits are written most significant first. An XOR-coded value is `0` when
//! the value repeats; `10` plus the significant bits when the previous
//! leading and trailing zero counts still cover the XOR; or `11`, five bits
//! of leading zeros, six bits of significant-bit count (0 means 64) and the
//! bits themselves.
//!
//! Values travel as bit patterns (`f64::to_bits`/`from_bits`), never
//! through arithmetic, so a stale marker keeps its exact payload.

/// Prometheus's `value.StaleNaN`: the NaN payload that marks a series as
/// gone at a timestamp. Equal by bits only; `==` is false for every NaN.
pub const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// Bytes of the sample-count header.
const HEADER_SIZE: usize = 2;

/// Why a chunk did not decode.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ChunkError {
    /// The data is too short to even carry the sample count.
    #[error("chunk is {0} bytes, shorter than its 2-byte header")]
    Header(usize),
    /// The bit stream ended before the header's sample count was reached.
    #[error("chunk ended after {read} of {total} samples")]
    Truncated { read: u16, total: u16 },
    /// A timestamp varint did not fit 64 bits.
    #[error("varint overflows 64 bits after {read} of {total} samples")]
    Varint { read: u16, total: u16 },
    /// Leading zeros plus significant bits of a value exceed 64.
    #[error("invalid value bit counts after {read} of {total} samples")]
    Corrupt { read: u16, total: u16 },
}

/// A read failure before the iterator knows which sample it was on.
enum Fault {
    Eof,
    Varint,
    Corrupt,
}

/// `bstreamReader`: bits most significant first.
struct BitReader<'a> {
    data: &'a [u8],
    /// Bits consumed so far.
    pos: usize,
}

impl<'a> BitReader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn read_bit(&mut self) -> Result<bool, Fault> {
        let byte = *self.data.get(self.pos / 8).ok_or(Fault::Eof)?;
        let bit = (byte >> (7 - self.pos % 8)) & 1 == 1;
        self.pos += 1;
        Ok(bit)
    }

    /// The next `n` bits, `n <= 64`, as the low bits of a `u64`.
    fn read_bits(&mut self, n: u8) -> Result<u64, Fault> {
        if n > 64 {
            return Err(Fault::Corrupt);
        }
        if self.pos + usize::from(n) > self.data.len() * 8 {
            return Err(Fault::Eof);
        }
        let mut out = 0u64;
        let mut remaining = u32::from(n);
        while remaining > 0 {
            let byte = u32::from(self.data[self.pos / 8]);
            let offset = (self.pos % 8) as u32;
            let take = (8 - offset).min(remaining);
            let bits = (byte >> (8 - offset - take)) & ((1 << take) - 1);
            out = (out << take) | u64::from(bits);
            self.pos += take as usize;
            remaining -= take;
        }
        Ok(out)
    }

    fn read_byte(&mut self) -> Result<u8, Fault> {
        self.read_bits(8).map(|b| b as u8)
    }

    /// Go's `binary.ReadUvarint`: LEB128, at most ten bytes.
    fn read_uvarint(&mut self) -> Result<u64, Fault> {
        let mut x = 0u64;
        let mut shift = 0u32;
        for i in 0..10 {
            let b = self.read_byte()?;
            if b < 0x80 {
                if i == 9 && b > 1 {
                    return Err(Fault::Varint);
                }
                return Ok(x | u64::from(b) << shift);
            }
            x |= u64::from(b & 0x7f) << shift;
            shift += 7;
        }
        Err(Fault::Varint)
    }

    /// Go's `binary.ReadVarint`: zigzag over the unsigned form.
    fn read_varint(&mut self) -> Result<i64, Fault> {
        let ux = self.read_uvarint()?;
        let x = (ux >> 1) as i64;
        Ok(if ux & 1 == 1 { !x } else { x })
    }
}

/// `xorIterator`: the samples of one chunk, in order. Stops at the first
/// error and yields nothing after it.
pub struct XorIterator<'a> {
    reader: BitReader<'a>,
    total: u16,
    read: u16,
    t: i64,
    val_bits: u64,
    leading: u8,
    trailing: u8,
    t_delta: u64,
}

impl<'a> XorIterator<'a> {
    /// Over a whole chunk, header included.
    pub fn new(data: &'a [u8]) -> Result<Self, ChunkError> {
        let (header, body) = data
            .split_at_checked(HEADER_SIZE)
            .ok_or(ChunkError::Header(data.len()))?;
        Ok(Self {
            reader: BitReader::new(body),
            total: u16::from_be_bytes([header[0], header[1]]),
            read: 0,
            t: 0,
            val_bits: 0,
            leading: 0,
            trailing: 0,
            t_delta: 0,
        })
    }

    /// The sample count from the header.
    pub fn num_samples(&self) -> u16 {
        self.total
    }

    fn fault(&self, fault: Fault) -> ChunkError {
        let (read, total) = (self.read, self.total);
        match fault {
            Fault::Eof => ChunkError::Truncated { read, total },
            Fault::Varint => ChunkError::Varint { read, total },
            Fault::Corrupt => ChunkError::Corrupt { read, total },
        }
    }

    /// `xorIterator.Next` for one sample.
    fn step(&mut self) -> Result<(), Fault> {
        match self.read {
            0 => {
                self.t = self.reader.read_varint()?;
                self.val_bits = self.reader.read_bits(64)?;
            }
            1 => {
                self.t_delta = self.reader.read_uvarint()?;
                self.t = self.t.wrapping_add(self.t_delta as i64);
                self.read_value()?;
            }
            _ => {
                let mut d = 0u8;
                for _ in 0..4 {
                    d <<= 1;
                    if !self.reader.read_bit()? {
                        break;
                    }
                    d |= 1;
                }
                let dod = match d {
                    0b0 => 0,
                    0b10 => self.read_dod(14)?,
                    0b110 => self.read_dod(17)?,
                    0b1110 => self.read_dod(20)?,
                    _ => self.reader.read_bits(64)? as i64,
                };
                self.t_delta = (self.t_delta as i64).wrapping_add(dod) as u64;
                self.t = self.t.wrapping_add(self.t_delta as i64);
                self.read_value()?;
            }
        }
        self.read += 1;
        Ok(())
    }

    /// A delta-of-delta of `size` bits; negative numbers come back as high
    /// unsigned numbers, see Prometheus's `docs/bstream.md`.
    fn read_dod(&mut self, size: u8) -> Result<i64, Fault> {
        let mut bits = self.reader.read_bits(size)?;
        if bits > 1 << (size - 1) {
            bits = bits.wrapping_sub(1 << size);
        }
        Ok(bits as i64)
    }

    /// `xorRead`.
    fn read_value(&mut self) -> Result<(), Fault> {
        if !self.reader.read_bit()? {
            return Ok(());
        }
        let (mbits, trailing) = if !self.reader.read_bit()? {
            (64 - self.leading - self.trailing, self.trailing)
        } else {
            let leading = self.reader.read_bits(5)? as u8;
            let mut mbits = self.reader.read_bits(6)? as u8;
            if mbits == 0 {
                mbits = 64;
            }
            if u32::from(leading) + u32::from(mbits) > 64 {
                return Err(Fault::Corrupt);
            }
            let trailing = 64 - leading - mbits;
            self.leading = leading;
            self.trailing = trailing;
            (mbits, trailing)
        };
        let bits = self.reader.read_bits(mbits)?;
        self.val_bits ^= bits << trailing;
        Ok(())
    }
}

impl Iterator for XorIterator<'_> {
    type Item = Result<(i64, f64), ChunkError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.read >= self.total {
            return None;
        }
        match self.step() {
            Ok(()) => Some(Ok((self.t, f64::from_bits(self.val_bits)))),
            Err(fault) => {
                let err = self.fault(fault);
                self.read = self.total;
                Some(Err(err))
            }
        }
    }
}

/// Every sample of a chunk, header included, as `(timestamp_ms, value)`.
pub fn decode_xor(data: &[u8]) -> Result<Vec<(i64, f64)>, ChunkError> {
    let iter = XorIterator::new(data)?;
    let mut out = Vec::with_capacity(usize::from(iter.num_samples()));
    for sample in iter {
        out.push(sample?);
    }
    Ok(out)
}

/// `bstream` as a writer.
struct BitWriter {
    stream: Vec<u8>,
    /// Free low bits in the last byte.
    count: u8,
}

impl BitWriter {
    fn write_bit(&mut self, bit: bool) {
        if self.count == 0 {
            self.stream.push(0);
            self.count = 8;
        }
        if bit {
            *self.stream.last_mut().expect("just pushed") |= 1 << (self.count - 1);
        }
        self.count -= 1;
    }

    fn write_byte(&mut self, byte: u8) {
        if self.count == 0 {
            self.stream.push(byte);
            return;
        }
        // Complete the last byte with the leftmost `count` bits, then start
        // a new one with the rest; the free count stays the same.
        *self.stream.last_mut().expect("non-empty") |= byte >> (8 - self.count);
        self.stream.push(byte << self.count);
    }

    /// The `nbits` low bits of `u`, high bit first.
    fn write_bits(&mut self, mut u: u64, mut nbits: u32) {
        if nbits == 0 {
            return;
        }
        u <<= 64 - nbits;
        while nbits >= 8 {
            self.write_byte((u >> 56) as u8);
            u <<= 8;
            nbits -= 8;
        }
        while nbits > 0 {
            self.write_bit(u >> 63 == 1);
            u <<= 1;
            nbits -= 1;
        }
    }
}

/// `xorAppender`: builds one chunk sample by sample. Timestamps should
/// ascend, as in Prometheus, though the format itself does not require it.
pub struct XorAppender {
    w: BitWriter,
    num: u16,
    t: i64,
    v_bits: u64,
    t_delta: u64,
    leading: u8,
    trailing: u8,
}

impl Default for XorAppender {
    fn default() -> Self {
        Self::new()
    }
}

impl XorAppender {
    pub fn new() -> Self {
        Self {
            w: BitWriter {
                stream: vec![0; HEADER_SIZE],
                count: 0,
            },
            num: 0,
            t: i64::MIN,
            v_bits: 0,
            t_delta: 0,
            // "Unset": the first XOR-coded value always writes its own
            // leading/trailing counts.
            leading: 0xff,
            trailing: 0,
        }
    }

    /// `xorAppender.Append`. A chunk holds at most `u16::MAX` samples.
    pub fn append(&mut self, t: i64, v: f64) {
        let v_bits = v.to_bits();
        let mut t_delta = 0u64;
        match self.num {
            0 => {
                for b in varint_bytes(t) {
                    self.w.write_byte(b);
                }
                self.w.write_bits(v_bits, 64);
            }
            1 => {
                t_delta = t.wrapping_sub(self.t) as u64;
                for b in uvarint_bytes(t_delta) {
                    self.w.write_byte(b);
                }
                self.write_vdelta(v_bits);
            }
            _ => {
                t_delta = t.wrapping_sub(self.t) as u64;
                let dod = t_delta.wrapping_sub(self.t_delta) as i64;
                // Gorilla has a max resolution of seconds, Prometheus
                // milliseconds, hence the larger buckets.
                if dod == 0 {
                    self.w.write_bit(false);
                } else if bit_range(dod, 14) {
                    // 0b10 size code combined with 6 bits of dod, then the
                    // bottom 8 bits.
                    self.w.write_byte(0b10 << 6 | ((dod >> 8) as u8 & 0x3f));
                    self.w.write_byte(dod as u8);
                } else if bit_range(dod, 17) {
                    self.w.write_bits(0b110, 3);
                    self.w.write_bits(dod as u64, 17);
                } else if bit_range(dod, 20) {
                    self.w.write_bits(0b1110, 4);
                    self.w.write_bits(dod as u64, 20);
                } else {
                    self.w.write_bits(0b1111, 4);
                    self.w.write_bits(dod as u64, 64);
                }
                self.write_vdelta(v_bits);
            }
        }
        self.t = t;
        self.v_bits = v_bits;
        self.t_delta = t_delta;
        self.num += 1;
        self.w.stream[..HEADER_SIZE].copy_from_slice(&self.num.to_be_bytes());
    }

    /// `xorWrite`.
    fn write_vdelta(&mut self, new_bits: u64) {
        let delta = new_bits ^ self.v_bits;
        if delta == 0 {
            self.w.write_bit(false);
            return;
        }
        self.w.write_bit(true);

        // Clamp the leading zeros to five bits' worth.
        let new_leading = (delta.leading_zeros() as u8).min(31);
        let new_trailing = delta.trailing_zeros() as u8;

        if self.leading != 0xff && new_leading >= self.leading && new_trailing >= self.trailing {
            // Stick with the current leading/trailing.
            self.w.write_bit(false);
            self.w.write_bits(
                delta >> self.trailing,
                64 - u32::from(self.leading) - u32::from(self.trailing),
            );
            return;
        }

        self.leading = new_leading;
        self.trailing = new_trailing;
        self.w.write_bit(true);
        self.w.write_bits(u64::from(new_leading), 5);
        // sigbits == 64 does not fit six bits; it is written as 0 and the
        // reader turns 0 back into 64. 0 itself never occurs: that is the
        // delta == 0 case above.
        let sigbits = 64 - new_leading - new_trailing;
        self.w.write_bits(u64::from(sigbits), 6);
        self.w.write_bits(delta >> new_trailing, u32::from(sigbits));
    }

    /// The finished chunk, header included.
    pub fn finish(self) -> Vec<u8> {
        self.w.stream
    }
}

/// Whether `x` fits `nbits` in the encoder's asymmetric range.
fn bit_range(x: i64, nbits: u8) -> bool {
    -((1i64 << (nbits - 1)) - 1) <= x && x <= 1i64 << (nbits - 1)
}

/// Go's `binary.PutUvarint`.
fn uvarint_bytes(mut x: u64) -> Vec<u8> {
    let mut out = Vec::with_capacity(10);
    while x >= 0x80 {
        out.push(x as u8 | 0x80);
        x >>= 7;
    }
    out.push(x as u8);
    out
}

/// Go's `binary.PutVarint`: zigzag, then unsigned.
fn varint_bytes(x: i64) -> Vec<u8> {
    let mut ux = (x as u64) << 1;
    if x < 0 {
        ux = !ux;
    }
    uvarint_bytes(ux)
}

/// One chunk holding `samples`, header included.
pub fn encode_xor(samples: &[(i64, f64)]) -> Vec<u8> {
    let mut appender = XorAppender::new();
    for &(t, v) in samples {
        appender.append(t, v);
    }
    appender.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bits_equal(a: &[(i64, f64)], b: &[(i64, f64)]) -> bool {
        a.len() == b.len()
            && a.iter()
                .zip(b)
                .all(|(x, y)| x.0 == y.0 && x.1.to_bits() == y.1.to_bits())
    }

    #[test]
    fn a_short_chunk_is_a_header_error() {
        assert_eq!(decode_xor(&[]), Err(ChunkError::Header(0)));
        assert_eq!(decode_xor(&[7]), Err(ChunkError::Header(1)));
    }

    #[test]
    fn an_empty_chunk_has_no_samples() {
        assert_eq!(encode_xor(&[]), vec![0, 0]);
        assert_eq!(decode_xor(&[0, 0]), Ok(vec![]));
    }

    #[test]
    fn one_zero_sample_is_the_known_bytes() {
        // Header 0001, varint(0) = 00, then 64 zero bits.
        let mut want = vec![0, 1, 0];
        want.extend([0; 8]);
        assert_eq!(encode_xor(&[(0, 0.0)]), want);
        assert_eq!(decode_xor(&want), Ok(vec![(0, 0.0)]));
    }

    #[test]
    fn a_regular_counter_roundtrips() {
        let samples: Vec<(i64, f64)> = (0..120)
            .map(|i| (1_700_000_000_000 + i * 15_000, (i * i) as f64 * 0.5))
            .collect();
        let chunk = encode_xor(&samples);
        assert_eq!(&chunk[..2], &120u16.to_be_bytes());
        assert!(bits_equal(&decode_xor(&chunk).unwrap(), &samples));
    }

    #[test]
    fn every_delta_of_delta_bucket_roundtrips() {
        // Deltas chosen so the dod lands in each prefix code, both signs.
        let deltas: [i64; 12] = [
            15_000,
            15_100,         // +100      -> 14 bits
            14_900,         // -200      -> 14 bits
            8_192 + 14_900, // +8192     -> the top of the 14-bit range
            55_000,         // +31908    -> 17 bits
            14_900,         // -40100    -> 17 bits
            415_000,        // +400100   -> 20 bits
            14_900,         // -400100   -> 20 bits
            1 << 40,        // huge      -> 64 bits
            15_000,         // -(1<<40)  -> 64 bits
            15_000,         // 0
            15_000,         // 0
        ];
        let mut t = -5_000;
        let mut samples = vec![(t, 1.0)];
        for d in deltas {
            t += d;
            samples.push((t, 1.0));
        }
        let chunk = encode_xor(&samples);
        assert!(bits_equal(&decode_xor(&chunk).unwrap(), &samples));
    }

    #[test]
    fn special_values_keep_their_bits() {
        let samples = [
            (0, f64::NAN),
            (1_000, f64::from_bits(0x7ff8_0000_0000_0001)),
            (2_000, f64::from_bits(STALE_NAN_BITS)),
            (3_000, f64::INFINITY),
            (4_000, f64::NEG_INFINITY),
            (5_000, 0.0),
            (6_000, -0.0),
            (7_000, 1.0),
            // XOR with 64 significant bits: written as sigbits 0.
            (8_000, f64::from_bits(0x8000_0000_0000_0001)),
            // XOR of 1: 63 leading zeros, clamped to 31.
            (9_000, f64::from_bits(0x8000_0000_0000_0000)),
            (10_000, f64::from_bits(0x8000_0000_0000_0000)),
        ];
        let chunk = encode_xor(&samples);
        assert!(bits_equal(&decode_xor(&chunk).unwrap(), &samples));
    }

    #[test]
    fn a_truncated_chunk_reports_the_samples_it_got() {
        let chunk = encode_xor(&[(0, 1.0), (15_000, 2.0), (30_000, 3.0)]);
        let cut = &chunk[..chunk.len() - 2];
        let err = decode_xor(cut).unwrap_err();
        assert!(
            matches!(err, ChunkError::Truncated { total: 3, read } if read < 3),
            "{err:?}"
        );
        assert!(decode_xor(&chunk[..3]).is_err());
    }

    #[test]
    fn varints_match_go() {
        assert_eq!(uvarint_bytes(0), [0]);
        assert_eq!(uvarint_bytes(300), [0xac, 0x02]);
        assert_eq!(varint_bytes(0), [0]);
        assert_eq!(varint_bytes(-1), [1]);
        assert_eq!(varint_bytes(1), [2]);
        assert_eq!(varint_bytes(-2), [3]);
    }

    /// Fixtures written by Prometheus's own `chunkenc` through
    /// `scripts/gen-xor-fixtures`; values are the f64 bits in hex.
    #[derive(serde::Deserialize)]
    struct Fixture {
        name: String,
        hex: String,
        samples: Vec<FixtureSample>,
    }

    #[derive(serde::Deserialize)]
    struct FixtureSample {
        t: i64,
        v: String,
    }

    fn fixtures() -> Vec<Fixture> {
        let json = include_str!("../testdata/xor_chunks.json");
        serde_json::from_str(json).expect("fixture file parses")
    }

    fn unhex(s: &str) -> Vec<u8> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16).expect("hex byte"))
            .collect()
    }

    #[test]
    fn go_fixtures_decode() {
        let fixtures = fixtures();
        assert!(fixtures.len() > 5);
        for f in &fixtures {
            let want: Vec<(i64, f64)> = f
                .samples
                .iter()
                .map(|s| {
                    (
                        s.t,
                        f64::from_bits(u64::from_str_radix(&s.v, 16).expect("bits")),
                    )
                })
                .collect();
            let got = decode_xor(&unhex(&f.hex)).unwrap_or_else(|e| panic!("{}: {e}", f.name));
            assert!(bits_equal(&got, &want), "{}: {got:?} != {want:?}", f.name);
        }
    }

    #[test]
    fn go_fixtures_reencode_byte_for_byte() {
        for f in fixtures() {
            let samples: Vec<(i64, f64)> = f
                .samples
                .iter()
                .map(|s| {
                    (
                        s.t,
                        f64::from_bits(u64::from_str_radix(&s.v, 16).expect("bits")),
                    )
                })
                .collect();
            assert_eq!(encode_xor(&samples), unhex(&f.hex), "{}", f.name);
        }
    }
}
