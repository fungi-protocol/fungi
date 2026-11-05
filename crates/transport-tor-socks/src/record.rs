//! Lightning TLV records (BOLT #1): a BigSize type, a BigSize length, and
//! that many bytes of payload.
//!
//! Decoding borrows payloads from its input. [`records`] splits bytes that
//! hold whole records, as in a record's payload; [`Buffer`] also takes
//! records split across inputs, as from a byte stream.

/// The longest a type and a length can be: two nine-byte BigSize integers.
const MAX_HEADER_LEN: usize = 18;

/// Malformed records.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    /// A record announced a length above the limit.
    #[error("record announces {len} bytes, above the limit of {max}")]
    TooLarge {
        /// Announced length in bytes.
        len: u64,
        /// Largest accepted length in bytes.
        max: u32,
    },
    /// A type or length is spelled in more bytes than it needs.
    #[error("non-minimal BigSize integer")]
    NonMinimal,
    /// The bytes end inside a record.
    #[error("bytes end inside a record")]
    Truncated,
}

/// The records, each as its type, its length and its payload.
pub fn encode<'a>(records: impl IntoIterator<Item = (u64, &'a [u8])>) -> Vec<u8> {
    let mut out = Vec::new();
    for (ty, payload) in records {
        put_bigsize(&mut out, ty);
        put_bigsize(&mut out, payload.len() as u64);
        out.extend_from_slice(payload);
    }
    out
}

/// Each record in `bytes`, which must end where a record ends.
pub fn records(mut bytes: &[u8]) -> impl Iterator<Item = Result<(u64, &[u8]), Error>> {
    std::iter::from_fn(move || {
        if bytes.is_empty() {
            return None;
        }
        let record = header(bytes).and_then(|header| {
            let (ty, len, rest) = header.ok_or(Error::Truncated)?;
            let payload = usize::try_from(len)
                .ok()
                .and_then(|len| rest.get(..len))
                .ok_or(Error::Truncated)?;
            bytes = &rest[payload.len()..];
            Ok((ty, payload))
        });
        if record.is_err() {
            bytes = &[];
        }
        Some(record)
    })
}

/// Records split across inputs, with payloads up to a limit.
///
/// A record that ends in a later input is copied into a part as large as the
/// limit, allocated the first time it is needed. [`Buffer::parse`] takes the
/// buffer and hands it back at the end of the input, so the part is allocated
/// once for the whole stream.
pub struct Buffer {
    max: u32,
    state: State,
    part: Fixed,
}

enum State {
    /// The first `len` bytes of a header.
    AwaitingHeader {
        bytes: [u8; MAX_HEADER_LEN],
        len: usize,
    },
    /// A header announced `len` bytes, the first of which are in `part`.
    AwaitingPayload {
        ty: u64,
        len: usize,
    },
    Failed(Error),
}

const BOUNDARY: State = State::AwaitingHeader {
    bytes: [0; MAX_HEADER_LEN],
    len: 0,
};

/// Where a complete payload is.
enum Payload<'a> {
    Input(&'a [u8]),
    Part,
}

impl Buffer {
    /// Take records whose payload is at most `max` bytes. Allocates that
    /// much once a record is split across inputs.
    pub fn new(max: u32) -> Self {
        Self {
            max,
            state: BOUNDARY,
            part: Fixed::new(max as usize),
        }
    }

    /// The records completed by `input`, after the one begun by earlier
    /// inputs, then the buffer itself. Bytes past the last complete record
    /// stay in the buffer for the next input. After a failure, each input
    /// yields that failure once.
    pub fn parse(self, input: &[u8]) -> Records<'_> {
        Records {
            input,
            buffer: Some(self),
            failed: false,
        }
    }

    /// Fails if the inputs ended inside a record, or earlier failed.
    pub fn end(&self) -> Result<(), Error> {
        match &self.state {
            State::AwaitingHeader { len: 0, .. } => Ok(()),
            State::Failed(error) => Err(error.clone()),
            _ => Err(Error::Truncated),
        }
    }

    /// The next record in `input`, or `None` once it is all taken.
    fn step<'a>(&mut self, input: &mut &'a [u8]) -> Result<Option<(u64, Payload<'a>)>, Error> {
        match self.state {
            State::Failed(ref error) => Err(error.clone()),
            State::AwaitingPayload { ty, len } => {
                let (now, rest) = input.split_at(input.len().min(len - self.part.len()));
                self.part.try_extend_from_slice(now).expect(FITS);
                *input = rest;
                if self.part.len() < len {
                    return Ok(None);
                }
                self.state = BOUNDARY;
                Ok(Some((ty, Payload::Part)))
            }
            State::AwaitingHeader { mut bytes, len } => {
                let take = input.len().min(MAX_HEADER_LEN - len);
                bytes[len..len + take].copy_from_slice(&input[..take]);
                let Some((ty, payload_len, rest)) = header(&bytes[..len + take])? else {
                    self.state = State::AwaitingHeader {
                        bytes,
                        len: len + take,
                    };
                    *input = &input[take..];
                    return Ok(None);
                };
                // Checked before any of the payload is awaited.
                if payload_len > u64::from(self.max) {
                    return Err(Error::TooLarge {
                        len: payload_len,
                        max: self.max,
                    });
                }
                *input = &input[take - rest.len()..];
                // At most `max`, so it fits.
                self.payload(input, ty, payload_len as usize)
            }
        }
    }

    /// The record whose header was just taken, or its start kept for later.
    fn payload<'a>(
        &mut self,
        input: &mut &'a [u8],
        ty: u64,
        len: usize,
    ) -> Result<Option<(u64, Payload<'a>)>, Error> {
        if let Some(payload) = input.get(..len) {
            *input = &input[len..];
            self.state = BOUNDARY;
            return Ok(Some((ty, Payload::Input(payload))));
        }
        self.part.clear();
        self.part.try_extend_from_slice(input).expect(FITS);
        *input = &[];
        self.state = State::AwaitingPayload { ty, len };
        Ok(None)
    }
}

/// Leaves out the bytes held.
impl std::fmt::Debug for Buffer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Buffer")
            .field("max", &self.max)
            .finish_non_exhaustive()
    }
}

/// What [`Records::next`] yields.
#[derive(Debug)]
pub enum Item<'r> {
    /// A record's type and payload.
    Record(u64, &'r [u8]),
    /// The end of the input, with the buffer to pass the next one to.
    Suspend(Buffer),
}

/// The records of one input, returned by [`Buffer::parse`].
///
/// Not an [`Iterator`], since a record completed in the buffer borrows it,
/// and so this, until the next call.
pub struct Records<'a> {
    input: &'a [u8],
    /// `None` once handed back.
    buffer: Option<Buffer>,
    failed: bool,
}

impl Records<'_> {
    /// The next record, a failure, or the buffer once the input is taken or
    /// has failed; then `None`.
    #[expect(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Option<Result<Item<'_>, Error>> {
        let buffer = self.buffer.as_mut()?;
        if self.failed {
            return self.buffer.take().map(Item::Suspend).map(Ok);
        }
        let before = self.input.len();
        let step = buffer.step(&mut self.input);
        // Each record takes a byte at least, so every input ends.
        debug_assert!(!matches!(step, Ok(Some(_))) || self.input.len() < before);
        match step {
            Ok(Some((ty, Payload::Input(payload)))) => Some(Ok(Item::Record(ty, payload))),
            Ok(Some((ty, Payload::Part))) => {
                let part = &self.buffer.as_ref()?.part;
                Some(Ok(Item::Record(ty, part.as_slice())))
            }
            Ok(None) => self.buffer.take().map(Item::Suspend).map(Ok),
            Err(error) => {
                self.failed = true;
                buffer.state = State::Failed(error.clone());
                Some(Err(error))
            }
        }
    }
}

/// A record's type and length, and the bytes after them.
type Header<'a> = (u64, u64, &'a [u8]);

/// The header at the start of `bytes`, or `None` while incomplete.
fn header(bytes: &[u8]) -> Result<Option<Header<'_>>, Error> {
    let Some((ty, rest)) = bigsize(bytes)? else {
        return Ok(None);
    };
    let Some((len, rest)) = bigsize(rest)? else {
        return Ok(None);
    };
    Ok(Some((ty, len, rest)))
}

/// The BigSize integer at the start of `bytes` and the bytes after it, or
/// `None` while it is incomplete.
fn bigsize(bytes: &[u8]) -> Result<Option<(u64, &[u8])>, Error> {
    let Some((&first, rest)) = bytes.split_first() else {
        return Ok(None);
    };
    let (width, floor) = match first {
        0xfd => (2, 0xfd),
        0xfe => (4, 0x1_0000),
        0xff => (8, 0x1_0000_0000),
        small => return Ok(Some((u64::from(small), rest))),
    };
    if rest.len() < width {
        return Ok(None);
    }
    let (digits, rest) = rest.split_at(width);
    let mut be = [0; 8];
    be[8 - width..].copy_from_slice(digits);
    let value = u64::from_be_bytes(be);
    if value < floor {
        return Err(Error::NonMinimal);
    }
    Ok(Some((value, rest)))
}

/// Append the minimal BigSize encoding of `value`.
fn put_bigsize(out: &mut Vec<u8>, value: u64) {
    let (prefix, width) = match value {
        0..=0xfc => (None, 1),
        0xfd..=0xffff => (Some(0xfd), 2),
        0x1_0000..=0xffff_ffff => (Some(0xfe), 4),
        _ => (Some(0xff), 8),
    };
    out.extend(prefix);
    out.extend_from_slice(&value.to_be_bytes()[8 - width..]);
}

/// Lengths are checked against the limit, the capacity of the part.
const FITS: &str = "a payload within the limit fits the part";

/// Bytes past the capacity of a [`Fixed`].
#[derive(Debug, PartialEq, Eq)]
struct Full;

/// Bytes up to a capacity fixed at creation, allocated on first use.
struct Fixed {
    capacity: usize,
    bytes: Option<Box<[u8]>>,
    len: usize,
}

impl Fixed {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            bytes: None,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn as_slice(&self) -> &[u8] {
        self.bytes
            .as_deref()
            .map_or(&[], |bytes| &bytes[..self.len])
    }

    fn clear(&mut self) {
        self.len = 0;
    }

    /// Append `more`, unless it does not fit.
    fn try_extend_from_slice(&mut self, more: &[u8]) -> Result<(), Full> {
        let end = self.len + more.len();
        if end > self.capacity {
            return Err(Full);
        }
        let capacity = self.capacity;
        let bytes = self
            .bytes
            .get_or_insert_with(|| vec![0; capacity].into_boxed_slice());
        bytes[self.len..end].copy_from_slice(more);
        self.len = end;
        Ok(())
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use proptest::prelude::*;

    use super::{Buffer, Error, Fixed, Full, Item, bigsize, encode, put_bigsize, records};

    const MAX: u32 = 16;

    type Owned = Vec<(u64, Vec<u8>)>;

    type Parsed = Vec<Result<(u64, Vec<u8>), Error>>;

    fn owned<'a>(
        records: impl IntoIterator<Item = Result<(u64, &'a [u8]), Error>>,
    ) -> Result<Owned, Error> {
        records
            .into_iter()
            .map(|record| record.map(|(ty, payload)| (ty, payload.to_vec())))
            .collect()
    }

    /// Every record in `bytes`. Each takes at least two bytes, so the bound
    /// only cuts short a split that never ends.
    fn split(bytes: &[u8]) -> Result<Owned, Error> {
        owned(records(bytes).take(bytes.len() / 2 + 1))
    }

    fn encoded(records: &Owned) -> Vec<u8> {
        encode(records.iter().map(|(ty, payload)| (*ty, &payload[..])))
    }

    /// What `input` yields, and the buffer it ends with. Each record takes at
    /// least a byte of it, and a failure and the buffer one item each, so the
    /// bound only cuts short a parse that never ends.
    fn drain(buffer: Buffer, input: &[u8]) -> (Parsed, Buffer) {
        let mut parsed = buffer.parse(input);
        let mut out = Vec::new();
        for _ in 0..input.len() + 2 {
            match parsed.next().expect("ends with the buffer") {
                Ok(Item::Record(ty, payload)) => out.push(Ok((ty, payload.to_vec()))),
                Ok(Item::Suspend(buffer)) => {
                    assert!(parsed.next().is_none());
                    return (out, buffer);
                }
                Err(error) => out.push(Err(error)),
            }
        }
        panic!("parse never ended");
    }

    /// Every record, then the end, of `bytes` passed in pieces cut at `cuts`.
    /// Stops at the first failure.
    fn cut(bytes: &[u8], cuts: &[usize]) -> Result<Owned, Error> {
        let mut buffer = Buffer::new(MAX);
        let mut out = Vec::new();
        let mut start = 0;
        for end in cuts.iter().copied().chain([bytes.len()]) {
            let parsed;
            (parsed, buffer) = drain(buffer, &bytes[start..end]);
            out.extend(parsed.into_iter().collect::<Result<Owned, _>>()?);
            start = end;
        }
        buffer.end()?;
        Ok(out)
    }

    /// BOLT #1 Appendix A, BigSize decoding tests.
    #[test]
    fn decodes_the_bigsize_test_vectors() {
        let valid: [(u64, &[u8]); 8] = [
            (0, &[0x00]),
            (252, &[0xfc]),
            (253, &[0xfd, 0x00, 0xfd]),
            (65535, &[0xfd, 0xff, 0xff]),
            (65536, &[0xfe, 0x00, 0x01, 0x00, 0x00]),
            (4294967295, &[0xfe, 0xff, 0xff, 0xff, 0xff]),
            (4294967296, &[0xff, 0, 0, 0, 0x01, 0, 0, 0, 0]),
            (u64::MAX, &[0xff; 9]),
        ];
        for (value, bytes) in valid {
            assert_eq!(bigsize(bytes).unwrap(), Some((value, &[][..])));
            let mut out = Vec::new();
            put_bigsize(&mut out, value);
            assert_eq!(out, bytes);
        }

        let non_minimal: [&[u8]; 3] = [
            &[0xfd, 0x00, 0xfc],
            &[0xfe, 0x00, 0x00, 0xff, 0xff],
            &[0xff, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff],
        ];
        for bytes in non_minimal {
            assert_eq!(bigsize(bytes), Err(Error::NonMinimal));
        }

        let short: [&[u8]; 7] = [
            &[],
            &[0xfd],
            &[0xfd, 0x00],
            &[0xfe],
            &[0xfe, 0xff, 0xff],
            &[0xff],
            &[0xff, 0xff, 0xff, 0xff, 0xff],
        ];
        for bytes in short {
            assert_eq!(bigsize(bytes).unwrap(), None);
        }
    }

    #[test]
    fn encodes_the_type_the_length_then_the_payload() {
        let bytes = encode([(2, &b"first"[..]), (253, b"")]);

        assert_eq!(bytes, b"\x02\x05first\xfd\x00\xfd\x00");
    }

    #[test]
    fn splits_records_in_order() {
        let bytes = encode([(2, &b"first"[..]), (1, b""), (2, b"second")]);

        assert_eq!(
            split(&bytes).unwrap(),
            [
                (2, b"first".to_vec()),
                (1, Vec::new()),
                (2, b"second".to_vec())
            ]
        );
    }

    #[test]
    fn bytes_ending_inside_a_record_are_truncated() {
        for whole in [encode([(2, &b"cut"[..])]), encode([(0xfd, &b"cut"[..])])] {
            for len in 1..whole.len() {
                let mut split = records(&whole[..len]);
                assert_eq!(split.next(), Some(Err(Error::Truncated)));
                assert_eq!(split.next(), None);
            }
        }
    }

    /// Whatever the length, bytes that end before it are truncated.
    #[test]
    fn a_length_past_the_bytes_is_truncated() {
        let mut split = records(&[0x02, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);

        assert_eq!(split.next(), Some(Err(Error::Truncated)));
        assert_eq!(split.next(), None);
    }

    #[test]
    fn splitting_refuses_a_non_minimal_length() {
        let mut split = records(&[0x02, 0xfd, 0x00, 0x01, b'x']);

        assert_eq!(split.next(), Some(Err(Error::NonMinimal)));
        assert_eq!(split.next(), None);
    }

    #[test]
    fn borrows_whole_records_from_the_input() {
        let bytes = encode([(1, &b"ab"[..]), (2, b"cd")]);
        let mut parsed = Buffer::new(MAX).parse(&bytes);

        let Some(Ok(Item::Record(_, payload))) = parsed.next() else {
            panic!("a record");
        };
        assert!(std::ptr::eq(payload, &bytes[2..4]));
    }

    /// The buffer comes back even when the input ends on a record boundary.
    #[test]
    fn ends_every_input_with_the_buffer() {
        let bytes = encode([(2, &b"whole"[..])]);

        let (parsed, buffer) = drain(Buffer::new(MAX), &bytes);

        assert_eq!(parsed, [Ok((2, b"whole".to_vec()))]);
        assert_eq!(buffer.end(), Ok(()));
        assert!(drain(buffer, &[]).0.is_empty());
    }

    #[test]
    fn takes_a_record_split_inside_a_multi_byte_type() {
        let bytes = encode([(0xfd, &b"x"[..])]);

        assert_eq!(cut(&bytes, &[1, 2]), Ok(vec![(0xfd, b"x".to_vec())]));
    }

    /// The record completed in the part is taken before the start of the next
    /// is copied over it.
    #[test]
    fn keeps_the_next_record_once_one_completes() {
        let bytes = encode([(2, &b"first"[..]), (2, b"second")]);

        let (parsed, buffer) = drain(Buffer::new(MAX), &bytes[..4]);
        assert!(parsed.is_empty());
        let (parsed, buffer) = drain(buffer, &bytes[4..10]);
        assert_eq!(parsed, [Ok((2, b"first".to_vec()))]);
        let (parsed, buffer) = drain(buffer, &bytes[10..]);
        assert_eq!(parsed, [Ok((2, b"second".to_vec()))]);
        assert_eq!(buffer.end(), Ok(()));
    }

    #[test]
    fn accepts_a_record_at_the_limit() {
        let payload = vec![7; MAX as usize];
        let bytes = encode([(2, &payload[..])]);

        assert_eq!(cut(&bytes, &[3]), Ok(vec![(2, payload)]));
    }

    /// The announced length is refused before any of the payload arrives, so
    /// a peer cannot make the receiver hold what it announces.
    #[test]
    fn refuses_a_length_above_the_limit() {
        let mut header = Vec::new();
        put_bigsize(&mut header, 2);
        put_bigsize(&mut header, u64::from(MAX) + 1);
        let (parsed, _) = drain(Buffer::new(MAX), &header);

        assert_eq!(parsed, [Err(Error::TooLarge { len: 17, max: MAX })]);
    }

    /// After a failure the position in the stream is unknown, so bytes that
    /// follow must not be read as records.
    #[test]
    fn yields_the_failure_once_per_input_afterwards() {
        let bytes = [&[0x02, 0xfd, 0x00, 0x01][..], &encode([(2, &b"next"[..])])].concat();
        let (parsed, buffer) = drain(Buffer::new(MAX), &bytes);
        assert_eq!(parsed, [Err(Error::NonMinimal)]);

        let (after, buffer) = drain(buffer, &encode([(2, &b"next"[..])]));

        assert_eq!(after, [Err(Error::NonMinimal)]);
        assert_eq!(buffer.end(), Err(Error::NonMinimal));
    }

    #[test]
    fn ending_inside_a_record_is_truncated() {
        let whole = encode([(2, &b"cut"[..])]);

        for len in 1..whole.len() {
            let (parsed, buffer) = drain(Buffer::new(MAX), &whole[..len]);
            assert!(parsed.is_empty());
            assert_eq!(buffer.end(), Err(Error::Truncated));
        }
    }

    #[test]
    fn debug_output_omits_held_bytes() {
        let (_, buffer) = drain(Buffer::new(MAX), b"\x02\x06secr");

        assert_eq!(format!("{buffer:?}"), "Buffer { max: 16, .. }");
    }

    /// Whole records need no storage, so the limit costs nothing until a
    /// record is split; from then on the same part is reused.
    #[test]
    fn allocates_only_for_a_split_record() {
        let bytes = encode([(2, &b"first"[..]), (2, b"second"), (2, b"third")]);

        let (_, buffer) = drain(Buffer::new(MAX), &bytes[..8]);
        assert!(buffer.part.bytes.is_none());

        let (_, buffer) = drain(buffer, &bytes[8..10]);
        let allocated = buffer.part.bytes.as_deref().map(<[u8]>::as_ptr);
        assert!(allocated.is_some());

        let (_, buffer) = drain(buffer, &bytes[10..18]);
        assert_eq!(buffer.part.bytes.as_deref().map(<[u8]>::as_ptr), allocated);
    }

    #[test]
    fn fixed_refuses_bytes_past_its_capacity() {
        let mut fixed = Fixed::new(2);

        assert_eq!(fixed.try_extend_from_slice(&[1, 2, 3]), Err(Full));
        assert!(fixed.bytes.is_none());
        assert_eq!(fixed.as_slice(), []);
        assert_eq!(fixed.try_extend_from_slice(&[1]), Ok(()));
        assert_eq!(fixed.try_extend_from_slice(&[2, 3]), Err(Full));
        assert_eq!(fixed.try_extend_from_slice(&[2]), Ok(()));
        assert_eq!(fixed.as_slice(), [1, 2]);
    }

    fn within_limit() -> impl Strategy<Value = Owned> {
        let payload = prop::collection::vec(any::<u8>(), 0..=MAX as usize);
        prop::collection::vec((any::<u64>(), payload), 0..8)
    }

    /// Cut points within `len`, in order.
    fn cuts(len: usize, picks: Vec<prop::sample::Index>) -> Vec<usize> {
        let mut cuts: Vec<usize> = picks.iter().map(|pick| pick.index(len + 1)).collect();
        cuts.sort_unstable();
        cuts
    }

    proptest! {
        #[test]
        fn splits_what_was_encoded(records in within_limit()) {
            prop_assert_eq!(split(&encoded(&records)), Ok(records));
        }

        /// Fragmenting or concatenating the encoded bytes changes nothing.
        #[test]
        fn takes_what_was_encoded_however_cut(
            records in within_limit(),
            picks in prop::collection::vec(any::<prop::sample::Index>(), 0..12),
        ) {
            let bytes = encoded(&records);
            prop_assert_eq!(cut(&bytes, &cuts(bytes.len(), picks)), Ok(records));
        }

        /// Whatever the bytes, the cuts change neither the records nor the
        /// failure.
        #[test]
        fn arbitrary_bytes_parse_the_same_however_cut(
            bytes in prop::collection::vec(any::<u8>(), 0..64),
            picks in prop::collection::vec(any::<prop::sample::Index>(), 0..12),
        ) {
            prop_assert_eq!(cut(&bytes, &cuts(bytes.len(), picks)), cut(&bytes, &[]));
        }

        /// One spelling per integer: whatever decodes re-encodes to the bytes
        /// it took.
        #[test]
        fn decoded_bigsize_is_minimal(bytes in prop::collection::vec(any::<u8>(), 0..10)) {
            if let Ok(Some((value, rest))) = bigsize(&bytes) {
                let mut out = Vec::new();
                put_bigsize(&mut out, value);
                prop_assert_eq!(&out[..], &bytes[..bytes.len() - rest.len()]);
            }
        }

        #[test]
        fn encoded_bigsize_decodes_to_itself(value in any::<u64>()) {
            let mut out = Vec::new();
            put_bigsize(&mut out, value);
            prop_assert_eq!(bigsize(&out), Ok(Some((value, &[][..]))));
        }
    }
}
