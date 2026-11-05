//! This module parses Lightning BOLT 1 TLV records ([`tlv`]) from a byte stream
//! that arrives in chunks of arbitrary size. The byte stream is not necessarily
//! a TLV stream, so the TLV field order requirement is not enforced.
//!
//! The bytestream input is considered potentially adversarial. A maximum record
//! size is enforced, and allocations are bounded to no more than this size.
//!
//! When chunks coincide with TLV boundaries the parsed payloads are simply
//! sliced from the inputs. If chunking results in an incomplete TLV record,
//! this suspends parsing. As new input data becomes available, parsing can be
//! resumed.
//!
//! Parsed output is given as the [`Records`] lending iterator. A lending
//! iterator is an `Iterator` variant that allows the items to be borrowed one
//! at a time. [`Records::next`](LendingIterator::next) yields each record in
//! turn, borrowing directly from the input where possible and from the buffer
//! only as necessary. When the input is used up, it yields the parse state for
//! the next input (which also avoids freeing and reallocating the buffer).

use crate::tlv::{self, Error, MAX_HEADER_LEN, TypedSlice};

/// A parser for concatenated TLV records can be in one of two states: reading
/// the header of a TLV record (the type and length) or reading the value
/// itself.
///
/// `N` limits the payload length and must be at least [`MAX_HEADER_LEN`].
/// Payloads contained in one input chunk borrow from that input. If a TLV spans
/// more than one input chunk, it is buffered in a fixed `N`-byte array, which
/// is allocated at most once.
///
/// Since a TLV record may have an empty value, the Payload state may not
/// advance the input position, but the Header state necessarily consumes
/// additional input.
pub(crate) enum State<const N: usize> {
    Header(Buffer<N>),
    Payload {
        buffer: Buffer<N>,
        ty: u64,
        len: usize,
    },
}

/// A fixed size, lazily allocated buffer for incomplete TLV headers or payloads.
#[derive(Default)]
pub(crate) struct Buffer<const N: usize> {
    array: Option<Box<[u8; N]>>,
    held: usize,
}

/// A lending iterator for TLV records.
///
/// Each [`next`](LendingIterator::next) yields one record. A payload that lies
/// entirely within the input will be borrowed from the input. A payload that
/// spans multiple inputs is lent from the buffer array. When the input is used
/// up, `next` yields [`Item::Incomplete`] with the parse state for the next
/// input. An error ends the iteration and drops the parse state.
pub(crate) struct Records<'a, const N: usize> {
    /// The unparsed rest of the input.
    input: &'a [u8],
    /// The state is `None` once it is handed back, or dropped by an error.
    state: Option<State<N>>,
}

pub(crate) enum Item<T, const N: usize> {
    Record(TypedSlice<T>),
    Incomplete(State<N>),
}

/// An `Iterator` like trait where the `Item`s borrow from the iterator.
pub(crate) trait LendingIterator {
    type Item<'b>
    where
        Self: 'b;

    fn next(&mut self) -> Option<Self::Item<'_>>;
}

impl<const N: usize> Default for State<N> {
    fn default() -> Self {
        Self::Header(Buffer::default())
    }
}

impl<const N: usize> State<N> {
    /// Resume parsing with `input`. The records are yielded by the returned [`Records`].
    pub fn resume(self, input: &[u8]) -> Records<'_, N> {
        // The maximum payload size must be enough to contain a TLV header
        const { assert!(N >= MAX_HEADER_LEN) };

        Records {
            input,
            state: Some(self),
        }
    }

    /// Returns `Truncated` if the stream ended inside a record.
    pub fn end(&self) -> Result<(), Error> {
        match self {
            Self::Header(Buffer { held: 0, .. }) => Ok(()),
            _ => Err(Error::Truncated),
        }
    }

    /// Reads one TLV record, or retains its incomplete bytes for the next
    /// input.
    fn read<'a>(self, new_input: &mut &'a [u8]) -> Result<Item<ContiguousInput<'a, N>, N>, Error> {
        match self {
            // Try to parse the header of the next record. If the header can be
            // fully read, transitions into the Payload state
            Self::Header(buffer) => {
                if new_input.is_empty() {
                    return Ok(Item::Incomplete(Self::Header(buffer)));
                }

                // Headers are a pair of BigSize integers, which have variable
                // length. If the buffer is empty the header can be parsed
                // directly from the input, otherwise copy enough data from the
                // input.
                let buffered = buffer.held;
                let (input_slice_or_buffer, _) =
                    buffer.slice_input(new_input, MAX_HEADER_LEN - buffered);

                let mut parse_input = input_slice_or_buffer.as_ref();
                let available = parse_input.len();
                debug_assert!(available > buffered);

                let Some((ty, payload_len)) = tlv::parse_header(&mut parse_input)? else {
                    *new_input = &[];
                    return Ok(Item::Incomplete(Self::Header(
                        input_slice_or_buffer.into_buffer(),
                    )));
                };

                // Advance `new_input` to the first byte of the payload.
                let header_len = available - parse_input.len();
                *new_input = &new_input[header_len - buffered..];

                // Discard the header and any copied payload bytes. The payload
                // bytes are still in `new_input`.
                let buffer = input_slice_or_buffer.clear();

                // Transition to the payload state, and resume parsing the record
                let len = check_len(payload_len, N)?;
                Self::Payload { buffer, ty, len }.read(new_input)
            }

            // Try to read the remainder of the buffered, incomplete record
            Self::Payload { buffer, ty, len } => {
                debug_assert!(
                    len == 0 || buffer.held < len,
                    "a complete payload must not be suspended"
                );

                let take = len - buffer.held;
                let (new_payload_bytes, remaining_input) = buffer.slice_input(new_input, take);
                *new_input = remaining_input;

                if new_payload_bytes.as_ref().len() == len {
                    Ok(Item::Record(TypedSlice {
                        ty,
                        value: new_payload_bytes,
                    }))
                } else {
                    let buffer = new_payload_bytes.into_buffer();
                    Ok(Item::Incomplete(Self::Payload { buffer, ty, len }))
                }
            }
        }
    }

    /// Returns the array, or an empty slice if it is not allocated.
    #[cfg(test)]
    fn array(&self) -> &[u8] {
        let buffer = match self {
            Self::Header(buffer) | Self::Payload { buffer, .. } => buffer,
        };
        buffer.array.as_deref().map_or(&[], |array| &array[..])
    }
}

impl<const N: usize> Buffer<N> {
    fn append(&mut self, bytes: &[u8]) -> &[u8] {
        let array = self
            .array
            .get_or_insert_with(|| vec![0; N].into_boxed_slice().try_into().expect("N bytes"));
        array[self.held..][..bytes.len()].copy_from_slice(bytes);
        self.held += bytes.len();
        &array[..self.held]
    }

    // Parsing needs a contiguous &[u8]. When the buffer already contains
    // incomplete data (which previously couldn't be parsed), the new input data
    // is appended to the buffer before slicing it, otherwise if the buffer is
    // empty the input data is sliced directly.
    fn slice_input(self, input: &[u8], take: usize) -> (ContiguousInput<'_, N>, &[u8]) {
        let (bytes, remaining_input) = input.split_at(input.len().min(take));

        let mut buffer = self;
        if buffer.held != 0 {
            buffer.append(bytes);
        }

        (ContiguousInput { buffer, bytes }, remaining_input)
    }

    fn record<'a>(&'a mut self, ty: u64, bytes: &'a [u8]) -> TypedSlice<&'a [u8]> {
        let held = self.held;
        self.clear();
        let payload = if held == 0 {
            bytes
        } else {
            &self.array.as_ref().unwrap()[..held]
        };
        TypedSlice { ty, value: payload }
    }

    fn clear(&mut self) {
        self.held = 0;
    }
}

/// Contiguous input data for parsing. This can either borrow from the input, or
/// from an internal buffer when the input chunks contain incomplete TLVs. The
/// buffer is reused, only allocated once if needed.
struct ContiguousInput<'a, const N: usize> {
    buffer: Buffer<N>,
    bytes: &'a [u8],
}

impl<const N: usize> AsRef<[u8]> for ContiguousInput<'_, N> {
    fn as_ref(&self) -> &[u8] {
        if self.buffer.held == 0 {
            self.bytes
        } else {
            &self.buffer.array.as_ref().unwrap()[..self.buffer.held]
        }
    }
}

impl<const N: usize> ContiguousInput<'_, N> {
    fn clear(mut self) -> Buffer<N> {
        self.buffer.clear();
        self.buffer
    }

    /// Retains borrowed input before suspending parsing.
    fn into_buffer(mut self) -> Buffer<N> {
        if self.buffer.held == 0 && !self.bytes.is_empty() {
            self.buffer.append(self.bytes);
        }
        self.buffer
    }
}

impl<const N: usize> LendingIterator for Records<'_, N> {
    type Item<'b>
        = Result<Item<&'b [u8], N>, Error>
    where
        Self: 'b;

    fn next(&mut self) -> Option<Self::Item<'_>> {
        let state = self.state.take()?;

        let (ty, input) = match state.read(&mut self.input) {
            Ok(Item::Record(TypedSlice { ty, value: input })) => (ty, input),
            Ok(Item::Incomplete(state)) => return Some(Ok(Item::Incomplete(state))),
            Err(error) => return Some(Err(error)),
        };

        let ContiguousInput { buffer, bytes } = input;
        let State::Header(buffer) = self.state.insert(State::Header(buffer)) else {
            unreachable!()
        };
        let record = buffer.record(ty, bytes);
        Some(Ok(Item::Record(record)))
    }
}

/// Fails if the payload length exceeds `max`.
fn check_len(len: u64, max: usize) -> Result<usize, Error> {
    if len > max as u64 {
        return Err(Error::TooLarge { len, max });
    }
    // `len <= max`, so it fits in a `usize`.
    Ok(len as usize)
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
mod tests {
    use proptest::prelude::*;

    use super::{Error, Item, LendingIterator, State, TypedSlice};
    use crate::tlv::tests::{Owned, encode, encode_owned};

    const MAX: usize = 32;

    /// Returns the length of the record, header included.
    fn encoded_len(ty: u64, payload: &[u8]) -> usize {
        encode([(ty, payload)]).len()
    }

    /// Returns the address of the array, or `None` if it is not allocated.
    fn address<const N: usize>(state: &State<N>) -> Option<*const u8> {
        let array = state.array();
        (!array.is_empty()).then_some(array.as_ptr())
    }

    /// Parses `bytes` as inputs split at `cuts`, then ends the stream. Returns
    /// the records parsed before the end or a failure, and the outcome.
    ///
    /// Checks where each record is borrowed from. A payload that lies within
    /// the input is borrowed from the input. Any other payload is lent from
    /// the array. The array keeps its address once allocated.
    fn reassemble_cut_with_limit<const N: usize>(
        bytes: &[u8],
        cuts: &[usize],
    ) -> (Owned, Result<(), Error>) {
        let mut state = State::<N>::default();
        let mut out = Owned::new();
        let mut allocated = None;
        let mut pos = 0;
        let mut start = 0;
        for end in cuts.iter().copied().chain([bytes.len()]) {
            let input = &bytes[start..end];
            let mut records = state.resume(input);
            let mut from_array = Vec::new();
            // Each record takes at least a byte of the input, so the bound
            // only cuts short a parse that never ends.
            let mut steps = 0..=input.len();
            state = loop {
                steps.next().expect("the parse ends");
                match records
                    .next()
                    .expect("ends with the parse state or a failure")
                {
                    Ok(Item::Record(TypedSlice { ty, value: payload })) => {
                        let payload_end = pos + encoded_len(ty, payload);
                        let payload_start = payload_end - payload.len();
                        pos = payload_end;
                        if start <= payload_start && payload_end <= end {
                            let in_input = &input[payload_start - start..payload_end - start];
                            assert!(std::ptr::eq(payload, in_input), "borrowed from the input");
                        } else {
                            from_array.push(payload.as_ptr());
                        }
                        out.push((ty, payload.to_vec()));
                    }
                    Ok(Item::Incomplete(state)) => {
                        assert!(records.next().is_none());
                        break state;
                    }
                    Err(error) => {
                        assert!(records.next().is_none());
                        return (out, Err(error));
                    }
                }
            };
            let array = address(&state);
            for pointer in from_array {
                let array = array.expect("lent from the array");
                assert!(array <= pointer && pointer <= array.wrapping_add(N));
            }
            if allocated.is_some() {
                assert_eq!(array, allocated, "the array keeps its address");
            }
            allocated = array;
            start = end;
        }
        (out, state.end())
    }

    fn reassemble_cut(bytes: &[u8], cuts: &[usize]) -> (Owned, Result<(), Error>) {
        reassemble_cut_with_limit::<MAX>(bytes, cuts)
    }

    /// A length cut off in the middle of its bytes completes in the array.
    /// The payload is then borrowed from the input. A length this long does
    /// not fit `MAX`.
    #[test]
    fn borrows_the_payload_after_a_cut_off_length() {
        let payload = vec![7; 253];
        let bytes = encode([(2, &payload[..])]);
        assert_eq!(bytes[..4], [2, 0xfd, 0x00, 0xfd]);

        let mut records = State::<512>::default().resume(&bytes[..3]);
        let Some(Ok(Item::Incomplete(state))) = records.next() else {
            panic!("the parse state");
        };
        let mut records = state.resume(&bytes[3..]);
        let Some(Ok(Item::Record(TypedSlice { ty, value: payload }))) = records.next() else {
            panic!("a record");
        };
        assert_eq!(ty, 2);
        assert!(std::ptr::eq(payload, &bytes[4..]));
    }

    #[test]
    fn accepts_a_payload_at_the_limit() {
        let payload = vec![7; MAX];
        for ty in [2, u64::MAX] {
            let bytes = encode([(ty, &payload[..])]);
            for cuts in [vec![], vec![3], (1..bytes.len()).collect()] {
                assert_eq!(
                    reassemble_cut(&bytes, &cuts),
                    (vec![(ty, payload.clone())], Ok(()))
                );
            }
        }
    }

    /// The announced length is refused before any of the payload arrives, so
    /// a peer cannot make the receiver hold what it announces.
    #[test]
    fn refuses_a_length_above_the_limit() {
        let header = [2, MAX as u8 + 1];
        let mut records = State::<MAX>::default().resume(&header);

        let len = MAX as u64 + 1;
        let Some(Err(error)) = records.next() else {
            panic!("a failure");
        };
        assert_eq!(error, Error::TooLarge { len, max: MAX });
        assert!(records.next().is_none());
    }

    /// After a failure the position in the stream is unknown, so the parse
    /// state is not handed back.
    #[test]
    fn a_failure_inside_a_split_record_drops_the_reassembler() {
        let bytes = [0x02, 0xfd, 0x00, 0x01, b'x'];

        assert_eq!(
            reassemble_cut(&bytes, &[2]),
            (vec![], Err(Error::NonMinimal))
        );
    }

    /// Generates payloads both within and above the limit.
    fn generate() -> impl Strategy<Value = Owned> {
        let payload = prop::collection::vec(any::<u8>(), 0..=MAX + 8);
        prop::collection::vec((any::<u64>(), payload), 0..8)
    }

    /// Generates payloads at most `MAX` bytes long.
    fn generate_within_limit() -> impl Strategy<Value = Owned> {
        let payload = prop::collection::vec(any::<u8>(), 0..=MAX);
        prop::collection::vec((any::<u64>(), payload), 0..8)
    }

    /// Returns cut points within `len`, in order.
    fn pick_cuts(len: usize, picks: Vec<prop::sample::Index>) -> Vec<usize> {
        let mut cuts: Vec<usize> = picks.iter().map(|pick| pick.index(len + 1)).collect();
        cuts.sort_unstable();
        cuts
    }

    /// Returns the records before the first payload longer than `MAX`, and the
    /// outcome: `TooLarge` for that record, or the end of the stream.
    fn expect(records: &Owned) -> (Owned, Result<(), Error>) {
        let mut out = Owned::new();
        for (ty, payload) in records {
            let len = payload.len();
            if len > MAX {
                let error = Error::TooLarge {
                    len: len as u64,
                    max: MAX,
                };
                return (out, Err(error));
            }
            out.push((*ty, payload.clone()));
        }
        (out, Ok(()))
    }

    proptest! {
        #[test]
        fn reassembles_multibyte_lengths_whole_or_bytewise(
            records in prop::collection::vec(
                (any::<u64>(), prop::collection::vec(any::<u8>(), 253..=300)), 1..4,
            ),
        ) {
            let bytes = encode_owned(&records);
            for cuts in [vec![], (1..bytes.len()).collect()] {
                let (parsed, outcome) = reassemble_cut_with_limit::<512>(&bytes, &cuts);
                prop_assert_eq!(&parsed, &records);
                prop_assert_eq!(outcome, Ok(()));
            }
        }

        /// Fragmenting or concatenating the encoded bytes changes nothing.
        #[test]
        fn reassembles_what_was_encoded_however_cut(
            records in generate(),
            picks in prop::collection::vec(any::<prop::sample::Index>(), 0..12),
        ) {
            let bytes = encode_owned(&records);
            for cuts in [vec![], pick_cuts(bytes.len(), picks), (1..bytes.len()).collect()] {
                prop_assert_eq!(reassemble_cut(&bytes, &cuts), expect(&records));
            }
        }

        /// A stream that ends inside a record yields the records before it,
        /// then fails at the end.
        #[test]
        fn ending_inside_a_record_is_truncated(
            records in generate_within_limit().prop_filter("a record", |records| !records.is_empty()),
            inside in any::<prop::sample::Index>(),
            offset in any::<prop::sample::Index>(),
            picks in prop::collection::vec(any::<prop::sample::Index>(), 0..12),
        ) {
            let inside = inside.index(records.len());
            let lens: Vec<usize> = records.iter().map(|(ty, payload)| encoded_len(*ty, payload)).collect();
            let cut = lens[..inside].iter().sum::<usize>() + 1 + offset.index(lens[inside] - 1);
            let bytes = &encode_owned(&records)[..cut];

            for cuts in [vec![], pick_cuts(bytes.len(), picks), (1..bytes.len()).collect()] {
                let (parsed, outcome) = reassemble_cut(bytes, &cuts);
                prop_assert_eq!(&parsed, &records[..inside]);
                prop_assert_eq!(outcome, Err(Error::Truncated));
            }
        }

        /// Whatever the bytes, the cuts change neither the records nor the
        /// failure.
        #[test]
        fn arbitrary_bytes_parse_the_same_however_cut(
            bytes in prop::collection::vec(any::<u8>(), 0..96),
            picks in prop::collection::vec(any::<prop::sample::Index>(), 0..12),
        ) {
            for cuts in [pick_cuts(bytes.len(), picks), (1..bytes.len()).collect()] {
                prop_assert_eq!(reassemble_cut(&bytes, &cuts), reassemble_cut(&bytes, &[]));
            }
        }
    }
}
