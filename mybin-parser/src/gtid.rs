//! gtid related events and parsing logic
use crate::error::Error;
use linked_hash_map::LinkedHashMap;
use nom::bytes::streaming::take;
use nom::error::ErrorKind;
use nom::error::ParseError;
use nom::number::streaming::{le_u128, le_u32, le_u64, le_u8};
use nom::IResult;
use serde_derive::*;
use std::convert::TryFrom;

/// Data of GtidEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h#L933
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GtidData {
    pub gtid_flags: u8,
    // from source code
    pub encoded_sid: u128,
    pub encoded_gno: u64,
    // below fields may not exist
    // in versions earlier than 5.7.4
    pub ts_type: u8,
    pub last_committed: u64,
    pub seq_num: u64,
}

pub(crate) fn parse_gtid_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (GtidData, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    // before 5.7.4, the layout is different
    // here we only support
    // debug_assert_eq!(42, post_header_len);
    let (input, gtid_flags) = le_u8(input)?;
    let (input, encoded_sid) = le_u128(input)?;
    let (input, encoded_gno) = le_u64(input)?;
    // consumed 25 bytes now
    let (input, optional_ts, crc32) = if checksum {
        let (input, optional_ts) = parse_optional_ts(input, len - 25 - 4)?;
        let (input, crc32) = le_u32(input)?;
        (input, optional_ts, crc32)
    } else {
        let (input, optional_ts) = parse_optional_ts(input, len - 25)?;
        (input, optional_ts, 0)
    };
    let (ts_type, last_committed, seq_num) = optional_ts.unwrap_or((0, 0, 0));
    Ok((
        input,
        (
            GtidData {
                gtid_flags,
                encoded_sid,
                encoded_gno,
                ts_type,
                last_committed,
                seq_num,
            },
            crc32,
        ),
    ))
}

fn parse_optional_ts<'a, E>(
    input: &'a [u8],
    len: u32,
) -> IResult<&'a [u8], Option<(u8, u64, u64)>, E>
where
    E: ParseError<&'a [u8]>,
{
    if len < 17 {
        let (input, _) = take(len)(input)?;
        return Ok((input, None));
    }
    let (input, ts_type) = le_u8(input)?;
    let (input, last_committed) = le_u64(input)?;
    let (input, seq_num) = le_u64(input)?;
    Ok((input, Some((ts_type, last_committed, seq_num))))
}

/// Data of PreviousGtidsEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h#L1074
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreviousGtidsData<'a> {
    pub payload: &'a [u8],
}

impl<'a> PreviousGtidsData<'a> {
    pub fn gtid_set(&self) -> Result<GtidSet, Error> {
        GtidSet::try_from(self.payload)
    }
}

/// parse previous gtids data
///
/// seems layout introduction on mysql dev website is wrong,
/// so follow source code: https://github.com/mysql/mysql-server/blob/5.7/sql/rpl_gtid_set.cc#L1469
pub(crate) fn parse_previous_gtids_data<'a, E>(
    input: &'a [u8],
    len: u32,
    post_header_len: u8,
    checksum: bool,
) -> IResult<&'a [u8], (PreviousGtidsData<'a>, u32), E>
where
    E: ParseError<&'a [u8]>,
{
    debug_assert_eq!(0, post_header_len);
    if checksum {
        let (input, payload) = take(len - 4)(input)?;
        let (input, crc32) = le_u32(input)?;
        Ok((input, (PreviousGtidsData { payload }, crc32)))
    } else {
        let (input, payload) = take(len)(input)?;
        Ok((input, (PreviousGtidsData { payload }, 0)))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GtidSet {
    sids: LinkedHashMap<u128, GtidRange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GtidRange {
    pub sid: u128,
    pub intervals: Vec<GtidInterval>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GtidInterval {
    pub start: u64,
    // inclusive
    pub end: u64,
}

/// parse GtidSet from byte array
impl<'a> TryFrom<&'a [u8]> for GtidSet {
    type Error = Error;
    fn try_from(input: &'a [u8]) -> Result<Self, Error> {
        let (_, gtid_set) = parse_gtid_set(input).map_err(|e| Error::from((input, e)))?;
        Ok(gtid_set)
    }
}

/// parse gtid set from payload of PreviousGtidsLogEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/sql/rpl_gtid_set.cc#L1469
pub fn parse_gtid_set<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], GtidSet, E>
where
    E: ParseError<&'a [u8]>,
{
    let (mut input, n_sids) = le_u64(input)?;
    let n_sids = n_sids as usize;
    let mut sids = LinkedHashMap::with_capacity(n_sids);
    for _ in 0..n_sids {
        let (in1, gtid_range) = parse_gtid_range(input)?;
        // todo: may need to handle duplicate sids
        sids.insert(gtid_range.sid, gtid_range);
        input = in1;
    }
    Ok((input, GtidSet { sids }))
}

pub fn parse_gtid_range<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], GtidRange, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, sid) = le_u128(input)?;
    let (mut input, n_intervals) = le_u64(input)?;
    let mut last = 0u64;
    let mut intervals = Vec::with_capacity(n_intervals as usize);
    for _ in 0..n_intervals as usize {
        let (in1, start) = le_u64(input)?;
        let (in1, end) = le_u64(in1)?;
        if start <= last || end <= start {
            return Err(nom::Err::Error(E::from_error_kind(in1, ErrorKind::Digit)));
        }
        last = end;
        // here we use inclusive end
        intervals.push(GtidInterval {
            start,
            end: end - 1,
        });
        input = in1;
    }
    Ok((input, GtidRange { sid, intervals }))
}
