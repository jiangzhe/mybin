//! gtid related events and parsing logic
use linked_hash_map::LinkedHashMap;
use bytes_parser::ReadAs;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::number::ReadNumber;
use bytes_parser::error::{Result, Error};

/// Data of GtidEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h#L933
#[derive(Debug, Clone)]
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

impl ReadAs<'_, GtidData> for [u8] {
    fn read_as(&self, offset: usize) -> Result<(usize, GtidData)> {
        let (offset, gtid_flags) = self.read_u8(offset)?;
        let (offset, encoded_sid) = self.read_le_u128(offset)?;
        let (offset, encoded_gno) = self.read_le_u64(offset)?;
        // consumed 25 bytes now
        let (offset, LogicalTs{ts_type, last_committed, seq_num}) = self.read_as(offset)?;
        Ok((offset, GtidData{
            gtid_flags,
            encoded_sid,
            encoded_gno,
            ts_type,
            last_committed,
            seq_num,
        }))
    }
}

#[derive(Debug)]
struct LogicalTs {
    ts_type: u8,
    last_committed: u64,
    seq_num: u64,
}

impl ReadAs<'_, LogicalTs> for [u8] {
    fn read_as(&self, offset: usize) -> Result<(usize, LogicalTs)> {
        if self.len() - offset < 17 {
            return Ok((self.len(), LogicalTs{ts_type: 0, last_committed: 0, seq_num: 0}));
        }
        let (offset, ts_type) = self.read_u8(offset)?;
        let (offset, last_committed) = self.read_le_u64(offset)?;
        let (offset, seq_num) = self.read_le_u64(offset)?;
        Ok((offset, LogicalTs{ts_type, last_committed, seq_num}))
    }
}

/// Data of PreviousGtidsEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h#L1074
#[derive(Debug, Clone)]
pub struct PreviousGtidsData<'a> {
    pub payload: &'a [u8],
}

impl<'a> PreviousGtidsData<'a> {
    pub fn gtid_set(&self) -> Result<GtidSet> {
        let (_, gtid_set) = self.payload.read_as(0)?;
        Ok(gtid_set)
    }
}

/// parse previous gtids data
///
/// seems layout introduction on mysql dev website is wrong,
/// so follow source code: https://github.com/mysql/mysql-server/blob/5.7/sql/rpl_gtid_set.cc#L1469
impl<'a> ReadAs<'a, PreviousGtidsData<'a>> for [u8] {
    fn read_as(&'a self, offset: usize) -> Result<(usize, PreviousGtidsData<'a>)> {
        let (offset, payload) = self.take_len(offset, self.len() - offset)?;
        Ok((offset, PreviousGtidsData{payload}))
    }
}

#[derive(Debug, Clone)]
pub struct GtidSet {
    sids: LinkedHashMap<u128, GtidRange>,
}

#[derive(Debug, Clone)]
pub struct GtidRange {
    pub sid: u128,
    pub intervals: Vec<GtidInterval>,
}

#[derive(Debug, Clone)]
pub struct GtidInterval {
    pub start: u64,
    // inclusive
    pub end: u64,
}

/// parse gtid set from payload of PreviousGtidsLogEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/sql/rpl_gtid_set.cc#L1469
impl ReadAs<'_, GtidSet> for [u8] {
    fn read_as(&self, offset: usize) -> Result<(usize, GtidSet)> {
        let (mut offset, n_sids) = self.read_le_u64(offset)?;
        let n_sids = n_sids as usize;
        let mut sids = LinkedHashMap::with_capacity(n_sids);
        for _ in 0..n_sids {
            let (os1, gtid_range): (_, GtidRange) = self.read_as(offset)?;
            // todo: may need to handle duplicate sids
            sids.insert(gtid_range.sid, gtid_range);
            offset = os1;
        }
        Ok((offset, GtidSet{sids}))
    }
}

impl ReadAs<'_, GtidRange> for [u8] {
    fn read_as(&self, offset: usize) -> Result<(usize, GtidRange)> {
        let (offset, sid) = self.read_le_u128(offset)?;
        let (mut offset, n_intervals) = self.read_le_u64(offset)?;
        let mut last = 0u64;
        let mut intervals = Vec::with_capacity(n_intervals as usize);
        for _ in 0..n_intervals as usize {
            let (os1, start) = self.read_le_u64(offset)?;
            let (os1, end) = self.read_le_u64(os1)?;
            if start <= last || end <= start {
                return Err(Error::ConstraintError(format!("invalid gtid range: start={}, end={}, last={}", start, end, last)));
            }
            last = end;
            // here we use inclusive end
            intervals.push(GtidInterval{
                start,
                end: end -1,
            });
            offset = os1;
        }
        Ok((offset, GtidRange{sid, intervals}))
    }
}
