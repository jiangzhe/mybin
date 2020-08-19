//! gtid related events and parsing logic
use bytes_parser::error::{Error, Result};
use bytes_parser::{ReadFromBytes, ReadBytesExt};
use bytes::{Buf, Bytes};
use linked_hash_map::LinkedHashMap;

/// Data of GtidEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h#L933
#[derive(Debug, Clone)]
pub struct GtidLogData {
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

impl ReadFromBytes for GtidLogData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let gtid_flags = input.read_u8()?;
        let encoded_sid = input.read_le_u128()?;
        let encoded_gno = input.read_le_u64()?;
        // consumed 25 bytes now
        let LogicalTs {
            ts_type,
            last_committed,
            seq_num,
        } = LogicalTs::read_from(input)?;
        Ok(GtidLogData {
            gtid_flags,
            encoded_sid,
            encoded_gno,
            ts_type,
            last_committed,
            seq_num,
        })
    }
}

#[derive(Debug, Clone)]
pub struct AnonymousGtidLogData {
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

impl ReadFromBytes for AnonymousGtidLogData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let gld = GtidLogData::read_from(input)?;
        Ok(AnonymousGtidLogData{
            gtid_flags: gld.gtid_flags,
            encoded_sid: gld.encoded_sid,
            encoded_gno: gld.encoded_gno,
            ts_type: gld.ts_type,
            last_committed: gld.last_committed,
            seq_num: gld.seq_num,
        })
    }
}

#[derive(Debug, Clone)]
struct LogicalTs {
    ts_type: u8,
    last_committed: u64,
    seq_num: u64,
}

impl ReadFromBytes for LogicalTs {
    fn read_from(input: &mut Bytes) -> Result<LogicalTs> {
        if input.remaining() < 17 {
            return Ok(LogicalTs {
                ts_type: 0,
                last_committed: 0,
                seq_num: 0,
            });
        }
        let ts_type = input.read_u8()?;
        let last_committed = input.read_le_u64()?;
        let seq_num = input.read_le_u64()?;
        Ok(LogicalTs {
            ts_type,
            last_committed,
            seq_num,
        })
    }
}

/// Data of PreviousGtidsEvent
///
/// reference: https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h#L1074
#[derive(Debug, Clone)]
pub struct PreviousGtidsLogData {
    pub payload: Bytes,
}

impl PreviousGtidsLogData {
    pub fn gtid_set(&self) -> Result<GtidSet> {
        let mut payload = self.payload.clone();
        GtidSet::read_from(&mut payload)
    }
}

/// parse previous gtids data
///
/// seems layout introduction on mysql dev website is wrong,
/// so follow source code: https://github.com/mysql/mysql-server/blob/5.7/sql/rpl_gtid_set.cc#L1469
impl ReadFromBytes for PreviousGtidsLogData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let payload = input.split_to(input.remaining());
        Ok(PreviousGtidsLogData { payload })
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
impl ReadFromBytes for GtidSet {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let n_sids = input.read_le_u64()? as usize;
        let mut sids = LinkedHashMap::with_capacity(n_sids);
        for _ in 0..n_sids {
            let gtid_range = GtidRange::read_from(input)?;
            // todo: may need to handle duplicate sids
            sids.insert(gtid_range.sid, gtid_range);
        }
        Ok(GtidSet { sids })
    }
}

impl ReadFromBytes for GtidRange {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let sid = input.read_le_u128()?;
        let n_intervals = input.read_le_u64()? as usize;
        let mut last = 0u64;
        let mut intervals = Vec::with_capacity(n_intervals as usize);
        for _ in 0..n_intervals {
            let start = input.read_le_u64()?;
            let end = input.read_le_u64()?;
            if start <= last || end <= start {
                return Err(Error::ConstraintError(format!(
                    "invalid gtid range: start={}, end={}, last={}",
                    start, end, last
                )));
            }
            last = end;
            // here we use inclusive end
            intervals.push(GtidInterval {
                start,
                end: end - 1,
            });
        }
        Ok(GtidRange { sid, intervals })
    }
}
