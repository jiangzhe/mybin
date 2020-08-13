use super::Command;
use bitflags::bitflags;

/// Request a binlog network stream from master 
/// starting a given postion
#[derive(Debug, Clone)]
pub struct ComBinlogDump {
    pub cmd: Command,
    pub binlog_pos: u32,
    pub flags: u16,
    pub server_id: u32,
    pub binlog_filename: String,
}

impl ComBinlogDump {

    pub fn new<S: Into<String>>(
        binlog_filename: S, 
        binlog_pos: u32, 
        server_id: u32, 
        non_blocking: bool,
    ) -> Self {
        let flags = if non_blocking { 1 } else { 0 };
        let binlog_filename = binlog_filename.into();
        ComBinlogDump{
            cmd: Command::BinlogDump,
            binlog_pos,
            flags,
            server_id,
            binlog_filename,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bs = Vec::new();
        bs.push(self.cmd.to_byte());
        bs.extend(&self.binlog_pos.to_le_bytes());
        bs.extend(&self.flags.to_le_bytes());
        bs.extend(&self.server_id.to_le_bytes());
        bs.extend(&self.binlog_filename.as_bytes()[..]);
        bs
    }
}

/// request the binlog network stream based on a GTID
#[derive(Debug, Clone)]
pub struct ComBinlogDumpGtid {
    pub cmd: Command,
    pub flags: BinlogDumpGtidFlags,
    pub server_id: u32,
    // 4-byte filename len
    pub binlog_filename: String,
    pub binlog_pos: u64,
    // if flags & BINLOG_THROUGH_GTID
    // 4-byte length before the real data
    pub data: SidData,
}

impl ComBinlogDumpGtid {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bs = Vec::new();
        bs.push(self.cmd.to_byte());
        bs.extend(&self.flags.bits().to_le_bytes());
        bs.extend(&self.server_id.to_le_bytes());
        // 4-byte length of filename
        let fn_len = self.binlog_filename.len() as u32;
        bs.extend(&fn_len.to_le_bytes());
        bs.extend(self.binlog_filename.as_bytes());
        bs.extend(&self.binlog_pos.to_le_bytes());
        if self.flags.contains(BinlogDumpGtidFlags::THROUGH_GTID) {
            let data = self.data.to_bytes();
            // 4-byte length of data
            let data_len = data.len() as u32;
            bs.extend(&data_len.to_le_bytes());
            bs.extend(data);
        }
        bs
    }
}

bitflags! {
    pub struct BinlogDumpGtidFlags: u16 {
        const NON_BLOCK = 0x0001;
        const THROUGH_POSITION = 0x0002;
        const THROUGH_GTID = 0x0004;
    }
}

/// Sid data contains multiple sid with intervals
#[derive(Debug, Clone)]
pub struct SidData(Vec<SidRange>);

impl SidData {

    pub fn empty() -> Self {
        SidData(vec![])
    }

    /// convert SidData to binary representation
    /// to send to MySQL server
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bs = Vec::new();
        // 4-byte length of sids
        let n_sids = self.0.len() as u32;
        bs.extend(&n_sids.to_le_bytes());
        for sid in &self.0 {
            bs.extend(sid.to_bytes());
        }
        bs
    }
}


#[derive(Debug, Clone)]
pub struct SidRange {
    pub sid: [u8; 16],
    // 8-byte length 
    pub intervals: Vec<(i64, i64)>,
}

impl SidRange {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bs = Vec::new();
        bs.extend(&self.sid[..]);
        // 8-byte length of intervals
        let n_intervals = self.intervals.len() as u64;
        bs.extend(&n_intervals.to_le_bytes());
        for (start, end) in &self.intervals {
            bs.extend(&start.to_le_bytes());
            bs.extend(&end.to_le_bytes());
        }
        bs
    }
}