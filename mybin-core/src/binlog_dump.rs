use super::Command;
use bitflags::bitflags;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

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
        ComBinlogDump {
            cmd: Command::BinlogDump,
            binlog_pos,
            flags,
            server_id,
            binlog_filename,
        }
    }
}

impl WriteToBytes for ComBinlogDump {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_le_u32(self.binlog_pos)?;
        len += out.write_le_u16(self.flags)?;
        len += out.write_le_u32(self.server_id)?;
        len += out.write_bytes(self.binlog_filename.as_bytes())?;
        Ok(len)
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
    pub sid_data: SidData,
}

impl ComBinlogDumpGtid {
    
    pub fn binlog_pos(mut self, binlog_pos: u64) -> Self {
        self.binlog_pos = binlog_pos;
        self
    }

    pub fn binlog_filename<S: Into<String>>(mut self, binlog_filename: S) -> Self {
        self.binlog_filename = binlog_filename.into();
        self
    }

    pub fn server_id(mut self, server_id: u32) -> Self {
        self.server_id = server_id;
        self
    }

    pub fn sid(mut self, sid: SidRange) -> Self {
        self.sid_data.0.push(sid);
        self
    }

    pub fn sids(mut self, sids: Vec<SidRange>) -> Self {
        self.sid_data.0.extend(sids);
        self
    }

    pub fn use_gtid(mut self, use_gtid: bool) -> Self {
        if use_gtid {
            self.flags.remove(BinlogDumpGtidFlags::THROUGH_POSITION);
            self.flags.insert(BinlogDumpGtidFlags::THROUGH_GTID);
        } else {
            self.flags.insert(BinlogDumpGtidFlags::THROUGH_POSITION);
            self.flags.remove(BinlogDumpGtidFlags::THROUGH_GTID);
        }
        self
    }

    pub fn non_block(mut self, non_block: bool) -> Self {
        if non_block {
            self.flags.insert(BinlogDumpGtidFlags::NON_BLOCK);
        } else {
            self.flags.remove(BinlogDumpGtidFlags::NON_BLOCK);
        }
        self
    }
}

impl Default for ComBinlogDumpGtid {
    fn default() -> Self {
        ComBinlogDumpGtid{
            cmd: Command::BinlogDumpGtid,
            binlog_pos: 4,
            binlog_filename: String::new(),
            flags: BinlogDumpGtidFlags::THROUGH_POSITION,
            server_id: 0,
            sid_data: SidData::empty(),
        }
    }
}

impl WriteToBytes for ComBinlogDumpGtid {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_le_u16(self.flags.bits())?;
        len += out.write_le_u32(self.server_id)?;
        // 4-byte length of filename
        let fn_len = self.binlog_filename.len() as u32;
        len += out.write_le_u32(fn_len)?;
        len += out.write_bytes(self.binlog_filename.as_bytes())?;
        len += out.write_le_u64(self.binlog_pos)?;
        if self.flags.contains(BinlogDumpGtidFlags::THROUGH_GTID) {
            len += out.write_bytes(self.sid_data)?;
        }
        Ok(len)
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
pub struct SidData(pub Vec<SidRange>);

impl SidData {
    pub fn empty() -> Self {
        SidData(vec![])
    }
}

impl WriteToBytes for SidData {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        // 4-byte length of sids
        let n_sids = self.0.len() as u32;
        len += out.write_le_u32(n_sids)?;
        for sid in self.0 {
            len += out.write_bytes(sid)?;
        }
        Ok(len)
    }
}

#[derive(Debug, Clone)]
pub struct SidRange {
    pub sid: u128,
    // 8-byte length
    pub intervals: Vec<(i64, i64)>,
}

impl WriteToBytes for SidRange {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_le_u128(self.sid)?;
        // 8-byte length of intervals
        let n_intervals = self.intervals.len() as u64;
        len += out.write_le_u64(n_intervals)?;
        for (start, end) in self.intervals {
            len += out.write_le_i64(start)?;
            len += out.write_le_i64(end)?;
        }
        Ok(len)
    }
}
