use crate::Command;
use bitflags::bitflags;
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::error::{Error, Result};
use bytes_parser::{ReadBytesExt, ReadFromBytes, WriteBytesExt, WriteToBytes};

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
    pub fn binlog_pos(mut self, binlog_pos: u32) -> Self {
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

    /// official non blocking flag...
    ///
    /// see https://github.com/mysql/mysql-server/blob/5.7/sql/rpl_binlog_sender.cc#L129
    pub fn non_block(mut self, non_block: bool) -> Self {
        if non_block {
            self.flags = 0x01;
        } else {
            self.flags = 0x00;
        }
        self
    }
}

impl Default for ComBinlogDump {
    fn default() -> Self {
        ComBinlogDump {
            cmd: Command::BinlogDump,
            binlog_pos: 4,
            flags: 0,
            server_id: 0,
            binlog_filename: String::new(),
        }
    }
}

impl ReadFromBytes for ComBinlogDump {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        use std::convert::TryFrom;

        let cmd = input.read_u8()?;
        let cmd = Command::try_from(cmd).map_err(|_| {
            Error::ConstraintError(format!(
                "invalid command code expected=0x12, actual={:02x}",
                cmd
            ))
        })?;
        let binlog_pos = input.read_le_u32()?;
        let flags = input.read_le_u16()?;
        let server_id = input.read_le_u32()?;
        let binlog_filename = input.split_to(input.remaining());
        let binlog_filename = String::from_utf8(Vec::from(binlog_filename.bytes()))?;
        Ok(ComBinlogDump {
            cmd,
            binlog_pos,
            flags,
            server_id,
            binlog_filename,
        })
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

    /// official non blocking flag...
    ///
    /// see https://github.com/mysql/mysql-server/blob/5.7/sql/rpl_binlog_sender.cc#L129
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
        ComBinlogDumpGtid {
            cmd: Command::BinlogDumpGtid,
            binlog_pos: 4,
            binlog_filename: String::new(),
            flags: BinlogDumpGtidFlags::THROUGH_POSITION,
            server_id: 0,
            sid_data: SidData::empty(),
        }
    }
}

impl ReadFromBytes for ComBinlogDumpGtid {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        use std::convert::TryFrom;

        let cmd = input.read_u8()?;
        let cmd = Command::try_from(cmd).map_err(|_| {
            Error::ConstraintError(format!(
                "invalid command code expected=0x1e, actual={:02x}",
                cmd
            ))
        })?;
        let flags = input.read_le_u16()?;
        let flags = BinlogDumpGtidFlags::from_bits(flags).ok_or_else(|| {
            Error::ConstraintError(format!("invalid binlog dump gtid flags {:04x}", flags))
        })?;
        let server_id = input.read_le_u32()?;
        let binlog_filename_len = input.read_le_u32()?;
        let binlog_filename = input.read_len(binlog_filename_len as usize)?;
        let binlog_filename = String::from_utf8(Vec::from(binlog_filename.bytes()))?;
        let binlog_pos = input.read_le_u64()?;
        // always read sid_data
        let sid_data_len = input.read_le_u32()?;
        let mut raw_data = input.read_len(sid_data_len as usize)?;
        let sid_data = SidData::read_from(&mut raw_data)?;
        Ok(ComBinlogDumpGtid {
            cmd,
            flags,
            server_id,
            binlog_filename,
            binlog_pos,
            sid_data,
        })
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
        // no matter what the flag is, always write out sid data
        len += out.write_le_u32(self.sid_data.bytes_len() as u32)?;
        len += out.write_bytes(self.sid_data)?;
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
#[derive(Debug, Clone, PartialEq)]
pub struct SidData(pub Vec<SidRange>);

impl SidData {
    pub fn empty() -> Self {
        SidData(vec![])
    }

    pub fn bytes_len(&self) -> usize {
        self.0.iter().map(|s| s.bytes_len()).sum::<usize>() + 8
    }
}

impl ReadFromBytes for SidData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let n_sids = input.read_le_u64()?;
        let mut data = Vec::with_capacity(n_sids as usize);
        for _ in 0..n_sids {
            let sid_range = SidRange::read_from(input)?;
            data.push(sid_range);
        }
        Ok(SidData(data))
    }
}

impl WriteToBytes for SidData {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        // document is WRONG! this is 8-byte length
        // https://github.com/mysql/mysql-server/blob/5.7/sql/rpl_gtid_set.cc#L1463
        let n_sids = self.0.len() as u64;
        len += out.write_le_u64(n_sids)?;
        for sid in self.0 {
            len += out.write_bytes(sid)?;
        }
        Ok(len)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SidRange {
    pub sid: u128,
    // 8-byte length
    pub intervals: Vec<(i64, i64)>,
}

impl SidRange {
    pub fn bytes_len(&self) -> usize {
        16 + 8 + 16 * self.intervals.len()
    }
}

impl ReadFromBytes for SidRange {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let sid = input.read_le_u128()?;
        let n_intervals = input.read_le_u64()?;
        let mut intervals = Vec::with_capacity(n_intervals as usize);
        for _ in 0..n_intervals {
            let start = input.read_le_i64()?;
            let end = input.read_le_i64()?;
            intervals.push((start, end));
        }
        Ok(SidRange { sid, intervals })
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binlog_dump_cmd() {
        let dump = ComBinlogDump::default()
            .binlog_filename("mysql-bin.000001")
            .binlog_pos(4)
            .non_block(true)
            .server_id(123);
        let mut buf = BytesMut::new();
        dump.write_to(&mut buf).unwrap();
        let decoded = ComBinlogDump::read_from(&mut buf.freeze()).unwrap();
        assert_eq!(Command::BinlogDump, decoded.cmd);
        assert_eq!(0x01, decoded.flags);
        assert_eq!("mysql-bin.000001", decoded.binlog_filename);
        assert_eq!(4, decoded.binlog_pos);
        assert_eq!(123, decoded.server_id);
    }

    #[test]
    fn test_binlog_dump_gtid_cmd() {
        let dump_gtid = ComBinlogDumpGtid::default()
            .binlog_filename("mysql-bin.000001")
            .binlog_pos(4)
            .non_block(true)
            .server_id(123)
            .use_gtid(true)
            .sid(SidRange {
                sid: 456,
                intervals: vec![(1, 5)],
            });

        let mut buf = BytesMut::new();
        dump_gtid.write_to(&mut buf).unwrap();

        let decoded = ComBinlogDumpGtid::read_from(&mut buf.freeze()).unwrap();
        assert_eq!(Command::BinlogDumpGtid, decoded.cmd);
        assert!(decoded
            .flags
            .contains(BinlogDumpGtidFlags::NON_BLOCK | BinlogDumpGtidFlags::THROUGH_GTID));
        assert_eq!("mysql-bin.000001", decoded.binlog_filename);
        assert_eq!(4, decoded.binlog_pos);
        assert_eq!(123, decoded.server_id);
        assert_eq!(
            SidData(vec![SidRange {
                sid: 456,
                intervals: vec![(1, 5)],
            }]),
            decoded.sid_data
        );
    }
}
