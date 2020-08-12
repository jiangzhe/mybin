use serde_derive::*;
use super::Command;

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComBinlogDumpGtid {
    pub cmd: Command,
    pub flags: u16,
    pub server_id: u32,
    // 4-byte filename len
    pub binlog_filename: String,
    pub binlog_pos: u64,
    // if flags & BINLOG_THROUGH_GTID
    pub data: Vec<u8>,
}