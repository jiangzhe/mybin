pub mod col;
pub mod error;
pub mod flag;
pub mod handshake;
pub mod packet;
pub mod query;
pub mod resultset;
pub mod binlog;
pub mod binlog_dump;
mod util;

pub use crate::binlog_dump::*;
pub use crate::error::{Error, Result};
pub use crate::query::*;
use std::convert::TryFrom;

// pub mod prelude {
//     pub use bytes_parser::number::ReadNumber;
//     pub use bytes_parser::take::TakeBytes;
//     pub use bytes_parser::my::ReadMyEncoding;
//     pub use bytes_parser::{ReadFrom, ReadWithContext, WriteTo, WriteWithContext};
// }

#[derive(Debug, Clone)]
pub enum Command {
    Sleep,
    Quit,
    InitDB,
    Query,
    FieldList,
    CreateDB,
    DropDB,
    Refresh,
    Shutdown,
    Statistics,
    ProcessInfo,
    Connect,
    ProcessKill,
    Debug,
    Ping,
    Time,
    DelayedInsert,
    ChangeUser,
    BinlogDump,
    TableDump,
    ConnectOut,
    RegisterSlave,
    StmtPrepare,
    StmtExecute,
    StmtSendLongData,
    StmtClose,
    StmtReset,
    SetOption,
    StmtFetch,
    Daemon,
    BinlogDumpGtid,
    ResetConnection,
}

impl Command {
    pub fn to_byte(&self) -> u8 {
        match self {
            Command::Sleep => 0x00,
            Command::Quit => 0x01,
            Command::InitDB => 0x02,
            Command::Query => 0x03,
            Command::FieldList => 0x04,
            Command::CreateDB => 0x05,
            Command::DropDB => 0x06,
            Command::Refresh => 0x07,
            Command::Shutdown => 0x08,
            Command::Statistics => 0x09,
            Command::ProcessInfo => 0x0a,
            Command::Connect => 0x0b,
            Command::ProcessKill => 0x0c,
            Command::Debug => 0x0d,
            Command::Ping => 0x0e,
            Command::Time => 0x0f,
            Command::DelayedInsert => 0x10,
            Command::ChangeUser => 0x11,
            Command::BinlogDump => 0x12,
            Command::TableDump => 0x13,
            Command::ConnectOut => 0x14,
            Command::RegisterSlave => 0x15,
            Command::StmtPrepare => 0x16,
            Command::StmtExecute => 0x17,
            Command::StmtSendLongData => 0x18,
            Command::StmtClose => 0x19,
            Command::StmtReset => 0x1a,
            Command::SetOption => 0x1b,
            Command::StmtFetch => 0x1c,
            Command::Daemon => 0x1d,
            Command::BinlogDumpGtid => 0x1e,
            Command::ResetConnection => 0x1f,
        }
    }
}

impl TryFrom<u8> for Command {
    type Error = Error;
    fn try_from(src: u8) -> Result<Self> {
        let cmd = match src {
            0x00 => Command::Sleep,
            0x01 => Command::Quit,
            0x02 => Command::InitDB,
            0x03 => Command::Query,
            0x04 => Command::FieldList,
            0x05 => Command::CreateDB,
            0x06 => Command::DropDB,
            0x07 => Command::Refresh,
            0x08 => Command::Shutdown,
            0x09 => Command::Statistics,
            0x0a => Command::ProcessInfo,
            0x0b => Command::Connect,
            0x0c => Command::ProcessKill,
            0x0d => Command::Debug,
            0x0e => Command::Ping,
            0x0f => Command::Time,
            0x10 => Command::DelayedInsert,
            0x11 => Command::ChangeUser,
            0x12 => Command::BinlogDump,
            0x13 => Command::TableDump,
            0x14 => Command::ConnectOut,
            0x15 => Command::RegisterSlave,
            0x16 => Command::StmtPrepare,
            0x17 => Command::StmtExecute,
            0x18 => Command::StmtSendLongData,
            0x19 => Command::StmtClose,
            0x1a => Command::StmtReset,
            0x1b => Command::SetOption,
            0x1c => Command::StmtFetch,
            0x1d => Command::Daemon,
            0x1e => Command::BinlogDumpGtid,
            0x1f => Command::ResetConnection,
            _ => return Err(Error::InvalidCommandCode(src)),
        };
        Ok(cmd)
    }
}
