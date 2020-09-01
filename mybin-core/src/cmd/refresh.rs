use crate::resp::ComResponse;
use crate::Command;
use bitflags::bitflags;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComRefresh {
    pub cmd: Command,
    pub sub_cmd: RefreshFlags,
}

impl ComRefresh {
    pub fn new(sub_cmd: RefreshFlags) -> Self {
        ComRefresh {
            cmd: Command::Refresh,
            sub_cmd,
        }
    }
}

impl WriteToBytes for ComRefresh {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.write_u8(self.cmd.to_byte())?;
        out.write_u8(self.sub_cmd.bits())?;
        Ok(2)
    }
}

pub type ComRefreshResponse = ComResponse;

bitflags! {
    pub struct RefreshFlags: u8 {
        const GRANT     = 0x01;
        const LOG       = 0x02;
        const TABLES    = 0x04;
        const HOSTS     = 0x08;
        const STATUS    = 0x10;
        const THREADS   = 0x20;
        const SLAVE     = 0x40;
        const MASTER    = 0x80;
    }
}
