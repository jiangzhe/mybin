use crate::Command;
use bytes::BytesMut;
use bytes_parser::error::Result;
use bytes_parser::{WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComRegisterSlave {
    pub cmd: Command,
    pub server_id: u32,
    pub slave_hostname: String,
    pub slave_user: String,
    pub slave_password: String,
    pub slave_port: u16,
    pub replication_rank: u32,
    pub master_id: u32,
}

impl WriteToBytes for ComRegisterSlave {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_le_u32(self.server_id)?;
        let slave_hostname_len = self.slave_hostname.len() as u8;
        len += out.write_u8(slave_hostname_len)?;
        len += out.write_bytes(&self.slave_hostname.as_bytes()[..slave_hostname_len as usize])?;
        let slave_user_len = self.slave_user.len() as u8;
        len += out.write_u8(slave_user_len)?;
        len += out.write_bytes(&self.slave_user.as_bytes()[..slave_user_len as usize])?;
        let slave_password_len = self.slave_password.len() as u8;
        len += out.write_u8(slave_password_len)?;
        len += out.write_bytes(&self.slave_password.as_bytes()[..slave_password_len as usize])?;
        len += out.write_le_u16(self.slave_port)?;
        len += out.write_le_u32(self.replication_rank)?;
        len += out.write_le_u32(self.master_id)?;
        Ok(len)
    }
}