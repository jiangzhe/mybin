use bytes_parser::error::Result;
use bytes_parser::{ReadBytesExt, ReadFromBytes};
use bytes::Bytes;

/// Data of IncidentEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/incident-event.html
#[derive(Debug, Clone)]
pub struct IncidentData {
    // https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h
    pub incident_type: u16,
    // below is variable part
    pub msg_len: u8,
    pub msg: Bytes,
}

impl ReadFromBytes for IncidentData {
    fn read_from(input: &mut Bytes) -> Result<Self> {
        let incident_type = input.read_le_u16()?;
        let msg_len = input.read_u8()?;
        let msg = input.read_len(msg_len as usize)?;
        Ok(IncidentData {
            incident_type,
            msg_len,
            msg,
        })
    }
}
