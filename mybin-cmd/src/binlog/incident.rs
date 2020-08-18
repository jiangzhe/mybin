use bytes_parser::error::Result;
use bytes_parser::number::ReadNumber;
use bytes_parser::bytes::ReadBytes;
use bytes_parser::ReadFrom;

/// Data of IncidentEvent
///
/// reference: https://dev.mysql.com/doc/internals/en/incident-event.html
#[derive(Debug, Clone)]
pub struct IncidentData<'a> {
    // https://github.com/mysql/mysql-server/blob/5.7/libbinlogevents/include/control_events.h
    pub incident_type: u16,
    // below is variable part
    pub message_length: u8,
    pub message: &'a [u8],
}

impl<'a> ReadFrom<'a, IncidentData<'a>> for [u8] {
    fn read_from(&'a self, offset: usize) -> Result<(usize, IncidentData<'a>)> {
        let (offset, incident_type) = self.read_le_u16(offset)?;
        let (offset, message_length) = self.read_u8(offset)?;
        let (offset, message) = self.take_len(offset, message_length as usize)?;
        Ok((
            offset,
            IncidentData {
                incident_type,
                message_length,
                message,
            }
        ))
    }
}
