use crate::col::ColumnDefinition;
use crate::flag::CapabilityFlags;
use crate::packet::{EofPacket, ErrPacket, OkPacket};
use crate::resultset::TextRow;
use crate::Command;
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::error::{Error, Needed, Result};
use bytes_parser::my::ReadMyEnc;
use bytes_parser::{ReadFromBytesWithContext, WriteBytesExt, WriteToBytes};

#[derive(Debug, Clone)]
pub struct ComQuery {
    pub cmd: Command,
    pub query: String,
}

impl ComQuery {
    pub fn new<S: Into<String>>(query: S) -> Self {
        ComQuery {
            cmd: Command::Query,
            query: query.into(),
        }
    }
}

impl WriteToBytes for ComQuery {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        let mut len = 0;
        len += out.write_u8(self.cmd.to_byte())?;
        len += out.write_bytes(self.query.as_bytes())?;
        Ok(len)
    }
}

/// response of COM_QUERY
///
/// reference: https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
///
/// todo: support local_infile_request
#[derive(Debug, Clone)]
pub enum ComQueryResponse {
    Ok(OkPacket),
    Err(ErrPacket),
    // below are result set related packets
    ColCnt(u64),
    ColDef(ColumnDefinition),
    Eof(EofPacket),
    Row(TextRow),
}

#[derive(Debug, Clone, PartialEq)]
enum ComQueryState {
    Pending,
    ColDefs(usize),
    Rows,
    Err,
    // also for RowsEof
    Ok,
}

#[derive(Debug)]
pub struct ComQueryStateMachine {
    col_cnt: usize,
    state: ComQueryState,
    cap_flags: CapabilityFlags,
}

impl ComQueryStateMachine {
    pub fn new(cap_flags: CapabilityFlags) -> Self {
        ComQueryStateMachine {
            col_cnt: 0,
            state: ComQueryState::Pending,
            cap_flags,
        }
    }

    pub fn end(&self) -> bool {
        match self.state {
            ComQueryState::Ok | ComQueryState::Err => true,
            _ => false,
        }
    }

    pub fn next(&mut self, input: Bytes) -> Result<ComQueryResponse> {
        let (ns, resp) = self.next_state(input)?;
        self.state = ns;
        Ok(resp)
    }

    fn next_state<'a>(&mut self, input: Bytes) -> Result<(ComQueryState, ComQueryResponse)> {
        match self.state {
            ComQueryState::Pending => self.on_pending(input),
            ComQueryState::Ok | ComQueryState::Err => Err(Error::ConstraintError(format!(
                "illegal state to receive message {:?}",
                self.state
            ))),
            ComQueryState::ColDefs(col_cnt) => self.on_col_defs(input, col_cnt),
            ComQueryState::Rows => self.on_rows(input),
        }
    }

    fn on_pending(&mut self, mut input: Bytes) -> Result<(ComQueryState, ComQueryResponse)> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            0x00 => {
                let ok = OkPacket::read_with_ctx(&mut input, &self.cap_flags)?;
                Ok((ComQueryState::Ok, ComQueryResponse::Ok(ok)))
            }
            0xff => {
                let err = ErrPacket::read_with_ctx(&mut input, (&self.cap_flags, true))?;
                Ok((ComQueryState::Err, ComQueryResponse::Err(err)))
            }
            _ => {
                // must be length encoded column count packet
                let col_cnt = input.read_len_enc_int()?;
                let col_cnt = col_cnt.to_u64().ok_or_else(|| {
                    Error::ConstraintError(format!("invalid column count {:?}", col_cnt))
                })?;
                self.col_cnt = col_cnt as usize;
                Ok((
                    ComQueryState::ColDefs(self.col_cnt),
                    ComQueryResponse::ColCnt(col_cnt),
                ))
            }
        }
    }

    fn on_col_defs(
        &mut self,
        mut input: Bytes,
        col_cnt: usize,
    ) -> Result<(ComQueryState, ComQueryResponse)> {
        if col_cnt > 0 {
            let col_def = ColumnDefinition::read_with_ctx(&mut input, false)?;
            let col_cnt = col_cnt - 1;
            if col_cnt == 0 && self.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                return Ok((ComQueryState::Rows, ComQueryResponse::ColDef(col_def)));
            }
            return Ok((
                ComQueryState::ColDefs(col_cnt),
                ComQueryResponse::ColDef(col_def),
            ));
        }
        // must be EOF
        let eof = EofPacket::read_with_ctx(&mut input, &self.cap_flags)?;
        Ok((ComQueryState::Rows, ComQueryResponse::Eof(eof)))
    }

    fn on_rows(&mut self, mut input: Bytes) -> Result<(ComQueryState, ComQueryResponse)> {
        if !input.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match input[0] {
            // EOF Packet
            0xfe if input.remaining() <= 0xffffff => {
                if self.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                    let ok = OkPacket::read_with_ctx(&mut input, &self.cap_flags)?;
                    return Ok((ComQueryState::Ok, ComQueryResponse::Ok(ok)));
                }
                let eof = EofPacket::read_with_ctx(&mut input, &self.cap_flags)?;
                Ok((ComQueryState::Ok, ComQueryResponse::Eof(eof)))
            }
            _ => {
                let row = TextRow::read_with_ctx(&mut input, self.col_cnt)?;
                Ok((ComQueryState::Rows, ComQueryResponse::Row(row)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;

    #[test]
    fn test_ok_result_set() -> Result<()> {
        let cap_flags = CapabilityFlags::PROTOCOL_41 | CapabilityFlags::DEPRECATE_EOF;
        let pkts = vec![
            vec![1u8],
            vec![
                3, 100, 101, 102, 0, 0, 0, 0, 0, 12, 33, 0, 0, 0, 0, 0, 253, 1, 0, 31, 0, 0,
            ],
            vec![0],
            vec![254, 0, 0, 2, 0, 0, 0],
        ];
        let mut sm = ComQueryStateMachine::new(cap_flags);
        for pkt in pkts.into_iter().map(|x| (&x[..]).to_bytes()) {
            let resp = sm.next(pkt)?;
            dbg!(resp);
        }
        assert_eq!(ComQueryState::Ok, sm.state);
        Ok(())
    }

    #[test]
    fn test_eof_result_set() -> Result<()> {
        let cap_flags = CapabilityFlags::PROTOCOL_41;
        let pkts = vec![
            vec![1],
            vec![
                3, 100, 101, 102, 0, 0, 0, 1, 49, 0, 12, 63, 0, 1, 0, 0, 0, 8, 129, 0, 0, 0, 0,
            ],
            vec![254, 0, 0, 2, 0],
            vec![1, 49],
            vec![254, 0, 0, 2, 0],
        ];
        let mut sm = ComQueryStateMachine::new(cap_flags);
        for pkt in pkts.into_iter().map(|x| (&x[..]).to_bytes()) {
            let resp = sm.next(pkt)?;
            dbg!(resp);
        }
        assert_eq!(ComQueryState::Ok, sm.state);
        Ok(())
    }

    #[test]
    fn test_empty_result_set() -> Result<()> {
        let cap_flags = CapabilityFlags::PROTOCOL_41 | CapabilityFlags::DEPRECATE_EOF;
        let pkts = vec![vec![0, 0, 0, 2, 0, 0, 0]];
        let mut sm = ComQueryStateMachine::new(cap_flags);
        for pkt in pkts.into_iter().map(|x| (&x[..]).to_bytes()) {
            let resp = sm.next(pkt)?;
            dbg!(resp);
        }
        assert_eq!(ComQueryState::Ok, sm.state);
        Ok(())
    }

    #[test]
    fn test_err_result_set() -> Result<()> {
        let cap_flags = CapabilityFlags::PROTOCOL_41 | CapabilityFlags::DEPRECATE_EOF;
        let pkts = vec![vec![
            255, 40, 4, 35, 52, 50, 48, 48, 48, 89, 111, 117, 32, 104, 97, 118, 101, 32, 97, 110,
            32, 101, 114, 114, 111, 114, 32, 105, 110, 32, 121, 111, 117, 114, 32, 83, 81, 76, 32,
            115, 121, 110, 116, 97, 120, 59, 32, 99, 104, 101, 99, 107, 32, 116, 104, 101, 32, 109,
            97, 110, 117, 97, 108, 32, 116, 104, 97, 116, 32, 99, 111, 114, 114, 101, 115, 112,
            111, 110, 100, 115, 32, 116, 111, 32, 121, 111, 117, 114, 32, 77, 121, 83, 81, 76, 32,
            115, 101, 114, 118, 101, 114, 32, 118, 101, 114, 115, 105, 111, 110, 32, 102, 111, 114,
            32, 116, 104, 101, 32, 114, 105, 103, 104, 116, 32, 115, 121, 110, 116, 97, 120, 32,
            116, 111, 32, 117, 115, 101, 32, 110, 101, 97, 114, 32, 39, 115, 101, 45, 116, 32, 64,
            97, 98, 99, 32, 61, 32, 49, 39, 32, 97, 116, 32, 108, 105, 110, 101, 32, 49,
        ]];
        let mut sm = ComQueryStateMachine::new(cap_flags);
        for pkt in pkts.into_iter().map(|x| (&x[..]).to_bytes()) {
            let resp = sm.next(pkt)?;
            dbg!(resp);
        }
        assert_eq!(ComQueryState::Err, sm.state);
        Ok(())
    }
}
