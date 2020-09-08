use crate::conn::Conn;
use crate::error::{Error, Needed, Result};
use bytes::{Buf, Bytes};
use bytes_parser::{ReadBytesExt, ReadFromBytes, ReadFromBytesWithContext};
use futures::{ready, AsyncRead, AsyncWrite, Stream};
use mybin_core::binlog::*;
use mybin_core::cmd::*;
use mybin_core::col::TextColumnValue;
use mybin_core::packet::{EofPacket, ErrPacket};
use mybin_core::resultset::{ColumnExtractor, RowMapper};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use uuid::adapter::Hyphenated;
use uuid::Uuid;

/// wrapper on connection to provide binlog functionalities
///
/// if server has GTID_MODE=ON, use ComBinlogDumpGtid
/// otherwise, use ComBinlogDump
#[derive(Debug)]
pub struct Binlog<'s, S> {
    conn: &'s mut Conn<S>,
    binlog_filename: String,
    binlog_pos: u64,
    server_id: u32,
    sids: Vec<SidRange>,
    non_block: bool,
    validate_checksum: bool,
}

impl<'s, S> Binlog<'s, S> {
    pub fn new(conn: &'s mut Conn<S>) -> Self {
        Binlog {
            conn,
            binlog_filename: String::new(),
            binlog_pos: 4,
            server_id: 0,
            sids: vec![],
            non_block: false,
            validate_checksum: false,
        }
    }

    pub fn binlog_pos(mut self, binlog_pos: u64) -> Self {
        self.binlog_pos = binlog_pos;
        self
    }

    pub fn binlog_filename<T: Into<String>>(mut self, binlog_filename: T) -> Self {
        self.binlog_filename = binlog_filename.into();
        self
    }

    pub fn server_id(mut self, server_id: u32) -> Self {
        self.server_id = server_id;
        self
    }

    pub fn sid(mut self, sid: SidRange) -> Self {
        self.sids.push(sid);
        self
    }

    pub fn sids(mut self, sids: Vec<SidRange>) -> Self {
        self.sids.extend(sids);
        self
    }

    pub fn non_block(mut self, non_block: bool) -> Self {
        self.non_block = non_block;
        self
    }

    pub fn validate_checksum(mut self, validate_checksum: bool) -> Self {
        self.validate_checksum = validate_checksum;
        self
    }
}

impl<'s, S> Binlog<'s, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn stream(self) -> Result<BinlogStream<'s, S>> {
        use rand::Rng;
        log::debug!("setup preconditions before request binlog stream");
        // 1. fetch server_id as master_id
        let master_id: u32 = self
            .conn
            .get_var("SERVER_ID", true)
            .await?
            .ok_or_else(|| Error::EmptyResultSet)?;
        log::debug!("server_id={}", master_id);
        // 2. set @master_heartbeat_period = 30
        //    mysql will send HeartbeatLogEvent if in such period there is no event
        //    client should have large network timeout than period and can silently
        //    discard the heartbeat event
        self.conn
            .set_user_var("MASTER_HEARTBEAT_PERIOD", 30u32)
            .await?;
        log::debug!("set @master_heartbeat_period to 30");
        // 3. fetch binlog_checksum
        //    the binlog parser needs to know if checksum is enabled
        let binlog_checksum: String = self
            .conn
            .get_var("BINLOG_CHECKSUM", true)
            .await?
            .ok_or_else(|| Error::CustomError("missing variable binlog_checksum".to_owned()))?;
        log::debug!("binlog_checksum={}", binlog_checksum);
        // the only available checksum algorithm is CRC32
        let checksum = binlog_checksum == "CRC32";
        // 4. set @master_binlog_checksum same as binlog_checksum
        log::debug!("set @master_binlog_checksum to {}", binlog_checksum);
        self.conn
            .set_user_var("MASTER_BINLOG_CHECKSUM", binlog_checksum)
            .await?;
        // 5. fetch gtid_mode
        let gtid_mode: String = self
            .conn
            .get_var("GTID_MODE", true)
            .await?
            .ok_or_else(|| Error::CustomError("missing variable gtid_mode".to_owned()))?;
        log::debug!("gtid_mode={}", gtid_mode);
        // 6. fetch server_uuid
        let server_uuid: String = self
            .conn
            .get_var("SERVER_UUID", true)
            .await?
            .ok_or_else(|| Error::CustomError("missing variable server_uuid".to_owned()))?;
        log::debug!("server_uuid={}", server_uuid);
        // 7. set @slave_uuid to random uuid
        let slave_uuid = {
            let mut buf = vec![0u8; Hyphenated::LENGTH];
            let s = Uuid::new_v4().to_hyphenated().encode_lower(&mut buf);
            s.to_owned()
        };
        log::debug!("set @slave_uuid to {}", slave_uuid);
        self.conn.set_user_var("SLAVE_UUID", slave_uuid).await?;

        // 8. register slave
        let slave_id: u32 = if self.server_id != 0 {
            self.server_id
        } else {
            rand::thread_rng().gen()
        };
        let register = ComRegisterSlave::new(slave_id, master_id);
        self.conn.send_msg(register, true).await?;
        let mut msg = self.conn.recv_msg().await?;
        match ComRegisterSlaveResponse::read_with_ctx(&mut msg, &self.conn.cap_flags)? {
            ComRegisterSlaveResponse::Ok(_) => (),
            ComRegisterSlaveResponse::Err(err) => return Err(err.into()),
        }
        log::debug!("register slave with slave_id={}", slave_id);
        // 9. binlog dump
        if gtid_mode == "ON" {
            let dump = ComBinlogDumpGtid::default()
                .server_id(slave_id)
                .binlog_filename(self.binlog_filename.clone())
                .binlog_pos(self.binlog_pos)
                .sids(self.sids.clone())
                .use_gtid(!self.sids.is_empty())
                .non_block(self.non_block);
            log::debug!("gtid_dump={:?}", dump);
            self.conn.send_msg(dump, true).await?;
        } else {
            let dump = ComBinlogDump::default()
                .server_id(slave_id)
                .binlog_filename(self.binlog_filename.clone())
                .binlog_pos(self.binlog_pos as u32)
                .non_block(self.non_block);
            log::debug!("dump={:?}", dump);
            self.conn.send_msg(dump, true).await?;
        }
        let mut msg = self.conn.recv_msg().await?;
        if !msg.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        match msg[0] {
            0xff => {
                let err = ErrPacket::read_with_ctx(&mut msg, (&self.conn.cap_flags, true))?;
                return Err(err.into());
            }
            0xfe => {
                EofPacket::read_with_ctx(&mut msg, &self.conn.cap_flags)?;
                return Ok(BinlogStream {
                    conn: self.conn,
                    // a pseudo parser which won't be called
                    pv4: ParserV4::new(vec![], false),
                    validate_checksum: self.validate_checksum,
                    completed: true,
                });
            }
            0x00 => {
                // first event is always RotateEvent
                // remove single byte of 0x00
                msg.read_u8()?;
                let eh = EventHeader::read_from(&mut msg)?;
                if LogEventType::from(eh.type_code) != LogEventType::RotateEvent {
                    return Err(Error::CustomError(
                        "first event of binlog stream must be fake RotateEvent".to_owned(),
                    ));
                }
                if checksum {
                    let mut crc32 = msg.split_off(msg.remaining() - 4);
                    let crc32 = crc32.read_le_u32()?;
                    log::debug!("checksum={}", crc32);
                }
                let rd = RotateData::read_from(&mut msg)?;
                log::debug!("rotate={:?}", rd);
            }
            _ => {
                return Err(Error::PacketError(format!(
                    "invalid binlog stream header {:02x}",
                    msg[0]
                )))
            }
        }

        // second event is always FDE, and we can construct parser from this event
        let mut msg = self.conn.recv_msg().await?;
        msg.read_u8()?;
        let (pv4, crc32) = ParserV4::from_fde_bytes(&mut msg)?;
        if let Some(crc32) = crc32 {
            log::debug!("checksum={:?}", crc32);
        }
        log::debug!("pv4={:?}", pv4);
        Ok(BinlogStream {
            conn: self.conn,
            pv4,
            validate_checksum: self.validate_checksum,
            completed: false,
        })
    }
}

#[derive(Debug, Clone)]
pub struct BinlogFile {
    pub filename: String,
    pub size: u64,
}

#[derive(Debug)]
pub struct BinlogFileMapper;

impl RowMapper<TextColumnValue> for BinlogFileMapper {
    type Output = Result<BinlogFile>;

    fn map_row(&self, extr: &ColumnExtractor, row: Vec<TextColumnValue>) -> Self::Output {
        let filename = extr.get_col(&row, 0)?;
        let size = extr.get_col(&row, 1)?;
        Ok(BinlogFile { filename, size })
    }
}

#[derive(Debug)]
pub struct BinlogStream<'s, S> {
    conn: &'s mut Conn<S>,
    pv4: ParserV4,
    validate_checksum: bool,
    completed: bool,
}

impl<'s, S> Stream for BinlogStream<'s, S>
where
    S: AsyncRead + Unpin,
{
    type Item = Result<Event>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.completed {
            return Poll::Ready(None);
        }

        loop {
            let mut recv_msg = self.conn.recv_msg();
            match ready!(Pin::new(&mut recv_msg).poll(cx)) {
                Ok(mut msg) => {
                    // check header
                    if !msg.has_remaining() {
                        return Poll::Ready(Some(Err(Error::InputIncomplete(
                            Bytes::new(),
                            Needed::Unknown,
                        ))));
                    }
                    // won't fail
                    let header = msg.read_u8().unwrap();
                    if header != 0x00 {
                        log::trace!("non-event packet={:?}", msg.bytes());
                        self.completed = true;
                        return Poll::Ready(None);
                    }
                    match self.pv4.parse_event(&mut msg, self.validate_checksum) {
                        Ok(Some(event)) => {
                            log::trace!("parsed event={:?}", event);
                            return Poll::Ready(Some(Ok(event)));
                        }
                        Ok(None) => log::trace!("unsupported event"),
                        Err(e) => return Poll::Ready(Some(Err(e.into()))),
                    }
                }
                Err(e) => return Poll::Ready(Some(Err(e.into()))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // use super::*;
    use crate::conn::tests::new_conn;
    // use bigdecimal::BigDecimal;
    use uuid::adapter::Hyphenated;
    use uuid::Uuid;

    #[smol_potat::test]
    async fn test_show_binlog_related_variables() {
        let mut conn = new_conn().await;
        let server_id: Option<u32> = conn.get_var("SERVER_ID", true).await.unwrap();
        dbg!(server_id);
        let binlog_checksum: Option<String> = conn.get_var("BINLOG_CHECKSUM", true).await.unwrap();
        dbg!(binlog_checksum);
        let gtid_mode: Option<String> = conn.get_var("GTID_MODE", true).await.unwrap();
        dbg!(gtid_mode);
        let server_uuid: Option<String> = conn.get_var("SERVER_UUID", true).await.unwrap();
        dbg!(server_uuid);
    }

    #[smol_potat::test]
    async fn test_setup_binlog_related_variables() {
        let mut conn = new_conn().await;
        conn.set_user_var("MASTER_HEARTBEAT_PERIOD", 30u32)
            .await
            .unwrap();
        let prd: Option<u32> = conn.get_user_var("MASTER_HEARTBEAT_PERIOD").await.unwrap();
        assert_eq!(30, prd.unwrap());
        conn.set_user_var("MASTER_BINLOG_CHECKSUM", "CRC32".to_owned())
            .await
            .unwrap();
        let chksum: Option<String> = conn.get_user_var("MASTER_BINLOG_CHECKSUM").await.unwrap();
        assert_eq!("CRC32", chksum.as_ref().unwrap());
        let uuid1 = {
            let mut buf = vec![0u8; Hyphenated::LENGTH];
            Uuid::new_v4().to_hyphenated().encode_lower(&mut buf);
            String::from_utf8(buf).unwrap()
        };
        conn.set_user_var("SLAVE_UUID", uuid1.clone())
            .await
            .unwrap();
        let uuid2: Option<String> = conn.get_user_var("SLAVE_UUID").await.unwrap();
        assert_eq!(&uuid1, uuid2.as_ref().unwrap());
        dbg!(uuid2);
    }

    #[smol_potat::test]
    async fn test_request_binlog_stream() {
        env_logger::init();
        use futures::StreamExt;
        let mut conn = new_conn().await;
        let mut binlog_stream = conn
            .binlog()
            .binlog_filename("mysql-bin.000001")
            .binlog_pos(4)
            .non_block(true)
            .stream()
            .await
            .unwrap();
        let mut cnt = 0;
        while let Some(re) = binlog_stream.next().await {
            dbg!(re.unwrap());
            cnt += 1;
            if cnt == 10 {
                break;
            }
        }
    }
}
