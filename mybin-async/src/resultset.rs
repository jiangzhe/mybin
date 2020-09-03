use crate::conn::Conn;
use crate::error::{Error, Result};
use bytes::{Buf, Bytes};
use bytes_parser::my::LenEncInt;
use bytes_parser::{ReadFromBytes, ReadFromBytesWithContext};
use futures::stream::Stream;
use futures::{ready, AsyncRead, AsyncWrite};
use mybin_core::col::{BinaryColumnValue, ColumnDefinition, ColumnType, TextColumnValue};
use mybin_core::flag::CapabilityFlags;
use mybin_core::packet::{EofPacket, ErrPacket, OkPacket};
use mybin_core::resultset::{ResultSetColExtractor, RowMapper};
use mybin_core::row::{BinaryRow, TextRow};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::task::{Context, Poll};

/// construct a new result set from given connection
///
/// the incoming bytes should follow either text protocol or binary protocol of result set
/// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
/// https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
pub async fn new_result_set<'s, S, Q>(conn: &'s mut Conn<S>) -> Result<ResultSet<'s, S, Q>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut msg = conn.recv_msg().await?;
    let col_cnt = parse_col_cnt_packet(&mut msg, &conn.cap_flags)?;
    if col_cnt == 0 {
        return Ok(ResultSet::empty(conn));
    }
    let mut col_defs = Vec::with_capacity(col_cnt as usize);
    for _ in 0..col_cnt {
        let mut msg = conn.recv_msg().await?;
        let col_def = ColumnDefinition::read_with_ctx(&mut msg, false)?;
        log::trace!("col_def={:?}", col_def);
        col_defs.push(col_def);
    }
    if !conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
        // additional EOF if not deprecate
        let mut msg = conn.recv_msg().await?;
        EofPacket::read_with_ctx(&mut msg, &conn.cap_flags)?;
    }
    // incoming rows
    Ok(ResultSet::new(conn, col_defs))
}

/// parse column count packet
/// if returns 0, means the response is completed
fn parse_col_cnt_packet(msg: &mut Bytes, cap_flags: &CapabilityFlags) -> Result<u32> {
    match msg[0] {
        0xff => {
            let err = ErrPacket::read_with_ctx(msg, (cap_flags, true))?;
            Err(err.into())
        }
        0x00 => {
            OkPacket::read_with_ctx(msg, cap_flags)?;
            return Ok(0);
        }
        _ => {
            let lei = LenEncInt::read_from(msg)?;
            Ok(lei.to_u32().ok_or_else(|| {
                Error::PacketError(format!("invalid column count packet header={:02x}", msg[0]))
            })?)
        }
    }
}

/// async result set
#[derive(Debug)]
pub struct ResultSet<'s, S: 's, Q> {
    pub(crate) conn: &'s mut Conn<S>,
    // unchangable col defs
    pub col_defs: Vec<ColumnDefinition>,
    pub(crate) completed: bool,
    // only used for binary columns
    col_types: Vec<ColumnType>,
    _marker: PhantomData<Q>,
}

impl<'s, S: 's, Q> ResultSet<'s, S, Q> {
    /// construct an empty result set
    pub fn empty(conn: &'s mut Conn<S>) -> Self {
        Self {
            conn,
            col_defs: vec![],
            completed: true,
            col_types: vec![],
            _marker: PhantomData,
        }
    }

    pub fn new(conn: &'s mut Conn<S>, col_defs: Vec<ColumnDefinition>) -> Self {
        let col_types = col_defs.iter().map(|d| d.col_type).collect();
        Self {
            conn,
            col_defs,
            completed: false,
            col_types,
            _marker: PhantomData,
        }
    }

    /// create a column extractor base on column definitions
    pub fn extractor(&self) -> ResultSetColExtractor {
        ResultSetColExtractor::new(&self.col_defs)
    }

    pub fn map_rows<M>(self, mapper: M) -> MapperResultSet<'s, S, M, Q>
    where
        M: RowMapper<Q> + Unpin,
    {
        let extractor = self.extractor();
        MapperResultSet {
            rs: self,
            mapper,
            extractor,
        }
    }
}

impl<'s, S: 's> Stream for ResultSet<'s, S, TextColumnValue>
where
    S: AsyncRead + Unpin,
{
    type Item = Vec<TextColumnValue>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.completed {
            return Poll::Ready(None);
        }

        let mut recv_fut = self.conn.recv_msg();
        match ready!(Pin::new(&mut recv_fut).as_mut().poll(cx)) {
            Err(err) => {
                log::warn!("parse message error: {:?}", err);
                Poll::Ready(None)
            }
            Ok(mut msg) => {
                if !msg.has_remaining() {
                    log::warn!("message is empty: {:?}", msg);
                    return Poll::Ready(None);
                }
                match msg[0] {
                    // EOF Packet
                    0xfe if msg.remaining() <= 0xffffff => {
                        if self.conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                            match OkPacket::read_with_ctx(&mut msg, &self.conn.cap_flags) {
                                Ok(_) => {
                                    self.completed = true;
                                    return Poll::Ready(None);
                                }
                                Err(e) => {
                                    log::warn!("parse ok packet error {}", e);
                                    return Poll::Ready(None);
                                }
                            }
                        }
                        match EofPacket::read_with_ctx(&mut msg, &self.conn.cap_flags) {
                            Ok(_) => {
                                self.completed = true;
                                Poll::Ready(None)
                            }
                            Err(e) => {
                                log::warn!("parse eof packet error {}", e);
                                Poll::Ready(None)
                            }
                        }
                    }
                    _ => match TextRow::read_with_ctx(&mut msg, self.col_defs.len()) {
                        Ok(row) => Poll::Ready(Some(row.0)),
                        Err(e) => {
                            log::warn!("parse text row error {}", e);
                            Poll::Ready(None)
                        }
                    },
                }
            }
        }
    }
}

impl<'s, S: 's> Stream for ResultSet<'s, S, BinaryColumnValue>
where
    S: AsyncRead + Unpin,
{
    type Item = Vec<BinaryColumnValue>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.completed {
            return Poll::Ready(None);
        }

        let mut recv_fut = self.conn.recv_msg();
        match ready!(Pin::new(&mut recv_fut).as_mut().poll(cx)) {
            Err(err) => {
                log::warn!("parse message error: {:?}", err);
                Poll::Ready(None)
            }
            Ok(mut msg) => {
                if !msg.has_remaining() {
                    log::warn!("message is empty: {:?}", msg);
                    return Poll::Ready(None);
                }
                match msg[0] {
                    // EOF Packet
                    0xfe if msg.remaining() <= 0xffffff => {
                        if self.conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                            match OkPacket::read_with_ctx(&mut msg, &self.conn.cap_flags) {
                                Ok(_) => {
                                    self.completed = true;
                                    return Poll::Ready(None);
                                }
                                Err(e) => {
                                    log::warn!("parse ok packet error {}", e);
                                    return Poll::Ready(None);
                                }
                            }
                        }
                        match EofPacket::read_with_ctx(&mut msg, &self.conn.cap_flags) {
                            Ok(_) => {
                                self.completed = true;
                                Poll::Ready(None)
                            }
                            Err(e) => {
                                log::warn!("parse eof packet error {}", e);
                                Poll::Ready(None)
                            }
                        }
                    }
                    _ => match BinaryRow::read_with_ctx(&mut msg, &self.col_types) {
                        Ok(row) => Poll::Ready(Some(row.0)),
                        Err(e) => {
                            log::warn!("parse text row error {}", e);
                            Poll::Ready(None)
                        }
                    },
                }
            }
        }
    }
}

pub struct MapperResultSet<'s, S: 's, M, Q> {
    rs: ResultSet<'s, S, Q>,
    mapper: M,
    extractor: ResultSetColExtractor,
}

impl<'s, S: 's, M, Q> Stream for MapperResultSet<'s, S, M, Q>
where
    S: AsyncRead + Unpin,
    M: RowMapper<Q> + Unpin,
    Q: Unpin,
    ResultSet<'s, S, Q>: Stream<Item = Vec<Q>>,
{
    type Item = <M as RowMapper<Q>>::Output;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(Pin::new(&mut self.rs).poll_next(cx)) {
            Some(row) => {
                let item = self.mapper.map_row(&self.extractor, row);
                Poll::Ready(Some(item))
            }
            None => Poll::Ready(None),
        }
    }
}
