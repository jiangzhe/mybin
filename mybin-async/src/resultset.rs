use crate::conn::Conn;
use crate::error::{Error, Result};
use bytes::{Buf, Bytes};
use bytes_parser::my::LenEncInt;
use bytes_parser::ReadFromBytes;
use futures::{AsyncRead, AsyncWrite};
use mybin_core::col::{BinaryColumnValue, ColumnDefinition, ColumnType, TextColumnValue};
use mybin_core::flag::CapabilityFlags;
use mybin_core::packet::{EofPacket, ErrPacket, OkPacket};
use mybin_core::resultset::{ColumnExtractor, RowMapper};
use mybin_core::row::{BinaryRow, TextRow};
use mybin_core::cmd::ComStmtClose;
use std::marker::PhantomData;

/// construct a new result set from given connection
///
/// the incoming bytes should follow either text protocol or binary protocol of result set
/// https://dev.mysql.com/doc/internals/en/com-query-response.html#packet-ProtocolText::Resultset
/// https://dev.mysql.com/doc/internals/en/binary-protocol-resultset.html
pub async fn new_result_set<'s, S, Q>(conn: &'s mut Conn<S>, stmt_id: Option<u32>) -> Result<ResultSet<'s, S, Q>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut msg = conn.recv_msg().await?;
    let col_cnt = parse_col_cnt_packet(&mut msg, &conn.cap_flags)?;
    if col_cnt == 0 {
        return Ok(ResultSet::empty(conn, stmt_id));
    }
    let mut col_defs = Vec::with_capacity(col_cnt as usize);
    for _ in 0..col_cnt {
        let mut msg = conn.recv_msg().await?;
        let col_def = ColumnDefinition::read_from(&mut msg, false)?;
        log::trace!("col_def={:?}", col_def);
        col_defs.push(col_def);
    }
    if !conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
        // additional EOF if not deprecate
        let mut msg = conn.recv_msg().await?;
        EofPacket::read_from(&mut msg, &conn.cap_flags)?;
    }
    // incoming rows
    Ok(ResultSet::new(conn, col_defs, stmt_id))
}

/// parse column count packet
/// if returns 0, means the response is completed
fn parse_col_cnt_packet(msg: &mut Bytes, cap_flags: &CapabilityFlags) -> Result<u32> {
    match msg[0] {
        0xff => {
            let err = ErrPacket::read_from(msg, cap_flags, true)?;
            Err(err.into())
        }
        0x00 => {
            OkPacket::read_from(msg, cap_flags)?;
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
#[must_use = "futures do nothing unless you `.await` or poll them"]
#[derive(Debug)]
pub struct ResultSet<'s, S: 's, Q> {
    pub(crate) conn: &'s mut Conn<S>,
    // unchangable col defs
    pub col_defs: Vec<ColumnDefinition>,
    pub(crate) completed: bool,
    // only used for binary columns
    col_types: Vec<ColumnType>,
    stmt_id: Option<u32>,
    _marker: PhantomData<Q>,
}

impl<'s, S: 's, Q> ResultSet<'s, S, Q> {
    /// construct an empty result set
    // todo
    pub fn empty(conn: &'s mut Conn<S>, stmt_id: Option<u32>) -> Self {
        Self {
            conn,
            col_defs: vec![],
            completed: true,
            col_types: vec![],
            stmt_id,
            _marker: PhantomData,
        }
    }

    pub fn new(conn: &'s mut Conn<S>, col_defs: Vec<ColumnDefinition>, stmt_id: Option<u32>) -> Self {
        let col_types = col_defs.iter().map(|d| d.col_type).collect();
        Self {
            conn,
            col_defs,
            completed: false,
            col_types,
            stmt_id,
            _marker: PhantomData,
        }
    }

    /// create a column extractor base on column definitions
    pub fn extractor(&self) -> ColumnExtractor {
        ColumnExtractor::new(&self.col_defs)
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

impl<'s, S: 's, Q> ResultSet<'s, S, Q>
where
    S: AsyncRead + Unpin,
    Self: RowReader<Column=Q>,
{
    pub async fn all(mut self) -> Result<Vec<Vec<Q>>> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    pub async fn first(self) -> Result<Vec<Q>> {
        self.first_or_none()
            .await?
            .ok_or_else(|| Error::EmptyResultSet)
    }

    pub async fn first_or_none(mut self) -> Result<Option<Vec<Q>>> {
        let mut first = None;
        while let Some(row) = self.next_row().await? {
            if first.is_none() {
                first.replace(row);
            }
        }
        Ok(first)
    }

    pub async fn count(mut self) -> Result<usize> {
        let mut cnt = 0;
        while let Some(_) = self.next_row().await? {
            cnt += 1;
        }
        Ok(cnt)
    }

    pub async fn next_row(&mut self) -> Result<Option<Vec<Q>>> {
        if self.completed {
            return Ok(None);
        }
        let mut msg = self.conn.recv_msg().await?;
        if !msg.has_remaining() {
            // log::warn!("payload is empty");
            return Err(Error::PacketError("payload is empty".to_owned()));
        }
        match msg[0] {
            // EOF Packet
            0xfe if msg.remaining() <= 0xffffff => {
                if self.conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                    match OkPacket::read_from(&mut msg, &self.conn.cap_flags) {
                        Ok(_) => {
                            self.completed = true;
                            return Ok(None);
                        }
                        Err(e) => {
                            // log::warn!("parse ok packet error {}", e);
                            return Err(Error::PacketError(e.to_string()));
                        }
                    }
                }
                match EofPacket::read_from(&mut msg, &self.conn.cap_flags) {
                    Ok(_) => {
                        self.completed = true;
                        Ok(None)
                    }
                    Err(e) => {
                        // log::warn!("parse eof packet error {}", e);
                        Err(Error::PacketError(e.to_string()))
                    }
                }
            }
            _ => {
                let r = self.read_row(&mut msg)?;
                Ok(Some(r))
            }
        }
    }
}

impl<'s, S: 's, Q> ResultSet<'s, S, Q>
where
    S: AsyncWrite + Unpin,
{
    pub async fn close(&mut self) -> Result<()> {
        if let Some(stmt_id) = self.stmt_id.take() {
            let cmd = ComStmtClose::new(stmt_id);
            self.conn.send_msg(cmd, true).await?;
        }
        Ok(())
    }
}

pub trait RowReader {
    type Column;

    fn read_row(&self, input: &mut Bytes) -> Result<Vec<Self::Column>>;
}

impl<'s, S> RowReader for ResultSet<'s, S, BinaryColumnValue> {
    type Column = BinaryColumnValue;
    fn read_row(&self, input: &mut Bytes) -> Result<Vec<Self::Column>> {
        let r = BinaryRow::read_from(input, &self.col_types)?;
        Ok(r.0)
    }
}

impl<'s, S> RowReader for ResultSet<'s, S, TextColumnValue> {
    type Column = TextColumnValue;
    fn read_row(&self, input: &mut Bytes) -> Result<Vec<Self::Column>> {
        let r = TextRow::read_from(input, self.col_defs.len())?;
        Ok(r.0)
    }
}

pub struct MapperResultSet<'s, S: 's, M, Q> {
    rs: ResultSet<'s, S, Q>,
    mapper: M,
    extractor: ColumnExtractor,
}

impl<'s, S: 's, M, Q> MapperResultSet<'s, S, M, Q>
where
    S: AsyncRead + Unpin,
    M: RowMapper<Q> + Unpin,
    ResultSet<'s, S, Q>: RowReader<Column=Q>,
{
    pub async fn all(mut self) -> Result<Vec<M::Output>> {
        let mut rows = Vec::new();
        while let Some(row) = self.next_row().await? {
            rows.push(row);
        }
        Ok(rows)
    }

    pub async fn first(self) -> Result<M::Output> {
        self.first_or_none()
            .await?
            .ok_or_else(|| Error::EmptyResultSet)
    }

    pub async fn first_or_none(mut self) -> Result<Option<M::Output>> {
        let mut first = None;
        while let Some(row) = self.next_row().await? {
            if first.is_none() {
                first.replace(row);
            }
        }
        Ok(first)
    }

    pub async fn count(mut self) -> Result<usize> {
        let mut cnt = 0;
        while let Some(_) = self.next_row().await? {
            cnt += 1;
        }
        Ok(cnt)
    }

    pub async fn next_row(&mut self) -> Result<Option<<M as RowMapper<Q>>::Output>> {
        let r = self.rs.next_row().await?.map(|r| self.mapper.map_row(&self.extractor, r));
        Ok(r)
    }
}

impl<'s, S: 's, M, Q> MapperResultSet<'s, S, M, Q>
where
    S: AsyncWrite + Unpin,
{
    // close() should always be called after consuming all rows
    // to release server resource
    pub async fn close(mut self) -> Result<()> {
        self.rs.close().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conn::tests::new_conn;

    #[smol_potat::test]
    async fn test_result_set_ops_simple() {
        let mut conn = new_conn().await;
        let all_rs = conn.query().qry("select 1").await.unwrap().all().await.unwrap();
        assert_eq!(1, all_rs.len());
        let first_rs = conn.query().qry("select 1").await.unwrap().first().await;
        dbg!(first_rs.unwrap());
        let first_or_none_rs = conn
            .query()
            .qry("select 1")
            .await
            .unwrap()
            .first_or_none()
            .await.unwrap();
        assert!(first_or_none_rs.is_some());
        let count_rs = conn.query().qry("select 1").await.unwrap().count().await;
        assert_eq!(1, count_rs.unwrap());
    }

    #[smol_potat::test]
    async fn test_result_set_ops_mapper() {
        let mut conn = new_conn().await;
        let all_rs = conn
            .query()
            .qry("select 1")
            .await
            .unwrap()
            .map_rows(|_extr: &ColumnExtractor, _row: Vec<TextColumnValue>| ())
            .all()
            .await.unwrap();
        assert_eq!(1, all_rs.len());
        let first_rs = conn
            .query()
            .qry("select 1")
            .await
            .unwrap()
            .map_rows(|_extr: &ColumnExtractor, _row: Vec<TextColumnValue>| ())
            .first()
            .await;
        dbg!(first_rs.unwrap());
        let first_or_none_rs = conn
            .query()
            .qry("select 1")
            .await
            .unwrap()
            .map_rows(|_extr: &ColumnExtractor, _row: Vec<TextColumnValue>| ())
            .first_or_none()
            .await.unwrap();
        assert!(first_or_none_rs.is_some());
        let count_rs = conn
            .query()
            .qry("select 1")
            .await
            .unwrap()
            .map_rows(|_extr: &ColumnExtractor, _row: Vec<TextColumnValue>| ())
            .count()
            .await;
        assert_eq!(1, count_rs.unwrap());
    }

    #[smol_potat::test]
    async fn test_result_set_ops_empty() {
        let mut conn = new_conn().await;
        let all_rs = conn
            .query()
            .qry("select 1 from dual where 1 = 2")
            .await
            .unwrap()
            .all()
            .await.unwrap();
        assert!(all_rs.is_empty());
        let first_rs = conn
            .query()
            .qry("select 1 from dual where 1 = 2")
            .await
            .unwrap()
            .first()
            .await;
        dbg!(first_rs.unwrap_err());
        let first_or_none_rs = conn
            .query()
            .qry("select 1 from dual where 1 = 2")
            .await
            .unwrap()
            .first_or_none()
            .await.unwrap();
        assert!(first_or_none_rs.is_none());
        let count_rs = conn
            .query()
            .qry("select 1 from dual where 1 = 2")
            .await
            .unwrap()
            .count()
            .await;
        assert_eq!(0, count_rs.unwrap());
    }
}
