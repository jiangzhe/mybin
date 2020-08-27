use crate::conn::Conn;
use crate::error::{Error, Result, SqlError};
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::WriteToBytes;
use futures::stream::Stream;
use futures::{ready, AsyncRead, AsyncWrite};
use mybin_core::col::{ColumnDefinition, TextColumnValue};
use mybin_core::query::{ComQuery, ComQueryResponse, ComQueryState, ComQueryStateMachine};
use mybin_core::resultset::{ResultSetColExtractor, RowMapper};
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct Query<'a, S> {
    conn: &'a mut Conn<S>,
}

impl<'a, S> Query<'a, S>
where
    S: AsyncRead + AsyncWrite + Clone + Unpin,
{
    pub fn new(conn: &'a mut Conn<S>) -> Self {
        Query { conn }
    }

    pub fn exec<Q: Into<String>>(self, qry: Q) -> QueryExecFuture<'a, S> {
        let qry = ComQuery::new(qry);
        QueryExecFuture::new(self.conn, qry)
    }

    pub fn qry<Q: Into<String>>(self, qry: Q) -> QueryResultSetFuture<'a, S> {
        let qry = ComQuery::new(qry);
        QueryResultSetFuture::new(self.conn, qry)
    }
}

#[derive(Debug)]
pub struct QueryExecFuture<'s, S: 's> {
    conn: &'s mut Conn<S>,
    qry: Bytes,
    sm: ComQueryStateMachine,
    // use PhantomData to prevent concurrent
    // usage on same connection
    _marker: PhantomData<&'s S>,
}

impl<'s, S: 's> QueryExecFuture<'s, S> {
    pub fn new<Q>(conn: &'s mut Conn<S>, qry: Q) -> Self
    where
        Q: WriteToBytes,
    {
        let mut bs = BytesMut::new();
        qry.write_to(&mut bs).unwrap();
        let cap_flags = conn.cap_flags.clone();
        QueryExecFuture {
            conn,
            qry: bs.freeze(),
            sm: ComQueryStateMachine::new(cap_flags),
            _marker: PhantomData,
        }
    }

    fn on_msg(&mut self, msg: Bytes) -> Result<bool> {
        match self.sm.next(msg)? {
            (_, ComQueryResponse::Err(err)) => {
                return Err(Error::SqlError(SqlError {
                    error_code: err.error_code,
                    sql_state_marker: err.sql_state_marker,
                    sql_state: String::from_utf8(Vec::from(err.sql_state.bytes()))?,
                    error_message: String::from_utf8(Vec::from(err.error_message.bytes()))?,
                }));
            }
            (_, ComQueryResponse::Ok(_)) => {
                return Ok(false);
            }
            (ComQueryState::Ok, ComQueryResponse::Eof(_)) => {
                return Ok(false);
            }
            _ => Ok(true),
        }
    }
}

impl<'a, S> Future for QueryExecFuture<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<()>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.qry.has_remaining() {
            let qry = self.qry.clone();
            let mut send_fut = self.conn.send_msg(qry, true);
            match ready!(Pin::new(&mut send_fut).poll(cx)) {
                Ok(_) => {
                    self.qry.clear();
                }
                Err(e) => return Poll::Ready(Err(e.into())),
            }
        }
        loop {
            let mut recv_fut = self.conn.recv_msg();
            match ready!(Pin::new(&mut recv_fut).poll(cx)) {
                Ok(msg) => match self.on_msg(msg) {
                    Err(e) => return Poll::Ready(Err(e)),
                    Ok(cont) => {
                        if !cont {
                            return Poll::Ready(Ok(()));
                        }
                    }
                },
                Err(e) => {
                    return Poll::Ready(Err(e.into()));
                }
            }
        }
    }
}

/// async result set
#[derive(Debug)]
pub struct QueryResultSet<'s, S: 's> {
    conn: Conn<S>,
    sm: ComQueryStateMachine,
    // unchangable col defs
    pub col_defs: Arc<Vec<ColumnDefinition>>,
    // use PhantomData to prevent concurrent
    // usage on same connection
    _marker: PhantomData<&'s S>,
}

impl<'s, S: 's> QueryResultSet<'s, S> {
    /// create a column extractor base on column definitions
    pub fn extractor(&self) -> ResultSetColExtractor {
        ResultSetColExtractor::new(&self.col_defs)
    }

    pub fn map_rows<M>(self, mapper: M) -> MapperResultSet<'s, S, M>
    where
        M: RowMapper<TextColumnValue> + Unpin,
    {
        let extractor = self.extractor();
        MapperResultSet {
            rs: self,
            mapper,
            extractor,
        }
    }
}

impl<'s, S: 's> Stream for QueryResultSet<'s, S>
where
    S: AsyncRead + Unpin,
{
    type Item = Vec<TextColumnValue>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.sm.end() {
            return Poll::Ready(None);
        }
        let Self { conn, sm, .. } = &mut *self;
        let recv_fut = &mut conn.recv_msg();
        let mut recv_fut = Pin::new(recv_fut);
        match ready!(recv_fut.as_mut().poll(cx)) {
            Ok(msg) => match sm.next(msg) {
                Ok((_, ComQueryResponse::Eof(_))) | Ok((_, ComQueryResponse::Ok(_))) => {
                    Poll::Ready(None)
                }
                Ok((_, ComQueryResponse::Row(row))) => Poll::Ready(Some(row.0)),
                other => {
                    log::warn!("unexpected packet in result set: {:?}", other);
                    Poll::Ready(None)
                }
            },
            Err(err) => {
                log::debug!("parse message error: {:?}", err);
                return Poll::Ready(None);
            }
        }
    }
}

pub struct MapperResultSet<'s, S: 's, M> {
    rs: QueryResultSet<'s, S>,
    mapper: M,
    extractor: ResultSetColExtractor,
}

impl<'s, S: 's, M> Stream for MapperResultSet<'s, S, M>
where
    S: AsyncRead + Unpin,
    M: RowMapper<TextColumnValue> + Unpin,
{
    type Item = <M as RowMapper<TextColumnValue>>::Output;

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

#[derive(Debug)]
pub struct QueryResultSetFuture<'a, S> {
    conn: &'a mut Conn<S>,
    qry: Bytes,
    sm: ComQueryStateMachine,
    col_defs: Vec<ColumnDefinition>,
}

impl<'a, S: 'a> QueryResultSetFuture<'a, S> {
    pub fn new<Q: WriteToBytes>(conn: &'a mut Conn<S>, qry: Q) -> Self
    where
        S: Clone,
    {
        let mut bs = BytesMut::new();
        qry.write_to(&mut bs).unwrap();
        let cap_flags = conn.cap_flags.clone();
        QueryResultSetFuture {
            conn: conn,
            qry: bs.freeze(),
            sm: ComQueryStateMachine::new(cap_flags),
            col_defs: vec![],
        }
    }

    fn on_msg(&mut self, msg: Bytes) -> Result<bool> {
        match self.sm.next(msg)? {
            (_, ComQueryResponse::Err(err)) => {
                log::debug!("receive error packet: {:?}", err);
                Err(Error::SqlError(SqlError {
                    error_code: err.error_code,
                    sql_state_marker: err.sql_state_marker,
                    sql_state: String::from_utf8(Vec::from(err.sql_state.bytes()))?,
                    error_message: String::from_utf8(Vec::from(err.error_message.bytes()))?,
                }))
            }
            (_, ComQueryResponse::Ok(ok)) => {
                log::debug!("receive ok packet: {:?}", ok);
                Ok(false)
            }
            (_, ComQueryResponse::ColCnt(cnt)) => {
                log::debug!("receive column count packet: {}", cnt);
                Ok(true)
            }
            (ComQueryState::ColDefs(..), ComQueryResponse::ColDef(col_def)) => {
                log::debug!("receive column definition packet: {:?}", col_def);
                self.col_defs.push(col_def);
                Ok(true)
            }
            (ComQueryState::Rows, ComQueryResponse::ColDef(col_def)) => {
                // DEPRECATE_EOF is enabled, so the EOF following col defs will not
                // be sent
                log::debug!("receive the last column definition packet: {:?}", col_def);
                self.col_defs.push(col_def);
                Ok(false)
            }
            (ComQueryState::Rows, ComQueryResponse::Eof(eof)) => {
                // DEPRECATE_EOF is not enabled, one EOF follows col defs
                log::debug!("receive eof packet after column definitions: {:?}", eof);
                Ok(false)
            }
            (_, resp) => {
                log::debug!("receive unexpected packet: {:?}", resp);
                Err(Error::PacketError(format!(
                    "receive unexpected packet: {:?}",
                    resp
                )))
            }
        }
    }
}

impl<'s, S> Future for QueryResultSetFuture<'s, S>
where
    S: AsyncRead + AsyncWrite + Clone + Unpin,
{
    type Output = Result<QueryResultSet<'s, S>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.qry.has_remaining() {
            let qry = self.qry.clone();
            let mut send_fut = self.conn.send_msg(qry, true);
            match ready!(Pin::new(&mut send_fut).poll(cx)) {
                Ok(_) => {
                    self.qry.clear();
                }
                Err(e) => return Poll::Ready(Err(e.into())),
            }
        }

        loop {
            // let Self { conn, .. } = &mut *self;
            let mut recv_fut = self.conn.recv_msg();
            match ready!(Pin::new(&mut recv_fut).as_mut().poll(cx)) {
                Err(e) => return Poll::Ready(Err(e.into())),
                Ok(msg) => match self.on_msg(msg) {
                    Err(e) => return Poll::Ready(Err(e.into())),
                    Ok(cont) => {
                        if !cont {
                            let col_defs = std::mem::replace(&mut self.col_defs, vec![]);
                            let col_defs = Arc::new(col_defs);
                            let sm = self.sm.clone();
                            return Poll::Ready(Ok(QueryResultSet {
                                conn: self.conn.clone(),
                                sm,
                                col_defs,
                                _marker: PhantomData,
                            }));
                        }
                    }
                },
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::conn::{Conn, ConnOpts};
    use bigdecimal::BigDecimal;
    use chrono::{NaiveDate, NaiveDateTime};
    use futures::stream::StreamExt;
    use mybin_core::resultset::{MyBit, MyBytes, MyI24, MyString, MyTime, MyU24, MyYear};

    #[smol_potat::test]
    async fn test_query_set() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.query()
            .exec("set @master_binlog_checksum = @@global.binlog_checksum")
            .await
            .unwrap();
    }

    #[smol_potat::test]
    async fn test_query_exec_error() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let fail = conn.query().exec("drop table not_exist_table").await;
        assert!(fail.is_err());
    }

    #[smol_potat::test]
    async fn test_query_select_1() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let mut rs = conn
            .query()
            .qry("select 1, current_timestamp()")
            .await
            .unwrap();
        dbg!(&rs.col_defs);
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_query_select_null() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let mut rs = conn.query().qry("select null").await.unwrap();
        dbg!(&rs.col_defs);
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_query_select_variable() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        // select variable will return type LongBlob
        let mut rs = conn
            .query()
            .qry("select @master_binlog_checksum")
            .await
            .unwrap();
        dbg!(&rs.col_defs);
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_query_select_error() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let fail = conn.query().qry("select * from not_exist_table").await;
        dbg!(fail.unwrap_err());
    }

    #[smol_potat::test]
    async fn test_query_table_and_column() {
        // env_logger::init();
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        // create database
        conn.query()
            .exec(
                r#"
        CREATE DATABASE IF NOT EXISTS bintest1 DEFAULT CHARACTER SET utf8
        "#,
            )
            .await
            .unwrap();
        // drop table if exists
        conn.query()
            .exec("DROP TABLE IF EXISTS bintest1.typetest")
            .await
            .unwrap();
        // create table
        conn.query()
            .exec(
                r#"
        CREATE TABLE bintest1.typetest (
            c1 DECIMAL,
            c2 TINYINT,
            c3 TINYINT UNSIGNED,
            c4 SMALLINT,
            c5 SMALLINT UNSIGNED,
            c6 INT,
            c7 INT UNSIGNED,
            c8 FLOAT,
            c9 FLOAT UNSIGNED,
            c10 DOUBLE,
            c11 DOUBLE UNSIGNED,
            c12 TIMESTAMP,
            c13 BIGINT,
            c14 BIGINT UNSIGNED,
            c15 MEDIUMINT,
            c16 MEDIUMINT UNSIGNED,
            c17 DATE,
            c18 TIME,
            c19 DATETIME(6),
            c20 YEAR,
            c21 VARCHAR(50),
            c22 VARCHAR(120) CHARACTER SET binary,
            c23 BIT(16),
            c24 DECIMAL(18, 4),
            c25 TINYBLOB,
            c26 MEDIUMBLOB,
            c27 LONGBLOB,
            c28 BLOB,
            c29 TEXT CHARACTER SET latin1 COLLATE latin1_general_cs,
            c30 TEXT CHARACTER SET utf8,
            c31 TEXT BINARY,
            c32 BOOLEAN
        )
        "#,
            )
            .await
            .unwrap();
        // insert data
        conn.query()
            .exec(
                r#"
        INSERT INTO bintest1.typetest VALUES (
            -100.0,
            -5,
            18,
            -4892,
            32003,
            -159684321,
            2003495865,
            -0.5,
            1.5,
            -0.625,
            1.625,
            '2020-01-01 01:02:03',
            -12948340587434,
            9348578923762,
            -90034,
            87226,
            '2020-12-31',
            '-212:30:40',
            '2012-06-07 15:38:46.092000',
            2021,
            'hello, world', 
            'hello, java',
            b'1000110001100001',
            123456789.22,
            _binary 'hello, tinyblob',
            _binary 'hello, mediumblob',
            _binary 'hello, longblob',
            _binary 'hello, blob',
            'hello, latin1',
            'hello, utf8',
            'hello, binary',
            true
        )
        "#,
            )
            .await
            .unwrap();
        // select data
        let mut rs = conn
            .query()
            .qry("SELECT * from bintest1.typetest")
            .await
            .unwrap();
        let extractor = rs.extractor();
        while let Some(row) = rs.next().await {
            dbg!(&row);
            let c1: BigDecimal = extractor.get_named_col(&row, "c1").unwrap().unwrap();
            dbg!(c1);
            let c2: i8 = extractor.get_named_col(&row, "c2").unwrap().unwrap();
            dbg!(c2);
            let c3: u8 = extractor.get_named_col(&row, "c3").unwrap().unwrap();
            dbg!(c3);
            let c4: i16 = extractor.get_named_col(&row, "c4").unwrap().unwrap();
            dbg!(c4);
            let c5: u16 = extractor.get_named_col(&row, "c5").unwrap().unwrap();
            dbg!(c5);
            let c6: i32 = extractor.get_named_col(&row, "c6").unwrap().unwrap();
            dbg!(c6);
            let c7: u32 = extractor.get_named_col(&row, "c7").unwrap().unwrap();
            dbg!(c7);
            let c8: f32 = extractor.get_named_col(&row, "c8").unwrap().unwrap();
            dbg!(c8);
            let c9: f32 = extractor.get_named_col(&row, "c9").unwrap().unwrap();
            dbg!(c9);
            let c10: f64 = extractor.get_named_col(&row, "c10").unwrap().unwrap();
            dbg!(c10);
            let c11: f64 = extractor.get_named_col(&row, "c11").unwrap().unwrap();
            dbg!(c11);
            let c12: NaiveDateTime = extractor.get_named_col(&row, "c12").unwrap().unwrap();
            dbg!(c12);
            let c13: i64 = extractor.get_named_col(&row, "c13").unwrap().unwrap();
            dbg!(c13);
            let c14: u64 = extractor.get_named_col(&row, "c14").unwrap().unwrap();
            dbg!(c14);
            let c15: MyI24 = extractor.get_named_col(&row, "c15").unwrap().unwrap();
            dbg!(c15);
            let c16: MyU24 = extractor.get_named_col(&row, "c16").unwrap().unwrap();
            dbg!(c16);
            let c17: NaiveDate = extractor.get_named_col(&row, "c17").unwrap().unwrap();
            dbg!(c17);
            let c18: MyTime = extractor.get_named_col(&row, "c18").unwrap().unwrap();
            dbg!(c18);
            let c19: NaiveDateTime = extractor.get_named_col(&row, "c19").unwrap().unwrap();
            dbg!(c19);
            let c20: MyYear = extractor.get_named_col(&row, "c20").unwrap().unwrap();
            dbg!(c20);
            let c21: MyString = extractor.get_named_col(&row, "c21").unwrap().unwrap();
            dbg!(c21);
            let c22: MyString = extractor.get_named_col(&row, "c22").unwrap().unwrap();
            dbg!(c22);
            let c23: MyBit = extractor.get_named_col(&row, "c23").unwrap().unwrap();
            dbg!(c23);
            let c24: BigDecimal = extractor.get_named_col(&row, "c24").unwrap().unwrap();
            dbg!(c24);
            let c25: MyBytes = extractor.get_named_col(&row, "c25").unwrap().unwrap();
            dbg!(c25);
            let c26: MyBytes = extractor.get_named_col(&row, "c26").unwrap().unwrap();
            dbg!(c26);
            let c27: MyBytes = extractor.get_named_col(&row, "c27").unwrap().unwrap();
            dbg!(c27);
            let c28: MyBytes = extractor.get_named_col(&row, "c28").unwrap().unwrap();
            dbg!(c28);
            let c29: MyString = extractor.get_named_col(&row, "c29").unwrap().unwrap();
            dbg!(c29);
            let c30: MyString = extractor.get_named_col(&row, "c30").unwrap().unwrap();
            dbg!(c30);
            let c31: MyString = extractor.get_named_col(&row, "c31").unwrap().unwrap();
            dbg!(c31);
            let c32: bool = extractor.get_named_col(&row, "c32").unwrap().unwrap();
            dbg!(c32);
        }
    }

    #[smol_potat::test]
    async fn test_result_set_with_extractor() {
        use chrono::NaiveDateTime;

        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let mut rs = conn
            .query()
            .qry("select 1 as id, current_timestamp(6) as ts")
            .await
            .unwrap();
        let extractor = rs.extractor();
        while let Some(row) = rs.next().await {
            dbg!(&row);
            let id: u32 = extractor.get_col(&row, 0).unwrap().unwrap();
            dbg!(id);
            let named_id: u32 = extractor.get_named_col(&row, "id").unwrap().unwrap();
            dbg!(named_id);
            let ts: NaiveDateTime = extractor.get_col(&row, 1).unwrap().unwrap();
            dbg!(ts);
            let named_ts: NaiveDateTime = extractor.get_named_col(&row, "ts").unwrap().unwrap();
            dbg!(named_ts);
        }
    }

    #[smol_potat::test]
    async fn test_result_set_with_mapper() {
        use bytes::Buf;
        use mybin_core::col::TextColumnValue;
        use mybin_core::resultset::ResultSetColExtractor;
        #[derive(Debug)]
        struct IdAndName {
            id: u32,
            name: String,
        }

        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let rs = conn
            .query()
            .qry("select 1 as id, 'hello' as name")
            .await
            .unwrap();
        let mut rs = rs.map_rows(
            |extractor: &ResultSetColExtractor, row: Vec<TextColumnValue>| {
                let id: u32 = extractor.get_named_col(&row, "id").unwrap().unwrap();
                let name: MyString = extractor.get_named_col(&row, "name").unwrap().unwrap();
                let name = String::from_utf8(Vec::from(name.0.bytes())).unwrap();
                IdAndName { id, name }
            },
        );
        while let Some(obj) = rs.next().await {
            dbg!(obj);
        }
    }

    fn conn_opts() -> ConnOpts {
        ConnOpts {
            username: "root".to_owned(),
            password: "password".to_owned(),
            database: "".to_owned(),
        }
    }
}
