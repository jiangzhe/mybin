use crate::conn::Conn;
use crate::error::{Error, Result, SqlError};
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::WriteToBytes;
use futures::stream::Stream;
use futures::{ready, AsyncRead, AsyncWrite};
use mybin_core::col::ColumnDefinition;
use mybin_core::query::{ComQuery, ComQueryResponse, ComQueryState, ComQueryStateMachine};
use mybin_core::resultset::TextRow;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::marker::PhantomData;

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
    pub col_defs: Vec<ColumnDefinition>,
    // use PhantomData to prevent concurrent 
    // usage on same connection
    _marker: PhantomData<&'s S>,
}

impl<'s, S: 's> Stream for QueryResultSet<'s, S>
where
    S: AsyncRead + Unpin,
{
    type Item = TextRow;

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
                Ok((_, ComQueryResponse::Row(row))) => Poll::Ready(Some(row)),
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
                Err(e) => {
                    return Poll::Ready(Err(e.into()))
                }
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
    use futures::stream::StreamExt;

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
    async fn test_query_select_1() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let mut rs = conn
            .query()
            .qry("select 1, current_timestamp()")
            .await
            .unwrap();
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_query_select_null() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let mut rs = conn.query().qry("select null").await.unwrap();
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
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_query_table_and_column() {
        env_logger::init();
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        // create database
        conn.query().exec(r#"
        CREATE DATABASE IF NOT EXISTS bintest1 DEFAULT CHARACTER SET utf8
        "#).await.unwrap();
        // create table
        conn.query().exec(r#"
        CREATE TABLE IF NOT EXISTS bintest1.typetest (
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
            c31 TEXT BINARY
        )
        "#).await.unwrap();
        // truncate table
        conn.query().exec("TRUNCATE TABLE bintest1.typetest").await.unwrap();
        // insert data
        conn.query().exec(r#"
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
            '12:30:40',
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
            'hello, binary'
        )
        "#).await.unwrap();
        // select data
        let mut rs = conn.query().qry("SELECT * from bintest1.typetest").await.unwrap();
        while let Some(row) = rs.next().await {
            dbg!(row);
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
