use crate::conn::Conn;
use crate::error::{Error, Result, SqlError};
use bytes::Buf;
use mybin_core::query::{ComQuery, ComQueryResponse, ComQueryStateMachine};
use mybin_core::resultset::TextRow;
use pin_project::pin_project;
use smol::future::Future;
use smol::ready;
use smol::stream::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct Query<'a> {
    conn: &'a mut Conn,
    sm: ComQueryStateMachine,
}

impl<'a> Query<'a> {
    pub fn new(conn: &'a mut Conn) -> Self {
        let cap_flags = conn.cap_flags.clone();
        Query {
            conn,
            sm: ComQueryStateMachine::new(cap_flags),
        }
    }

    pub async fn exec<S: Into<String>>(mut self, qry: S) -> Result<()> {
        let qry = ComQuery::new(qry);
        let mut sm = ComQueryStateMachine::new(self.conn.cap_flags.clone());
        self.conn.send_msg(qry).await?;
        loop {
            let msg = self.conn.recv_msg().await?;
            log::debug!("{:?}", msg);
            let resp = sm.next(msg)?;
            match resp {
                ComQueryResponse::Err(err) => {
                    return Err(Error::SqlError(SqlError {
                        error_code: err.error_code,
                        sql_state_marker: err.sql_state_marker,
                        sql_state: String::from_utf8(Vec::from(err.sql_state.bytes()))?,
                        error_message: String::from_utf8(Vec::from(err.error_message.bytes()))?,
                    }));
                }
                ComQueryResponse::Ok(_) => {
                    // currently ignore status_flags, session_state_changes
                    return Ok(());
                }
                ComQueryResponse::Eof(_) => {
                    // currently ignore status_flags
                    return Ok(());
                }
                _ => (),
            }
        }
    }

    pub async fn query<'b, S: Into<String>>(
        mut self,
        qry: S,
        buf: Vec<u8>,
    ) -> Result<QueryResultSet<'a>> {
        let qry = ComQuery::new(qry);
        let mut sm = ComQueryStateMachine::new(self.conn.cap_flags.clone());
        self.conn.send_msg(qry).await?;
        let msg = self.conn.recv_msg().await?;
        log::debug!("{:?}", msg);
        match sm.next(msg)? {
            ComQueryResponse::Err(err) => {
                return Err(Error::SqlError(SqlError {
                    error_code: err.error_code,
                    sql_state_marker: err.sql_state_marker,
                    sql_state: String::from_utf8(Vec::from(err.sql_state.bytes()))?,
                    error_message: String::from_utf8(Vec::from(err.error_message.bytes()))?,
                }));
            }
            // ComQueryResponse::Ok(_) => {
            //     return Ok(QueryResultSet{
            //         conn: self.conn,
            //         sm: self.sm,
            //         buf,
            //         // column cnt will be ignored
            //         col_cnt: 0,
            //     });
            // }
            _ => todo!(),
        }
    }
}

/// async result set
pub struct QueryResultSet<'c> {
    conn: &'c mut Conn,
    sm: ComQueryStateMachine,
    // buf: &'b mut Vec<u8>,
    col_cnt: usize,
}

// impl<'c, 'b> QueryResultSet<'c, 'b> {
//     fn conn_and_buf(&mut self) -> (&'c mut Conn, &'b mut Vec<u8>) {
//         (self.conn, self.buf)
//     }
// }

impl<'c> Stream for QueryResultSet<'c> {
    type Item = TextRow;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.sm.end() {
            return Poll::Ready(None);
        }
        todo!()
        // let conn = self.conn;
        // let sm = self.sm;
        // let col_cnt = self.col_cnt;

        // self.buf.clear();
        // let recv_fut = &mut conn.recv_msg_fut(self.buf);
        // let mut recv_fut = Pin::new(recv_fut);
        // match ready!(recv_fut.as_mut().poll(cx)) {
        //     Ok(_) => {
        //         let buf = &self.buf;
        //         match buf.read_with_ctx(0, col_cnt) {
        //             Err(err) => {
        //                 log::debug!("parse row error: {:?}", err);
        //                 return Poll::Ready(None);
        //             }
        //             Ok((_, row)) => {
        //                 log::debug!("parsed row: {:?}", row);
        //                 return Poll::Ready(Some(row));
        //             }
        //         }
        //     }
        //     Err(err) => {
        //         log::debug!("parse message error: {:?}", err);
        //         return Poll::Ready(None);
        //     }
        // }
    }
}
