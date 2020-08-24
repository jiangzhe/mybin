use crate::conn::Conn;
use crate::error::{Error, Result};
use futures::{AsyncRead, Stream};
// use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum BinlogStreamState {
    Prepare,
    Query,
    Receive,
}



/// wrapper of Conn instance to provide readable
/// stream of binlog
#[derive(Debug)]
pub struct BinlogStream<S> {
    conn: Conn<S>,
    end: bool,
}



impl<S> BinlogStream<S> {
    pub async fn into_conn(self) -> Result<Conn<S>> {
        if self.end {
            return Ok(self.conn);
        }
        Err(Error::BinlogStreamNotEnded)
    }
}

impl<S> BinlogStream<S>
where
    S: AsyncRead + Unpin,
{
    pub fn new(conn: Conn<S>) -> Self {
        BinlogStream { conn, end: false }
    }
}

impl<S> Stream for BinlogStream<S>
where
    S: AsyncRead + Unpin,
{
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
