use smol::stream::Stream;
use smol::ready;
use std::pin::Pin;
use std::future::Future;
use std::task::{Context, Poll};
use crate::conn::Conn;
use crate::error::Result;

/// wrapper of Conn instance to provide readable 
/// stream of binlog
pub struct BinlogStream {
    conn: Conn,
    end: bool,
}

impl BinlogStream {
    pub async fn get_back_conn(self) -> Result<Conn> {
        todo!()
    }
}

impl Stream for BinlogStream {
    type Item = Result<Vec<u8>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        todo!()
    }
}
