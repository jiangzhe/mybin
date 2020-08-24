use crate::conn::Conn;
use futures::{AsyncRead, AsyncWrite};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct BinlogFiles<'c, S> {
    conn: &'c mut Conn<S>,
}

impl<'c, S> Future for BinlogFiles<'c, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Vec<BinlogFile>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

#[derive(Debug, Clone)]
pub struct BinlogFile {
    pub filename: String,
    pub pos: u64,
}
