use crate::conn::Conn;
use crate::error::Result;
use bytes::{Buf, Bytes};
use futures::{AsyncRead, AsyncWrite, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct BinlogFiles<'c, S> {
    conn: &'c mut Conn<S>,
}

impl<'c, S> BinlogFiles<'c, S>
where
    S: AsyncRead + AsyncWrite + Clone + Unpin,
{
    pub async fn list(&mut self) -> Result<Vec<BinlogFile>> {
        let mut rs = self.conn.query().qry("SHOW MASTER LOGS").await?;
        let mut files = vec![];
        while let Some(row) = rs.next().await {}
        Ok(files)
    }
}

#[derive(Debug, Clone)]
pub struct BinlogFile {
    pub filename: String,
    pub pos: u64,
}
