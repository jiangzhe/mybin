mod files;
mod stream;

use crate::conn::Conn;
use crate::error::Result;
use files::BinlogFiles;
use futures::{AsyncRead, AsyncWrite};
use mybin_core::cmd::{BinlogDumpGtidFlags, ComBinlogDumpGtid, SidData, SidRange};
use stream::BinlogStream;

/// extends connection with binlog functionalities
pub trait BinlogExt<S> {
    fn binlog_files(&mut self) -> Result<BinlogFiles<S>>;

    /// consume the connection and return the binlog stream
    fn binlog_stream(self, cmd: ComBinlogDumpGtid) -> Result<BinlogStream<S>>;
}

impl<S> BinlogExt<S> for Conn<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn binlog_files(&mut self) -> Result<BinlogFiles<S>> {
        todo!()
    }

    fn binlog_stream(self, cmd: ComBinlogDumpGtid) -> Result<BinlogStream<S>> {
        todo!()
    }
}
