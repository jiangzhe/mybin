use crate::Command;
use bytes::Bytes;

// todo: long data sending currently not supported
#[derive(Debug, Clone)]
pub struct ComStmtSendLongData {
    pub cmd: Command,
    pub stmt_id: u32,
    pub param_id: u16,
    pub data: Bytes,
}
