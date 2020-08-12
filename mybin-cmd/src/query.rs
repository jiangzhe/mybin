use serde_derive::*;
use crate::Command;
use mybin_parser::packet::{OkPacket, ErrPacket};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComQuery {
    pub cmd: Command,
    pub query: String,
}

impl ComQuery {

    pub fn new<S: Into<String>>(query: S) -> Self {
        ComQuery{
            cmd: Command::Query,
            query: query.into(),
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bs = vec![];
        bs.push(self.cmd.to_byte());
        bs.extend(self.query.as_bytes());
        bs
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ComQueryResponse<'a> {
    Ok(#[serde(borrow)] OkPacket<'a>),
    Err(#[serde(borrow)] ErrPacket<'a>),
}
