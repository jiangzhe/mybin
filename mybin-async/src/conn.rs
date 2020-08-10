use async_net::TcpStream;
use serde_derive::*;
use std::net::{SocketAddr, ToSocketAddrs};
use crate::error::{Result, Error};
use mybin_parser::handshake::{InitialHandshake, initial_handshake};
use crate::number::{AsyncReadNumber, AsyncWriteNumber};
use crate::bytes::AsyncReadBytes;
use smol::io::AsyncWriteExt;

#[derive(Debug)]
pub struct Conn {
    socket_addr: SocketAddr,
    stream: TcpStream,
    buf: Vec<u8>,
}

impl Conn {
    /// create a new connection to MySQL
    /// 
    /// this method only make the initial connection to MySQL server,
    /// user has to finish the handshake manually
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Conn> {
        // maybe try all addresses?
        let socket_addr = match addr.to_socket_addrs()?.next() {
            Some(addr) => addr,
            None => return Err(Error::AddrNotFound),
        };
        let stream = TcpStream::connect(socket_addr).await?;
        log::debug!("connected to MySQL: {}", socket_addr);
        Ok(Conn{socket_addr, stream, buf: Vec::new()})
    }

    pub async fn connect_handshake<T: ToSocketAddrs>(
        addr: T, 
        opts: ConnOpts,
    ) -> Result<Conn> {
        let mut conn = Self::connect(addr).await?;
        let msg = conn.recv_msg().await?;
        let handshake = initial_handshake(msg)?;
        log::debug!("protocol version: {}, server version: {}, connection_id: {}", 
            handshake.protocol_version, 
            String::from_utf8_lossy(handshake.server_version),
            handshake.connection_id);
        
        

        Ok(conn)
    }

    /// receive message from MySQL server
    /// 
    /// this method will concat mutliple packets if payload too large.
    pub async fn recv_msg(&mut self) -> Result<&[u8]> {
        self.reset_buf();
        loop {
            let payload_len = self.stream.read_le_u24().await?;
            let _seq_id = self.stream.read_u8().await?;
            self.stream.take_out(payload_len as usize, &mut self.buf).await?;
            // read multiple packets if payload larger than or equal to 2^24-1
            // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
            if payload_len < 0xffffff {
                break;
            }
        }
        Ok(&self.buf)
    }

    /// send message to MySQL server
    /// 
    /// this method will split message into multiple packets if payload too large.
    pub async fn send_msg(&mut self, msg: &[u8]) -> Result<()> {
        let mut chunk_size = 0;
        let mut seq_id = 0;
        for chunk in msg.chunks(0xffffff) {
            chunk_size = chunk.len();
            self.stream.write_le_u24(chunk_size as u32).await?;
            self.stream.write_u8(seq_id).await?;
            self.stream.write_all(chunk).await?;
            seq_id += 1;
        }
        if chunk_size == 0xffffff {
            // send empty chunk to confirm the end
            self.stream.write_le_u24(0).await?;
            self.stream.write_u8(seq_id).await?;
        }
        Ok(())
    } 

    fn reset_buf(&mut self) {
        self.buf.clear();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnOpts {
    pub username: String,
    pub password: String,
    pub database: Option<String>,
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_sha1() {
        use crypto::digest::Digest;
        use crypto::sha1::Sha1;
        let mut hasher = Sha1::new();
        hasher.input_str("hello");
        let hex = hasher.result_str();
        println!("{}", hex);
    }
}