use crate::auth_plugin::{AuthPlugin, MysqlNativePassword};
use crate::bytes::AsyncReadBytes;
use crate::error::{Error, Result};
use crate::number::{AsyncReadNumber, AsyncWriteNumber};
use crate::recv_msg::{RecvMsg, RecvMsgFuture};
use crate::binlog_stream::BinlogStream;
use async_net::TcpStream;
use bytes_parser::{ReadFrom, ReadWithContext, WriteTo};
use mybin_core::flag::{CapabilityFlags, StatusFlags};
use mybin_core::handshake::{HandshakeClientResponse41, InitialHandshake};
use mybin_core::packet::HandshakeMessage;
use serde_derive::*;
use smol::io::AsyncWriteExt;
use std::net::{SocketAddr, ToSocketAddrs};


#[derive(Debug)]
pub struct Conn {
    socket_addr: SocketAddr,
    pub(crate) stream: TcpStream,
    cap_flags: CapabilityFlags,
    pkt_nr: u8,
    server_status: StatusFlags,
    read_buf: Vec<u8>,
    write_buf: Vec<u8>,
}

#[allow(dead_code)]
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
        Ok(Conn {
            socket_addr,
            stream,
            cap_flags: CapabilityFlags::empty(),
            pkt_nr: 0,
            server_status: StatusFlags::empty(),
            read_buf: Vec::with_capacity(1024 * 8),
            write_buf: Vec::with_capacity(1024 * 8),
        })
    }

    /// process the initial handshake with MySQL server,
    /// should be called before any other commands
    /// this method will change the connect capability flags
    pub async fn handshake(&mut self, opts: ConnOpts) -> Result<()> {
        let msg = self.recv_msg().await?;
        let (_, handshake): (_, InitialHandshake) = msg.read_from(0)?;
        log::debug!(
            "protocol version: {}, server version: {}, connection_id: {}",
            handshake.protocol_version,
            String::from_utf8_lossy(handshake.server_version),
            handshake.connection_id,
        );
        log::debug!("auth_plugin={}, auth_data_1={:?}, auth_data_2={:?}", 
            String::from_utf8_lossy(handshake.auth_plugin_name), 
            handshake.auth_plugin_data_1, handshake.auth_plugin_data_2);
        let mut seed = vec![];
        seed.extend(handshake.auth_plugin_data_1);
        seed.extend(handshake.auth_plugin_data_2);

        self.cap_flags.insert(CapabilityFlags::PLUGIN_AUTH);
        self.cap_flags.insert(CapabilityFlags::LONG_PASSWORD);
        self.cap_flags.insert(CapabilityFlags::PROTOCOL_41);
        self.cap_flags.insert(CapabilityFlags::TRANSACTIONS);
        self.cap_flags.insert(CapabilityFlags::MULTI_RESULTS);
        self.cap_flags.insert(CapabilityFlags::SECURE_CONNECTION);
        self.cap_flags
            .insert(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA);
        CapabilityFlags::default();
        // disable ssl currently
        self.cap_flags.remove(CapabilityFlags::SSL);
        // by default use mysql_native_password auth_plugin
        let (auth_plugin_name, auth_response) = 
            generate_auth_response(&opts.username, &opts.password, &seed)?;

        if !opts.database.is_empty() {
            self.cap_flags.insert(CapabilityFlags::CONNECT_WITH_DB);
        }

        let client_resp = HandshakeClientResponse41 {
            capability_flags: self.cap_flags.clone(),
            username: opts.username,
            auth_response,
            database: opts.database,
            auth_plugin_name,
            ..Default::default()
        };
        self.send_msg(client_resp).await?;
        let cap_flags = self.cap_flags.clone();
        let msg = self.recv_msg().await?;
        // todo: handle auth switch request
        match msg.read_with_ctx(0, &cap_flags)?.1 {
            HandshakeMessage::Ok(ok) => {
                log::debug!("handshake succeeds");
                self.server_status = ok.status_flags;
                // reset packet number for command phase
                self.reset_pkt_nr();
            }
            HandshakeMessage::Err(err) => {
                return Err(Error::PacketError(format!(
                    "error_code: {}, error_message: {}",
                    err.error_code,
                    String::from_utf8_lossy(err.error_message)
                )))
            }
            HandshakeMessage::Switch(switch) => {
                log::debug!(
                    "switch auth_plugin={}, auth_data={:?}",
                    String::from_utf8_lossy(switch.plugin_name),
                    switch.auth_plugin_data
                );
                unimplemented!();
            }
        }
        Ok(())
    }

    /// receive message from MySQL server
    ///
    /// this method will concat mutliple packets if payload too large.
    pub async fn recv_msg(&mut self) -> Result<&[u8]> {
        // let mut out = Vec::new();
        self.read_buf.clear();
        loop {
            let payload_len = self.stream.read_le_u24().await?;
            let seq_id = self.stream.read_u8().await?;
            log::debug!(
                "receive packet: payload_len={}, seq_id={}",
                payload_len,
                seq_id
            );
            if seq_id != self.pkt_nr {
                return Err(Error::PacketError(format!(
                    "Get server packet out of order: {} != {}",
                    seq_id, self.pkt_nr
                )));
            }
            self.pkt_nr += 1;
            self.stream
                .take_out(payload_len as usize, &mut self.read_buf)
                .await?;
            // read multiple packets if payload larger than or equal to 2^24-1
            // https://dev.mysql.com/doc/internals/en/sending-more-than-16mbyte.html
            if payload_len < 0xffffff {
                break;
            }
        }
        Ok(&self.read_buf)
    }

    /// send message to MySQL server
    ///
    /// this method will split message into multiple packets if payload too large.
    pub async fn send_msg<'a, T>(&mut self, msg: T) -> Result<()>
    where
        Vec<u8>: WriteTo<'a, T>,
        T: 'a,
    {
        self.write_buf.clear();
        self.write_buf.write_to(msg)?;
        let mut chunk_size = 0;
        // let mut seq_id = 0;
        for chunk in self.write_buf.chunks(0xffffff) {
            chunk_size = chunk.len();
            self.stream.write_le_u24(chunk_size as u32).await?;
            self.stream.write_u8(self.pkt_nr).await?;
            self.stream.write_all(chunk).await?;
            // seq_id += 1;
            self.pkt_nr += 1;
        }
        if chunk_size == 0xffffff {
            // send empty chunk to confirm the end
            self.stream.write_le_u24(0).await?;
            self.stream.write_u8(self.pkt_nr).await?;
            self.pkt_nr += 1;
        }
        Ok(())
    }

    /// consume the connection and return the binlog stream
    pub async fn request_binlog_stream(mut self) -> Result<BinlogStream> {
        todo!()
    }

    /// reset packet number to 0
    ///
    /// this method should be called before each command sent
    pub fn reset_pkt_nr(&mut self) {
        self.pkt_nr = 0;
    }
}

fn generate_auth_response(username: &str, password: &str, seed: &[u8]) -> Result<(String, Vec<u8>)> {
    let mut seed = seed;
    if let Some(0x00) = seed.last() {
        // remove trailing 0x00 byte
        seed = &seed[..seed.len()-1];
    }
    let mut ap = MysqlNativePassword::new();
    ap.set_credential(username, password);
    let mut auth_response = vec![];
    ap.next(&seed, &mut auth_response)?;
    Ok(("mysql_native_password".to_owned(), auth_response))
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnOpts {
    pub username: String,
    pub password: String,
    pub database: String,
}

#[cfg(test)]
mod tests {
    // use super::*;

    // #[smol_potat::test]
    // async fn test_real_conn() {
    //     let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
    //     conn.handshake(ConnOpts{
    //         username: "root".to_owned(),
    //         password: "password".to_owned(),
    //         database: "".to_owned(),
    //     }).await.unwrap();
    // }
}
