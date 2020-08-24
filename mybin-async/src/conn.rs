use crate::auth_plugin::{AuthPlugin, MysqlNativePassword};
use crate::error::{Error, Result};
use crate::msg::{RecvMsgFuture, SendMsgFuture};
use crate::query::Query;
use async_net::TcpStream;
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::{ReadFromBytes, ReadFromBytesWithContext, WriteToBytes};
use mybin_core::flag::{CapabilityFlags, StatusFlags};
use mybin_core::handshake::{HandshakeClientResponse41, InitialHandshake};
use mybin_core::packet::HandshakeMessage;
use serde_derive::*;
// use smol::io::AsyncWriteExt;
use futures::{AsyncRead, AsyncWrite};
use std::net::ToSocketAddrs;

/// MySQL connection
///
/// A generic MySQL connection based on AsyncRead and AsyncWrite.
#[derive(Debug, Clone)]
pub struct Conn<S> {
    pub(crate) stream: S,
    pub(crate) cap_flags: CapabilityFlags,
    pub(crate) server_status: StatusFlags,
    pub(crate) pkt_nr: u8,
}

impl<S> Conn<S> {
    /// reset packet number to 0
    ///
    /// this method should be called before each command sent
    pub fn reset_pkt_nr(&mut self) {
        self.pkt_nr = 0;
    }
}

#[allow(dead_code)]
impl Conn<TcpStream> {
    /// create a new connection to MySQL
    ///
    /// this method only make the initial connection to MySQL server,
    /// user has to finish the handshake manually
    pub async fn connect<T: ToSocketAddrs>(addr: T) -> Result<Self> {
        // maybe try all addresses?
        let socket_addr = match addr.to_socket_addrs()?.next() {
            Some(addr) => addr,
            None => return Err(Error::AddrNotFound),
        };
        let stream = TcpStream::connect(socket_addr).await?;
        log::debug!("connected to MySQL: {}", socket_addr);
        Ok(Conn {
            stream,
            cap_flags: CapabilityFlags::empty(),
            pkt_nr: 0,
            server_status: StatusFlags::empty(),
        })
    }
}

impl<S> Conn<S>
where
    S: AsyncRead + Unpin,
{
    /// receive message from MySQL server
    ///
    /// this method will concat mutliple packets if payload too large.
    pub fn recv_msg<'s>(&'s mut self) -> RecvMsgFuture<'s, S> {
        RecvMsgFuture::new(self)
    }
}

impl<S> Conn<S>
where
    S: AsyncWrite + Unpin,
{
    /// send message to MySQL server
    ///
    /// this method will split message into multiple packets if payload too large.
    pub fn send_msg<'a, T>(&'a mut self, msg: T) -> SendMsgFuture<'a, S>
    where
        T: WriteToBytes,
    {
        SendMsgFuture::new(self, msg)
    }
}

#[allow(dead_code)]
impl<S> Conn<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// create new connection given an already connected stream and client/server status
    pub fn new(stream: S, cap_flags: CapabilityFlags, server_status: StatusFlags) -> Self {
        Conn {
            stream,
            cap_flags,
            server_status,
            pkt_nr: 0,
        }
    }

    /// process the initial handshake with MySQL server,
    /// should be called before any other commands
    /// this method will change the connect capability flags
    pub async fn handshake(&mut self, opts: ConnOpts) -> Result<()> {
        let mut msg = self.recv_msg().await?;
        let handshake = InitialHandshake::read_from(&mut msg)?;
        log::debug!(
            "protocol version: {}, server version: {}, connection_id: {}",
            handshake.protocol_version,
            String::from_utf8_lossy(handshake.server_version.bytes()),
            handshake.connection_id,
        );
        log::debug!(
            "auth_plugin={}, auth_data_1={:?}, auth_data_2={:?}",
            String::from_utf8_lossy(handshake.auth_plugin_name.bytes()),
            handshake.auth_plugin_data_1,
            handshake.auth_plugin_data_2
        );
        let mut seed = vec![];
        seed.extend(handshake.auth_plugin_data_1);
        seed.extend(handshake.auth_plugin_data_2);

        self.cap_flags.insert(CapabilityFlags::PLUGIN_AUTH);
        self.cap_flags.insert(CapabilityFlags::LONG_PASSWORD);
        self.cap_flags.insert(CapabilityFlags::PROTOCOL_41);
        self.cap_flags.insert(CapabilityFlags::TRANSACTIONS);
        self.cap_flags.insert(CapabilityFlags::MULTI_RESULTS);
        self.cap_flags.insert(CapabilityFlags::SECURE_CONNECTION);
        // deprecate EOF to allow server send OK packet instead of EOF packet
        self.cap_flags.insert(CapabilityFlags::DEPRECATE_EOF);
        self.cap_flags
            .insert(CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA);
        CapabilityFlags::default();
        // disable ssl currently
        self.cap_flags.remove(CapabilityFlags::SSL);
        // by default use mysql_native_password auth_plugin
        let (auth_plugin_name, auth_response) =
            gen_auth_resp(&opts.username, &opts.password, &seed)?;

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
        let mut msg = self.recv_msg().await?;
        // todo: handle auth switch request
        match HandshakeMessage::read_with_ctx(&mut msg, &cap_flags)? {
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
                    String::from_utf8_lossy(err.error_message.bytes()),
                )))
            }
            HandshakeMessage::Switch(switch) => {
                log::debug!(
                    "switch auth_plugin={}, auth_data={:?}",
                    String::from_utf8_lossy(switch.plugin_name.bytes()),
                    switch.auth_plugin_data
                );
                unimplemented!();
            }
        }
        Ok(())
    }
}

impl<S> Conn<S>
where
    S: AsyncRead + AsyncWrite + Clone + Unpin,
{
    pub fn query(&mut self) -> Query<S> {
        Query::new(self)
    }
}

fn gen_auth_resp(username: &str, password: &str, seed: &[u8]) -> Result<(String, Vec<u8>)> {
    let mut seed = seed;
    if let Some(0x00) = seed.last() {
        // remove trailing 0x00 byte
        seed = &seed[..seed.len() - 1];
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
    use super::*;

    #[smol_potat::test]
    async fn test_conn_and_handshake() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
    }

    fn conn_opts() -> ConnOpts {
        ConnOpts {
            username: "root".to_owned(),
            password: "password".to_owned(),
            database: "".to_owned(),
        }
    }
}
