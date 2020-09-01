use crate::auth_plugin::{AuthPlugin, MysqlNativePassword};
use crate::error::{Error, Result};
use crate::msg::{RecvMsgFuture, SendMsgFuture};
use crate::query::{Query, QueryResultSetFuture};
use async_net::TcpStream;
use bytes::{Buf, BytesMut};
use bytes_parser::{
    ReadFromBytes, ReadFromBytesWithContext, WriteToBytes, WriteToBytesWithContext,
};
use futures::{AsyncRead, AsyncWrite};
use mybin_core::cmd::*;
use mybin_core::col::ColumnDefinition;
use mybin_core::flag::{CapabilityFlags, StatusFlags};
use mybin_core::handshake::{ConnectAttr, HandshakeClientResponse41, InitialHandshake};
use mybin_core::packet::{HandshakeMessage, OkPacket};
use mybin_core::quit::ComQuit;
use mybin_core::resp::ComResponse;
use serde_derive::*;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;

/// MySQL connection
///
/// A generic MySQL connection based on AsyncRead and AsyncWrite.
#[derive(Debug, Clone)]
pub struct Conn<S> {
    pub(crate) stream: S,
    pub(crate) cap_flags: CapabilityFlags,
    pub(crate) server_status: StatusFlags,
    pub(crate) pkt_nr: Arc<AtomicU8>,
}

impl<S> Conn<S> {
    /// reset packet number to 0
    ///
    /// this method should be called before each command sent
    pub fn reset_pkt_nr(&self) {
        self.pkt_nr.as_ref().store(0, Ordering::SeqCst);
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
            pkt_nr: Arc::new(AtomicU8::new(0)),
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
    pub fn send_msg<'a, T>(&'a mut self, msg: T, reset_pkt_nr: bool) -> SendMsgFuture<'a, S>
    where
        T: WriteToBytes,
    {
        if reset_pkt_nr {
            self.reset_pkt_nr();
        }
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
            pkt_nr: Arc::new(AtomicU8::new(0)),
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
        self.send_msg(client_resp, false).await?;
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

    /// tells the server that client wants to close the connection
    pub async fn quit(mut self) -> Result<()> {
        // use mybin_core::packet::OkPacket;
        let cmd = ComQuit::new();
        self.send_msg(cmd, true).await?;
        // let mut msg = self.recv_msg().await?;
        // OkPacket::read_with_ctx(&mut msg, &self.cap_flags)?;
        Ok(())
    }

    /// change the default schema of the connection
    pub async fn init_db<T: AsRef<str>>(&mut self, db_name: T) -> Result<()> {
        let cmd = ComInitDB::new(db_name);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComResponse::read_with_ctx(&mut msg, &self.cap_flags)? {
            ComResponse::Ok(_) => Ok(()),
            ComResponse::Err(e) => Err(e.into()),
        }
    }

    /// get column definitions of a table
    pub async fn field_list<T: Into<String>, U: Into<String>>(
        &mut self,
        table: T,
        field_wildcard: U,
    ) -> Result<Vec<ColumnDefinition>> {
        let cmd = ComFieldList::new(table, field_wildcard);
        self.send_msg(cmd, true).await?;
        let mut col_defs = Vec::new();
        loop {
            let mut msg = self.recv_msg().await?;
            match ComFieldListResponse::read_with_ctx(&mut msg, (&self.cap_flags, true))? {
                ComFieldListResponse::Err(err) => return Err(err.into()),
                ComFieldListResponse::Eof(_) => return Ok(col_defs),
                ComFieldListResponse::ColDef(col_def) => col_defs.push(col_def),
            }
        }
    }

    /// already deprecated so not expose as public method
    ///
    /// use COM_QUERY instead
    #[allow(dead_code)]
    async fn create_db<T: Into<String>>(&mut self, db_name: T) -> Result<()> {
        let cmd = ComCreateDB::new(db_name);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComCreateDBResponse::read_with_ctx(&mut msg, &self.cap_flags)? {
            ComCreateDBResponse::Ok(_) => Ok(()),
            ComCreateDBResponse::Err(err) => Err(err.into()),
        }
    }

    /// already deprecated so not expose as public method
    ///
    /// use COM_QUERY instead
    #[allow(dead_code)]
    async fn drop_db<T: Into<String>>(&mut self, db_name: T) -> Result<()> {
        let cmd = ComDropDB::new(db_name);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComDropDBResponse::read_with_ctx(&mut msg, &self.cap_flags)? {
            ComDropDBResponse::Ok(_) => Ok(()),
            ComDropDBResponse::Err(err) => Err(err.into()),
        }
    }

    /// a low-level version of several FLUSH ... and RESET ... statements
    pub async fn refresh(&mut self, sub_cmd: RefreshFlags) -> Result<()> {
        let cmd = ComRefresh::new(sub_cmd);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComRefreshResponse::read_with_ctx(&mut msg, &self.cap_flags)? {
            ComRefreshResponse::Ok(_) => Ok(()),
            ComRefreshResponse::Err(err) => Err(err.into()),
        }
    }

    /// get human readable string of internal statistics
    pub async fn statistics(&mut self) -> Result<String> {
        let cmd = ComStatistics::new();
        self.send_msg(cmd, true).await?;
        let msg = self.recv_msg().await?;
        Ok(String::from_utf8(Vec::from(msg.bytes()))?)
    }

    /// triggers a dump on internal debug info to stdout of the mysql server
    pub async fn debug(&mut self) -> Result<()> {
        let cmd = ComDebug::new();
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComDebugResponse::read_with_ctx(&mut msg, &self.cap_flags)? {
            ComDebugResponse::Eof(_) => Ok(()),
            ComDebugResponse::Err(err) => Err(err.into()),
        }
    }

    /// check if the server is alive
    pub async fn ping(&mut self) -> Result<()> {
        let cmd = ComPing::new();
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        OkPacket::read_with_ctx(&mut msg, &self.cap_flags)?;
        Ok(())
    }

    /// change the user of the current connection
    pub async fn change_user(
        &mut self,
        username: &str,
        password: &str,
        db_name: &str,
        attrs: Vec<ConnectAttr>,
    ) -> Result<()> {
        // let (auth_plugin_name, auth_response) = gen_auth_resp(username, password, &self.auth_data)?;
        let cmd = ComChangeUser::new(username, vec![], db_name, "mysql_native_password", vec![]);
        let mut buf = BytesMut::new();
        cmd.write_with_ctx(&mut buf, &self.cap_flags)?;
        self.send_msg(buf.freeze(), true).await?;

        loop {
            let mut msg = self.recv_msg().await?;
            match ComChangeUserResponse::read_with_ctx(&mut msg, &self.cap_flags)? {
                ComChangeUserResponse::Ok(_) => break,
                ComChangeUserResponse::Err(err) => return Err(err.into()),
                ComChangeUserResponse::Switch(switch) => {
                    // currently only support mysql_native_password
                    let (_, data) = gen_auth_resp(username, password, switch.auth_plugin_data)?;
                    self.send_msg(&data[..], false).await?;
                }
                ComChangeUserResponse::More(_) => unimplemented!(),
            }
        }
        Ok(())
    }

    pub async fn process_kill(&mut self, conn_id: u32) -> Result<()> {
        let cmd = ComProcessKill::new(conn_id);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComProcessKillResponse::read_with_ctx(&mut msg, &self.cap_flags)? {
            ComProcessKillResponse::Ok(_) => Ok(()),
            ComProcessKillResponse::Err(e) => Err(e.into()),
        }
    }

    pub async fn reset_connection(&mut self) -> Result<()> {
        let cmd = ComResetConnection::new();
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComResetConnectionResponse::read_with_ctx(&mut msg, &self.cap_flags)? {
            ComResetConnectionResponse::Ok(_) => Ok(()),
            ComResetConnectionResponse::Err(err) => Err(err.into()),
        }
    }
}

impl<S> Conn<S>
where
    S: AsyncRead + AsyncWrite + Clone + Unpin,
{
    /// provide a handle that could send the server a text-based query that
    /// is executed immediately
    ///
    /// it returns a future of QueryResultSet, which can be treated as
    /// stream of rows
    pub fn query(&mut self) -> Query<S> {
        Query::new(self)
    }

    /// get a list of active threads
    pub fn process_info<'a>(&'a mut self) -> QueryResultSetFuture<'a, S> {
        let cmd = ComProcessInfo::new();
        QueryResultSetFuture::new(self, cmd)
    }
}

fn gen_auth_resp<U, P, S>(username: U, password: P, seed: S) -> Result<(String, Vec<u8>)>
where
    U: AsRef<str>,
    P: AsRef<str>,
    S: AsRef<[u8]>,
{
    let mut seed = seed.as_ref();
    if let Some(0x00) = seed.last() {
        // remove trailing 0x00 byte
        seed = &seed[..seed.len() - 1];
    }
    let mut ap = MysqlNativePassword::new();
    ap.set_credential(username.as_ref(), password.as_ref());
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

    #[smol_potat::test]
    async fn test_quit() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.quit().await.unwrap();
    }

    #[smol_potat::test]
    async fn test_init_db_success() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.init_db("mysql").await.unwrap();
    }

    #[smol_potat::test]
    async fn test_init_db_fail() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let fail = conn.init_db("not_exists").await;
        dbg!(fail.unwrap_err())
    }

    #[smol_potat::test]
    async fn test_field_list_success() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.init_db("mysql").await.unwrap();
        let col_defs = conn.field_list("user", "%").await.unwrap();
        dbg!(col_defs);
    }

    #[smol_potat::test]
    async fn test_field_list_fail() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.init_db("mysql").await.unwrap();
        let fail = conn.field_list("not_exists", "%").await;
        dbg!(fail.unwrap_err());
    }

    #[smol_potat::test]
    #[should_panic]
    async fn test_create_db_already_deprecated() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.create_db("create_db_test1").await.unwrap();
    }

    #[smol_potat::test]
    #[should_panic]
    async fn test_drop_db_already_deprecated() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.drop_db("drop_db_test1").await.unwrap();
    }

    #[smol_potat::test]
    async fn test_refresh() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.refresh(RefreshFlags::GRANT).await.unwrap();
    }

    #[smol_potat::test]
    async fn test_statistics() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let stats = conn.statistics().await.unwrap();
        dbg!(stats);
    }

    #[smol_potat::test]
    async fn test_process_info() {
        use futures::StreamExt;
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        let mut process_info = conn.process_info().await.unwrap();
        while let Some(row) = process_info.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_process_kill_success() {
        use futures::StreamExt;
        let mut conn1 = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn1.handshake(conn_opts()).await.unwrap();

        let mut conn2 = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn2.handshake(conn_opts()).await.unwrap();
        let mut process_ids = Vec::new();
        let mut process_info = conn2.process_info().await.unwrap();
        while let Some(row) = process_info.next().await {
            let id = std::str::from_utf8(row[0].as_ref().unwrap().bytes()).unwrap();
            let id: u32 = id.parse().unwrap();
            process_ids.push(id);
        }
        conn2.process_kill(process_ids[0]).await.unwrap();
    }

    #[smol_potat::test]
    #[should_panic]
    async fn test_process_kill_fail() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.process_kill(38475836).await.unwrap();
    }

    #[smol_potat::test]
    async fn test_debug() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.debug().await.unwrap();
    }

    #[smol_potat::test]
    async fn test_ping() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.ping().await.unwrap();
    }

    #[smol_potat::test]
    async fn test_change_user() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.change_user("root", "password", "", vec![])
            .await
            .unwrap();
    }

    #[smol_potat::test]
    async fn test_reset_connection() {
        let mut conn = Conn::connect("127.0.0.1:13306").await.unwrap();
        conn.handshake(conn_opts()).await.unwrap();
        conn.reset_connection().await.unwrap();
    }

    fn conn_opts() -> ConnOpts {
        ConnOpts {
            username: "root".to_owned(),
            password: "password".to_owned(),
            database: "".to_owned(),
        }
    }
}
