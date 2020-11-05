use crate::auth_plugin::{AuthPlugin, MysqlNativePassword};
use crate::binlog::{Binlog, BinlogFile, BinlogFileMapper};
use crate::error::{Error, Result};
use crate::query::Query;
use crate::resultset::{new_result_set, ResultSet};
use crate::stmt::Stmt;
use bytes::{Buf, Bytes, BytesMut};
use bytes_parser::{ReadFromBytes, WriteToBytes, WriteToBytesWithContext};
use futures::io::{AsyncReadExt, AsyncWriteExt};
use futures::{AsyncRead, AsyncWrite};
use mybin_core::cmd::*;
use mybin_core::col::{ColumnDefinition, TextColumnValue};
use mybin_core::flag::{CapabilityFlags, StatusFlags};
use mybin_core::handshake::{ConnectAttr, HandshakeClientResponse41, InitialHandshake};
use mybin_core::packet::{HandshakeMessage, OkPacket};
use mybin_core::quit::ComQuit;
use mybin_core::resp::ComResponse;
use mybin_core::resultset::{ColumnExtractor, FromColumnValue, RowMapper};
use mybin_core::stmt::ToColumnValue;
use serde_derive::*;
use std::marker::PhantomData;
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

impl<S> Conn<S>
where
    S: AsyncRead + Unpin,
{
    /// receive message from MySQL server
    ///
    /// this method will concat mutliple packets if payload too large.
    pub async fn recv_msg(&mut self) -> Result<Bytes> {
        let mut bs = Vec::new();
        loop {
            // 1. first 3 bytes as message length
            let mut len = [0u8; 3];
            let _ = self.stream.read_exact(&mut len).await?;
            let len = (len[0] as u64) + ((len[1] as u64) << 8) + ((len[2] as u64) << 16);
            // 2. then 1 byte packet sequence
            let mut seq = 0u8;
            let _ = self
                .stream
                .read_exact(std::slice::from_mut(&mut seq))
                .await?;
            if seq == 0xff {
                self.pkt_nr = 0;
            } else {
                self.pkt_nr = seq + 1;
            }
            // 3. payload with <msg_len> bytes
            // if msg_len equals 0xffffff, additional packet follows
            let start = bs.len();
            bs.reserve(len as usize);
            for _ in 0..len {
                bs.push(0);
            }
            let _ = self.stream.read_exact(&mut bs[start..]).await?;
            if len < 0xff_ffff {
                break;
            }
        }
        Ok(Bytes::from(bs))
    }
}

impl<S> Conn<S>
where
    S: AsyncWrite + Unpin,
{
    /// send message to MySQL server
    ///
    /// this method will split message into multiple packets if payload too large.
    pub async fn send_msg<T: WriteToBytes + std::fmt::Debug>(
        &mut self,
        msg: T,
        reset_pkt_nr: bool,
    ) -> Result<()> {
        if reset_pkt_nr {
            self.reset_pkt_nr();
        }
        let mut bs = BytesMut::new();
        msg.write_to(&mut bs)?;
        let mut bs = bs.freeze();
        while bs.remaining() >= 0xff_ffff {
            let payload = bs.split_to(0xff_ffff);
            self.send_packet(payload).await?;
        }
        // even if packet is 0 byte, still send to notify the ending
        self.send_packet(bs).await?;
        Ok(())
    }

    async fn send_packet(&mut self, payload: Bytes) -> Result<()> {
        // 1. 3-byte packet length
        let len = payload.remaining();
        let len = [
            (len & 0xff) as u8,
            ((len >> 8) & 0xff) as u8,
            ((len >> 16) & 0xff) as u8,
        ];
        let _ = self.stream.write_all(&len[..]).await?;
        // 2. 1-byte seq
        let seq = self.pkt_nr;
        let _ = self.stream.write_all(std::slice::from_ref(&seq)).await?;
        // 3. <len> bytes payload
        let _ = self.stream.write_all(payload.bytes()).await?;
        // increment pkt_nr at end
        self.pkt_nr += 1;
        Ok(())
    }
}

#[allow(dead_code)]
impl<S> Conn<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// create new connection given an already connected stream
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            cap_flags: CapabilityFlags::empty(),
            server_status: StatusFlags::empty(),
            pkt_nr: 0,
            // msg_state: MsgState::Len,
            // recv_buf: Vec::new(),
            // recv_len: 0,
            // send_buf: vec![],
        }
    }

    /// create new connection given an already connected stream and client/server status
    pub fn with_status(stream: S, cap_flags: CapabilityFlags, server_status: StatusFlags) -> Self {
        Conn {
            stream,
            cap_flags,
            server_status,
            pkt_nr: 0,
            // msg_state: MsgState::Len,
            // recv_buf: Vec::new(),
            // recv_len: 0,
            // send_buf: vec![],
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
        match HandshakeMessage::read_from(&mut msg, &cap_flags)? {
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
        match ComResponse::read_from(&mut msg, &self.cap_flags)? {
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
            match ComFieldListResponse::read_from(&mut msg, &self.cap_flags, true)? {
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
        match ComCreateDBResponse::read_from(&mut msg, &self.cap_flags)? {
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
        match ComDropDBResponse::read_from(&mut msg, &self.cap_flags)? {
            ComDropDBResponse::Ok(_) => Ok(()),
            ComDropDBResponse::Err(err) => Err(err.into()),
        }
    }

    /// a low-level version of several FLUSH ... and RESET ... statements
    pub async fn refresh(&mut self, sub_cmd: RefreshFlags) -> Result<()> {
        let cmd = ComRefresh::new(sub_cmd);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComRefreshResponse::read_from(&mut msg, &self.cap_flags)? {
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
        match ComDebugResponse::read_from(&mut msg, &self.cap_flags)? {
            ComDebugResponse::Eof(_) => Ok(()),
            ComDebugResponse::Err(err) => Err(err.into()),
        }
    }

    /// check if the server is alive
    pub async fn ping(&mut self) -> Result<()> {
        let cmd = ComPing::new();
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        OkPacket::read_from(&mut msg, &self.cap_flags)?;
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
        // send empty auth data in first request
        // and send read auth data in AuthSwitchResponse
        let cmd = ComChangeUser::new(username, vec![], db_name, "mysql_native_password", attrs);
        let mut buf = BytesMut::new();
        cmd.write_with_ctx(&mut buf, &self.cap_flags)?;
        self.send_msg(buf.freeze(), true).await?;

        loop {
            let mut msg = self.recv_msg().await?;
            match ComChangeUserResponse::read_from(&mut msg, &self.cap_flags)? {
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

    /// ask the server to terminate a connection
    pub async fn process_kill(&mut self, conn_id: u32) -> Result<()> {
        let cmd = ComProcessKill::new(conn_id);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComProcessKillResponse::read_from(&mut msg, &self.cap_flags)? {
            ComProcessKillResponse::Ok(_) => Ok(()),
            ComProcessKillResponse::Err(e) => Err(e.into()),
        }
    }

    /// reset session state
    pub async fn reset_connection(&mut self) -> Result<()> {
        let cmd = ComResetConnection::new();
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComResetConnectionResponse::read_from(&mut msg, &self.cap_flags)? {
            ComResetConnectionResponse::Ok(_) => Ok(()),
            ComResetConnectionResponse::Err(err) => Err(err.into()),
        }
    }

    /// enable or disable multiple statements for current connection
    pub async fn set_multi_stmts(&mut self, multi_stmts: bool) -> Result<()> {
        let cmd = ComSetOption::new(multi_stmts);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComSetOptionResponse::read_from(&mut msg, &self.cap_flags)? {
            ComSetOptionResponse::Eof(_) => {
                if multi_stmts {
                    self.cap_flags.insert(CapabilityFlags::MULTI_STATEMENTS);
                } else {
                    self.cap_flags.remove(CapabilityFlags::MULTI_STATEMENTS);
                }
                Ok(())
            }
            ComSetOptionResponse::Err(err) => Err(err.into()),
        }
    }

    /// register a slave at the master
    ///
    /// should set slave_uuid before register as slave
    pub async fn register_slave(&mut self, server_id: u32) -> Result<()> {
        let cmd = ComRegisterSlave::anonymous(server_id);
        self.send_msg(cmd, true).await?;
        let mut msg = self.recv_msg().await?;
        match ComRegisterSlaveResponse::read_from(&mut msg, &self.cap_flags)? {
            ComRegisterSlaveResponse::Ok(_) => Ok(()),
            ComRegisterSlaveResponse::Err(err) => Err(err.into()),
        }
    }

    /// get a list of active threads
    pub async fn process_info<'a>(&'a mut self) -> Result<ResultSet<'a, S, TextColumnValue>> {
        let cmd = ComProcessInfo::new();
        self.send_msg(cmd, true).await?;
        new_result_set(self, None).await
    }

    /// get a list of binlog files
    pub async fn binlog_files(&mut self) -> Result<Vec<BinlogFile>> {
        let mut rs = self
            .query()
            .qry("SHOW MASTER LOGS")
            .await?
            .map_rows(BinlogFileMapper);

        let mut files = vec![];
        while let Some(file) = rs.next_row().await? {
            files.push(file?);
        }
        Ok(files)
    }

    /// get variable by name
    ///
    /// SQL:
    /// SHOW VARIABLES like '<name>'
    pub async fn get_var<T, U>(&mut self, name: U, global: bool) -> Result<Option<T>>
    where
        U: AsRef<str>,
        T: FromColumnValue<TextColumnValue> + Unpin,
    {
        let qry = if global {
            format!("SHOW GLOBAL VARIABLES LIKE '{}'", name.as_ref())
        } else {
            format!("SHOW VARIABLES LIKE '{}'", name.as_ref())
        };
        let rs = self
            .query()
            .qry(qry)
            .await?
            .map_rows(VariableMapper::<T> {
                _marker: PhantomData,
            })
            .first_or_none()
            .await?;
        if let Some(var) = rs {
            let var = var?;
            return Ok(Some(var));
        }
        Ok(None)
    }

    /// set variable by name and value
    ///
    /// SQL:
    /// SET @@<name> = <value>
    pub async fn set_var<T, V>(&mut self, name: T, value: V, global: bool) -> Result<()>
    where
        T: AsRef<str>,
        V: ToColumnValue,
    {
        // todo: value must be correct type, not only string
        let qry = if global {
            format!("SET @@GLOBAL.{} = ?", name.as_ref())
        } else {
            format!("SET @@{} = ?", name.as_ref())
        };
        let stmt = self.stmt().prepare(qry).await?;
        stmt.exec_close(vec![value.to_col()]).await
    }

    /// get user defined variable by name
    ///
    /// SQL:
    /// SELECT @<name>
    pub async fn get_user_var<T, U>(&mut self, name: U) -> Result<Option<T>>
    where
        U: AsRef<str>,
        T: FromColumnValue<TextColumnValue> + Unpin,
    {
        let rs = self
            .query()
            .qry(format!("SELECT @{}", name.as_ref()))
            .await?
            .map_rows(
                |extr: &ColumnExtractor, row: Vec<TextColumnValue>| -> Result<T> {
                    let val = extr.get_col(&row, 0)?;
                    Ok(val)
                },
            )
            .first_or_none()
            .await?;
        if let Some(var) = rs {
            let var = var?;
            return Ok(Some(var));
        }
        Ok(None)
    }

    /// set user defined variable
    ///
    /// SQL:
    /// SET @<name> = <value>
    pub async fn set_user_var<T, V>(&mut self, name: T, value: V) -> Result<()>
    where
        T: AsRef<str>,
        V: ToColumnValue,
    {
        let stmt = self
            .stmt()
            .prepare(format!("SET @{} = ?", name.as_ref()))
            .await?;
        stmt.exec_close(vec![value.to_col()]).await
    }

    pub fn binlog(&mut self) -> Binlog<S> {
        Binlog::new(self)
    }

    /// provide a handle that could send the server a text-based query that
    /// is executed immediately
    ///
    /// it returns a future of QueryResultSet, which can be treated as
    /// stream of rows
    pub fn query(&mut self) -> Query<S> {
        Query::new(self)
    }

    pub fn stmt(&mut self) -> Stmt<S> {
        Stmt::new(self)
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

#[derive(Debug)]
pub struct VariableMapper<T> {
    _marker: PhantomData<T>,
}

impl<T> RowMapper<TextColumnValue> for VariableMapper<T>
where
    T: FromColumnValue<TextColumnValue> + Unpin,
{
    type Output = Result<T>;

    fn map_row(&self, extr: &ColumnExtractor, row: Vec<TextColumnValue>) -> Self::Output {
        let val = extr.get_col(&row, 1)?;
        Ok(val)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use async_net::TcpStream;

    pub(crate) async fn new_conn() -> Conn<async_net::TcpStream> {
        let stream = TcpStream::connect("127.0.0.1:13306").await.unwrap();
        let mut conn = Conn::new(stream);
        let opts = ConnOpts {
            username: "root".to_owned(),
            password: "password".to_owned(),
            database: "".to_owned(),
        };
        conn.handshake(opts).await.unwrap();
        conn
    }

    #[smol_potat::test]
    async fn test_conn_and_handshake() {
        new_conn().await;
    }

    #[smol_potat::test]
    async fn test_quit() {
        let conn = new_conn().await;
        conn.quit().await.unwrap();
    }

    #[smol_potat::test]
    async fn test_init_db_success() {
        let mut conn = new_conn().await;
        conn.init_db("mysql").await.unwrap();
    }

    #[smol_potat::test]
    async fn test_init_db_fail() {
        let mut conn = new_conn().await;
        let fail = conn.init_db("not_exists").await;
        dbg!(fail.unwrap_err())
    }

    #[smol_potat::test]
    async fn test_field_list_success() {
        let mut conn = new_conn().await;
        conn.init_db("mysql").await.unwrap();
        let col_defs = conn.field_list("user", "%").await.unwrap();
        dbg!(col_defs);
    }

    #[smol_potat::test]
    async fn test_field_list_fail() {
        let mut conn = new_conn().await;
        conn.init_db("mysql").await.unwrap();
        let fail = conn.field_list("not_exists", "%").await;
        dbg!(fail.unwrap_err());
    }

    #[smol_potat::test]
    #[should_panic]
    async fn test_create_db_already_deprecated() {
        let mut conn = new_conn().await;
        conn.create_db("create_db_test1").await.unwrap();
    }

    #[smol_potat::test]
    #[should_panic]
    async fn test_drop_db_already_deprecated() {
        let mut conn = new_conn().await;
        conn.drop_db("drop_db_test1").await.unwrap();
    }

    #[smol_potat::test]
    async fn test_refresh() {
        let mut conn = new_conn().await;
        conn.refresh(RefreshFlags::GRANT).await.unwrap();
    }

    #[smol_potat::test]
    async fn test_statistics() {
        let mut conn = new_conn().await;
        let stats = conn.statistics().await.unwrap();
        dbg!(stats);
    }

    #[smol_potat::test]
    async fn test_process_info() {
        let mut conn = new_conn().await;
        let mut process_info = conn.process_info().await.unwrap();
        while let Some(row) = process_info.next_row().await.unwrap() {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_process_kill_success() {
        use bytes::Buf;
        let mut conn1 = new_conn().await;
        let mut conn_id_row = conn1
            .query()
            .qry("select connection_id()")
            .await
            .unwrap()
            .next_row()
            .await
            .unwrap()
            .unwrap();
        let conn_id_bytes = conn_id_row.pop().unwrap().unwrap();
        let conn_id: u32 = String::from_utf8_lossy(conn_id_bytes.bytes())
            .parse()
            .unwrap();
        log::debug!("connection_id={}", conn_id);
        let mut conn2 = new_conn().await;
        conn2.process_kill(conn_id).await.unwrap();
    }

    #[smol_potat::test]
    #[should_panic]
    async fn test_process_kill_fail() {
        let mut conn = new_conn().await;
        conn.process_kill(38475836).await.unwrap();
    }

    #[smol_potat::test]
    async fn test_debug() {
        let mut conn = new_conn().await;
        conn.debug().await.unwrap();
    }

    #[smol_potat::test]
    async fn test_ping() {
        let mut conn = new_conn().await;
        conn.ping().await.unwrap();
    }

    #[smol_potat::test]
    async fn test_change_user() {
        let mut conn = new_conn().await;
        conn.change_user("root", "password", "", vec![])
            .await
            .unwrap();
    }

    #[smol_potat::test]
    async fn test_reset_connection() {
        let mut conn = new_conn().await;
        conn.reset_connection().await.unwrap();
    }

    #[smol_potat::test]
    async fn test_set_multi_stmts() {
        let mut conn = new_conn().await;
        conn.set_multi_stmts(true).await.unwrap();
    }

    #[smol_potat::test]
    async fn test_register_slave() {
        let mut conn = new_conn().await;
        // set slave_id before register
        // this is important
        // otherwise, the query 'SHOW SLAVE HOSTS' will return
        // malformed result set (missing SLAVE_UUID)
        conn.query()
            .exec(format!(
                "SET @slave_uuid = '{}'",
                "e919a265-ede3-11ea-8c72-0242ac110002"
            ))
            .await
            .unwrap();
        conn.register_slave(1234567).await.unwrap();
        let mut rs = conn.query().qry("SHOW SLAVE HOSTS").await.unwrap();
        while let Some(row) = rs.next_row().await.unwrap() {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_conn_binlog_files() {
        let mut conn = new_conn().await;
        let files = conn.binlog_files().await.unwrap();
        assert!(!files.is_empty());
    }

    #[smol_potat::test]
    async fn test_conn_ops_var() {
        let mut conn = new_conn().await;
        let sql_warnings: Option<String> = conn.get_var("SQL_WARNINGS", false).await.unwrap();
        dbg!(sql_warnings);
        conn.set_var("SQL_WARNINGS", "OFF".to_owned(), false)
            .await
            .unwrap();
        let max_connections: Option<u32> = conn.get_var("MAX_CONNECTIONS", true).await.unwrap();
        dbg!(max_connections);
        conn.set_var("MAX_CONNECTIONS", 500_u32, true)
            .await
            .unwrap();
    }
}
