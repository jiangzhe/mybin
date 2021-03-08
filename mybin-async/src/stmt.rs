use crate::conn::Conn;
use crate::error::{Error, Needed, Result};
use crate::resultset::{new_result_set, ResultSet};
use bytes::{Buf, Bytes};
use bytes_parser::ReadFromBytes;
use futures::{AsyncRead, AsyncWrite};
use mybin_core::cmd::{ComStmtClose, ComStmtExecute, ComStmtPrepare, StmtPrepareOk};
use mybin_core::col::{BinaryColumnValue, ColumnDefinition};
use mybin_core::flag::CapabilityFlags;
use mybin_core::packet::{EofPacket, ErrPacket, OkPacket};
use mybin_core::stmt::StmtColumnValue;

#[derive(Debug)]
pub struct Stmt<'s, S> {
    conn: &'s mut Conn<S>,
}

impl<'a, S> Stmt<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn new(conn: &'a mut Conn<S>) -> Self {
        Self { conn }
    }

    pub async fn prepare<Q: Into<String>>(self, qry: Q) -> Result<PreparedStmt<'a, S>> {
        let cmd = ComStmtPrepare::new(qry);
        self.conn.send_msg(cmd, true).await?;
        let mut msg = self.conn.recv_msg().await?;
        if !msg.has_remaining() {
            return Err(Error::InputIncomplete(Bytes::new(), Needed::Unknown));
        }
        let ok = match msg[0] {
            0xff => {
                let err = ErrPacket::read_from(&mut msg, &self.conn.cap_flags, true)?;
                return Err(err.into());
            }
            _ => StmtPrepareOk::read_from(&mut msg)?,
        };
        log::debug!("prepared ok: {:?}", ok);
        // parameter definition packets
        let param_defs = if ok.n_params == 0 {
            Vec::new()
        } else {
            let mut defs = Vec::with_capacity(ok.n_params as usize);
            for _ in 0..ok.n_params {
                let mut msg = self.conn.recv_msg().await?;
                let def = ColumnDefinition::read_from(&mut msg, false)?;
                defs.push(def);
            }
            // eof packet
            if !self.conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                let mut msg = self.conn.recv_msg().await?;
                EofPacket::read_from(&mut msg, &self.conn.cap_flags)?;
            }
            defs
        };
        // column definition packets
        let col_defs = if ok.n_cols == 0 {
            Vec::new()
        } else {
            let mut defs = Vec::with_capacity(ok.n_params as usize);
            for _ in 0..ok.n_cols {
                let mut msg = self.conn.recv_msg().await?;
                let def = ColumnDefinition::read_from(&mut msg, false)?;
                defs.push(def);
            }
            // eof packet
            if !self.conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                let mut msg = self.conn.recv_msg().await?;
                EofPacket::read_from(&mut msg, &self.conn.cap_flags)?;
            }
            defs
        };
        Ok(PreparedStmt {
            conn: self.conn,
            stmt_id: ok.stmt_id,
            col_defs,
            param_defs,
            n_warnings: ok.n_warnings,
        })
    }
}

#[derive(Debug)]
pub struct PreparedStmt<'s, S> {
    conn: &'s mut Conn<S>,
    pub stmt_id: u32,
    pub col_defs: Vec<ColumnDefinition>,
    pub param_defs: Vec<ColumnDefinition>,
    pub n_warnings: u16,
}

impl<'s, S> PreparedStmt<'s, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub async fn exec(&mut self, params: Vec<StmtColumnValue>) -> Result<()> {
        let cmd = ComStmtExecute::single(self.stmt_id, params);
        self.conn.send_msg(cmd, true).await?;
        loop {
            let mut msg = self.conn.recv_msg().await?;
            match msg[0] {
                0xff => {
                    let err = ErrPacket::read_from(&mut msg, &self.conn.cap_flags, true)?;
                    return Err(err.into());
                }
                0x00 => {
                    OkPacket::read_from(&mut msg, &self.conn.cap_flags)?;
                    // todo: handle session state changes and provide close handler
                    return Ok(());
                }
                _ => {
                    log::warn!("execute statement but returns additional data");
                }
            }
        }
    }

    pub async fn exec_close(mut self, params: Vec<StmtColumnValue>) -> Result<()> {
        match self.exec(params).await {
            Ok(_) => {
                let _ = self.close().await;
                Ok(())
            }
            Err(e) => {
                let _ = self.close().await;
                Err(e)
            }
        }
    }

    /// close the statement
    ///
    /// close() should be called to release the prepared
    /// statement on server side
    pub async fn close(self) -> Result<()> {
        let cmd = ComStmtClose::new(self.stmt_id);
        self.conn.send_msg(cmd, true).await
    }
}

/// todo:
/// refine the API so user can reuse the prepared statement
/// to query mutiple result sets
impl<'s, S> PreparedStmt<'s, S>
where
    S: AsyncRead + AsyncWrite + Clone + Unpin,
{
    pub async fn qry(
        self,
        params: Vec<StmtColumnValue>,
    ) -> Result<ResultSet<'s, S, BinaryColumnValue>> {
        let cmd = ComStmtExecute::single(self.stmt_id, params);
        self.conn.send_msg(cmd, true).await?;
        let rs = new_result_set(self.conn, Some(self.stmt_id)).await?;
        Ok(rs)
    }
}

#[cfg(test)]
mod tests {
    use crate::conn::tests::new_conn;
    use bigdecimal::BigDecimal;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use mybin_core::stmt::StmtColumnValue;

    #[smol_potat::test]
    async fn test_stmt_exec_success() {
        let mut conn = new_conn().await;
        conn.stmt()
            .prepare("create database if not exists bintest1")
            .await
            .unwrap()
            .exec(vec![])
            .await
            .unwrap();
    }

    #[smol_potat::test]
    async fn test_stmt_exec_fail() {
        let mut conn = new_conn().await;
        let fail = conn
            .stmt()
            .prepare("create table stmt_exec_fail_test1")
            .await;
        dbg!(fail.unwrap_err());
    }

    #[smol_potat::test]
    async fn test_stmt_exec_multi() {
        let mut conn = new_conn().await;
        conn.query()
            .exec("create database if not exists bintest1")
            .await
            .unwrap();
        conn.init_db("bintest1").await.unwrap();
        conn.query()
            .exec("drop table if exists exec_multi")
            .await
            .unwrap();
        conn.query()
            .exec("create table exec_multi (id int)")
            .await
            .unwrap();
        let mut stmt = conn
            .stmt()
            .prepare("insert into exec_multi (id) values (?)")
            .await
            .unwrap();
        stmt.exec(vec![StmtColumnValue::new_int(1)]).await.unwrap();
        stmt.exec(vec![StmtColumnValue::new_int(2)]).await.unwrap();
        stmt.exec(vec![StmtColumnValue::new_int(3)]).await.unwrap();
        stmt.close().await.unwrap();
        let count = conn
            .query()
            .qry("select * from exec_multi")
            .await
            .unwrap()
            .count()
            .await
            .unwrap();
        assert_eq!(3, count);
    }

    #[smol_potat::test]
    async fn test_stmt_qry_empty() {
        let mut conn = new_conn().await;
        conn.query()
            .exec("create database if not exists stmttest1")
            .await
            .unwrap();
        log::debug!("database created");
        conn.init_db("stmttest1").await.unwrap();
        log::debug!("database changed");
        conn.query()
            .exec("create table if not exists stmt_empty (id int)")
            .await
            .unwrap();
        log::debug!("table created");
        let prepared = conn
            .stmt()
            .prepare("select * from stmt_empty")
            .await
            .unwrap();
        log::debug!("stmt prepared");
        let mut rs = prepared.qry(vec![]).await.unwrap();
        while let Some(row) = rs.next_row().await.unwrap() {
            dbg!(row);
        }
        rs.close().await.unwrap();
    }

    #[smol_potat::test]
    async fn test_stmt_prepare_update() {
        let mut conn = new_conn().await;
        conn.query()
            .exec("create database if not exists stmttest2")
            .await
            .unwrap();
        log::debug!("database created");
        conn.init_db("stmttest2").await.unwrap();
        log::debug!("database changed");
        conn.query()
            .exec("create table if not exists stmt_prp_upd (id int)")
            .await
            .unwrap();
        log::debug!("table created");

        let mut ins = conn
            .stmt()
            .prepare("insert into stmt_prp_upd (id) values (?)")
            .await
            .unwrap();
        log::debug!("ins stmt prepared");
        ins.exec(vec![StmtColumnValue::new_int(1)]).await.unwrap();
        ins.close().await.unwrap();

        let mut upd = conn
            .stmt()
            .prepare("update stmt_prp_upd set id = id + ? where id = ?")
            .await
            .unwrap();
        log::debug!("upd stmt prepared");
        upd.exec(vec![
            StmtColumnValue::new_int(1),
            StmtColumnValue::new_int(1),
        ])
        .await
        .unwrap();
        upd.close().await.unwrap();

        let mut sel = conn
            .query()
            .qry("select * from stmt_prp_upd")
            .await
            .unwrap();
        while let Ok(Some(row)) = sel.next_row().await {
            println!("row={:?}", row);
        }

        conn.query()
            .exec("drop database if exists stmttest2")
            .await
            .unwrap();
        log::debug!("database dropped");
    }

    #[smol_potat::test]
    async fn test_stmt_table_and_column() {
        use bytes::Bytes;
        use mybin_core::resultset::{MyBit, MyI24, MyU24, MyYear};
        use mybin_core::time::MyTime;
        use std::str::FromStr;
        let mut conn = new_conn().await;
        // create database
        conn.stmt()
            .prepare(
                r#"
        CREATE DATABASE IF NOT EXISTS bintest1 DEFAULT CHARACTER SET utf8
        "#,
            )
            .await
            .unwrap()
            .exec(vec![])
            .await
            .unwrap();
        // drop table if exists
        conn.stmt()
            .prepare("DROP TABLE IF EXISTS bintest1.typetest_exec")
            .await
            .unwrap()
            .exec(vec![])
            .await
            .unwrap();
        // create table
        conn.stmt()
            .prepare(
                r#"
        CREATE TABLE bintest1.typetest_exec (
            c1 DECIMAL,
            c2 TINYINT,
            c3 TINYINT UNSIGNED,
            c4 SMALLINT,
            c5 SMALLINT UNSIGNED,
            c6 INT,
            c7 INT UNSIGNED,
            c8 FLOAT,
            c9 FLOAT UNSIGNED,
            c10 DOUBLE,
            c11 DOUBLE UNSIGNED,
            c12 TIMESTAMP,
            c13 BIGINT,
            c14 BIGINT UNSIGNED,
            c15 MEDIUMINT,
            c16 MEDIUMINT UNSIGNED,
            c17 DATE,
            c18 TIME,
            c19 DATETIME(6),
            c20 YEAR,
            c21 VARCHAR(50),
            c22 VARCHAR(120) CHARACTER SET binary,
            c23 BIT(16),
            c24 DECIMAL(18, 4),
            c25 TINYBLOB,
            c26 MEDIUMBLOB,
            c27 LONGBLOB,
            c28 BLOB,
            c29 TEXT CHARACTER SET latin1 COLLATE latin1_general_cs,
            c30 TEXT CHARACTER SET utf8,
            c31 TEXT BINARY,
            c32 BOOLEAN,
            c33 CHAR(20)
        )
        "#,
            )
            .await
            .unwrap()
            .exec(vec![])
            .await
            .unwrap();
        // insert data
        conn.stmt()
            .prepare(
                r#"
        INSERT INTO bintest1.typetest_exec (
            c1,c2,c3,c4,c5,
            c6,c7,c8,c9,c10,
            c11,c12,c13,c14,c15,
            c16,c17,c18,c19,c20,
            c21,c22,c23,c24,c25,
            c26,c27,c28,c29,c30,
            c31,c32,c33
        ) VALUES (
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?,?,?,
            ?,?,?
        )
        "#,
            )
            .await
            .unwrap()
            .exec(vec![
                StmtColumnValue::new_decimal(BigDecimal::from(-100_i64)),
                StmtColumnValue::new_tinyint(-5),
                StmtColumnValue::new_unsigned_tinyint(18),
                StmtColumnValue::new_smallint(-4892),
                StmtColumnValue::new_unsigned_smallint(32003),
                StmtColumnValue::new_int(-159684321),
                StmtColumnValue::new_unsigned_int(2003495865),
                StmtColumnValue::new_float(-0.5),
                StmtColumnValue::new_float(1.5),
                StmtColumnValue::new_double(-0.625),
                StmtColumnValue::new_double(1.625),
                StmtColumnValue::new_timestamp(NaiveDate::from_ymd(2020, 1, 1).and_hms(1, 2, 3)),
                StmtColumnValue::new_bigint(-12948340587434),
                StmtColumnValue::new_unsigned_bigint(9348578923762),
                StmtColumnValue::new_int(-90034),
                StmtColumnValue::new_unsigned_int(87226),
                StmtColumnValue::new_date(NaiveDate::from_ymd(2020, 12, 31)),
                StmtColumnValue::new_time(true, 8, NaiveTime::from_hms(20, 30, 40)),
                StmtColumnValue::new_datetime(
                    NaiveDate::from_ymd(2012, 6, 7).and_hms_micro(15, 38, 46, 92000),
                ),
                StmtColumnValue::new_year(2021),
                StmtColumnValue::new_varstring(Bytes::from("hello, world")),
                StmtColumnValue::new_varstring(Bytes::from("hello, java")),
                StmtColumnValue::new_bit(vec![0b01100001, 0b10001100]),
                StmtColumnValue::new_decimal(BigDecimal::from_str("123456789.22").unwrap()),
                StmtColumnValue::new_blob("hello, tinyblob"),
                StmtColumnValue::new_blob("hello, mediumblob"),
                StmtColumnValue::new_blob("hello, longblob"),
                StmtColumnValue::new_blob("hello, blob"),
                StmtColumnValue::new_text("hello, latin1"),
                StmtColumnValue::new_text("hello, utf8"),
                StmtColumnValue::new_text("hello, binary"),
                StmtColumnValue::new_bool(true),
                StmtColumnValue::new_string("fixed"),
            ])
            .await
            .unwrap();
        // select data
        let mut rs = conn
            .stmt()
            .prepare("SELECT * from bintest1.typetest_exec")
            .await
            .unwrap()
            .qry(vec![])
            .await
            .unwrap();
        let extractor = rs.extractor();
        while let Some(row) = rs.next_row().await.unwrap() {
            dbg!(&row);
            let c1: BigDecimal = extractor.get_named_col(&row, "c1").unwrap();
            dbg!(c1);
            let c2: i8 = extractor.get_named_col(&row, "c2").unwrap();
            dbg!(c2);
            let c3: u8 = extractor.get_named_col(&row, "c3").unwrap();
            dbg!(c3);
            let c4: i16 = extractor.get_named_col(&row, "c4").unwrap();
            dbg!(c4);
            let c5: u16 = extractor.get_named_col(&row, "c5").unwrap();
            dbg!(c5);
            let c6: i32 = extractor.get_named_col(&row, "c6").unwrap();
            dbg!(c6);
            let c7: u32 = extractor.get_named_col(&row, "c7").unwrap();
            dbg!(c7);
            let c8: f32 = extractor.get_named_col(&row, "c8").unwrap();
            dbg!(c8);
            let c9: f32 = extractor.get_named_col(&row, "c9").unwrap();
            dbg!(c9);
            let c10: f64 = extractor.get_named_col(&row, "c10").unwrap();
            dbg!(c10);
            let c11: f64 = extractor.get_named_col(&row, "c11").unwrap();
            dbg!(c11);
            let c12: NaiveDateTime = extractor.get_named_col(&row, "c12").unwrap();
            dbg!(c12);
            let c13: i64 = extractor.get_named_col(&row, "c13").unwrap();
            dbg!(c13);
            let c14: u64 = extractor.get_named_col(&row, "c14").unwrap();
            dbg!(c14);
            let c15: MyI24 = extractor.get_named_col(&row, "c15").unwrap();
            dbg!(c15);
            let c16: MyU24 = extractor.get_named_col(&row, "c16").unwrap();
            dbg!(c16);
            let c17: NaiveDate = extractor.get_named_col(&row, "c17").unwrap();
            dbg!(c17);
            let c18: MyTime = extractor.get_named_col(&row, "c18").unwrap();
            dbg!(c18);
            let c19: NaiveDateTime = extractor.get_named_col(&row, "c19").unwrap();
            dbg!(c19);
            let c20: MyYear = extractor.get_named_col(&row, "c20").unwrap();
            dbg!(c20);
            let c21: String = extractor.get_named_col(&row, "c21").unwrap();
            dbg!(c21);
            let c22: String = extractor.get_named_col(&row, "c22").unwrap();
            dbg!(c22);
            let c23: MyBit = extractor.get_named_col(&row, "c23").unwrap();
            dbg!(c23);
            let c24: BigDecimal = extractor.get_named_col(&row, "c24").unwrap();
            dbg!(c24);
            let c25: Bytes = extractor.get_named_col(&row, "c25").unwrap();
            dbg!(c25);
            let c26: Bytes = extractor.get_named_col(&row, "c26").unwrap();
            dbg!(c26);
            let c27: Bytes = extractor.get_named_col(&row, "c27").unwrap();
            dbg!(c27);
            let c28: Bytes = extractor.get_named_col(&row, "c28").unwrap();
            dbg!(c28);
            let c29: String = extractor.get_named_col(&row, "c29").unwrap();
            dbg!(c29);
            let c30: String = extractor.get_named_col(&row, "c30").unwrap();
            dbg!(c30);
            let c31: String = extractor.get_named_col(&row, "c31").unwrap();
            dbg!(c31);
            let c32: bool = extractor.get_named_col(&row, "c32").unwrap();
            dbg!(c32);
            let c33: String = extractor.get_named_col(&row, "c33").unwrap();
            dbg!(c33);
        }
        rs.close().await.unwrap();
    }
}
