use crate::conn::Conn;
use crate::error::{Error, Needed, Result};
use crate::resultset::{new_result_set, ResultSet};
use bytes::{Buf, Bytes};
use bytes_parser::{ReadFromBytes, ReadFromBytesWithContext};
use futures::{AsyncRead, AsyncWrite};
use mybin_core::cmd::{ComStmtExecute, ComStmtPrepare, StmtExecValue, StmtPrepareOk};
use mybin_core::col::{BinaryColumnValue, ColumnDefinition};
use mybin_core::flag::CapabilityFlags;
use mybin_core::packet::{EofPacket, ErrPacket, OkPacket};

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
                let err = ErrPacket::read_with_ctx(&mut msg, (&self.conn.cap_flags, true))?;
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
                let def = ColumnDefinition::read_with_ctx(&mut msg, false)?;
                defs.push(def);
            }
            // eof packet
            if !self.conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                let mut msg = self.conn.recv_msg().await?;
                EofPacket::read_with_ctx(&mut msg, &self.conn.cap_flags)?;
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
                let def = ColumnDefinition::read_with_ctx(&mut msg, false)?;
                defs.push(def);
            }
            // eof packet
            if !self.conn.cap_flags.contains(CapabilityFlags::DEPRECATE_EOF) {
                let mut msg = self.conn.recv_msg().await?;
                EofPacket::read_with_ctx(&mut msg, &self.conn.cap_flags)?;
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
    pub async fn exec(self, params: Vec<StmtExecValue>) -> Result<()> {
        let cmd = ComStmtExecute::single(self.stmt_id, params);
        self.conn.send_msg(cmd, true).await?;
        loop {
            let mut msg = self.conn.recv_msg().await?;
            match msg[0] {
                0xff => {
                    let err = ErrPacket::read_with_ctx(&mut msg, (&self.conn.cap_flags, true))?;
                    return Err(err.into());
                }
                0x00 => {
                    OkPacket::read_with_ctx(&mut msg, &self.conn.cap_flags)?;
                    // todo: handle session state changes and provide close handler
                    return Ok(());
                }
                _ => {
                    log::warn!("execute statement but returns additional data");
                }
            }
        }
    }

    pub async fn qry(
        self,
        params: Vec<StmtExecValue>,
    ) -> Result<ResultSet<'s, S, BinaryColumnValue>> {
        let cmd = ComStmtExecute::single(self.stmt_id, params);
        self.conn.send_msg(cmd, true).await?;
        new_result_set(self.conn).await
    }
}

#[cfg(test)]
mod tests {
    use crate::conn::tests::new_conn;
    use bigdecimal::BigDecimal;
    use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
    use mybin_core::cmd::StmtExecValue;

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
    async fn test_stmt_qry_empty() {
        use futures::StreamExt;
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
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_stmt_table_and_column() {
        use futures::StreamExt;
        use mybin_core::resultset::{MyBit, MyBytes, MyI24, MyString, MyTime, MyU24, MyYear};
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
                StmtExecValue::new_decimal(BigDecimal::from(-100_i64)),
                StmtExecValue::new_tinyint(-5),
                StmtExecValue::new_unsigned_tinyint(18),
                StmtExecValue::new_smallint(-4892),
                StmtExecValue::new_unsigned_smallint(32003),
                StmtExecValue::new_int(-159684321),
                StmtExecValue::new_unsigned_int(2003495865),
                StmtExecValue::new_float(-0.5),
                StmtExecValue::new_float(1.5),
                StmtExecValue::new_double(-0.625),
                StmtExecValue::new_double(1.625),
                StmtExecValue::new_timestamp(NaiveDate::from_ymd(2020, 1, 1).and_hms(1, 2, 3)),
                StmtExecValue::new_bigint(-12948340587434),
                StmtExecValue::new_unsigned_bigint(9348578923762),
                StmtExecValue::new_int(-90034),
                StmtExecValue::new_unsigned_int(87226),
                StmtExecValue::new_date(NaiveDate::from_ymd(2020, 12, 31)),
                StmtExecValue::new_time(true, 8, NaiveTime::from_hms(20, 30, 40)),
                StmtExecValue::new_datetime(
                    NaiveDate::from_ymd(2012, 6, 7).and_hms_micro(15, 38, 46, 92000),
                ),
                StmtExecValue::new_year(2021),
                StmtExecValue::new_varchar("hello, world"),
                StmtExecValue::new_varchar("hello, java"),
                StmtExecValue::new_bit(vec![0b01100001, 0b10001100]),
                StmtExecValue::new_decimal(BigDecimal::from_str("123456789.22").unwrap()),
                StmtExecValue::new_blob(b"hello, tinyblob".to_vec()),
                StmtExecValue::new_blob(b"hello, mediumblob".to_vec()),
                StmtExecValue::new_blob(b"hello, longblob".to_vec()),
                StmtExecValue::new_blob(b"hello, blob".to_vec()),
                StmtExecValue::new_text("hello, latin1"),
                StmtExecValue::new_text("hello, utf8"),
                StmtExecValue::new_text("hello, binary"),
                StmtExecValue::new_bool(true),
                StmtExecValue::new_char("fixed"),
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
        while let Some(row) = rs.next().await {
            dbg!(&row);
            let c1: BigDecimal = extractor.get_named_col(&row, "c1").unwrap().unwrap();
            dbg!(c1);
            let c2: i8 = extractor.get_named_col(&row, "c2").unwrap().unwrap();
            dbg!(c2);
            let c3: u8 = extractor.get_named_col(&row, "c3").unwrap().unwrap();
            dbg!(c3);
            let c4: i16 = extractor.get_named_col(&row, "c4").unwrap().unwrap();
            dbg!(c4);
            let c5: u16 = extractor.get_named_col(&row, "c5").unwrap().unwrap();
            dbg!(c5);
            let c6: i32 = extractor.get_named_col(&row, "c6").unwrap().unwrap();
            dbg!(c6);
            let c7: u32 = extractor.get_named_col(&row, "c7").unwrap().unwrap();
            dbg!(c7);
            let c8: f32 = extractor.get_named_col(&row, "c8").unwrap().unwrap();
            dbg!(c8);
            let c9: f32 = extractor.get_named_col(&row, "c9").unwrap().unwrap();
            dbg!(c9);
            let c10: f64 = extractor.get_named_col(&row, "c10").unwrap().unwrap();
            dbg!(c10);
            let c11: f64 = extractor.get_named_col(&row, "c11").unwrap().unwrap();
            dbg!(c11);
            let c12: NaiveDateTime = extractor.get_named_col(&row, "c12").unwrap().unwrap();
            dbg!(c12);
            let c13: i64 = extractor.get_named_col(&row, "c13").unwrap().unwrap();
            dbg!(c13);
            let c14: u64 = extractor.get_named_col(&row, "c14").unwrap().unwrap();
            dbg!(c14);
            let c15: MyI24 = extractor.get_named_col(&row, "c15").unwrap().unwrap();
            dbg!(c15);
            let c16: MyU24 = extractor.get_named_col(&row, "c16").unwrap().unwrap();
            dbg!(c16);
            let c17: NaiveDate = extractor.get_named_col(&row, "c17").unwrap().unwrap();
            dbg!(c17);
            let c18: MyTime = extractor.get_named_col(&row, "c18").unwrap().unwrap();
            dbg!(c18);
            let c19: NaiveDateTime = extractor.get_named_col(&row, "c19").unwrap().unwrap();
            dbg!(c19);
            let c20: MyYear = extractor.get_named_col(&row, "c20").unwrap().unwrap();
            dbg!(c20);
            let c21: MyString = extractor.get_named_col(&row, "c21").unwrap().unwrap();
            dbg!(c21);
            let c22: MyString = extractor.get_named_col(&row, "c22").unwrap().unwrap();
            dbg!(c22);
            let c23: MyBit = extractor.get_named_col(&row, "c23").unwrap().unwrap();
            dbg!(c23);
            let c24: BigDecimal = extractor.get_named_col(&row, "c24").unwrap().unwrap();
            dbg!(c24);
            let c25: MyBytes = extractor.get_named_col(&row, "c25").unwrap().unwrap();
            dbg!(c25);
            let c26: MyBytes = extractor.get_named_col(&row, "c26").unwrap().unwrap();
            dbg!(c26);
            let c27: MyBytes = extractor.get_named_col(&row, "c27").unwrap().unwrap();
            dbg!(c27);
            let c28: MyBytes = extractor.get_named_col(&row, "c28").unwrap().unwrap();
            dbg!(c28);
            let c29: MyBytes = extractor.get_named_col(&row, "c29").unwrap().unwrap();
            dbg!(c29);
            let c30: MyBytes = extractor.get_named_col(&row, "c30").unwrap().unwrap();
            dbg!(c30);
            let c31: MyBytes = extractor.get_named_col(&row, "c31").unwrap().unwrap();
            dbg!(c31);
            let c32: bool = extractor.get_named_col(&row, "c32").unwrap().unwrap();
            dbg!(c32);
            let c33: MyString = extractor.get_named_col(&row, "c33").unwrap().unwrap();
            dbg!(c33);
        }
    }
}
