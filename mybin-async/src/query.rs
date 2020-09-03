use crate::conn::Conn;
use crate::error::Result;
use crate::resultset::{new_result_set, ResultSet};
use bytes_parser::ReadFromBytesWithContext;
use futures::{AsyncRead, AsyncWrite};
use mybin_core::cmd::ComQuery;
use mybin_core::col::TextColumnValue;
use mybin_core::packet::{ErrPacket, OkPacket};

/// wrapper struct on Conn to provide query functionality
#[derive(Debug)]
pub struct Query<'a, S> {
    conn: &'a mut Conn<S>,
}

impl<'a, S> Query<'a, S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// construct new query from connection
    pub fn new(conn: &'a mut Conn<S>) -> Self {
        Query { conn }
    }

    /// execute a query
    /// 
    /// the query should not return any rows
    pub async fn exec<Q: Into<String>>(self, qry: Q) -> Result<()> {
        // let qry = ComQuery::new(qry);
        // QueryExecFuture::new(self.conn, qry)
        let qry = ComQuery::new(qry);
        self.conn.send_msg(qry, true).await?;
        // handle query like result set
        loop {
            let mut msg = self.conn.recv_msg().await?;
            match msg[0] {
                0xff => {
                    let err = ErrPacket::read_with_ctx(&mut msg, (&self.conn.cap_flags, true))?;
                    return Err(err.into());
                }
                0x00 => {
                    OkPacket::read_with_ctx(&mut msg, &self.conn.cap_flags)?;
                    return Ok(());
                }
                _ => {
                    log::warn!("execute statement but returns additional data");
                }
            }
        }
    }

    pub async fn qry<Q: Into<String>>(self, qry: Q) -> Result<ResultSet<'a, S, TextColumnValue>> {
        let qry = ComQuery::new(qry);
        self.conn.send_msg(qry, true).await?;
        new_result_set(self.conn).await
    }
}

#[cfg(test)]
mod tests {
    use crate::conn::tests::new_conn;
    use bigdecimal::BigDecimal;
    use chrono::{NaiveDate, NaiveDateTime};
    use futures::stream::StreamExt;
    use mybin_core::resultset::{MyBit, MyBytes, MyI24, MyString, MyTime, MyU24, MyYear};

    #[smol_potat::test]
    async fn test_query_set() {
        let mut conn = new_conn().await;
        conn.query()
            .exec("set @master_binlog_checksum = @@global.binlog_checksum")
            .await
            .unwrap();
    }

    #[smol_potat::test]
    async fn test_query_exec_error() {
        let mut conn = new_conn().await;
        let fail = conn.query().exec("drop table not_exist_table").await;
        assert!(fail.is_err());
    }

    #[smol_potat::test]
    async fn test_query_select_1() {
        let mut conn = new_conn().await;
        let mut rs = conn
            .query()
            .qry("select 1, current_timestamp()")
            .await
            .unwrap();
        dbg!(&rs.col_defs);
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_query_select_null() {
        let mut conn = new_conn().await;
        let mut rs = conn.query().qry("select null").await.unwrap();
        dbg!(&rs.col_defs);
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_query_select_variable() {
        let mut conn = new_conn().await;
        // select variable will return type LongBlob
        let mut rs = conn
            .query()
            .qry("select @master_binlog_checksum")
            .await
            .unwrap();
        dbg!(&rs.col_defs);
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_query_select_error() {
        let mut conn = new_conn().await;
        let fail = conn.query().qry("select * from not_exist_table").await;
        dbg!(fail.unwrap_err());
    }

    #[smol_potat::test]
    async fn test_query_table_and_column() {
        let mut conn = new_conn().await;
        // create database
        conn.query()
            .exec(
                r#"
        CREATE DATABASE IF NOT EXISTS bintest1 DEFAULT CHARACTER SET utf8
        "#,
            )
            .await
            .unwrap();
        // drop table if exists
        conn.query()
            .exec("DROP TABLE IF EXISTS bintest1.typetest")
            .await
            .unwrap();
        // create table
        conn.query()
            .exec(
                r#"
        CREATE TABLE bintest1.typetest (
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
            .unwrap();
        // insert data
        conn.query()
            .exec(
                r#"
        INSERT INTO bintest1.typetest VALUES (
            -100.0,
            -5,
            18,
            -4892,
            32003,
            -159684321,
            2003495865,
            -0.5,
            1.5,
            -0.625,
            1.625,
            '2020-01-01 01:02:03',
            -12948340587434,
            9348578923762,
            -90034,
            87226,
            '2020-12-31',
            '-212:30:40',
            '2012-06-07 15:38:46.092000',
            2021,
            'hello, world', 
            'hello, java',
            b'1000110001100001',
            123456789.22,
            _binary 'hello, tinyblob',
            _binary 'hello, mediumblob',
            _binary 'hello, longblob',
            _binary 'hello, blob',
            'hello, latin1',
            'hello, utf8',
            'hello, binary',
            true,
            'fixed'
        )
        "#,
            )
            .await
            .unwrap();
        // select data
        let mut rs = conn
            .query()
            .qry("SELECT * from bintest1.typetest")
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
            let c29: MyString = extractor.get_named_col(&row, "c29").unwrap().unwrap();
            dbg!(c29);
            let c30: MyString = extractor.get_named_col(&row, "c30").unwrap().unwrap();
            dbg!(c30);
            let c31: MyString = extractor.get_named_col(&row, "c31").unwrap().unwrap();
            dbg!(c31);
            let c32: bool = extractor.get_named_col(&row, "c32").unwrap().unwrap();
            dbg!(c32);
        }
    }

    #[smol_potat::test]
    async fn test_result_set_empty() {
        use futures::StreamExt;
        let mut conn = new_conn().await;
        let mut rs = conn
            .query()
            .qry("select * from mysql.user where 1 = 0")
            .await
            .unwrap();
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_result_set_slave_hosts() {
        use futures::StreamExt;
        let mut conn = new_conn().await;
        let mut rs = conn.query().qry("SHOW SLAVE HOSTS").await.unwrap();
        while let Some(row) = rs.next().await {
            dbg!(row);
        }
    }

    #[smol_potat::test]
    async fn test_result_set_with_extractor() {
        use chrono::NaiveDateTime;

        let mut conn = new_conn().await;
        let mut rs = conn
            .query()
            .qry("select 1 as id, current_timestamp(6) as ts")
            .await
            .unwrap();
        let extractor = rs.extractor();
        while let Some(row) = rs.next().await {
            dbg!(&row);
            let id: u32 = extractor.get_col(&row, 0).unwrap().unwrap();
            dbg!(id);
            let named_id: u32 = extractor.get_named_col(&row, "id").unwrap().unwrap();
            dbg!(named_id);
            let ts: NaiveDateTime = extractor.get_col(&row, 1).unwrap().unwrap();
            dbg!(ts);
            let named_ts: NaiveDateTime = extractor.get_named_col(&row, "ts").unwrap().unwrap();
            dbg!(named_ts);
        }
    }

    #[smol_potat::test]
    async fn test_result_set_with_mapper() {
        use bytes::Buf;
        use mybin_core::col::TextColumnValue;
        use mybin_core::resultset::ResultSetColExtractor;
        #[derive(Debug)]
        struct IdAndName {
            id: u32,
            name: String,
        }

        let mut conn = new_conn().await;
        let rs = conn
            .query()
            .qry("select 1 as id, 'hello' as name")
            .await
            .unwrap();
        let mut rs = rs.map_rows(
            |extractor: &ResultSetColExtractor, row: Vec<TextColumnValue>| {
                let id: u32 = extractor.get_named_col(&row, "id").unwrap().unwrap();
                let name: MyString = extractor.get_named_col(&row, "name").unwrap().unwrap();
                let name = String::from_utf8(Vec::from(name.0.bytes())).unwrap();
                IdAndName { id, name }
            },
        );
        while let Some(obj) = rs.next().await {
            dbg!(obj);
        }
    }
}
