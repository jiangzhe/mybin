mod opts;

use anyhow::Result;
use async_net::TcpStream;
use mybin_async::conn::{Conn, ConnOpts};
use mybin_core::binlog::Event;
use mybin_core::col::{ColumnDefinition, ColumnMetas};
use mybin_core::sql::{self, SqlCollection};
use opts::Opts;
use smol::stream::StreamExt;
use smol_str::SmolStr;
use std::collections::HashMap;
use structopt::StructOpt;

fn main() -> Result<()> {
    smol::block_on(async {
        env_logger::init();
        let opts = Opts::from_args();
        let addr = format!("{}:{}", opts.host, opts.port);
        // main connection to fetch binlogs
        let mut conn = Conn::new(TcpStream::connect(&addr).await?);
        let conn_opts = ConnOpts {
            username: opts.username,
            password: opts.password,
            database: String::new(),
        };
        conn.handshake(conn_opts.clone()).await?;
        let files = conn.binlog_files().await?;
        println!("{:#?}", files);
        // helper connection to fetch column names
        let mut helper = Conn::new(TcpStream::connect(&addr).await?);
        helper.handshake(conn_opts).await?;
        // start binlog stream
        let mut binlog_stream = conn.binlog()
            .binlog_filename(files.last().as_ref().unwrap().filename.clone())
            .binlog_pos(4)
            .non_block(true).stream().await?;
        let mut tbls = HashMap::new();
        while let Some(event) = binlog_stream.next().await {
            let event = event?;
            dbg!(&event);
            match event {
                Event::TableMapEvent(raw) => {
                    let tbl_id = raw.data.table_id;
                    if !tbls.contains_key(&tbl_id) {
                        let tm = raw.data.into_table_map()?;
                        dbg!(&tm);
                        helper.init_db(&*tm.schema_name).await?;
                        let col_defs = helper.field_list(&*tm.table_name, "%").await?;
                        tbls.insert(
                            tbl_id,
                            TableMeta {
                                db: tm.schema_name,
                                tbl: tm.table_name,
                                col_metas: tm.col_metas,
                                col_defs,
                            },
                        );
                    }
                }
                Event::DeleteRowsEventV2(raw) => {
                    let tbl_id = raw.data.table_id;
                    if let Some(tm) = tbls.get(&tbl_id) {
                        let rows = raw.data.into_rows(&tm.col_metas)?;
                        let del_sql =
                            sql::delete(tm.db.clone(), tm.tbl.clone(), rows, &tm.col_defs);
                        println!("{:#?}", del_sql);
                        for s in del_sql.sql_list() {
                            println!("{}", s);
                        }
                    }
                }
                Event::WriteRowsEventV2(raw) => {
                    let tbl_id = raw.data.table_id;
                    if let Some(tm) = tbls.get(&tbl_id) {
                        let rows = raw.data.into_rows(&tm.col_metas)?;
                        let ins_sql =
                            sql::insert(tm.db.clone(), tm.tbl.clone(), rows, &tm.col_defs);
                        println!("{:#?}", ins_sql);
                        for s in ins_sql.sql_list() {
                            println!("{}", s);
                        }
                    }
                }
                _ => (),
            }
        }
        Ok(())
    })
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub db: SmolStr,
    pub tbl: SmolStr,
    pub col_metas: ColumnMetas,
    pub col_defs: Vec<ColumnDefinition>,
}
