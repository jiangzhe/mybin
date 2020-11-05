mod opts;

use anyhow::Result;
use async_net::TcpStream;
use mybin_async::conn::{Conn, ConnOpts};
use mybin_core::binlog::Event;
use mybin_core::col::{ColumnDefinition, ColumnMetas};
use mybin_core::sql::{self, SqlCollection};
use opts::{Command, Opts};
// use smol::stream::StreamExt;
use regex::Regex;
use smol_str::SmolStr;
use std::collections::{HashMap, HashSet};
use structopt::StructOpt;

fn main() -> Result<()> {
    smol::block_on(async {
        env_logger::init();
        let opts = Opts::from_args();
        exec(&opts).await
    })
}

async fn exec(opts: &Opts) -> Result<()> {
    match &opts.cmd {
        Command::List => {
            let mut conn = connect(&opts).await?;
            let files = conn.binlog_files().await?;
            println!("{:#?}", files);
        }
        Command::Dml {
            filename,
            until_now,
            database_filter,
            table_filter,
            block,
            limit,
        } => {
            // helper connection to fetch column names
            let conn = connect(&opts).await?;
            let helper = connect(&opts).await?;
            let database_filter = if let Some(s) = database_filter {
                Some(Regex::new(s)?)
            } else {
                None
            };
            let table_filter = if let Some(s) = table_filter {
                Some(Regex::new(s)?)
            } else {
                None
            };
            print_dmls(
                conn,
                filename,
                *until_now,
                database_filter,
                table_filter,
                !block,
                *limit,
                helper,
            )
            .await?;
        }
    }
    Ok(())
}

async fn connect(opts: &Opts) -> Result<Conn<TcpStream>> {
    let addr = format!("{}:{}", opts.host, opts.port);
    // main connection to fetch binlogs
    let mut conn = Conn::new(TcpStream::connect(&addr).await?);
    let conn_opts = ConnOpts {
        username: opts.username.to_owned(),
        password: opts.password.to_owned(),
        database: String::new(),
    };
    conn.handshake(conn_opts.clone()).await?;
    Ok(conn)
}

async fn print_dmls(
    mut conn: Conn<TcpStream>,
    filename: &str,
    until_now: bool,
    database_filter: Option<Regex>,
    table_filter: Option<Regex>,
    non_block: bool,
    limit: usize,
    mut helper: Conn<TcpStream>,
) -> Result<()> {
    // start binlog stream
    let mut binlog_stream = conn
        .binlog()
        .binlog_filename(filename)
        .binlog_pos(4)
        .non_block(non_block)
        .request_stream()
        .await?;
    let mut tbls = HashMap::new();
    let mut skip_tbls = HashSet::new();
    let mut n_rows = 0usize;
    'outer: while let Some(event) = binlog_stream.next_event().await? {
        match event {
            Event::TableMapEvent(raw) => {
                let tbl_id = raw.data.table_id;
                if !tbls.contains_key(&tbl_id) && !skip_tbls.contains(&tbl_id) {
                    let tm = raw.data.into_table_map()?;
                    if let Some(re) = database_filter.as_ref() {
                        if !re.is_match(&tm.schema_name) {
                            skip_tbls.insert(tbl_id);
                            continue;
                        }
                    }
                    if let Some(re) = table_filter.as_ref() {
                        if !re.is_match(&tm.table_name) {
                            skip_tbls.insert(tbl_id);
                            continue;
                        }
                    }
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
                if skip_tbls.contains(&tbl_id) {
                    continue;
                }
                if let Some(tm) = tbls.get(&tbl_id) {
                    let rows = raw.data.into_rows(&tm.col_metas)?;
                    let del_sql = sql::delete(tm.db.clone(), tm.tbl.clone(), rows, &tm.col_defs);
                    for s in del_sql.sql_list() {
                        println!("{}", s);
                        n_rows += 1;
                        if n_rows >= limit {
                            break 'outer;
                        }
                    }
                }
            }
            Event::WriteRowsEventV2(raw) => {
                let tbl_id = raw.data.table_id;
                if skip_tbls.contains(&tbl_id) {
                    continue;
                }
                if let Some(tm) = tbls.get(&tbl_id) {
                    let rows = raw.data.into_rows(&tm.col_metas)?;
                    let ins_sql = sql::insert(tm.db.clone(), tm.tbl.clone(), rows, &tm.col_defs);
                    for s in ins_sql.sql_list() {
                        println!("{}", s);
                        n_rows += 1;
                        if n_rows >= limit {
                            break 'outer;
                        }
                    }
                }
            }
            Event::RotateEvent(_) => {
                // when log file rotated, the table id cache must be reset
                tbls.clear();
                skip_tbls.clear();
                if !until_now {
                    break;
                }
            }
            evt @ Event::HeartbeatLogEvent(_) => {
                eprintln!("{:#?}", evt);
            }
            _ => (),
        }
    }
    Ok(())
}

#[derive(Debug, Clone)]
pub struct TableMeta {
    pub db: SmolStr,
    pub tbl: SmolStr,
    pub col_metas: ColumnMetas,
    pub col_defs: Vec<ColumnDefinition>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[smol_potat::test]
    async fn test_list_log_files() {
        let opts = new_opts();
        exec(&opts).await.unwrap();
    }

    #[smol_potat::test]
    async fn test_print_dmls() {
        let mut opts = new_opts();
        opts.cmd = Command::Dml {
            filename: String::from("mysql-bin.000001"),
            until_now: true,
            database_filter: None,
            table_filter: None,
            block: false,
            limit: 100,
        };
        exec(&opts).await.unwrap();
    }

    fn new_opts() -> Opts {
        Opts {
            host: String::from("127.0.0.1"),
            port: String::from("13306"),
            username: String::from("root"),
            password: String::from("password"),
            cmd: Command::List,
        }
    }
}
