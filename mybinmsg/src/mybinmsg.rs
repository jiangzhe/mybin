pub mod cmd_opt;
pub mod config;

use anyhow::{Context, Result};
use async_io::Async;
use cmd_opt::CommandOpt;
use config::{Config, Tcp};
use mybin_async::conn::{Conn, ConnOpts};
use std::fs::File;
use std::io::Read;
use std::net::{TcpStream, ToSocketAddrs};
use structopt::StructOpt;

fn main() -> Result<()> {
    env_logger::init();
    let opt = CommandOpt::from_args();
    let mut conf_file = File::open(&opt.config).context("failed to read config file")?;
    let mut toml_str = String::new();
    conf_file.read_to_string(&mut toml_str)?;
    let conf: Config = toml::from_str(&toml_str)?;

    smol::block_on(async {
        let mut conn = connect_tcp(&conf.connect.tcp().unwrap()).await?;
        let binlog_files = conn.binlog_files().await?;
        for bf in binlog_files {
            println!("{:?}", bf);
        }
        Ok(())
    })
}

async fn connect_tcp(conf: &Tcp) -> Result<Conn<Async<TcpStream>>> {
    let addr = format!("{}:{}", conf.host, conf.port)
        .to_socket_addrs()?
        .next()
        .unwrap();
    let login = ConnOpts {
        username: conf.username.to_owned(),
        password: conf.password.to_owned(),
        database: String::new(),
    };
    let stream = Async::<TcpStream>::connect(addr).await?;
    let mut conn = Conn::new(stream);
    conn.handshake(login).await?;
    Ok(conn)
}
