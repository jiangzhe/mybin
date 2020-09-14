use structopt::StructOpt;

#[structopt(name = "mybinlog", about = "Utility to process MySQL binlog")]
#[derive(Debug, Clone, StructOpt)]
pub struct Opts {
    #[structopt(short = "h", long, env = "MYBIN_HOST", default_value = "localhost")]
    pub host: String,
    #[structopt(short = "P", long, env = "MYBIN_PORT", default_value = "3306")]
    pub port: String,
    #[structopt(short = "u", long, env = "MYBIN_USERNAME")]
    pub username: String,
    #[structopt(short = "p", long, env = "MYBIN_PASSWORD")]
    pub password: String,
}
