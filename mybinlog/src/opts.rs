use structopt::StructOpt;

#[structopt(name = "mybinlog", about = "Utility to process MySQL binlog")]
#[derive(Debug, Clone, StructOpt)]
pub struct Opts {
    #[structopt(short = "h", long, env = "MYBIN_HOST")]
    pub host: String,
    #[structopt(short = "P", long, env = "MYBIN_PORT")]
    pub port: String,
    #[structopt(short = "u", long, env = "MYBIN_USERNAME")]
    pub username: String,
    #[structopt(short = "p", long, env = "MYBIN_PASSWORD")]
    pub password: String,
    #[structopt(subcommand)]
    pub cmd: Command,
}


#[derive(Debug, Clone, StructOpt)]
pub enum Command {
    Dml {
        #[structopt(short, long)]
        filename: String,
        #[structopt(short, long)]
        until_now: bool,
        #[structopt(short, long)]
        database_filter: Option<String>,
        #[structopt(short, long)]
        table_filter: Option<String>,
        #[structopt(short, long)]
        block: bool,
    },
    List,
}