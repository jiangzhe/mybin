use structopt::StructOpt;

#[derive(Debug, StructOpt)]
pub struct CommandOpt {
    #[structopt(short = "c", default_value = "config.toml")]
    pub config: String,
}
