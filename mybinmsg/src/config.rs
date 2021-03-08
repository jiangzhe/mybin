use serde_derive::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    #[serde(flatten)]
    pub connect: Connect,
    pub subscribe: Subscribe,
    pub notify: Notify,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Connect {
    #[serde(rename = "tcp")]
    Tcp(Tcp),
    #[serde(rename = "unix_socket")]
    UnixSocket(UnixSocket),
}

impl Connect {
    pub fn tcp(self) -> Option<Tcp> {
        match self {
            Connect::Tcp(t) => Some(t),
            _ => None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Tcp {
    pub host: String,
    pub port: u32,
    pub username: String,
    pub password: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct UnixSocket {
    pub path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Subscribe {
    database_filter: Option<String>,
    table_filter: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Notify {
    #[serde(rename = "type")]
    pub ty: NotifyType,
    #[serde(flatten)]
    pub execution: NotifyExecution,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum NotifyType {
    #[serde(rename = "http")]
    Http,
    #[serde(rename = "stdout")]
    Stdout,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct NotifyExecution {
    // used for http notify
    pub url: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_unix_socket() {
        let toml_str = r#"
        [unix_socket]
        path = "/path/to/tmp.sock"
        [subscribe]
        database_filter = ".*"
        [notify]
        type = "stdout"
        "#;
        let config: Config = toml::from_str(toml_str).unwrap();
        println!("{:?}", config);
    }
}
