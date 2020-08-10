use crate::error::Result;
use crypto::digest::Digest;
use crypto::sha1::Sha1;

/// Auth plugin
/// 
/// this trait and impls refers to MySQL 5.1.49 
/// JDBC implementation but simplified
pub trait AuthPlugin {
    
    const NAME: &'static str;

    /// set credentials
    /// 
    /// this method should be called before next() method
    fn set_credential(&mut self, user: &str, password: &str);
    
    /// process authentication handshake data from server
    /// amd optionally produce data to be sent to server
    fn next(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()>;

}

#[derive(Debug)]
pub struct MysqlNativePassword {
    password: Vec<u8>,
}

impl MysqlNativePassword {
    pub fn new() -> Self {
        MysqlNativePassword{password: vec![]}
    }
}

impl AuthPlugin for MysqlNativePassword {

    const NAME: &'static str = "mysql_native_password";

    fn set_credential(&mut self, user: &str, password: &str) {
        self.password = Vec::from(password.as_bytes());
    }

    fn next(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        if self.password.is_empty() {
            // no password
            return Ok(());
        }
        let rst = scramble411(&self.password, input)?;
        output.extend(rst);
        Ok(())
    }
}


fn scramble411(password: &[u8], seed: &[u8]) -> Result<Vec<u8>> {
    let mut hasher = Sha1::new();
    let stage1 = {
        let mut out = vec![];
        hasher.input(password);
        hasher.result(&mut out);
        out
    };
    hasher.reset();
    let stage2 = {
        let mut out = vec![];
        hasher.input(&stage1);
        hasher.result(&mut out);
        out
    };
    hasher.reset();

    let seed_hash = {
        let mut out = vec![];
        hasher.input(seed);
        hasher.input(&stage2);
        hasher.result(&mut out);
        out
    };
    let rst = seed_hash.iter()
        .zip(stage1.iter())
        .map(|(b1, b2)| b1 ^ b2)
        .collect();
    Ok(rst)
}