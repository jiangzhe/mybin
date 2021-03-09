use crate::error::{Error, Result};
use crypto::digest::Digest;
use crypto::sha1::Sha1;
use crypto::sha2::Sha256;

/// Auth plugin
///
/// this trait and impls refers to MySQL 5.1.49
/// JDBC implementation but simplified
pub trait AuthPlugin {
    /// name of this plugin
    fn name(&self) -> &str;

    /// set credentials
    ///
    /// this method should be called before next() method
    fn set_credential(&mut self, username: &str, password: &str);

    /// process authentication handshake data from server
    /// and optionally produce data to be sent to server
    fn next(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()>;
}

/// implementation of mysql_native_password
#[derive(Debug)]
pub struct MysqlNativePassword {
    password: Vec<u8>,
}

impl MysqlNativePassword {
    pub fn new() -> Self {
        MysqlNativePassword { password: vec![] }
    }
}

impl AuthPlugin for MysqlNativePassword {
    fn name(&self) -> &'static str {
        "mysql_native_password"
    }

    fn set_credential(&mut self, _username: &str, password: &str) {
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
        let mut out = vec![0u8; 20];
        hasher.input(password);
        hasher.result(&mut out);
        out
    };
    hasher.reset();
    let stage2 = {
        let mut out = vec![0u8; 20];
        hasher.input(&stage1);
        hasher.result(&mut out);
        out
    };
    hasher.reset();

    let seed_hash = {
        let mut out = vec![0u8; 20];
        hasher.input(seed);
        hasher.input(&stage2);
        hasher.result(&mut out);
        out
    };
    let rst = seed_hash
        .iter()
        .zip(stage1.iter())
        .map(|(b1, b2)| b1 ^ b2)
        .collect();
    Ok(rst)
}

/// implementation of caching_sha2_password
#[derive(Debug)]
pub struct CachingSha2Password {
    password: Vec<u8>,
    seed: Vec<u8>,
    stage: CachingSha2Stage,
    ssl: bool,
    pubkey_requested: bool,
}

impl CachingSha2Password {
    pub fn with_ssl(ssl: bool) -> Self {
        CachingSha2Password {
            password: vec![],
            seed: vec![],
            stage: CachingSha2Stage::FastAuthSendScramble,
            ssl,
            pubkey_requested: false,
        }
    }
}

impl AuthPlugin for CachingSha2Password {
    fn name(&self) -> &str {
        "caching_sha2_password"
    }

    fn set_credential(&mut self, username: &str, password: &str) {
        self.password = Vec::from(password.as_bytes());
    }

    fn next(&mut self, input: &[u8], output: &mut Vec<u8>) -> Result<()> {
        if self.password.is_empty() {
            return Ok(());
        }
        match self.stage {
            CachingSha2Stage::FastAuthSendScramble => {
                let resp = scramble_caching_sha2(&self.password, &self.seed)?;
                output.extend(resp);
                self.stage = CachingSha2Stage::FastAuthReadResult;
                return Ok(());
            }
            CachingSha2Stage::FastAuthReadResult => {
                if input.is_empty() {
                    return Err(Error::CustomError("empty fast auth result".to_owned()));
                }
                match input[0] {
                    3 => {
                        self.stage = CachingSha2Stage::FastAuthComplete;
                        return Ok(());
                    }
                    4 => self.stage = CachingSha2Stage::FullAuth,
                    _ => {
                        return Err(Error::CustomError(
                            "unknown server response after fast auth".to_owned(),
                        ))
                    }
                }
            }
            _ => (),
        }
        // full auth process
        if self.ssl {
            // ssl established, just send password in plain text
            output.extend_from_slice(&self.password);
            output.push(0);
            return Ok(());
        }
        // todo: support server public key stored locally
        // todo: make allowPublicKeyRetrieval a property of connection string
        // todo: RSA/ECB/OAEPWithSHA-1AndMGF1Padding?
        Err(Error::CustomError("full auth unimplemented".to_owned()))
    }
}

/// Scrambling for caching_sha2_password plugin
///
/// Scramble = XOR(SHA2(password), SHA2(SHA2(SHA2(password)), Nonce))
fn scramble_caching_sha2(password: &[u8], seed: &[u8]) -> Result<Vec<u8>> {
    let mut hasher = Sha256::new();
    let mut dig1 = vec![0u8; 32];
    let mut dig2 = vec![0u8; 32];
    let mut scramble1 = vec![0u8; 32];
    // SHA2(src) => dig1
    hasher.input(password);
    hasher.result(&mut dig1);
    hasher.reset();

    // SHA2(dig1) => dig2
    hasher.input(&dig1);
    hasher.result(&mut dig2);
    hasher.reset();

    // SHA2(dig2, m_rnd) => scramble1
    hasher.input(&dig2);
    hasher.input(seed);
    hasher.result(&mut scramble1);

    let rst = dig1
        .iter()
        .zip(scramble1.iter())
        .map(|(b1, b2)| b1 ^ b2)
        .collect();
    Ok(rst)
}

#[derive(Debug, Clone, Copy)]
enum CachingSha2Stage {
    FastAuthSendScramble,
    FastAuthReadResult,
    FastAuthComplete,
    FullAuth,
}

#[cfg(test)]
mod tests {

    #[test]
    fn test_sha1() {
        use crypto::digest::Digest;
        use crypto::sha1::Sha1;
        let mut hasher = Sha1::new();
        hasher.input_str("hello");
        let hex = hasher.result_str();
        println!("{}", hex);
    }
}
