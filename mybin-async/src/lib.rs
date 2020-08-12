mod auth_plugin;
mod bytes;
mod conn;
mod error;
mod number;
mod cmd;
mod util;

pub use crate::bytes::*;
pub use error::*;
pub use number::*;

#[cfg(test)]
mod tests {

    const PROTOCOL: &[u8] = include_bytes!("../data/protocol.dat");

    use super::*;

    #[smol_potat::test]
    async fn test_mysql_conn_protocol() {
        let reader = &mut PROTOCOL;
        let payload_length = reader.read_le_u24().await.unwrap();
        println!("payload_length={}", payload_length);
        let sequence_id = reader.read_u8().await.unwrap();
        println!("sequence_id={}", sequence_id);
        let data = reader.take(payload_length as usize).await.unwrap();
        println!("data={:?}", data);
    }
}
