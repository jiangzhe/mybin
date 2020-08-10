mod error;
mod number;
mod util;
mod bytes;
mod conn;
mod auth_plugin;

pub use error::*;
pub use number::*;
pub use crate::bytes::*;

#[cfg(test)]
mod tests {

    const protocol: &[u8] = include_bytes!("../data/protocol.dat");

    use super::*;
    use async_net::TcpStream;

    #[smol_potat::test]
    async fn test_mysql_conn_protocol() {
        let reader = &mut protocol;
        let payload_length = reader.read_le_u24().await.unwrap();
        println!("payload_length={}", payload_length);
        let sequence_id = reader.read_u8().await.unwrap();
        println!("sequence_id={}", sequence_id);
        let data = reader.take(payload_length as usize).await.unwrap();
        println!("data={:?}", data);
    }
}