use serde_derive::*;
use nom::IResult;
use nom::error::ParseError;
use nom::number::streaming::{le_u8, le_u24};
use nom::bytes::streaming::take;

/// MySQL packet
/// 
/// reference: https://dev.mysql.com/doc/internals/en/mysql-packet.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Packet<'a> {
    pub payload_length: u32,
    pub sequence_id: u8,
    pub payload: &'a [u8],
}

/// parse packet
/// 
/// this method requires a fixed input, so may not
/// suitable to parse a real packet from network
pub fn parse_packet<'a, E>(input: &'a [u8]) -> IResult<&'a [u8], Packet<'a>, E>
where
    E: ParseError<&'a [u8]>,
{
    let (input, payload_length) = le_u24(input)?;
    let (input, sequence_id) = le_u8(input)?;
    let (input, payload) = take(payload_length)(input)?;
    Ok((input, Packet{payload_length, sequence_id, payload}))
}

#[cfg(test)]
mod tests {

    const packet_data: &[u8] = include_bytes!("../data/packet.dat");

    use super::*;
    use nom::error::VerboseError;

    #[test]
    fn test_packet() {
        let (input, pkt) = parse_packet::<VerboseError<_>>(packet_data).unwrap();
        assert!(input.is_empty());
        dbg!(pkt);
    }
}