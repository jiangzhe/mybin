use crc_any::CRCu32;

pub struct Checksum(CRCu32);

impl Checksum {
    pub fn new() -> Self {
        Checksum(CRCu32::crc32())
    }

    pub fn checksum(&mut self, bytes: &[u8]) -> u32 {
        // self.0.reset();
        self.0 = CRCu32::crc32();
        self.0.digest(bytes);
        self.0.get_crc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_iso_3309() {
        let mut cs = Checksum::new();
        assert_eq!(907060870, cs.checksum(b"hello"));
        assert_eq!(980881731, cs.checksum(b"world"));
    }
}
