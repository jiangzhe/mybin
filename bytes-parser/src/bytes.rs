use crate::error::Result;

pub trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait WriteToBytes {

    /// write anything that impls ToBytes
    fn write_to_bytes<T: ToBytes>(&mut self, tb: T) -> Result<()>;
}

impl WriteToBytes for Vec<u8> {

    fn write_to_bytes<T: ToBytes>(&mut self, tb: T) -> Result<()> {
        self.extend(tb.to_bytes());
        Ok(())
    }
}

