use crc_any::CRCu32;

pub(crate) fn checksum_crc32(bytes: &[u8]) -> u32 {
    let mut hasher = CRCu32::crc32();
    hasher.digest(bytes);
    hasher.get_crc()
}

#[macro_export]
macro_rules! try_from_text_column_value {
    ($($struct_name:ident),*) => {
        $(
            impl $crate::resultset::FromColumnValue<$crate::col::TextColumnValue> for $struct_name {
                fn from_value(value: $crate::col::TextColumnValue) -> Result<Option<Self>> {
                    use bytes::Buf;

                    match value {
                        None => Ok(None),
                        Some(bs) => {
                            let s = std::str::from_utf8(bs.bytes())?;
                            Ok(Some(s.parse()?))
                        }
                    }
                }
            }
        )*
    };
}

#[macro_export]
macro_rules! try_number_from_binary_column_value {
    ($num_type:ident, $($enum_variant:ident => $inter_type:ty),+) => {
        impl $crate::resultset::FromColumnValue<$crate::col::BinaryColumnValue> for $num_type {
            fn from_value(value: BinaryColumnValue) -> Result<Option<Self>> {
                match value {
                    BinaryColumnValue::Null => Ok(None),
                    $(
                        BinaryColumnValue::$enum_variant(v) => Ok(Some(v as $inter_type as $num_type)),
                    )+
                    _ => Err(Error::column_type_mismatch(stringify!($num_type), &value))
                }
            }
        }
    }
}

#[macro_export]
macro_rules! single_byte_cmd {
    ($struct_name:ident, $enum_name: ident) => {
        #[derive(Debug, Clone)]
        pub struct $struct_name {
            pub cmd: $crate::Command,
        }

        impl $struct_name {
            pub fn new() -> Self {
                Self {
                    cmd: $crate::Command::$enum_name,
                }
            }
        }

        impl bytes_parser::WriteToBytes for $struct_name {
            fn write_to(self, out: &mut bytes::BytesMut) -> bytes_parser::error::Result<usize> {
                use bytes_parser::WriteBytesExt;
                out.write_u8(self.cmd.to_byte())
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checksum_iso_3309() {
        assert_eq!(907060870, checksum_crc32(b"hello"));
        assert_eq!(980881731, checksum_crc32(b"world"));
    }
}
