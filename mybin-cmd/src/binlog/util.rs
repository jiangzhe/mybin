#[macro_export]
macro_rules! raw_event {
    ($event_name:ident) => {
        pub struct $event_name($crate::binlog::RawEvent<()>);

        impl ReadWithContext<'_, '_, $event_name> for [u8] {
            type Context = bool;
            
            fn read_with_ctx(&self, offset: usize, checksum: Self::Context) -> Result<(usize, $event_name)> {
                let (offset, header): (_, $crate::binlog::header::EventHeader) = self.read_from(offset)?;
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((offset, $event_name($crate::binlog::RawEvent{
                    header,
                    data: (),
                    crc32,
                })))
            }
        }
    };
    ($event_name:ident, $data_name:ident) => {
        pub struct $event_name($crate::binlog::RawEvent<$data_name>);

        impl std::ops::Deref for $event_name {
            type Target = $crate::binlog::RawEvent<$data_name>;
        
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }
        
        impl std::ops::DerefMut for $event_name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.0
            }
        }

        impl ReadWithContext<'_, '_, $event_name> for [u8] {
            type Context = bool;
            
            fn read_with_ctx(&self, offset: usize, checksum: Self::Context) -> Result<(usize, $event_name)> {
                let (offset, header): (_, $crate::binlog::header::EventHeader) = self.read_from(offset)?;
                let data_len = if checksum {
                    header.data_len() - 4
                } else {
                    header.data_len()
                };
                let (offset, data) = self[..data_len as usize].read_from(offset)?;
                debug_assert_eq!(offset, data_len as usize);
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((offset, $event_name($crate::binlog::RawEvent{
                    header,
                    data,
                    crc32,
                })))
            }
        }
    };
    ($event_name:ident, $data_name:ident, $lt:tt) => {
        pub struct $event_name<$lt>($crate::binlog::RawEvent<$data_name<$lt>>);

        impl<$lt> std::ops::Deref for $event_name<$lt> {
            type Target = $crate::binlog::RawEvent<$data_name<'a>>;
        
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<$lt> ReadWithContext<$lt, '_, $event_name<$lt>> for [u8] {
            type Context = bool;
            
            fn read_with_ctx(&self, offset: usize, checksum: Self::Context) -> Result<(usize, $event_name)> {
                let (offset, header): (_, $crate::binlog::header::EventHeader) = self.read_from(offset)?;
                let data_len = if checksum {
                    header.data_len() - 4
                } else {
                    header.data_len()
                };
                let (offset, data) = self[..data_len as usize].read_from(offset)?;
                debug_assert_eq!(offset, data_len as usize);
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((offset, $event_name($crate::binlog::RawEvent{
                    header,
                    data,
                    crc32,
                })))
            }
        }
    }
}
