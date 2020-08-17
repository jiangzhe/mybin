#[macro_export]
macro_rules! raw_owned_event {
    ($event_name:ident, $data_name:ident) => {
        pub struct $event_name($crate::event::RawEvent<$data_name>);

        impl std::ops::Deref for $event_name {
            type Target = $crate::event::RawEvent<$data_name>;
        
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
                let (offset, header): (_, $crate::event::header::EventHeader) = self.read_as(offset)?;
                let data_len = if checksum {
                    header.data_len() - 4
                } else {
                    header.data_len()
                };
                let (offset, data) = self[..data_len as usize].read_as(offset)?;
                debug_assert_eq!(offset, data_len);
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((offset, $event_name($crate::event::RawEvent{
                    header,
                    data,
                    crc32,
                })))
            }
        }
    }
}

#[macro_export]
macro_rules! raw_borrowed_event {
    ($event_name:ident, $data_name:ident) => {
        pub struct $event_name<'a>($crate::event::RawEvent<$data_name<'a>>);

        impl<'a> std::ops::Deref for $event_name<'a> {
            type Target = $crate::event::RawEvent<$data_name<'a>>;
        
            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<'a> ReadWithContext<'a, '_, $event_name<'a>> for [u8] {
            type Context = bool;
            
            fn read_with_ctx(&self, offset: usize, checksum: Self::Context) -> Result<(usize, $event_name)> {
                let (offset, header): (_, $crate::event::header::EventHeader) = self.read_as(offset)?;
                let data_len = if checksum {
                    header.data_len() - 4
                } else {
                    header.data_len()
                };
                let (offset, data) = self[..data_len as usize].read_as(offset)?;
                debug_assert_eq!(offset, data_len);
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((offset, $event_name($crate::event::RawEvent{
                    header,
                    data,
                    crc32,
                })))
            }
        }
    }
}

#[macro_export]
macro_rules! raw_empty_event {
    ($event_name:ident) => {
        pub struct $event_name($crate::event::RawEvent<()>);

        impl ReadWithContext<'_, '_, $event_name> for [u8] {
            type Context = bool;
            
            fn read_with_ctx(&self, offset: usize, checksum: Self::Context) -> Result<(usize, $event_name)> {
                let (offset, header): (_, $crate::event::header::EventHeader) = self.read_as(offset)?;
                let data_len = if checksum {
                    header.data_len() - 4
                } else {
                    header.data_len()
                };
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((offset, $event_name($crate::event::RawEvent{
                    header,
                    data: (),
                    crc32,
                })))
            }
        }
    }
}


/// helper function to get indexed bool value from bitmap
#[inline]
pub(crate) fn bitmap_index(bitmap: &[u8], idx: usize) -> bool {
    let bucket = idx >> 3;
    let offset = idx & 7;
    let bit = 1 << offset;
    bit & bitmap[bucket] == bit
}