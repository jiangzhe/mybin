#[macro_export]
macro_rules! raw_event {
    ($event_name:ident) => {
        #[derive(Debug, Clone)]
        pub struct $event_name($crate::binlog::RawEvent<()>);

        impl bytes_parser::ReadWithContext<'_, '_, $event_name> for [u8] {
            type Context = bool;

            fn read_with_ctx(
                &self,
                offset: usize,
                checksum: Self::Context,
            ) -> Result<(usize, $event_name)> {
                let (offset, header): (_, $crate::binlog::header::EventHeader) =
                    self.read_from(offset)?;
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((
                    offset,
                    $event_name($crate::binlog::RawEvent {
                        header,
                        data: (),
                        crc32,
                    }),
                ))
            }
        }

        impl $crate::binlog::HasCrc32 for $event_name {
            fn crc32(&self) -> u32 {
                self.0.crc32
            }
        }
    };
    ($event_name:ident, $data_name:ident) => {
        #[derive(Debug, Clone)]
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

        impl bytes_parser::ReadWithContext<'_, '_, $event_name> for [u8] {
            type Context = bool;

            fn read_with_ctx(
                &self,
                offset: usize,
                checksum: Self::Context,
            ) -> Result<(usize, $event_name)> {
                let (offset, header): (_, $crate::binlog::header::EventHeader) =
                    self.read_from(offset)?;
                let data_len = if checksum {
                    header.data_len() - 4
                } else {
                    header.data_len()
                };
                let (offset, data) = self.take_len(offset, data_len as usize)?;
                let (_, data) = data.read_from(0)?;
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((
                    offset,
                    $event_name($crate::binlog::RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
        }

        impl $crate::binlog::HasCrc32 for $event_name {
            fn crc32(&self) -> u32 {
                self.0.crc32
            }
        }
    };
    ($event_name:ident, $data_name:ident, $lt:tt) => {
        #[derive(Debug, Clone)]
        pub struct $event_name<$lt>($crate::binlog::RawEvent<$data_name<$lt>>);

        impl<$lt> std::ops::Deref for $event_name<$lt> {
            type Target = $crate::binlog::RawEvent<$data_name<'a>>;

            fn deref(&self) -> &Self::Target {
                &self.0
            }
        }

        impl<$lt> ReadWithContext<$lt, '_, $event_name<$lt>> for [u8] {
            type Context = bool;

            fn read_with_ctx(
                &self,
                offset: usize,
                checksum: Self::Context,
            ) -> Result<(usize, $event_name)> {
                let (offset, header): (_, $crate::binlog::header::EventHeader) =
                    self.read_from(offset)?;
                let data_len = if checksum {
                    header.data_len() - 4
                } else {
                    header.data_len()
                };
                let (offset, data) = self.take_len(offset, data_len as usize)?;
                let (_, data) = data.read_from(0)?;
                let (offset, crc32) = if checksum {
                    self.read_le_u32(offset)?
                } else {
                    (offset, 0)
                };
                Ok((
                    offset,
                    $event_name($crate::binlog::RawEvent {
                        header,
                        data,
                        crc32,
                    }),
                ))
            }
        }

        impl $crate::binlog::HasCrc32 for $event_name<'_> {
            fn crc32(&self) -> u32 {
                self.0.crc32
            }
        }
    };
}

#[macro_export]
macro_rules! try_from_event {
    ($event_name:ident) => {
        impl std::convert::TryFrom<$crate::binlog::Event<'_>> for $event_name {
            type Error = $crate::error::Error;
            fn try_from(src: $crate::binlog::Event) -> $crate::error::Result<Self> {
                match src {
                    $crate::binlog::Event::$event_name(inner) => Ok(inner),
                    other => Err($crate::error::Error::BinlogEventError(format!(
                        "invalid conversion from {:?}",
                        other
                    ))),
                }
            }
        }
    };
    ($event_name:ident, $lt:tt) => {
        impl<$lt> std::convert::TryFrom<$crate::binlog::Event<$lt>> for $event_name<$lt> {
            type Error = $crate::error::Error;
            fn try_from(src: $crate::binlog::Event<$lt>) -> $crate::error::Result<Self> {
                match src {
                    $crate::binlog::Event::$event_name(inner) => Ok(inner),
                    other => Err($crate::error::Error::BinlogEventError(format!(
                        "invalid conversion from {:?}",
                        other
                    ))),
                }
            }
        }
    };
}
