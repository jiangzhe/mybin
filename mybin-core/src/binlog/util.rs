#[macro_export]
macro_rules! try_from_event {
    ($event_name:ident, $data_name:ident) => {
        impl std::convert::TryFrom<$crate::binlog::Event> for $crate::binlog::RawEvent<$data_name> {
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
    }
}
