use bitflags::bitflags;
use bytes::{BufMut, BytesMut};
use bytes_parser::error::Result;
use bytes_parser::WriteToBytes;

bitflags! {
    pub struct CapabilityFlags: u32 {
        const LONG_PASSWORD     = 0x0000_0001;
        const FOUND_ROWS        = 0x0000_0002;
        const LONG_FLAG         = 0x0000_0004;
        const CONNECT_WITH_DB   = 0x0000_0008;
        const NO_SCHEMA         = 0x0000_0010;
        const COMPRESS          = 0x0000_0020;
        const ODBC              = 0x0000_0040;
        const LOCAL_FILES       = 0x0000_0080;
        const IGNORE_SPACE      = 0x0000_0100;
        const PROTOCOL_41       = 0x0000_0200;
        const INTERACTIVE       = 0x0000_0400;
        const SSL               = 0x0000_0800;
        const IGNORE_SIGPIPE    = 0x0000_1000;
        const TRANSACTIONS      = 0x0000_2000;
        const RESERVED          = 0x0000_4000;
        const SECURE_CONNECTION = 0x0000_8000;
        const MULTI_STATEMENTS  = 0x0001_0000;
        const MULTI_RESULTS     = 0x0002_0000;
        const PS_MULTI_RESULTS  = 0x0004_0000;
        const PLUGIN_AUTH       = 0x0008_0000;
        const CONNECT_ATTRS     = 0x0010_0000;
        const PLUGIN_AUTH_LENENC_CLIENT_DATA = 0x0020_0000;
        const CAN_HANDLE_EXPIRED_PASSWORDS = 0x0040_0000;
        const SESSION_TRACK     = 0x0080_0000;
        const DEPRECATE_EOF     = 0x0100_0000;
        const SSL_VERITY_SERVER_CERT = 0x4000_0000;
        const REMEMBER_OPTIONS  = 0x8000_0000;
    }
}

impl Default for CapabilityFlags {
    fn default() -> Self {
        Self::empty()
        | CapabilityFlags::LONG_PASSWORD
        | CapabilityFlags::FOUND_ROWS
        | CapabilityFlags::LONG_FLAG
        // | CapabilityFlags::CONNECT_WITH_DB
        // | CapabilityFlags::NO_SCHEMA
        // | CapabilityFlags::COMPRESS
        // | CapabilityFlags::ODBC 
        // | CapabilityFlags::LOCAL_FILES
        // | CapabilityFlags::IGNORE_SPACE
        | CapabilityFlags::PROTOCOL_41
        // | CapabilityFlags::INTERACTIVE 
        // | CapabilityFlags::SSL 
        // | CapabilityFlags::IGNORE_SIGPIPE 
        | CapabilityFlags::TRANSACTIONS
        | CapabilityFlags::RESERVED
        // | CapabilityFlags::SECURE_CONNECTION 
        // | CapabilityFlags::MULTI_STATEMENTS 
        | CapabilityFlags::MULTI_RESULTS
        | CapabilityFlags::PS_MULTI_RESULTS
        | CapabilityFlags::PLUGIN_AUTH
        | CapabilityFlags::CONNECT_ATTRS
        | CapabilityFlags::PLUGIN_AUTH_LENENC_CLIENT_DATA
        // | CapabilityFlags::CAN_HANDLE_EXPIRED_PASSWORDS 
        | CapabilityFlags::SESSION_TRACK
        | CapabilityFlags::DEPRECATE_EOF
        // | CapabilityFlags::SSL_VERITY_SERVER_CERT
        // | CapabilityFlags::REMEMBER_OPTIONS
    }
}

impl WriteToBytes for CapabilityFlags {
    fn write_to(self, out: &mut BytesMut) -> Result<usize> {
        out.put_u32_le(self.bits());
        Ok(4)
    }
}

bitflags! {
    pub struct StatusFlags: u16 {
        const STATUS_IN_TRANS           = 0x0001;
        const STATUS_AUTOCOMMIT         = 0x0002;
        const MORE_RESULTS_EXISTS       = 0x0008;
        const STATUS_NO_GOOD_INDEX_USED = 0x0010;
        const STATUS_NO_INDEX_USED      = 0x0020;
        const STATUS_CURSOR_EXISTS      = 0x0040;
        const STATUS_LAST_ROW_SENT      = 0x0080;
        const STATUS_DB_DROPPED         = 0x0100;
        const STATUS_NO_BACKSLASH_ESCAPES = 0x0200;
        const STATUS_METADATA_CHANGED   = 0x0400;
        const QUERY_WAS_SLOW            = 0x0800;
        const PS_OUT_PARAMS             = 0x1000;
        const STATUS_IN_TRANS_READONLY  = 0x2000;
        const SESSION_STATE_CHANGED     = 0x4000;
    }
}
