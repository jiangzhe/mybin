//! essential parsing of bytes
//! 
//! inspired by nom parser combinator (https://github.com/Geal/nom)
pub mod error;
pub mod number;
pub mod number_async;
pub mod my;
pub mod util;
pub mod bytes;

/// global empty byte array as place holder
pub const EMPTY_BYTE_ARRAY: [u8;0] = [];
