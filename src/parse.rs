//! Utilities for parsing from byte streams

mod buffer;
mod parser;

pub(crate) use self::buffer::Buffer;
pub(crate) use self::parser::Parser;

use std::io;

use thiserror::Error;

use crate::object::ParseIdError;

#[derive(Debug, Error)]
pub(crate) enum Error {
    #[error("expected byte {0:?} was not found")]
    DelimNotFound(u8),
    #[error("unexpected end of file")]
    UnexpectedEof,
    #[error("the file length is invalid")]
    InvalidLength,
    #[error("an object id is malformed")]
    InvalidId(
        #[source]
        #[from]
        ParseIdError,
    ),
    #[error("io error while parsing")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
}
