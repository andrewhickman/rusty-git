use std::io::BufRead;

use crate::object::{Parser, ParseError, Header};

pub struct Blob {
    data: Vec<u8>,
}

impl Blob {
    pub fn parse<R: BufRead>(mut parser: Parser<R>, header: &Header) -> Result<Self, ParseError> {
        parser.reserve(header.len);
        Ok(Blob {
            data: parser.read_to_end()?
        })
    }
}