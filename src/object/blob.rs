use std::io::BufRead;

use crate::object::{Header, ParseError, Parser};

pub struct Blob {
    data: Vec<u8>,
}

impl Blob {
    pub fn parse<R: BufRead>(mut parser: Parser<R>, header: &Header) -> Result<Self, ParseError> {
        parser.reserve(header.len);

        let data = parser.read_to_end()?;
        if data.len() != header.len {
            return Err(ParseError::InvalidLength)
        }

        Ok(Blob {
            data,
        })
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }
}
