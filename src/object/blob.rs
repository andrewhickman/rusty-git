use std::io::Read;

use crate::object::{Header, ParseError, Parser};

pub struct Blob {
    data: Vec<u8>,
    pos: usize,
}

impl Blob {
    pub fn parse<R: Read>(mut parser: Parser<R>, header: &Header) -> Result<Self, ParseError> {
        let pos = parser.pos();
        Ok(Blob {
            data: parser.finish(),
            pos,
        })
    }

    pub fn data(&self) -> &[u8] {
        &self.data[self.pos..]
    }
}
