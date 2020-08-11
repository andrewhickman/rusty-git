use std::fmt;
use std::io::Read;

use bstr::{BStr, ByteSlice};

use crate::object::{ParseError, Parser};

pub struct Blob {
    data: Box<[u8]>,
    pos: usize,
}

impl Blob {
    pub fn parse<R: Read>(parser: Parser<R>) -> Result<Self, ParseError> {
        let pos = parser.pos();
        Ok(Blob {
            data: parser.finish(),
            pos,
        })
    }

    pub fn data(&self) -> &BStr {
        self.data[self.pos..].as_bstr()
    }
}

impl fmt::Debug for Blob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Blob").field("data", &self.data()).finish()
    }
}
