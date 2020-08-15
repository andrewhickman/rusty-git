use std::fmt;

use bstr::{BStr, ByteSlice};
use thiserror::Error;

use crate::parse::Parser;

pub struct Blob {
    data: Box<[u8]>,
    pos: usize,
}

#[derive(Debug, Error)]
pub enum ParseBlobError {}

impl Blob {
    pub(in crate::object) fn parse(parser: Parser<Box<[u8]>>) -> Result<Self, ParseBlobError> {
        Ok(Blob {
            pos: parser.pos(),
            data: parser.into_inner(),
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
