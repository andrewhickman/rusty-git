use std::io::Read;

use crate::object::{Header, ParseError, Parser};

pub struct Tag;

impl Tag {
    pub fn parse<R: Read>(mut parser: Parser<R>, header: &Header) -> Result<Self, ParseError> {
        todo!()
    }
}
