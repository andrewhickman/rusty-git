use std::io::BufRead;

use crate::object::{Parser, ParseError, Header};

pub struct Tag;

impl Tag {
    pub fn parse<R: BufRead>(mut parser: Parser<R>, header: &Header) -> Result<Self, ParseError> {
        todo!()
    }
}