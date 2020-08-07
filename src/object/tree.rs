use std::io::BufRead;

use crate::object::{Header, ParseError, Parser};

pub struct Tree;

impl Tree {
    pub fn parse<R: BufRead>(mut parser: Parser<R>, header: &Header) -> Result<Self, ParseError> {
        todo!()
    }
}
