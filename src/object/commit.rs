use std::io::Read;

use crate::object::{ParseError, Parser};

#[derive(Debug)]
pub struct Commit;

impl Commit {
    pub fn parse<R: Read>(_parser: Parser<R>) -> Result<Self, ParseError> {
        todo!()
    }
}
