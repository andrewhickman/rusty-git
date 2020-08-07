use std::str::{self, FromStr};
use std::io::{self, BufRead};

use thiserror::Error;

use crate::object::{Object, Blob, Tag, Commit, Tree};

pub struct Parser<R> {
    buffer: Vec<u8>,
    reader: R,
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("unknown object type")]
    UnknownType,
    #[error("object header is malformed")]
    InvalidHeader,
    #[error("object size is invalid")]
    InvalidSize,
    #[error("io error reading object")]
    Io(#[from]#[source] io::Error),
}

#[derive(Debug, PartialEq)]
pub enum ObjectType {
    Commit,
    Tree,
    Blob,
    Tag,
}

#[derive(Debug, PartialEq)]
pub struct Header {
    pub ty: ObjectType,
    pub len: usize,
}

impl<R: BufRead> Parser<R> {
    pub fn new(reader: R) -> Self {
        Parser {
            buffer: Vec::new(),
            reader,
        }
    }

    pub fn parse(mut self) -> Result<Object, ParseError> {
        let header = self.parse_header()?;
        match header.ty {
            ObjectType::Blob => Blob::parse(self, &header).map(Object::Blob),
            ObjectType::Commit => Commit::parse(self, &header).map(Object::Commit),
            ObjectType::Tree => Tree::parse(self, &header).map(Object::Tree),
            ObjectType::Tag => Tag::parse(self, &header).map(Object::Tag),
        }
    }

    fn parse_header(&mut self) -> Result<Header, ParseError> {
        let type_bytes = self.consume_until(b' ')?.ok_or(ParseError::InvalidHeader)?;
        let ty = match type_bytes {
            b"commit" => ObjectType::Commit,
            b"tree" => ObjectType::Tree,
            b"blob" => ObjectType::Blob,
            b"tag" => ObjectType::Tag,
            _ => return Err(ParseError::InvalidSize),
        };

        let size_bytes = self.consume_until(b'\0')?.ok_or(ParseError::InvalidHeader)?;
        let size_str = str::from_utf8(&size_bytes).map_err(|_| ParseError::InvalidHeader)?;
        let len = usize::from_str(&size_str).map_err(|_| ParseError::InvalidSize)?;

        Ok(Header { ty, len })
    }

    pub fn reserve(&mut self, len: usize) {
        self.buffer.reserve(len.saturating_sub(self.buffer.len()));
    }

    pub fn read_to_end(mut self) -> io::Result<Vec<u8>> {
        self.buffer.clear();

        self.reader.read_to_end(&mut self.buffer)?;
        Ok(self.buffer)
    }

    pub fn consume_until<'a>(&'a mut self, delim: u8) -> io::Result<Option<&'a [u8]>> {
        self.buffer.clear();

        self.reader.read_until(delim, &mut self.buffer)?;
        if self.buffer.ends_with(&[delim]) {
            Ok(Some(&self.buffer[..(self.buffer.len() - 1)]))
        } else {
            Ok(None)
        }
    }
}

#[test]
fn test_parse_header() {
    let object = b"tree 3\0abc";

    let mut parser = Parser::new(io::Cursor::new(object));
    assert_eq!(parser.parse_header().unwrap(), Header {
        ty: ObjectType::Tree,
        len: 3,
    });
    assert_eq!(parser.read_to_end().unwrap(), b"abc");
}