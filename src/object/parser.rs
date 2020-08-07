use std::io::{self, BufRead};
use std::str::{self, FromStr};

use thiserror::Error;

use crate::object::{Blob, Commit, Object, Tag, Tree};

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
    #[error("object size is too large")]
    InvalidLength,
    #[error("object size header doesn't match actual size")]
    LengthMismatch,
    #[error("io error reading object")]
    Io(
        #[from]
        #[source]
        io::Error,
    ),
}

#[derive(Debug, PartialEq)]
pub enum ObjectKind {
    Commit,
    Tree,
    Blob,
    Tag,
}

#[derive(Debug, PartialEq)]
pub struct Header {
    pub kind: ObjectKind,
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
        match header.kind {
            ObjectKind::Blob => Blob::parse(self, &header).map(Object::Blob),
            ObjectKind::Commit => Commit::parse(self, &header).map(Object::Commit),
            ObjectKind::Tree => Tree::parse(self, &header).map(Object::Tree),
            ObjectKind::Tag => Tag::parse(self, &header).map(Object::Tag),
        }
    }

    fn parse_header(&mut self) -> Result<Header, ParseError> {
        let kind = self.consume_until(b' ')?.ok_or(ParseError::InvalidHeader)?;
        let kind = match kind {
            b"commit" => ObjectKind::Commit,
            b"tree" => ObjectKind::Tree,
            b"blob" => ObjectKind::Blob,
            b"tag" => ObjectKind::Tag,
            _ => return Err(ParseError::InvalidLength),
        };

        let len = self
            .consume_until(b'\0')?
            .ok_or(ParseError::InvalidHeader)?;
        let len = str::from_utf8(&len).map_err(|_| ParseError::InvalidHeader)?;
        let len = usize::from_str(&len).map_err(|_| ParseError::InvalidLength)?;

        Ok(Header { kind, len })
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
    assert_eq!(
        parser.parse_header().unwrap(),
        Header {
            kind: ObjectKind::Tree,
            len: 3,
        }
    );
    assert_eq!(parser.read_to_end().unwrap(), b"abc");
}
