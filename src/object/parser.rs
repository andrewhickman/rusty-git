use std::io::{self, Read};
use std::str::{self, FromStr};

use memchr::memchr;
use thiserror::Error;

use crate::object::{Blob, Commit, Object, Tag, Tree};

const MAX_HEADER_LEN: usize = 28;

pub struct Parser<R> {
    buffer: Vec<u8>,
    pos: usize,
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

impl<R: Read> Parser<R> {
    pub fn new(reader: R) -> Self {
        Parser {
            buffer: Vec::new(),
            reader,
            pos: 0,
        }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn parse(mut self) -> Result<Object, ParseError> {
        let header = self.parse_header()?;
        let start = self.pos;
        let end = start
            .checked_add(header.len)
            .ok_or(ParseError::InvalidLength)?;

        self.buffer.reserve(end.saturating_sub(self.buffer.len()));
        self.reader.read_to_end(&mut self.buffer)?;

        if self.buffer.len() != end {
            return Err(ParseError::InvalidLength);
        }

        match header.kind {
            ObjectKind::Blob => Blob::parse(self, &header).map(Object::Blob),
            ObjectKind::Commit => Commit::parse(self, &header).map(Object::Commit),
            ObjectKind::Tree => Tree::parse(self, &header).map(Object::Tree),
            ObjectKind::Tag => Tag::parse(self, &header).map(Object::Tag),
        }
    }

    pub fn parse_header(&mut self) -> Result<Header, ParseError> {
        debug_assert_eq!(self.pos, 0);

        read_max(&mut self.reader, &mut self.buffer, self.pos, MAX_HEADER_LEN)?;

        let kind = self.consume_until(b' ').ok_or(ParseError::InvalidHeader)?;
        let kind = match kind {
            b"commit" => ObjectKind::Commit,
            b"tree" => ObjectKind::Tree,
            b"blob" => ObjectKind::Blob,
            b"tag" => ObjectKind::Tag,
            _ => return Err(ParseError::InvalidLength),
        };

        let len = self
            .consume_until(b'\0')
            .ok_or(ParseError::InvalidHeader)?;
        let len = str::from_utf8(&len).map_err(|_| ParseError::InvalidHeader)?;
        let len = usize::from_str(&len).map_err(|_| ParseError::InvalidLength)?;

        Ok(Header { kind, len })
    }

    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    pub fn consume_until<'a>(&'a mut self, ch: u8) -> Option<&'a [u8]> {
        match memchr(ch, self.buffer.as_slice()) {
            Some(ch_pos) => {
                let result = &self.buffer[self.pos..ch_pos];
                self.pos = ch_pos + 1;
                Some(result)
            }
            None => None,
        }
    }
}

/// Read at most `buf.len()` bytes from `reader`.
fn read_max(reader: &mut impl Read, buf: &mut Vec<u8>, mut pos: usize, max: usize) -> io::Result<()> {
    buf.resize(pos + max, 0);
    while pos != buf.len() {
        match reader.read(&mut buf[pos..]) {
            Ok(0) => {
                buf.truncate(pos);
                return Ok(());
            }
            Ok(n) => {
                pos += n;
            }
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }

    return Ok(());
}

#[test]
fn test_max_header_len() {
    assert_eq!(MAX_HEADER_LEN, format!("commit {}\0", u64::MAX).len());
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
}
