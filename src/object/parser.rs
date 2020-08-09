use std::io::{self, Read};
use std::str::{self, FromStr};

use memchr::memchr;
use thiserror::Error;

use crate::object::{Blob, Commit, ObjectData, Tag, Tree};

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
    #[error("the tree is invalid: {0}")]
    InvalidTree(&'static str),
    #[error("a commit object is invalid")]
    InvalidCommit,
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

    pub fn bytes(&self) -> &[u8] {
        &self.buffer
    }

    pub fn remaining(&self) -> usize {
        self.remaining_buffer().len()
    }

    pub fn finished(&self) -> bool {
        self.remaining_buffer().is_empty()
    }

    pub fn advance(&mut self, len: usize) -> bool {
        if self.remaining() < len {
            false
        } else {
            self.pos += len;
            true
        }
    }

    pub fn parse(mut self) -> Result<ObjectData, ParseError> {
        let header = self.parse_header()?;

        self.read_body(&header)?;

        match header.kind {
            ObjectKind::Blob => Blob::parse(self).map(ObjectData::Blob),
            ObjectKind::Commit => Commit::parse(self).map(ObjectData::Commit),
            ObjectKind::Tree => Tree::parse(self).map(ObjectData::Tree),
            ObjectKind::Tag => Tag::parse(self).map(ObjectData::Tag),
        }
    }

    pub fn parse_header(&mut self) -> Result<Header, ParseError> {
        debug_assert_eq!(self.pos, 0);

        let end = self.read_header()?;

        let kind = self.consume_until(b' ').ok_or(ParseError::InvalidHeader)?;
        let kind = match kind {
            b"commit" => ObjectKind::Commit,
            b"tree" => ObjectKind::Tree,
            b"blob" => ObjectKind::Blob,
            b"tag" => ObjectKind::Tag,
            _ => return Err(ParseError::InvalidLength),
        };

        let len = &self.buffer[self.pos..end];
        let len = str::from_utf8(&len).map_err(|_| ParseError::InvalidHeader)?;
        let len = usize::from_str(&len).map_err(|_| ParseError::InvalidLength)?;

        debug_assert_eq!(self.buffer[end], b'\0');
        self.pos = end + 1;

        Ok(Header { kind, len })
    }

    pub fn finish(self) -> Vec<u8> {
        self.buffer
    }

    pub fn consume_bytes(&mut self, bytes: &[u8]) -> bool {
        if self.remaining_buffer().starts_with(bytes) {
            self.pos += bytes.len();
            true
        } else {
            false
        }
    }

    pub fn consume_until<'a>(&'a mut self, ch: u8) -> Option<&'a [u8]> {
        match memchr(ch, self.remaining_buffer()) {
            Some(ch_pos) => {
                let result = &self.buffer[self.pos..][..ch_pos];
                self.pos += ch_pos + 1;
                Some(result)
            }
            None => None,
        }
    }

    fn remaining_buffer(&self) -> &[u8] {
        &self.buffer[self.pos..]
    }

    fn read_header(&mut self) -> Result<usize, ParseError> {
        debug_assert!(self.buffer.is_empty());

        self.buffer.resize(MAX_HEADER_LEN, 0);

        let mut len = 0;
        while !self.buffer[len..].is_empty() {
            match self.reader.read(&mut self.buffer[len..]) {
                Ok(0) => return Err(ParseError::InvalidHeader),
                Ok(read) => {
                    let new_len = len + read;
                    if let Some(header_end) = memchr(b'\0', &self.buffer[len..new_len]) {
                        self.buffer.truncate(new_len);
                        return Ok(len + header_end);
                    }
                    len = new_len;
                }
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(err.into()),
            }
        }

        return Err(ParseError::InvalidHeader);
    }

    fn read_body(&mut self, header: &Header) -> Result<(), ParseError> {
        let start = self.pos;
        let end = start
            .checked_add(header.len)
            .ok_or(ParseError::InvalidLength)?;

        self.buffer.reserve(end.saturating_sub(self.buffer.len()));
        self.reader.read_to_end(&mut self.buffer)?;

        if self.buffer.len() != end {
            return Err(ParseError::InvalidLength);
        }

        Ok(())
    }
}

#[cfg(test)]
impl Parser<io::Cursor<Vec<u8>>> {
    pub fn from_bytes(bytes: impl Into<Vec<u8>>) -> Self {
        let mut parser = Parser::new(io::Cursor::new(bytes.into()));
        parser.reader.read_to_end(&mut parser.buffer).unwrap();
        parser
    }
}

#[test]
fn test_max_header_len() {
    assert_eq!(MAX_HEADER_LEN, format!("commit {}\0", u64::MAX).len());
}

#[test]
fn test_parse_header() {
    fn parse_header(bytes: &[u8]) -> Result<Header, ParseError> {
        Parser::new(io::Cursor::new(bytes)).parse_header()
    }

    assert_eq!(
        parse_header(b"tree 3\0abc").unwrap(),
        Header {
            kind: ObjectKind::Tree,
            len: 3,
        }
    );
    assert_eq!(
        parse_header(b"blob 3\0abc").unwrap(),
        Header {
            kind: ObjectKind::Blob,
            len: 3,
        }
    );
    assert!(parse_header(b"commit 333333333333333333333\0abc").is_err(),);
    assert!(parse_header(b"blob 3").is_err(),);
    assert!(parse_header(b"blob3\0abc").is_err(),);
}
