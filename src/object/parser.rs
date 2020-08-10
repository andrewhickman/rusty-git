use std::io;
use std::ops::Range;
use std::slice::SliceIndex;
use std::str::{self, FromStr};
use std::path::{Path, PathBuf};
use std::mem::size_of;

use byteorder::{NetworkEndian, ByteOrder};
use memchr::memchr;
use thiserror::Error;

use crate::object::{Blob, Commit, Id, ObjectData, Tag, Tree, ID_HEX_LEN};

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
    #[error("unexpected end of file")]
    UnexpectedEof,
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
    #[error("a signature is invalid")]
    InvalidSignature,
    #[error("an object id is invalid")]
    InvalidId,
    #[error("an tag object is invalid: {0}")]
    InvalidTag(&'static str),
    #[error("unknown pack format version")]
    UnknownPackVersion,
    #[error("io error reading object")]
    Io(
        #[from]
        #[source]
        io::Error,
    ),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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

impl<R: io::Read> Parser<R> {
    pub fn new(reader: R) -> Self {
        Parser {
            buffer: Vec::new(),
            reader,
            pos: 0,
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
        let kind = ObjectKind::from_bytes(self.bytes(kind))?;

        let len = &self.buffer[self.pos..end];
        let len = str::from_utf8(&len).map_err(|_| ParseError::InvalidHeader)?;
        let len = usize::from_str(&len).map_err(|_| ParseError::InvalidLength)?;

        debug_assert_eq!(self.buffer[end], b'\0');
        self.pos = end + 1;

        Ok(Header { kind, len })
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

impl Parser<()> {
    pub fn from_bytes(buffer: impl Into<Vec<u8>>) -> Self {
        Parser {
            buffer: buffer.into(),
            pos: 0,
            reader: (),
        }
    }

    pub fn from_file(path: impl AsRef<Path> + Into<PathBuf>) -> io::Result<Self> {
        Ok(Parser::from_bytes(fs_err::read(path)?))
    }
}

impl<R> Parser<R> {
    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn bytes<I>(&self, index: I) -> &I::Output
    where
        I: SliceIndex<[u8]>,
    {
        self.buffer.get(index).expect("invalid index")
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

    pub fn consume_until(&mut self, ch: u8) -> Option<Range<usize>> {
        match memchr(ch, self.remaining_buffer()) {
            Some(ch_pos) => {
                let start = self.pos;
                let end = start + ch_pos;
                self.pos = end + 1;
                Some(start..end)
            }
            None => None,
        }
    }

    pub fn consume_u32(&mut self, value: u32) -> bool {
        let len = size_of::<u32>();
        if self.remaining() < len || NetworkEndian::read_u32(self.remaining_buffer()) != value {
            false
        } else {
            self.pos += len;
            true
        }
    }

    // Consume 4 bytes and convert them from network-endian to native-endian format.
    pub fn parse_u32(&mut self) -> Result<u32, ParseError> {
        let len = size_of::<u32>();
        if self.remaining() < len  {
            Err(ParseError::UnexpectedEof)
        } else {
            let value = NetworkEndian::read_u32(self.remaining_buffer());
            self.pos += len;
            Ok(value)
        }
    }

    pub fn parse_prefix_line(&mut self, prefix: &[u8]) -> Result<Option<Range<usize>>, ParseError> {
        if !self.consume_bytes(prefix) {
            return Ok(None);
        }

        let start = self.pos();
        let end = match self.consume_until(b'\n') {
            Some(line) => start + line.len(),
            None => return Err(ParseError::UnexpectedEof),
        };

        Ok(Some(start..end))
    }

    pub fn parse_hex_id_line(&mut self, prefix: &[u8]) -> Result<Option<usize>, ParseError> {
        if !self.consume_bytes(prefix) {
            return Ok(None);
        }

        let start = self.pos();
        if !self.advance(ID_HEX_LEN) || !self.consume_bytes(b"\n") {
            return Err(ParseError::UnexpectedEof);
        }

        if Id::from_hex(&self.bytes(start..)[..ID_HEX_LEN]).is_err() {
            return Err(ParseError::UnexpectedEof);
        }

        Ok(Some(start))
    }

    fn remaining_buffer(&self) -> &[u8] {
        &self.buffer[self.pos..]
    }
}

impl ObjectKind {
    pub fn from_bytes(input: &[u8]) -> Result<Self, ParseError> {
        match input {
            b"commit" => Ok(ObjectKind::Commit),
            b"tree" => Ok(ObjectKind::Tree),
            b"blob" => Ok(ObjectKind::Blob),
            b"tag" => Ok(ObjectKind::Tag),
            _ => Err(ParseError::UnknownType),
        }
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
