use std::io::{self, Read};
use std::str::{self, FromStr};

use thiserror::Error;

use crate::object::{
    Blob, Commit, ObjectData, ObjectKind, ParseBlobError, ParseCommitError, ParseTagError,
    ParseTreeError, Tag, Tree,
};
use crate::parse::{self, Buffer, Parser};

#[derive(Debug, Error)]
pub(in crate::object) enum ParseObjectError {
    #[error("the object header is invalid")]
    InvalidHeader(#[source] ParseHeaderError),
    #[error("the blob object is invalid")]
    InvalidBlob(
        #[source]
        #[from]
        ParseBlobError,
    ),
    #[error("the tree object is invalid")]
    InvalidTree(
        #[source]
        #[from]
        ParseTreeError,
    ),
    #[error("the commit object is invalid")]
    InvalidCommit(
        #[source]
        #[from]
        ParseCommitError,
    ),
    #[error("the tag object is invalid")]
    InvalidTag(
        #[source]
        #[from]
        ParseTagError,
    ),
    #[error("io error reading object")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
}

#[derive(Debug, Error)]
#[error("unknown object type `{0}`")]
pub(in crate::object) struct ParseObjectKindError(String);

#[derive(Debug, PartialEq)]
pub(in crate::object) struct Header {
    pub kind: ObjectKind,
    pub len: usize,
}

#[derive(Debug, Error)]
pub(in crate::object) enum ParseHeaderError {
    #[error("unsupported object type")]
    UnsupportedObjectKind,
    #[error("object size doesn't match actual size")]
    LengthMismatch,
    #[error("object size is too big")]
    LengthTooBig,
    #[error("{0}")]
    Other(&'static str),
    #[error(transparent)]
    ParseObjectKind(#[from] ParseObjectKindError),
    #[error(transparent)]
    Parse(parse::Error),
}

impl Header {
    const MAX_LEN: usize = 28;
}

impl<R: Read> Buffer<R> {
    pub(in crate::object) fn read_object_header(&mut self) -> Result<Header, ParseHeaderError> {
        let range = self.read_until(b'\0', Header::MAX_LEN)?;
        let mut parser = self.parser(range);
        let header = parser.parse_object_header()?;
        debug_assert!(parser.finished());
        Ok(header)
    }

    pub(in crate::object) fn read_object(mut self) -> Result<ObjectData, ParseObjectError> {
        let header = self.read_object_header()?;
        let parser = self
            .read_into_parser(header.len)
            .map_err(ParseHeaderError::from)?;
        parser.parse_object_body(header.kind)
    }
}

impl Parser<Box<[u8]>> {
    fn parse_object_body(self, kind: ObjectKind) -> Result<ObjectData, ParseObjectError> {
        match kind {
            ObjectKind::Blob => Blob::parse(self)
                .map(ObjectData::Blob)
                .map_err(ParseObjectError::InvalidBlob),
            ObjectKind::Commit => Commit::parse(self)
                .map(ObjectData::Commit)
                .map_err(ParseObjectError::InvalidCommit),
            ObjectKind::Tree => Tree::parse(self)
                .map(ObjectData::Tree)
                .map_err(ParseObjectError::InvalidTree),
            ObjectKind::Tag => Tag::parse(self)
                .map(ObjectData::Tag)
                .map_err(ParseObjectError::InvalidTag),
        }
    }
}

impl<B: AsRef<[u8]>> Parser<B> {
    pub(in crate::object) fn parse_object_header(&mut self) -> Result<Header, ParseHeaderError> {
        debug_assert_eq!(self.pos(), 0);

        let kind = self
            .consume_until(b' ')
            .ok_or(ParseHeaderError::Other("failed to parse object kind"))?;
        let kind = ObjectKind::from_bytes(&self[kind])?;

        let len = self
            .consume_until(b'\0')
            .ok_or(ParseHeaderError::Other("failed to parse object length"))?;
        let len = str::from_utf8(&self[len])
            .map_err(|_| ParseHeaderError::Other("failed to parse object length"))?;
        let len = usize::from_str(&len).map_err(|_| ParseHeaderError::LengthTooBig)?;

        Ok(Header { kind, len })
    }
}

impl ObjectKind {
    pub(in crate::object) fn from_bytes(input: &[u8]) -> Result<Self, ParseObjectKindError> {
        match input {
            b"commit" => Ok(ObjectKind::Commit),
            b"tree" => Ok(ObjectKind::Tree),
            b"blob" => Ok(ObjectKind::Blob),
            b"tag" => Ok(ObjectKind::Tag),
            input => Err(ParseObjectKindError(
                String::from_utf8_lossy(input).into_owned(),
            )),
        }
    }
}

impl From<ParseHeaderError> for ParseObjectError {
    fn from(err: ParseHeaderError) -> Self {
        match err {
            ParseHeaderError::Parse(parse::Error::Io(err)) => ParseObjectError::Io(err),
            err => ParseObjectError::InvalidHeader(err),
        }
    }
}

impl From<parse::Error> for ParseHeaderError {
    fn from(err: parse::Error) -> Self {
        match err {
            parse::Error::InvalidLength => ParseHeaderError::LengthMismatch,
            err => ParseHeaderError::Parse(err),
        }
    }
}

#[test]
fn test_max_header_len() {
    assert_eq!(Header::MAX_LEN, format!("commit {}\0", u64::MAX).len());
}

#[test]
fn test_parse_header() {
    fn parse_header(bytes: &[u8]) -> Result<Header, ParseHeaderError> {
        Parser::new(bytes).parse_object_header()
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
