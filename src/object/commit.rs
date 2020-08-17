use std::fmt;
use std::ops::Range;

use bstr::{BStr, ByteSlice};
use bytes::Bytes;
use smallvec::SmallVec;

use crate::object::signature::{ParseSignatureError, Signature, SignatureRaw};
use crate::object::{Id, ID_HEX_LEN};
use crate::parse::Parser;
use thiserror::Error;

#[derive(Clone)]
pub struct Commit {
    data: Bytes,
    tree: usize,
    parents: SmallVec<[usize; 1]>,
    author: SignatureRaw,
    committer: SignatureRaw,
    encoding: Option<Range<usize>>,
    message: usize,
}

#[derive(Debug, Error)]
pub(in crate::object) enum ParseCommitError {
    #[error(transparent)]
    Signature(#[from] ParseSignatureError),
    #[error("{0}")]
    Other(&'static str),
}

impl Commit {
    pub(in crate::object) fn parse(mut parser: Parser<Bytes>) -> Result<Self, ParseCommitError> {
        let tree = parser
            .parse_hex_id_line(b"tree ")
            .map_err(|_| ParseCommitError::Other("invalid tree object id"))?
            .ok_or(ParseCommitError::Other("missing tree object id"))?;

        let mut parents = SmallVec::new();
        while let Some(parent) = parser
            .parse_hex_id_line(b"parent ")
            .map_err(|_| ParseCommitError::Other("invalid parent object id"))?
        {
            parents.push(parent);
        }

        // TODO: validate author
        let author = parser
            .parse_signature(b"author ")?
            .ok_or(ParseCommitError::Other("missing author"))?;

        // Some tools create multiple author fields, ignore the extra ones
        while parser.parse_signature(b"author ")?.is_some() {}

        let committer = parser
            .parse_signature(b"committer ")?
            .ok_or(ParseCommitError::Other("missing committer"))?;

        let mut encoding = None;
        // Consume additional commit headers
        while !parser.consume_bytes(b"\n") {
            if let Some(range) = parser
                .parse_prefix_line(b"encoding ")
                .map_err(|_| ParseCommitError::Other("invalid encoding"))?
            {
                encoding = Some(range);
            } else if parser.consume_until(b'\n').is_none() {
                return Err(ParseCommitError::Other("missing message"));
            }
        }

        let message = parser.pos();

        Ok(Commit {
            data: parser.into_inner(),
            tree,
            parents,
            author,
            committer,
            encoding,
            message,
        })
    }

    pub fn tree(&self) -> Id {
        Id::from_hex(&self.data[self.tree..][..ID_HEX_LEN]).expect("id already validated")
    }

    pub fn parents<'a>(&'a self) -> impl ExactSizeIterator<Item = Id> + 'a {
        self.parents.iter().map(move |&parent| {
            Id::from_hex(&self.data[parent..][..ID_HEX_LEN]).expect("id already validated")
        })
    }

    pub fn author<'a>(&'a self) -> Signature<'a> {
        Signature::new(&self.data, &self.author)
    }

    pub fn committer<'a>(&'a self) -> Signature<'a> {
        Signature::new(&self.data, &self.committer)
    }

    pub fn encoding(&self) -> Option<&BStr> {
        self.encoding
            .clone()
            .map(|encoding| self.data[encoding].as_bstr())
    }

    pub fn message(&self) -> &BStr {
        self.data[self.message..].as_bstr()
    }
}

impl fmt::Debug for Commit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Commit")
            .field("tree", &self.tree())
            .field("parents", &self.parents().collect::<Vec<_>>())
            .field("author", &self.author())
            .field("committer", &self.committer())
            .field("encoding", &self.encoding())
            .field("message", &self.message())
            .finish()
    }
}

impl<'a> fmt::Debug for Signature<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Signature")
            .field("name", &self.name())
            .field("email", &self.email())
            .field("timestamp", &self.timestamp())
            .field("timezone", &self.timezone())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bstr::ByteSlice;

    use super::*;

    #[test]
    fn test_parse_commit() {
        let parser = Parser::new(
            b"\
tree a552334b3ba0630d8f82ac9f27ab55625085d9bd
parent befc2587746bb7aeb8588788caeaeadd3eb06e4b
author Andrew Hickman <me@andrewhickman.dev> 1596907199 +0100
committer Andrew Hickman <me@andrewhickman.dev>
header value
encoding UTF-8

message"
                .to_vec()
                .into_boxed_slice(),
        );

        let commit = Commit::parse(parser).unwrap();
        assert_eq!(
            commit.tree(),
            Id::from_str("a552334b3ba0630d8f82ac9f27ab55625085d9bd").unwrap()
        );
        assert_eq!(
            commit.parents().collect::<Vec<_>>(),
            &[Id::from_str("befc2587746bb7aeb8588788caeaeadd3eb06e4b").unwrap()]
        );
        assert_eq!(commit.author().name(), "Andrew Hickman");
        assert_eq!(commit.author().email(), "me@andrewhickman.dev");
        assert_eq!(commit.author().timestamp(), Some(b"1596907199".as_bstr()));
        assert_eq!(commit.author().timezone(), Some(b"+0100".as_bstr()));
        assert_eq!(commit.committer().name(), "Andrew Hickman");
        assert_eq!(commit.committer().timestamp(), None);
        assert_eq!(commit.committer().timezone(), None);
        assert_eq!(commit.encoding(), Some(b"UTF-8".as_bstr()));
        assert_eq!(commit.message(), "message");
    }
}
