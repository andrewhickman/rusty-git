use std::fmt;
use std::ops::Range;

use bstr::{BStr, ByteSlice};

use crate::object::signature::{Signature, SignatureRaw};
use crate::object::{Id, ParseError, Parser, ID_HEX_LEN};

pub struct Commit {
    data: Vec<u8>,
    tree: usize,
    parents: Vec<usize>,
    author: SignatureRaw,
    committer: SignatureRaw,
    encoding: Option<Range<usize>>,
    message: usize,
}

impl Commit {
    pub fn parse<R>(mut parser: Parser<R>) -> Result<Self, ParseError> {
        let tree = parser
            .parse_hex_id_line(b"tree ")?
            .ok_or(ParseError::InvalidCommit)?;

        let mut parents = Vec::with_capacity(1);
        while let Some(parent) = parser.parse_hex_id_line(b"parent ")? {
            parents.push(parent);
        }

        // TODO: validate author
        let author = parser
            .parse_signature(b"author ")?
            .ok_or(ParseError::InvalidCommit)?;

        // Some tools create multiple author fields, ignore the extra ones
        while parser.parse_signature(b"author ")?.is_some() {}

        let committer = parser
            .parse_signature(b"committer ")?
            .ok_or(ParseError::InvalidCommit)?;

        let mut encoding = None;
        // Consume additional commit headers
        while !parser.consume_bytes(b"\n") {
            if let Some(range) = parser.parse_prefix_line(b"encoding ")? {
                encoding = Some(range);
            } else if parser.consume_until(b'\n').is_none() {
                return Err(ParseError::InvalidCommit);
            }
        }

        let message = parser.pos();

        Ok(Commit {
            data: parser.finish(),
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

    use crate::object::{Commit, Id, Parser};

    #[test]
    fn test_parse_commit() {
        let parser = Parser::from_bytes(
            b"\
tree a552334b3ba0630d8f82ac9f27ab55625085d9bd
parent befc2587746bb7aeb8588788caeaeadd3eb06e4b
author Andrew Hickman <me@andrewhickman.dev> 1596907199 +0100
committer Andrew Hickman <me@andrewhickman.dev>
header value
encoding UTF-8

message"
                .to_vec(),
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
