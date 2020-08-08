use std::fmt;
use std::io::Read;
use std::ops::Range;

use bstr::{BStr, ByteSlice};

use crate::object::{Id, ParseError, Parser, ID_HEX_LEN};

pub struct Commit {
    data: Vec<u8>,
    tree: usize,
    parents: Vec<usize>,
    author: Range<usize>,
    committer: Range<usize>,
    encoding: Option<Range<usize>>,
    message: usize,
}

impl Commit {
    pub fn parse<R: Read>(mut parser: Parser<R>) -> Result<Self, ParseError> {
        let tree = parser
            .parse_hex_id_line(b"tree ")?
            .ok_or(ParseError::InvalidCommit)?;

        let mut parents = Vec::with_capacity(1);
        while let Some(parent) = parser.parse_hex_id_line(b"parent ")? {
            parents.push(parent);
        }

        // TODO: validate author
        let author = parser
            .parse_line(b"author ")?
            .ok_or(ParseError::InvalidCommit)?;

        // Some tools create multiple author fields, ignore the extra ones
        while parser.parse_line(b"author ")?.is_some() {}

        let committer = parser
            .parse_line(b"committer ")?
            .ok_or(ParseError::InvalidCommit)?;

        let mut encoding = None;
        // Consume additional commit headers
        while !parser.consume_bytes(b"\n") {
            if let Some(range) = parser.parse_line(b"encoding ")? {
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

    pub fn author(&self) -> &BStr {
        self.data[self.author.clone()].as_bstr()
    }

    pub fn committer(&self) -> &BStr {
        self.data[self.committer.clone()].as_bstr()
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

impl<R: Read> Parser<R> {
    fn parse_hex_id_line(&mut self, prefix: &[u8]) -> Result<Option<usize>, ParseError> {
        if !self.consume_bytes(prefix) {
            return Ok(None);
        }

        let start = self.pos();
        if !self.advance(ID_HEX_LEN) || !self.consume_bytes(b"\n") {
            return Err(ParseError::InvalidCommit);
        }

        if let Err(_) = Id::from_hex(&self.bytes()[start..][..ID_HEX_LEN]) {
            return Err(ParseError::InvalidCommit);
        }

        Ok(Some(start))
    }

    fn parse_line<'a>(&'a mut self, prefix: &[u8]) -> Result<Option<Range<usize>>, ParseError> {
        if !self.consume_bytes(prefix) {
            return Ok(None);
        }

        let start = self.pos();
        let end = match self.consume_until(b'\n') {
            Some(line) => start + line.len(),
            None => return Err(ParseError::InvalidCommit),
        };

        Ok(Some(start..end))
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use bstr::{ByteSlice};

    use crate::object::{Commit, Parser, Id};

    #[test]
    fn test_parse_tree() {
        let parser = Parser::from_bytes(
            b"\
tree a552334b3ba0630d8f82ac9f27ab55625085d9bd
parent befc2587746bb7aeb8588788caeaeadd3eb06e4b
author Andrew Hickman <andrew.hickman1@sky.com> 1596907199 +0100
committer Andrew Hickman <andrew.hickman1@sky.com> 1596907199 +0100
header value
encoding UTF-8

message".to_vec(),
        );

        let commit = Commit::parse(parser).unwrap();
        assert_eq!(commit.tree(), Id::from_str("a552334b3ba0630d8f82ac9f27ab55625085d9bd").unwrap());
        assert_eq!(commit.parents().collect::<Vec<_>>(), &[Id::from_str("befc2587746bb7aeb8588788caeaeadd3eb06e4b").unwrap()]);
        assert_eq!(commit.author(), "Andrew Hickman <andrew.hickman1@sky.com> 1596907199 +0100");
        assert_eq!(commit.committer(), "Andrew Hickman <andrew.hickman1@sky.com> 1596907199 +0100");
        assert_eq!(commit.encoding(), Some(b"UTF-8".as_bstr()));
        assert_eq!(commit.message(), "message");
    }
}
