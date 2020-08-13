use bstr::ByteSlice;
use std::io::{self, Read};
use std::ops::Range;

use memchr::memchr;
use thiserror::Error;

use crate::object::ParseIdError;
use crate::reference::{Direct, ReferenceTarget, Symbolic};

const SYMBOLIC_PREFIX: &[u8] = b"ref: ";
const INVALID_REFERENCE_START: &[u8] = b"\n #";

pub struct Parser<R> {
    buffer: Vec<u8>,
    pos: usize,
    reader: R,
}

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("reference size is too large")]
    InvalidLength,
    #[error("no reference data found")]
    Empty,
    #[error("no symbolic reference found")]
    EmptySymbolic,
    #[error("reference data was invalid")]
    InvalidReference,
    #[error("peel object id was invalid")]
    InvalidPeelIdentifier,
    #[error("direct reference object id was invalid")]
    InvalidDirectIdentifier(
        #[from]
        #[source]
        ParseIdError,
    ),
    #[error("io error reading reference")]
    Io(
        #[from]
        #[source]
        io::Error,
    ),
}

impl<R: Read> Parser<R> {
    pub fn new(reader: R) -> Self {
        Parser {
            buffer: Vec::new(),
            reader,
            pos: 0,
        }
    }

    pub fn finished(&self) -> bool {
        self.remaining_buffer().is_empty()
    }

    fn remaining_buffer(&self) -> &[u8] {
        &self.buffer[self.pos..]
    }

    pub fn parse(mut self) -> Result<ReferenceTarget, ParseError> {
        self.reader
            .read_to_end(&mut self.buffer)
            .map_err(ParseError::Io)?;

        let range = self
            .read_until_valid_reference_line()?
            .ok_or_else(|| ParseError::Empty)?;

        let mut line = &self.buffer[range];

        if line.starts_with(SYMBOLIC_PREFIX) {
            line = &line[SYMBOLIC_PREFIX.len()..];
        }

        let peel = match memchr(b' ', line) {
            Some(ch_pos) => {
                let p = line[..ch_pos].trim_end();
                line = &line[(ch_pos + 1)..];
                Some(p)
            }
            None => None,
        };

        let target = match memchr(b'/', line) {
            Some(_) => ReferenceTarget::Symbolic(Symbolic::from_bytes(&line.trim_end(), peel)?),
            None => ReferenceTarget::Direct(Direct::from_bytes(&line.trim_end())?),
        };

        Ok(target)
    }

    pub fn read_until_valid_reference_line(&mut self) -> Result<Option<Range<usize>>, ParseError> {
        while !self.finished() {
            let start = self.pos;
            self.pos = match self.consume_until(b'\n') {
                Some(_) => self.pos,
                None => self.buffer.len(),
            };

            if self.reference_line_is_valid(&self.buffer[start..self.pos]) {
                return Ok(Some(start..self.pos));
            }
        }

        Ok(None)
    }

    pub fn reference_line_is_valid(&self, bytes: &[u8]) -> bool {
        !INVALID_REFERENCE_START.contains(&bytes[0])
    }

    pub fn consume_until(&mut self, ch: u8) -> Option<&[u8]> {
        match memchr(ch, self.remaining_buffer()) {
            Some(ch_pos) => {
                let result = &self.buffer[self.pos..][..ch_pos];
                self.pos += ch_pos + 1;
                Some(result)
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ParseError, Parser};
    use crate::object::ParseIdError;
    use crate::reference::{Direct, ReferenceTarget, Symbolic};
    use proptest::prelude::*;
    use proptest::{arbitrary::any, collection::vec, proptest};
    use std::io;

    fn parse_ref(bytes: &[u8]) -> Result<ReferenceTarget, ParseError> {
        Parser::new(io::Cursor::new(bytes)).parse()
    }

    macro_rules! assert_display_eq {
        ($lhs:expr, $rhs:expr) => {
            assert_eq!(format!("{}", $lhs), format!("{}", $rhs));
        };
    }

    #[test]
    fn test_parse_symbolic_reference_directory_format() {
        assert_eq!(
            parse_ref(b"ref: refs/heads/master").unwrap(),
            ReferenceTarget::Symbolic(Symbolic::from_bytes(b"refs/heads/master", None).unwrap())
        );
    }

    #[test]
    fn test_parse_symbolic_reference_packed_format() {
        assert_eq!(
            parse_ref(b"da1a5d18c0ab0c03b20fdd91581bc90acd10d512 refs/remotes/origin/master")
                .unwrap(),
            ReferenceTarget::Symbolic(
                Symbolic::from_bytes(
                    b"refs/remotes/origin/master",
                    Some(b"da1a5d18c0ab0c03b20fdd91581bc90acd10d512")
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn test_parse_skips_commented_lines() {
        assert_eq!(
            parse_ref(b"# pack-refs with: peeled fully-peeled sorted\nda1a5d18c0ab0c03b20fdd91581bc90acd10d512 refs/remotes/origin/master").unwrap(),
            ReferenceTarget::Symbolic(
                Symbolic::from_bytes(
                    b"refs/remotes/origin/master",
                    Some(b"da1a5d18c0ab0c03b20fdd91581bc90acd10d512")
                )
                .unwrap()
            )
        );
    }

    #[test]
    fn test_parse_direct_reference_directory_format() {
        assert_eq!(
            parse_ref(b"dbaac6ca0b9ec8ff358224e7808cd5a21395b88c").unwrap(),
            ReferenceTarget::Direct(
                Direct::from_bytes(b"dbaac6ca0b9ec8ff358224e7808cd5a21395b88c").unwrap()
            )
        );
    }

    #[test]
    fn test_parse_fails_on_empty_input() {
        assert_display_eq!(ParseError::Empty, parse_ref(b"").err().unwrap());
        assert_display_eq!(ParseError::Empty, parse_ref(b" ").err().unwrap());
        assert_display_eq!(ParseError::Empty, parse_ref(b"\n").err().unwrap());
        assert_display_eq!(ParseError::Empty, parse_ref(b"# stuff").err().unwrap());
        assert_display_eq!(
            ParseError::Empty,
            parse_ref(b"\n\n# stuff\n\n").err().unwrap()
        );
    }

    #[test]
    fn test_parse_fails_on_bad_identifiers() {
        assert_display_eq!(
            ParseError::InvalidDirectIdentifier(ParseIdError::TooShort),
            parse_ref(b"badid").err().unwrap()
        );
        assert_display_eq!(
            ParseError::InvalidDirectIdentifier(ParseIdError::TooLong),
            parse_ref(b"01234567890123456789012345678901234567890123456789")
                .err()
                .unwrap()
        );
        assert_display_eq!(
            ParseError::InvalidDirectIdentifier(ParseIdError::TooShort),
            parse_ref(b"badid ref").err().unwrap()
        );
    }

    proptest! {
        #![proptest_config(ProptestConfig {
          cases: 10000, .. ProptestConfig::default()
        })]
        #[test]
        fn randomized_data_does_not_panic(bytes in vec(any::<u8>(), ..200)) {
            parse_ref(&bytes).ok();
        }
    }
}
