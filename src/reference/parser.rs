use std::io::{self, Read};
use std::ops::Range;

use memchr::memchr;
use thiserror::Error;

use crate::object::Id;
use crate::reference::{Direct, ReferenceData, ReferenceTarget, Symbolic};

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
    #[error("symbolic reference was invalid")]
    InvalidSymbolicReference,
    #[error("peel object id was invalid")]
    InvalidPeelIdentifier,
    #[error("direct reference object id was invalid")]
    InvalidDirectIdentifier,
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

    pub fn parse(mut self) -> Result<ReferenceData, ParseError> {
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
                line = &line[(ch_pos + 1)..];
                Some(Id::from_bytes(&line[..ch_pos]))
            }
            None => None,
        };

        let target = match memchr(b'/', line) {
            Some(_) => ReferenceTarget::Symbolic(
                Symbolic::from_bytes(&line).map_err(|_| ParseError::InvalidSymbolicReference)?,
            ),
            None => ReferenceTarget::Direct(Direct::from_bytes(&line)),
        };

        Ok(ReferenceData { target, peel })
    }

    pub fn read_until_valid_reference_line(&mut self) -> Result<Option<Range<usize>>, ParseError> {
        while !self.finished() {
            let start = self.pos;
            let end = match self.consume_until(b'\n') {
                Some(_) => self.pos,
                None => self.buffer.len(),
            };

            if self.reference_line_is_valid(&self.buffer[start..end]) {
                return Ok(Some(start..end));
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
