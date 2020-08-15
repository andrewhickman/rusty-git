use std::mem::size_of;
use std::ops::{Index, Range};
use std::slice::SliceIndex;

use byteorder::{ByteOrder, NetworkEndian};
use memchr::memchr;

use crate::parse::Error;
use crate::object::{ID_HEX_LEN, Id};

pub(crate) struct Parser<B> {
    buffer: B,
    pos: usize,
}

impl<B> Parser<B> {
    pub fn new(buffer: B) -> Self {
        Parser { buffer, pos: 0 }
    }

    pub fn with_position(buffer: B, pos: usize) -> Self {
        Parser { buffer, pos }
    }
}

impl<B> Parser<B>
where
    B: AsRef<[u8]>,
{
    pub fn pos(&self) -> usize {
        self.pos
    }

    pub fn advance(&mut self, len: usize) -> bool {
        if len <= self.remaining() {
            self.pos += len;
            true
        } else {
            false
        }
    }

    pub fn remaining(&self) -> usize {
        self.remaining_buffer().len()
    }

    pub fn remaining_buffer(&self) -> &[u8] {
        &self.buffer.as_ref()[self.pos..]
    }

    pub fn finished(&self) -> bool {
        self.remaining_buffer().is_empty()
    }

    pub fn into_inner(self) -> B {
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
    pub fn parse_u32(&mut self) -> Result<u32, Error> {
        let len = size_of::<u32>();
        if self.remaining() < len {
            Err(Error::UnexpectedEof)
        } else {
            let value = NetworkEndian::read_u32(self.remaining_buffer());
            self.pos += len;
            Ok(value)
        }
    }

    // If the next line starts with the given prefix, returns it.
    pub fn parse_prefix_line(&mut self, prefix: &[u8]) -> Result<Option<Range<usize>>, Error> {
        if !self.consume_bytes(prefix) {
            return Ok(None);
        }

        let start = self.pos();
        let end = match self.consume_until(b'\n') {
            Some(line) => start + line.len(),
            None => return Err(Error::UnexpectedEof),
        };

        Ok(Some(start..end))
    }

    pub fn parse_hex_id_line(&mut self, prefix: &[u8]) -> Result<Option<usize>, Error> {
        if !self.consume_bytes(prefix) {
            return Ok(None);
        }

        let start = self.pos();
        if !self.advance(ID_HEX_LEN) || !self.consume_bytes(b"\n") {
            return Err(Error::UnexpectedEof);
        }

        let _ = Id::from_hex(&self[start..][..ID_HEX_LEN])?;

        Ok(Some(start))
    }
}

impl<B, I> Index<I> for Parser<B>
where
    B: AsRef<[u8]>,
    I: SliceIndex<[u8]>,
{
    type Output = I::Output;

    fn index(&self, idx: I) -> &Self::Output {
        &self.buffer.as_ref()[idx]
    }
}
