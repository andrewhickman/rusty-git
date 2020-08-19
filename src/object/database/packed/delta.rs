use std::convert::TryFrom;
use std::io::Read;
use std::mem::size_of;

use bytes::{Bytes, BytesMut};
use thiserror::Error;

use crate::object::{ObjectHeader, ObjectKind};
use crate::parse;

#[derive(Debug, Error)]
pub(in crate::object::database::packed) enum DeltaError {
    #[error("the delta header is invalid")]
    InvalidHeader,
    #[error("the delta is invalid")]
    InvalidCommand,
    #[error("the delta format is unsupported")]
    UnsupportedCommand,
    #[error("the resulting object is too long")]
    TooLong,
    #[error("the base length in the delta does not match the actual base length")]
    BaseLengthMismatch,
    #[error("the result length in the delta does not match the actual result length")]
    ResultLengthMismatch,
    #[error(transparent)]
    Parse(#[from] parse::Error),
}

enum Command {
    CopyFromBase { offset: usize, len: usize },
    CopyFromDelta { len: usize },
}

pub(in crate::object::database::packed) fn apply_delta<R>(
    kind: ObjectKind,
    base: &[u8],
    delta: &mut parse::Buffer<R>,
) -> Result<(ObjectHeader, Bytes), DeltaError>
where
    R: Read,
{
    let header = delta.read_delta_header()?;
    let mut result = BytesMut::with_capacity(header.result_len);

    if header.base_len != base.len() {
        return Err(DeltaError::BaseLengthMismatch);
    }

    while let Some(cmd) = delta.read_command()? {
        let src = match cmd {
            Command::CopyFromBase { offset, len } => {
                base
                    .get(offset..)
                    .ok_or(DeltaError::InvalidCommand)?
                    .get(..len)
                    .ok_or(DeltaError::InvalidCommand)?
            }
            Command::CopyFromDelta { len } => {
                let range = delta.read_exact(len)?;
                &delta[range]
            }
        };

        result.copy_from_slice(&src);
        delta.clear_buffer();
    }

    if header.result_len != result.len() {
        return Err(DeltaError::ResultLengthMismatch);
    }

    Ok((
        ObjectHeader {
            kind,
            len: header.result_len,
        },
        result.freeze(),
    ))
}

struct DeltaHeader {
    base_len: usize,
    result_len: usize,
}

impl DeltaError {
    const MAX_VARINT_LEN: usize = (size_of::<u64>() * 8) / 7 + 1;
}

impl<R> parse::Buffer<R>
where
    R: Read,
{
    fn read_delta_header(&mut self) -> Result<DeltaHeader, DeltaError> {
        let base_len = self.read_delta_header_len(DeltaError::MAX_VARINT_LEN * 2)?;
        let result_len = self.read_delta_header_len(DeltaError::MAX_VARINT_LEN)?;

        Ok(DeltaHeader {
            base_len,
            result_len,
        })
    }

    fn read_delta_header_len(&mut self, max: usize) -> Result<usize, DeltaError> {
        let range = self
            .read_until(max, |slice| {
                slice
                    .iter()
                    .position(|&byte| byte & 0b1000_0000 == 0)
                    .map(|offset| offset + 1)
            })?
            .ok_or(DeltaError::InvalidHeader)?;
        let mut parser = self.parser(range);

        let mut len = 0;
        let mut shift = 0;
        while parser.remaining() != 0 {
            let byte = parser.parse_byte()?;
            len |= usize::from(byte & 0b0111_1111)
                .checked_shl(shift)
                .ok_or(DeltaError::InvalidHeader)?;
            shift += 7;
        }

        Ok(len)
    }

    fn read_command(&mut self) -> Result<Option<Command>, DeltaError> {
        let cmd = match self.read_byte() {
            Ok(cmd) => cmd,
            Err(parse::Error::InvalidLength) => return Ok(None),
            Err(err) => return Err(err.into()),
        };

        if intersects(cmd, 0b1000_000) {
            let mut offset = 0;
            if intersects(cmd, 0b0000_0001) {
                offset |= u64::from(self.read_byte()?) << 0;
            }
            if intersects(cmd, 0b0000_0010) {
                offset |= u64::from(self.read_byte()?) << 8;
            }
            if intersects(cmd, 0b0000_0100) {
                offset |= u64::from(self.read_byte()?) << 16;
            }
            if intersects(cmd, 0b0000_1000) {
                offset |= u64::from(self.read_byte()?) << 24;
            }

            let mut len = 0;
            if intersects(cmd, 0b0001_0000) {
                len |= u64::from(self.read_byte()?) << 0;
            }
            if intersects(cmd, 0b0010_0000) {
                len |= u64::from(self.read_byte()?) << 8;
            }
            if intersects(cmd, 0b0100_0000) {
                len |= u64::from(self.read_byte()?) << 16;
            }
            if len == 0 {
                len = 0x1_0000;
            }

            Ok(Some(Command::CopyFromBase {
                offset: usize::try_from(offset).map_err(|_| DeltaError::TooLong)?,
                len: usize::try_from(len).map_err(|_| DeltaError::TooLong)?,
            }))
        } else if cmd != 0 {
            Ok(Some(Command::CopyFromDelta {
                len: usize::from(cmd),
            }))
        } else {
            Err(DeltaError::UnsupportedCommand)
        }
    }

    fn read_byte(&mut self) -> Result<u8, parse::Error> {
        // TODO: if we ever read from anything other than a zlib stream,
        // this will be slow
        self.read_exact_as_parser(1)?.parse_byte()
    }
}

fn intersects(byte: u8, mask: u8) -> bool {
    byte & mask != 0
}
