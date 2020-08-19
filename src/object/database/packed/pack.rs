use std::fmt;
use std::io::{self, Read, Seek, SeekFrom};
use std::mem::size_of;
use std::path::PathBuf;
use std::sync::Mutex;

use byteorder::NetworkEndian;
use bytes::Bytes;
use dashmap::mapref::entry::Entry as DashMapEntry;
use dashmap::DashMap;
use fs_err::File;
use smallvec::SmallVec;
use thiserror::Error;
use zerocopy::byteorder::U32;
use zerocopy::FromBytes;

use crate::object::database::packed::delta::{apply_delta, DeltaError};
use crate::object::database::packed::index::{FindIndexOffsetError, IndexFile};
use crate::object::database::ObjectReader;
use crate::object::{Id, ObjectHeader, ObjectKind, ParseObjectError, ShortId, ID_LEN};
use crate::parse;

pub(in crate::object::database::packed) struct PackFile {
    id: Id,
    file: Mutex<parse::Buffer<File>>,
    cache: DashMap<u64, (ObjectHeader, Bytes)>,
    version: PackFileVersion,
    count: u32,
}

#[derive(Debug, Error)]
pub(in crate::object::database::packed) enum ReadPackFileError {
    #[error("the signature of the pack file is invalid")]
    InvalidSignature,
    #[error("cannot parse a pack file with version `{0}`")]
    UnknownVersion(u32),
    #[error("cannot parse object type `{0}`")]
    UnknownType(u8),
    #[error("error finding base object offset in pack index file")]
    FindIndexOffset(
        #[from]
        #[source]
        FindIndexOffsetError,
    ),
    #[error("a base object is invalid")]
    ParseObjectError(
        #[from]
        #[source]
        ParseObjectError,
    ),
    #[error("failed to apply a delta")]
    ParseDeltaError(
        #[from]
        #[source]
        DeltaError,
    ),
    #[error("{0}")]
    Other(&'static str),
    #[error(transparent)]
    Parse(#[from] parse::Error),
    #[error("io error reading pack index file")]
    Io(
        #[from]
        #[source]
        io::Error,
    ),
}

#[derive(Debug)]
enum PackFileVersion {
    V2,
    V3,
}

#[repr(C)]
#[derive(Copy, Clone, Debug, FromBytes)]
struct PackFileHeader {
    signature: U32<NetworkEndian>,
    version: U32<NetworkEndian>,
    count: U32<NetworkEndian>,
}

type Chain = SmallVec<[ChainEntry; 16]>;

#[derive(Debug)]
struct ChainEntry {
    // The offset of the object header (used as its key in the cache)
    key: u64,
    // The offset of the object data, following the header
    offset: u64,
    header: ObjectHeader,
}

impl PackFile {
    const SIGNATURE: u32 = u32::from_be_bytes(*b"PACK");

    pub fn open(path: PathBuf) -> Result<Self, ReadPackFileError> {
        let mut file = Mutex::new(parse::Buffer::with_capacity(File::open(path)?, ID_LEN));
        let buffer = file.get_mut().unwrap();
        let header = buffer.read_pack_file_header()?;

        if header.signature.get() != PackFile::SIGNATURE {
            return Err(ReadPackFileError::InvalidSignature);
        }

        let version = match header.version.get() {
            2 => PackFileVersion::V2,
            3 => PackFileVersion::V3,
            n => return Err(ReadPackFileError::UnknownVersion(n)),
        };

        buffer.seek(SeekFrom::End(-(ID_LEN as i64)))?;
        let id = buffer.read_id()?;

        Ok(PackFile {
            version,
            cache: DashMap::new(),
            count: header.count.get(),
            file,
            id,
        })
    }

    pub fn read_object(
        &self,
        index: &IndexFile,
        offset: u64,
    ) -> Result<ObjectReader, ReadPackFileError> {
        let (chain, mut header, mut base) = self.find_chain(index, offset)?;
        for entry in chain {
            let (new_header, new_base) = self.apply_delta(base, entry)?;
            header = new_header;
            base = new_base;
        }

        Ok(ObjectReader::from_bytes(header, base))
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    fn find_chain(
        &self,
        index: &IndexFile,
        mut offset: u64,
    ) -> Result<(Chain, ObjectHeader, Bytes), ReadPackFileError> {
        let mut chain = Chain::new();

        let mut buffer = self.file.lock().unwrap();

        loop {
            let cache_entry = match self.cache.entry(offset) {
                DashMapEntry::Occupied(entry) => {
                    return Ok((chain, entry.get().0, entry.get().1.clone()))
                }
                DashMapEntry::Vacant(entry) => entry,
            };

            buffer.seek(SeekFrom::Start(offset))?;

            let header = buffer.read_pack_object_header()?;

            let base_offset = match header.kind {
                ObjectKind::OfsDelta => {
                    let delta_offset = buffer.read_delta_offset()?;
                    offset
                        .checked_sub(delta_offset)
                        .ok_or(ReadPackFileError::Other("invalid delta offset"))?
                }
                ObjectKind::RefDelta => {
                    let id = buffer.read_delta_reference()?;
                    let (offset, _) = index.find_offset(&ShortId::from(id))?;
                    offset
                }
                _ => {
                    let base = buffer.read_exact(header.len)?;
                    let base = buffer.take_buffer(base);
                    cache_entry.insert((header, base.clone()));
                    return Ok((chain, header, base));
                }
            };

            chain.push(ChainEntry {
                key: offset,
                offset: offset + buffer.pos() as u64,
                header,
            });

            if base_offset == offset {
                return Err(ReadPackFileError::Other("loop in deltas"));
            }
            offset = base_offset;
        }
    }

    fn apply_delta(
        &self,
        base: Bytes,
        delta: ChainEntry,
    ) -> Result<(ObjectHeader, Bytes), ReadPackFileError> {
        let mut buffer = self.file.lock().unwrap();

        buffer.seek(SeekFrom::Start(delta.offset))?;

        let result = apply_delta(delta.header.kind, &base, &mut buffer.decompress_exact(delta.header.len))?;

        Ok(self
            .cache
            .insert(delta.key, result.clone())
            .unwrap_or(result))
    }

    pub fn id(&self) -> Id {
        self.id
    }
}

impl PackFileHeader {
    const LEN: usize = size_of::<PackFileHeader>();
}

impl ObjectHeader {
    const MAX_PACKED_LEN: usize = 1 + (size_of::<usize>() * 8 - 4) / 7 + 1;
    const MAX_DELTA_OFFSET_LEN: usize = (size_of::<u64>() * 8) / 7 + 1;
}

impl<R: Read> parse::Buffer<R> {
    fn read_pack_file_header(&mut self) -> Result<PackFileHeader, ReadPackFileError> {
        let range = self.read_exact(PackFileHeader::LEN)?;
        let mut parser = self.parser(range);
        Ok(*parser.parse_struct::<PackFileHeader>()?)
    }

    fn read_pack_object_header(&mut self) -> Result<ObjectHeader, ReadPackFileError> {
        let range = self
            .read_until(ObjectHeader::MAX_PACKED_LEN, |slice| {
                slice
                    .iter()
                    .position(|&byte| byte & 0b1000_0000 == 0)
                    .map(|offset| offset + 1)
            })?
            .ok_or(ReadPackFileError::Other("invalid object size"))?;
        let parser = &mut self.parser(range);

        let mut byte = parser.parse_byte()?;
        let kind = match (byte & 0b0111_0000) >> 4 {
            1 => ObjectKind::Commit,
            2 => ObjectKind::Tree,
            3 => ObjectKind::Blob,
            4 => ObjectKind::Tag,
            6 => ObjectKind::OfsDelta,
            7 => ObjectKind::RefDelta,
            n => return Err(ReadPackFileError::UnknownType(n)),
        };

        let mut len = usize::from(byte & 0b0000_1111);
        let mut shift = 4;
        while parser.remaining() != 0 {
            byte = parser.parse_byte()?;
            len |= usize::from(byte & 0b0111_1111)
                .checked_shl(shift)
                .ok_or(ReadPackFileError::Other("invalid object size"))?;
            shift += 7;
        }

        Ok(ObjectHeader { len, kind })
    }

    fn read_delta_offset(&mut self) -> Result<u64, ReadPackFileError> {
        let range = self
            .read_until(ObjectHeader::MAX_DELTA_OFFSET_LEN, |slice| {
                slice
                    .iter()
                    .position(|&byte| byte & 0b1000_0000 == 0)
                    .map(|offset| offset + 1)
            })?
            .ok_or(ReadPackFileError::Other("invalid delta offset"))?;
        let parser = &mut self.parser(range);

        let mut offset: u64 = 0;
        while parser.remaining() != 0 {
            let byte = parser.parse_byte()?;
            offset <<= 7;
            offset += u64::from(byte & 0b0111_1111);
        }

        Ok(offset)
    }

    fn read_delta_reference(&mut self) -> Result<Id, ReadPackFileError> {
        Ok(self.read_id()?)
    }
}

impl fmt::Debug for PackFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PackFile")
            .field("version", &self.version)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use bstr::B;

    use super::*;

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn pack_object_header_max_len() {
        let max_len_header = b"\x9F\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x0F";
        assert_eq!(max_len_header.len(), ObjectHeader::MAX_PACKED_LEN);
        let mut buffer = parse::Buffer::new(io::Cursor::new(B(max_len_header)));
        let parsed_header = buffer.read_pack_object_header().unwrap();
        assert_eq!(parsed_header.kind, ObjectKind::Commit);
        assert_eq!(parsed_header.len, usize::MAX);
    }

    #[test]
    fn pack_object_header_max_delta_offset_len() {
        let max_len_header = b"\x81\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\x7F";
        assert_eq!(max_len_header.len(), ObjectHeader::MAX_DELTA_OFFSET_LEN);
        let mut buffer = parse::Buffer::new(io::Cursor::new(B(max_len_header)));
        assert_eq!(buffer.read_delta_offset().unwrap(), u64::MAX);
    }
}
