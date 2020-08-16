use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::mem::size_of;
use std::ops::Range;
use std::path::PathBuf;

use byteorder::NetworkEndian;
use thiserror::Error;
use zerocopy::byteorder::{U32, U64};
use zerocopy::{FromBytes, LayoutVerified};

use crate::object::{Id, Parser, ShortId, ID_LEN};

pub(in crate::object::database::packed) struct IndexFile {
    data: Box<[u8]>,
    version: Version,
    count: usize,
}

#[derive(Debug, Error)]
pub(in crate::object::database::packed) enum ReadIndexFileError {
    #[error("cannot parse an pack file index with version `{0}`")]
    UnknownVersion(u32),
    #[error("{0}")]
    Other(&'static str),
    #[error("io error reading pack file index")]
    Io(
        #[from]
        #[source]
        io::Error,
    ),
}

#[derive(Debug, Error)]
pub(in crate::object::database::packed) enum FindIndexOffsetError {
    #[error("the object id was not found in the pack file index")]
    NotFound,
    #[error("the object id is ambiguous in the pack file index")]
    Ambiguous,
    #[error(transparent)]
    ReadIndexFile(ReadIndexFileError),
}

#[derive(Debug, PartialEq)]
enum Version {
    V1,
    V2,
}

#[repr(C)]
#[derive(Debug, FromBytes)]
struct EntryV1 {
    offset: U32<NetworkEndian>,
    id: Id,
}

#[repr(C)]
#[derive(Debug, FromBytes)]
struct EntryV2 {
    id: Id,
}

impl IndexFile {
    const SIGNATURE: u32 = 0xff744f63;
    const HEADER_LEN: usize = 8;
    const LEVEL_ONE_COUNT: usize = 256;
    const LEVEL_ONE_LEN: usize = IndexFile::LEVEL_ONE_COUNT * 4;
    const ENTRY_LEN_V1: usize = size_of::<EntryV1>();
    const ENTRY_LEN_V2: usize = size_of::<EntryV2>();
    const TRAILER_LEN: usize = ID_LEN + ID_LEN;

    pub fn open(path: PathBuf) -> Result<Self, ReadIndexFileError> {
        let bytes = fs_err::read(path)?.into_boxed_slice();
        IndexFile::parse(Parser::new(bytes))
    }

    fn parse(mut parser: Parser<Box<[u8]>>) -> Result<Self, ReadIndexFileError> {
        let version = if parser.consume_u32(IndexFile::SIGNATURE) {
            let version = parser
                .parse_u32()
                .map_err(|_| ReadIndexFileError::Other("file is too short"))?;

            match version {
                2 => Version::V2,
                n => return Err(ReadIndexFileError::UnknownVersion(n)),
            }
        } else {
            Version::V1
        };

        let mut count = 0;
        for _ in 0..IndexFile::LEVEL_ONE_COUNT {
            let n = parser
                .parse_u32()
                .map_err(|_| ReadIndexFileError::Other("file is too short"))?;
            if n < count {
                return Err(ReadIndexFileError::Other("the fan out is not monotonic"));
            }
            count = n;
        }
        let count =
            usize::try_from(count).or(Err(ReadIndexFileError::Other("invalid index count")))?;

        let mut min_size = count
            .checked_mul(version.entry_len())
            .ok_or(ReadIndexFileError::Other("invalid index count"))?
            .checked_add(IndexFile::TRAILER_LEN)
            .ok_or(ReadIndexFileError::Other("invalid index count"))?;
        if version == Version::V2 {
            min_size = count
                .checked_mul(8)
                .ok_or(ReadIndexFileError::Other("invalid index count"))?
                .checked_add(min_size)
                .ok_or(ReadIndexFileError::Other("invalid index count"))?;
        }

        let max_size = match version {
            Version::V1 => min_size,
            Version::V2 => min_size
                .checked_add(
                    count
                        .saturating_sub(1)
                        .checked_mul(8)
                        .ok_or(ReadIndexFileError::Other("invalid index count"))?,
                )
                .ok_or(ReadIndexFileError::Other("invalid index count"))?,
        };

        if parser.remaining() < min_size || parser.remaining() > max_size {
            return Err(ReadIndexFileError::Other(
                "index length is an invalid length",
            ));
        }

        Ok(IndexFile {
            data: parser.into_inner(),
            count,
            version,
        })
    }

    pub fn find_offset(&self, short_id: &ShortId) -> Result<(usize, Id), FindIndexOffsetError> {
        let level_one = self.level_one();
        let first_byte = short_id.first_byte() as usize;
        let index_end = level_one[first_byte].get() as usize;
        let index_start = match first_byte.checked_sub(1) {
            Some(prev) => level_one[prev].get() as usize,
            None => 0,
        };

        fn binary_search<'a, T: Entry>(
            entries: &'a [T],
            short_id: &ShortId,
        ) -> Result<(usize, &'a T), FindIndexOffsetError> {
            match entries.binary_search_by(|entry| entry.id().cmp_short(short_id)) {
                Ok(index) => Ok((index, &entries[index])),
                Err(index) => {
                    let mut matches = entries[index..]
                        .iter()
                        .take_while(|entry| entry.id().starts_with(short_id));
                    let entry = matches
                        .next()
                        .ok_or_else(|| FindIndexOffsetError::NotFound)?;
                    if matches.next().is_some() {
                        return Err(FindIndexOffsetError::Ambiguous);
                    }
                    Ok((index, entry))
                }
            }
        }

        let (offset, id) = match self.version {
            Version::V1 => {
                let (_, entry) = binary_search(self.entries_v1(index_start..index_end)?, short_id)?;
                (u64::from(entry.offset.get()), entry.id)
            }
            Version::V2 => {
                let (index, entry) =
                    binary_search(self.entries_v2(index_start..index_end)?, short_id)?;
                let (small_offsets, large_offsets) = self.offsets();
                let small_offset = small_offsets[index_start + index].get();
                let offset = if (small_offset & 0x80000000) == 0 {
                    u64::from(small_offsets[index_start + index].get())
                } else {
                    let large_offset_index = usize::try_from(small_offset & 0x7fffffff)
                        .map_err(|_| FindIndexOffsetError::read_index_file("invalid offset"))?;
                    large_offsets
                        .get(large_offset_index)
                        .ok_or(FindIndexOffsetError::read_index_file("invalid offset"))?
                        .get()
                };
                (offset, entry.id)
            }
        };

        let offset = usize::try_from(offset)
            .map_err(|_| FindIndexOffsetError::read_index_file("invalid offset"))?;

        Ok((offset, id))
    }

    pub fn count(&self) -> u32 {
        self.count as u32
    }

    fn level_one(&self) -> &[U32<NetworkEndian>] {
        LayoutVerified::new_slice(&self.data()[..IndexFile::LEVEL_ONE_LEN])
            .unwrap()
            .into_slice()
    }

    fn entries_v1(&self, range: Range<usize>) -> Result<&[EntryV1], FindIndexOffsetError> {
        Ok(LayoutVerified::<_, [EntryV1]>::new_slice(self.entries())
            .unwrap()
            .into_slice()
            .get(range)
            .ok_or(FindIndexOffsetError::read_index_file("invalid offset"))?)
    }

    fn entries_v2(&self, range: Range<usize>) -> Result<&[EntryV2], FindIndexOffsetError> {
        Ok(LayoutVerified::<_, [EntryV2]>::new_slice(self.entries())
            .unwrap()
            .into_slice()
            .get(range)
            .ok_or(FindIndexOffsetError::read_index_file("invalid offset"))?)
    }

    fn entries(&self) -> &[u8] {
        let data = self.data();
        &data[IndexFile::LEVEL_ONE_LEN..][..(self.count * self.version.entry_len())]
    }

    fn offsets(&self) -> (&[U32<NetworkEndian>], &[U64<NetworkEndian>]) {
        debug_assert_eq!(self.version, Version::V2);

        let data = &self.data()[IndexFile::LEVEL_ONE_LEN..];
        let start = self.count * (IndexFile::ENTRY_LEN_V2 + 4);
        let mid = start + self.count * 4;
        let end = data.len() - IndexFile::TRAILER_LEN;

        (
            LayoutVerified::new_slice(&data[start..mid])
                .unwrap()
                .into_slice(),
            LayoutVerified::new_slice(&data[mid..end])
                .unwrap()
                .into_slice(),
        )
    }

    fn data(&self) -> &[u8] {
        match self.version {
            Version::V1 => &self.data,
            Version::V2 => &self.data[IndexFile::HEADER_LEN..],
        }
    }

    pub fn id(&self) -> Id {
        let pos = self.data.len() - IndexFile::TRAILER_LEN;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }

    // TODO: check this
    #[allow(unused)]
    fn crc(&self) -> Id {
        let pos = self.data.len() - IndexFile::TRAILER_LEN + ID_LEN;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }
}

impl Version {
    fn entry_len(&self) -> usize {
        match self {
            Version::V1 => IndexFile::ENTRY_LEN_V1,
            Version::V2 => IndexFile::ENTRY_LEN_V2,
        }
    }
}

trait Entry {
    fn id(&self) -> &Id;
}

impl Entry for EntryV1 {
    fn id(&self) -> &Id {
        &self.id
    }
}

impl Entry for EntryV2 {
    fn id(&self) -> &Id {
        &self.id
    }
}

impl fmt::Debug for IndexFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("IndexFile")
            .field("version", &self.version)
            .field("count", &self.count)
            .field("id", &self.id())
            .finish()
    }
}

impl FindIndexOffsetError {
    fn read_index_file(message: &'static str) -> Self {
        FindIndexOffsetError::ReadIndexFile(ReadIndexFileError::Other(message))
    }
}

#[cfg(test)]
mod tests {
    use std::mem::{align_of, size_of};
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_entry_layout() {
        assert_eq!(size_of::<EntryV1>(), IndexFile::ENTRY_LEN_V1);
        assert_eq!(align_of::<EntryV1>(), 1);
        assert_eq!(size_of::<EntryV2>(), IndexFile::ENTRY_LEN_V2);
        assert_eq!(align_of::<EntryV2>(), 1);
    }

    fn id(s: &str) -> Id {
        Id::from_str(s).unwrap()
    }

    fn short(s: &str) -> ShortId {
        ShortId::from_str(s).unwrap()
    }

    impl FindIndexOffsetError {
        fn is_ambiguous(&self) -> bool {
            match self {
                FindIndexOffsetError::Ambiguous => true,
                _ => false,
            }
        }

        fn is_not_found(&self) -> bool {
            match self {
                FindIndexOffsetError::NotFound => true,
                _ => false,
            }
        }
    }

    #[test]
    fn parse_v1() {
        let mut bytes = Vec::new();

        for _ in 0..32 {
            bytes.extend(b"\x00\x00\x00\x00");
        }
        for _ in 32..64 {
            bytes.extend(b"\x00\x00\x00\x01");
        }
        for _ in 64..IndexFile::LEVEL_ONE_COUNT {
            bytes.extend(b"\x00\x00\x00\x03");
        }

        bytes.extend(b"\x00\x00\x00\x24");
        bytes.extend(id("2057bab324290cc76e3669cd24ff7345e907fd13").as_bytes());
        bytes.extend(b"\x00\x00\x00\x42");
        bytes.extend(id("4046b3b7c67ec0dedab9c5952d630b241eebf820").as_bytes());
        bytes.extend(b"\x00\x00\x00\x61");
        bytes.extend(id("4046d56282d07200068541199583f49c65f707f7").as_bytes());

        bytes.extend(id("ea0e0aa8f197e86ba6d2c2203e280b26ecbadb76").as_bytes());
        bytes.extend(Id::default().as_bytes());

        let parser = Parser::new(bytes.into_boxed_slice());

        let index = IndexFile::parse(parser).unwrap();

        assert_eq!(index.count, 3);
        assert_eq!(index.version, Version::V1);
        assert_eq!(
            index.find_offset(&short("2057bab324290cc7")).unwrap(),
            (0x24, id("2057bab324290cc76e3669cd24ff7345e907fd13"))
        );
        assert_eq!(
            index
                .find_offset(&short("2057bab324290cc76e3669cd24ff7345e907fd13"))
                .unwrap(),
            (0x24, id("2057bab324290cc76e3669cd24ff7345e907fd13"))
        );
        assert_eq!(
            index
                .find_offset(&short("4046b3b7c67ec0dedab9c5952d630b241eebf820"))
                .unwrap(),
            (0x42, id("4046b3b7c67ec0dedab9c5952d630b241eebf820"))
        );
        assert_eq!(
            index
                .find_offset(&short("4046d56282d07200068541199583f49c65f707f7"))
                .unwrap(),
            (0x61, id("4046d56282d07200068541199583f49c65f707f7"))
        );
        assert!(index
            .find_offset(&ShortId::from_str("4046").unwrap())
            .unwrap_err()
            .is_ambiguous());
        assert!(index
            .find_offset(&ShortId::from_str("4048").unwrap())
            .unwrap_err()
            .is_not_found());
    }

    #[test]
    fn parse_v2() {
        let mut bytes = Vec::new();
        bytes.extend(b"\xff\x74\x4f\x63");
        bytes.extend(b"\x00\x00\x00\x02");

        for _ in 0..32 {
            bytes.extend(b"\x00\x00\x00\x00");
        }
        for _ in 32..64 {
            bytes.extend(b"\x00\x00\x00\x01");
        }
        for _ in 64..IndexFile::LEVEL_ONE_COUNT {
            bytes.extend(b"\x00\x00\x00\x03");
        }

        bytes.extend(id("2057bab324290cc76e3669cd24ff7345e907fd13").as_bytes());
        bytes.extend(id("4046b3b7c67ec0dedab9c5952d630b241eebf820").as_bytes());
        bytes.extend(id("4046d56282d07200068541199583f49c65f707f7").as_bytes());

        bytes.extend(b"\x00\x00\x00\x00");
        bytes.extend(b"\x00\x00\x00\x00");
        bytes.extend(b"\x00\x00\x00\x00");

        bytes.extend(b"\x00\x00\x00\x24");
        bytes.extend(b"\x80\x00\x00\x00");
        bytes.extend(b"\x00\x00\x00\x61");

        bytes.extend(b"\x00\x00\x00\x00\x00\x00\x00\x42");

        bytes.extend(id("ea0e0aa8f197e86ba6d2c2203e280b26ecbadb76").as_bytes());
        bytes.extend(Id::default().as_bytes());

        let parser = Parser::new(bytes.into_boxed_slice());

        let index = IndexFile::parse(parser).unwrap();

        assert_eq!(index.count, 3);
        assert_eq!(index.version, Version::V2);
        assert_eq!(
            index.find_offset(&short("2057bab324290cc7")).unwrap(),
            (0x24, id("2057bab324290cc76e3669cd24ff7345e907fd13"))
        );
        assert_eq!(
            index
                .find_offset(&short("2057bab324290cc76e3669cd24ff7345e907fd13"))
                .unwrap(),
            (0x24, id("2057bab324290cc76e3669cd24ff7345e907fd13"))
        );
        assert_eq!(
            index
                .find_offset(&short("4046b3b7c67ec0dedab9c5952d630b241eebf820"))
                .unwrap(),
            (0x42, id("4046b3b7c67ec0dedab9c5952d630b241eebf820"))
        );
        assert_eq!(
            index
                .find_offset(&short("4046d56282d07200068541199583f49c65f707f7"))
                .unwrap(),
            (0x61, id("4046d56282d07200068541199583f49c65f707f7"))
        );
        assert!(index
            .find_offset(&ShortId::from_str("4046").unwrap())
            .unwrap_err()
            .is_ambiguous());
        assert!(index
            .find_offset(&ShortId::from_str("4048").unwrap())
            .unwrap_err()
            .is_not_found());
    }
}
