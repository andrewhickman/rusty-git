use std::convert::TryFrom;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::ops::Range;

use byteorder::NetworkEndian;
use dashmap::DashMap;
use zerocopy::byteorder::U32;
use zerocopy::{FromBytes, LayoutVerified};

use crate::object::database::Reader;
use crate::object::{Error, Id, ParseError, Parser, ShortId, ID_LEN};

const PACKS_FOLDER: &str = "objects/pack";
const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const PACK_SIGNATURE: u32 = 0x5041434b;

struct PackFile {
    index: IndexFile,
    version: PackFileVersion,
    data: Box<[u8]>,
}

struct IndexFile {
    data: Box<[u8]>,
    version: IndexFileVersion,
    count: usize,
}

#[derive(Debug)]
enum IndexFileVersion {
    V1,
    V2,
}

#[derive(Debug)]
enum PackFileVersion {
    V2,
    V3,
}

#[repr(C)]
#[derive(FromBytes)]
struct IndexEntryV1 {
    id: Id,
}

#[repr(C)]
#[derive(FromBytes)]
struct IndexEntryV2 {
    id: Id,
    crc: U32<NetworkEndian>,
}

#[derive(Debug)]
pub struct PackedObjectDatabase {
    path: PathBuf,
    // last: Mutex<Arc<PackFile>>, why is this useful?
    packs: DashMap<PathBuf, Arc<PackFile>>,
    last_refresh: Mutex<Option<Instant>>,
}

impl PackedObjectDatabase {
    pub fn open(path: &Path) -> Self {
        PackedObjectDatabase {
            path: path.join(PACKS_FOLDER),
            packs: DashMap::new(),
            last_refresh: Mutex::new(None),
        }
    }

    pub fn read_object(&self, short_id: &ShortId) -> Result<Reader, Error> {
        match self.try_read_object(short_id) {
            Err(Error::ObjectNotFound(_)) if self.refresh()? => self.try_read_object(short_id),
            result => result,
        }
    }

    fn try_read_object(&self, short_id: &ShortId) -> Result<Reader, Error> {
        for pack in self.packs.iter() {
            match pack.value().index.find_offset(short_id) {
                Ok((offset, _)) => return pack.read_object(offset),
                Err(Error::ObjectNotFound(_)) => continue,
                Err(err) => return Err(err),
            }
        }

        return Err(Error::ObjectNotFound(*short_id))
    }

    fn refresh(&self) -> Result<bool, Error> {
        // Keep the mutex locked while refreshing so we don't have multiple thread refreshing simultaneously.
        // This isn't necessary for correctness, but is just an optimization.
        let mut last_refresh_guard = self.last_refresh.lock().unwrap();
        match *last_refresh_guard {
            Some(last_refresh) if last_refresh.elapsed() < MAX_REFRESH_INTERVAL => return Ok(false),
            _ => (),
        }

        for entry in fs_err::read_dir(&self.path)? {
            let path = entry?.path();
            if path.extension() == Some("idx".as_ref()) {
                self.packs
                    .entry(path.clone())
                    .or_try_insert_with(move || PackFile::open(path).map(Arc::new))?;
            }
        }

        *last_refresh_guard = Some(Instant::now());
        Ok(true)
    }
}

impl PackFile {
    fn open(index_path: PathBuf) -> Result<PackFile, ParseError> {
        let pack_path = index_path.with_extension("pack");
        let index = IndexFile::open(index_path)?;

        let mut parser = Parser::from_file(pack_path)?;
        if !parser.consume_u32(PACK_SIGNATURE) {
            return Err(ParseError::InvalidPack);
        }
        let version = match parser.parse_u32()? {
            2 => PackFileVersion::V2,
            3 => PackFileVersion::V3,
            _ => return Err(ParseError::UnknownPackVersion),
        };
        let entry_count = usize::try_from(parser.parse_u32()?).or(Err(ParseError::InvalidPack))?;
        if index.count != entry_count {
            return Err(ParseError::InvalidPackIndex);
        }

        if parser.remaining() < ID_LEN {
            return Err(ParseError::InvalidPack);
        }

        let pack_file = PackFile {
            index,
            version,
            data: parser.finish(),
        };

        if pack_file.index.id() != pack_file.id() {
            return Err(ParseError::InvalidPack);
        }

        Ok(pack_file)
    }

    fn read_object(&self, offset: usize) -> Result<Reader, Error> {
        todo!()
    }

    fn id(&self) -> Id {
        let pos = self.data.len() - ID_LEN;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }
}

impl IndexFile {
    const SIGNATURE: u32 = 0xff744f63;
    const HEADER_LEN: usize = 8;
    const LEVEL_ONE_COUNT: usize = 256;
    const LEVEL_ONE_LEN: usize = IndexFile::LEVEL_ONE_COUNT * 4;
    const ENTRY_SIZE_V1: usize = ID_LEN;
    const ENTRY_SIZE_V2: usize = ID_LEN + 4;
    const TRAILER_LEN: usize = ID_LEN + ID_LEN;

    fn open(path: PathBuf) -> Result<IndexFile, ParseError> {
        let mut parser = Parser::from_file(path)?;

        let version = if parser.consume_u32(IndexFile::SIGNATURE) {
            let version = parser.parse_u32()?;

            match version {
                2 => IndexFileVersion::V2,
                _ => return Err(ParseError::UnknownPackVersion),
            }
        } else {
            IndexFileVersion::V1
        };

        let mut count = 0;
        for _ in 0..IndexFile::LEVEL_ONE_COUNT {
            let n = parser.parse_u32()?;
            if n < count {
                return Err(ParseError::NonMonotonicPackIndex);
            }
            count = n;
        }
        let count = usize::try_from(count).or(Err(ParseError::InvalidPackIndex))?;

        let (min_size, max_size) = match version {
            IndexFileVersion::V1 => {
                let size = count
                    .checked_mul(IndexFile::ENTRY_SIZE_V1 + 4)
                    .ok_or(ParseError::InvalidPackIndex)?
                    .checked_add(IndexFile::TRAILER_LEN)
                    .ok_or(ParseError::InvalidPackIndex)?;
                (size, size)
            }
            IndexFileVersion::V2 => {
                let min_size = count
                    .checked_mul(IndexFile::ENTRY_SIZE_V2 + 4)
                    .ok_or(ParseError::InvalidPackIndex)?
                    .checked_add(IndexFile::TRAILER_LEN)
                    .ok_or(ParseError::InvalidPackIndex)?;
                let max_size = min_size
                    .checked_add(
                        count
                            .saturating_sub(1)
                            .checked_mul(8)
                            .ok_or(ParseError::InvalidPackIndex)?,
                    )
                    .ok_or(ParseError::InvalidPackIndex)?;
                (min_size, max_size)
            }
        };
        if parser.remaining() < min_size || parser.remaining() > max_size {
            return Err(ParseError::InvalidPackIndex);
        }

        Ok(IndexFile {
            data: parser.finish(),
            count,
            version,
        })
    }

    fn find_offset(&self, short_id: &ShortId) -> Result<(usize, Id), Error> {
        let first_byte = short_id.first_byte() as usize;
        let offset_end = self.level_one()[first_byte] as usize;
        let offset_start = self.level_one()[first_byte.saturating_sub(1)] as usize;

        fn binary_search<T: IndexEntry>(
            entries: &[T],
            short_id: &ShortId,
        ) -> Result<(usize, Id), Error> {
            match entries.binary_search_by(|entry| entry.id().cmp_short(short_id)) {
                Ok(index) => Ok((index, *entries[index].id())),
                Err(index) => {
                    let mut matches = entries[index..]
                        .iter()
                        .take_while(|entry| entry.id().starts_with(short_id));
                    let entry = matches
                        .next()
                        .ok_or_else(|| Error::ObjectNotFound(*short_id))?;
                    if matches.next().is_some() {
                        return Err(Error::Ambiguous(*short_id));
                    }
                    Ok((index, *entry.id()))
                }
            }
        }

        match self.version {
            IndexFileVersion::V1 => binary_search(self.entries_v1(offset_start..offset_end)?, short_id),
            IndexFileVersion::V2 => binary_search(self.entries_v2(offset_start..offset_end)?, short_id),
        }
    }

    fn level_one(&self) -> &[u8] {
        &self.data()[..IndexFile::LEVEL_ONE_LEN]
    }

    fn entries_v1(&self, range: Range<usize>) -> Result<&[IndexEntryV1], Error> {
        Ok(
            LayoutVerified::<_, [IndexEntryV1]>::new_slice(self.entries(range))
                .ok_or(Error::InvalidObject(ParseError::InvalidPackIndex))?
                .into_slice(),
        )
    }

    fn entries_v2(&self, range: Range<usize>) -> Result<&[IndexEntryV2], Error> {
        Ok(
            LayoutVerified::<_, [IndexEntryV2]>::new_slice(self.entries(range))
                .ok_or(Error::InvalidObject(ParseError::InvalidPackIndex))?
                .into_slice(),
        )
    }

    fn entries(&self, range: Range<usize>) -> &[u8] {
        let data = self.data();
        &data[IndexFile::LEVEL_ONE_LEN..][..self.count][range]
    }

    fn data(&self) -> &[u8] {
        match self.version {
            IndexFileVersion::V1 => &self.data,
            IndexFileVersion::V2 => &self.data[IndexFile::HEADER_LEN..],
        }
    }

    fn id(&self) -> Id {
        let pos = self.data.len() - ID_LEN * 2;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }

    // TODO: check this
    #[allow(unused)]
    fn crc(&self) -> Id {
        let pos = self.data.len() - ID_LEN;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }
}

trait IndexEntry {
    fn id(&self) -> &Id;
}

impl IndexEntry for IndexEntryV1 {
    fn id(&self) -> &Id {
        &self.id
    }
}

impl IndexEntry for IndexEntryV2 {
    fn id(&self) -> &Id {
        &self.id
    }
}

impl fmt::Debug for PackFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PackFile")
            .field("index", &self.index)
            .field("version", &self.version)
            .finish()
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

#[cfg(test)]
mod tests {
    use std::mem::{align_of, size_of};

    use super::*;

    #[test]
    fn test_entry_layout() {
        assert_eq!(size_of::<IndexEntryV1>(), IndexFile::ENTRY_SIZE_V1);
        assert_eq!(align_of::<IndexEntryV1>(), 1);
        assert_eq!(size_of::<IndexEntryV2>(), IndexFile::ENTRY_SIZE_V2);
        assert_eq!(align_of::<IndexEntryV2>(), 1);
    }
}
