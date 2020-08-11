use std::convert::TryFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use std::fmt;

use dashmap::DashMap;

use crate::object::database::Reader;
use crate::object::{Error, Id, ParseError, Parser, ID_LEN};

const PACKS_FOLDER: &str = "objects/pack";
const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const INDEX_SIGNATURE: u32 = 0xff744f63;
const PACK_SIGNATURE: u32 = 0x5041434b;
const INDEX_ENTRY_COUNT: usize = 256;

struct PackFile {
    index: IndexFile,
    version: PackFileVersion,
    data: Box<[u8]>,
}

struct IndexFile {
    data: Box<[u8]>,
    version: IndexFileVersion,
    count: usize,
    pos: usize,
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

    pub fn read_object(&self, id: &Id) -> Result<Reader, Error> {
        self.refresh()?;

        Err(Error::ObjectNotFound(*id))
    }

    fn refresh(&self) -> Result<(), Error> {
        // Keep the mutex locked while refreshing so we don't have multiple thread refreshing simultaneously.
        // This isn't necessary for correctness, but is just an optimization.
        let mut last_refresh_guard = self.last_refresh.lock().unwrap();
        match *last_refresh_guard {
            Some(last_refresh) if last_refresh.elapsed() < MAX_REFRESH_INTERVAL => return Ok(()),
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
        Ok(())
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
            index, version, data: parser.finish(),
        };

        if pack_file.index.id() != pack_file.id() {
            return Err(ParseError::InvalidPack);
        }

        Ok(pack_file)
    }

    fn id(&self) -> Id {
        let pos = self.data.len() - ID_LEN;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }
}

impl IndexFile {
    fn open(path: PathBuf) -> Result<IndexFile, ParseError> {
        let mut parser = Parser::from_file(path)?;

        let version = if parser.consume_u32(INDEX_SIGNATURE) {
            let version = parser.parse_u32()?;

            match version {
                2 => IndexFileVersion::V2,
                _ => return Err(ParseError::UnknownPackVersion),
            }
        } else {
            IndexFileVersion::V1
        };

        let mut count = 0;
        for _ in 0..INDEX_ENTRY_COUNT {
            let n = parser.parse_u32()?;
            if n < count {
                return Err(ParseError::NonMonotonicPackIndex);
            }
            count = n;
        }
        let count = usize::try_from(count).or(Err(ParseError::InvalidPackIndex))?;

        let pos = parser.pos();

        let (min_size, max_size) = match version {
            IndexFileVersion::V1 => {
                let size = count
                    .checked_mul(ID_LEN + 4)
                    .ok_or(ParseError::InvalidPackIndex)?
                    .checked_add(ID_LEN * 2)
                    .ok_or(ParseError::InvalidPackIndex)?;
                (size, size)
            }
            IndexFileVersion::V2 => {
                let min_size = count
                    .checked_mul(ID_LEN + 4 + 4)
                    .ok_or(ParseError::InvalidPackIndex)?
                    .checked_add(ID_LEN * 2)
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
            pos,
        })
    }

    fn id(&self) -> Id {
        let pos = self.data.len() - ID_LEN * 2;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }

    // TODO: check this
    #[allow(unused)]
    fn checksum(&self) -> Id {
        let pos = self.data.len() - ID_LEN;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
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
            .field("pos", &self.pos)
            .field("version", &self.version)
            .field("count", &self.count)
            .field("id", &self.id())
            .finish()
    }
}
