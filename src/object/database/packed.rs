mod index;
mod pack;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use self::index::{FindIndexOffsetError, IndexFile, ReadIndexFileError};
use self::pack::{PackFile, ReadPackFileError};
use crate::object::database::Reader;
use crate::object::ShortId;
use thiserror::Error;

const PACKS_FOLDER: &str = "objects/pack";
const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct PackedObjectDatabase {
    path: PathBuf,
    // last: Mutex<Arc<PackFile>>, why is this useful?
    packs: DashMap<PathBuf, Arc<Entry>>,
    last_refresh: Mutex<Option<Instant>>,
}

#[derive(Debug, Error)]
pub(in crate::object) enum ReadPackedError {
    #[error("the object id was not found in the packed database")]
    NotFound,
    #[error("the object id is ambiguous in the packed database")]
    Ambiguous,
    #[error(transparent)]
    ReadEntry(#[from] ReadEntryError),
    #[error("io error reading from the packed object database")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
}

#[derive(Debug, Error)]
#[error("failed to read packed database entry {name}")]
pub(in crate::object) struct ReadEntryError {
    name: String,
    #[source]
    kind: ReadEntryErrorKind,
}

#[derive(Debug, Error)]
enum ReadEntryErrorKind {
    #[error("failed to read the index file")]
    ReadIndexFile(ReadIndexFileError),
    #[error("failed to read the pack file")]
    ReadPackFile(ReadPackFileError),
    #[error("the index file and pack file have a different number of entries")]
    CountMismatch,
    #[error("the index file and pack file have a different id")]
    IdMismatch,
}

#[derive(Debug)]
struct Entry {
    name: String,
    index: IndexFile,
    pack: PackFile,
}

impl PackedObjectDatabase {
    pub fn open(path: &Path) -> Self {
        PackedObjectDatabase {
            path: path.join(PACKS_FOLDER),
            packs: DashMap::new(),
            last_refresh: Mutex::new(None),
        }
    }

    pub(in crate::object::database) fn read_object(
        &self,
        short_id: &ShortId,
    ) -> Result<Reader, ReadPackedError> {
        match self.try_read_object(short_id) {
            Err(ReadPackedError::NotFound) if self.refresh()? => self.try_read_object(short_id),
            result => result,
        }
    }

    fn try_read_object(&self, short_id: &ShortId) -> Result<Reader, ReadPackedError> {
        let mut result = None;
        let mut found_id = None;
        for entry in self.packs.iter() {
            match entry.value().index.find_offset(short_id) {
                Err(FindIndexOffsetError::Ambiguous) => return Err(ReadPackedError::Ambiguous),
                Ok((_, id)) if found_id.is_some() && found_id != Some(id) => {
                    return Err(ReadPackedError::Ambiguous)
                }
                Ok((offset, id)) => {
                    found_id = Some(id);
                    result = Some((entry.value().clone(), offset))
                }
                Err(FindIndexOffsetError::NotFound) => continue,
                Err(FindIndexOffsetError::ReadIndexFile(err)) => {
                    return Err(ReadPackedError::ReadEntry(ReadEntryError {
                        name: entry.name.clone(),
                        kind: ReadEntryErrorKind::ReadIndexFile(err),
                    }))
                }
            }
        }

        match result {
            Some((entry, offset)) => match entry.pack.read_object(offset) {
                Ok(reader) => Ok(reader),
                Err(err) => Err(ReadPackedError::ReadEntry(ReadEntryError {
                    name: entry.name.clone(),
                    kind: ReadEntryErrorKind::ReadPackFile(err),
                })),
            },
            None => Err(ReadPackedError::NotFound),
        }
    }

    fn refresh(&self) -> Result<bool, ReadPackedError> {
        // Keep the mutex locked while refreshing so we don't have multiple thread refreshing simultaneously.
        // This isn't necessary for correctness, but is just an optimization.
        let mut last_refresh_guard = self.last_refresh.lock().unwrap();
        match *last_refresh_guard {
            Some(last_refresh) if last_refresh.elapsed() < MAX_REFRESH_INTERVAL => {
                return Ok(false)
            }
            _ => (),
        }

        for entry in fs_err::read_dir(&self.path)? {
            let path = entry?.path();
            if path.extension() == Some("idx".as_ref()) {
                self.packs
                    .entry(path.clone())
                    .or_try_insert_with(move || Entry::open(path).map(Arc::new))?;
            }
        }

        *last_refresh_guard = Some(Instant::now());
        Ok(true)
    }
}

impl Entry {
    fn open(path: PathBuf) -> Result<Self, ReadEntryError> {
        // The file has an extension so it must have a file name
        let name = path.file_name().unwrap().to_string_lossy().into_owned();

        let index = match IndexFile::open(path.clone()) {
            Ok(index) => index,
            Err(err) => {
                return Err(ReadEntryError {
                    name,
                    kind: ReadEntryErrorKind::ReadIndexFile(err),
                })
            }
        };

        let pack = match PackFile::open(path.with_extension("pack")) {
            Ok(pack) => pack,
            Err(err) => {
                return Err(ReadEntryError {
                    name,
                    kind: ReadEntryErrorKind::ReadPackFile(err),
                })
            }
        };

        if index.count() != pack.count() {
            return Err(ReadEntryError {
                name,
                kind: ReadEntryErrorKind::CountMismatch,
            });
        }

        if index.id() != pack.id() {
            return Err(ReadEntryError {
                name,
                kind: ReadEntryErrorKind::IdMismatch,
            });
        }

        Ok(Entry { pack, index, name })
    }
}
