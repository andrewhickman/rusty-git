mod index;
mod pack;

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use self::index::IndexFile;
use self::pack::PackFile;
use crate::object::database::Reader;
use crate::object::{Error, ShortId};

const PACKS_FOLDER: &str = "objects/pack";
const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug)]
pub struct PackedObjectDatabase {
    path: PathBuf,
    // last: Mutex<Arc<PackFile>>, why is this useful?
    packs: DashMap<PathBuf, Arc<Entry>>,
    last_refresh: Mutex<Option<Instant>>,
}

#[derive(Debug)]
struct Entry {
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

    pub fn read_object(&self, short_id: &ShortId) -> Result<Reader, Error> {
        match self.try_read_object(short_id) {
            Err(Error::ObjectNotFound(_)) if self.refresh()? => self.try_read_object(short_id),
            result => result,
        }
    }

    fn try_read_object(&self, short_id: &ShortId) -> Result<Reader, Error> {
        for entry in self.packs.iter() {
            match entry.value().index.find_offset(short_id) {
                Ok((offset, _)) => return entry.value().pack.read_object(offset),
                Err(Error::ObjectNotFound(_)) => continue,
                Err(err) => return Err(err),
            }
        }

        return Err(Error::ObjectNotFound(*short_id));
    }

    fn refresh(&self) -> Result<bool, Error> {
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
    fn open(path: PathBuf) -> Result<Self, Error> {
        let pack = PackFile::open(path.with_extension("pack"))?;
        let index = IndexFile::open(path)?;

        // if index.count != entry_count {
        //     return Err(ParseError::InvalidPackIndex(
        //         "index count does not match pack file count",
        //     ));
        // }

        // if pack_file.index.id() != pack_file.id() {
        //     return Err(ParseError::InvalidPack);
        // }

        Ok(Entry { pack, index })
    }
}
