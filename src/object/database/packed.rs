use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;

use crate::object::{Error, ParseError, Parser};

const PACKS_FOLDER: &str = "objects/pack";
const MAX_REFRESH_INTERVAL: Duration = Duration::from_secs(2);
const INDEX_SIGNATURE: u32 = 0xff744f63;

#[derive(Debug)]
struct PackFile {
    data: Vec<u8>,
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

    fn refresh(&self) -> Result<(), Error> {
        // Keep the mutex locked while refreshing so we don't have multiple thread refreshing simultaneously.
        // This isn't necessary for correctness, but is just an optimization.
        let mut last_refresh_guard = self.last_refresh.lock().unwrap();
        match *last_refresh_guard {
            Some(last_refresh) if last_refresh.elapsed() < MAX_REFRESH_INTERVAL => return Ok(()),
            _ => (),
        }

        for entry in fs_err::read_dir(&self.path)? {
            let entry = entry?;

            let path = entry.path();
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
    fn open(path: PathBuf) -> Result<PackFile, ParseError> {
        let mut parser = Parser::from_file(path)?;

        if parser.consume_u32(INDEX_SIGNATURE) {
            let version = parser.parse_u32()?;

            match version {
                2 => PackFile::parse_v2(parser),
                _ => Err(ParseError::UnknownPackVersion),
            }
        } else {
            PackFile::parse_v1(parser)
        }
    }

    fn parse_v1<R>(parser: Parser<R>) -> Result<Self, ParseError> {
        todo!()
    }

    fn parse_v2<R>(parser: Parser<R>) -> Result<Self, ParseError> {
        todo!()
    }
}
