use std::io;
use std::path::PathBuf;

use crate::reference::{Error, Reference};

const HEAD: &str = "HEAD";
const PACKED_REFS: &str = "packed_refs";

#[derive(Debug)]
pub struct ReferenceDatabase {
    path: PathBuf,
}

impl ReferenceDatabase {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        ReferenceDatabase { path: path.into() }
    }

    pub fn read_head(&self) -> Result<impl io::Read, Error> {
        match fs_err::File::open(self.path.join(HEAD)) {
            Ok(file) => Ok(file),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Err(Error::ReferenceNotFound),
            Err(err) => Err(err.into()),
        }
    }

    pub fn read_packed_references(&self) -> Result<impl io::Read, Error> {
        match fs_err::File::open(self.path.join(PACKED_REFS)) {
            Ok(file) => Ok(file),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Err(Error::ReferenceNotFound),
            Err(err) => Err(err.into()),
        }
    }

    pub fn parse_reference(&self, name: &[u8]) -> Result<Reference, Error> {
        // TODO: use name to get reference.
        Ok(Reference::from_reader(self.read_packed_references()?)?)
    }

    pub fn head(&self) -> Result<Reference, Error> {
        Ok(Reference::from_reader(self.read_head()?)?)
    }
}
