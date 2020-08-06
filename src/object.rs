use std::path::PathBuf;
use std::{fs, io};

use thiserror::Error;

#[derive(Debug)]
pub(crate) struct ObjectDatabase {
    path: PathBuf,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("the object database is invalid")]
    Invalid(
        #[source]
        #[from]
        InvalidError,
    ),
    #[error("io error in object database")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
}

#[derive(Debug, Error)]
pub enum InvalidError {
    #[error("objects directory not found at `{0}`")]
    ObjectsDirNotFound(PathBuf),
}

impl ObjectDatabase {
    pub(crate) fn open(path: impl Into<PathBuf>) -> Result<Self, Error> {
        let path = path.into();
        match fs::metadata(&path) {
            Ok(metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(Error::from(InvalidError::ObjectsDirNotFound(path))),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(Error::from(InvalidError::ObjectsDirNotFound(path)))
            }
            Err(err) => return Err(Error::from(err)),
        };

        Ok(ObjectDatabase { path })
    }
}
