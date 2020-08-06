use std::path::PathBuf;
use std::{fs, io};

use thiserror::Error;

use crate::object::{self, ObjectDatabase};

const DOTGIT_FOLDER: &str = ".git";
const OBJECTS_FOLDER: &str = "objects";

#[derive(Debug)]
pub struct Repository {
    workdir: PathBuf,
    dotgit: PathBuf,
    objects: ObjectDatabase,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenError {
    #[error("repository not found at `{0}`")]
    NotFound(PathBuf),
    #[error("repository at `{0}` is invalid")]
    Invalid(
        #[source]
        #[from]
        InvalidError,
    ),
    #[error("io error opening repository")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum InvalidError {
    #[error("objects database is invalid")]
    ObjectDatabase(
        #[source]
        #[from]
        object::InvalidError,
    ),
}

impl Repository {
    pub fn open<P>(path: P) -> Result<Repository, OpenError>
    where
        P: Into<PathBuf>,
    {
        let path = path.into();

        let dotgit = path.join(DOTGIT_FOLDER);
        match fs::metadata(&dotgit) {
            Ok(metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(OpenError::NotFound(path)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(OpenError::NotFound(path))
            }
            Err(err) => return Err(OpenError::from(err)),
        };

        let objects = ObjectDatabase::open(dotgit.join(OBJECTS_FOLDER))?;

        Ok(Repository {
            workdir: path,
            dotgit,
            objects,
        })
    }
}

impl From<object::Error> for OpenError {
    fn from(err: object::Error) -> OpenError {
        match err {
            object::Error::Invalid(err) => OpenError::Invalid(err.into()),
            object::Error::Io(err) => OpenError::Io(err),
        }
    }
}
