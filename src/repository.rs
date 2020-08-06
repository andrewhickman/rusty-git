use std::io;
use std::path::PathBuf;

use thiserror::Error;

use crate::object::{self, ObjectDatabase};

const DOTGIT_FOLDER: &str = ".git";
const OBJECTS_FOLDER: &str = "objects";

#[derive(Debug)]
pub struct Repository {
    workdir: PathBuf,
    dotgit: PathBuf,
    object_database: ObjectDatabase,
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum OpenError {
    #[error("repository not found at `{0}`")]
    NotFound(PathBuf),
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
        match fs_err::metadata(&dotgit) {
            Ok(metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(OpenError::NotFound(path)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(OpenError::NotFound(path))
            }
            Err(err) => return Err(OpenError::from(err)),
        };

        let object_database = ObjectDatabase::open(dotgit.join(OBJECTS_FOLDER));

        Ok(Repository {
            workdir: path,
            dotgit,
            object_database,
        })
    }

    pub fn object_database(&self) -> &ObjectDatabase {
        &self.object_database
    }
}
