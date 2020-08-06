use std::path::PathBuf;
use std::{fs, io};

use thiserror::Error;

const DOTGIT_FOLDER: &str = ".git";
const OBJECTS_FOLDER: &str = "objects";

#[derive(Debug)]
pub struct Repository {
    root: PathBuf,
    dotgit: PathBuf,
    objects: PathBuf,
}

impl Repository {
    pub fn open<P>(path: P) -> Result<Repository, OpenError>
    where
        P: Into<PathBuf>,
    {
        let root = path.into();

        let dotgit = root.join(DOTGIT_FOLDER);
        match fs::metadata(&dotgit) {
            Ok(metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(OpenError::NotFound(root)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(OpenError::NotFound(root))
            }
            Err(err) => return Err(OpenError::from(err)),
        };

        let objects = dotgit.join(OBJECTS_FOLDER);
        match fs::metadata(&dotgit) {
            Ok(metadata) if metadata.is_dir() => (),
            Ok(_) => return Err(OpenError::from(InvalidError::ObjectsDirNotFound)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                return Err(OpenError::from(InvalidError::ObjectsDirNotFound))
            }
            Err(err) => return Err(OpenError::from(err)),
        };

        Ok(Repository {
            root,
            dotgit,
            objects,
        })
    }
}

#[derive(Debug, Error)]
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
pub enum InvalidError {
    #[error("objects directory not found")]
    ObjectsDirNotFound,
}
