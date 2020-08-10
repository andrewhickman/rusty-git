#[cfg(unix)]
use std::ffi::OsStr;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

use std::fs;
use std::io;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::str;

use crate::reference::{Error, Reference};

const REFS: &[u8] = b"refs";
const HEADS: &[u8] = b"heads";
const TAGS: &[u8] = b"tags";
const REMOTES: &[u8] = b"remotes";
const HEAD: &[u8] = b"HEAD";

#[derive(Debug)]
pub struct ReferenceDatabase {
    path: PathBuf,
}

impl ReferenceDatabase {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        ReferenceDatabase { path: path.into() }
    }

    pub fn head(&self) -> Result<Reference, Error> {
        Ok(Reference::from_reader(self.read_head()?)?)
    }

    pub fn reference(&self, name: &[u8]) -> Result<Reference, Error> {
        Ok(Reference::from_reader(self.read_reference_file(name)?)?)
    }

    pub fn reference_names(&self) -> Result<Vec<Vec<u8>>, Error> {
        let mut refs = self.head_reference_names()?;
        refs.append(&mut self.tag_reference_names()?);
        refs.append(&mut self.remote_reference_names()?);
        Ok(refs)
    }

    pub fn head_reference_names(&self) -> Result<Vec<Vec<u8>>, Error> {
        self.reference_names_from_dir(
            &self
                .path
                .join(ReferenceDatabase::bytes_to_path(REFS)?)
                .join(ReferenceDatabase::bytes_to_path(HEADS)?),
        )
    }

    pub fn tag_reference_names(&self) -> Result<Vec<Vec<u8>>, Error> {
        self.reference_names_from_dir(
            &self
                .path
                .join(ReferenceDatabase::bytes_to_path(REFS)?)
                .join(ReferenceDatabase::bytes_to_path(TAGS)?),
        )
    }

    pub fn remote_reference_names(&self) -> Result<Vec<Vec<u8>>, Error> {
        self.reference_names_from_dir(
            &self
                .path
                .join(ReferenceDatabase::bytes_to_path(REFS)?)
                .join(ReferenceDatabase::bytes_to_path(REMOTES)?),
        )
    }

    pub fn read_head(&self) -> Result<impl io::Read, Error> {
        self.read_reference_file(HEAD)
    }

    pub fn read_reference_file(&self, name: &[u8]) -> Result<impl io::Read, Error> {
        match fs_err::File::open(self.path.join(ReferenceDatabase::bytes_to_path(name)?)) {
            Ok(file) => Ok(file),
            Err(err) if err.kind() == io::ErrorKind::NotFound => Err(Error::ReferenceNotFound),
            Err(err) => Err(err.into()),
        }
    }

    pub fn parse_reference(&self, name: &[u8]) -> Result<Reference, Error> {
        // TODO: use name to get reference.
        Ok(Reference::from_reader(self.read_reference_file(name)?)?)
    }

    fn reference_names_from_dir(&self, path: &Path) -> Result<Vec<Vec<u8>>, Error> {
        let files = fs::read_dir(path).map_err(Error::Io)?;
        Result::<Vec<Vec<u8>>, Error>::from_iter(
            files
                .filter(|f| f.is_ok())
                .map(|f| f.unwrap())
                .map(|f| f.path())
                .map(|p| pathdiff::diff_paths(p, &self.path).unwrap())
                .map(|p| {
                    ReferenceDatabase::path_to_bytes(&p).map(|bytes| {
                        bytes
                            .iter()
                            .map(|b| match b {
                                b'\\' => b'/',
                                _ => *b,
                            })
                            .collect::<Vec<u8>>()
                    })
                })
                .collect::<Vec<Result<Vec<u8>, Error>>>(),
        )
    }

    #[cfg(windows)]
    fn path_to_bytes(path: &Path) -> Result<&[u8], Error> {
        Ok(path
            .as_os_str()
            .to_str()
            .ok_or(Error::ReferenceNameInvalidUtf16)?
            .as_bytes())
    }

    #[cfg(unix)]
    fn path_to_bytes(path: &Path) -> Result<&[u8], Error> {
        Ok(path.as_os_str().as_bytes())
    }

    #[cfg(windows)]
    fn bytes_to_path(bytes: &[u8]) -> Result<&Path, Error> {
        Ok(Path::new(
            str::from_utf8(bytes).map_err(|_| Error::ReferenceNameInvalidUtf8)?,
        ))
    }

    #[cfg(unix)]
    fn bytes_to_path(bytes: &[u8]) -> Result<&Path, Error> {
        Ok(OsStr::from_bytes(bytes)
            .to_path()
            .map_err(|_| Error::ReferenceNameInvalidUtf8)?)
    }
}
