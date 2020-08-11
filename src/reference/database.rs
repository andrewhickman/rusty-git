use std::fs::{self};
use std::io;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::ffi::OsStr;
#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;

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
        let files = self.get_all_file_paths_from_dir(&path)?;
        files
            .iter()
            .map(|p| self.reference_name_from_file(p))
            .collect()
    }

    fn get_all_file_paths_from_dir(&self, path: &Path) -> Result<Vec<PathBuf>, Error> {
        Result::<Vec<PathBuf>, Error>::from_iter(self.get_file_paths_from_dir(&path))
    }

    fn get_file_paths_from_dir(&self, path: &Path) -> Vec<Result<PathBuf, Error>> {
        match fs::read_dir(path) {
            Ok(files) => files
                .filter(|f| f.is_ok())
                .map(|f| f.unwrap())
                .flat_map(|f| match f.file_type().map(|ft| ft.is_dir()) {
                    Ok(true) => self.get_file_paths_from_dir(&f.path()),
                    Ok(false) => vec![f.path()].into_iter().map(Ok).collect(),
                    Err(error) => vec![Err(Error::Io(error))],
                })
                .collect(),
            Err(error) => vec![Err(Error::Io(error))],
        }
    }

    fn reference_name_from_file(&self, path: &Path) -> Result<Vec<u8>, Error> {
        ReferenceDatabase::path_to_bytes(&pathdiff::diff_paths(path, &self.path).unwrap()).map(
            |bytes| {
                bytes
                    .iter()
                    .map(|b| match b {
                        b'\\' => b'/',
                        _ => *b,
                    })
                    .collect()
            },
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
            std::str::from_utf8(bytes).map_err(|_| Error::ReferenceNameInvalidUtf8)?,
        ))
    }

    #[cfg(unix)]
    fn bytes_to_path(bytes: &[u8]) -> Result<&Path, Error> {
        Ok(OsStr::from_bytes(bytes)
            .map_err(|_| Error::ReferenceNameInvalidUtf8)?
            .as_ref())
    }
}
