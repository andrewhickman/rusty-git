use std::fmt;
use std::fs::OpenOptions;
use std::io::{self, Write as _};
use std::path::PathBuf;
use std::str::FromStr;

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use hex::FromHex;
use sha1::digest::Digest;
use sha1::Sha1;

use thiserror::Error;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; 20]);

#[derive(Debug)]
pub struct ObjectDatabase {
    path: PathBuf,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("object `{0}` not found")]
    ObjectNotFound(Box<Id>),
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
pub enum InvalidError {}

impl Id {
    pub fn from_hash(bytes: &[u8]) -> Self {
        Id(Sha1::new().chain(bytes).finalize().into())
    }

    pub fn from_hex(hex: &[u8]) -> Result<Self, hex::FromHexError> {
        FromHex::from_hex(hex).map(Id)
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }
}

impl ObjectDatabase {
    pub fn open(path: impl Into<PathBuf>) -> Self {
        ObjectDatabase { path: path.into() }
    }

    pub fn read_object(&self, id: &Id) -> Result<impl io::Read, Error> {
        let hex = id.to_hex();
        let (dir, file) = object_path_parts(&hex);
        let mut path = self.path.join(dir);
        path.push(file);

        match fs_err::File::open(path) {
            Ok(file) => Ok(ZlibDecoder::new(file)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(Error::ObjectNotFound(Box::new(*id)))
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn write_object(&self, bytes: &[u8]) -> Result<(), Error> {
        let id = Id::from_hash(bytes);
        let hex = id.to_hex();
        let (dir, file) = object_path_parts(&hex);

        let mut path = self.path.join(dir);
        fs_err::create_dir(&path)?;

        path.push(file);
        let file = match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => fs_err::File::from_parts(file, path),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        let mut encoder = ZlibEncoder::new(file, Compression::best());
        encoder.write_all(bytes)?;
        encoder.finish()?;
        Ok(())
    }
}

fn object_path_parts(hex: &str) -> (&str, &str) {
    hex.split_at(2)
}

impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.to_hex().fmt(f)
    }
}

impl fmt::Debug for Id {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for Id {
    type Err = hex::FromHexError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Id::from_hex(input.as_bytes())
    }
}