use std::fs::OpenOptions;
use std::io::{self, Write as _};
use std::path::PathBuf;

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;

use crate::object::{Error, Id, Object};

#[derive(Debug)]
pub struct ObjectDatabase {
    path: PathBuf,
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

    pub fn parse_object(&self, id: &Id) -> Result<Object, Error> {
        Ok(Object::from_reader(*id, self.read_object(id)?)?)
    }

    pub fn write_object(&self, bytes: &[u8]) -> Result<(), Error> {
        let id = Id::from_hash(bytes);
        let hex = id.to_hex();
        let (dir, file) = object_path_parts(&hex);

        let mut path = self.path.join(dir);
        match fs_err::create_dir(&path) {
            Err(err) if err.kind() != io::ErrorKind::AlreadyExists => return Err(err.into()),
            _ => (),
        }

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
