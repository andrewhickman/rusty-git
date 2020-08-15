mod loose;
mod packed;

use std::io;
use std::path::Path;

use thiserror::Error;

use self::loose::{LooseObjectDatabase, ReadLooseError, WriteLooseError};
use self::packed::{PackedObjectDatabase, ReadPackedError};
use crate::object::{Id, Object, ReadObjectError, ShortId};

type Reader = flate2::read::ZlibDecoder<fs_err::File>;

#[derive(Debug)]
pub struct ObjectDatabase {
    loose: LooseObjectDatabase,
    packed: PackedObjectDatabase,
}

#[derive(Debug, Error)]
pub(in crate::object) enum ReadError {
    #[error("the object id was not found")]
    NotFound,
    #[error("the object id is ambiguous")]
    Ambiguous,
    #[error(transparent)]
    Loose(loose::ReadLooseError),
    #[error(transparent)]
    Packed(packed::ReadPackedError),
}

#[derive(Debug, Error)]
#[error(transparent)]
pub struct WriteError {
    kind: WriteErrorKind,
}

#[derive(Debug, Error)]
pub(in crate::object) enum WriteErrorKind {
    #[error(transparent)]
    Loose(#[from] loose::WriteLooseError),
}

impl ObjectDatabase {
    pub fn open(dotgit: &Path) -> Self {
        ObjectDatabase {
            loose: LooseObjectDatabase::open(dotgit),
            packed: PackedObjectDatabase::open(dotgit),
        }
    }

    pub fn parse_object(&self, id: Id) -> Result<Object, ReadObjectError> {
        match Object::from_reader(id, self.read_object(id)?) {
            Ok(object) => Ok(object),
            Err(err) => Err(ReadObjectError::new(id, err)),
        }
    }

    pub fn read_object(&self, id: Id) -> Result<impl io::Read, ReadObjectError> {
        match self.packed.read_object(&ShortId::from(id)) {
            Ok(reader) => return Ok(reader),
            Err(ReadPackedError::NotFound) => (),
            Err(err) => return Err(ReadObjectError::new(id, ReadError::from(err))),
        };

        match self.loose.read_object(&id) {
            Ok(reader) => return Ok(reader),
            Err(ReadLooseError::NotFound) => (),
            Err(err) => return Err(ReadObjectError::new(id, ReadError::from(err))),
        }

        // object may have just been packed, try again
        self.packed.read_object(&ShortId::from(id))
            .map_err(|err |ReadObjectError::new(id, ReadError::from(err)))
    }

    pub fn write_object(&self, bytes: &[u8]) -> Result<Id, WriteError> {
        Ok(self.loose.write_object(bytes)?)
    }
}

impl From<ReadLooseError> for ReadError {
    fn from(err: ReadLooseError) -> Self {
        match err {
            ReadLooseError::NotFound => ReadError::NotFound,
            ReadLooseError::Ambiguous => ReadError::Ambiguous,
            err => ReadError::Loose(err),
        }
    }
}

impl From<ReadPackedError> for ReadError {
    fn from(err: ReadPackedError) -> Self {
        match err {
            ReadPackedError::NotFound => ReadError::NotFound,
            ReadPackedError::Ambiguous => ReadError::Ambiguous,
            err => ReadError::Packed(err),
        }
    }
}

impl From<WriteLooseError> for WriteError {
    fn from(err: WriteLooseError) -> Self {
        WriteError {
            kind: err.into()
        }
    }
}
