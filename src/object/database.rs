mod loose;
mod packed;

use std::io;
use std::path::Path;

use self::loose::LooseObjectDatabase;
use self::packed::PackedObjectDatabase;
use crate::object::{Error, Id, Object, ShortId};

type Reader = flate2::read::ZlibDecoder<fs_err::File>;

#[derive(Debug)]
pub struct ObjectDatabase {
    loose: LooseObjectDatabase,
    packed: PackedObjectDatabase,
}

impl ObjectDatabase {
    pub fn open(dotgit: &Path) -> Self {
        ObjectDatabase {
            loose: LooseObjectDatabase::open(dotgit),
            packed: PackedObjectDatabase::open(dotgit),
        }
    }

    pub fn parse_object(&self, id: &Id) -> Result<Object, Error> {
        Ok(Object::from_reader(*id, self.read_object(id)?)?)
    }

    pub fn read_object(&self, id: &Id) -> Result<impl io::Read, Error> {
        match self.packed.read_object(&ShortId::from(*id)) {
            Ok(reader) => return Ok(reader),
            Err(Error::ObjectNotFound(_)) => (),
            Err(err) => return Err(err),
        };

        match self.loose.read_object(id) {
            Ok(reader) => return Ok(reader),
            Err(Error::ObjectNotFound(_)) => (),
            Err(err) => return Err(err),
        }

        // object may have just been packed, try again
        self.packed.read_object(&ShortId::from(*id))
    }

    pub fn write_object(&self, bytes: &[u8]) -> Result<Id, Error> {
        self.loose.write_object(bytes)
    }
}
