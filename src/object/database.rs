mod loose;

use std::io;
use std::path::Path;

use crate::object::{Error, Id, Object};
use self::loose::LooseObjectDatabase;

#[derive(Debug)]
pub struct ObjectDatabase {
    loose: LooseObjectDatabase,
}

impl ObjectDatabase {
    pub fn open(dotgit: &Path) -> Self {
        ObjectDatabase {
            loose: LooseObjectDatabase::open(dotgit)
        }
    }

    pub fn parse_object(&self, id: &Id) -> Result<Object, Error> {
        Ok(Object::from_reader(*id, self.read_object(id)?)?)
    }

    pub fn read_object(&self, id: &Id) -> Result<impl io::Read, Error> {
        self.loose.read_object(id)
    }

    pub fn write_object(&self, bytes: &[u8]) -> Result<(), Error> {
        self.loose.write_object(bytes)
    }
}