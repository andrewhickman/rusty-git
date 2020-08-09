mod blob;
mod commit;
mod database;
mod parser;
mod signature;
mod tag;
mod tree;

pub use self::blob::Blob;
pub use self::commit::Commit;
pub use self::database::ObjectDatabase;
pub use self::signature::Signature;
pub use self::tag::Tag;
pub use self::tree::{Tree, TreeEntry};

use std::convert::TryInto;
use std::fmt;
use std::io::{self, Cursor};
use std::str::FromStr;

use hex::FromHex;
use sha1::digest::Digest;
use sha1::Sha1;
use thiserror::Error;

use self::parser::{ObjectKind, ParseError, Parser};

pub const ID_LEN: usize = 20;
pub const ID_HEX_LEN: usize = ID_LEN * 2;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; ID_LEN]);

#[derive(Debug)]
pub enum ObjectData {
    Commit(Commit),
    Tree(Tree),
    Blob(Blob),
    Tag(Tag),
}

#[derive(Debug)]
pub struct Object {
    id: Id,
    data: ObjectData,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("object `{0}` not found")]
    ObjectNotFound(Box<Id>),
    #[error("the object database is invalid")]
    InvalidObject(
        #[source]
        #[from]
        ParseError,
    ),
    #[error("io error in object database")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
}

impl ObjectData {
    pub fn from_reader<R: io::Read>(reader: R) -> Result<Self, ParseError> {
        Parser::new(reader).parse()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        Parser::new(Cursor::new(bytes)).parse()
    }
}

impl Object {
    pub fn from_reader<R: io::Read>(id: Id, reader: R) -> Result<Self, ParseError> {
        Ok(Object {
            data: ObjectData::from_reader(reader)?,
            id,
        })
    }

    pub fn from_bytes(id: Id, bytes: &[u8]) -> Result<Self, ParseError> {
        Ok(Object {
            data: ObjectData::from_bytes(bytes)?,
            id,
        })
    }

    pub fn id(&self) -> &Id {
        &self.id
    }

    pub fn data(&self) -> &ObjectData {
        &self.data
    }
}

impl Id {
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Id(bytes.try_into().expect("invalid length for id"))
    }

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
