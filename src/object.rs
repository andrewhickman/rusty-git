mod blob;
mod commit;
mod database;
mod parser;
mod tag;
mod tree;

pub use self::blob::Blob;
pub use self::commit::Commit;
pub use self::database::ObjectDatabase;
pub use self::tag::Tag;
pub use self::tree::{Tree, TreeEntry};

use std::fmt;
use std::io::{self, Cursor};
use std::str::FromStr;
use std::convert::TryInto;

use hex::FromHex;
use sha1::digest::Digest;
use sha1::Sha1;
use thiserror::Error;

use self::parser::{ParseError, Parser};

pub const ID_LEN: usize = 20;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Id([u8; ID_LEN]);

#[derive(Debug)]
pub enum Object {
    Commit(Commit),
    Tree(Tree),
    Blob(Blob),
    Tag(Tag),
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

impl Object {
    pub fn from_reader<R: io::Read>(reader: R) -> Result<Self, ParseError> {
        Parser::new(reader).parse()
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, ParseError> {
        Parser::new(Cursor::new(bytes)).parse()
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
