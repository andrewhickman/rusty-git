mod blob;
mod commit;
mod database;
mod parse;
mod signature;
mod tag;
mod tree;

pub use self::blob::Blob;
pub use self::commit::Commit;
pub use self::database::ObjectDatabase;
pub use self::signature::Signature;
pub use self::tag::Tag;
pub use self::tree::{Tree, TreeEntry};

use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt;
use std::io;
use std::str::FromStr;

use hex::FromHex;
use sha1::digest::Digest;
use sha1::Sha1;
use thiserror::Error;
use zerocopy::FromBytes;

use self::blob::ParseBlobError;
use self::commit::ParseCommitError;
use self::parse::ParseObjectError;
use self::tag::ParseTagError;
use self::tree::ParseTreeError;
use crate::parse::{Buffer, Parser};

pub const ID_LEN: usize = 20;
pub const ID_HEX_LEN: usize = ID_LEN * 2;

pub const SHORT_ID_MIN_LEN: usize = 2;
pub const SHORT_ID_MIN_HEX_LEN: usize = SHORT_ID_MIN_LEN * 2;

#[repr(transparent)]
#[derive(Copy, Clone, Default, PartialEq, Eq, PartialOrd, Ord, Hash, FromBytes)]
pub struct Id([u8; ID_LEN]);

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShortId {
    id: [u8; ID_LEN],
    len: u32,
}

#[derive(Debug, Clone)]
pub enum ObjectData {
    Commit(Commit),
    Tree(Tree),
    Blob(Blob),
    Tag(Tag),
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum ObjectKind {
    Commit = 1,
    Tree = 2,
    Blob = 3,
    Tag = 4,
    OfsDelta = 6,
    RefDelta = 7,
}

#[derive(Debug, Clone)]
pub struct Object {
    id: Id,
    data: ObjectData,
}

/// An error when reading an object from the database.
#[derive(Debug)]
pub struct ReadObjectError {
    id: ShortId,
    kind: ReadObjectErrorKind,
}

#[derive(Debug)]
enum ReadObjectErrorKind {
    Database(database::ReadError),
    Parse(ParseObjectError),
    Io(io::Error),
}

#[derive(Debug, Error)]
pub enum ParseIdError {
    #[error("ids must be at least {} characters long", SHORT_ID_MIN_HEX_LEN)]
    TooShort,
    #[error("ids can be at most {} characters long", ID_HEX_LEN)]
    TooLong,
    #[error(transparent)]
    Hex(#[from] hex::FromHexError),
}

impl ObjectData {
    fn from_reader<R: io::Read>(reader: R) -> Result<Self, ParseObjectError> {
        Buffer::new(reader).read_object()
    }
}

impl Object {
    fn from_reader<R: io::Read>(id: Id, reader: R) -> Result<Self, ParseObjectError> {
        Ok(Object {
            data: ObjectData::from_reader(reader)?,
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

    pub fn from_hex(hex: &[u8]) -> Result<Self, ParseIdError> {
        Ok(Id(FromHex::from_hex(hex)?))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
    }

    fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    fn starts_with(&self, short_id: &ShortId) -> bool {
        self.0.starts_with(short_id.as_bytes())
    }

    pub fn cmp_short(&self, short_id: &ShortId) -> Ordering {
        short_id.cmp_id(self).reverse()
    }
}

impl ShortId {
    fn first_byte(&self) -> u8 {
        self.id[0]
    }

    fn as_bytes(&self) -> &[u8] {
        &self.id[..(self.len as usize)]
    }

    /// Compare to an id. Partial ids are sorted just before ids they are a prefix of.
    pub fn cmp_id(&self, id: &Id) -> Ordering {
        self.as_bytes().cmp(id.as_bytes())
    }

    pub fn from_hex(hex: &[u8]) -> Result<Self, ParseIdError> {
        if hex.len() < SHORT_ID_MIN_HEX_LEN {
            return Err(ParseIdError::TooShort);
        }
        if hex.len() > ID_HEX_LEN {
            return Err(ParseIdError::TooLong);
        }

        let mut id = [0; ID_LEN];
        let len = hex.len() / 2;
        hex::decode_to_slice(hex, &mut id[..len])?;
        Ok(ShortId {
            id,
            len: len as u32,
        })
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.as_bytes())
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
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Id::from_hex(input.as_bytes())
    }
}

impl fmt::Display for ShortId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        self.to_hex().fmt(f)
    }
}

impl fmt::Debug for ShortId {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl FromStr for ShortId {
    type Err = ParseIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        ShortId::from_hex(input.as_bytes())
    }
}

impl From<Id> for ShortId {
    fn from(id: Id) -> Self {
        ShortId {
            id: id.0,
            len: ID_LEN as u32,
        }
    }
}

impl ReadObjectError {
    fn new(id: impl Into<ShortId>, kind: impl Into<ReadObjectErrorKind>) -> Self {
        ReadObjectError {
            id: id.into(),
            kind: kind.into(),
        }
    }
}

impl fmt::Display for ReadObjectError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self.kind {
            ReadObjectErrorKind::Database(database::ReadError::NotFound) => {
                write!(f, "object id `{}` was not found", self.id)
            }
            ReadObjectErrorKind::Database(database::ReadError::Ambiguous) => {
                write!(f, "object id `{}` is ambiguous", self.id)
            }
            ReadObjectErrorKind::Database(_) => {
                write!(f, "failed to read object `{}` from the database", self.id)
            }
            ReadObjectErrorKind::Parse(_) => write!(f, "object `{}` is invalid", self.id),
            ReadObjectErrorKind::Io(_) => write!(f, "io error reading object `{}`", self.id),
        }
    }
}

impl std::error::Error for ReadObjectError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self.kind {
            ReadObjectErrorKind::Database(database::ReadError::NotFound) => None,
            ReadObjectErrorKind::Database(database::ReadError::Ambiguous) => None,
            ReadObjectErrorKind::Database(ref err) => Some(err),
            ReadObjectErrorKind::Parse(ref err) => Some(err),
            ReadObjectErrorKind::Io(ref err) => Some(err),
        }
    }
}

impl From<ParseObjectError> for ReadObjectErrorKind {
    fn from(err: ParseObjectError) -> Self {
        match err {
            ParseObjectError::Io(err) => ReadObjectErrorKind::Io(err),
            err => ReadObjectErrorKind::Parse(err),
        }
    }
}

impl From<database::ReadError> for ReadObjectErrorKind {
    fn from(err: database::ReadError) -> Self {
        ReadObjectErrorKind::Database(err)
    }
}

impl ReadObjectError {
    pub fn id(&self) -> ShortId {
        self.id
    }

    pub fn is_ambiguous(&self) -> bool {
        match self.kind {
            ReadObjectErrorKind::Database(database::ReadError::Ambiguous) => true,
            _ => false,
        }
    }

    pub fn is_not_found(&self) -> bool {
        match self.kind {
            ReadObjectErrorKind::Database(database::ReadError::NotFound) => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_id_ordering() {
        let ids = &[
            Id::from_str("12049b174da6220c0838aace2dfd510f2b97196b").unwrap(),
            Id::from_str("57805b76ecad518a31cb9fc8e84a9d65a73e2432").unwrap(),
            Id::from_str("8698a75639e76bc407828b78ff8b2decf28dcab6").unwrap(),
            Id::from_str("cde2000000000000000000000000000000000000").unwrap(),
            Id::from_str("cde2e10bfdb6c4945f322c6b4d59b077c9077f76").unwrap(),
            Id::from_str("cde2e10bfdb6c4945f322c6b4d59b077c9077f77").unwrap(),
            Id::from_str("fe7e5f30468d0292cd083e8289cf679adeaf85fd").unwrap(),
        ];
        let short = ShortId::from_str("cde2").unwrap();

        assert_eq!(short.cmp_id(&ids[0]), Ordering::Greater);
        assert_eq!(short.cmp_id(&ids[1]), Ordering::Greater);
        assert_eq!(short.cmp_id(&ids[2]), Ordering::Greater);
        assert_eq!(short.cmp_id(&ids[3]), Ordering::Less);
        assert_eq!(short.cmp_id(&ids[4]), Ordering::Less);
        assert_eq!(short.cmp_id(&ids[5]), Ordering::Less);
        assert_eq!(short.cmp_id(&ids[6]), Ordering::Less);

        assert_eq!(ids.binary_search_by(|id| id.cmp_short(&short)), Err(3));
    }
}
