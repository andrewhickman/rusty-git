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

use std::cmp::Ordering;
use std::convert::TryInto;
use std::fmt;
use std::io::{self, Cursor};
use std::str::FromStr;

use hex::FromHex;
use sha1::digest::Digest;
use sha1::Sha1;
use thiserror::Error;
use zerocopy::FromBytes;

use self::parser::{ObjectKind, ParseError, Parser};

pub const ID_LEN: usize = 20;
pub const ID_HEX_LEN: usize = ID_LEN * 2;

pub const SHORT_ID_MIN_LEN: usize = 2;
pub const SHORT_ID_MIN_HEX_LEN: usize = SHORT_ID_MIN_LEN * 2;

#[repr(transparent)]
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, FromBytes)]
pub struct Id([u8; ID_LEN]);

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ShortId {
    id: [u8; ID_LEN],
    len: u32,
}

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
    ObjectNotFound(ShortId),
    #[error("object id `{0}` is ambiguous")]
    Ambiguous(ShortId),
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

    pub fn from_hex(hex: &[u8]) -> Result<Self, ParseIdError> {
        Ok(Id(FromHex::from_hex(hex)?))
    }

    pub fn to_hex(&self) -> String {
        hex::encode(&self.0)
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
        &self.id
    }

    pub fn is_full(&self) -> bool {
        self.len as usize == ID_LEN
    }

    /// Compare to an id. Partial ids are sorted just before ids they are a prefix of.
    pub fn cmp_id(&self, id: &Id) -> Ordering {
        Ord::cmp(&self.id, &id.0).then_with(|| {
            if self.is_full() {
                Ordering::Equal
            } else {
                Ordering::Less
            }
        })
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
        hex::encode(&self.id[..(self.len as usize)])
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
