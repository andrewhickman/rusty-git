use std::fmt;
use std::path::PathBuf;

use byteorder::NetworkEndian;
use zerocopy::byteorder::U32;
use zerocopy::{FromBytes, LayoutVerified};

use crate::object::database::Reader;
use crate::object::{Error, Id, ParseError, Parser, ID_LEN};

pub(in crate::object::database::packed) struct PackFile {
    data: Box<[u8]>,
    version: PackFileVersion,
}

#[derive(Debug)]
enum PackFileVersion {
    V2,
    V3,
}

#[repr(C)]
#[derive(Debug, FromBytes)]
pub struct Header {
    signature: U32<NetworkEndian>,
    version: U32<NetworkEndian>,
    count: U32<NetworkEndian>,
}

impl PackFile {
    const SIGNATURE: u32 = 0x5041434b;

    pub fn open(path: PathBuf) -> Result<Self, ParseError> {
        PackFile::parse(Parser::from_file(path)?)
    }

    fn parse<R>(mut parser: Parser<R>) -> Result<Self, ParseError> {
        if !parser.consume_u32(PackFile::SIGNATURE) {
            return Err(ParseError::InvalidPack);
        }
        let version = match parser.parse_u32()? {
            2 => PackFileVersion::V2,
            3 => PackFileVersion::V3,
            _ => return Err(ParseError::UnknownPackVersion),
        };

        parser.parse_u32().or(Err(ParseError::InvalidPack))?;

        if parser.remaining() < ID_LEN {
            return Err(ParseError::InvalidPack);
        }

        let pack_file = PackFile {
            version,
            data: parser.finish(),
        };

        Ok(pack_file)
    }

    pub fn read_object(&self, _offset: usize) -> Result<Reader, Error> {
        todo!()
    }

    pub fn count(&self) -> u32 {
        self.header().count.get()
    }

    fn header(&self) -> &Header {
        LayoutVerified::<&[u8], Header>::new_from_prefix(&self.data)
            .unwrap()
            .0
            .into_ref()
    }

    pub fn id(&self) -> Id {
        let pos = self.data.len() - ID_LEN;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }
}

impl fmt::Debug for PackFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PackFile")
            .field("version", &self.version)
            .finish()
    }
}
