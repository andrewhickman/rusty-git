use std::fmt;
use std::io;
use std::path::PathBuf;

use byteorder::NetworkEndian;
use thiserror::Error;
use zerocopy::byteorder::U32;
use zerocopy::{FromBytes, LayoutVerified};

use crate::object::database::packed::ReadPackedError;
use crate::object::database::Reader;
use crate::object::{Id, Parser, ID_LEN};

pub(in crate::object::database::packed) struct PackFile {
    data: Box<[u8]>,
    version: PackFileVersion,
}

#[derive(Debug, Error)]
pub(in crate::object::database::packed) enum ReadPackFileError {
    #[error("the signature of the pack file is invalid")]
    InvalidSignature,
    #[error("cannot parse a pack file with version `{0}`")]
    UnknownVersion(u32),
    #[error("{0}")]
    Other(&'static str),
    #[error("io error reading index file")]
    Io(
        #[from]
        #[source]
        io::Error,
    ),
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

    pub fn open(path: PathBuf) -> Result<Self, ReadPackFileError> {
        let bytes = fs_err::read(path)?.into_boxed_slice();
        PackFile::parse(Parser::new(bytes))
    }

    fn parse(mut parser: Parser<Box<[u8]>>) -> Result<Self, ReadPackFileError> {
        if !parser.consume_u32(PackFile::SIGNATURE) {
            return Err(ReadPackFileError::InvalidSignature);
        }
        let version = match parser
            .parse_u32()
            .map_err(|_| ReadPackFileError::Other("file is too short"))?
        {
            2 => PackFileVersion::V2,
            3 => PackFileVersion::V3,
            n => return Err(ReadPackFileError::UnknownVersion(n)),
        };

        parser
            .parse_u32()
            .or(Err(ReadPackFileError::Other("file is too short")))?;

        if parser.remaining() < ID_LEN {
            return Err(ReadPackFileError::Other("file is too short"));
        }

        let pack_file = PackFile {
            version,
            data: parser.into_inner(),
        };

        Ok(pack_file)
    }

    pub fn read_object(&self, _offset: usize) -> Result<Reader, ReadPackedError> {
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
