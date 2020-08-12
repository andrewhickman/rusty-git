use std::convert::TryFrom;
use std::fmt;
use std::path::PathBuf;

use crate::object::database::packed::IndexFile;
use crate::object::database::Reader;
use crate::object::{Error, Id, ParseError, Parser, ID_LEN};

pub(in crate::object::database::packed) struct PackFile {
    index: IndexFile,
    version: PackFileVersion,
    data: Box<[u8]>,
}

#[derive(Debug)]
enum PackFileVersion {
    V2,
    V3,
}

impl PackFile {
    const SIGNATURE: u32 = 0x5041434b;

    pub fn open(index_path: PathBuf) -> Result<PackFile, ParseError> {
        let pack_path = index_path.with_extension("pack");
        let index = IndexFile::open(index_path)?;

        let mut parser = Parser::from_file(pack_path)?;
        if !parser.consume_u32(PackFile::SIGNATURE) {
            return Err(ParseError::InvalidPack);
        }
        let version = match parser.parse_u32()? {
            2 => PackFileVersion::V2,
            3 => PackFileVersion::V3,
            _ => return Err(ParseError::UnknownPackVersion),
        };

        let _entry_count = usize::try_from(parser.parse_u32()?).or(Err(ParseError::InvalidPack))?;

        if parser.remaining() < ID_LEN {
            return Err(ParseError::InvalidPack);
        }

        let pack_file = PackFile {
            index,
            version,
            data: parser.finish(),
        };

        Ok(pack_file)
    }

    pub fn read_object(&self, offset: usize) -> Result<Reader, Error> {
        todo!()
    }

    fn id(&self) -> Id {
        let pos = self.data.len() - ID_LEN;
        Id::from_bytes(&self.data[pos..][..ID_LEN])
    }
}

impl fmt::Debug for PackFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("PackFile")
            .field("index", &self.index)
            .field("version", &self.version)
            .finish()
    }
}
