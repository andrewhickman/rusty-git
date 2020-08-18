use std::io::{self, BufRead, BufReader, Read};

use bytes::buf::ext::BufExt;
use bytes::Bytes;
use flate2::bufread::ZlibDecoder;

use crate::object::parse::ParseObjectError;
use crate::object::{ObjectHeader, ObjectData};
use crate::parse;

pub struct ObjectReader {
    header: Option<ObjectHeader>,
    reader: ZlibDecoder<ReaderKind>,
}

enum ReaderKind {
    File(BufReader<fs_err::File>),
    Bytes(bytes::buf::ext::Reader<Bytes>),
}

impl ObjectReader {
    pub(in crate::object) fn from_file(header: impl Into<Option<ObjectHeader>>, file: fs_err::File) -> Self {
        ObjectReader {
            header: header.into(),
            reader: ZlibDecoder::new(ReaderKind::File(BufReader::new(file))),
        }
    }

    pub(in crate::object) fn from_bytes(header: impl Into<Option<ObjectHeader>>, bytes: Bytes) -> Self {
        ObjectReader {
            header: header.into(),
            reader: ZlibDecoder::new(ReaderKind::Bytes(bytes.reader())),
        }
    }

    pub fn reader(&mut self) -> &mut impl Read {
        &mut self.reader
    }

    pub(in crate::object) fn parse(self) -> Result<ObjectData, ParseObjectError> {
        let mut buffer = parse::Buffer::new(self.reader);

        let header = match self.header {
            Some(header) => header,
            None => buffer.read_object_header()?,
        };

        buffer.read_object_body(header)
    }
}

impl Read for ReaderKind {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            ReaderKind::File(file) => file.read(buf),
            ReaderKind::Bytes(bytes) => bytes.read(buf),
        }
    }
}

impl BufRead for ReaderKind {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        match self {
            ReaderKind::File(file) => file.fill_buf(),
            ReaderKind::Bytes(bytes) => bytes.fill_buf(),
        }
    }

    fn consume(&mut self, amt: usize) {
        match self {
            ReaderKind::File(file) => file.consume(amt),
            ReaderKind::Bytes(bytes) => bytes.consume(amt),
        }
    }
}
