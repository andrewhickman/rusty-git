use std::fmt;
use std::io::Read;
use std::str;

use bstr::{BStr, ByteSlice};

use crate::object::{Id, ParseError, Parser, ID_LEN};

pub struct Tree {
    data: Vec<u8>,
    entries: Vec<TreeEntryRaw>,
}

pub struct TreeEntry<'a> {
    data: &'a [u8],
    entry: TreeEntryRaw,
}

#[derive(Copy, Clone)]
struct TreeEntryRaw {
    mode: u16,
    id: usize,
    filename_start: usize,
    filename_end: usize,
}

impl Tree {
    pub fn parse<R: Read>(mut parser: Parser<R>) -> Result<Self, ParseError> {
        let mut entries = Vec::with_capacity(parser.remaining() / 140);

        while !parser.finished() {
            let mode = parser.consume_until(b' ').ok_or(ParseError::InvalidTree)?;
            let mode = str::from_utf8(mode).map_err(|_| ParseError::InvalidTree)?;
            let mode = u16::from_str_radix(mode, 8).map_err(|_| ParseError::InvalidTree)?;

            let filename_start = parser.pos();
            let filename_end = filename_start
                + parser
                    .consume_until(0)
                    .ok_or(ParseError::InvalidTree)?
                    .len();

            let id = parser.pos();
            if !parser.advance(ID_LEN) {
                return Err(ParseError::InvalidTree);
            }

            entries.push(TreeEntryRaw {
                mode,
                filename_start,
                filename_end,
                id,
            })
        }

        entries.shrink_to_fit();
        Ok(Tree {
            data: parser.finish(),
            entries,
        })
    }

    pub fn entries<'a>(&'a self) -> impl ExactSizeIterator<Item = TreeEntry<'a>> {
        self.entries.iter().copied().map(move |entry| TreeEntry {
            data: &self.data,
            entry,
        })
    }
}

impl<'a> TreeEntry<'a> {
    pub fn mode(&self) -> u16 {
        self.entry.mode
    }

    pub fn id(&self) -> Id {
        Id::from_bytes(&self.data[self.entry.id..(self.entry.id + ID_LEN)])
    }

    pub fn filename(&self) -> &'a BStr {
        self.data[self.entry.filename_start..self.entry.filename_end].as_bstr()
    }
}

impl fmt::Debug for Tree {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_list().entries(self.entries()).finish()
    }
}

impl<'a> fmt::Debug for TreeEntry<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("TreeEntry")
            .field("mode", &self.mode())
            .field("id", &self.id())
            .field("filename", &self.filename())
            .finish()
    }
}
