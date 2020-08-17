use std::fmt;
use std::ops::Range;
use std::str;
use std::sync::Arc;

use bstr::{BStr, ByteSlice};
use bytes::Bytes;
use thiserror::Error;

use crate::object::{Id, Parser, ID_LEN};

#[derive(Clone)]
pub struct Tree {
    data: Bytes,
    entries: Arc<[TreeEntryRaw]>,
}

pub struct TreeEntry<'a> {
    data: &'a [u8],
    entry: TreeEntryRaw,
}

#[derive(Debug, Error)]
#[error("{0}")]
pub(in crate::object) struct ParseTreeError(&'static str);

#[derive(Clone)]
struct TreeEntryRaw {
    mode: u16,
    id: usize,
    filename: Range<usize>,
}

impl Tree {
    pub(in crate::object) fn parse(mut parser: Parser<Bytes>) -> Result<Self, ParseTreeError> {
        let mut entries = Vec::with_capacity(parser.remaining() / 140);

        while !parser.finished() {
            let mode = parser
                .consume_until(b' ')
                .ok_or(ParseTreeError("invalid mode"))?;
            let mode = str::from_utf8(&parser[mode]).map_err(|_| ParseTreeError("invalid mode"))?;
            let mode = u16::from_str_radix(mode, 8).map_err(|_| ParseTreeError("invalid mode"))?;

            let filename = parser
                .consume_until(0)
                .ok_or(ParseTreeError("invalid filename"))?;

            let id = parser.pos();
            if !parser.advance(ID_LEN) {
                return Err(ParseTreeError("invalid id"));
            }

            entries.push(TreeEntryRaw { mode, filename, id })
        }

        Ok(Tree {
            data: parser.into_inner(),
            entries: Arc::from(entries.as_slice()),
        })
    }

    pub fn entries(&self) -> impl ExactSizeIterator<Item = TreeEntry> {
        self.entries.iter().cloned().map(move |entry| TreeEntry {
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
        Id::from_bytes(&self.data[self.entry.id..][..ID_LEN])
    }

    pub fn filename(&self) -> &'a BStr {
        self.data[self.entry.filename.clone()].as_bstr()
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

#[cfg(test)]
mod tests {
    use crate::object::{Parser, Tree};

    #[test]
    fn test_parse_tree() {
        let parser = Parser::new(
            b"\
40000 .github\0\x49\x19\x89\xb9\x30\xc1\xe5\xd0\x83\xa4\xd2\xa1\xf7\xfa\x42\xaa\xa8\x6c\x13\x75\
100644 .gitignore\0\x69\x36\x99\x04\x2b\x1a\x8c\xcf\x69\x76\x36\xd3\xcd\x34\xb2\x00\xf3\xa8\x27\x8b\
"
            .to_vec()
            .into_boxed_slice(),
        );

        let tree = Tree::parse(parser).unwrap();
        let entries: Vec<_> = tree.entries().collect();

        assert_eq!(entries[0].mode(), 16384);
        assert_eq!(
            entries[0].id().to_hex(),
            "491989b930c1e5d083a4d2a1f7fa42aaa86c1375"
        );
        assert_eq!(entries[0].filename(), ".github");
        assert_eq!(entries[1].mode(), 33188);
        assert_eq!(
            entries[1].id().to_hex(),
            "693699042b1a8ccf697636d3cd34b200f3a8278b"
        );
        assert_eq!(entries[1].filename(), ".gitignore");
    }
}
