use std::io::Read;
use std::ops::Range;
use std::fmt;

use bstr::{BStr, ByteSlice};

use crate::object::{ParseError, Parser, ObjectKind, Id, ID_HEX_LEN};
use crate::object::signature::{SignatureRaw, Signature};

pub struct Tag {
    data: Vec<u8>,
    tag: Range<usize>,
    object: usize,
    kind: ObjectKind,
    tagger: Option<SignatureRaw>,
    message: Option<usize>,
}

impl Tag {
    pub fn parse<R: Read>(mut parser: Parser<R>) -> Result<Self, ParseError> {
        let object = parser
            .parse_hex_id_line(b"object ")?
            .ok_or(ParseError::InvalidTag("object field not found"))?;

        let kind = parser
            .parse_prefix_line(b"type ")?
            .ok_or(ParseError::InvalidTag("type field not found"))?;
        let kind = ObjectKind::from_bytes(&parser.bytes(kind.clone()))?;

        let tag = parser.parse_prefix_line(b"tag ")?
            .ok_or(ParseError::InvalidTag("tag field not found"))?;

        let tagger = parser.parse_signature(b"tagger ")?;

        let message = if parser.consume_bytes(b"\n") {
            Some(parser.pos())
        } else {
            None
        };

        Ok(Tag {
            data: parser.finish(),
            object,
            kind,
            tag,
            tagger,
            message,
        })
    }

    pub fn tag(&self) -> &BStr {
        self.data[self.tag.clone()].as_bstr()
    }

    pub fn object(&self) -> Id {
        Id::from_hex(&self.data[self.object..][..ID_HEX_LEN]).expect("id already validated")
    }

    pub fn kind(&self) -> ObjectKind {
        self.kind
    }

    pub fn tagger(&self) -> Option<Signature> {
        self.tagger
            .as_ref()
            .map(|tagger| Signature::new(&self.data, &tagger))
    }

    pub fn message(&self) -> Option<&BStr> {
        self.message
            .map(|message| self.data[message..].as_bstr())
    }
}

impl fmt::Debug for Tag {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Tag")
            .field("tag", &self.tag())
            .field("object", &self.object())
            .field("kind", &self.kind())
            .field("tagger", &self.tagger())
            .field("message", &self.message())
            .finish()
    }
}