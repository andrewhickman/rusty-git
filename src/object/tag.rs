use std::fmt;
use std::ops::Range;

use bstr::{BStr, ByteSlice};
use bytes::Bytes;
use thiserror::Error;

use crate::object::parse::ParseObjectKindError;
use crate::object::signature::{ParseSignatureError, Signature, SignatureRaw};
use crate::object::{Id, ObjectKind, Parser, ID_HEX_LEN};

#[derive(Clone)]
pub struct Tag {
    data: Bytes,
    tag: Range<usize>,
    object: usize,
    kind: ObjectKind,
    tagger: Option<SignatureRaw>,
    message: Option<usize>,
}

#[derive(Debug, Error)]
pub(in crate::object) enum ParseTagError {
    #[error("{0}")]
    Other(&'static str),
    #[error(transparent)]
    ParseObjectKind(#[from] ParseObjectKindError),
    #[error(transparent)]
    Signature(#[from] ParseSignatureError),
}

impl Tag {
    pub(in crate::object) fn parse(mut parser: Parser<Bytes>) -> Result<Self, ParseTagError> {
        let object = parser
            .parse_hex_id_line(b"object ")
            .map_err(|_| ParseTagError::Other("object field not found"))?
            .ok_or(ParseTagError::Other("object field not found"))?;

        let kind = parser
            .parse_prefix_line(b"type ")
            .map_err(|_| ParseTagError::Other("type field not found"))?
            .ok_or(ParseTagError::Other("type field not found"))?;
        let kind = ObjectKind::from_bytes(&parser[kind])?;

        let tag = parser
            .parse_prefix_line(b"tag ")
            .map_err(|_| ParseTagError::Other("tag field not found"))?
            .ok_or(ParseTagError::Other("tag field not found"))?;

        let tagger = parser.parse_signature(b"tagger ")?;

        let message = if parser.consume_bytes(b"\n") {
            Some(parser.pos())
        } else {
            None
        };

        Ok(Tag {
            data: parser.into_inner(),
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
        self.message.map(|message| self.data[message..].as_bstr())
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
