use std::ops::Range;
use std::io::Read;

use bstr::{BStr, ByteSlice};
use once_cell::sync::Lazy;
use regex::bytes::{Captures, Regex};

use crate::object::{ParseError, Parser};

pub struct SignatureRaw {
    range: Range<usize>,
}

pub struct Signature<'a> {
    captures: Captures<'a>,
}

impl<'a> Signature<'a> {
    fn regex() -> &'static Regex {
        const PADDING_CHARS: &str = "[\x00-\x32.,:;<>\"\\\\']*";

        static REGEX: Lazy<Regex> = Lazy::new(|| {
            Regex::new(&format!(
                r"{pad}(.*){pad} <{pad}(.*){pad}>(?: (\d+)(?: ([+\-]\d+))?)?",
                pad = PADDING_CHARS
            ))
            .unwrap()
        });

        &*REGEX
    }

    fn is_valid(input: &[u8]) -> bool {
        Signature::regex().is_match(input)
    }

    pub(in crate::object) fn new(input: &'a [u8], raw: &SignatureRaw) -> Self {
        Signature {
            captures: Signature::regex()
                .captures(&input[raw.range.clone()])
                .expect("invalid signature"),
        }
    }

    pub fn name(&self) -> &'a BStr {
        self.captures.get(1).unwrap().as_bytes().as_bstr()
    }

    pub fn email(&self) -> &'a BStr {
        self.captures.get(2).unwrap().as_bytes().as_bstr()
    }

    pub fn timestamp(&self) -> Option<&'a BStr> {
        self.captures.get(3).map(|mat| mat.as_bytes().as_bstr())
    }

    pub fn timezone(&self) -> Option<&'a BStr> {
        self.captures.get(4).map(|mat| mat.as_bytes().as_bstr())
    }
}

impl<R: Read> Parser<R> {
    pub fn parse_signature(&mut self, prefix: &[u8]) -> Result<Option<SignatureRaw>, ParseError> {
        if let Some(range) = self.parse_prefix_line(prefix)? {
            if Signature::is_valid(&self.bytes()[range.clone()]) {
                Ok(Some(SignatureRaw { range }))
            } else {
                Err(ParseError::InvalidSignature)
            }
        } else {
            Ok(None)
        }
    }
}
