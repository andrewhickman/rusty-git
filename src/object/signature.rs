use std::ops::Range;

use bstr::{BStr, ByteSlice};
use once_cell::sync::Lazy;
use regex::bytes::{Captures, Regex};
use thiserror::Error;

use crate::parse::Parser;

#[derive(Clone)]
pub struct SignatureRaw {
    range: Range<usize>,
}

pub struct Signature<'a> {
    captures: Captures<'a>,
}

#[derive(Debug, Error)]
#[error("a signature line is invalid")]
pub struct ParseSignatureError;

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

impl<B: AsRef<[u8]>> Parser<B> {
    pub fn parse_signature(
        &mut self,
        prefix: &[u8],
    ) -> Result<Option<SignatureRaw>, ParseSignatureError> {
        if let Some(range) = self
            .parse_prefix_line(prefix)
            .map_err(|_| ParseSignatureError)?
        {
            if Signature::is_valid(&self[range.clone()]) {
                Ok(Some(SignatureRaw { range }))
            } else {
                Err(ParseSignatureError)
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use bstr::B;

    use super::*;

    #[test]
    fn test_parse_signature() {
        let mut parser = Parser::new(B(
            "author Andrew Hickman <me@andrewhickman.dev> 1596907199 +0100\n",
        ));
        let signature_raw = parser.parse_signature(b"author ").unwrap().unwrap();
        let buf = parser.into_inner();
        let signature = Signature::new(&buf, &signature_raw);

        assert_eq!(signature.name(), "Andrew Hickman");
        assert_eq!(signature.email(), "me@andrewhickman.dev");
        assert_eq!(signature.timestamp(), Some(b"1596907199".as_bstr()));
        assert_eq!(signature.timezone(), Some(b"+0100".as_bstr()));
    }

    #[test]
    fn test_parse_signature_no_timezone() {
        let mut parser = Parser::new(B(
            "author Andrew Hickman <me@andrewhickman.dev> 1596907199\n",
        ));
        let signature_raw = parser.parse_signature(b"author ").unwrap().unwrap();
        let buf = parser.into_inner();
        let signature = Signature::new(&buf, &signature_raw);

        assert_eq!(signature.name(), "Andrew Hickman");
        assert_eq!(signature.email(), "me@andrewhickman.dev");
        assert_eq!(signature.timestamp(), Some(b"1596907199".as_bstr()));
        assert_eq!(signature.timezone(), None);
    }

    #[test]
    fn test_parse_signature_no_timestamp() {
        let mut parser = Parser::new(B("author Andrew Hickman <me@andrewhickman.dev>\n"));
        let signature_raw = parser.parse_signature(b"author ").unwrap().unwrap();
        let buf = parser.into_inner();
        let signature = Signature::new(&buf, &signature_raw);

        assert_eq!(signature.name(), "Andrew Hickman");
        assert_eq!(signature.email(), "me@andrewhickman.dev");
        assert_eq!(signature.timestamp(), None);
        assert_eq!(signature.timezone(), None);
    }
}
