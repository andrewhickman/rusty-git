use bstr::{BStr, ByteSlice};
use std::fmt;

use crate::object::Object;
use crate::reference::{ParseError, ReferenceTarget};
use crate::repository::Repository;

pub struct Symbolic {
    data: Vec<u8>,
}

impl Symbolic {
    pub fn from_bytes(input: &[u8]) -> Result<Self, ParseError> {
        if input.is_empty() {
            return Err(ParseError::EmptySymbolic);
        }

        Ok(Symbolic {
            data: input.to_owned(),
        })
    }

    pub fn data(&self) -> &BStr {
        self.data.as_bstr()
    }

    pub fn peel(&self, repo: &Repository) -> Object {
        match repo
            .reference_database()
            .reference(&self.data)
            .unwrap()
            .target()
        {
            ReferenceTarget::Symbolic(s) => s.peel(repo),
            ReferenceTarget::Direct(d) => d.object(repo).unwrap(),
        }
    }
}

impl fmt::Debug for Symbolic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Symbolic")
            .field("data", &self.data())
            .finish()
    }
}
