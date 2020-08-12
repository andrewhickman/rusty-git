use bstr::{BStr, ByteSlice};
use std::fmt;

use crate::object::Object;
use crate::reference::{Direct, ParseError, ReferenceTarget};
use crate::repository::Repository;

#[derive(PartialEq)]
pub struct Symbolic {
    direct_peel: Option<Direct>,
    data: Vec<u8>,
}

impl Symbolic {
    pub fn from_bytes(reference: &[u8], peel: Option<&[u8]>) -> Result<Self, ParseError> {
        if reference.is_empty() {
            return Err(ParseError::EmptySymbolic);
        }

        Ok(Symbolic {
            data: reference.to_owned(),
            direct_peel: match peel {
                Some(bytes) => Some(Direct::from_bytes(bytes)),
                None => None,
            },
        })
    }

    pub fn data(&self) -> &BStr {
        self.data.as_bstr()
    }

    pub fn peel(&self, repo: &Repository) -> Object {
        match &self.direct_peel {
            Some(direct) => direct.object(repo).unwrap(),
            None => match repo
                .reference_database()
                .reference(&self.data)
                .unwrap()
                .target()
            {
                ReferenceTarget::Symbolic(s) => s.peel(repo),
                ReferenceTarget::Direct(d) => d.object(repo).unwrap(),
            },
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
