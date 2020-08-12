use bstr::{BStr, ByteSlice};
use std::fmt;

use crate::object::Object;
use crate::reference::{Direct, Error, ParseError};
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

    pub fn peel(&self, repo: &Repository) -> Result<Object, Error> {
        match &self.direct_peel {
            Some(direct) => direct.object(repo),
            None => repo.reference_database().reference(&self.data)?.peel(repo),
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
