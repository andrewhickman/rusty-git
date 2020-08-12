mod database;
mod direct;
mod parser;
mod symbolic;

use bstr::ByteSlice;
use std::io::{self, Cursor};
use thiserror::Error;

use crate::object::{self, Object};
use crate::repository::Repository;

pub use self::database::ReferenceDatabase;
pub use self::direct::Direct;
use self::parser::{ParseError, Parser};
pub use self::symbolic::Symbolic;

#[derive(Debug, PartialEq)]
pub enum ReferenceTarget {
    Direct(Direct),
    Symbolic(Symbolic),
}

#[derive(Debug)]
pub struct Reference {
    target: ReferenceTarget,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("reference not found")]
    ReferenceNotFound,
    #[error(
        "reference was stored as invalid Utf16, on windows reference names must be valid utf16"
    )]
    ReferenceNameInvalidUtf16,
    #[error("reference was given as invalid Utf8")]
    ReferenceNameInvalidUtf8,
    #[error("failed to dereference to an object")]
    DereferencingFailed(
        #[source]
        #[from]
        object::Error,
    ),
    #[error("the reference is invalid")]
    InvalidReference(
        #[source]
        #[from]
        ParseError,
    ),
    #[error("io error in reference database")]
    Io(
        #[source]
        #[from]
        io::Error,
    ),
}

impl ReferenceTarget {
    pub fn peel(&self, repo: &Repository) -> Result<Object, Error> {
        match self {
            ReferenceTarget::Symbolic(s) => s.peel(repo),
            ReferenceTarget::Direct(d) => d.object(repo),
        }
    }
}

impl Reference {
    pub fn from_reader<R: io::Read>(reader: R) -> Result<Self, Error> {
        Ok(Reference {
            target: Parser::new(reader)
                .parse()
                .map_err(Error::InvalidReference)?,
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Reference {
            target: Parser::new(Cursor::new(bytes))
                .parse()
                .map_err(Error::InvalidReference)?,
        })
    }

    pub fn name(&self) -> Option<&str> {
        match self.target() {
            ReferenceTarget::Symbolic(s) => s.data().to_str().ok(),
            _ => None,
        }
    }

    pub fn peel(&self, repo: &Repository) -> Result<Object, Error> {
        self.target().peel(repo)
    }

    pub fn target(&self) -> &ReferenceTarget {
        &self.target
    }
}
