mod database;
mod direct;
mod parser;
mod symbolic;

use bstr::ByteSlice;
use std::io::{self, Cursor};
use thiserror::Error;

use crate::object::Id;

pub use self::database::ReferenceDatabase;
pub use self::direct::Direct;
use self::parser::{ParseError, Parser};
pub use self::symbolic::Symbolic;

#[derive(Debug)]
pub enum ReferenceTarget {
    Direct(Direct),
    Symbolic(Symbolic),
}

#[derive(Debug)]
pub struct ReferenceData {
    target: ReferenceTarget,
    peel: Option<Id>,
}

#[derive(Debug)]
pub struct Reference {
    data: ReferenceData,
}

#[derive(Debug, Error)]
pub enum Error {
    #[error("reference not found")]
    ReferenceNotFound,
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

impl ReferenceData {
    pub fn target(&self) -> &ReferenceTarget {
        &self.target
    }

    pub fn peel(&self) -> Option<Id> {
        self.peel
    }
}

impl Reference {
    pub fn from_reader<R: io::Read>(reader: R) -> Result<Self, Error> {
        Ok(Reference {
            data: Parser::new(reader)
                .parse()
                .map_err(Error::InvalidReference)?,
        })
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        Ok(Reference {
            data: Parser::new(Cursor::new(bytes))
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

    pub fn peel(&self) -> Option<Id> {
        self.data.peel()
    }

    pub fn target(&self) -> &ReferenceTarget {
        &self.data.target()
    }
}
