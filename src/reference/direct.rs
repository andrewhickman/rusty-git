use crate::object::{Id, Object};
use crate::reference::{Error, ParseError};
use crate::repository::Repository;

#[derive(Debug, PartialEq)]
pub struct Direct {
    id: Id,
}

impl Direct {
    pub fn from_bytes(bytes: &[u8]) -> Result<Direct, ParseError> {
        Ok(Direct {
            id: Id::from_hex(bytes).map_err(ParseError::InvalidDirectIdentifier)?,
        })
    }

    pub fn object(&self, repo: &Repository) -> Result<Object, Error> {
        repo.object_database()
            .parse_object(&self.id)
            .map_err(Error::DereferencingFailed)
    }
}
