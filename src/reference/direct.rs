use crate::object::{Id, Object};
use crate::reference::Error;
use crate::repository::Repository;

#[derive(Debug, PartialEq)]
pub struct Direct {
    id: Id,
}

impl Direct {
    pub fn from_bytes(bytes: &[u8]) -> Direct {
        Direct {
            id: Id::from_hex(bytes).unwrap(),
        }
    }

    pub fn object(&self, repo: &Repository) -> Result<Object, Error> {
        repo.object_database()
            .parse_object(&self.id)
            .map_err(Error::DereferencingFailed)
    }
}
