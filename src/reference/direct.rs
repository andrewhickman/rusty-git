use crate::object::Id;

#[derive(Debug)]
pub struct Direct {
    id: Id,
}

impl Direct {
    pub fn from_bytes(bytes: &[u8]) -> Direct {
        Direct {
            id: Id::from_bytes(&bytes),
        }
    }
}
