use bstr::{BStr, ByteSlice};
use std::fmt;

use crate::reference::ParseError;

pub struct Symbolic {
    data: Vec<u8>,
}

impl Symbolic {
    pub fn from_bytes(input: &[u8]) -> Result<Self, ParseError> {
        if input.is_empty() {
            return Err(ParseError::EmptySymbolic);
        }

        Ok(Symbolic {
            data: input.trim_end().to_owned(),
        })
    }

    pub fn data(&self) -> &BStr {
        self.data.as_bstr()
    }
}

impl fmt::Debug for Symbolic {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Symbolic")
            .field("data", &self.data())
            .finish()
    }
}
