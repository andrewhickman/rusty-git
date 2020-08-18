use bytes::Bytes;

use crate::object::ObjectHeader;
use crate::parse::Parser;

pub(in crate::object::database::packed) fn apply_delta<B>(
    base: &[u8],
    delta: Parser<B>,
) -> (ObjectHeader, Bytes)
where
    B: AsRef<[u8]>,
{
    todo!()
}
