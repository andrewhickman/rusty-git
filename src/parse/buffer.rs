use std::io::{self, Read, Seek, SeekFrom};
use std::mem;
use std::ops::Range;
use std::slice::SliceIndex;

use bytes::{Bytes, BytesMut};
use memchr::memchr;

use crate::object::{Id, ID_LEN};
use crate::parse::{Error, Parser};

/// Similar to std::io::BufReader, but with a variable sized buffer
/// specialized for parsing git objects.
pub(crate) struct Buffer<R> {
    buffer: BytesMut,
    reader: R,
    // Marks the first byte not yet observed by the user.
    pos: usize,
}

impl<R: Read> Buffer<R> {
    pub fn new(reader: R) -> Self {
        Buffer {
            reader,
            buffer: BytesMut::new(),
            pos: 0,
        }
    }

    pub fn with_capacity(reader: R, capacity: usize) -> Self {
        Buffer {
            reader,
            buffer: BytesMut::with_capacity(capacity),
            pos: 0,
        }
    }

    pub fn pos(&self) -> usize {
        self.pos
    }

    /// Create a parser for the given range of bytes.
    pub fn parser<I>(&self, range: I) -> Parser<&[u8]>
    where
        I: SliceIndex<[u8], Output = [u8]>,
    {
        Parser::new(&self.buffer[range])
    }

    /// Read an exact number of bytes and create a parser.
    pub fn read_exact_as_parser(&mut self, size: usize) -> Result<Parser<&[u8]>, Error> {
        let range = self.read_exact(size)?;
        Ok(self.parser(range))
    }

    /// Read into an owned parser, .
    pub fn read_to_end_into_parser(self, size: usize) -> Result<Parser<Bytes>, Error> {
        let pos = self.pos;
        let buffer = self.read_to_end(size)?;
        Ok(Parser::with_position(buffer, pos))
    }

    /// Read from the reader, calling `pred` on each byte slice until it returns the offset of the end.
    ///
    /// If `pred` never returns an offset, the buffer position is not advanced and `None` is returned.
    pub(crate) fn read_until<F>(
        &mut self,
        size: usize,
        mut pred: F,
    ) -> Result<Option<Range<usize>>, Error>
    where
        F: FnMut(&[u8]) -> Option<usize>,
    {
        let start = self.pos;
        let end = start + size;

        while self.pos != end {
            let buf = match self.fill_buf_to(end) {
                Ok(&[]) => return Err(Error::UnexpectedEof),
                Ok(buf) => buf,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(Error::Io(err)),
            };

            if let Some(end) = pred(buf) {
                self.pos += end;
                return Ok(Some(start..self.pos));
            }

            self.pos += buf.len();
        }

        self.pos = start;
        Ok(None)
    }

    /// Read until the delimiter byte is encountered, the end of the reader
    /// is reached, or the maximum number of bytes has been read.
    ///
    /// Returns a slice containing the read bytes, up to and including
    /// the delimiter byte.
    pub(crate) fn read_until_byte(
        &mut self,
        delim: u8,
        size: usize,
    ) -> Result<Option<Range<usize>>, Error> {
        self.read_until(size, move |slice| {
            memchr(delim, slice).map(|index| index + 1)
        })
    }

    /// Read exactly `size` bytes from the reader
    pub fn read_exact(&mut self, size: usize) -> Result<Range<usize>, Error> {
        let start = self.pos;
        let end = self.pos.checked_add(size).ok_or(Error::InvalidLength)?;
        self.buffer.reserve(self.buffer.len().saturating_sub(end));

        while self.pos != end {
            let buf = match self.fill_buf_to(end) {
                Ok(&[]) => return Err(Error::InvalidLength),
                Ok(buf) => buf,
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(Error::Io(err)),
            };

            self.pos += buf.len();
        }

        Ok(start..end)
    }

    /// Read from the reader until the end and close it, returning a
    /// buffer containing its entire contents. If the total number of
    /// bytes read is not `size`, returns an error.
    pub fn read_to_end(mut self, size: usize) -> Result<Bytes, Error> {
        self.read_to_end_by_ref(size)
    }

    /// Read from the reader until the end and close it, returning a
    /// buffer containing its entire contents. If the total number of
    /// bytes read is not `size`, returns an error.
    pub fn read_to_end_by_ref(&mut self, size: usize) -> Result<Bytes, Error> {
        self.read_exact(size)?;

        // Read::read_to_end will grow the buffer unnecessarily for the
        // final zero-sized read call. Since we know the buffer size
        // ahead of time, we can avoid this.
        loop {
            match self.reader.read(&mut [0]) {
                Ok(0) => break,
                Ok(_) => return Err(Error::InvalidLength),
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                Err(err) => return Err(Error::Io(err)),
            }
        }

        let buffer = mem::take(&mut self.buffer);
        self.pos = 0;
        Ok(buffer.freeze())
    }

    /// Reads up to the byte at `end`, starting from `self.pos`, from the reader.
    fn fill_buf_to(&mut self, end: usize) -> io::Result<&[u8]> {
        if end > self.buffer.len() {
            // TODO ideally we would pass an uninitialized buffer to
            // the reader, but `Read::initializer` isn't stable yet.
            let old_len = self.buffer.len();
            self.buffer.resize(end, b'\0');

            match self.reader.read(&mut self.buffer[old_len..]) {
                Ok(read) => {
                    let read_end = old_len + read;
                    self.buffer.truncate(read_end);
                    Ok(&self.buffer[self.pos..read_end])
                }
                Err(err) => {
                    self.buffer.truncate(old_len);
                    Err(err)
                }
            }
        } else {
            Ok(&self.buffer[self.pos..end])
        }
    }

    /// Read a 20-byte object id from the reader
    pub fn read_id(&mut self) -> Result<Id, Error> {
        self.read_exact_as_parser(ID_LEN)?.parse_id()
    }
}

impl<R: Seek> Seek for Buffer<R> {
    /// Seek within the underlying reader. Any buffered data is discarded.
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.buffer.clear();
        self.pos = 0;
        self.reader.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    struct TestReader(vec::IntoIter<Box<dyn FnOnce(&mut [u8]) -> io::Result<usize>>>);

    impl TestReader {
        fn new(reads: Vec<Box<dyn FnOnce(&mut [u8]) -> io::Result<usize>>>) -> Self {
            TestReader(reads.into_iter())
        }
    }

    impl io::Read for TestReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            (self.0.next().expect("unexpected reader call"))(buf)
        }
    }

    impl Drop for TestReader {
        fn drop(&mut self) {
            if !std::thread::panicking() {
                assert!(self.0.next().is_none());
            }
        }
    }

    #[test]
    fn fill_buf() {
        let bytes = b"abcdefghijklmnopqrstuvwxyz";
        let size = 13;
        let read = 5;

        let reader = TestReader::new(vec![Box::new(move |buf: &mut [u8]| {
            assert_eq!(buf.len(), size);
            buf[..read].copy_from_slice(&bytes[..read]);
            Ok(read)
        })]);

        let mut buffer = Buffer::new(reader);

        assert_eq!(buffer.fill_buf_to(size).unwrap(), &bytes[..read]);
    }

    #[test]
    fn fill_buf_full() {
        let bytes = b"abcdefghijklmnopqrstuvwxyz";
        let size = 13;

        let reader = TestReader::new(vec![Box::new(move |buf: &mut [u8]| {
            assert_eq!(buf.len(), size);
            buf.copy_from_slice(&bytes[..buf.len()]);
            Ok(buf.len())
        })]);

        let mut buffer = Buffer::new(reader);

        assert_eq!(buffer.fill_buf_to(size).unwrap(), &bytes[..size]);
    }

    #[test]
    fn fill_buf_buffered() {
        let bytes = b"abcdefghijklmnopqrstuvwxyz";
        let pos = 5;
        let size = 13;
        let buffered = 10;
        let read = 4;

        let reader = TestReader::new(vec![Box::new(move |buf: &mut [u8]| {
            assert_eq!(buf.len(), pos + size - buffered);
            buf[..read].copy_from_slice(&bytes[buffered..][..read]);
            Ok(read)
        })]);

        let mut buffer = Buffer {
            reader,
            buffer: bytes[..buffered].to_vec(),
            pos,
        };

        assert_eq!(
            buffer.fill_buf_to(pos + size).unwrap(),
            &bytes[pos..][..(buffered - pos + read)]
        );
    }

    #[test]
    fn fill_buf_buffered_full() {
        let bytes = b"abcdefghijklmnopqrstuvwxyz";
        let pos = 5;
        let size = 13;
        let buffered = 10;

        let reader = TestReader::new(vec![Box::new(move |buf: &mut [u8]| {
            assert_eq!(buf.len(), pos + size - buffered);
            buf.copy_from_slice(&bytes[buffered..][..buf.len()]);
            Ok(buf.len())
        })]);

        let mut buffer = Buffer {
            reader,
            buffer: bytes[..buffered].to_vec(),
            pos,
        };

        assert_eq!(
            buffer.fill_buf_to(pos + size).unwrap(),
            &bytes[pos..][..size]
        );
    }

    #[test]
    fn fill_buf_no_read() {
        let bytes = b"abcdefghijklmnopqrstuvwxyz";
        let size = 13;

        let reader = TestReader::new(vec![]);

        let mut buffer = Buffer {
            reader,
            buffer: bytes.to_vec(),
            pos: 0,
        };

        assert_eq!(buffer.fill_buf_to(size).unwrap(), &bytes[..size]);
    }

    #[test]
    fn fill_buf_2() {
        let bytes = b"abcdefghijklmnopqrstuvwxyz";
        let size = 15;

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf.copy_from_slice(&bytes[..size]);
                Ok(size)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..(bytes.len() - size)].copy_from_slice(&bytes[size..]);
                Ok(bytes.len() - size)
            }),
        ]);

        let mut buffer = Buffer::new(reader);

        assert_eq!(buffer.fill_buf_to(size).unwrap(), &bytes[..size]);
        buffer.pos += size;
        assert_eq!(
            buffer.fill_buf_to(buffer.pos + size).unwrap(),
            &bytes[size..]
        );
    }

    #[test]
    fn fill_buf_error() {
        let size = 13;

        let reader = TestReader::new(vec![Box::new(move |_: &mut [u8]| {
            Err(io::Error::from(io::ErrorKind::Interrupted))
        })]);

        let mut buffer = Buffer::new(reader);

        buffer.fill_buf_to(size).unwrap_err();
        assert_eq!(buffer.buffer.len(), 0);
    }

    #[test]
    fn read_until() {
        let bytes = b"abcdefghijklnmnopqrstuvwxyz";
        let size = bytes.len() - 2;

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..5].copy_from_slice(&bytes[0..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 5);
                buf[..5].copy_from_slice(&bytes[5..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 10);
                buf[..5].copy_from_slice(&bytes[10..][..5]);
                Ok(5)
            }),
        ]);

        let mut buffer = Buffer::new(reader);

        assert_eq!(buffer.read_until_byte(b'n', size).unwrap().unwrap(), 0..13);
    }

    #[test]
    fn read_until_eof_after_delim() {
        let bytes = b"abcdefghijklnmnopqrstuvwxyz";
        let size = bytes.len() + 5;

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..5].copy_from_slice(&bytes[0..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 5);
                buf[..5].copy_from_slice(&bytes[5..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 10);
                buf[..5].copy_from_slice(&bytes[10..][..5]);
                Ok(5)
            }),
        ]);

        let mut buffer = Buffer::new(reader);

        assert_eq!(buffer.read_until_byte(b'n', size).unwrap().unwrap(), 0..13);
    }

    #[test]
    fn read_until_not_found() {
        let bytes = b"abcdefghijklnmnyz";
        let size = bytes.len() - 2;

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..5].copy_from_slice(&bytes[0..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 5);
                buf[..5].copy_from_slice(&bytes[5..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 10);
                buf[..5].copy_from_slice(&bytes[10..][..5]);
                Ok(5)
            }),
        ]);

        let mut buffer = Buffer::new(reader);

        assert!(buffer.read_until_byte(b'z', size).unwrap().is_none());
        assert_eq!(buffer.pos, 0);
    }

    #[test]
    fn read_until_eof() {
        let bytes = b"abcdefghij";
        let size = bytes.len() + 2;

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..5].copy_from_slice(&bytes[0..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 5);
                buf[..5].copy_from_slice(&bytes[5..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 10);
                Ok(0)
            }),
        ]);

        let mut buffer = Buffer::new(reader);

        match buffer.read_until_byte(b'z', size).unwrap_err() {
            Error::UnexpectedEof => (),
            err => panic!("unexpected error {:?}", err),
        }
    }

    #[test]
    fn read_until_out_of_range() {
        let bytes = b"abcdefghijz";
        let size = bytes.len() - 3;

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..5].copy_from_slice(&bytes[0..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 5);
                let len = buf.len();
                buf[..len].copy_from_slice(&bytes[5..][..len]);
                Ok(len)
            }),
        ]);

        let mut buffer = Buffer::new(reader);

        assert!(buffer.read_until_byte(b'z', size).unwrap().is_none());
    }

    #[test]
    fn read_until_interrupted() {
        let bytes = b"abcdefghijklnmnopqrstuvwxyz";
        let size = bytes.len() - 2;

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..5].copy_from_slice(&bytes[0..][..5]);
                Ok(5)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 5);
                buf[..5].copy_from_slice(&bytes[5..][..5]);
                Ok(5)
            }),
            Box::new(move |_: &mut [u8]| Err(io::Error::from(io::ErrorKind::Interrupted))),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 10);
                buf[..5].copy_from_slice(&bytes[10..][..5]);
                Ok(5)
            }),
        ]);

        let mut buffer = Buffer::new(reader);

        assert_eq!(buffer.read_until_byte(b'n', size).unwrap().unwrap(), 0..13);
    }

    #[test]
    fn read_until_2() {
        let bytes = b"abcdefghijklznmnopqrstuvwxyza";
        let size = bytes.len() - 2;

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                buf[..10].copy_from_slice(&bytes[0..][..10]);
                Ok(10)
            }),
            Box::new(move |buf: &mut [u8]| {
                buf[..10].copy_from_slice(&bytes[10..][..10]);
                Ok(10)
            }),
            Box::new(move |buf: &mut [u8]| {
                buf[..9].copy_from_slice(&bytes[20..][..9]);
                Ok(9)
            }),
        ]);

        let mut buffer = Buffer::new(reader);

        assert_eq!(buffer.read_until_byte(b'z', size).unwrap().unwrap(), 0..13);
        assert_eq!(buffer.read_until_byte(b'z', size).unwrap().unwrap(), 13..28);
    }

    #[test]
    fn read_to_end() {
        let bytes = b"abcdefghijklznmnopqrstuvwxyza";
        let size = bytes.len();

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..10].copy_from_slice(&bytes[0..][..10]);
                Ok(10)
            }),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 10);
                buf.copy_from_slice(&bytes[10..][..buf.len()]);
                Ok(buf.len())
            }),
            Box::new(move |_: &mut [u8]| Ok(0)),
        ]);

        let buffer = Buffer::new(reader);

        assert_eq!(buffer.read_to_end(size).unwrap(), Box::from(*bytes));
    }

    #[test]
    fn read_to_end_interrupted() {
        let bytes = b"abcdefghijklznmnopqrstuvwxyza";
        let size = bytes.len();

        let reader = TestReader::new(vec![
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size);
                buf[..10].copy_from_slice(&bytes[0..][..10]);
                Ok(10)
            }),
            Box::new(move |_: &mut [u8]| Err(io::Error::from(io::ErrorKind::Interrupted))),
            Box::new(move |buf: &mut [u8]| {
                assert_eq!(buf.len(), size - 10);
                buf.copy_from_slice(&bytes[10..][..buf.len()]);
                Ok(buf.len())
            }),
            Box::new(move |_: &mut [u8]| Err(io::Error::from(io::ErrorKind::Interrupted))),
            Box::new(move |_: &mut [u8]| Ok(0)),
        ]);

        let buffer = Buffer::new(reader);

        assert_eq!(buffer.read_to_end(size).unwrap(), Box::from(*bytes));
    }
}
