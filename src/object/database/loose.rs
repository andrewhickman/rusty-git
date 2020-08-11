use std::fs::OpenOptions;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use filetime::{set_file_mtime, FileTime};
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;

use crate::object::{Error, Id};
use crate::object::database::Reader;

const OBJECTS_FOLDER: &str = "objects";

#[derive(Debug)]
pub struct LooseObjectDatabase {
    path: PathBuf,
}

impl LooseObjectDatabase {
    pub fn open(path: &Path) -> Self {
        LooseObjectDatabase {
            path: path.join(OBJECTS_FOLDER),
        }
    }

    pub fn read_object(&self, id: &Id) -> Result<Reader, Error> {
        let hex = id.to_hex();
        let (dir, file) = object_path_parts(&hex);
        let mut path = self.path.join(dir);
        path.push(file);

        match fs_err::File::open(path) {
            Ok(file) => Ok(ZlibDecoder::new(file)),
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                Err(Error::ObjectNotFound(*id))
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn write_object(&self, bytes: &[u8]) -> Result<Id, Error> {
        let id = Id::from_hash(bytes);
        let hex = id.to_hex();
        let (dir, file) = object_path_parts(&hex);

        let mut path = self.path.join(dir);
        match fs_err::create_dir(&path) {
            Err(err) if err.kind() != io::ErrorKind::AlreadyExists => return Err(err.into()),
            _ => (),
        }

        path.push(file);
        let file = match OpenOptions::new().create_new(true).write(true).open(&path) {
            Ok(file) => fs_err::File::from_parts(file, path),
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                let _ = set_file_mtime(path, FileTime::now());
                return Ok(id);
            }
            Err(err) => return Err(err.into()),
        };

        let mut encoder = ZlibEncoder::new(file, Compression::best());
        encoder.write_all(bytes)?;
        encoder.finish()?;
        Ok(id)
    }
}

fn object_path_parts(hex: &str) -> (&str, &str) {
    hex.split_at(2)
}

#[cfg(test)]
mod tests {
    use std::fs::{create_dir, metadata};
    use std::io::Read;

    use proptest::{arbitrary::any, collection::vec, prop_assert_eq, proptest};
    use tempdir::TempDir;

    use super::{object_path_parts, LooseObjectDatabase, OBJECTS_FOLDER};

    proptest! {
        #[test]
        fn roundtrip_object(bytes in vec(any::<u8>(), ..1000)) {
            let tempdir = TempDir::new("rusty_git_odb_loose_tests").unwrap();
            create_dir(tempdir.path().join(OBJECTS_FOLDER)).unwrap();

            let db = LooseObjectDatabase::open(tempdir.path());

            let id = db.write_object(&bytes).unwrap();

            let mut read_bytes = Vec::new();
            db.read_object(&id).unwrap().read_to_end(&mut read_bytes).unwrap();

            prop_assert_eq!(read_bytes, bytes);
        }
    }

    #[test]
    fn updates_file_mtime_on_already_exists() {
        let tempdir = TempDir::new("rusty_git_odb_loose_tests").unwrap();
        let odb_path = tempdir.path().join(OBJECTS_FOLDER);
        create_dir(&odb_path).unwrap();
        let db = LooseObjectDatabase::open(tempdir.path());

        let id = db.write_object(b"hello").unwrap();
        let hex = id.to_hex();
        let (dir, file) = object_path_parts(&hex);
        let path = odb_path.join(dir).join(file);

        let mtime1 = metadata(&path).unwrap().modified().unwrap();

        assert_eq!(db.write_object(b"hello").unwrap(), id);
        let mtime2 = metadata(&path).unwrap().modified().unwrap();

        assert_ne!(mtime1, mtime2);
    }
}
