use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::panic;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::Output;
use std::str;
use std::str::FromStr as _;

use tempdir::TempDir;

use rusty_git::repository::Repository;
use rusty_git::object::Object;

#[test]
fn reading_objects_produces_same_result_as_libgit2() {
    run_test(|path| {
        let test_file = test_create_hello_world_file(path);
        git_add_file(path, test_file.as_path())
            .expect("failed to add hello world file to git to create test object");

        let cli_objects = git_get_objects(path);
        let target_object_id = cli_objects[0].to_owned();

        let lg2_repo = git2::Repository::init(path).expect("failed to initialize git repository");

        let lg2_odb = lg2_repo.odb().expect("failed to open object database");

        let lg2_object_id = git2::Oid::from_str(target_object_id.as_str())
            .expect("failed to read real git id using lg2");

        let lg2_object = lg2_odb
            .read(lg2_object_id)
            .expect("failed to read object using lg2");

        assert_eq!(b"Hello world!", lg2_object.data());

        let repo = Repository::open(path).expect("failed to open repository with rusty_git");

        let object_id = rusty_git::object::Id::from_str(target_object_id.as_str())
            .expect("failed to read object ID using rusty_git");

        let object = repo
            .object_database()
            .parse_object(&object_id)
            .expect("failed to get object with rusty_git");
        let blob = match object {
            Object::Blob(blob) => blob,
            _ => panic!("expected object to be a blob"),
        };

        assert_eq!(b"Hello world!", blob.data());
    });
}

fn test_create_hello_world_file(path: &Path) -> PathBuf {
    let file_name = "hello_world.txt";
    let mut file = File::create(path.join(file_name)).expect("failed to create hello world file");

    file.write_all(b"Hello world!")
        .expect("failed to write to hello world file");

    path.join(file_name)
}

fn git_add_file(path: &Path, file: &Path) -> Result<Output, io::Error> {
    Command::new("git")
        .current_dir(path)
        .arg("add")
        .arg(file)
        .output()
}

fn git_get_objects(path: &Path) -> Vec<String> {
    let output = Command::new("git")
        .current_dir(path)
        .arg("cat-file")
        .arg("--batch-check")
        .arg("--batch-all-objects")
        .output()
        .expect("failed to read git objects using cat-file");

    let text =
        str::from_utf8(output.stdout.as_slice()).expect("failed to parse output from git cat-file");

    text.split('\n')
        .map(|line| line.split(' ').collect::<Vec<&str>>()[0])
        .map(|s| String::from(s))
        .collect()
}

fn git_init(path: &Path) -> Result<Output, io::Error> {
    Command::new("git").current_dir(path).arg("init").output()
}

fn run_test<T>(test: T)
where
    T: FnOnce(&Path) + panic::UnwindSafe,
{
    let directory = setup();
    let result = panic::catch_unwind(|| test(directory.path()));
    teardown(directory);

    assert!(result.is_ok());
}

fn setup() -> TempDir {
    let temp = TempDir::new("test-").expect("failed to create test directory");
    println!("path: {}", temp.path().display());
    git_init(temp.path()).expect("failed to initialize git repository");
    temp
}

fn teardown(temp: TempDir) {
    let path = temp.path().to_owned();
    temp.close()
        .unwrap_or_else(|_| panic!("failed to clean up test directory: {}", path.display()));
}
