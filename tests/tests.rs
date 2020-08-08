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

use rusty_git::object::Object;
use rusty_git::repository::Repository;

#[test]
fn reading_file_produces_same_result_as_libgit2() {
    run_test(|path| {
        let test_file = test_create_file(path, b"Hello world!");

        git_add_file(path, test_file.as_path())
            .expect("failed to add hello world file to git to create test object");

        let cli_objects = git_get_objects(path);
        let target_object_id = cli_objects[0].to_owned();

        let lg2_object = test_libgit2_read_object(path, target_object_id.as_str());
        assert_eq!(b"Hello world!", lg2_object.as_slice());

        let object = test_rusty_git_read_object(path, target_object_id.as_str());
        assert_eq!(b"Hello world!", object.as_slice());
    });
}

#[test]
fn reading_commit_produces_same_result_as_libgit2() {
    run_test(|path| {
        let test_file = test_create_file(path, b"Hello world!");

        git_add_file(path, test_file.as_path())
            .expect("failed to add hello world file to git to create test object");

        git_commit(path, "Initial commit.").expect("failed to git commit added file");

        let cli_objects = git_get_objects(path);
        let target_object_id = cli_objects[2].to_owned();

        let expected_commit_message = String::from_utf8(
            git_log(
                path,
                &[
                    "-1",
                    format!("--format=tree {}%nauthor %an <%ae> %ad%ncommitter %cn <%ce> %cd%n%n%B", cli_objects[1]).as_str(),
                    "--date=raw",
                ],
            )
            .expect("failed to get latest git commit message")
            .stdout
            .split_last()
            .unwrap()
            .1
            .to_vec(),
        )
        .expect("failed to parse commit message as utf8");

        let lg2_object = test_libgit2_read_object(path, target_object_id.as_str());
        assert_eq!(
            expected_commit_message,
            String::from_utf8(lg2_object).expect("failed to parse libgit2 commit object as utf8")
        );

        let object = test_rusty_git_read_object(path, target_object_id.as_str());
        assert_eq!(
            expected_commit_message,
            String::from_utf8(object).expect("failed to parse rusty git commit object as utf8")
        );
    });
}

fn test_rusty_git_read_object(cwd: &Path, id: &str) -> Vec<u8> {
    let repo = Repository::open(cwd).expect("failed to open repository with rusty_git");

    let object_id =
        rusty_git::object::Id::from_str(id).expect("failed to read object ID using rusty_git");

    let object = repo
        .object_database()
        .parse_object(&object_id)
        .expect("failed to get object with rusty_git");

    let blob = match object {
        Object::Blob(blob) => blob,
        _ => panic!("expected object to be a blob"),
    };

    blob.data().to_vec()
}

fn test_libgit2_read_object(cwd: &Path, id: &str) -> Vec<u8> {
    let repo = git2::Repository::init(cwd).expect("failed to initialize git repository");

    let odb = repo.odb().expect("failed to open object database");

    let object_id = git2::Oid::from_str(id).expect("failed to read real git id using lg2");

    let object = odb
        .read(object_id)
        .expect("failed to read object using lg2");

    object.data().to_vec()
}

fn test_create_file(path: &Path, content: &[u8]) -> PathBuf {
    let file_name = "hello_world.txt";
    let mut file = File::create(path.join(file_name)).expect("failed to create hello world file");

    file.write_all(content)
        .expect("failed to write to hello world file");

    path.join(file_name)
}

fn git_log(cwd: &Path, args: &[&str]) -> Result<Output, io::Error> {
    Command::new("git")
        .current_dir(cwd)
        .arg("log")
        .args(args)
        .output()
}

fn git_commit(cwd: &Path, message: &str) -> Result<Output, io::Error> {
    Command::new("git")
        .current_dir(cwd)
        .arg("commit")
        .arg("-m")
        .arg(message)
        .output()
}

fn git_add_file(cwd: &Path, file: &Path) -> Result<Output, io::Error> {
    Command::new("git")
        .current_dir(cwd)
        .arg("add")
        .arg(file)
        .output()
}

fn git_get_objects(cwd: &Path) -> Vec<String> {
    let output = Command::new("git")
        .current_dir(cwd)
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

fn git_init(cwd: &Path) -> Result<Output, io::Error> {
    Command::new("git").current_dir(cwd).arg("init").output()
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
