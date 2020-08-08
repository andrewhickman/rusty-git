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

use rusty_git::object::{ObjectData, TreeEntry};
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

        let object = test_rusty_git_read_blob(path, target_object_id.as_str());
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

        let target_object_id = String::from_utf8(
            git_log(path, &["-1", "--format=%H"])
                .expect("failed to get latest git commit hash")
                .stdout,
        )
        .expect("failed to parse commit hash as utf8")
        .trim()
        .to_owned();

        let git_author_name = abuse_git_log_to_get_data(path, "%an");
        let git_author_email = abuse_git_log_to_get_data(path, "%ae");
        let git_committer_name = abuse_git_log_to_get_data(path, "%cn");
        let git_committer_email = abuse_git_log_to_get_data(path, "%ce");

        let lg2_repo = git2::Repository::init(path).expect("failed to initialize git repository");
        let lg2_object_id = git2::Oid::from_str(target_object_id.as_str())
            .expect("failed to read real git id using lg2");

        let lg2_commit = lg2_repo
            .find_commit(lg2_object_id)
            .expect("failed to read commit using lg2");

        assert_eq!(git_author_name, lg2_commit.author().name().unwrap());
        assert_eq!(git_author_email, lg2_commit.author().email().unwrap());
        assert_eq!(git_committer_name, lg2_commit.committer().name().unwrap());
        assert_eq!(git_committer_email, lg2_commit.committer().email().unwrap());

        let repo = Repository::open(path).expect("failed to open repository with rusty_git");

        let object_id = rusty_git::object::Id::from_str(target_object_id.as_str())
            .expect("failed to read object ID using rusty_git");

        let commit_object = repo
            .object_database()
            .parse_object(&object_id)
            .expect("failed to parse tree object with rusty git");

        let commit = match commit_object.data() {
            ObjectData::Commit(commit) => commit,
            _ => panic!("expected object to be a commit"),
        };

        assert_eq!(git_author_name, commit.author().name());
        assert_eq!(git_author_email, commit.author().email());
        assert_eq!(git_committer_name, commit.committer().name());
        assert_eq!(git_committer_email, commit.committer().email());
    });
}

#[test]
fn reading_tree_produces_same_result_as_libgit2() {
    run_test(|path| {
        let test_file = test_create_file(path, b"Hello world!");

        git_add_file(path, test_file.as_path())
            .expect("failed to add hello world file to git to create test object");

        git_commit(path, "Initial commit.").expect("failed to git commit added file");

        let cli_objects = git_get_objects(path);

        let lg2_repo = git2::Repository::init(path).expect("failed to initialize git repository");
        let lg2_head = lg2_repo.head().unwrap();
        let lg2_tree = lg2_head.peel_to_tree().unwrap();

        let lg2_tree_id = lg2_tree.id().to_string();
        let mut lg2_blob_id = String::new();

        lg2_tree
            .walk(git2::TreeWalkMode::PreOrder, |_, entry| {
                // There is only one thing in this tree, and we know it's a blob.
                lg2_blob_id = entry.id().to_string();
                git2::TreeWalkResult::Ok
            })
            .unwrap();

        assert!(cli_objects.contains(&lg2_tree_id));
        assert!(cli_objects.contains(&lg2_blob_id));

        let repo = Repository::open(path).expect("failed to open repository with rusty_git");
        let target_tree_id = rusty_git::object::Id::from_str(lg2_tree_id.as_str())
            .expect("failed to read tree ID using rusty_git");

        let tree_object = repo
            .object_database()
            .parse_object(&target_tree_id)
            .expect("failed to parse tree object with rusty git");

        let tree = match tree_object.data() {
            ObjectData::Tree(tree) => tree,
            _ => panic!("expected object to be a tree"),
        };

        let tree_id = tree_object.id().to_string();
        let blob_id = tree.entries().collect::<Vec<TreeEntry>>()[0]
            .id()
            .to_string();

        assert_eq!(lg2_tree_id, tree_id);
        assert_eq!(lg2_blob_id, blob_id);
    });
}

fn test_rusty_git_read_blob(cwd: &Path, id: &str) -> Vec<u8> {
    let repo = Repository::open(cwd).expect("failed to open repository with rusty_git");

    let object_id =
        rusty_git::object::Id::from_str(id).expect("failed to read object ID using rusty_git");

    let object = repo
        .object_database()
        .parse_object(&object_id)
        .expect("failed to get object with rusty_git");

    let blob = match object.data() {
        ObjectData::Blob(blob) => blob,
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

fn abuse_git_log_to_get_data(cwd: &Path, format: &str) -> String {
    str::from_utf8(
        git_log(
            cwd,
            &[format!("--format={}", format).as_str(), "--date=raw"],
        )
        .unwrap()
        .stdout
        .as_slice(),
    )
    .unwrap()
    .trim()
    .to_owned()
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
        .map(String::from)
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
