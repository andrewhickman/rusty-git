mod common;

use std::panic;
use std::path::Path;
use std::str;
use std::str::FromStr as _;

use rusty_git::object::{ObjectData, TreeEntry};
use rusty_git::repository::Repository;

use self::common::*;

#[test]
fn reading_head_produces_same_result_as_libgit2() {
    run_test(|path| {
        let test_file = test_create_file(path, b"Hello world!");

        git_add_file(path, test_file.as_path())
            .expect("failed to add hello world file to git to create test object");

        git_commit(path, "Initial commit.").expect("failed to git commit added file");

        let lg2_repo =
            git2::Repository::open(path).expect("failed to open repository with libgit2");
        let lg2_head = lg2_repo
            .head()
            .expect("failed to get head from libgit2 repo");

        let repo = Repository::open(path).expect("failed to open repository with rusty_git");
        let head = repo
            .reference_database()
            .head()
            .expect("failed to get head from rusty_git reference database");

        assert_eq!(
            lg2_head.name().ok_or("libgit2 name was empty").unwrap(),
            head.name().ok_or("rusty_git name was empty").unwrap()
        );
    });
}

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
    run_test_in_repo(|path| {
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
    run_test_in_repo(|path| {
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
