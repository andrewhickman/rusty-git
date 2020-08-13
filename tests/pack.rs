mod common;

use std::str::FromStr;

use common::run_test_in_repo;
use rusty_git::object::Id;
use rusty_git::repository::Repository;

#[test]
fn test_pack_file() {
    run_test_in_repo("tests/resources/repo.git", |path| {
        let repo = Repository::open(path).unwrap();

        repo.object_database()
            .read_object(Id::from_str("90012941912143fcf042590f8e152c41b13d5520").unwrap())
            .unwrap();
    });
}
