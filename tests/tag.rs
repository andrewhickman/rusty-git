mod common;

use std::str::FromStr;
use std::string::ToString;

use self::common::*;

use rusty_git::object::{self, ObjectData};
use rusty_git::repository::Repository;

#[test]
fn reading_tag_produces_same_result_as_libgit2() {
    run_test_in_repo(|path| {
        git_tag(path, "mytag", Some("my message"));

        let lg_repo = git2::Repository::open(path).unwrap();
        let rg_repo = Repository::open(path).unwrap();

        let lg_ref = lg_repo.find_reference("refs/tags/mytag").unwrap();
        let lg_id = lg_ref.target().unwrap();
        let rg_id = object::Id::from_str(&lg_id.to_string()).unwrap();

        let lg_tag = lg_repo.find_tag(lg_id).unwrap();
        let rg_obj = rg_repo.object_database().parse_object(&rg_id).unwrap();
        let rg_tag = match rg_obj.data() {
            ObjectData::Tag(tag) => tag,
            _ => panic!("expected tag"),
        };

        assert_eq!(lg_tag.name_bytes(), rg_tag.tag());
        assert_eq!(lg_tag.target_id().to_string(), rg_tag.object().to_string());
        assert_eq!(lg_tag.message_bytes().unwrap(), rg_tag.message().unwrap());
        assert_eq!(
            lg_tag.tagger().unwrap().name_bytes(),
            rg_tag.tagger().unwrap().name()
        );
    })
}
