use anyhow::Result;
use rusty_git::repository::Repository;

pub fn main() -> Result<()> {
    let repo = Repository::open(".")?;
    let head = repo.reference_database().head();

    println!("{:#?}", head);
    Ok(())
}