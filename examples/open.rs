use anyhow::Result;
use rusty_git::repository::Repository;

pub fn main() -> Result<()> {
    let repo = Repository::open(".")?;

    println!("{:?}", repo);
    Ok(())
}
