use anyhow::Result;
use rusty_git::object;
use rusty_git::repository::Repository;
use structopt::StructOpt;

/// Print an object
#[derive(StructOpt)]
struct Args {
    /// The id of the object to print
    #[structopt(parse(try_from_str))]
    id: object::Id,
}

pub fn main() -> Result<()> {
    let args = Args::from_args();
    let repo = Repository::open(".")?;

    let object = repo.object_database().parse_object(&args.id)?;

    println!("{:#?}", object);
    Ok(())
}
