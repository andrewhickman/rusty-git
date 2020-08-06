use std::io;

use anyhow::Result;
use rusty_git::repository::Repository;
use rusty_git::object;
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

    let mut reader = repo.object_database().read_object(&args.id)?;

    io::copy(&mut reader, &mut io::stdout().lock())?;
    Ok(())
}
