use anyhow::Result;
use rusty_git::repository::Repository;
use structopt::StructOpt;

/// List references, print a reference, or print an object a reference is pointing to.
/// Example (list): cargo run --example refs
/// Example (head): cargo run --example refs -- --name head
/// Example (ref): cargo run --example refs -- --name "refs/heads/master"
/// Example (peel): cargo run --example refs -- --name "refs/heads/master" --peel
#[derive(StructOpt)]
struct Args {
    /// The name of the ref to read
    #[structopt(long, short, default_value = "")]
    name: String,

    /// If true, print what the reference points to instead of the reference.
    #[structopt(long, short)]
    peel: bool,
}

pub fn main() -> Result<()> {
    let args = Args::from_args();
    let repo = Repository::open(".")?;

    if args.name.is_empty() {
        println!(
            "{:#?}",
            repo.reference_database()
                .reference_names()?
                .iter()
                .map(|n| String::from_utf8_lossy(n).as_ref().to_owned())
                .collect::<Vec<String>>()
        );
        return Ok(());
    }

    let reference = match args.name.to_uppercase().as_str() {
        "HEAD" => repo.reference_database().head()?,
        _ => repo.reference_database().reference(args.name.as_bytes())?,
    };

    if args.peel {
        println!("{:#?}", reference.peel(&repo));
        return Ok(());
    }

    println!("{:#?}", reference);
    Ok(())
}
