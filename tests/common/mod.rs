#![allow(dead_code)]

use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::panic;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::str;

use tempdir::TempDir;

pub fn test_write_file(path: &Path, content: &[u8], file_name: &str) -> PathBuf {
    let mut file = File::create(path.join(file_name)).expect("failed to create file");
    file.write_all(content).expect("failed to write to file");

    path.join(file_name)
}

pub fn abuse_git_log_to_get_data(cwd: &Path, format: &str) -> String {
    str::from_utf8(
        git_log(
            cwd,
            &[format!("--format={}", format).as_str(), "--date=raw"],
        )
        .stdout
        .as_slice(),
    )
    .unwrap()
    .trim()
    .to_owned()
}

pub fn git_log(cwd: &Path, args: &[&str]) -> Output {
    let output = Command::new("git")
        .current_dir(cwd)
        .arg("log")
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success());
    output
}

pub fn git_branch(cwd: &Path, name: &str) {
    assert!(Command::new("git")
        .current_dir(cwd)
        .arg("branch")
        .arg(name)
        .status()
        .unwrap()
        .success())
}

pub fn git_commit(cwd: &Path, message: &str) {
    assert!(Command::new("git")
        .stderr(Stdio::null())
        .stdout(Stdio::null())
        .current_dir(cwd)
        .arg("-c")
        .arg("user.name=test")
        .arg("commit")
        .arg("--message")
        .arg(message)
        .status()
        .unwrap()
        .success())
}

pub fn git_tag(cwd: &Path, name: &str, message: Option<&str>) {
    let mut cmd = Command::new("git");
    cmd.stderr(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.arg("-c");
    cmd.arg("user.name=test");
    cmd.current_dir(cwd).arg("tag").arg(name);
    if let Some(message) = message {
        cmd.arg("--annotate");
        cmd.arg("--message");
        cmd.arg(message);
    }
    assert!(cmd.status().unwrap().success());
}

pub fn git_add_file(cwd: &Path, file: &Path) {
    assert!(Command::new("git")
        .current_dir(cwd)
        .arg("add")
        .arg(file)
        .status()
        .unwrap()
        .success());
}

pub fn git_get_objects(cwd: &Path) -> Vec<String> {
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

pub fn git_init(cwd: &Path) -> Result<Output, io::Error> {
    Command::new("git").current_dir(cwd).arg("init").output()
}

pub fn git_clone(cwd: &Path, src: &str) {
    assert!(Command::new("git")
        .current_dir(cwd)
        .arg("clone")
        .arg(src)
        .arg(cwd)
        .output()
        .unwrap()
        .status
        .success());
}

pub fn run_test<T>(test: T)
where
    T: FnOnce(&Path) + panic::UnwindSafe,
{
    let directory = setup();
    let result = panic::catch_unwind(|| test(directory.path()));
    teardown(directory);

    assert!(result.is_ok());
}

pub fn run_test_in_new_repo<T>(test: T)
where
    T: FnOnce(&Path) + panic::UnwindSafe,
{
    run_test(|path| {
        git_init(path).expect("failed to initialize git repository");

        let test_file = test_write_file(path, b"Hello world!", "hello_world.txt");

        git_add_file(path, test_file.as_path());

        git_commit(path, "Initial commit.");

        test(path)
    })
}

pub fn run_test_in_repo<T>(repo: &str, test: T)
where
    T: FnOnce(&Path) + panic::UnwindSafe,
{
    run_test(|path| {
        let repo_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(repo);

        git_clone(path, repo_path.as_os_str().to_str().unwrap());

        test(path)
    })
}

pub fn setup() -> TempDir {
    let temp = TempDir::new("test-").expect("failed to create test directory");
    println!("path: {}", temp.path().display());
    temp
}

pub fn teardown(temp: TempDir) {
    let path = temp.path().to_owned();
    temp.close()
        .unwrap_or_else(|_| panic!("failed to clean up test directory: {}", path.display()));
}
