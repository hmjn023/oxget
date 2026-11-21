use std::{
    io::Write,
    path::Path,
    process::{Command, Stdio},
};

use serde_json::{Value, json};

fn cli(path: &Path, args: &[&str], stdin: Option<&str>) -> (i32, Value) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_oxget"))
        .arg("-d")
        .arg(path)
        .arg("--json")
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    if let Some(text) = stdin {
        input.write_all(text.as_bytes()).unwrap();
    }
    drop(input);
    let result = child.wait_with_output().unwrap();
    assert!(
        result.stderr.is_empty(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let text = String::from_utf8(result.stdout).unwrap();
    assert_eq!(text.lines().count(), 1, "{text}");
    let value: Value = serde_json::from_str(&text).expect("stdout is one JSON response");
    assert_eq!(value["schema_version"], 1);
    (result.status.code().unwrap(), value)
}

#[test]
fn offline_commands_are_idempotent_and_preserve_files() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let (code, added) = cli(
        path,
        &[
            "add",
            "https://example.org/a.zip",
            "https://example.org/b.zip",
        ],
        None,
    );
    assert_eq!(code, 0);
    assert_eq!(added["data"]["ids"], json!([1, 2]));
    assert_eq!(added["data"]["added_ids"], json!([1, 2]));
    assert_eq!(added["data"]["running"], false);
    let (_, repeated) = cli(
        path,
        &["add", "https://example.org/a.zip", "--paused"],
        None,
    );
    assert_eq!(repeated["data"]["existing_ids"], json!([1]));
    assert_eq!(repeated["data"]["jobs"][0]["state"], "queued");
    for _ in 0..2 {
        assert_eq!(cli(path, &["pause", "1"], None).0, 0);
    }
    let (_, list) = cli(path, &["list", "--state", "paused"], None);
    assert_eq!(list["data"]["jobs"].as_array().unwrap().len(), 1);
    assert_eq!(list["data"]["jobs"][0]["id"], 1);
    assert_eq!(cli(path, &["move", "2", "--before", "1"], None).0, 0);
    let (_, list) = cli(path, &["list"], None);
    assert_eq!(list["data"]["jobs"][0]["id"], 2);
    assert_eq!(list["data"]["jobs"][1]["id"], 1);
    assert_eq!(cli(path, &["pause", "2", "9999"], None).0, 1);
    assert_eq!(
        cli(path, &["status", "2"], None).1["data"]["jobs"][0]["state"],
        "queued"
    );
    std::fs::write(path.join("a.zip"), b"keep completed file").unwrap();
    std::fs::create_dir_all(path.join(".oxget/parts/1")).unwrap();
    std::fs::write(path.join(".oxget/parts/1/0.part"), b"discard partial").unwrap();
    for _ in 0..2 {
        assert_eq!(cli(path, &["remove", "1"], None).0, 0);
    }
    assert_eq!(
        std::fs::read(path.join("a.zip")).unwrap(),
        b"keep completed file"
    );
    assert!(!path.join(".oxget/parts/1").exists());
    assert_eq!(
        cli(path, &["status", "1"], None).1["error"]["code"],
        "not_found"
    );
    for _ in 0..2 {
        assert_eq!(cli(path, &["stop"], None).0, 0);
    }
}

#[test]
fn stdin_and_structured_errors_work_without_a_terminal() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let (code, added) = cli(
        path,
        &["add", "-i", "-", "--paused"],
        Some("\u{feff}# list\nhttps://example.org/a\nhttps://example.org/b\n"),
    );
    assert_eq!(code, 0);
    assert_eq!(added["data"]["jobs"][0]["state"], "paused");
    let (code, timed_out) = cli(path, &["wait", "1", "--timeout", "0"], None);
    assert_eq!(code, 3);
    assert_eq!(timed_out["error"]["code"], "timeout");
    assert_eq!(
        cli(path, &["wait", "999", "--timeout", "0"], None).1["error"]["code"],
        "not_found"
    );
    for args in [
        vec!["pause"],
        vec!["move", "1"],
        vec!["pause", "1", "--all"],
        vec!["add"],
        vec!["add", "ftp://example.org/a"],
    ] {
        let (code, error) = cli(path, &args, None);
        assert_eq!(code, 2, "{args:?}: {error}");
        assert_eq!(error["ok"], false);
        assert_eq!(error["error"]["code"], "invalid_arguments");
    }
}

#[test]
fn failed_jobs_can_be_filtered_retried_and_waited_on_offline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    cli(path, &["add", "https://example.org/a"], None);
    let state = path.join(".oxget/queue.json");
    let mut queue: Value = serde_json::from_slice(&std::fs::read(&state).unwrap()).unwrap();
    queue["jobs"][0]["status"] = json!("Failed");
    queue["jobs"][0]["error"] = json!("test transfer failure");
    std::fs::write(&state, serde_json::to_vec(&queue).unwrap()).unwrap();
    let (code, result) = cli(path, &["wait", "1", "--timeout", "0"], None);
    assert_eq!(code, 4);
    assert_eq!(result["error"]["code"], "download_failed");
    assert_eq!(
        cli(path, &["list", "--state", "failed"], None).1["data"]["jobs"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    for _ in 0..2 {
        assert_eq!(cli(path, &["retry", "--failed"], None).0, 0);
    }
    let (_, state) = cli(path, &["status", "1"], None);
    assert_eq!(state["data"]["jobs"][0]["state"], "queued");
    assert!(state["data"]["jobs"][0]["error"].is_null());
}

#[test]
fn checksum_arguments_are_validated_and_existing_hashes_cannot_silently_change() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path();
    let a = "ab".repeat(32);
    let b = "cd".repeat(32);
    assert_eq!(
        cli(
            path,
            &["add", "https://example.org/a", "--sha256", &a, "--paused"],
            None
        )
        .0,
        0
    );
    assert_eq!(
        cli(
            path,
            &["add", "https://example.org/a", "--sha256", &a],
            None
        )
        .1["data"]["existing_ids"],
        json!([1])
    );
    let state = std::fs::read(path.join(".oxget/queue.json")).unwrap();
    let (code, error) = cli(
        path,
        &["add", "https://example.org/a", "--sha256", &b],
        None,
    );
    assert_eq!(code, 1);
    assert_eq!(error["error"]["code"], "hash_conflict");
    assert_eq!(
        std::fs::read(path.join(".oxget/queue.json")).unwrap(),
        state
    );
    for args in [
        vec!["add", "https://example.org/b", "--sha256", "short"],
        vec![
            "add",
            "https://example.org/b",
            "https://example.org/c",
            "--sha256",
            &a,
        ],
        vec!["status", "--sha256", &a],
    ] {
        assert_eq!(cli(path, &args, None).0, 2);
    }
    // A checksum can be attached to an existing unstarted job, retaining its ID.
    cli(path, &["add", "https://example.org/b", "--paused"], None);
    let (_, attached) = cli(
        path,
        &["add", "https://example.org/b", "--sha256", &a],
        None,
    );
    assert_eq!(attached["data"]["ids"], json!([2]));
    assert_eq!(
        attached["data"]["jobs"][0]["verification"]["expected_sha256"],
        a
    );
}
