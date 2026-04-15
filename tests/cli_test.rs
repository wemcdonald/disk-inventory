use assert_cmd::Command;

#[test]
fn test_help() {
    Command::cargo_bin("disk-inventory")
        .unwrap()
        .arg("--help")
        .assert()
        .success();
}

#[test]
fn test_usage_help() {
    Command::cargo_bin("disk-inventory")
        .unwrap()
        .args(["usage", "--help"])
        .assert()
        .success();
}

#[test]
fn test_top_help() {
    Command::cargo_bin("disk-inventory")
        .unwrap()
        .args(["top", "--help"])
        .assert()
        .success();
}

#[test]
fn test_search_help() {
    Command::cargo_bin("disk-inventory")
        .unwrap()
        .args(["search", "--help"])
        .assert()
        .success();
}

#[test]
fn test_types_help() {
    Command::cargo_bin("disk-inventory")
        .unwrap()
        .args(["types", "--help"])
        .assert()
        .success();
}
