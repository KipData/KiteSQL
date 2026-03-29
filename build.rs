use std::env;
use std::fs;
use std::path::Path;

const SUPPORTED_ROCKSDB_VERSION: &str = "0.23.0";
const SUPPORTED_LIBROCKSDB_SYS_VERSION: &str = "0.17.1+9.9.3";

fn main() {
    println!("cargo:rerun-if-changed=Cargo.lock");
    println!("cargo:rerun-if-env-changed=CARGO_FEATURE_UNSAFE_TXDB_CHECKPOINT");

    if env::var_os("CARGO_FEATURE_UNSAFE_TXDB_CHECKPOINT").is_none() {
        return;
    }

    let lock_path = Path::new("Cargo.lock");
    let lock_contents = fs::read_to_string(lock_path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", lock_path.display()));

    ensure_locked_version(
        &lock_contents,
        "rocksdb",
        SUPPORTED_ROCKSDB_VERSION,
        "unsafe_txdb_checkpoint",
    );
    ensure_locked_version(
        &lock_contents,
        "librocksdb-sys",
        SUPPORTED_LIBROCKSDB_SYS_VERSION,
        "unsafe_txdb_checkpoint",
    );
}

fn ensure_locked_version(
    lock_contents: &str,
    expected_name: &str,
    expected_version: &str,
    feature_name: &str,
) {
    let found_version = find_locked_version(lock_contents, expected_name);

    match found_version.as_deref() {
        Some(version) if version == expected_version => {}
        Some(version) => panic!(
            "feature `{feature_name}` only supports `{expected_name} = {expected_version}`, found `{expected_name} = {version}` in Cargo.lock; disable the feature or re-validate the implementation"
        ),
        None => panic!(
            "feature `{feature_name}` requires `{expected_name} = {expected_version}` in Cargo.lock, but `{expected_name}` was not found"
        ),
    }
}

fn find_locked_version(lock_contents: &str, expected_name: &str) -> Option<String> {
    let mut current_name = None;
    let mut current_version = None;

    for line in lock_contents.lines() {
        let line = line.trim();
        if line == "[[package]]" {
            if current_name.as_deref() == Some(expected_name) {
                return current_version;
            }
            current_name = None;
            current_version = None;
            continue;
        }

        if let Some(value) = extract_toml_string(line, "name") {
            current_name = Some(value.to_string());
            continue;
        }

        if let Some(value) = extract_toml_string(line, "version") {
            current_version = Some(value.to_string());
        }
    }

    if current_name.as_deref() == Some(expected_name) {
        current_version
    } else {
        None
    }
}

fn extract_toml_string<'a>(line: &'a str, key: &str) -> Option<&'a str> {
    let prefix = match key {
        "name" => "name = \"",
        "version" => "version = \"",
        _ => return None,
    };
    let rest = line.strip_prefix(prefix)?;
    rest.strip_suffix('"')
}
