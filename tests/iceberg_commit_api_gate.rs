use std::fs;
use std::path::{Path, PathBuf};

fn collect_rust_files(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(dir).expect("read source directory") {
        let entry = entry.expect("read source entry");
        let path = entry.path();
        if path.is_dir() {
            collect_rust_files(&path, out);
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("rs") {
            out.push(path);
        }
    }
}

fn is_allowed_legacy_test_helper(repo: &Path, path: &Path) -> bool {
    let rel = path.strip_prefix(repo).expect("source under repo");
    rel == Path::new("src/connector/iceberg/commit/test_helpers.rs")
}

#[test]
fn production_sources_do_not_call_legacy_run_iceberg_commit() {
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut files = Vec::new();
    collect_rust_files(&repo.join("src"), &mut files);

    let mut offenders = Vec::new();
    for file in files {
        if is_allowed_legacy_test_helper(&repo, &file) {
            continue;
        }
        let text = fs::read_to_string(&file).expect("read rust source");
        for (idx, line) in text.lines().enumerate() {
            let trimmed = line.trim_start();
            if trimmed.starts_with("//") || trimmed.starts_with("*") {
                continue;
            }
            if line.contains("run_iceberg_commit(") {
                let rel = file.strip_prefix(&repo).expect("relative source path");
                offenders.push(format!("{}:{}", rel.display(), idx + 1));
            }
        }
    }

    assert!(
        offenders.is_empty(),
        "production code must call run_iceberg_commit_typed, not legacy run_iceberg_commit:\n{}",
        offenders.join("\n")
    );
}
