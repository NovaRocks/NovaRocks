use crate::types::*;
use anyhow::{Context, Result};
use regex::Regex;
use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

pub fn split_row(line: &str) -> Vec<String> {
    line.split('\t').map(ToString::to_string).collect()
}

pub fn parse_result_set(lines: &[String]) -> ResultSet {
    let lines: Vec<String> = lines
        .iter()
        .map(|line| line.trim_end_matches('\r'))
        .filter(|line| !line.trim().is_empty())
        .map(ToString::to_string)
        .collect();

    if lines.is_empty() {
        return ResultSet::default();
    }

    let header = split_row(&lines[0]);
    let rows = lines[1..].iter().map(|line| split_row(line)).collect();
    ResultSet { header, rows }
}

pub fn render_result_set(header: &[String], rows: &[Vec<String>]) -> String {
    if header.is_empty() && rows.is_empty() {
        return String::new();
    }

    let mut out_lines = Vec::with_capacity(rows.len() + 1);
    out_lines.push(header.join("\t"));
    out_lines.extend(rows.iter().map(|row| row.join("\t")));
    format!("{}\n", out_lines.join("\n"))
}

pub fn load_expected_results(
    result_path: &Path,
    multi_step: bool,
    marker_re: &Regex,
) -> Option<BTreeMap<usize, ResultSet>> {
    use crate::parser::extract_query_number;

    let content = match fs::read_to_string(result_path) {
        Ok(c) => c,
        Err(exc) => {
            println!(
                "Warning: failed to load expected result from {}: {}",
                result_path.display(),
                exc
            );
            return None;
        }
    };

    if multi_step {
        let lines: Vec<String> = content.lines().map(ToString::to_string).collect();
        let markers: Vec<(usize, usize)> = lines
            .iter()
            .enumerate()
            .filter_map(|(idx, line)| extract_query_number(line, marker_re).map(|num| (idx, num)))
            .collect();
        if markers.is_empty() {
            println!(
                "Warning: multi-step expected result must use '-- query N' sections: {}",
                result_path.display()
            );
            return None;
        }

        let mut result_sets = BTreeMap::new();
        for (idx, (start, query_number)) in markers.iter().enumerate() {
            let end = markers
                .get(idx + 1)
                .map(|(next_start, _)| *next_start)
                .unwrap_or(lines.len());
            let body_lines = lines[start + 1..end].to_vec();
            result_sets.insert(*query_number, parse_result_set(&body_lines));
        }
        return Some(result_sets);
    }

    let result_set =
        parse_result_set(&content.lines().map(ToString::to_string).collect::<Vec<_>>());
    Some(BTreeMap::from([(1usize, result_set)]))
}

pub fn write_result_file(
    path: &Path,
    result_sets: &BTreeMap<usize, ResultSet>,
    multi_step: bool,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("create parent dir failed: {}", parent.display()))?;
    }

    let content = if multi_step {
        let mut blocks = Vec::new();
        for (query_number, result_set) in result_sets {
            let mut block = format!("-- query {}\n", query_number);
            block.push_str(&render_result_set(&result_set.header, &result_set.rows));
            blocks.push(block.trim_end_matches('\n').to_string());
        }
        if blocks.is_empty() {
            String::new()
        } else {
            format!("{}\n", blocks.join("\n\n"))
        }
    } else {
        let result_set = result_sets.get(&1).cloned().unwrap_or_default();
        render_result_set(&result_set.header, &result_set.rows)
    };

    fs::write(path, content).with_context(|| format!("write file failed: {}", path.display()))?;
    Ok(())
}

pub fn parse_output(stdout: &str) -> (Vec<String>, Vec<Vec<String>>) {
    let lines: Vec<&str> = stdout
        .lines()
        .map(|line| line.trim_end_matches('\r'))
        .filter(|line| !line.trim().is_empty())
        .collect();

    if lines.is_empty() {
        return (vec![], vec![]);
    }

    let header = split_row(lines[0]);
    let rows = lines[1..].iter().map(|line| split_row(line)).collect();
    (header, rows)
}

pub fn render_output(header: &[String], rows: &[Vec<String>]) -> String {
    let mut lines = Vec::new();
    if !header.is_empty() {
        lines.push(header.join("\t"));
    }
    lines.extend(rows.iter().map(|row| row.join("\t")));
    lines.join("\n")
}

pub fn verify_text_assertions(step: &SqlStep, execution: &QueryExecution) -> (bool, String) {
    let haystack = execution.text_output.as_str();
    for needle in &step.meta.result_contains {
        if !haystack.contains(needle) {
            return (
                false,
                format!("result missing required substring {:?}", needle),
            );
        }
    }
    if !step.meta.result_contains_any.is_empty()
        && !step
            .meta
            .result_contains_any
            .iter()
            .any(|needle| haystack.contains(needle))
    {
        return (
            false,
            format!(
                "result missing all required substrings {:?}",
                step.meta.result_contains_any
            ),
        );
    }
    for needle in &step.meta.result_not_contains {
        if haystack.contains(needle) {
            return (
                false,
                format!("result unexpectedly contains substring {:?}", needle),
            );
        }
    }
    (true, String::new())
}

pub fn parse_float(cell: &str) -> Option<f64> {
    let value = cell.parse::<f64>().ok()?;
    if value.is_finite() { Some(value) } else { None }
}

pub fn cell_equal(expected: &str, actual: &str, epsilon: Option<f64>) -> bool {
    if expected == actual {
        return true;
    }
    let Some(eps) = epsilon else {
        return false;
    };

    let Some(left) = parse_float(expected) else {
        return false;
    };
    let Some(right) = parse_float(actual) else {
        return false;
    };

    (left - right).abs() <= eps
}

pub fn compare_headers(expected: &[String], actual: &[String]) -> (bool, String) {
    if expected == actual {
        (true, String::new())
    } else {
        (
            false,
            format!(
                "header mismatch (actual={:?}, expected={:?})",
                actual, expected
            ),
        )
    }
}

pub fn compare_rows_ordered(
    expected_rows: &[Vec<String>],
    actual_rows: &[Vec<String>],
    epsilon: Option<f64>,
) -> (bool, String) {
    if expected_rows.len() != actual_rows.len() {
        return (
            false,
            format!(
                "row count mismatch (actual={}, expected={})",
                actual_rows.len(),
                expected_rows.len()
            ),
        );
    }

    for (row_idx, (expected_row, actual_row)) in
        expected_rows.iter().zip(actual_rows.iter()).enumerate()
    {
        if expected_row.len() != actual_row.len() {
            return (
                false,
                format!(
                    "column count mismatch at row {} (actual={}, expected={})",
                    row_idx,
                    actual_row.len(),
                    expected_row.len()
                ),
            );
        }

        for (col_idx, (expected_cell, actual_cell)) in
            expected_row.iter().zip(actual_row.iter()).enumerate()
        {
            if !cell_equal(expected_cell, actual_cell, epsilon) {
                return (
                    false,
                    format!(
                        "value mismatch at row {}, col {} (actual={}, expected={})",
                        row_idx, col_idx, actual_cell, expected_cell
                    ),
                );
            }
        }
    }

    (true, String::new())
}

pub fn normalized_cell_for_sort(cell: &str, epsilon: Option<f64>) -> String {
    let Some(eps) = epsilon else {
        return format!("s:{}", cell);
    };

    let Some(value) = parse_float(cell) else {
        return format!("s:{}", cell);
    };

    let bucket = (value / eps).round() as i64;
    format!("f:{}", bucket)
}

pub fn compare_rows_unordered(
    expected_rows: &[Vec<String>],
    actual_rows: &[Vec<String>],
    epsilon: Option<f64>,
) -> (bool, String) {
    if expected_rows.len() != actual_rows.len() {
        return (
            false,
            format!(
                "row count mismatch (actual={}, expected={})",
                actual_rows.len(),
                expected_rows.len()
            ),
        );
    }

    if epsilon.is_none() {
        let mut expected_counter: HashMap<Vec<String>, usize> = HashMap::new();
        let mut actual_counter: HashMap<Vec<String>, usize> = HashMap::new();

        for row in expected_rows {
            *expected_counter.entry(row.clone()).or_insert(0) += 1;
        }
        for row in actual_rows {
            *actual_counter.entry(row.clone()).or_insert(0) += 1;
        }

        if expected_counter == actual_counter {
            return (true, String::new());
        }

        let mut missing_detail = None;
        for (row, count) in &expected_counter {
            let actual_count = actual_counter.get(row).copied().unwrap_or(0);
            if *count > actual_count {
                missing_detail = Some(format!("missing row x{}: {:?}", count - actual_count, row));
                break;
            }
        }

        let mut extra_detail = None;
        for (row, count) in &actual_counter {
            let expected_count = expected_counter.get(row).copied().unwrap_or(0);
            if *count > expected_count {
                extra_detail = Some(format!(
                    "unexpected row x{}: {:?}",
                    count - expected_count,
                    row
                ));
                break;
            }
        }

        let mut details = Vec::new();
        if let Some(d) = missing_detail {
            details.push(d);
        }
        if let Some(d) = extra_detail {
            details.push(d);
        }

        return (false, details.join("; "));
    }

    let mut expected_sorted = expected_rows.to_vec();
    let mut actual_sorted = actual_rows.to_vec();

    let sort_by_epsilon = |a: &Vec<String>, b: &Vec<String>| -> Ordering {
        let ka: Vec<String> = a
            .iter()
            .map(|cell| normalized_cell_for_sort(cell, epsilon))
            .collect();
        let kb: Vec<String> = b
            .iter()
            .map(|cell| normalized_cell_for_sort(cell, epsilon))
            .collect();
        ka.cmp(&kb)
    };

    expected_sorted.sort_by(sort_by_epsilon);
    actual_sorted.sort_by(sort_by_epsilon);
    compare_rows_ordered(&expected_sorted, &actual_sorted, epsilon)
}

pub fn compare_result_sets(
    expected_header: &[String],
    expected_rows: &[Vec<String>],
    actual_header: &[String],
    actual_rows: &[Vec<String>],
    order_sensitive: bool,
    epsilon: Option<f64>,
) -> (bool, String) {
    let (ok, msg) = compare_headers(expected_header, actual_header);
    if !ok {
        return (false, msg);
    }

    if order_sensitive {
        compare_rows_ordered(expected_rows, actual_rows, epsilon)
    } else {
        compare_rows_unordered(expected_rows, actual_rows, epsilon)
    }
}

pub fn case_result_path(result_dir: &Path, case_id: &str) -> PathBuf {
    result_dir.join(format!("{}.result", case_id))
}

pub fn find_legacy_result_paths(result_dir: &Path, case_id: &str) -> Result<Vec<PathBuf>> {
    let prefix = format!("{}-", case_id);
    let mut paths = Vec::new();
    for entry in fs::read_dir(result_dir)
        .with_context(|| format!("read dir failed: {}", result_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if file_name.starts_with(&prefix) && file_name.ends_with(".result") {
            paths.push(path);
        }
    }
    paths.sort();
    Ok(paths)
}

pub fn step_allows_missing_expected_result(step: &SqlStep) -> bool {
    step.meta.expect_error.is_some()
        || step.meta.skip_result_check
        || !step.meta.result_contains.is_empty()
        || !step.meta.result_contains_any.is_empty()
        || !step.meta.result_not_contains.is_empty()
        || step_has_implicit_skip_result(step)
}

pub fn step_requires_recorded_result(step: &SqlStep) -> bool {
    !step_allows_missing_expected_result(step)
}

pub fn step_has_implicit_skip_result(step: &SqlStep) -> bool {
    let normalized = step
        .sql
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();
    normalized.starts_with("refresh materialized view ") && normalized.contains(" with sync mode")
}

pub fn step_retry_count(step: &SqlStep) -> usize {
    step.meta.retry_count.unwrap_or(1)
}

pub fn step_retry_interval(step: &SqlStep) -> Duration {
    Duration::from_millis(step.meta.retry_interval_ms.unwrap_or(1000))
}

pub fn write_mismatch_artifacts(
    root_dir: &Path,
    suite_name: &str,
    artifact_id: &str,
    expected_header: &[String],
    expected_rows: &[Vec<String>],
    actual_header: &[String],
    actual_rows: &[Vec<String>],
    reason: &str,
) -> Result<()> {
    let out_dir = root_dir.join(suite_name).join(artifact_id);
    fs::create_dir_all(&out_dir)
        .with_context(|| format!("create dir failed: {}", out_dir.display()))?;

    let expected = BTreeMap::from([(
        1usize,
        ResultSet {
            header: expected_header.to_vec(),
            rows: expected_rows.to_vec(),
        },
    )]);
    let actual = BTreeMap::from([(
        1usize,
        ResultSet {
            header: actual_header.to_vec(),
            rows: actual_rows.to_vec(),
        },
    )]);

    write_result_file(&out_dir.join("expected.tsv"), &expected, false)?;
    write_result_file(&out_dir.join("actual.tsv"), &actual, false)?;
    fs::write(out_dir.join("diff.txt"), format!("{}\n", reason))
        .with_context(|| format!("write diff failed: {}", out_dir.display()))?;
    Ok(())
}
