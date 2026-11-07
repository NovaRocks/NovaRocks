// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::{
    fs,
    panic::{AssertUnwindSafe, catch_unwind},
    path::{Path, PathBuf},
};

use novarocks_parser::{TokenKind, lex, parse};

const MUTATION_SEED: u64 = 0x5A11_C0DE_2026_0818;

#[test]
fn lex_smoke_sql_suite_corpus_is_lossless() {
    let repo_root = repo_root();
    let files = sql_suite_files(&repo_root);
    assert!(
        !files.is_empty(),
        "no SQL corpus files found under the correctness root below {}",
        repo_root.join("tests/sql").display()
    );

    let failures = files
        .iter()
        .flat_map(|path| corpus_failures(&repo_root, path))
        .collect::<Vec<_>>();

    assert!(
        failures.is_empty(),
        "lex-smoke failures (expected empty exception list):\n{}",
        failures.join("\n")
    );
}

#[test]
fn lex_smoke_deterministic_mutations_do_not_panic() {
    let repo_root = repo_root();
    let mut seed = MUTATION_SEED;
    let mut failures = Vec::new();
    let mut mutation_count = 0;

    for path in sql_suite_files(&repo_root) {
        for payload in sql_payloads(&repo_root, &path) {
            let tokens = match checked_payload_tokens(&payload) {
                Ok(Some(tokens)) => tokens,
                // A declared syntax negative that the lexer actually refused
                // has no token stream to mutate. Parse negatives still do.
                Ok(None) => continue,
                Err(error) => {
                    failures.push(format!("{}: {error}", payload.label));
                    continue;
                }
            };

            let mut mutations = Vec::with_capacity(3);
            mutations.push((
                "char-boundary truncation",
                truncate_at_char_boundary(&payload.sql, &mut seed),
            ));
            if let Some(token) = pick_non_end_token(&tokens, &mut seed) {
                mutations.push((
                    "token replacement",
                    replace_token(
                        &payload.sql,
                        token.span.start(),
                        token.span.end(),
                        &mut seed,
                    ),
                ));
                mutations.push((
                    "token deletion",
                    delete_token(&payload.sql, token.span.start(), token.span.end()),
                ));
            }

            for (kind, mutated) in mutations {
                mutation_count += 1;
                if catch_unwind(AssertUnwindSafe(|| lex(&mutated))).is_err() {
                    failures.push(format!("{}: {kind} panicked", payload.label));
                }
            }
        }
    }

    assert!(mutation_count > 0, "the mutation sweep generated no inputs");
    assert!(
        failures.is_empty(),
        "lex-smoke mutation failures (seed {MUTATION_SEED:#x}):\n{}",
        failures.join("\n")
    );
}

fn repo_root() -> PathBuf {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .ancestors()
        .find(|ancestor| {
            ancestor.join("tests/sql/correctness").is_dir() && ancestor.join("Cargo.toml").is_file()
        })
        .map(Path::to_path_buf)
        .unwrap_or_else(|| {
            panic!(
                "could not locate repository root by walking up from {}",
                manifest_dir.display()
            )
        })
}

fn sql_suite_files(repo_root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    collect_sql_files(&repo_root.join("tests/sql/correctness"), &mut files);
    files.sort();
    files
}

fn collect_sql_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", directory.display()))
    {
        let entry = entry.unwrap_or_else(|error| {
            panic!(
                "failed to enumerate an entry below {}: {error}",
                directory.display()
            )
        });
        let path = entry.path();
        if path.is_dir() {
            collect_sql_files(&path, files);
        } else if path.extension().is_some_and(|extension| extension == "sql") {
            files.push(path);
        }
    }
}

fn corpus_failures(repo_root: &Path, path: &Path) -> Vec<String> {
    sql_payloads(repo_root, path)
        .into_iter()
        .filter_map(|payload| {
            checked_payload_tokens(&payload)
                .err()
                .map(|error| format!("{}: {error}", payload.label))
        })
        .collect()
}

fn checked_payload_tokens(
    payload: &SqlPayload,
) -> Result<Option<Vec<novarocks_parser::Token>>, String> {
    let tokens = match lex(&payload.sql) {
        Ok(tokens) => tokens,
        Err(_) if payload.expects_syntax_error => return Ok(None),
        Err(error) => return Err(format!("SQL payload lex failed: {error:?}")),
    };
    // A syntax-negative payload whose lexer succeeded has the same lossless
    // token obligation as any other input, and must actually fail parsing.
    lossless_token_stream(&payload.sql, &tokens)?;
    if payload.expects_syntax_error && parse(&payload.sql).is_ok() {
        return Err("declared SYNTAX_ERROR payload was accepted by the parser".to_owned());
    }
    Ok(Some(tokens))
}

struct SqlPayload {
    label: String,
    sql: String,
    expects_syntax_error: bool,
}

fn sql_payloads(repo_root: &Path, path: &Path) -> Vec<SqlPayload> {
    let relative = path
        .strip_prefix(repo_root)
        .expect("SQL suite file must be below the repository root")
        .display()
        .to_string();
    let source = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read {}: {error}", path.display()));
    payloads_from_source(&relative, &source)
}

fn payloads_from_source(relative: &str, source: &str) -> Vec<SqlPayload> {
    let lines = source.lines().collect::<Vec<_>>();
    let markers = lines
        .iter()
        .enumerate()
        .filter_map(|(index, line)| query_number(line).map(|number| (index, number)))
        .collect::<Vec<_>>();
    let sections = if markers.len() > 1
        && markers
            .iter()
            .enumerate()
            .all(|(index, (_, number))| *number == index + 1)
    {
        markers
            .iter()
            .enumerate()
            .map(|(index, (start, number))| {
                let end = markers
                    .get(index + 1)
                    .map_or(lines.len(), |(next, _)| *next);
                (*number, lines[*start..end].join("\n"))
            })
            .collect()
    } else if is_shell_payload(source) {
        Vec::new()
    } else {
        // Match the native loader's no-marker, line-ending-semicolon frames.
        // A negative directive on one statement cannot excuse a later input.
        let mut sections = Vec::new();
        let mut current = Vec::new();
        let mut number = 1;
        for line in &lines {
            current.push(*line);
            if line.trim().ends_with(';') {
                if current
                    .iter()
                    .any(|line| !line.trim().is_empty() && !line.trim_start().starts_with("--"))
                {
                    sections.push((number, current.join("\n")));
                    number += 1;
                }
                current.clear();
            }
        }
        if current
            .iter()
            .any(|line| !line.trim().is_empty() && !line.trim_start().starts_with("--"))
        {
            sections.push((number, current.join("\n")));
        }
        if sections.is_empty() {
            vec![(1, source.to_owned())]
        } else {
            sections
        }
    };

    sections
        .into_iter()
        .filter(|(_, section)| !is_shell_payload(section))
        .map(|(number, sql)| SqlPayload {
            label: format!("{relative} (query {number})"),
            expects_syntax_error: expects_syntax_error(&sql),
            sql,
        })
        .collect()
}

fn expects_syntax_error(section: &str) -> bool {
    // The native loader reads directives only in the statement's preface.
    // Exact error name/type and last-wins apply; prose or SQL comments after
    // the first executable line grant no negative-input exception.
    section
        .lines()
        .map(str::trim)
        .take_while(|line| line.is_empty() || line.starts_with("--"))
        .filter_map(|line| {
            let body = line.strip_prefix("--")?.trim_start().strip_prefix('@')?;
            let Some((key, value)) = body.split_once('=') else {
                return body
                    .trim()
                    .eq_ignore_ascii_case("expect_trino_error")
                    .then_some(false);
            };
            key.trim()
                .eq_ignore_ascii_case("expect_trino_error")
                .then(|| {
                    value.split(',').map(str::trim).collect::<Vec<_>>()
                        == ["SYNTAX_ERROR", "USER_ERROR"]
                })
        })
        .last()
        .unwrap_or(false)
}

fn query_number(line: &str) -> Option<usize> {
    let body = line.trim().strip_prefix("--")?.trim_start();
    let number = body.strip_prefix("query")?.trim();
    (!number.is_empty()).then(|| number.parse().ok()).flatten()
}

fn is_shell_payload(section: &str) -> bool {
    section
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty() && !line.starts_with("--"))
        .is_some_and(|line| line.starts_with("shell:"))
}

fn lossless_token_stream(source: &str, tokens: &[novarocks_parser::Token]) -> Result<(), String> {
    let mut reconstructed = String::with_capacity(source.len());
    let mut cursor = 0;

    for (index, token) in tokens.iter().enumerate() {
        if token.kind == TokenKind::End {
            if index + 1 != tokens.len() {
                return Err(format!("End token at index {index} is not final"));
            }
            if token.span.start() != source.len() || token.span.end() != source.len() {
                return Err(format!(
                    "End token span {}..{} does not point at EOF {}",
                    token.span.start(),
                    token.span.end(),
                    source.len()
                ));
            }
            continue;
        }

        let start = token.span.start();
        let end = token.span.end();
        if start != cursor {
            return Err(format!(
                "token {index} starts at {start}, expected {cursor}"
            ));
        }
        if start > end || end > source.len() {
            return Err(format!(
                "token {index} has invalid span {start}..{end} for {} bytes",
                source.len()
            ));
        }
        if !source.is_char_boundary(start) || !source.is_char_boundary(end) {
            return Err(format!(
                "token {index} span {start}..{end} is not UTF-8 aligned"
            ));
        }
        reconstructed.push_str(&source[start..end]);
        cursor = end;
    }

    if cursor != source.len() {
        return Err(format!(
            "non-End token spans stop at {cursor}, expected EOF"
        ));
    }
    if reconstructed != source {
        return Err("non-End token spans do not reconstruct the source exactly".to_owned());
    }
    Ok(())
}

fn pick_non_end_token<'a>(
    tokens: &'a [novarocks_parser::Token],
    seed: &mut u64,
) -> Option<&'a novarocks_parser::Token> {
    let non_end = tokens
        .iter()
        .filter(|token| token.kind != TokenKind::End)
        .collect::<Vec<_>>();
    non_end.get(next_index(seed, non_end.len())).copied()
}

fn truncate_at_char_boundary(source: &str, seed: &mut u64) -> String {
    let boundaries = source
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(source.len()))
        .collect::<Vec<_>>();
    source[..boundaries[next_index(seed, boundaries.len())]].to_owned()
}

fn replace_token(source: &str, start: usize, end: usize, seed: &mut u64) -> String {
    const REPLACEMENTS: &[&str] = &["'", "/*", "`", "@", "?"];
    let replacement = REPLACEMENTS[next_index(seed, REPLACEMENTS.len())];
    format!("{}{}{}", &source[..start], replacement, &source[end..])
}

fn delete_token(source: &str, start: usize, end: usize) -> String {
    format!("{}{}", &source[..start], &source[end..])
}

fn next_index(seed: &mut u64, upper_bound: usize) -> usize {
    assert!(
        upper_bound > 0,
        "cannot choose from an empty mutation input"
    );
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*seed >> 32) as usize) % upper_bound
}

#[test]
fn syntax_negative_payload_requires_an_actual_lexer_or_parser_rejection() {
    let payload = |sql: &str| SqlPayload {
        label: "independent negative input".to_owned(),
        sql: sql.to_owned(),
        expects_syntax_error: true,
    };
    assert!(
        checked_payload_tokens(&payload("SELECT @x;"))
            .unwrap()
            .is_none()
    );
    assert!(
        checked_payload_tokens(&payload("SELECT (1;"))
            .unwrap()
            .is_some()
    );
    assert!(checked_payload_tokens(&payload("SELECT 1;")).is_err());
    let ordinary = SqlPayload {
        expects_syntax_error: false,
        ..payload("SELECT @x;")
    };
    assert!(checked_payload_tokens(&ordinary).is_err());
}

#[test]
fn syntax_negative_directive_is_local_to_a_marked_payload_preface() {
    let source = "-- query 1\n-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\nSELECT @x;\n\
                  -- query 2\nSELECT 1;\n\
                  -- query 3\nSELECT '-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR';\n\
                  -- query 4\nSELECT 1;\n-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\nSELECT @late;\n";
    let payloads = payloads_from_source("independent.sql", source);
    assert_eq!(payloads.len(), 4);
    assert!(payloads[0].expects_syntax_error);
    assert!(
        payloads[1..]
            .iter()
            .all(|payload| !payload.expects_syntax_error)
    );
    assert!(checked_payload_tokens(&payloads[0]).unwrap().is_none());
    assert!(checked_payload_tokens(&payloads[1]).unwrap().is_some());
    assert!(checked_payload_tokens(&payloads[2]).unwrap().is_some());
    assert!(checked_payload_tokens(&payloads[3]).is_err());
}

#[test]
fn syntax_negative_directive_does_not_leak_across_semicolon_frames() {
    let payloads = payloads_from_source(
        "independent.sql",
        "-- query 9\n-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\nSELECT @x;\nSELECT @unmarked;\n",
    );
    assert_eq!(payloads.len(), 2);
    assert!(payloads[0].expects_syntax_error);
    assert!(!payloads[1].expects_syntax_error);
    assert!(checked_payload_tokens(&payloads[0]).unwrap().is_none());
    assert!(checked_payload_tokens(&payloads[1]).is_err());
    assert!(payloads_from_source("independent.sql", "shell: echo first;\necho next;").is_empty());
}

#[test]
fn syntax_negative_directive_requires_exact_type_and_respects_last_assignment() {
    for preface in [
        "-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR",
        "-- @expect_trino_error=SYNTAX_ERROR,INTERNAL_ERROR",
        "-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR,extra",
        "-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\n-- @expect_trino_error",
        "-- @expect_trino_error=SYNTAX_ERROR,USER_ERROR\n-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR",
    ] {
        assert!(!expects_syntax_error(&format!("{preface}\nSELECT @x;")));
    }
    assert!(expects_syntax_error(
        "-- @expect_trino_error=INVALID_SESSION_PROPERTY,USER_ERROR\n-- @expect_trino_error = SYNTAX_ERROR, USER_ERROR\nSELECT @x;",
    ));
}
