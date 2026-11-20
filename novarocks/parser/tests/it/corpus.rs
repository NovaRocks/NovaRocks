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
    panic::catch_unwind,
    path::{Path, PathBuf},
};

use novarocks_parser::{
    ast::SyntaxEq,
    lexer::lex,
    parser::{frame_statements, parse_statement},
    printer::print_statement,
    token::{Token, TokenKind},
};

const MUTATION_SEED: u64 = 0x5A11_C0DE_2026_0818;
const REPLACEMENTS: &[&str] = &["'", "/*", "`", "@", "?", "$$"];

fn lossless(source: &str) -> Result<Vec<Token>, String> {
    let tokens = lex(source).map_err(|error| format!("{error:?}"))?;
    let Some((end, content)) = tokens.split_last() else {
        return Err("missing EOF token".into());
    };
    if end.kind != TokenKind::End
        || end.span.start() != source.len()
        || end.span.end() != source.len()
    {
        return Err(format!("invalid EOF token {end:?}"));
    }
    let mut offset = 0;
    let mut reconstructed = String::with_capacity(source.len());
    for token in content {
        let start = token.span.start();
        let end = token.span.end();
        if token.kind == TokenKind::End || start != offset || end <= start {
            return Err(format!("unexpected token {token:?} at byte {offset}"));
        }
        let piece = source
            .get(start..end)
            .ok_or_else(|| format!("invalid UTF-8 span {start}..{end}"))?;
        reconstructed.push_str(piece);
        offset = end;
    }
    if reconstructed != source {
        return Err(format!(
            "token spans reconstruct {offset} of {} bytes",
            source.len()
        ));
    }
    Ok(tokens)
}

fn next_index(seed: &mut u64, length: usize) -> usize {
    *seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
    ((*seed >> 32) as usize) % length
}

fn mutations(source: &str, tokens: &[Token], seed: &mut u64) -> Vec<(&'static str, String)> {
    let boundaries: Vec<_> = source
        .char_indices()
        .map(|(index, _)| index)
        .chain(std::iter::once(source.len()))
        .collect();
    let truncate_at = boundaries[next_index(seed, boundaries.len())];
    let mut results = vec![("truncate", source[..truncate_at].to_owned())];
    let Some((_, content)) = tokens.split_last() else {
        return results;
    };
    if !content.is_empty() {
        let token = content[next_index(seed, content.len())];
        let before = &source[..token.span.start()];
        let after = &source[token.span.end()..];
        let replacement = REPLACEMENTS[next_index(seed, REPLACEMENTS.len())];
        results.push(("replace", format!("{before}{replacement}{after}")));
        results.push(("delete", format!("{before}{after}")));
    }
    results
}

fn check_round_trip(source: &str) -> Result<(), String> {
    let statement = parse_statement(source).map_err(|error| format!("parse: {error:?}"))?;
    let rendered = print_statement(&statement).map_err(|error| format!("print: {error:?}"))?;
    let reparsed = parse_statement(&rendered).map_err(|error| {
        format!(
            "reparse: {error:?}
printed: {rendered}"
        )
    })?;
    if !statement.syntax_eq(&reparsed) {
        return Err(format!(
            "syntax trees differ after printing
printed: {rendered}"
        ));
    }
    Ok(())
}

fn sql_files(directory: &Path, files: &mut Vec<PathBuf>) {
    for entry in fs::read_dir(directory).expect("read checked-in SQL directory") {
        let entry = entry.expect("read SQL directory entry");
        let path = entry.path();
        let kind = entry.file_type().expect("read SQL entry type");
        if kind.is_dir() {
            sql_files(&path, files);
        } else if kind.is_file() && path.extension().is_some_and(|ext| ext == "sql") {
            files.push(path);
        }
    }
}

#[test]
fn repository_corpus_lexer_and_parser_keep_sql_bytes_and_survive_mutation() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../tests/sql/correctness");
    let mut files = Vec::new();
    sql_files(&root, &mut files);
    files.sort();
    let mut seed = MUTATION_SEED;
    let mut failures = Vec::new();
    let (mut lossless_files, mut framed_files, mut round_trips, mut edited) = (0, 0, 0, 0);
    for file in &files {
        let label = file.strip_prefix(&root).unwrap().display();
        let source = fs::read_to_string(file).unwrap_or_else(|error| panic!("{label}: {error}"));
        match catch_unwind(|| lex(&source)) {
            Ok(Ok(_)) => {
                if let Err(error) = lossless(&source) {
                    failures.push(format!("{label} lossless lexer: {error}"));
                }
                lossless_files += 1;
            }
            Ok(Err(_)) => {}
            Err(_) => {
                failures.push(format!("{label} lexer panicked"));
                continue;
            }
        }
        // The SQL runner owns shell sections, placeholders and error controls.
        // Here raw files exercise framing and lossless lexing of accepted SQL.
        let frames = match catch_unwind(|| frame_statements(&source)) {
            Ok(Ok(frames)) => frames,
            Ok(Err(_)) => continue,
            Err(_) => {
                failures.push(format!("{label} framing panicked"));
                continue;
            }
        };
        framed_files += 1;
        for frame in frames {
            let source = &source[frame.source.start()..frame.source.end()];
            let context = format!("{label} at byte {}", frame.source.start());
            match catch_unwind(|| parse_statement(source)) {
                Ok(Ok(_)) => {
                    if let Err(error) = check_round_trip(source) {
                        failures.push(format!("{context} round trip: {error}"));
                    }
                    round_trips += 1;
                }
                Ok(Err(_)) => {}
                Err(_) => failures.push(format!("{context} parser panicked")),
            }
            let Ok(tokens) = lossless(source) else {
                continue;
            };
            for (kind, mutated) in mutations(source, &tokens, &mut seed) {
                edited += 1;
                if catch_unwind(|| lex(&mutated)).is_err() {
                    failures.push(format!("{context} {kind}: lexer panicked"));
                }
                if catch_unwind(|| parse_statement(&mutated)).is_err() {
                    failures.push(format!("{context} {kind}: parser panicked"));
                }
            }
        }
    }
    eprintln!(
        "files={} lossless={lossless_files} framed={framed_files} round_trips={round_trips} mutations={edited}",
        files.len()
    );
    assert!(lossless_files > 0 && framed_files > 0 && round_trips > 0 && edited > 0);
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}
