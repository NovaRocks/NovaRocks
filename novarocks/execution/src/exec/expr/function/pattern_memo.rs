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
//! Per-evaluation memoization of compiled pattern arguments.

use std::collections::HashMap;

/// How many distinct patterns one evaluation keeps compiled.
///
/// A regex that has searched holds roughly 10-120 KiB of program and search
/// cache, so retaining every distinct pattern of a high-cardinality column
/// could hold hundreds of MiB for a single 4096-row chunk.
const MAX_RETAINED_PATTERNS: usize = 64;

/// Memoizes the compiled form of a pattern argument, such as a regex or a
/// JSON path, while one function evaluation walks the rows of a chunk.
///
/// The memo is a local of that evaluation: it borrows the pattern text from
/// the evaluated argument array and is dropped with it, so compiled state
/// never outlives the evaluation, is never shared between evaluations, and
/// never lives in the immutable expression arena.
///
/// Compilation stays lazy. A pattern is compiled on the first row that needs
/// it, so an invalid pattern fails on the same row, with the same error, as
/// compiling on every row would, and a pattern no row needs is never
/// compiled. A failed compilation is memoized like a successful one.
///
/// Every row of a constant argument repeats the previous lookup, which costs
/// one string comparison. The first `MAX_RETAINED_PATTERNS` distinct patterns
/// compile at most once per evaluation; a pattern beyond them is kept only
/// until a different pattern is looked up and compiles again if it recurs
/// later, which is never worse than compiling per row.
pub(super) struct PatternMemo<'a, T, E> {
    /// Retained entries, then at most one entry past the retention bound.
    entries: Vec<(&'a str, Result<T, E>)>,
    /// Index into `entries` of each retained pattern.
    retained: HashMap<&'a str, usize>,
    /// Index into `entries` of the previous lookup.
    last: Option<usize>,
}

impl<'a, T, E> PatternMemo<'a, T, E> {
    pub(super) fn new() -> Self {
        Self {
            entries: Vec::new(),
            retained: HashMap::new(),
            last: None,
        }
    }

    /// Returns the compiled `pattern`, calling `compile` only when this
    /// evaluation has not compiled the same text yet.
    pub(super) fn get_or_compile<F>(&mut self, pattern: &'a str, compile: F) -> Result<&T, &E>
    where
        F: FnOnce(&str) -> Result<T, E>,
    {
        let index = match self.last {
            Some(index) if self.entries[index].0 == pattern => index,
            _ => {
                let index = match self.retained.get(pattern) {
                    Some(&index) => index,
                    None => self.insert(pattern, compile(pattern)),
                };
                self.last = Some(index);
                index
            }
        };
        self.entries[index].1.as_ref()
    }

    fn insert(&mut self, pattern: &'a str, compiled: Result<T, E>) -> usize {
        let index = self.entries.len().min(MAX_RETAINED_PATTERNS);
        if index < MAX_RETAINED_PATTERNS {
            self.retained.insert(pattern, index);
        }
        if index == self.entries.len() {
            self.entries.push((pattern, compiled));
        } else {
            // Past the retention bound: replace the previous unretained entry.
            self.entries[index] = (pattern, compiled);
        }
        index
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_RETAINED_PATTERNS, PatternMemo};

    /// Looks `pattern` up and records every compilation the memo asks for.
    /// A pattern starting with `!` fails to compile.
    fn lookup<'a>(
        memo: &mut PatternMemo<'a, usize, String>,
        compiled: &mut Vec<String>,
        pattern: &'a str,
    ) -> Result<usize, String> {
        memo.get_or_compile(pattern, |text| {
            compiled.push(text.to_string());
            if text.starts_with('!') {
                Err(format!("cannot compile {text}"))
            } else {
                Ok(text.len())
            }
        })
        .copied()
        .map_err(Clone::clone)
    }

    #[test]
    fn compiles_each_distinct_pattern_once_in_first_use_order() {
        let mut memo = PatternMemo::new();
        let mut compiled = Vec::new();
        let results = ["ab", "c", "ab", "def", "c", "ab"]
            .into_iter()
            .map(|pattern| lookup(&mut memo, &mut compiled, pattern))
            .collect::<Vec<_>>();

        assert_eq!(results, vec![Ok(2), Ok(1), Ok(2), Ok(3), Ok(1), Ok(2)]);
        assert_eq!(compiled, vec!["ab", "c", "def"]);
    }

    #[test]
    fn equal_text_in_distinct_buffers_compiles_once() {
        // A literal argument materializes its own copy of the text per row.
        let rows = vec!["abc.*".to_string(); 4096];
        let mut memo = PatternMemo::new();
        let mut compiled = Vec::new();
        for row in &rows {
            assert_eq!(lookup(&mut memo, &mut compiled, row), Ok(5));
        }

        assert_eq!(compiled, vec!["abc.*"]);
    }

    #[test]
    fn compile_errors_are_memoized() {
        let mut memo = PatternMemo::new();
        let mut compiled = Vec::new();
        let results = ["!x", "ok", "!x"]
            .into_iter()
            .map(|pattern| lookup(&mut memo, &mut compiled, pattern))
            .collect::<Vec<_>>();

        let error = Err("cannot compile !x".to_string());
        assert_eq!(results, vec![error.clone(), Ok(2), error]);
        assert_eq!(compiled, vec!["!x", "ok"]);
    }

    #[test]
    fn patterns_past_the_retention_bound_compile_again_when_they_recur() {
        let patterns = (0..MAX_RETAINED_PATTERNS + 3)
            .map(|i| format!("p{i}"))
            .collect::<Vec<_>>();
        let mut memo = PatternMemo::new();
        let mut compiled = Vec::new();
        for _ in 0..2 {
            for pattern in &patterns {
                assert_eq!(lookup(&mut memo, &mut compiled, pattern), Ok(pattern.len()));
            }
        }

        let past_bound = &patterns[MAX_RETAINED_PATTERNS..];
        let expected = patterns.iter().chain(past_bound).collect::<Vec<_>>();
        assert_eq!(compiled.iter().collect::<Vec<_>>(), expected);
    }

    #[test]
    fn a_run_of_one_pattern_past_the_retention_bound_compiles_once() {
        let patterns = (0..MAX_RETAINED_PATTERNS)
            .map(|i| format!("p{i}"))
            .collect::<Vec<_>>();
        let mut memo = PatternMemo::new();
        let mut compiled = Vec::new();
        for pattern in &patterns {
            lookup(&mut memo, &mut compiled, pattern).unwrap();
        }
        compiled.clear();

        for _ in 0..4 {
            assert_eq!(lookup(&mut memo, &mut compiled, "past-bound"), Ok(10));
        }
        assert_eq!(lookup(&mut memo, &mut compiled, "p0"), Ok(2));
        assert_eq!(lookup(&mut memo, &mut compiled, "past-bound-2"), Ok(12));
        assert_eq!(lookup(&mut memo, &mut compiled, "p1"), Ok(2));
        assert_eq!(compiled, vec!["past-bound", "past-bound-2"]);
    }
}
