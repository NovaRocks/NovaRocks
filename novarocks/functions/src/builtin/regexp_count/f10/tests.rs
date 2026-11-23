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

use super::PatternPrograms;
use super::program::Program;
use super::rows::RegexpValueFailure;
use super::search::RegexpSearch;
use crate::builtin::array_invocation_test_support::Host;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Default)]
struct Control {
    observations: AtomicUsize,
    stop: Option<usize>,
}

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= MAX_UNOBSERVED_KERNEL_WORK);
        let count = self.observations.fetch_add(1, Ordering::Relaxed) + 1;
        if self.stop == Some(count) {
            Err(KernelFailure::Cancelled)
        } else {
            Ok(())
        }
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("regexp search must not wait");
    }
}

fn with_search(
    pattern: &str,
    f: impl FnOnce(&mut RegexpSearch<'_, '_>, &mut EvaluationCheckpoints<'_>),
) {
    let host = Host::with_limit(1 << 20);
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let mut programs = PatternPrograms::try_new(host.clone()).unwrap();
    let program = programs.get(pattern, &mut work).unwrap().unwrap();
    let mut search = RegexpSearch::try_new(program, host.clone(), &mut work).unwrap();
    f(&mut search, &mut work);
    drop(search);
    drop(programs);
    work.finish().unwrap();
    host.released();
}

#[test]
fn regexp_f10_search_uses_original_source_and_match_priority() {
    with_search("^b", |search, work| {
        assert_eq!(search.position("ab", 2, 1, work).unwrap(), -1);
        assert_eq!(search.position("b", 1, 1, work).unwrap(), 1);
    });
    with_search(r"\z", |search, work| {
        assert_eq!(search.position("é中", 2, 1, work).unwrap(), 3);
        assert_eq!(search.position("é中", 3, 1, work).unwrap(), -1);
    });
    with_search("中", |search, work| {
        assert_eq!(search.position("é中a中", 2, 2, work).unwrap(), 4);
        assert_eq!(search.position("é中a中", 5, 1, work).unwrap(), -1);
        assert!(matches!(
            search.position("é中a中", 1, 0, work),
            Err(RegexpValueFailure::Data(_))
        ));
    });
    for (pattern, expected) in [("(a+|a)", "aaa"), ("(a|a+)", "a"), ("(a+?)", "a")] {
        with_search(pattern, |search, work| {
            assert_eq!(search.extract("baaa", 1, work).unwrap(), Some(expected));
        });
    }
    with_search("(?:a?b)*", |search, work| {
        assert_eq!(search.extract("abbX", 0, work).unwrap(), Some("abb"));
    });
}

#[test]
fn regexp_f10_search_preserves_empty_and_optional_captures() {
    with_search("", |search, work| {
        assert_eq!(search.count("é中", work).unwrap(), 3);
        assert_eq!(search.count("", work).unwrap(), 1);
        assert_eq!(search.position("", 1, 1, work).unwrap(), -1);
    });
    with_search("(x)?a", |search, work| {
        let mut captures = Vec::new();
        search
            .extract_all("abxa", 1, work, |capture, _| {
                captures.push(capture);
                Ok(())
            })
            .unwrap();
        assert_eq!(captures, [None, Some("x")]);
        assert_eq!(search.extract("b", 1, work).unwrap(), None);
        assert!(matches!(
            search.extract("b", 2, work),
            Err(RegexpValueFailure::Data(_))
        ));
    });
    for (pattern, expected) in [("(a?)?", Some("")), ("(a?)??", None)] {
        with_search(pattern, |search, work| {
            assert_eq!(search.extract("", 1, work).unwrap(), expected);
        });
    }
    with_search("(a(b)?)+", |search, work| {
        assert_eq!(search.extract("aba", 2, work).unwrap(), Some("b"));
    });
    with_search("((a)b)|a", |search, work| {
        assert_eq!(search.extract("a", 2, work).unwrap(), None);
    });
}

#[test]
fn regexp_f10_replacement_checks_references_only_on_a_match() {
    with_search(".*", |search, work| {
        let mut result = Vec::<u8>::new();
        search
            .replace("abcd", "xx", work, |bytes, _| {
                result.extend(bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(result, b"xxxx");
    });
    with_search("(?<letter>a)(b)?", |search, work| {
        let mut result = Vec::<u8>::new();
        search
            .replace("ac", r"${letter}  \\\$ $10 $2", work, |bytes, _| {
                result.extend(bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(result, b"a  \\$ a0 c");
        result.clear();
        search
            .replace("z", "$3", work, |bytes, _| {
                result.extend(bytes);
                Ok(())
            })
            .unwrap();
        assert_eq!(result, b"z");
        let error = search.replace("a", "$3", work, |_, _| Ok(())).unwrap_err();
        assert!(matches!(
            error,
            RegexpValueFailure::Data("unknown group in regexp replacement")
        ));
    });
}

#[test]
fn regexp_f10_controls_observe_inside_a_single_search() {
    let host = Host::with_limit(1 << 20);
    let control = Control::default();
    let mut prepare_work = EvaluationCheckpoints::new(&control);
    let program = Program::compile("a+z", host.clone(), &mut prepare_work)
        .unwrap()
        .unwrap();
    let mut search = RegexpSearch::try_new(&program, host.clone(), &mut prepare_work).unwrap();
    prepare_work.finish().unwrap();

    let source = "a".repeat(10_000);
    let stop = Control {
        stop: Some(2),
        ..Control::default()
    };
    let mut work = EvaluationCheckpoints::new(&stop);
    assert!(matches!(
        search.extract(&source, 0, &mut work),
        Err(RegexpValueFailure::Kernel(KernelFailure::Cancelled))
    ));
    assert_eq!(stop.observations.load(Ordering::Relaxed), 2);
    drop(search);
    drop(program);
    host.released();
}

#[test]
fn regexp_f10_search_scratch_refusal_releases_previously_admitted_backing() {
    let host = Host::with_limit(1 << 20);
    let control = Control::default();
    let mut work = EvaluationCheckpoints::new(&control);
    let program = Program::compile("(a+)z", host.clone(), &mut work)
        .unwrap()
        .unwrap();
    host.arm_allocation(3, KernelFailure::DeadlineExceeded);
    assert!(matches!(
        RegexpSearch::try_new(&program, host.clone(), &mut work),
        Err(KernelFailure::DeadlineExceeded)
    ));
    drop(program);
    host.released();
}
