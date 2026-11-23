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

use super::{ClassTerm, Node, PatternRejection, parse};
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::builtin::array_invocation_test_support::Host;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{KernelEvaluationControl, KernelFailure, MAX_UNOBSERVED_KERNEL_WORK};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

#[derive(Default)]
struct Control {
    quanta: AtomicUsize,
    stop_at: Option<usize>,
}

impl KernelEvaluationControl for Control {
    fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
        assert!(work <= MAX_UNOBSERVED_KERNEL_WORK);
        if work == MAX_UNOBSERVED_KERNEL_WORK {
            let quanta = self.quanta.fetch_add(1, Ordering::Relaxed) + 1;
            if self.stop_at == Some(quanta) {
                return Err(KernelFailure::Cancelled);
            }
        }
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("regexp pattern parsing must not wait");
    }
}

#[test]
fn regexp_f10_parser_emits_ordered_postfix_with_exact_capture_and_class_facts() {
    use Node::*;
    let host = Host::with_limit(1 << 20);
    {
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        let parsed = parse(r"^(?<g>ab|[a-z]\D)?(?s:.)\z", &allocator, &mut work)
            .unwrap()
            .unwrap();
        assert_eq!(parsed.names.as_slice(), &[None, Some("g")]);
        assert_eq!(
            parsed.terms.as_slice(),
            &[
                ClassTerm::Range {
                    first: 'a',
                    last: 'z'
                },
                ClassTerm::Digit { negated: true },
            ]
        );
        assert_eq!(
            parsed.nodes.as_slice(),
            &[
                Start,
                Literal('a'),
                Literal('b'),
                Concat,
                Class {
                    start: 0,
                    end: 1,
                    negated: false
                },
                Class {
                    start: 1,
                    end: 2,
                    negated: false
                },
                Concat,
                Alternate,
                Capture(1),
                Optional { greedy: true },
                Concat,
                Dot { matches_lf: true },
                Concat,
                End,
                Concat,
            ]
        );
        let scopes = parse("(?:(?s).).", &allocator, &mut work).unwrap().unwrap();
        assert_eq!(
            scopes.nodes.as_slice(),
            &[Dot { matches_lf: true }, Dot { matches_lf: false }, Concat,]
        );
        let literals = parse("a{,}", &allocator, &mut work).unwrap().unwrap();
        assert_eq!(
            literals.nodes.as_slice(),
            &[
                Literal('a'),
                Literal('{'),
                Concat,
                Literal(','),
                Concat,
                Literal('}'),
                Concat,
            ]
        );
        work.finish().unwrap();
    }
    host.released();
}

#[test]
fn regexp_f10_parser_proves_repeated_body_progress() {
    let host = Host::with_limit(1 << 20);
    {
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        for pattern in ["(a?)??", "(?:a?b)*", "(?:^a|b)+?", "[^)]*"] {
            assert!(
                parse(pattern, &allocator, &mut work).unwrap().is_ok(),
                "{pattern:?}"
            );
        }
        for pattern in ["(a?)*", "(?:a?|b)*", "(?:^)+?", "(?:a*|b)+", "(?:a|)+"] {
            assert!(
                matches!(
                    parse(pattern, &allocator, &mut work).unwrap(),
                    Err(PatternRejection::Capability(
                        "repeated regexp subexpression may match empty input"
                    ))
                ),
                "{pattern:?}"
            );
        }
        work.finish().unwrap();
    }
    host.released();
}

#[test]
fn regexp_f10_parser_preserves_known_syntax_errors_after_deferred_constructs() {
    let host = Host::with_limit(1 << 20);
    {
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        for pattern in [
            "a(?=b)(",
            "a(?=b)[",
            "(?=a)[z-a]",
            "(?m)a{2,1}",
            "a{100001}",
            "(?>a)[",
            r"(a)\2",
            r"[a-\d]",
            "(?<x>a)(?<x>b)",
            "(?<1>x)",
            "(?<@a>x)",
            "(?< >x)",
            r"\k<-1>(a)",
            "(?x:a # (\n)(",
            "(?=a)\\",
        ] {
            assert!(
                matches!(
                    parse(pattern, &allocator, &mut work).unwrap(),
                    Err(PatternRejection::Data(_))
                ),
                "{pattern:?}"
            );
        }
        for pattern in [
            "a(?=b)",
            "a{2,4}",
            r"(a)\1",
            "a++",
            "a**",
            "(?m)^",
            "$",
            r"\b",
            r"\Q[(\E",
            r"[\p{Han}]",
            r"\p{foo}",
            "[[:lower:]]",
            "[a&&[b]]",
            "(?<foo-bar>a)",
            "(?<a(b)>x)",
            r"(a)\k<-1>",
        ] {
            assert!(
                matches!(
                    parse(pattern, &allocator, &mut work).unwrap(),
                    Err(PatternRejection::Capability(_))
                ),
                "{pattern:?}"
            );
        }
        for pattern in [r"a\(\?=b\)", r"[\d-a]", "[]a]", "(?<_g>a)", "(?'x'a)", ""] {
            assert!(
                parse(pattern, &allocator, &mut work).unwrap().is_ok(),
                "{pattern:?}"
            );
        }
        work.finish().unwrap();
    }
    host.released();
}

#[test]
fn regexp_f10_parser_stops_during_long_pattern_and_releases_original_capacity() {
    let host = Host::with_limit(4 << 20);
    {
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        let control = Control {
            stop_at: Some(2),
            ..Control::default()
        };
        let mut work = EvaluationCheckpoints::new(&control);
        let pattern = "中文".repeat(10_000);
        assert!(matches!(
            parse(&pattern, &allocator, &mut work),
            Err(KernelFailure::Cancelled)
        ));
        assert_eq!(control.quanta.load(Ordering::Relaxed), 2);
    }
    host.released();
}

#[test]
fn regexp_f10_parser_propagates_original_allocation_refusal() {
    let host = Host::with_limit(1 << 20);
    {
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        let first_request = host.attempts();
        *host.refusal.lock().unwrap() = Some((first_request + 2, KernelFailure::ResourceExhausted));
        let control = Control::default();
        let mut work = EvaluationCheckpoints::new(&control);
        assert!(matches!(
            parse(
                "a small pattern with a larger compiled buffer",
                &allocator,
                &mut work
            ),
            Err(KernelFailure::ResourceExhausted)
        ));
    }
    host.released();
}
