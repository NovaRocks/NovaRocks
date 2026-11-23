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

//! Thompson graph construction with observed, host-owned storage and patching.

use super::parse::{ClassTerm, Node, Parsed, PatternRejection, parse};
use super::push_observed;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_control::internal;
use crate::kernel_input::EvaluationCheckpoints;
use crate::{AggregateStateAllocator, KernelFailure};
use allocator_api2::vec::Vec as HostVec;
use std::sync::Arc;

#[derive(Clone, Copy)]
pub(super) enum State {
    Match,
    Jump(usize),
    Split { first: usize, second: usize },
    Save { slot: usize, next: usize },
    Start(usize),
    End(usize),
    Consume { matcher: Matcher, next: usize },
}

#[derive(Clone, Copy)]
pub(super) enum Matcher {
    Literal(char),
    Dot {
        matches_lf: bool,
    },
    Class {
        start: usize,
        end: usize,
        negated: bool,
    },
}

impl Matcher {
    pub(super) fn matches(
        self,
        program: &Program<'_>,
        ch: char,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<bool, KernelFailure> {
        match self {
            Self::Literal(expected) => Ok(ch == expected),
            Self::Dot { matches_lf } => Ok(matches_lf || ch != '\n'),
            Self::Class {
                start,
                end,
                negated,
            } => {
                for term in &program.terms[start..end] {
                    work.step()?;
                    let matched = match *term {
                        ClassTerm::Range { first, last } => first <= ch && ch <= last,
                        ClassTerm::Digit { negated } => ch.is_ascii_digit() != negated,
                        ClassTerm::Word { negated } => {
                            (ch.is_ascii_alphanumeric() || ch == '_') != negated
                        }
                        ClassTerm::Space { negated } => {
                            matches!(ch, ' ' | '\t' | '\n' | '\r' | '\u{b}' | '\u{c}') != negated
                        }
                    };
                    if matched {
                        return Ok(!negated);
                    }
                }
                Ok(negated)
            }
        }
    }
}

pub(super) struct Program<'p> {
    pub(super) states: HostVec<State, HostAggregateAllocator>,
    pub(super) start: usize,
    pub(super) names: HostVec<Option<&'p str>, HostAggregateAllocator>,
    terms: HostVec<ClassTerm, HostAggregateAllocator>,
}

impl<'p> Program<'p> {
    pub(super) fn compile(
        pattern: &'p str,
        host: Arc<dyn AggregateStateAllocator>,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Result<Self, PatternRejection>, KernelFailure> {
        work.flush()?;
        let allocator = HostAggregateAllocator::try_new_with_journal(host)?;
        let parsed = match parse(pattern, &allocator, work)? {
            Ok(parsed) => parsed,
            Err(rejection) => {
                work.flush()?;
                return Ok(Err(rejection));
            }
        };
        Self::assemble(parsed, allocator, work).map(Ok)
    }

    fn assemble(
        parsed: Parsed<'p>,
        allocator: HostAggregateAllocator,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Self, KernelFailure> {
        let mut builder = Builder {
            states: HostVec::new_in(allocator.clone()),
            fragments: HostVec::new_in(allocator.clone()),
            patches: HostVec::new_in(allocator),
        };
        for &node in &parsed.nodes {
            work.step()?;
            builder.append(node, work)?;
        }
        let body = builder.pop()?;
        if !builder.fragments.is_empty() {
            return Err(internal("regex parser emitted excess fragments"));
        }
        let fragment = builder.capture(body, 0, work)?;
        let terminal = builder.emit(State::Match, work)?;
        builder.connect(fragment.exit, terminal, work)?;
        work.flush()?;
        Ok(Self {
            states: builder.states,
            start: fragment.start,
            names: parsed.names,
            terms: parsed.terms,
        })
    }
}

const PENDING: usize = usize::MAX;

#[derive(Clone, Copy)]
enum Edge {
    Next,
    First,
    Second,
}

#[derive(Clone, Copy)]
struct Patch {
    state: usize,
    edge: Edge,
    following: Option<usize>,
}

#[derive(Clone, Copy)]
struct Exit {
    first: usize,
    last: usize,
}

#[derive(Clone, Copy)]
struct Fragment {
    start: usize,
    exit: Exit,
}

struct Builder {
    states: HostVec<State, HostAggregateAllocator>,
    fragments: HostVec<Fragment, HostAggregateAllocator>,
    patches: HostVec<Patch, HostAggregateAllocator>,
}

impl Builder {
    fn append(
        &mut self,
        node: Node,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let fragment = match node {
            Node::Empty => self.leaf(State::Jump(PENDING), work)?,
            Node::Literal(ch) => self.consume(Matcher::Literal(ch), work)?,
            Node::Dot { matches_lf } => self.consume(Matcher::Dot { matches_lf }, work)?,
            Node::Class {
                start,
                end,
                negated,
            } => self.consume(
                Matcher::Class {
                    start,
                    end,
                    negated,
                },
                work,
            )?,
            Node::Start => self.leaf(State::Start(PENDING), work)?,
            Node::End => self.leaf(State::End(PENDING), work)?,
            Node::Concat => {
                let right = self.pop()?;
                let left = self.pop()?;
                self.connect(left.exit, right.start, work)?;
                Fragment {
                    start: left.start,
                    exit: right.exit,
                }
            }
            Node::Alternate => {
                let right = self.pop()?;
                let left = self.pop()?;
                let start = self.emit(
                    State::Split {
                        first: left.start,
                        second: right.start,
                    },
                    work,
                )?;
                Fragment {
                    start,
                    exit: self.join(left.exit, right.exit, work)?,
                }
            }
            Node::Capture(group) => {
                let body = self.pop()?;
                self.capture(body, group, work)?
            }
            Node::Optional { greedy } => {
                let body = self.pop()?;
                let (start, skip) = self.branch_with_exit(body.start, greedy, work)?;
                Fragment {
                    start,
                    exit: self.join(body.exit, skip, work)?,
                }
            }
            Node::ZeroOrMore { greedy } | Node::OneOrMore { greedy } => {
                let body = self.pop()?;
                let (repeat, exit) = self.branch_with_exit(body.start, greedy, work)?;
                self.connect(body.exit, repeat, work)?;
                let start = if matches!(node, Node::OneOrMore { .. }) {
                    body.start
                } else {
                    repeat
                };
                Fragment { start, exit }
            }
        };
        push_observed(&mut self.fragments, fragment, work)
    }

    fn pop(&mut self) -> Result<Fragment, KernelFailure> {
        self.fragments
            .pop()
            .ok_or_else(|| internal("regex parser omitted a fragment"))
    }

    fn emit(
        &mut self,
        state: State,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<usize, KernelFailure> {
        let id = self.states.len();
        push_observed(&mut self.states, state, work)?;
        Ok(id)
    }

    fn consume(
        &mut self,
        matcher: Matcher,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Fragment, KernelFailure> {
        self.leaf(
            State::Consume {
                matcher,
                next: PENDING,
            },
            work,
        )
    }

    fn leaf(
        &mut self,
        state: State,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Fragment, KernelFailure> {
        let start = self.emit(state, work)?;
        let exit = self.hole(start, Edge::Next, work)?;
        Ok(Fragment { start, exit })
    }

    fn capture(
        &mut self,
        body: Fragment,
        group: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Fragment, KernelFailure> {
        let slot = group
            .checked_mul(2)
            .ok_or(KernelFailure::ResourceExhausted)?;
        let begin = self.emit(
            State::Save {
                slot,
                next: body.start,
            },
            work,
        )?;
        let end = self.leaf(
            State::Save {
                slot: slot + 1,
                next: PENDING,
            },
            work,
        )?;
        self.connect(body.exit, end.start, work)?;
        Ok(Fragment {
            start: begin,
            exit: end.exit,
        })
    }

    fn branch_with_exit(
        &mut self,
        body: usize,
        greedy: bool,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(usize, Exit), KernelFailure> {
        let (first, second, edge) = if greedy {
            (body, PENDING, Edge::Second)
        } else {
            (PENDING, body, Edge::First)
        };
        let split = self.emit(State::Split { first, second }, work)?;
        Ok((split, self.hole(split, edge, work)?))
    }

    fn hole(
        &mut self,
        state: usize,
        edge: Edge,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Exit, KernelFailure> {
        let id = self.patches.len();
        push_observed(
            &mut self.patches,
            Patch {
                state,
                edge,
                following: None,
            },
            work,
        )?;
        Ok(Exit {
            first: id,
            last: id,
        })
    }

    /// Splice lists in constant work. Each open edge is visited exactly once
    /// when its destination becomes known, even for deeply nested choices.
    fn join(
        &mut self,
        left: Exit,
        right: Exit,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Exit, KernelFailure> {
        work.step()?;
        let tail = &mut self.patches[left.last];
        if tail.following.replace(right.first).is_some() {
            return Err(internal("regex fragment exit was already joined"));
        }
        Ok(Exit {
            first: left.first,
            last: right.last,
        })
    }

    fn connect(
        &mut self,
        exit: Exit,
        target: usize,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<(), KernelFailure> {
        let mut id = Some(exit.first);
        while let Some(index) = id {
            work.step()?;
            let patch = self.patches[index];
            let next = match (&mut self.states[patch.state], patch.edge) {
                (State::Jump(next) | State::Start(next) | State::End(next), Edge::Next)
                | (State::Save { next, .. } | State::Consume { next, .. }, Edge::Next)
                | (State::Split { first: next, .. }, Edge::First)
                | (State::Split { second: next, .. }, Edge::Second) => next,
                _ => return Err(internal("regex patch references the wrong edge")),
            };
            if *next != PENDING {
                return Err(internal("regex transition was already wired"));
            }
            *next = target;
            id = patch.following;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builtin::array_invocation_test_support::Host;
    use crate::{KernelEvaluationControl, MAX_UNOBSERVED_KERNEL_WORK};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Default)]
    struct CompileControl {
        full_observations: AtomicUsize,
        stop: Option<usize>,
    }

    impl KernelEvaluationControl for CompileControl {
        fn checkpoint(&self, work: u32) -> Result<(), KernelFailure> {
            assert!(work <= MAX_UNOBSERVED_KERNEL_WORK);
            if work == MAX_UNOBSERVED_KERNEL_WORK {
                let count = self.full_observations.fetch_add(1, Ordering::Relaxed) + 1;
                if self.stop == Some(count) {
                    return Err(KernelFailure::Cancelled);
                }
            }
            Ok(())
        }

        fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
            panic!("regexp compilation must not wait");
        }
    }

    #[test]
    fn regexp_f10_graph_compilation_stops_before_publishing_a_long_literal() {
        let host = Host::with_limit(1 << 24);
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        let pattern = "é".repeat(2000);
        let parse_control = CompileControl::default();
        let mut parse_work = EvaluationCheckpoints::new(&parse_control);
        let parsed = parse(&pattern, &allocator, &mut parse_work)
            .unwrap()
            .unwrap();
        parse_work.finish().unwrap();

        let stop = CompileControl {
            stop: Some(2),
            ..CompileControl::default()
        };
        let mut compile_work = EvaluationCheckpoints::new(&stop);
        assert!(matches!(
            Program::assemble(parsed, allocator, &mut compile_work),
            Err(KernelFailure::Cancelled)
        ));
        assert_eq!(stop.full_observations.load(Ordering::Relaxed), 2);
        host.released();
    }

    #[test]
    fn regexp_f10_graph_compilation_preserves_host_refusal_and_releases_storage() {
        let host = Host::with_limit(1 << 20);
        let allocator = HostAggregateAllocator::try_new_with_journal(host.clone()).unwrap();
        let control = CompileControl::default();
        let mut work = EvaluationCheckpoints::new(&control);
        let parsed = parse("ab+|é", &allocator, &mut work).unwrap().unwrap();
        // Permit the initial state allocation and refuse the first patch list.
        *host.refusal.lock().unwrap() =
            Some((host.attempts() + 1, KernelFailure::ResourceExhausted));
        assert!(matches!(
            Program::assemble(parsed, allocator, &mut work),
            Err(KernelFailure::ResourceExhausted)
        ));
        host.released();
    }
}
