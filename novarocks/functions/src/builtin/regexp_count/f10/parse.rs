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

//! A single observed parse produces either postfix instructions or the
//! classification of a pattern outside the installed grammar.

use super::push_observed;
use crate::KernelFailure;
use crate::aggregate_host_allocator::HostAggregateAllocator;
use crate::kernel_input::EvaluationCheckpoints;
use allocator_api2::vec::Vec as HostVec;

#[cfg(test)]
mod tests;
mod tokens;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Node {
    Empty,
    Literal(char),
    Dot {
        matches_lf: bool,
    },
    Class {
        start: usize,
        end: usize,
        negated: bool,
    },
    Start,
    End,
    Concat,
    Alternate,
    Capture(usize),
    Optional {
        greedy: bool,
    },
    ZeroOrMore {
        greedy: bool,
    },
    OneOrMore {
        greedy: bool,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ClassTerm {
    Range { first: char, last: char },
    Digit { negated: bool },
    Word { negated: bool },
    Space { negated: bool },
}

pub(super) struct Parsed<'p> {
    pub(super) nodes: HostVec<Node, HostAggregateAllocator>,
    pub(super) terms: HostVec<ClassTerm, HostAggregateAllocator>,
    pub(super) names: HostVec<Option<&'p str>, HostAggregateAllocator>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum PatternRejection {
    Data(&'static str),
    Capability(&'static str),
}

pub(super) fn parse<'p>(
    pattern: &'p str,
    allocator: &HostAggregateAllocator,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Result<Parsed<'p>, PatternRejection>, KernelFailure> {
    work.flush()?;
    let mut names = HostVec::new_in(allocator.clone());
    push_observed(&mut names, None, work)?;
    let parser = Parser {
        input: Cursor { remaining: pattern },
        parsed: Parsed {
            nodes: HostVec::new_in(allocator.clone()),
            terms: HostVec::new_in(allocator.clone()),
            names,
        },
        groups: HostVec::new_in(allocator.clone()),
        classes: HostVec::new_in(allocator.clone()),
        frame: Frame::default(),
        largest_numeric_reference: 0,
        capability: None,
        work,
    };
    let result = match parser.run() {
        Ok(parsed) => parsed,
        Err(ParseFailure::Data(message)) => Err(PatternRejection::Data(message)),
        Err(ParseFailure::Kernel(cause)) => return Err(cause),
    };
    work.flush()?;
    Ok(result)
}

enum ParseFailure {
    Data(&'static str),
    Kernel(KernelFailure),
}

impl From<KernelFailure> for ParseFailure {
    fn from(cause: KernelFailure) -> Self {
        Self::Kernel(cause)
    }
}

type ParseResult<T> = Result<T, ParseFailure>;

#[derive(Clone, Copy)]
struct Cursor<'p> {
    remaining: &'p str,
}

impl<'p> Cursor<'p> {
    fn peek(self) -> Option<char> {
        self.remaining.chars().next()
    }

    fn next(
        &mut self,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<Option<char>, KernelFailure> {
        let Some(ch) = self.peek() else {
            return Ok(None);
        };
        work.step()?;
        self.remaining = &self.remaining[ch.len_utf8()..];
        Ok(Some(ch))
    }

    fn eat(
        &mut self,
        ch: char,
        work: &mut EvaluationCheckpoints<'_>,
    ) -> Result<bool, KernelFailure> {
        if self.peek() != Some(ch) {
            return Ok(false);
        }
        self.next(work)?;
        Ok(true)
    }

    fn before(self, end: Self) -> &'p str {
        &self.remaining[..self.remaining.len() - end.remaining.len()]
    }
}

#[derive(Clone, Copy, Default)]
struct Mode {
    dotall: bool,
    extended: bool,
}

#[derive(Clone, Copy)]
struct Atom {
    consumes: bool,
    quantified: bool,
}

#[derive(Clone, Copy, Default)]
struct Frame {
    group: Option<usize>,
    mode: Mode,
    atom: Option<Atom>,
    concat: Option<bool>,
    alternatives: Option<bool>,
}

struct Parser<'p, 'w, 'c> {
    input: Cursor<'p>,
    parsed: Parsed<'p>,
    groups: HostVec<Frame, HostAggregateAllocator>,
    classes: HostVec<tokens::Class, HostAggregateAllocator>,
    frame: Frame,
    largest_numeric_reference: usize,
    capability: Option<&'static str>,
    work: &'w mut EvaluationCheckpoints<'c>,
}

impl<'p> Parser<'p, '_, '_> {
    fn run(mut self) -> ParseResult<Result<Parsed<'p>, PatternRejection>> {
        while let Some(ch) = self.next()? {
            match ch {
                '#' if self.frame.mode.extended => self.line_comment()?,
                ' ' | '\t' | '\n' | '\r' | '\u{c}' if self.frame.mode.extended => (),
                '(' => self.group()?,
                ')' => self.close_group()?,
                '|' => self.finish_branch()?,
                '*' | '+' | '?' => self.repeat(ch)?,
                '{' if self.counted_repeat()? => self.repeat('{')?,
                '.' => self.leaf(
                    Node::Dot {
                        matches_lf: self.frame.mode.dotall,
                    },
                    true,
                )?,
                '^' => self.leaf(Node::Start, false)?,
                '$' => {
                    self.defer("regexp dollar end anchor is not supported");
                    self.leaf(Node::End, false)?;
                }
                '[' => {
                    let (start, end, negated) = self.character_class()?;
                    self.leaf(
                        Node::Class {
                            start,
                            end,
                            negated,
                        },
                        true,
                    )?;
                }
                '\\' => self.escaped_atom()?,
                _ => self.leaf(Node::Literal(ch), true)?,
            }
        }
        if !self.groups.is_empty() {
            return Err(ParseFailure::Data("unterminated regexp group"));
        }
        if self.largest_numeric_reference >= self.parsed.names.len() {
            return Err(ParseFailure::Data("invalid regexp backreference"));
        }
        self.finish_branch()?;
        Ok(match self.capability {
            Some(message) => Err(PatternRejection::Capability(message)),
            None => Ok(self.parsed),
        })
    }

    fn next(&mut self) -> ParseResult<Option<char>> {
        Ok(self.input.next(self.work)?)
    }

    fn eat(&mut self, ch: char) -> ParseResult<bool> {
        Ok(self.input.eat(ch, self.work)?)
    }

    fn defer(&mut self, message: &'static str) {
        self.capability.get_or_insert(message);
    }

    fn emit(&mut self, node: Node) -> ParseResult<()> {
        self.work.step()?;
        if self.capability.is_none() {
            push_observed(&mut self.parsed.nodes, node, self.work)?;
        }
        Ok(())
    }

    fn leaf(&mut self, node: Node, consumes: bool) -> ParseResult<()> {
        self.finish_atom()?;
        self.emit(node)?;
        self.frame.atom = Some(Atom {
            consumes,
            quantified: false,
        });
        Ok(())
    }

    fn finish_atom(&mut self) -> ParseResult<()> {
        if let Some(atom) = self.frame.atom.take() {
            let consumes = match self.frame.concat {
                Some(first) => {
                    self.emit(Node::Concat)?;
                    first || atom.consumes
                }
                None => atom.consumes,
            };
            self.frame.concat = Some(consumes);
        }
        Ok(())
    }

    fn finish_branch(&mut self) -> ParseResult<()> {
        self.finish_atom()?;
        let consumes = match self.frame.concat.take() {
            Some(consumes) => consumes,
            None => {
                self.emit(Node::Empty)?;
                false
            }
        };
        self.frame.alternatives = Some(match self.frame.alternatives {
            Some(first) => {
                self.emit(Node::Alternate)?;
                first && consumes
            }
            None => consumes,
        });
        Ok(())
    }

    fn close_group(&mut self) -> ParseResult<()> {
        let parent = self
            .groups
            .pop()
            .ok_or(ParseFailure::Data("unmatched regexp close parenthesis"))?;
        self.finish_branch()?;
        if let Some(group) = self.frame.group {
            self.emit(Node::Capture(group))?;
        }
        let consumes = self.frame.alternatives.expect("completed group");
        self.frame = parent;
        self.frame.atom = Some(Atom {
            consumes,
            quantified: false,
        });
        Ok(())
    }

    fn group(&mut self) -> ParseResult<()> {
        let mut mode = self.frame.mode;
        let group = if !self.eat('?')? {
            Some(self.capture_group(None)?)
        } else {
            match self.next()? {
                Some('#') => return self.group_comment(),
                Some(':') => None,
                Some('=' | '!') => {
                    self.defer("regexp lookaround is not supported");
                    None
                }
                Some('>') => {
                    self.defer("atomic regexp groups are not supported");
                    None
                }
                Some('<') if matches!(self.input.peek(), Some('=' | '!')) => {
                    self.next()?;
                    self.defer("regexp lookaround is not supported");
                    None
                }
                Some(delimiter @ ('<' | '\'')) => {
                    let end = if delimiter == '<' { '>' } else { '\'' };
                    let name = self.group_name(end)?;
                    Some(self.capture_group(Some(name))?)
                }
                Some(flag @ ('i' | 'm' | 's' | 'x' | '-')) => {
                    let (options, global) = self.options(flag)?;
                    if global {
                        self.finish_atom()?;
                        self.frame.mode = options;
                        return Ok(());
                    }
                    mode = options;
                    None
                }
                Some(_) => return Err(ParseFailure::Data("undefined regexp group option")),
                None => return Err(ParseFailure::Data("unterminated regexp group")),
            }
        };
        self.finish_atom()?;
        push_observed(&mut self.groups, self.frame, self.work)?;
        self.frame = Frame {
            group,
            mode,
            ..Frame::default()
        };
        Ok(())
    }

    fn group_comment(&mut self) -> ParseResult<()> {
        self.defer("regexp group comments are not supported");
        while let Some(ch) = self.next()? {
            match ch {
                ')' => return Ok(()),
                '\\' => {
                    self.next()?;
                }
                _ => (),
            }
        }
        Err(ParseFailure::Data("unterminated regexp group comment"))
    }

    fn line_comment(&mut self) -> ParseResult<()> {
        while let Some(ch) = self.next()? {
            if ch == '\n' {
                break;
            }
        }
        Ok(())
    }

    fn options(&mut self, mut flag: char) -> ParseResult<(Mode, bool)> {
        let mut mode = self.frame.mode;
        let mut enabled = true;
        let mut first = true;
        loop {
            match flag {
                's' if enabled && first => mode.dotall = true,
                's' => {
                    mode.dotall = enabled;
                    self.defer("combined regexp options are not supported");
                }
                'm' => self.defer("multiline regexp mode is not supported"),
                'i' => self.defer("regexp case folding is not supported"),
                'x' => {
                    mode.extended = enabled;
                    self.defer("extended regexp whitespace mode is not supported");
                }
                '-' => {
                    enabled = false;
                    self.defer("regexp option negation is not supported");
                }
                ':' => return Ok((mode, false)),
                ')' => return Ok((mode, true)),
                _ => return Err(ParseFailure::Data("undefined regexp group option")),
            }
            first = false;
            flag = self
                .next()?
                .ok_or(ParseFailure::Data("unterminated regexp group options"))?;
        }
    }

    fn capture_group(&mut self, name: Option<&'p str>) -> ParseResult<usize> {
        if let Some(name) = name {
            let Some(first) = name.chars().next() else {
                return Err(ParseFailure::Data("empty regexp capture name"));
            };
            if first.is_ascii() && !first.is_ascii_alphabetic() && first != '_' {
                return Err(ParseFailure::Data("invalid regexp capture name"));
            }
            if !first.is_ascii() {
                self.defer("regexp capture names must use the installed ASCII identifier syntax");
            }
            for ch in name.chars().skip(1) {
                self.work.step()?;
                if !ch.is_ascii_alphanumeric() && ch != '_' {
                    self.defer(
                        "regexp capture names must use the installed ASCII identifier syntax",
                    );
                }
            }
            if self.has_capture(name)? {
                return Err(ParseFailure::Data("duplicate regexp capture name"));
            }
        }
        let ordinal = self.parsed.names.len();
        push_observed(&mut self.parsed.names, name, self.work)?;
        Ok(ordinal)
    }

    fn group_name(&mut self, delimiter: char) -> ParseResult<&'p str> {
        let start = self.input;
        loop {
            let end = self.input;
            match self.next()? {
                Some(ch) if ch == delimiter && start.remaining.len() > end.remaining.len() => {
                    return Ok(start.before(end));
                }
                Some(ch) if ch != delimiter => (),
                _ => {
                    return Err(ParseFailure::Data(
                        "invalid or unterminated regexp group name",
                    ));
                }
            }
        }
    }

    fn has_capture(&mut self, name: &str) -> ParseResult<bool> {
        for candidate in &self.parsed.names {
            self.work.step()?;
            let Some(candidate) = candidate else { continue };
            if candidate.len() != name.len() {
                continue;
            }
            let mut equal = true;
            for (left, right) in candidate.bytes().zip(name.bytes()) {
                self.work.step()?;
                if left != right {
                    equal = false;
                    break;
                }
            }
            if equal {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn repeat(&mut self, operator: char) -> ParseResult<()> {
        let atom = self
            .frame
            .atom
            .ok_or(ParseFailure::Data("regexp quantifier has no target"))?;
        if atom.quantified {
            self.defer("stacked regexp quantifiers are not supported");
        }
        let greedy = !self.eat('?')?;
        if greedy && self.eat('+')? {
            self.defer("possessive regexp quantifiers are not supported");
        }
        if matches!(operator, '*' | '+') && !atom.consumes {
            self.defer("repeated regexp subexpression may match empty input");
        }
        let node = match operator {
            '?' => Node::Optional { greedy },
            '*' => Node::ZeroOrMore { greedy },
            '+' => Node::OneOrMore { greedy },
            '{' => {
                self.defer("counted regexp quantifiers are not supported");
                Node::Empty // No program is emitted for a deferred interval.
            }
            _ => unreachable!("only regexp quantifier tokens reach repeat"),
        };
        self.emit(node)?;
        self.frame.atom = Some(Atom {
            consumes: operator == '+' && atom.consumes,
            quantified: true,
        });
        Ok(())
    }
}
