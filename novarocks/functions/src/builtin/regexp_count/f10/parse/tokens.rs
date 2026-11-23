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

//! Character classes and lexemes that carry their own delimiters.

use super::{ClassTerm, Cursor, Node, ParseFailure, ParseResult, Parser, push_observed};
use crate::KernelFailure;
use crate::kernel_input::EvaluationCheckpoints;

#[derive(Clone, Copy)]
enum ClassAtom {
    Scalar(char),
    Term(ClassTerm),
    Set,
    Encoded,
}

#[derive(Clone, Copy)]
pub(super) struct Class {
    first: bool,
    leading_close: bool,
    previous: Option<char>,
    range_start: Option<char>,
}

impl Parser<'_, '_, '_> {
    pub(super) fn character_class(&mut self) -> ParseResult<(usize, usize, bool)> {
        let start = self.parsed.terms.len();
        let negated = self.open_class()?;
        while let Some(ch) = self.next()? {
            let class = self.classes.last_mut().expect("open regexp class");
            let atom = match ch {
                ']' if class.first => {
                    class.leading_close = true;
                    ClassAtom::Scalar(']')
                }
                ']' => {
                    self.classes.pop();
                    if self.classes.is_empty() {
                        return Ok((start, self.parsed.terms.len(), negated));
                    }
                    continue;
                }
                '[' => {
                    class.first = false;
                    self.defer(if self.input.peek() == Some(':') {
                        "POSIX regexp character classes are not supported"
                    } else {
                        "nested regexp character classes are not supported"
                    });
                    self.open_class()?;
                    continue;
                }
                '&' if self.input.eat('&', self.work)? => {
                    class.first = false;
                    class.previous = None;
                    class.range_start = None;
                    self.defer("regexp character-class intersections are not supported");
                    continue;
                }
                '-' if class.range_start.is_none()
                    && self.input.peek() != Some(']')
                    && !self.input.remaining.starts_with("&&") =>
                {
                    if let Some(start) = class.previous.take() {
                        class.range_start = Some(start);
                        continue;
                    }
                    ClassAtom::Scalar('-')
                }
                '\\' => self.class_escape()?,
                _ => ClassAtom::Scalar(ch),
            };
            self.class_atom(atom)?;
        }
        Err(ParseFailure::Data(
            if self.classes.last().is_some_and(|class| class.leading_close) {
                "empty or unterminated regexp character class"
            } else {
                "unterminated regexp character class"
            },
        ))
    }

    fn open_class(&mut self) -> ParseResult<bool> {
        let negated = self.eat('^')?;
        push_observed(
            &mut self.classes,
            Class {
                first: true,
                leading_close: false,
                previous: None,
                range_start: None,
            },
            self.work,
        )?;
        Ok(negated)
    }

    fn class_atom(&mut self, atom: ClassAtom) -> ParseResult<()> {
        let class = self.classes.last_mut().expect("open regexp class");
        class.first = false;
        if let Some(start) = class.range_start.take() {
            class.previous = None;
            match atom {
                ClassAtom::Scalar(end) if start > end => {
                    return Err(ParseFailure::Data("reversed regexp character-class range"));
                }
                ClassAtom::Scalar(end) => {
                    if self.capability.is_none() {
                        *self.parsed.terms.last_mut().expect("emitted range start") =
                            ClassTerm::Range {
                                first: start,
                                last: end,
                            };
                    }
                }
                ClassAtom::Term(_) | ClassAtom::Set => {
                    return Err(ParseFailure::Data(
                        "set at the end of a regexp character-class range",
                    ));
                }
                ClassAtom::Encoded => (),
            }
            return Ok(());
        }

        let term = match atom {
            ClassAtom::Scalar(ch) => {
                class.previous = Some(ch);
                Some(ClassTerm::Range {
                    first: ch,
                    last: ch,
                })
            }
            ClassAtom::Term(term) => {
                class.previous = None;
                Some(term)
            }
            ClassAtom::Set | ClassAtom::Encoded => {
                class.previous = None;
                None
            }
        };
        if let Some(term) = term.filter(|_| self.capability.is_none()) {
            push_observed(&mut self.parsed.terms, term, self.work)?;
        }
        Ok(())
    }

    fn class_escape(&mut self) -> ParseResult<ClassAtom> {
        let ch = self
            .next()?
            .ok_or(ParseFailure::Data("incomplete regexp escape"))?;
        if let Some(term) = shorthand(ch) {
            return Ok(ClassAtom::Term(term));
        }
        if matches!(ch, 'p' | 'P') && self.eat('{')? {
            self.property(true)?;
            return Ok(ClassAtom::Set);
        }
        self.literal_escape(ch, true)
    }

    pub(super) fn escaped_atom(&mut self) -> ParseResult<()> {
        let ch = self
            .next()?
            .ok_or(ParseFailure::Data("incomplete regexp escape"))?;
        if let Some(term) = shorthand(ch) {
            let start = self.parsed.terms.len();
            if self.capability.is_none() {
                push_observed(&mut self.parsed.terms, term, self.work)?;
            }
            return self.leaf(
                Node::Class {
                    start,
                    end: self.parsed.terms.len(),
                    negated: false,
                },
                true,
            );
        }
        match ch {
            'A' => return self.leaf(Node::Start, false),
            'z' => return self.leaf(Node::End, false),
            'Z' => {
                self.defer("regexp end-before-final-newline anchor is not supported");
                return self.leaf(Node::End, false);
            }
            'b' | 'B' => {
                self.defer("regexp word boundaries are not supported");
                return self.leaf(Node::Empty, false);
            }
            'G' => {
                self.defer("regexp previous-match anchor is not supported");
                return self.leaf(Node::Empty, false);
            }
            'Q' => return self.quote(),
            'p' | 'P' => {
                let braced = self.eat('{')?;
                self.property(braced)?;
                return self.leaf(Node::Empty, true);
            }
            'k' if matches!(self.input.peek(), Some('<' | '\'')) => {
                self.named_backreference()?;
                return self.leaf(Node::Empty, false);
            }
            '1'..='9' if self.numeric_backreference(ch)? => {
                return self.leaf(Node::Empty, false);
            }
            _ => (),
        }
        let atom = self.literal_escape(ch, false)?;
        self.leaf(
            match atom {
                ClassAtom::Scalar(ch) => Node::Literal(ch),
                // Deferred escape encodings never publish a graph.
                _ => Node::Empty,
            },
            true,
        )
    }

    fn quote(&mut self) -> ParseResult<()> {
        self.defer("regexp quoted literal sequences are not supported");
        let mut has_text = false;
        while let Some(ch) = self.next()? {
            if ch == '\\' && self.eat('E')? {
                break;
            }
            has_text = true;
        }
        if has_text {
            self.leaf(Node::Empty, true)?;
        }
        Ok(())
    }

    fn property(&mut self, braced: bool) -> ParseResult<()> {
        // The spelling of a property is a separate Unicode catalogue
        // capability. We still validate known delimiter errors.
        self.defer("regexp Unicode property validation is not supported");
        if braced {
            let mut any = false;
            while let Some(ch) = self.next()? {
                if ch == '}' {
                    return if any {
                        Ok(())
                    } else {
                        Err(ParseFailure::Data("empty regexp Unicode property"))
                    };
                }
                any = true;
            }
            return Err(ParseFailure::Data("unterminated regexp Unicode property"));
        }
        self.next()?
            .ok_or(ParseFailure::Data("missing regexp Unicode property"))?;
        Ok(())
    }

    fn literal_escape(&mut self, ch: char, class: bool) -> ParseResult<ClassAtom> {
        if matches!(
            ch,
            '\\' | '.'
                | '^'
                | '$'
                | '|'
                | '?'
                | '*'
                | '+'
                | '('
                | ')'
                | '{'
                | '}'
                | '['
                | ']'
                | '-'
                | '&'
                | '#'
        ) {
            return Ok(ClassAtom::Scalar(ch));
        }
        self.defer("regexp literal escape is outside the installed grammar");
        let literal = match ch {
            '0'..='7' => {
                read_digits(&mut self.input, 8, 2, self.work)?;
                return Ok(ClassAtom::Encoded);
            }
            'x' => return self.hexadecimal_escape(),
            'u' if self.input.peek().is_some() => {
                let value = read_digits(&mut self.input, 16, 4, self.work)?.unwrap_or(0);
                return Ok(char::from_u32(value).map_or(ClassAtom::Encoded, ClassAtom::Scalar));
            }
            'c' => return self.control_escape(),
            'n' => '\n',
            't' => '\t',
            'r' => '\r',
            'f' => '\u{c}',
            'v' => '\u{b}',
            'a' => '\u{7}',
            'b' if class => '\u{8}',
            'e' => '\u{1b}',
            _ => ch,
        };
        Ok(ClassAtom::Scalar(literal))
    }

    fn hexadecimal_escape(&mut self) -> ParseResult<ClassAtom> {
        let mut cursor = self.input;
        if cursor.eat('{', self.work)? {
            let value = read_digits(&mut cursor, 16, 8, self.work)?;
            if value.is_some_and(|value| value > i32::MAX as u32)
                || cursor.peek().is_some_and(|ch| ch.is_ascii_hexdigit())
            {
                return Err(ParseFailure::Data(
                    "regexp hexadecimal escape is out of range",
                ));
            }
            if let Some(value) = value
                && cursor.eat('}', self.work)?
            {
                self.input = cursor;
                return Ok(char::from_u32(value).map_or(ClassAtom::Encoded, ClassAtom::Scalar));
            }
        } else if self.input.peek().is_some() {
            let value = read_digits(&mut self.input, 16, 2, self.work)?.unwrap_or(0);
            return Ok(if value <= 127 {
                ClassAtom::Scalar(char::from_u32(value).expect("ASCII byte"))
            } else {
                ClassAtom::Encoded
            });
        }
        Ok(ClassAtom::Scalar('x'))
    }

    fn control_escape(&mut self) -> ParseResult<ClassAtom> {
        loop {
            let next = self
                .next()?
                .ok_or(ParseFailure::Data("incomplete regexp control escape"))?;
            if next != '\\' {
                break;
            }
            let escaped = self
                .next()?
                .ok_or(ParseFailure::Data("incomplete regexp escape"))?;
            if escaped != 'c' {
                break;
            }
        }
        Ok(ClassAtom::Encoded)
    }

    fn numeric_backreference(&mut self, first: char) -> ParseResult<bool> {
        let mut cursor = self.input;
        let first = first
            .to_digit(10)
            .expect("backreference starts with a decimal digit") as i32;
        let Decimal::Value(number) = decimal(&mut cursor, Some(first), self.work)? else {
            return Ok(false);
        };
        let reference = number as usize;
        if reference > 9 && (reference >= self.parsed.names.len() || reference > 1_000) {
            return Ok(false);
        }
        self.defer("regexp backreferences are not supported");
        self.largest_numeric_reference = self.largest_numeric_reference.max(reference);
        self.input = cursor;
        Ok(true)
    }

    fn named_backreference(&mut self) -> ParseResult<()> {
        self.defer("regexp backreferences are not supported");
        let delimiter = if self.eat('<')? {
            '>'
        } else {
            self.next()?;
            '\''
        };
        let name = self.group_name(delimiter)?;
        // Simple named and relative references can be checked against captures
        // already seen. Level-qualified forms remain deferred.
        let mut chars = name.chars();
        let first = chars.next().expect("group_name is nonempty");
        let mut simple = first.is_ascii_alphabetic() || first == '_';
        for ch in chars {
            self.work.step()?;
            simple &= ch.is_ascii_alphanumeric() || ch == '_';
        }
        if simple && !self.has_capture(name)? {
            return Err(ParseFailure::Data("undefined regexp capture reference"));
        }
        let mut cursor = Cursor { remaining: name };
        let relative = cursor.eat('-', self.work)?;
        if let Decimal::Value(number) = decimal(&mut cursor, None, self.work)?
            && cursor.remaining.is_empty()
        {
            if number == 0 || (relative && number as usize >= self.parsed.names.len()) {
                return Err(ParseFailure::Data("invalid regexp backreference"));
            }
            if !relative {
                self.largest_numeric_reference =
                    self.largest_numeric_reference.max(number as usize);
            }
        }
        Ok(())
    }

    pub(super) fn counted_repeat(&mut self) -> ParseResult<bool> {
        // Joni Java mode treats a brace without a recognized interval as
        // literal text. Overflow and descending bounds have their own errors.
        let mut cursor = self.input;
        let Some(lower) = repeat_count(decimal(&mut cursor, None, self.work)?)? else {
            return Ok(false);
        };
        let upper = if cursor.eat(',', self.work)? {
            repeat_count(decimal(&mut cursor, None, self.work)?)?
        } else {
            Some(lower)
        };
        if !cursor.eat('}', self.work)? {
            return Ok(false);
        }
        if upper.is_some_and(|upper| upper < lower) {
            return Err(ParseFailure::Data("reversed regexp repetition bounds"));
        }
        self.input = cursor;
        Ok(true)
    }
}

fn shorthand(ch: char) -> Option<ClassTerm> {
    let negated = ch.is_ascii_uppercase();
    Some(match ch.to_ascii_lowercase() {
        'd' => ClassTerm::Digit { negated },
        'w' => ClassTerm::Word { negated },
        's' => ClassTerm::Space { negated },
        _ => return None,
    })
}

fn read_digits(
    cursor: &mut Cursor<'_>,
    radix: u32,
    count: usize,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Option<u32>, KernelFailure> {
    let mut value = None;
    for _ in 0..count {
        let Some(digit) = cursor.peek().and_then(|ch| ch.to_digit(radix)) else {
            break;
        };
        cursor.next(work)?;
        value = Some(
            value
                .unwrap_or(0u32)
                .wrapping_mul(radix)
                .wrapping_add(digit),
        );
    }
    Ok(value)
}

enum Decimal {
    Missing,
    Value(i32),
    Overflow,
}

fn decimal(
    cursor: &mut Cursor<'_>,
    mut value: Option<i32>,
    work: &mut EvaluationCheckpoints<'_>,
) -> Result<Decimal, KernelFailure> {
    while let Some(digit) = cursor.peek().and_then(|ch| ch.to_digit(10)) {
        cursor.next(work)?;
        // Joni's decimal reader checks the sign bit after each Java int
        // operation, including positive wraps in otherwise valid lexemes.
        let next = value
            .unwrap_or(0)
            .wrapping_mul(10)
            .wrapping_add(digit as i32);
        if next < 0 {
            return Ok(Decimal::Overflow);
        }
        value = Some(next);
    }
    Ok(value.map_or(Decimal::Missing, Decimal::Value))
}

fn repeat_count(count: Decimal) -> ParseResult<Option<i32>> {
    // This is the Joni 2.1.5.3 syntax contract, independent of the
    // allocation policy of the installed matcher.
    match count {
        Decimal::Missing => Ok(None),
        Decimal::Value(count) if count <= 100_000 => Ok(Some(count)),
        Decimal::Value(_) | Decimal::Overflow => Err(ParseFailure::Data(
            "regexp repetition count is out of range",
        )),
    }
}
