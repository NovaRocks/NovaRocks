// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Shared forward matching, observed traversal and complete SUBSTRING_INDEX span decisions.
//! The callers retain their original evaluated-input, control and storage projections.

pub fn forward_match_indices<'text, 'delimiter>(
    text: &'text str,
    delimiter: &'delimiter str,
) -> std::str::MatchIndices<'text, &'delimiter str> {
    text.match_indices(delimiter)
}

pub trait ForwardMatchObserver {
    type Error;
    fn flush(&mut self) -> Result<(), Self::Error>;
    fn step(&mut self) -> Result<(), Self::Error>;
}

pub struct ForwardScan {
    pub count: usize,
    pub cut: Option<usize>,
}

pub fn scan_forward_matches<O: ForwardMatchObserver>(
    text: &str,
    delimiter: &str,
    target: Option<usize>,
    work: &mut O,
) -> Result<ForwardScan, O::Error> {
    // Retain forward match_indices, including its non-overlap direction.
    // The old negative-index algorithm selects from these forward matches,
    // rather than using reverse search. Std search state is borrowed/inline.
    work.flush()?;
    let mut matches = forward_match_indices(text, delimiter);
    work.flush()?;
    let mut count = 0usize;
    let mut cut = None;
    loop {
        work.flush()?;
        let next = matches.next();
        work.flush()?;
        if let Some((byte, _)) = next {
            count += 1;
            if target == Some(count) {
                cut = Some(byte);
            }
            work.step()?;
        } else {
            work.step()?;
            break;
        }
    }
    Ok(ForwardScan { count, cut })
}

/// Storage access for one immutable evaluated text and delimiter.
/// The span author calls cut accessors only with a legal one-based ordinal.
pub trait ForwardMatchAccess<'text> {
    type Error;
    fn text(&self) -> &'text str;
    fn first_pass(
        &mut self,
        text: &str,
        delimiter: &str,
        positive_target: Option<usize>,
    ) -> Result<usize, Self::Error>;
    fn prefix_cut(
        &mut self,
        text: &str,
        delimiter: &str,
        ordinal: usize,
    ) -> Result<usize, Self::Error>;
    fn suffix_cut(
        &mut self,
        text: &str,
        delimiter: &str,
        ordinal: usize,
    ) -> Result<usize, Self::Error>;
}

pub fn substring_index_span<'text, A: ForwardMatchAccess<'text>>(
    access: &mut A,
    delimiter: &str,
    count: i64,
) -> Result<Option<&'text str>, A::Error> {
    if delimiter.is_empty() || count == 0 {
        return Ok(None);
    }
    let text = access.text();
    let positive_target = if count > 0 {
        Some(count as usize)
    } else {
        None
    };
    let matches = access.first_pass(text, delimiter, positive_target)?;
    if matches == 0 {
        return Ok(Some(text));
    }
    if count > 0 {
        let distance = count as usize;
        if distance > matches {
            return Ok(Some(text));
        }
        let cut = access.prefix_cut(text, delimiter, distance)?;
        Ok(Some(&text[..cut]))
    } else {
        // Preserve the raw legacy checked-negation panic after the full first
        // pass. Installed Int32 counts are widened before reaching this author.
        let distance = (-count) as usize;
        if distance > matches {
            return Ok(Some(text));
        }
        let ordinal = matches - distance + 1;
        let cut = access.suffix_cut(text, delimiter, ordinal)?;
        Ok(Some(&text[cut + delimiter.len()..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;

    #[derive(Default)]
    struct Observer(Vec<u8>);
    impl ForwardMatchObserver for Observer {
        type Error = Infallible;
        fn flush(&mut self) -> Result<(), Infallible> {
            self.0.push(0);
            Ok(())
        }
        fn step(&mut self) -> Result<(), Infallible> {
            self.0.push(1);
            Ok(())
        }
    }
    #[test]
    fn string_substring_index_core_observer_consumes_all_forward_matches_and_terminal_step() {
        let mut observer = Observer::default();
        let scan = scan_forward_matches("aaaaa", "aa", Some(1), &mut observer).unwrap();
        assert_eq!(scan.count, 2);
        assert_eq!(scan.cut, Some(0));
        assert_eq!(observer.0, [0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 1]);
    }
    struct Access<'text> {
        text: &'text str,
        positions: Vec<usize>,
    }
    impl<'text> ForwardMatchAccess<'text> for Access<'text> {
        type Error = Infallible;
        fn text(&self) -> &'text str {
            self.text
        }
        fn first_pass(
            &mut self,
            text: &str,
            delimiter: &str,
            _: Option<usize>,
        ) -> Result<usize, Infallible> {
            self.positions = forward_match_indices(text, delimiter)
                .map(|(idx, _)| idx)
                .collect();
            Ok(self.positions.len())
        }
        fn prefix_cut(&mut self, _: &str, _: &str, ordinal: usize) -> Result<usize, Infallible> {
            Ok(self.positions[ordinal - 1])
        }
        fn suffix_cut(&mut self, _: &str, _: &str, ordinal: usize) -> Result<usize, Infallible> {
            Ok(self.positions[ordinal - 1])
        }
    }
    #[test]
    fn string_substring_index_core_span_outlives_independent_delimiter_and_stack_access() {
        let text = String::from("aaaaa");
        let span = {
            let delimiter = String::from("aa");
            let mut access = Access {
                text: &text,
                positions: Vec::new(),
            };
            substring_index_span(&mut access, &delimiter, -1)
                .unwrap()
                .unwrap()
        };
        assert_eq!(span, "a");
    }
    struct Unread;
    impl ForwardMatchAccess<'static> for Unread {
        type Error = Infallible;
        fn text(&self) -> &'static str {
            panic!("ineligible text must not be read")
        }
        fn first_pass(&mut self, _: &str, _: &str, _: Option<usize>) -> Result<usize, Infallible> {
            panic!("ineligible scan")
        }
        fn prefix_cut(&mut self, _: &str, _: &str, _: usize) -> Result<usize, Infallible> {
            panic!("ineligible prefix")
        }
        fn suffix_cut(&mut self, _: &str, _: &str, _: usize) -> Result<usize, Infallible> {
            panic!("ineligible suffix")
        }
    }
    #[test]
    fn string_substring_index_core_empty_delimiter_and_zero_count_do_not_read_text() {
        assert_eq!(
            substring_index_span(&mut Unread, "", i64::MIN).unwrap(),
            None
        );
        assert_eq!(substring_index_span(&mut Unread, "x", 0).unwrap(), None);
    }
}
