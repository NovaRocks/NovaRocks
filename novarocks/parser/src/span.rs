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

use novarocks_user_error::UserErrorLocation;

/// A half-open byte range in the original UTF-8 SQL text.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Span {
    start: usize,
    end: usize,
}

/// No user source location exists for a wholly synthesized syntax node.
pub const SYNTHETIC: Span = Span::new(0, 0);

impl Span {
    pub const fn new(start: usize, end: usize) -> Self {
        assert!(start <= end);
        Self { start, end }
    }

    pub const fn start(self) -> usize {
        self.start
    }

    pub const fn end(self) -> usize {
        self.end
    }

    pub const fn is_empty(self) -> bool {
        self.start == self.end
    }

    /// Covers both source ranges without assigning a location to synthetic nodes.
    ///
    /// Empty spans contribute no source location. Unlike source-order merging,
    /// this also supports generated trees that reuse user nodes in another order.
    pub const fn cover(self, other: Self) -> Self {
        if self.is_empty() {
            if other.is_empty() { SYNTHETIC } else { other }
        } else if other.is_empty() {
            self
        } else {
            Self::new(
                if self.start < other.start {
                    self.start
                } else {
                    other.start
                },
                if self.end > other.end {
                    self.end
                } else {
                    other.end
                },
            )
        }
    }

    /// Derives the user-facing byte-column location from this source span.
    pub fn to_user_error_location(self, source: &str) -> UserErrorLocation {
        let source_len = source.len();
        let start = self.start.min(source_len);
        let end = self.end.min(source_len).max(start);
        let start_location = line_col_at(source, start);
        let end_location = line_col_at(source, end);
        UserErrorLocation::new(
            start_location.line,
            start_location.column,
            Some(end_location.line),
            Some(end_location.column),
        )
    }
}

/// A 1-based line and UTF-8 byte-column view of a source position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LineCol {
    pub line: u32,
    pub column: u32,
}

fn line_col_at(source: &str, offset: usize) -> LineCol {
    let mut line = 1;
    let mut line_start = 0;
    for (index, byte) in source.bytes().enumerate() {
        if index >= offset {
            break;
        }
        if byte == b'\n' {
            line += 1;
            line_start = index + 1;
        }
    }
    LineCol {
        line,
        column: (offset - line_start + 1) as u32,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_uses_one_based_utf8_byte_columns() {
        let location = Span::new(4, 7).to_user_error_location("aé\nxyz");
        assert_eq!(location.line(), 2);
        assert_eq!(location.column(), 1);
        assert_eq!(location.end_line(), Some(2));
        assert_eq!(location.end_column(), Some(4));
    }
}
