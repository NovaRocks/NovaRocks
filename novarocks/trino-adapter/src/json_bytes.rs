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

//! Bounded, exact-capacity JSON bytes for immutable protocol metadata.
use serde::Serialize;
use std::io::{self, Write};

/// Count before allocating; the second pass cannot grow beyond the counted
/// extent. The caller must admit the retained source plus this new backing in
/// its original window before calling. No JSON value tree is constructed.
pub(crate) fn exact_json_bytes<T: Serialize + ?Sized>(
    value: &T,
    max_bytes: usize,
) -> Result<Vec<u8>, serde_json::Error> {
    let mut count = Count {
        bytes: 0,
        limit: max_bytes,
    };
    serde_json::to_writer(&mut count, value)?;
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(count.bytes).map_err(|error| {
        serde_json::Error::io(io::Error::other(format!(
            "allocate exact JSON metadata failed: {error}"
        )))
    })?;
    if bytes.capacity() > max_bytes {
        return Err(serde_json::Error::io(io::Error::other(
            "JSON metadata allocation exceeds its admitted limit",
        )));
    }
    let mut output = Exact {
        bytes,
        limit: count.bytes,
    };
    serde_json::to_writer(&mut output, value)?;
    if output.bytes.len() != count.bytes {
        return Err(serde_json::Error::io(io::Error::other(
            "immutable JSON metadata differs from its counted length",
        )));
    }
    Ok(output.bytes)
}
struct Count {
    bytes: usize,
    limit: usize,
}
impl Write for Count {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .bytes
            .checked_add(bytes.len())
            .filter(|next| *next <= self.limit)
            .ok_or_else(|| io::Error::other("JSON metadata exceeds its admitted limit"))?;
        self.bytes = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
struct Exact {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for Exact {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other(
                "immutable JSON metadata exceeds its counted length",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serializer;
    use std::cell::Cell;

    #[test]
    fn escaped_json_matches_independent_serialization_with_exact_backing() {
        for value in ["", "all ASCII", "\u{0000}\n\r\t\"\\", "中🙂é"] {
            let expected = serde_json::to_vec(value).unwrap();
            let actual = exact_json_bytes(value, expected.len()).unwrap();
            assert_eq!(actual, expected);
            assert_eq!(actual.capacity(), expected.len());
            assert!(exact_json_bytes(value, expected.len() - 1).is_err());
        }
        let values = [
            "\u{0000}".repeat(8192),
            "\"".repeat(8192),
            "🙂".repeat(8192),
        ];
        let expected = serde_json::to_vec(&values).unwrap();
        let actual = exact_json_bytes(&values, expected.len()).unwrap();
        assert_eq!(actual, expected);
        assert_eq!(actual.capacity(), expected.len());
    }

    struct ChangingSource {
        calls: Cell<usize>,
        longer: bool,
    }
    impl Serialize for ChangingSource {
        fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
            let first = self.calls.replace(self.calls.get() + 1) == 0;
            serializer.serialize_str(if first == self.longer {
                "a"
            } else {
                "a much longer second representation"
            })
        }
    }
    #[test]
    fn changed_source_is_refused_without_reallocating_beyond_first_pass() {
        for longer in [true, false] {
            let source = ChangingSource {
                calls: Cell::new(0),
                longer,
            };
            assert!(exact_json_bytes(&source, 1024).is_err());
            assert_eq!(source.calls.get(), 2);
        }
    }
    #[test]
    fn count_limit_rejects_before_allocating_or_starting_a_second_pass() {
        let source = ChangingSource {
            calls: Cell::new(0),
            longer: false,
        };
        assert!(exact_json_bytes(&source, 2).is_err());
        assert_eq!(source.calls.get(), 1);
        let mut count = Count {
            bytes: usize::MAX,
            limit: usize::MAX,
        };
        assert!(count.write_all(b"x").is_err());
        assert_eq!(count.bytes, usize::MAX);
        let mut output = Exact {
            bytes: Vec::with_capacity(1),
            limit: 1,
        };
        output.write_all(b"x").unwrap();
        assert!(output.write_all(b"y").is_err());
        assert_eq!(output.bytes, b"x");
        assert_eq!(output.bytes.capacity(), 1);
    }
}
