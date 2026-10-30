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

//! Adapter-local client display bound; original domain diagnostics are unchanged.
use std::fmt::{self, Write};

/// The complete JSON string token, including its two quote bytes, is bounded.
pub(crate) const MAX_MESSAGE_JSON_BYTES: usize = 16 * 1024;
/// Fixed envelope fields are accounted independently of the message and columns.
pub(crate) const MAX_METADATA_JSON_BYTES: usize = 4 * 1024;

pub(crate) struct BoundedMessage {
    pub(crate) message: String,
    pub(crate) json_bytes: usize,
    pub(crate) truncated: bool,
}

/// Return a UTF-8 prefix without first cloning or serializing the full input.
pub(crate) fn bound_message(message: &str) -> BoundedMessage {
    bound_display(message)
}

/// Sources with structured public Display output stream into this same bound.
/// The formatter stops on the first character that cannot fit, so a large
/// Session error does not need a temporary unbounded formatted String.
pub(crate) fn bound_display(message: impl fmt::Display) -> BoundedMessage {
    let mut writer = MessageWriter {
        message: [0; MAX_MESSAGE_JSON_BYTES - 2],
        message_len: 0,
        json_bytes: 2,
        truncated: false,
    };
    let result = write!(&mut writer, "{message}");
    assert!(
        result.is_ok() || writer.truncated,
        "public diagnostic Display failed before the client bound"
    );
    BoundedMessage {
        // The writer copied only complete UTF-8 characters. The final heap
        // backing is exactly the retained prefix, without geometric reserve.
        message: String::from_utf8(writer.message[..writer.message_len].to_vec())
            .expect("bounded diagnostic prefix is valid UTF-8"),
        json_bytes: writer.json_bytes,
        truncated: writer.truncated,
    }
}
struct MessageWriter {
    message: [u8; MAX_MESSAGE_JSON_BYTES - 2],
    message_len: usize,
    json_bytes: usize,
    truncated: bool,
}
impl fmt::Write for MessageWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.truncated {
            return Err(fmt::Error);
        }
        for character in value.chars() {
            let escaped = match character {
                '"' | '\\' | '\u{08}' | '\t' | '\n' | '\u{0c}' | '\r' => 2,
                '\u{00}'..='\u{1f}' => 6,
                character => character.len_utf8(),
            };
            if self.json_bytes + escaped > MAX_MESSAGE_JSON_BYTES {
                self.truncated = true;
                return Err(fmt::Error);
            }
            let mut encoded = [0; 4];
            let encoded = character.encode_utf8(&mut encoded).as_bytes();
            let end = self.message_len + encoded.len();
            self.message[self.message_len..end].copy_from_slice(encoded);
            self.message_len = end;
            self.json_bytes += escaped;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn json_escape_cost_matches_serde_for_controls_quotes_and_unicode() {
        for character in (0_u8..=127)
            .map(char::from)
            .chain(['中', '😀', '\u{2028}', '\u{2029}'])
        {
            let input = character.to_string().repeat(MAX_MESSAGE_JSON_BYTES);
            let bounded = bound_message(&input);
            let encoded = serde_json::to_string(&bounded.message).unwrap();
            assert_eq!(bounded.json_bytes, encoded.len(), "{character:?}");
            assert!(encoded.len() <= MAX_MESSAGE_JSON_BYTES);
            assert!(bounded.truncated);
            assert!(input.starts_with(&bounded.message));
            assert!(
                serde_json::to_string(&(bounded.message.clone() + &character.to_string()))
                    .unwrap()
                    .len()
                    > MAX_MESSAGE_JSON_BYTES
            );
        }
    }
    #[test]
    fn exact_limit_includes_quotes_and_stops_at_a_complete_character() {
        let input = "x".repeat(MAX_MESSAGE_JSON_BYTES - 2);
        let exact = bound_message(&input);
        assert!(!exact.truncated);
        assert_eq!(exact.json_bytes, MAX_MESSAGE_JSON_BYTES);
        assert_eq!(exact.message, input);
        let input = format!("{}中", "x".repeat(MAX_MESSAGE_JSON_BYTES - 3));
        let bounded = bound_message(&input);
        assert!(bounded.truncated);
        assert_eq!(bounded.message, "x".repeat(MAX_MESSAGE_JSON_BYTES - 3));
        assert_eq!(
            serde_json::to_string(&bounded.message).unwrap().len(),
            MAX_MESSAGE_JSON_BYTES - 1
        );
        let empty = bound_message("");
        assert_eq!(empty.json_bytes, 2);
        assert!(!empty.truncated);
    }
    #[test]
    fn formatted_source_stops_before_building_its_full_message() {
        struct Source<'a>(&'a std::cell::Cell<usize>);
        impl fmt::Display for Source<'_> {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("[query.execution.commit_unknown] ")?;
                for _ in 0..1_000_000 {
                    self.0.set(self.0.get() + 1);
                    formatter.write_str("\"")?;
                }
                Ok(())
            }
        }
        let visits = std::cell::Cell::new(0);
        let bounded = bound_display(Source(&visits));
        assert!(bounded.truncated);
        assert!(
            bounded
                .message
                .starts_with("[query.execution.commit_unknown] ")
        );
        assert!(visits.get() < MAX_MESSAGE_JSON_BYTES);
        assert!(serde_json::to_string(&bounded.message).unwrap().len() <= MAX_MESSAGE_JSON_BYTES);
    }
}
