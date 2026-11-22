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

/// Fixed versus varying SQL text with a finite length measured in code points.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum CharacterKind {
    Char,
    Varchar,
}

/// A bounded character declaration. Unbounded VARCHAR has its own string
/// identity and does not use a length sentinel in a frozen type.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct CharacterType {
    kind: CharacterKind,
    length: u32,
}

impl CharacterType {
    pub const MAX_CHAR_LENGTH: u32 = 65_536;
    pub const MAX_VARCHAR_LENGTH: u32 = i32::MAX as u32 - 1;

    pub const fn new(kind: CharacterKind, length: u32) -> Option<Self> {
        let max = match kind {
            CharacterKind::Char => Self::MAX_CHAR_LENGTH,
            CharacterKind::Varchar => Self::MAX_VARCHAR_LENGTH,
        };
        if length <= max {
            Some(Self { kind, length })
        } else {
            None
        }
    }

    pub const fn kind(self) -> CharacterKind {
        self.kind
    }

    pub const fn length(self) -> u32 {
        self.length
    }
}

#[cfg(test)]
mod tests {
    use super::{CharacterKind, CharacterType};

    #[test]
    fn bounded_character_types_preserve_kind_and_zero_length() {
        let char = CharacterType::new(CharacterKind::Char, 0).unwrap();
        let varchar = CharacterType::new(CharacterKind::Varchar, 0).unwrap();
        assert_ne!(char, varchar);
        assert_eq!(char.kind(), CharacterKind::Char);
        assert_eq!(varchar.length(), 0);
    }

    #[test]
    fn bounded_character_types_reject_out_of_domain_lengths() {
        assert!(CharacterType::new(CharacterKind::Char, 65_536).is_some());
        assert!(CharacterType::new(CharacterKind::Char, 65_537).is_none());
        assert!(CharacterType::new(CharacterKind::Varchar, 2_147_483_646).is_some());
        assert!(CharacterType::new(CharacterKind::Varchar, 2_147_483_647).is_none());
    }
}
