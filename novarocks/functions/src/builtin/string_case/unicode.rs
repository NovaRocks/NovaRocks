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

use icu_casemap::CaseMapper;
use regex_syntax::{
    Parser,
    hir::{Class, ClassUnicode, HirKind},
};
use std::{cmp::Ordering, sync::LazyLock};

/// Trino 484's JDK 25 uses Unicode 16.0, while ICU4X 2.1 uses Unicode 17.
/// The locked regex-syntax tables supply the Unicode 16.0 assigned ranges.
static UNICODE_16_AGE: LazyLock<ClassUnicode> = LazyLock::new(|| {
    let hir = Parser::new()
        .parse(r"\p{Age=16.0}")
        .expect("the locked Unicode tables contain Age=16.0");
    match hir.into_kind() {
        HirKind::Class(Class::Unicode(class)) => class,
        _ => unreachable!("Unicode Age is a scalar range class"),
    }
});

pub(in crate::builtin) fn assigned_in_unicode_16(character: char) -> bool {
    character.is_ascii()
        || UNICODE_16_AGE
            .ranges()
            .binary_search_by(|range| {
                if character < range.start() {
                    Ordering::Greater
                } else if character > range.end() {
                    Ordering::Less
                } else {
                    Ordering::Equal
                }
            })
            .is_ok()
}

fn guard(source: char, mapped: char) -> char {
    if source == mapped || (assigned_in_unicode_16(source) && assigned_in_unicode_16(mapped)) {
        mapped
    } else {
        source
    }
}

pub(in crate::builtin) fn lowercase(character: char) -> char {
    guard(character, CaseMapper::new().simple_lowercase(character))
}

pub(in crate::builtin) fn uppercase(character: char) -> char {
    guard(character, CaseMapper::new().simple_uppercase(character))
}

pub(in crate::builtin) fn titlecase(character: char) -> char {
    guard(character, CaseMapper::new().simple_titlecase(character))
}
