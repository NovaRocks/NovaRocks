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

use super::{ChildTyping, TypingError};
use arrow::datatypes::DataType;
use novarocks_functions::FunctionBindingError;
use novarocks_type_contract::{
    CharacterKind, FunctionOverloadId, ValueKind, trino_can_coerce_scalar,
};

/// Project existing unavailable declarations after actual catalogue selection.
pub(super) fn selection_error(
    name: &str,
    children: &[ChildTyping],
    error: FunctionBindingError,
) -> TypingError {
    if error == FunctionBindingError::UnknownFunction
        && novarocks_functions::builtin::intrinsic::builtin_disposition(name)
            == Some(novarocks_functions::builtin::intrinsic::BuiltinDisposition::LoweredOnly)
    {
        return TypingError::Unsupported(format!(
            "builtin function `{name}` has no admitted selected scalar implementation"
        ));
    }
    if !matches!(
        error,
        FunctionBindingError::UnknownFunction | FunctionBindingError::NoMatchingOverload
    ) {
        return error.into();
    }
    let text = |child: &ChildTyping| {
        trino_can_coerce_scalar(child.value_type.value_type().kind(), &ValueKind::String)
    };
    let bigint = |child: &ChildTyping| {
        trino_can_coerce_scalar(
            child.value_type.value_type().kind(),
            &ValueKind::Scalar(DataType::Int64),
        )
    };
    let overload = match (name, children) {
        ("format", [template, _, ..]) => {
            let actual = template.value_type.value_type().kind();
            let varchar = matches!(actual, ValueKind::String)
                || matches!(actual, ValueKind::BoundedCharacter(character)
                    if character.kind() == CharacterKind::Varchar);
            if !varchar {
                return TypingError::TypeMismatch(format!(
                    "Type of first argument to format() must be VARCHAR (actual: {actual:?})"
                ));
            }
            "builtin.scalar/format/trino-variadic-v1"
        }
        ("$overlay", [source, replacement, start])
            if text(source) && text(replacement) && bigint(start) =>
        {
            "builtin.scalar/$overlay/trino-varchar-three-v1"
        }
        ("$overlay", [source, replacement, start, length])
            if text(source) && text(replacement) && bigint(start) && bigint(length) =>
        {
            "builtin.scalar/$overlay/trino-varchar-four-v1"
        }
        ("trim" | "ltrim" | "rtrim", [source, characters]) if text(source) && text(characters) => {
            let character = matches!(
                source.value_type.value_type().kind(),
                ValueKind::BoundedCharacter(source) if source.kind() == CharacterKind::Char
            );
            match (name, character) {
                ("trim", false) => "builtin.scalar/trim/trino-varchar-codepoints-v1",
                ("trim", true) => "builtin.scalar/trim/trino-char-codepoints-v1",
                ("ltrim", false) => "builtin.scalar/ltrim/trino-varchar-codepoints-v1",
                ("ltrim", true) => "builtin.scalar/ltrim/trino-char-codepoints-v1",
                ("rtrim", false) => "builtin.scalar/rtrim/trino-varchar-codepoints-v1",
                ("rtrim", true) => "builtin.scalar/rtrim/trino-char-codepoints-v1",
                _ => unreachable!("checked trim name"),
            }
        }
        ("format" | "$overlay", _) => return FunctionBindingError::NoMatchingOverload.into(),
        _ => return error.into(),
    };
    FunctionBindingError::UnavailableImplementation(
        FunctionOverloadId::try_new(overload).expect("valid unavailable string overload identity"),
    )
    .into()
}
