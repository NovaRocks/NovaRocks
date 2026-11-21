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
use crate::FunctionId;
/// The FE author uses exactly the v1 wire namespace projection. This never
/// enters a math API or chooses a runtime implementation by display name.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NonCanonicalNativeV1FunctionName;
/// Sole native-v1 identity projection. Callers format their own full diagnostics.
/// This does not select a computation or a runtime implementation.
pub fn native_v1_function_name(
    identity: &FunctionId,
) -> Result<&str, NonCanonicalNativeV1FunctionName> {
    identity
        .as_str()
        .strip_prefix("builtin.")
        .or_else(|| identity.as_str().strip_prefix("parametric."))
        .and_then(|v| v.split_once('/').map(|(_, name)| name))
        .and_then(|name| name.strip_suffix("/v1"))
        .filter(|name| !name.is_empty())
        .ok_or(NonCanonicalNativeV1FunctionName)
}

pub const NATIVE_V1_ENCRYPTION_FUNCTIONS: &[(&str, &str)] = &[
    ("base64_decode_binary", "from_base64"),
    ("base64_decode_string", "from_base64"),
    ("from_base64", "from_base64"),
    ("from_binary", "from_binary"),
    ("md5", "md5"),
    ("sha2", "sha2"),
    ("to_base64", "to_base64"),
    ("to_binary", "to_binary"),
];
/// Non-value source provenance for the complete TO_BASE64 Utf8 profile.
/// Ordinary is authored evidence (slots, constants, casts or other calls), not absence.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum ToBase64ByteSource {
    Ordinary,
    NativeV1EncryptionLatin1,
}
impl ToBase64ByteSource {
    /// One classification of the native-v1 immediate dispatch identity. Used only by
    /// source producers/validators and the original arena projection, never by math.
    pub fn from_immediate_native_function_name(name: Option<&str>) -> Self {
        let canonical = name.and_then(|name| {
            let lower = name.to_lowercase();
            NATIVE_V1_ENCRYPTION_FUNCTIONS
                .iter()
                .find(|(registered, _)| *registered == lower.as_str())
                .map(|(_, canonical)| *canonical)
        });
        Self::from_immediate_encryption_identity(canonical)
    }
    /// Raw/Local construction tags are already dispatch identities; retain exact old tag semantics.
    pub fn from_immediate_encryption_identity(name: Option<&str>) -> Self {
        if matches!(name, Some("from_base64" | "to_binary")) {
            Self::NativeV1EncryptionLatin1
        } else {
            Self::Ordinary
        }
    }
    pub const fn prefers_latin1(self) -> bool {
        matches!(self, Self::NativeV1EncryptionLatin1)
    }
}
