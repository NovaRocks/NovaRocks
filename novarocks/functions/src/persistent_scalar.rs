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

//! Scalar definition dependencies whose value or evaluation contract changed.

use crate::{FunctionId, FunctionOverloadId, ResolvedFunctionBinding};

/// A generation is scoped to the stable function ID. Persistence retains
/// unknown future generations so an old reader can report the difference.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct PersistentScalarContract {
    function: FunctionId,
    overload: FunctionOverloadId,
    generation: std::num::NonZeroU32,
}

impl PersistentScalarContract {
    pub fn new(
        function: FunctionId,
        overload: FunctionOverloadId,
        generation: std::num::NonZeroU32,
    ) -> Self {
        Self {
            function,
            overload,
            generation,
        }
    }

    pub fn from_binding(binding: &ResolvedFunctionBinding) -> Option<Self> {
        let id = binding.function_id.as_str();
        let builtin = id.strip_prefix("builtin.scalar/")?.strip_suffix("/v1")?;
        let generation = match builtin {
            "abs"
            | "acos"
            | "asin"
            | "atan"
            | "atan2"
            | "cbrt"
            | "ceil"
            | "ceiling"
            | "cos"
            | "cosine_similarity"
            | "degrees"
            | "euclidean_distance"
            | "exp"
            | "floor"
            | "greatest"
            | "least"
            | "ln"
            | "log"
            | "log10"
            | "log2"
            | "mod"
            | "nullif"
            | "pow"
            | "power"
            | "radians"
            | "rand"
            | "random"
            | "round"
            | "sign"
            | "sin"
            | "sqrt"
            | "tan"
            | "truncate"
            | "typeof"
            | "uuid"
            | "bitwise_and"
            | "bitwise_or"
            | "bitwise_xor"
            | "bitwise_not"
            | "bitwise_left_shift"
            | "bitwise_right_shift"
            | "bitwise_right_shift_arithmetic" => std::num::NonZeroU32::MIN,
            _ => return None,
        };
        Some(Self::new(
            binding.function_id.clone(),
            binding.selected.overload.clone(),
            generation,
        ))
    }

    pub fn function(&self) -> &FunctionId {
        &self.function
    }

    pub fn overload(&self) -> &FunctionOverloadId {
        &self.overload
    }

    pub fn generation(&self) -> std::num::NonZeroU32 {
        self.generation
    }
}

/// Both callers supply canonical sets for the same definition. A missing
/// recorded fact matters only when the resolved definition requires it.
pub fn validate_published_scalar_contracts(
    recorded: &[PersistentScalarContract],
    resolved: &[PersistentScalarContract],
) -> Result<(), String> {
    if recorded == resolved {
        return Ok(());
    }
    let changed = resolved
        .iter()
        .find(|contract| recorded.binary_search(contract).is_err())
        .or_else(|| {
            recorded
                .iter()
                .find(|contract| resolved.binary_search(contract).is_err())
        })
        .ok_or("MV scalar contracts are not in canonical order")?;
    Err(format!(
        "MV scalar {} requires a full rebuild: publication does not match the selected overload {} generation {}",
        changed.function().as_str(),
        changed.overload().as_str(),
        changed.generation()
    ))
}
