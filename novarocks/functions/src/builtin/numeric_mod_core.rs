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
//! ONE original MOD row computation, independent of arena and output carrier.
//! Callers supply checked addresses, explicit work, and their result projection.
use crate::{KernelFailure, math_numeric::NumericArrayView};
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NumericModOp {
    Mod,
}
#[derive(Debug)]
pub enum NumericModError {
    RangeInvariant,
    Control(KernelFailure),
}
impl From<KernelFailure> for NumericModError {
    fn from(value: KernelFailure) -> Self {
        Self::Control(value)
    }
}
impl std::fmt::Display for NumericModError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RangeInvariant => {
                f.write_str("internal error: integer remainder exceeds its proven signed range")
            }
            Self::Control(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for NumericModError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Control(cause) => Some(cause),
            Self::RangeInvariant => None,
        }
    }
}
impl NumericModError {
    pub fn into_kernel(self) -> KernelFailure {
        match self {
            Self::Control(cause) => cause,
            cause => KernelFailure::from_domain(cause),
        }
    }
}
impl novarocks_failure_contract::FailureProjection for NumericModError {
    fn failure_fact(&self) -> novarocks_failure_contract::FailureFact {
        match self {
            Self::Control(cause) => cause.failure_fact(),
            Self::RangeInvariant => novarocks_failure_contract::FailureFact::new(
                novarocks_failure_contract::FailureClass::Invariant,
                novarocks_failure_contract::FailureReason::new(
                    novarocks_failure_contract::domain::KERNEL,
                    std::num::NonZeroU16::new(247).unwrap(),
                ),
            ),
        }
    }
}
impl novarocks_failure_contract::OuterFailureProjection for NumericModError {}
/// The original legacy order reads both numeric values before deciding NULL.
/// Raw legacy callers retain unchecked indexing/broadcast; pure callers provide
/// exact selected addresses and validate their non-null promises in `address`.
pub fn evaluate_mod_rows(
    _op: NumericModOp,
    left_view: &NumericArrayView<'_>,
    right_view: &NumericArrayView<'_>,
    rows: impl IntoIterator<Item = usize>,
    mut address: impl FnMut(usize, usize) -> Result<(usize, usize), KernelFailure>,
    mut emit: impl FnMut(Option<i64>) -> Result<(), KernelFailure>,
    mut observe: impl FnMut() -> Result<(), KernelFailure>,
) -> Result<(), NumericModError> {
    for (ordinal, row) in rows.into_iter().enumerate() {
        observe()?;
        let (left_row, right_row) = address(ordinal, row)?;
        let l = left_view.value_i64(left_row);
        let r = right_view.value_i64(right_row);
        let out = match (l, r) {
            (Some(a), Some(b)) if b != 0 => {
                // Widen before division because i64::MIN % -1 overflows.
                let v = (a as i128) % (b as i128);
                Some(i64::try_from(v).map_err(|_| NumericModError::RangeInvariant)?)
            }
            _ => None,
        };
        emit(out)?;
    }
    Ok(())
}
