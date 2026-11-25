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

//! Complete immutable aggregate selection retained by actual lowered calls.
//! This owner records a selection already validated against its original
//! request. Construction checks representation, not catalog admission.

use novarocks_functions::{FunctionBindingSelection, FunctionResultType};
use novarocks_type_contract::{
    FunctionId, ValueTypeStorageLifetime,
    owned_resources::layout::{LayoutResourceError, arc_layout},
};
use std::{alloc::Layout, fmt, sync::Arc};

#[derive(Debug)]
pub struct FrozenAggregateSelection {
    function_id: FunctionId,
    selected: Arc<FunctionBindingSelection>,
    logical_argument_count: usize,
    _storage: ValueTypeStorageLifetime,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FrozenAggregateSelectionError {
    MissingAggregate,
    NonScalarResult,
    InvalidLogicalArgumentCount,
}
impl fmt::Display for FrozenAggregateSelectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid frozen aggregate selection: {self:?}")
    }
}
impl std::error::Error for FrozenAggregateSelectionError {}
impl FrozenAggregateSelection {
    /// Move original identity, selected owner and actual creator lifetime.
    /// This performs no allocation and does not validate or invent a request.
    pub fn try_new(
        function_id: FunctionId,
        selected: Arc<FunctionBindingSelection>,
        logical_argument_count: usize,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, FrozenAggregateSelectionError> {
        if selected.aggregate.is_none() {
            return Err(FrozenAggregateSelectionError::MissingAggregate);
        }
        if !matches!(selected.result_type, FunctionResultType::Scalar(_)) {
            return Err(FrozenAggregateSelectionError::NonScalarResult);
        }
        if logical_argument_count > selected.argument_types.len() {
            return Err(FrozenAggregateSelectionError::InvalidLogicalArgumentCount);
        }
        Ok(Self {
            function_id,
            selected,
            logical_argument_count,
            _storage: storage,
        })
    }
    /// Compare the full frozen source against an independently prepared call.
    /// Private storage and creator identity never participate in type identity.
    pub fn matches_selection_observed<E: From<novarocks_type_contract::ValueTypeError>>(
        &self,
        function_id: &FunctionId,
        selected: &FunctionBindingSelection,
        logical_argument_count: usize,
        mut observe: impl FnMut() -> Result<(), E>,
    ) -> Result<bool, E> {
        fn name<E>(
            left: &str,
            right: &str,
            observe: &mut impl FnMut() -> Result<(), E>,
        ) -> Result<bool, E> {
            observe()?;
            if left.len() != right.len() {
                return Ok(false);
            }
            for (left, right) in left.bytes().zip(right.bytes()) {
                observe()?;
                if left != right {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        if !name(
            self.function_id.as_str(),
            function_id.as_str(),
            &mut observe,
        )? || !name(
            self.selected.overload.as_str(),
            selected.overload.as_str(),
            &mut observe,
        )? {
            return Ok(false);
        }
        observe()?;
        if self.logical_argument_count != logical_argument_count
            || self.selected.argument_types.len() != selected.argument_types.len()
        {
            return Ok(false);
        }
        for (actual, expected) in self
            .selected
            .argument_types
            .iter()
            .zip(selected.argument_types.iter())
        {
            use novarocks_functions::FunctionArgumentType;
            observe()?;
            match (actual, expected) {
                (FunctionArgumentType::Value(actual), FunctionArgumentType::Value(expected)) => {
                    if !actual.exactly_equals_observed(expected, &mut observe)? {
                        return Ok(false);
                    }
                }
                (
                    FunctionArgumentType::Lambda {
                        parameter_types: actual,
                        result_type: a_result,
                    },
                    FunctionArgumentType::Lambda {
                        parameter_types: expected,
                        result_type: e_result,
                    },
                ) => {
                    observe()?;
                    if actual.len() != expected.len() {
                        return Ok(false);
                    }
                    for (actual, expected) in actual.iter().zip(expected.iter()) {
                        if !actual.exactly_equals_observed(expected, &mut observe)? {
                            return Ok(false);
                        }
                    }
                    if !a_result.exactly_equals_observed(e_result, &mut observe)? {
                        return Ok(false);
                    }
                }
                _ => return Ok(false),
            }
        }
        observe()?;
        match (&self.selected.result_type, &selected.result_type) {
            (FunctionResultType::Scalar(actual), FunctionResultType::Scalar(expected)) => {
                if !actual.exactly_equals_observed(expected, &mut observe)? {
                    return Ok(false);
                }
            }
            _ => return Ok(false),
        }
        observe()?;
        match (&self.selected.aggregate, &selected.aggregate) {
            (Some(actual), Some(expected)) => {
                observe()?;
                if actual.state_argument_contract != expected.state_argument_contract
                    || !name(
                        actual.state_format.as_str(),
                        expected.state_format.as_str(),
                        &mut observe,
                    )?
                {
                    return Ok(false);
                }
                actual
                    .intermediate_type
                    .exactly_equals_observed(&expected.intermediate_type, &mut observe)
            }
            _ => Ok(false),
        }
    }
    /// Exact fixed holder request; child owners and identity backing are
    /// separate actual construction operations at the producer.
    pub fn storage_allocation_layout() -> Result<Layout, LayoutResourceError> {
        arc_layout(Layout::new::<Self>())
    }
    pub fn function_id(&self) -> &FunctionId {
        &self.function_id
    }
    pub fn selected(&self) -> &FunctionBindingSelection {
        &self.selected
    }
    pub fn selected_owner(&self) -> &Arc<FunctionBindingSelection> {
        &self.selected
    }
    pub fn logical_argument_count(&self) -> usize {
        self.logical_argument_count
    }
}
impl PartialEq for FrozenAggregateSelection {
    fn eq(&self, other: &Self) -> bool {
        self.function_id == other.function_id
            && self.selected == other.selected
            && self.logical_argument_count == other.logical_argument_count
    }
}
impl Eq for FrozenAggregateSelection {}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;
    use novarocks_functions::{AggregateBindingSelection, FunctionArgumentType};
    use novarocks_type_contract::{
        AggregateStateArgumentContract, AggregateStateFormatId, FunctionArgumentTypes,
        FunctionOverloadId, FunctionValueType, ValueLogicalType, ValueTypeError,
    };
    fn selected(logical: ValueLogicalType) -> FunctionBindingSelection {
        let input = FunctionValueType::try_with_logical_type(DataType::LargeBinary, false, logical)
            .unwrap();
        let state = FunctionValueType::new(DataType::Int64, false);
        FunctionBindingSelection {
            overload: FunctionOverloadId::try_new("test/aggregate/large-binary/v1").unwrap(),
            argument_types: FunctionArgumentTypes::from_pure_boxed(
                vec![FunctionArgumentType::Value(input.clone())].into_boxed_slice(),
            ),
            result_type: FunctionResultType::Scalar(input),
            aggregate: Some(AggregateBindingSelection {
                state_argument_contract: AggregateStateArgumentContract::ExactSignature,
                intermediate_type: state,
                state_format: AggregateStateFormatId::try_new("test/aggregate/state/v1").unwrap(),
            }),
        }
    }
    fn frozen() -> FrozenAggregateSelection {
        FrozenAggregateSelection::try_new(
            FunctionId::try_new("test/aggregate/v1").unwrap(),
            Arc::new(selected(ValueLogicalType::Json)),
            1,
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap()
    }
    #[test]
    fn complete_selection_refuses_same_carrier_nominal_and_nullable_drift() {
        let source = frozen();
        let mut expected = selected(ValueLogicalType::Physical);
        assert!(
            !source
                .matches_selection_observed::<ValueTypeError>(
                    source.function_id(),
                    &expected,
                    1,
                    || Ok(())
                )
                .unwrap()
        );
        expected = selected(ValueLogicalType::Json);
        let FunctionArgumentType::Value(input) = &expected.argument_types[0] else {
            panic!("value input")
        };
        // Publish an independently authored corrupted fixture; immutable
        // selected facts are never edited through their shared backing.
        expected.argument_types = FunctionArgumentTypes::from_pure_boxed(
            vec![FunctionArgumentType::Value(
                input.with_nullable(true).unwrap(),
            )]
            .into_boxed_slice(),
        );
        assert!(
            !source
                .matches_selection_observed::<ValueTypeError>(
                    source.function_id(),
                    &expected,
                    1,
                    || Ok(())
                )
                .unwrap()
        );
        let mut expected = selected(ValueLogicalType::Json);
        expected.aggregate.as_mut().unwrap().state_argument_contract =
            AggregateStateArgumentContract::ValueRootNullabilityIndependent;
        assert!(
            !source
                .matches_selection_observed::<ValueTypeError>(
                    source.function_id(),
                    &expected,
                    1,
                    || Ok(())
                )
                .unwrap()
        );
    }
    #[derive(Debug, Eq, PartialEq)]
    enum Failure {
        Refused(usize),
        Type,
    }
    impl From<ValueTypeError> for Failure {
        fn from(_: ValueTypeError) -> Self {
            Self::Type
        }
    }
    #[test]
    fn complete_selection_preserves_every_original_observer_refusal() {
        let source = frozen();
        let expected = selected(ValueLogicalType::Json);
        let mut visits = 0;
        assert!(
            source
                .matches_selection_observed::<Failure>(source.function_id(), &expected, 1, || {
                    visits += 1;
                    Ok(())
                })
                .unwrap()
        );
        for refused in 1..=visits {
            let mut at = 0;
            let result = source.matches_selection_observed::<Failure>(
                source.function_id(),
                &expected,
                1,
                || {
                    at += 1;
                    if at == refused {
                        Err(Failure::Refused(at))
                    } else {
                        Ok(())
                    }
                },
            );
            assert_eq!(result, Err(Failure::Refused(refused)));
            assert_eq!(at, refused);
        }
    }
    #[test]
    fn complete_selection_holds_the_actual_creator_until_last_arc() {
        struct Hold(Arc<std::sync::atomic::AtomicBool>);
        impl novarocks_type_contract::ValueTypeStorageHold for Hold {}
        impl Drop for Hold {
            fn drop(&mut self) {
                self.0.store(true, std::sync::atomic::Ordering::SeqCst);
            }
        }
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let owner = Arc::new(
            FrozenAggregateSelection::try_new(
                FunctionId::try_new("test/aggregate/v1").unwrap(),
                Arc::new(selected(ValueLogicalType::Json)),
                1,
                ValueTypeStorageLifetime::held(Arc::new(Hold(dropped.clone()))),
            )
            .unwrap(),
        );
        let retained = owner.clone();
        drop(owner);
        assert!(!dropped.load(std::sync::atomic::Ordering::SeqCst));
        drop(retained);
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
    }
}
