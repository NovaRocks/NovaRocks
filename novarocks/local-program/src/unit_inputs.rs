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

//! Bind checked unit ordinals to the actual fragment input of each root.
use crate::{
    ProgramNode, ProgramNodeId, ProgramNodeKind, ProgramRootRef, StaticLayout, StaticSinkProgram,
    StaticStreamRouting,
};
use novarocks_expression::CompiledExprSet;
use novarocks_type_contract::{ArrowTypeProfile, FunctionValueType};
use std::sync::Arc;

#[derive(Clone, Copy)]
enum Input<'a> {
    Empty,
    Layout(&'a StaticLayout),
}
impl Input<'_> {
    fn width(self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Layout(layout) => layout.schema().fields().len(),
        }
    }
    fn identity(self) -> usize {
        match self {
            Self::Empty => 0,
            Self::Layout(layout) => layout as *const StaticLayout as usize,
        }
    }
}

#[derive(Clone, Copy)]
enum Role {
    Single,
    Probe,
    Build,
    Combined,
}
impl Role {
    fn cache(self) -> usize {
        match self {
            Self::Single | Self::Probe => 0,
            Self::Build => 1,
            Self::Combined => 2,
        }
    }
}

enum SchemaComparisonFailure<E> {
    Observed(E),
    InvalidType,
}
impl<E> From<novarocks_type_contract::ValueTypeError> for SchemaComparisonFailure<E> {
    fn from(_: novarocks_type_contract::ValueTypeError) -> Self {
        Self::InvalidType
    }
}

/// The cache is a stack-local memo of an immutable comparison. It owns no
/// binding or capacity and does not add another expression/channel authority.
/// Single-input, probe, build and combined fields reuse their actual immutable
/// comparison separately, so alternating wide join keys do not rescan schemas.
/// Cache eviction changes work only; it cannot reject a valid new input.
struct Validation<'a, E, O, I> {
    units: &'a [Arc<CompiledExprSet>],
    observe: O,
    invalid: I,
    last: [Option<(usize, usize)>; 3],
    marker: std::marker::PhantomData<E>,
}
impl<E, O, I> Validation<'_, E, O, I>
where
    O: FnMut() -> Result<(), E>,
    I: Fn() -> E,
{
    fn root(&mut self, reference: ProgramRootRef, input: Input<'_>) -> Result<(), E> {
        self.root_in(reference, input, Role::Single)
    }
    fn root_in(
        &mut self,
        reference: ProgramRootRef,
        input: Input<'_>,
        role: Role,
    ) -> Result<(), E> {
        (self.observe)()?;
        let unit = self
            .units
            .get(reference.unit.index())
            .ok_or_else(&self.invalid)?;
        let root = unit.root(reference.root).ok_or_else(&self.invalid)?;
        let scope = unit.scope(root.scope).ok_or_else(&self.invalid)?;
        let key = (scope as *const _ as usize, input.identity());
        if self.last[role.cache()] == Some(key) {
            return Ok(());
        }
        // The sole compiler publishes this canonical empty AllPorts scope
        // for a zero-port unit. It contains no possible runtime input binding.
        if scope.schema().fields().is_empty() && scope.columns().is_empty() {
            return Ok(());
        }
        if scope.schema().fields().len() != input.width() {
            return Err((self.invalid)());
        }
        let Input::Layout(layout) = input else {
            return Err((self.invalid)());
        };
        // The canonical value map can omit repeated physical publications.
        // Check every runtime carrier and its original complete value facts.
        // Top-level labels and annotations are not expression input identity.
        for (expected, actual) in scope.schema().fields().iter().zip(layout.schema().fields()) {
            (self.observe)()?;
            if expected.is_nullable() != actual.is_nullable() {
                return Err((self.invalid)());
            }
            let equal =
                novarocks_type_contract::arrow_data_types_exact_borrowed_with_profile_observed::<
                    SchemaComparisonFailure<E>,
                >(
                    expected.data_type(),
                    actual.data_type(),
                    ArrowTypeProfile::CanonicalValue,
                    || (self.observe)().map_err(SchemaComparisonFailure::Observed),
                )
                .map_err(|failure| match failure {
                    SchemaComparisonFailure::Observed(error) => error,
                    SchemaComparisonFailure::InvalidType => (self.invalid)(),
                })?;
            if !equal {
                return Err((self.invalid)());
            }
        }
        for (ordinal, expected) in scope.ordinal_types().iter().enumerate() {
            (self.observe)()?;
            let binding = layout.value_binding_at(ordinal).ok_or_else(&self.invalid)?;
            // These are the actual complete runtime bindings, including nominal
            // children. Arrow carrier metadata is not a source of such facts.
            let actual = FunctionValueType::try_from_typed_binding(
                binding.clone_observed(&mut self.observe)?,
            )
            .map_err(|_| (self.invalid)())?;
            if !expected.exactly_equals_checked_observed(&actual, &mut self.observe)? {
                return Err((self.invalid)());
            }
        }
        self.last[role.cache()] = Some(key);
        Ok(())
    }
}

/// Borrow the caller's original observation scope. Graph construction and final
/// publication use this same core on the exact table each of them receives.
pub(crate) fn validate<E>(
    nodes: &[ProgramNode],
    root: ProgramNodeId,
    sink: Option<&StaticSinkProgram>,
    units: &[Arc<CompiledExprSet>],
    observe: impl FnMut() -> Result<(), E>,
    invalid: impl Fn() -> E,
) -> Result<(), E> {
    let mut validation = Validation {
        units,
        observe,
        invalid,
        last: [None; 3],
        marker: std::marker::PhantomData,
    };
    for node in nodes {
        (validation.observe)()?;
        match node.kind() {
            ProgramNodeKind::Join {
                condition,
                left_layout,
                right_layout,
                join_scope_layout,
                runtime_filters,
                runtime_filter_consumers,
                ..
            } => {
                // Physical ports keep their source numbering when the compiler
                // mirrors a join. The fields already identify the actual probe
                // and build inputs; do not assume physical port 0 is probe.
                for keys in [condition.keys(), condition.scope_keys()] {
                    for key in keys {
                        validation.root_in(key.probe, Input::Layout(left_layout), Role::Probe)?;
                    }
                    for key in keys {
                        validation.root_in(key.build, Input::Layout(right_layout), Role::Build)?;
                    }
                }
                if let Some(root) = condition.residual_predicate() {
                    validation.root_in(root, Input::Layout(join_scope_layout), Role::Combined)?;
                }
                for filter in runtime_filters {
                    validation.root_in(filter.expr_id, Input::Layout(right_layout), Role::Build)?;
                }
                for filter in runtime_filter_consumers {
                    validation.root_in(filter.expr_id, Input::Layout(left_layout), Role::Probe)?;
                }
            }
            ProgramNodeKind::NestedLoopJoin {
                join_conjunct,
                join_scope_layout,
                ..
            } => {
                if let Some(root) = join_conjunct {
                    validation.root_in(*root, Input::Layout(join_scope_layout), Role::Combined)?;
                }
            }
            kind => {
                let input = match kind {
                    ProgramNodeKind::Scan { .. } | ProgramNodeKind::ExchangeSource { .. } => {
                        Input::Layout(node.output_layout())
                    }
                    ProgramNodeKind::TableFinish {
                        inputs,
                        input_roles,
                        ..
                    } => Input::Layout(
                        nodes
                            .get(
                                inputs
                                    .get(input_roles.writer_results as usize)
                                    .ok_or_else(&validation.invalid)?
                                    .index(),
                            )
                            .ok_or_else(&validation.invalid)?
                            .output_layout(),
                    ),
                    ProgramNodeKind::AssertNumRows { input, .. }
                    | ProgramNodeKind::Project { input, .. }
                    | ProgramNodeKind::Unpivot { input, .. }
                    | ProgramNodeKind::Filter { input, .. }
                    | ProgramNodeKind::Repeat { input, .. }
                    | ProgramNodeKind::ChangeEventExpand { input, .. }
                    | ProgramNodeKind::Limit { input, .. }
                    | ProgramNodeKind::Sort { input, .. }
                    | ProgramNodeKind::TableFunction { input, .. }
                    | ProgramNodeKind::Aggregate { input, .. }
                    | ProgramNodeKind::GroupedTopN { input, .. }
                    | ProgramNodeKind::Analytic { input, .. }
                    | ProgramNodeKind::RuntimeFilterConsumer { input, .. }
                    | ProgramNodeKind::TableWriter { input, .. } => Input::Layout(
                        nodes
                            .get(input.index())
                            .ok_or_else(&validation.invalid)?
                            .output_layout(),
                    ),
                    // Values cells have an actual empty input. The remaining
                    // source/container kinds carry no expression root fields.
                    _ => Input::Empty,
                };
                kind.try_for_each_expression(|reference, _| match reference {
                    Some(root) => validation.root(root, input),
                    None => (validation.observe)(),
                })?;
            }
        }
    }
    if let Some(sink) = sink {
        let input = Input::Layout(
            nodes
                .get(root.index())
                .ok_or_else(&validation.invalid)?
                .output_layout(),
        );
        for branch in sink.branches() {
            (validation.observe)()?;
            if let StaticStreamRouting::ConditionalKeyV1 { applies } = branch.contract().routing {
                validation.root(applies, input)?;
            }
            for root in branch.partition_exprs() {
                validation.root(*root, input)?;
            }
        }
        for root in sink.split_exprs() {
            validation.root(*root, input)?;
        }
    }
    Ok(())
}
