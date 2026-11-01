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
//! Assertion processor for row-count contract checks.
//!
//! Responsibilities:
//! - Validates row-count constraints required by ASSERT NUM ROWS semantics at runtime.
//! - Fails fast with explicit errors when produced row counts violate configured predicates.
//!
//! Key exported interfaces:
//! - Types: `AssertNumRowsProcessorFactory`.
//!
//! Current limitations:
//! - Implements only the execution semantics currently wired by novarocks plan lowering and pipeline builder.
//! - Unsupported states should be surfaced as explicit runtime errors instead of fallback behavior.

use crate::exec::pipeline::operator::OperatorLifecycle;
use crate::runtime::fragment::{ExecutionFailure, ExecutionReason, ExecutionResult};
use novarocks_failure_contract::{
    BoundedDetail, FailureClass, FailureFact, FailureProjection, ResourceDimension, ResourceFacts,
    ResourceScope,
};
#[derive(Debug)]
enum AssertRowsFailure {
    Contract(BoundedDetail),
    Mismatch {
        detail: BoundedDetail,
    },
    DuplicateKey(BoundedDetail),
    CounterOverflow {
        current: u64,
        incoming: u64,
    },
    #[cfg(test)]
    KeyDisplay(arrow::error::ArrowError),
}
impl std::fmt::Display for AssertRowsFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(detail) | Self::DuplicateKey(detail) | Self::Mismatch { detail, .. } => {
                detail.fmt(f)
            }
            Self::CounterOverflow { current, incoming } => {
                write!(f, "assert row counter cannot add {incoming} to {current}")
            }
            #[cfg(test)]
            Self::KeyDisplay(source) => {
                write!(f, "display keyed assert_num_rows value failed: {source}")
            }
        }
    }
}
impl std::error::Error for AssertRowsFailure {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            #[cfg(test)]
            Self::KeyDisplay(source) => Some(source),
            _ => None,
        }
    }
}
impl FailureProjection for AssertRowsFailure {
    fn failure_fact(&self) -> FailureFact {
        let (class, reason) = match self {
            Self::Contract(_) => (FailureClass::Invariant, ExecutionReason::AssertRowsContract),
            Self::Mismatch { .. } => (FailureClass::Data, ExecutionReason::AssertRowsMismatch),
            Self::DuplicateKey(_) => (FailureClass::Data, ExecutionReason::AssertRowsDuplicateKey),
            Self::CounterOverflow { current, incoming } => {
                return FailureFact::new(
                    FailureClass::Resource,
                    ExecutionReason::AssertRowsCounter.reason(),
                )
                .with_resource(ResourceFacts {
                    request_scope: Some(ResourceScope::Operator),
                    constraint_scope: Some(ResourceScope::Operator),
                    dimension: ResourceDimension::Items,
                    requested: Some(*incoming),
                    required_growth: None,
                    limit: Some(u64::MAX),
                    live: Some(*current),
                    committed: None,
                });
            }
            #[cfg(test)]
            Self::KeyDisplay(arrow::error::ArrowError::MemoryError(_)) => (
                FailureClass::Resource,
                ExecutionReason::AssertRowsDisplayMemory,
            ),
            #[cfg(test)]
            Self::KeyDisplay(_) => (FailureClass::Invariant, ExecutionReason::AssertRowsDisplay),
        };
        FailureFact::new(class, reason.reason())
    }
}
fn assert_contract(detail: impl std::fmt::Display) -> ExecutionFailure {
    ExecutionFailure::from_domain(AssertRowsFailure::Contract(
        BoundedDetail::truncating_display(detail),
    ))
}

use crate::exec::hash_table::{
    allocation::{AggregateAllocator, AggregateVec},
    key_index::{
        IndexPurpose, KeyIndex, KeyInput, KeyMatch, KeySpec, NullPolicy, PreparedKeyBatch,
        hash_batch,
    },
    partition_keys::{prepare_canonical_tuple, source_key_owner},
};
use crate::exec::operators::compiled_expression::RuntimeKernelControl;
use crate::exec::pipeline::operator::{
    ConstructionFailure, InputConstructionFactory, InputConstructionOwner,
};
use crate::runtime::mem_tracker::MemTracker;
use crate::runtime::runtime_state::RuntimeState;
use allocator_api2::boxed::Box as AllocBox;
use arrow::array::{ArrayRef, BooleanArray};
use novarocks_functions::KernelEvaluationControl;
use novarocks_type_contract::FunctionValueType;
use novarocks_value_arrays::batch::hash::HashScratch;
use novarocks_value_arrays::reachability::ReachableRows;
use std::sync::Arc;

use arrow::array::Array;
#[cfg(test)]
use arrow::util::display::array_value_to_string;
use arrow::util::display::{ArrayFormatter, FormatOptions};

use crate::exec::chunk::Chunk;
use crate::exec::node::assert::{AssertNumRowsMode, Assertion};
use crate::exec::pipeline::operator::{InputEncodingPolicy, Operator};
use crate::exec::pipeline::operator_factory::OperatorFactory;
use crate::exec::pipeline::{
    input::PreparedInput,
    shape::{Transform, TransformKernel},
    step::{NextStep, StepResult},
    step_context::StepContext,
};
use novarocks_types::SlotId;

/// Factory for processors that enforce ASSERT NUM ROWS runtime constraints.
pub struct AssertNumRowsProcessorFactory {
    name: String,
    mode: AssertNumRowsMode,
    applicability: Option<SlotId>,
    key_types: Option<Arc<[FunctionValueType]>>,
}

impl AssertNumRowsProcessorFactory {
    pub fn new(node_id: i32, mode: AssertNumRowsMode) -> ExecutionResult<Self> {
        RuntimeAssertNumRowsMode::validate_plan_mode(&mode)?;

        let name = if node_id >= 0 {
            format!("AssertNumRows (id={node_id})")
        } else {
            "AssertNumRows".to_string()
        };
        Ok(Self {
            name,
            mode,
            applicability: None,
            key_types: None,
        })
    }
    pub fn with_key_types(mut self, key_types: Vec<FunctionValueType>) -> ExecutionResult<Self> {
        let AssertNumRowsMode::PerKeyAtMostOne { key_slots, .. } = &self.mode else {
            return Err(assert_contract(
                "frozen key types require keyed assertion mode",
            ));
        };
        if key_types.len() != key_slots.len() {
            return Err(assert_contract(
                "keyed assertion frozen key type arity differs from key slots",
            ));
        }
        self.key_types = Some(Arc::from(key_types));
        Ok(self)
    }

    /// Applicability is already materialized by the planner. FALSE and NULL
    /// bypass before any key value is inspected; TRUE requires a real key.
    pub fn new_with_applicability(
        node_id: i32,
        mode: AssertNumRowsMode,
        applicability: SlotId,
    ) -> ExecutionResult<Self> {
        if !matches!(mode, AssertNumRowsMode::PerKeyAtMostOne { .. }) {
            return Err(assert_contract(
                "applicability requires keyed assertion mode",
            ));
        }
        let mut factory = Self::new(node_id, mode)?;
        factory.applicability = Some(applicability);
        Ok(factory)
    }
}

impl OperatorFactory for AssertNumRowsProcessorFactory {
    fn name(&self) -> &str {
        &self.name
    }

    fn create(&self, _dop: i32, _driver_id: i32) -> Box<dyn Operator> {
        Box::new(Transform::hydrating(AssertNumRowsProcessorKernel {
            name: self.name.clone(),
            applicability: self.applicability,
            key_types: self.key_types.clone(),
            tracker: None,
            construction: None,
            control: None,
            mode: RuntimeAssertNumRowsMode::from_plan(&self.mode)
                .expect("validated assert_num_rows processor mode"),
        }))
    }
}

struct AssertNumRowsProcessorKernel {
    name: String,
    mode: RuntimeAssertNumRowsMode,
    applicability: Option<SlotId>,
    key_types: Option<Arc<[FunctionValueType]>>,
    tracker: Option<Arc<MemTracker>>,
    construction: Option<Arc<dyn InputConstructionFactory>>,
    control: Option<RuntimeKernelControl>,
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
enum KeyValue {
    Null,
    NonNull { data_type: String, display: String },
}

#[cfg(test)]
impl KeyValue {
    fn message_value(&self) -> String {
        match self {
            KeyValue::Null => "<NULL>".to_string(),
            KeyValue::NonNull { data_type, display } if is_string_key_type(data_type) => {
                format!("{display:?}")
            }
            KeyValue::NonNull { display, .. } => display.clone(),
        }
    }
}

enum RuntimeAssertNumRowsMode {
    Global {
        desired_num_rows: Option<u64>,
        assertion: Assertion,
        subquery_string: Option<String>,
        rows_seen: u64,
        done: bool,
    },
    PerKeyAtMostOne {
        key_slots: Vec<SlotId>,
        key_labels: Vec<String>,
        message_prefix: String,
        seen_keys: Option<AllocBox<TypedKeyState, AggregateAllocator>>,
    },
}

impl RuntimeAssertNumRowsMode {
    fn from_plan(mode: &AssertNumRowsMode) -> ExecutionResult<Self> {
        Self::validate_plan_mode(mode)?;

        Ok(match mode {
            AssertNumRowsMode::Global {
                desired_num_rows,
                assertion,
                subquery_string,
            } => Self::Global {
                desired_num_rows: desired_num_rows.map(|v| v as u64),
                assertion: assertion.clone(),
                subquery_string: subquery_string.clone(),
                rows_seen: 0,
                done: false,
            },
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots,
                key_labels,
                message_prefix,
            } => Self::PerKeyAtMostOne {
                key_slots: key_slots.clone(),
                key_labels: key_labels.clone(),
                message_prefix: message_prefix.clone(),
                seen_keys: None,
            },
        })
    }

    fn validate_plan_mode(mode: &AssertNumRowsMode) -> ExecutionResult<()> {
        if let AssertNumRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            ..
        } = mode
        {
            validate_keyed_invariants(key_slots, key_labels)?;
        }
        Ok(())
    }

    fn check_final(&mut self) -> ExecutionResult<()> {
        let Self::Global {
            desired_num_rows,
            assertion,
            subquery_string,
            rows_seen,
            done,
        } = self
        else {
            return Ok(());
        };

        if *done {
            return Ok(());
        }

        let Some(desired) = *desired_num_rows else {
            // No assertion configured, treat as no-op.
            *done = true;
            return Ok(());
        };
        let actual = *rows_seen;
        let ok = match *assertion {
            Assertion::Eq => actual == desired,
            Assertion::Ne => actual != desired,
            Assertion::Lt => actual < desired,
            Assertion::Le => actual <= desired,
            Assertion::Gt => actual > desired,
            Assertion::Ge => actual >= desired,
        };
        if ok {
            *done = true;
            return Ok(());
        }

        let op_str = match *assertion {
            Assertion::Eq => "=",
            Assertion::Ne => "!=",
            Assertion::Lt => "<",
            Assertion::Le => "<=",
            Assertion::Gt => ">",
            Assertion::Ge => ">=",
        };
        let base = format!(
            "assert_num_rows failed: actual={} row(s), expected {} {} row(s)",
            actual, op_str, desired
        );
        let msg = if let Some(sql) = subquery_string.as_ref() {
            format!("subquery '{}' {}", sql, base)
        } else {
            base
        };
        Err(ExecutionFailure::from_domain(AssertRowsFailure::Mismatch {
            detail: BoundedDetail::truncating(&msg),
        }))
    }

    fn observe_chunk(&mut self, chunk: &Chunk) -> ExecutionResult<()> {
        match self {
            Self::Global {
                desired_num_rows,
                assertion,
                subquery_string,
                rows_seen,
                done,
            } => {
                if *done {
                    return Ok(());
                }

                let rows = chunk.len() as u64;
                if rows == 0 {
                    return Ok(());
                }

                *rows_seen = rows_seen.checked_add(rows).ok_or_else(|| {
                    ExecutionFailure::from_domain(AssertRowsFailure::CounterOverflow {
                        current: *rows_seen,
                        incoming: rows,
                    })
                })?;
                Self::maybe_early_fail(
                    *desired_num_rows,
                    assertion,
                    subquery_string.as_ref(),
                    *rows_seen,
                )
            }
            Self::PerKeyAtMostOne { .. } => Err(assert_contract(
                "keyed assertion requires its installed state owner",
            )),
        }
    }

    fn maybe_early_fail(
        desired_num_rows: Option<u64>,
        assertion: &Assertion,
        subquery_string: Option<&String>,
        rows_seen: u64,
    ) -> ExecutionResult<()> {
        let Some(desired) = desired_num_rows else {
            return Ok(());
        };
        let actual = rows_seen;

        let must_fail = match assertion {
            Assertion::Eq | Assertion::Le => actual > desired,
            Assertion::Lt => actual >= desired,
            // For these, we can't be sure until EOS.
            Assertion::Ne | Assertion::Gt | Assertion::Ge => false,
        };

        if !must_fail {
            return Ok(());
        }

        let op_str = match assertion {
            Assertion::Eq => "=",
            Assertion::Ne => "!=",
            Assertion::Lt => "<",
            Assertion::Le => "<=",
            Assertion::Gt => ">",
            Assertion::Ge => ">=",
        };
        let base = format!(
            "assert_num_rows failed (early): actual={} row(s), expected {} {} row(s)",
            actual, op_str, desired
        );
        let msg = if let Some(sql) = subquery_string {
            format!("subquery '{}' {}", sql, base)
        } else {
            base
        };
        Err(ExecutionFailure::from_domain(AssertRowsFailure::Mismatch {
            detail: BoundedDetail::truncating(&msg),
        }))
    }
}

#[cfg(test)]
fn key_value(column: &dyn Array, row: usize) -> ExecutionResult<KeyValue> {
    if row >= column.len() {
        return Err(assert_contract(format_args!(
            "keyed assert_num_rows row {} exceeds key column length {}",
            row,
            column.len()
        )));
    }
    if column.is_null(row) {
        return Ok(KeyValue::Null);
    }
    let display = array_value_to_string(column, row)
        .map_err(|source| ExecutionFailure::from_domain(AssertRowsFailure::KeyDisplay(source)))?;
    Ok(KeyValue::NonNull {
        data_type: format!("{:?}", column.data_type()),
        display,
    })
}

fn validate_keyed_invariants(key_slots: &[SlotId], key_labels: &[String]) -> ExecutionResult<()> {
    if key_slots.is_empty() {
        return Err(assert_contract(
            "keyed assert_num_rows requires at least one key slot",
        ));
    }
    if key_labels.len() != key_slots.len() {
        return Err(assert_contract(format_args!(
            "keyed assert_num_rows key_labels length mismatch: key_slots={} labels={}",
            key_slots.len(),
            key_labels.len()
        )));
    }
    Ok(())
}

#[cfg(test)]
fn format_key_message(labels: &[String], key: &[KeyValue]) -> ExecutionResult<String> {
    if labels.len() != key.len() {
        return Err(assert_contract(format_args!(
            "keyed assert_num_rows key_labels length mismatch: key_values={} labels={}",
            key.len(),
            labels.len()
        )));
    }

    Ok(key
        .iter()
        .enumerate()
        .map(|(idx, value)| {
            let label = labels[idx].as_str();
            format!("{}={}", label, value.message_value())
        })
        .collect::<Vec<_>>()
        .join(", "))
}

#[cfg(test)]
fn is_string_key_type(data_type: &str) -> bool {
    matches!(data_type, "Utf8" | "LargeUtf8" | "Utf8View")
}

struct DuplicateKeyDisplay<'a, 'input> {
    prefix: &'a str,
    labels: &'a [String],
    columns: &'a [ArrayRef],
    tuples: Option<&'a crate::exec::hash_table::partition_keys::CanonicalTupleBatch<'input>>,
    row: usize,
}
impl std::fmt::Display for DuplicateKeyDisplay<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: duplicate ", self.prefix)?;
        for (index, column) in self.columns.iter().enumerate() {
            if index != 0 {
                f.write_str(", ")?;
            }
            write!(f, "{}=", self.labels[index])?;
            let Some(tuples) = self.tuples else {
                f.write_str("<unavailable>")?;
                continue;
            };
            match tuples.string_component(index, self.row) {
                Ok(Some(Some(value))) => write!(f, "{value:?}")?,
                Ok(Some(None)) => f.write_str("<NULL>")?,
                Ok(None) if column.is_null(self.row) => f.write_str("<NULL>")?,
                Ok(None) => {
                    // Arrow's byte formatter writes one hex byte at a time;
                    // nested formatters forward the same bounded writer.
                    let options = FormatOptions::default();
                    match ArrayFormatter::try_new(column.as_ref(), &options) {
                        Ok(formatter) => write!(f, "{}", formatter.value(self.row))?,
                        Err(_) => f.write_str("<unavailable>")?,
                    }
                }
                Err(_) => f.write_str("<unavailable>")?,
            }
        }
        Ok(())
    }
}

struct TypedKeyState {
    index: KeyIndex,
}
impl AssertNumRowsProcessorKernel {
    fn clear_state(&mut self) {
        if let RuntimeAssertNumRowsMode::PerKeyAtMostOne { seen_keys, .. } = &mut self.mode {
            *seen_keys = None;
        }
    }
    fn observe_keyed(&mut self, chunk: &Chunk, ctx: &mut StepContext<'_>) -> ExecutionResult<()> {
        let RuntimeAssertNumRowsMode::PerKeyAtMostOne {
            key_slots,
            key_labels,
            message_prefix,
            seen_keys,
        } = &mut self.mode
        else {
            return self.mode.observe_chunk(chunk);
        };
        ctx.checkpoint()?;
        let frozen = self
            .key_types
            .as_deref()
            .ok_or_else(|| assert_contract("keyed assertion frozen key types are missing"))?;
        let factory = self
            .construction
            .as_deref()
            .ok_or(ConstructionFailure::MissingOwner)?;
        let original_owner = factory.begin_input_construction(chunk)?;
        let owner = source_key_owner(chunk, original_owner.as_ref())?;
        let tracker = self
            .tracker
            .clone()
            .ok_or_else(|| assert_contract("keyed assertion tracker is not installed"))?;
        let allocator = AggregateAllocator::new(tracker.clone());
        let kernel_control = self
            .control
            .as_ref()
            .ok_or_else(|| assert_contract("keyed assertion runtime control is not installed"))?;
        owner.request(
            std::alloc::Layout::array::<usize>(chunk.len()).map_err(|_| {
                ConstructionFailure::from(
            novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic)
            })?,
        )?;
        let mut selected = Vec::new();
        selected.try_reserve_exact(chunk.len()).map_err(|source| {
            allocator.allocation_error("reserve assertion applicability rows", source)
        })?;
        let applies = self
            .applicability
            .map(|slot| chunk.column_by_slot_id(slot))
            .transpose()?;
        let applies = applies
            .as_ref()
            .map(|array| {
                array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| assert_contract("keyed assertion applicability must be Boolean"))
            })
            .transpose()?;
        for row in 0..chunk.len() {
            if row.is_multiple_of(256) {
                ctx.checkpoint()?;
            }
            if applies.is_none_or(|mask| !mask.is_null(row) && mask.value(row)) {
                selected.push(row);
            }
        }
        if selected.is_empty() {
            return Ok(());
        }
        owner.request(std::alloc::Layout::array::<ArrayRef>(key_slots.len()).map_err(|_| ConstructionFailure::from(novarocks_type_contract::owned_resources::layout::LayoutResourceError::Arithmetic))?)?;
        let mut columns = Vec::new();
        let mut types = AggregateVec::new_in(allocator.clone());
        columns
            .try_reserve_exact(key_slots.len())
            .map_err(|source| {
                allocator.allocation_error("reserve assertion key columns", source)
            })?;
        types
            .try_reserve_exact(key_slots.len())
            .map_err(|source| allocator.allocation_error("reserve assertion key types", source))?;
        for (slot, expected) in key_slots.iter().zip(frozen) {
            columns.push(chunk.column_by_slot_id(*slot)?);
            let signed = chunk
                .chunk_schema()
                .slot(*slot)
                .ok_or_else(|| assert_contract("keyed assertion slot declaration is missing"))?;
            if signed.field_schema().value_type() != expected.value_type() {
                return Err(assert_contract(
                    "keyed assertion source semantics differ from frozen key type",
                ));
            }
            types.push(expected.clone());
        }
        if let Some(state) = seen_keys.as_ref()
            && state.index.spec().types() != types.as_slice()
        {
            return Err(assert_contract(
                "keyed assertion exact key types changed between batches",
            ));
        }
        let (_, conversions) = ctx.value_conversion_scope();
        let checkpoint = || kernel_control.checkpoint(0).map_err(ExecutionFailure::from);
        let result = (|| {
            if seen_keys.is_none() {
                let policy = if self.applicability.is_some() {
                    NullPolicy::Unmatchable
                } else {
                    NullPolicy::Equal
                };
                let mut nulls = AggregateVec::new_in(allocator.clone());
                nulls.try_reserve_exact(types.len()).map_err(|source| {
                    allocator.allocation_error("reserve assertion NULL policy", source)
                })?;
                for _ in &types {
                    nulls.push(policy);
                }
                let spec = KeySpec::new(&types, &nulls, original_owner.as_ref(), &checkpoint)?;
                let index =
                    KeyIndex::new(spec, IndexPurpose::KeyIds, tracker, original_owner.as_ref())?;
                *seen_keys = Some(
                    AllocBox::try_new_in(TypedKeyState { index }, allocator.clone()).map_err(
                        |source| {
                            allocator.allocation_error("reserve assertion key state",
                        novarocks_functions::allocation_failure::AllocatorRequestRefusal::new(
                            std::alloc::Layout::new::<TypedKeyState>(), source))
                        },
                    )?,
                );
            }
            let state = seen_keys.as_mut().expect("original key state installed");
            let readable = ReachableRows::indices(chunk.len(), &selected)
                .materialize_observed(
                    &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                    &mut |_| checkpoint(),
                )
                .map_err(|cause| match cause {
                    novarocks_value_arrays::ValueFailure::Value(cause) => {
                        ExecutionFailure::from_domain(cause)
                    }
                    novarocks_value_arrays::ValueFailure::Caller(cause) => cause,
                })?;
            let input = KeyInput::new_for_spec(
                state.index.spec(),
                &columns,
                &types,
                &readable,
                &selected,
                conversions,
                owner.as_ref(),
                &checkpoint,
            )?;
            let prepared = PreparedKeyBatch::from_captured(
                state.index.spec(),
                &input,
                allocator.clone(),
                &checkpoint,
            )?;
            let hashes = hash_batch(
                &prepared,
                state.index.policy(),
                &mut HashScratch::new(),
                original_owner.as_ref(),
                &checkpoint,
            )?;
            if self.applicability.is_some() {
                for occurrence in 0..input.len() {
                    checkpoint()?;
                    if !hashes.participates(occurrence)? {
                        return Err(assert_contract(
                            "applicable keyed assertion row has a NULL target key",
                        ));
                    }
                }
            }
            let interned = state.index.intern_batch(
                &prepared,
                &hashes,
                original_owner.as_ref(),
                kernel_control,
            )?;
            let mut new_keys = interned.new_keys().iter().peekable();
            for (occurrence, found) in interned.matches().iter().enumerate() {
                checkpoint()?;
                if new_keys
                    .peek()
                    .is_some_and(|key| key.occurrence == occurrence)
                {
                    new_keys.next();
                    continue;
                }
                let KeyMatch::Found(_) = found else {
                    return Err(assert_contract(
                        "selected assertion key did not receive an equality class",
                    ));
                };
                let row = input.row(occurrence)?;
                // Only an already-proven duplicate needs display projection.
                // It uses the existing canonical diagnostic author over one
                // selected row, never a second lookup/hash/encoded key table.
                // Display refusal cannot replace the duplicate Data fact.
                let diagnostic_rows = [row];
                let tuples = prepare_canonical_tuple(
                    &columns,
                    &types,
                    &ReachableRows::indices(chunk.len(), &diagnostic_rows),
                    conversions,
                    owner.as_ref(),
                    &checkpoint,
                )
                .ok();
                return Err(ExecutionFailure::from_domain(
                    AssertRowsFailure::DuplicateKey(BoundedDetail::truncating_display(
                        DuplicateKeyDisplay {
                            prefix: message_prefix,
                            labels: key_labels,
                            columns: &columns,
                            tuples: tuples.as_ref(),
                            row,
                        },
                    )),
                ));
            }
            Ok(())
        })();
        result.map_err(|cause| kernel_control.project_required_failure(cause))
    }
}

impl OperatorLifecycle for AssertNumRowsProcessorKernel {
    fn bind_runtime_state(&mut self, state: &RuntimeState) -> ExecutionResult<()> {
        self.control = Some(RuntimeKernelControl::new(state.error_state()));
        Ok(())
    }
    fn prepare(&mut self) -> ExecutionResult<()> {
        if matches!(self.mode, RuntimeAssertNumRowsMode::PerKeyAtMostOne { .. })
            && self.key_types.is_none()
        {
            return Err(assert_contract(
                "keyed assertion frozen key types are missing",
            ));
        }
        Ok(())
    }

    fn creates_typed_storage(&self) -> bool {
        matches!(self.mode, RuntimeAssertNumRowsMode::PerKeyAtMostOne { .. })
    }
    fn set_mem_tracker(&mut self, tracker: Arc<MemTracker>) {
        self.tracker = Some(tracker);
    }
    fn set_construction_factory(&mut self, factory: Arc<dyn InputConstructionFactory>) {
        self.construction = Some(factory);
    }
    fn begin_exit(&mut self) -> ExecutionResult<()> {
        self.clear_state();
        Ok(())
    }
    fn close(&mut self) -> ExecutionResult<()> {
        self.clear_state();
        Ok(())
    }
    fn cancel(&mut self) {
        self.clear_state();
    }
    fn name(&self) -> &str {
        &self.name
    }
}
impl InputEncodingPolicy for AssertNumRowsProcessorKernel {
    fn retains_encoded_path(
        &self,
        _: crate::exec::pipeline::operator::EncodedValueOccurrence<'_>,
    ) -> bool {
        true
    }
}
impl TransformKernel for AssertNumRowsProcessorKernel {
    fn apply(
        &mut self,
        ctx: &mut StepContext<'_>,
        input: PreparedInput,
    ) -> ExecutionResult<StepResult> {
        let chunk = input.into_uncovered_chunk()?;
        let rows = u64::try_from(chunk.len()).expect("supported row count fits u64");
        self.observe_keyed(&chunk, ctx)?;
        Ok(StepResult::new(Some(chunk), NextStep::NeedInput, rows))
    }
    fn finish(&mut self, _: &mut StepContext<'_>) -> ExecutionResult<StepResult> {
        self.mode.check_final()?;
        Ok(StepResult::new(None, NextStep::Finished, 0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exec::pipeline::test_driver::TestDriver;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    use crate::runtime::runtime_state::RuntimeState;
    use novarocks_types::SlotId;

    fn make_key_chunk(values: Vec<i32>) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "_row_id",
            DataType::Int32,
            true,
        )]));
        let array = Arc::new(Int32Array::from(values)) as _;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        crate::exec::chunk::typed_fixture::chunk(
            batch,
            &[SlotId::new(7)],
            &[FunctionValueType::new(DataType::Int32, true)],
        )
    }

    fn make_chunk(rows: usize) -> Chunk {
        let schema = Arc::new(Schema::new(vec![Field::new("c1", DataType::Int32, true)]));
        let data: Vec<i32> = (0..rows as i32).collect();
        let array = Arc::new(Int32Array::from(data)) as _;
        let batch = RecordBatch::try_new(schema, vec![array]).expect("record batch");
        {
            let batch = batch;
            let chunk_schema = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(
                batch.schema().as_ref(),
                &[SlotId::new(1)],
            )
            .expect("chunk schema");
            Chunk::new_with_chunk_schema(batch, chunk_schema)
        }
    }

    fn run_ok(
        desired: Option<usize>,
        assertion: Assertion,
        chunks: &[usize],
    ) -> ExecutionResult<()> {
        let rt = RuntimeState::default();
        let mut op = Transform::hydrating(AssertNumRowsProcessorKernel {
            name: "test".to_string(),
            applicability: None,
            key_types: None,
            tracker: None,
            construction: None,
            control: None,
            mode: RuntimeAssertNumRowsMode::from_plan(&AssertNumRowsMode::Global {
                desired_num_rows: desired,
                assertion,
                subquery_string: Some("select c1 from test".to_string()),
            })
            .expect("runtime mode"),
        });
        let mut calls = TestDriver::new(&mut op, &rt);
        for &n in chunks {
            let _ = calls.input_and_collect(make_chunk(n))?;
        }
        calls.signal_eos()?;
        let _ = calls.collect_until_finished()?;
        Ok(())
    }

    fn run_err(
        desired: Option<usize>,
        assertion: Assertion,
        chunks: &[usize],
    ) -> crate::runtime::fragment::ExecutionFailure {
        let rt = RuntimeState::default();
        let mut op = Transform::hydrating(AssertNumRowsProcessorKernel {
            name: "test".to_string(),
            applicability: None,
            key_types: None,
            tracker: None,
            construction: None,
            control: None,
            mode: RuntimeAssertNumRowsMode::from_plan(&AssertNumRowsMode::Global {
                desired_num_rows: desired,
                assertion,
                subquery_string: Some("select c1 from test".to_string()),
            })
            .expect("runtime mode"),
        });
        let mut calls = TestDriver::new(&mut op, &rt);
        for &n in chunks {
            if let Err(error) = calls.input_and_collect(make_chunk(n)) {
                return error;
            }
        }
        if let Err(error) = calls.signal_eos() {
            return error;
        }
        calls
            .collect_until_finished()
            .expect_err("the asserted row constraint must fail")
    }

    #[test]
    fn assert_eq_pass_and_fail() {
        // desired = 1, actual = 1 -> ok
        run_ok(Some(1), Assertion::Eq, &[1]).expect("eq pass");

        // desired = 1, actual = 2 -> early fail
        let msg = run_err(Some(1), Assertion::Eq, &[1, 1]);
        assert!(msg.detail().contains("assert_num_rows failed"));
        assert!(msg.detail().contains("expected = 1 row(s)"));
    }

    #[test]
    fn assert_eq_early_error_matches_existing_text() {
        let msg = run_err(Some(1), Assertion::Eq, &[2]);
        assert_eq!(
            msg.detail(),
            "subquery 'select c1 from test' assert_num_rows failed (early): actual=2 row(s), expected = 1 row(s)"
        );
    }

    #[test]
    fn assert_le_early_fail() {
        // desired <= 2, actual = 2 -> ok
        run_ok(Some(2), Assertion::Le, &[1, 1]).expect("le pass");

        // desired <= 2, actual = 3 -> early fail
        let msg = run_err(Some(2), Assertion::Le, &[2, 1]);
        assert!(msg.detail().contains("failed (early)"));
    }

    #[test]
    fn assert_lt_and_ge_finalize() {
        // LT: desired = 2, actual = 1 -> ok
        run_ok(Some(2), Assertion::Lt, &[1]).expect("lt pass");

        // LT: desired = 2, actual = 2 -> early fail
        let msg = run_err(Some(2), Assertion::Lt, &[1, 1]);
        assert!(msg.detail().contains("failed (early)"));

        // GE: desired >= 2, actual = 2 -> ok, checked at finish()
        run_ok(Some(2), Assertion::Ge, &[1, 1]).expect("ge pass");
    }

    #[test]
    fn assert_ge_final_error_matches_existing_text() {
        let msg = run_err(Some(2), Assertion::Ge, &[1]);
        assert_eq!(
            msg.detail(),
            "subquery 'select c1 from test' assert_num_rows failed: actual=1 row(s), expected >= 2 row(s)"
        );
    }

    #[test]
    fn keyed_assert_num_rows_factory_rejects_empty_key_slots() {
        let err = match AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![],
                key_labels: vec![],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        ) {
            Ok(_) => panic!("empty keyed slots should fail"),
            Err(err) => err,
        };
        assert_eq!(
            err.detail(),
            "keyed assert_num_rows requires at least one key slot"
        );
    }

    #[test]
    fn keyed_assert_num_rows_factory_rejects_key_label_mismatch() {
        let err = match AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![SlotId::new(7)],
                key_labels: vec![],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        ) {
            Ok(_) => panic!("key label mismatch should fail"),
            Err(err) => err,
        };
        assert_eq!(
            err.detail(),
            "keyed assert_num_rows key_labels length mismatch: key_slots=1 labels=0"
        );
    }

    #[test]
    fn keyed_assert_num_rows_fails_on_second_key() {
        let rt = RuntimeState::default();
        let mut op = AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![SlotId::new(7)],
                key_labels: vec!["_row_id".to_string()],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        )
        .expect("factory")
        .with_key_types(vec![FunctionValueType::new(DataType::Int32, true)])
        .unwrap()
        .create(1, 0);
        let tracker = MemTracker::new_root("keyed assertion fixture");
        op.set_mem_tracker(tracker.clone());
        op.set_construction_factory(crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker, rt.error_state()).unwrap());
        op.prepare().expect("prepare keyed assertion fixture");
        op.bind_runtime_state(&rt)
            .expect("bind original keyed assertion runtime");
        let mut op = TestDriver::new(op.as_mut(), &rt);

        let err = op
            .input_and_collect(make_key_chunk(vec![7, 7]))
            .expect_err("duplicate key should fail");
        assert_eq!(err.detail(), "assert_num_rows failed: duplicate _row_id=7");
    }

    #[test]
    fn keyed_assert_num_rows_fails_on_duplicate_key_across_chunks() {
        let rt = RuntimeState::default();
        let mut op = AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![SlotId::new(7)],
                key_labels: vec!["_row_id".to_string()],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        )
        .expect("factory")
        .with_key_types(vec![FunctionValueType::new(DataType::Int32, true)])
        .unwrap()
        .create(1, 0);
        let tracker = MemTracker::new_root("keyed assertion fixture");
        op.set_mem_tracker(tracker.clone());
        op.set_construction_factory(crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker, rt.error_state()).unwrap());
        op.prepare().expect("prepare keyed assertion fixture");
        op.bind_runtime_state(&rt)
            .expect("bind original keyed assertion runtime");
        let mut op = TestDriver::new(op.as_mut(), &rt);

        let output = op
            .input_and_collect(make_key_chunk(vec![7]))
            .expect("first key");
        assert_eq!(output.len(), 1);

        let err = op
            .input_and_collect(make_key_chunk(vec![7]))
            .expect_err("duplicate key across chunks should fail");
        assert_eq!(err.detail(), "assert_num_rows failed: duplicate _row_id=7");
    }

    #[test]
    fn keyed_assert_num_rows_allows_distinct_keys_across_chunks() {
        let rt = RuntimeState::default();
        let mut op = AssertNumRowsProcessorFactory::new(
            11,
            AssertNumRowsMode::PerKeyAtMostOne {
                key_slots: vec![SlotId::new(7)],
                key_labels: vec!["_row_id".to_string()],
                message_prefix: "assert_num_rows failed".to_string(),
            },
        )
        .expect("factory")
        .with_key_types(vec![FunctionValueType::new(DataType::Int32, true)])
        .unwrap()
        .create(1, 0);
        let tracker = MemTracker::new_root("keyed assertion fixture");
        op.set_mem_tracker(tracker.clone());
        op.set_construction_factory(crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker, rt.error_state()).unwrap());
        op.prepare().expect("prepare keyed assertion fixture");
        op.bind_runtime_state(&rt)
            .expect("bind original keyed assertion runtime");
        let mut op = TestDriver::new(op.as_mut(), &rt);

        let output = op
            .input_and_collect(make_key_chunk(vec![7]))
            .expect("first key");
        assert_eq!(output.len(), 1);
        let output = op
            .input_and_collect(make_key_chunk(vec![8]))
            .expect("second key");
        assert_eq!(output.len(), 1);
        op.signal_eos().expect("signal EOS");
        assert!(
            op.collect_until_finished()
                .expect("final constraint")
                .is_empty()
        );
    }

    #[test]
    fn keyed_assert_num_rows_formats_sql_null_and_string_null_distinctly() {
        let labels = vec!["_row_id".to_string()];
        let values = arrow::array::StringArray::from(vec![None, Some("NULL")]);
        let sql_null = vec![key_value(&values, 0).unwrap()];
        let string_null = vec![key_value(&values, 1).unwrap()];

        assert_eq!(
            format_key_message(&labels, &sql_null).expect("format sql null"),
            "_row_id=<NULL>"
        );
        assert_eq!(
            format_key_message(&labels, &string_null).expect("format string null"),
            "_row_id=\"NULL\""
        );
    }

    #[test]
    fn keyed_assert_num_rows_format_key_message_rejects_label_mismatch() {
        let err = format_key_message(&[], &[KeyValue::Null]).expect_err("label mismatch");
        assert_eq!(
            err.detail(),
            "keyed assert_num_rows key_labels length mismatch: key_values=1 labels=0"
        );
    }
}

#[cfg(test)]
mod failure_fact_tests {
    use super::*;
    use arrow::{array::RecordBatch, datatypes::Schema};
    use std::sync::Arc;

    fn one_row() -> Chunk {
        let schema = Arc::new(Schema::empty());
        let options = arrow::array::RecordBatchOptions::new().with_row_count(Some(1));
        let batch = RecordBatch::try_new_with_options(schema.clone(), vec![], &options).unwrap();
        let shape = crate::exec::chunk::ChunkSchema::try_ref_from_schema_and_slot_ids(&schema, &[])
            .unwrap();
        Chunk::try_new_with_chunk_schema(batch, shape).unwrap()
    }

    #[test]
    fn actual_row_constraint_and_counter_refusal_have_different_domains_and_atomic_state() {
        let mut mode = RuntimeAssertNumRowsMode::from_plan(&AssertNumRowsMode::Global {
            desired_num_rows: Some(0),
            assertion: Assertion::Eq,
            subquery_string: Some("ResourceExhausted".into()),
        })
        .unwrap();
        let error = mode.observe_chunk(&one_row()).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Data);
        assert_eq!(fact.reason(), ExecutionReason::AssertRowsMismatch.reason());
        assert_eq!(fact.resource(), None);
        let mut mode = RuntimeAssertNumRowsMode::Global {
            desired_num_rows: Some(u64::MAX),
            assertion: Assertion::Ge,
            subquery_string: None,
            rows_seen: u64::MAX,
            done: false,
        };
        let error = mode.observe_chunk(&one_row()).unwrap_err();
        let fact = error.failure_fact();
        assert_eq!(fact.class(), FailureClass::Resource);
        assert_eq!(fact.reason(), ExecutionReason::AssertRowsCounter.reason());
        let resource = fact.resource().unwrap();
        assert_eq!(resource.requested, Some(1));
        assert_eq!(resource.live, Some(u64::MAX));
        assert_eq!(resource.limit, Some(u64::MAX));
        assert!(matches!(
            mode,
            RuntimeAssertNumRowsMode::Global {
                rows_seen: u64::MAX,
                done: false,
                ..
            }
        ));
    }
}

#[cfg(test)]
mod typed_state_tests {
    use super::*;
    use crate::exec::pipeline::test_driver::TestDriver;
    use crate::runtime::runtime_state::RuntimeState;
    use arrow::{
        array::{BooleanArray, Int32Array},
        datatypes::Field,
    };

    fn chunk(columns: Vec<ArrayRef>, types: &[FunctionValueType]) -> Chunk {
        let names = (0..types.len())
            .map(|index| format!("c{index}"))
            .collect::<Vec<_>>();
        let names = names.iter().map(String::as_str).collect::<Vec<_>>();
        let slots = (0..types.len())
            .map(|i| SlotId::new(i as u32 + 1))
            .collect::<Vec<_>>();
        let rows = columns.first().map_or(0, |column| column.len());
        crate::exec::chunk::typed_fixture::columns(&names, &slots, types, columns, rows)
    }
    fn operator(
        rt: &RuntimeState,
        tracker: Arc<MemTracker>,
        conditional: bool,
    ) -> Box<dyn Operator> {
        operator_typed(
            rt,
            tracker,
            conditional,
            FunctionValueType::new(arrow::datatypes::DataType::Int32, true),
        )
    }
    fn operator_typed(
        rt: &RuntimeState,
        tracker: Arc<MemTracker>,
        conditional: bool,
        key_type: FunctionValueType,
    ) -> Box<dyn Operator> {
        let mode = AssertNumRowsMode::PerKeyAtMostOne {
            key_slots: vec![SlotId::new(1)],
            key_labels: vec!["target".into()],
            message_prefix: "duplicate target".into(),
        };
        let factory = if conditional {
            AssertNumRowsProcessorFactory::new_with_applicability(1, mode, SlotId::new(2))
        } else {
            AssertNumRowsProcessorFactory::new(1, mode)
        }
        .unwrap();
        let mut op = factory.with_key_types(vec![key_type]).unwrap().create(1, 0);
        op.set_mem_tracker(tracker.clone());
        op.set_construction_factory(crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(), tracker, rt.error_state()).unwrap());
        op.prepare().expect("prepare keyed assertion fixture");
        op.bind_runtime_state(rt)
            .expect("bind original keyed assertion runtime");
        op
    }
    fn conditional(keys: Vec<Option<i32>>, mask: Vec<Option<bool>>) -> Chunk {
        chunk(
            vec![
                Arc::new(Int32Array::from(keys)),
                Arc::new(BooleanArray::from(mask)),
            ],
            &[
                FunctionValueType::new(arrow::datatypes::DataType::Int32, true),
                FunctionValueType::new(arrow::datatypes::DataType::Boolean, true),
            ],
        )
    }
    #[test]
    fn materialized_applicability_bypasses_false_and_null_before_key_identity() {
        for mask in [
            vec![Some(true), Some(false)],
            vec![Some(true), None],
            vec![Some(false), None],
        ] {
            let rt = RuntimeState::default();
            let tracker = MemTracker::new_root("applicability");
            let mut op = operator(&rt, tracker, true);
            let mut calls = TestDriver::new(op.as_mut(), &rt);
            calls
                .input_and_collect(conditional(vec![Some(9), Some(9)], mask))
                .unwrap();
        }
        let rt = RuntimeState::default();
        let tracker = MemTracker::new_root("duplicate applicable");
        let mut op = operator(&rt, tracker, true);
        let mut calls = TestDriver::new(op.as_mut(), &rt);
        let error = calls
            .input_and_collect(conditional(
                vec![Some(9), Some(9)],
                vec![Some(true), Some(true)],
            ))
            .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Data);
        assert_eq!(
            error.failure_fact().reason(),
            ExecutionReason::AssertRowsDuplicateKey.reason()
        );
    }
    #[test]
    fn applicable_null_is_contract_failure_and_bypassed_null_allocates_no_state() {
        let rt = RuntimeState::default();
        let tracker = MemTracker::new_root("null applicability");
        let mut op = operator(&rt, tracker.clone(), true);
        let before = tracker.current();
        {
            let mut calls = TestDriver::new(op.as_mut(), &rt);
            calls
                .input_and_collect(conditional(vec![None, None], vec![Some(false), None]))
                .unwrap();
        }
        assert_eq!(tracker.current(), before);
        let mut calls = TestDriver::new(op.as_mut(), &rt);
        let error = calls
            .input_and_collect(conditional(vec![None], vec![Some(true)]))
            .unwrap_err();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
    }
    #[test]
    fn equal_display_text_does_not_define_nested_typed_identity() {
        use arrow::array::ListBuilder;
        use arrow::array::StringBuilder;
        let mut builder = ListBuilder::new(StringBuilder::new());
        for values in [["a, b", "c"], ["a", "b, c"]] {
            for value in values {
                builder.values().append_value(value);
            }
            builder.append(true);
        }
        let array = Arc::new(builder.finish()) as ArrayRef;
        assert_eq!(
            array_value_to_string(array.as_ref(), 0).unwrap(),
            array_value_to_string(array.as_ref(), 1).unwrap()
        );
        let rt = RuntimeState::default();
        let tracker = MemTracker::new_root("display collision");
        let frozen = FunctionValueType::new(
            arrow::datatypes::DataType::List(Arc::new(Field::new(
                "item",
                arrow::datatypes::DataType::Utf8,
                true,
            ))),
            true,
        );
        let mut op = operator_typed(&rt, tracker, false, frozen.clone());
        let mut calls = TestDriver::new(op.as_mut(), &rt);
        assert_eq!(
            calls
                .input_and_collect(chunk(vec![array], &[frozen]))
                .unwrap()[0]
                .len(),
            2
        );
    }
    #[test]
    fn installed_parent_limit_refuses_state_and_cancel_releases_retained_layouts() {
        let rt = RuntimeState::default();
        let parent = MemTracker::new_root("keyed state parent");
        let tracker = MemTracker::new_child("assertion operator", &parent);
        let mut op = operator(&rt, tracker.clone(), false);
        let baseline = tracker.current();
        {
            let mut calls = TestDriver::new(op.as_mut(), &rt);
            let output = calls
                .input_and_collect(chunk(
                    vec![Arc::new(Int32Array::from(vec![0]))],
                    &[FunctionValueType::new(
                        arrow::datatypes::DataType::Int32,
                        true,
                    )],
                ))
                .expect("seed one retained key before installing the parent limit");
            assert_eq!(output.len(), 1);
        }
        assert!(tracker.current() > baseline);
        parent
            .install_limit_once(
                parent
                    .current()
                    .checked_add(1)
                    .expect("fixture limit extent"),
            )
            .unwrap();
        let error = {
            let mut calls = TestDriver::new(op.as_mut(), &rt);
            calls
                .input_and_collect(chunk(
                    vec![Arc::new(Int32Array::from(vec![1]))],
                    &[FunctionValueType::new(
                        arrow::datatypes::DataType::Int32,
                        true,
                    )],
                ))
                .expect_err("next distinct key must meet the measured parent limit")
        };
        assert_eq!(error.failure_fact().class(), FailureClass::Resource);
        op.cancel();
        op.close().unwrap();
        drop(op);
        assert_eq!(tracker.current(), 0);
        assert_eq!(parent.current(), 0);
    }
    #[test]
    fn frozen_logical_string_layout_accepts_actual_dictionary_and_detects_cross_batch_identity() {
        use arrow::{
            array::{DictionaryArray, StringArray},
            datatypes::{DataType, Int32Type},
        };
        let rt = RuntimeState::default();
        let tracker = MemTracker::new_root("frozen string identity");
        // Utf8View and Utf8 share D(L)=Utf8. The consumer receives L itself;
        // it never guesses that declaration by looking at dictionary values.
        let frozen = FunctionValueType::new(DataType::Utf8View, true);
        let mut op = operator_typed(&rt, tracker, false, frozen.clone());
        let mut calls = TestDriver::new(op.as_mut(), &rt);
        let first = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![0, 1]),
                Arc::new(StringArray::from(vec!["alpha", "beta"])) as ArrayRef,
            )
            .unwrap(),
        ) as ArrayRef;
        let first = crate::exec::hash_table::partition_keys::typed_encoded_tuple_fixture(
            &["target"],
            &[SlotId::new(1)],
            std::slice::from_ref(&frozen),
            vec![first],
            2,
        );
        assert_eq!(calls.input_and_collect(first).unwrap()[0].len(), 2);
        let next = Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                Int32Array::from(vec![1]),
                Arc::new(StringArray::from(vec!["other", "alpha"])) as ArrayRef,
            )
            .unwrap(),
        ) as ArrayRef;
        let next = crate::exec::hash_table::partition_keys::typed_encoded_tuple_fixture(
            &["target"],
            &[SlotId::new(1)],
            &[frozen],
            vec![next],
            1,
        );
        let error = calls.input_and_collect(next).unwrap_err();
        assert_eq!(
            error.failure_fact().reason(),
            ExecutionReason::AssertRowsDuplicateKey.reason()
        );
    }
    #[test]
    fn keyed_mode_refuses_missing_or_wrong_arity_frozen_declaration() {
        let mode = AssertNumRowsMode::PerKeyAtMostOne {
            key_slots: vec![SlotId::new(1)],
            key_labels: vec!["target".into()],
            message_prefix: "duplicate".into(),
        };
        let error = AssertNumRowsProcessorFactory::new(1, mode.clone())
            .unwrap()
            .with_key_types(vec![])
            .err()
            .unwrap();
        assert_eq!(error.failure_fact().class(), FailureClass::Invariant);
        let mut kernel = AssertNumRowsProcessorKernel {
            name: "missing declaration".into(),
            mode: RuntimeAssertNumRowsMode::from_plan(&mode).unwrap(),
            applicability: None,
            key_types: None,
            tracker: None,
            construction: None,
            control: None,
        };
        let error = kernel.prepare().unwrap_err();
        assert_eq!(
            error.detail(),
            "keyed assertion frozen key types are missing"
        );
    }
    #[test]
    fn duplicate_diagnostic_streams_large_string_and_binary_into_bounded_detail() {
        use arrow::array::{BinaryArray, StringArray};
        let large = "x".repeat(1024 * 1024);
        for (array, frozen) in [
            (
                Arc::new(StringArray::from(vec![large.as_str(), large.as_str()])) as ArrayRef,
                FunctionValueType::new(arrow::datatypes::DataType::Utf8, true),
            ),
            (
                Arc::new(BinaryArray::from(vec![large.as_bytes(), large.as_bytes()])) as ArrayRef,
                FunctionValueType::new(arrow::datatypes::DataType::Binary, true),
            ),
        ] {
            let rt = RuntimeState::default();
            let tracker = MemTracker::new_root("bounded duplicate diagnostic");
            let mut op = operator_typed(&rt, tracker, false, frozen.clone());
            let mut calls = TestDriver::new(op.as_mut(), &rt);
            let error = calls
                .input_and_collect(chunk(vec![array], &[frozen]))
                .unwrap_err();
            assert_eq!(
                error.failure_fact().reason(),
                ExecutionReason::AssertRowsDuplicateKey.reason()
            );
            assert!(
                error
                    .detail()
                    .starts_with("duplicate target: duplicate target=")
            );
            assert_eq!(
                error.detail().len(),
                novarocks_failure_contract::DIAGNOSTIC_MAX_BYTES
            );
        }
    }
}

#[cfg(test)]
mod key_index_migration_tests {
    use super::*;
    use arrow::{
        array::StringArray,
        datatypes::{DataType, Schema},
    };
    use novarocks_functions::{KernelEvaluationControl, KernelFailure};
    use novarocks_value_arrays::decode::ValueConversionContext;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct Control;
    impl KernelEvaluationControl for Control {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            Ok(())
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("local key operation never waits")
        }
    }
    fn fixture() -> (Box<dyn InputConstructionOwner>, Arc<MemTracker>) {
        let tracker = MemTracker::new_root("assert K migration fixture");
        let factory=crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),
            Arc::new(crate::runtime::runtime_state::RuntimeErrorState::default())).unwrap();
        let layout = crate::exec::chunk::typed_fixture::layout(Arc::new(Schema::empty()), &[], &[]);
        let schema = factory.create_compiled_chunk_schema(&layout).unwrap();
        (factory.begin_typed_construction(&schema).unwrap(), tracker)
    }
    fn state(owner: &dyn InputConstructionOwner, tracker: &Arc<MemTracker>) -> TypedKeyState {
        let spec = KeySpec::new(
            &[FunctionValueType::new(DataType::Utf8, false)],
            &[NullPolicy::Equal],
            owner,
            &|| Ok(()),
        )
        .unwrap();
        TypedKeyState {
            index: KeyIndex::new(spec, IndexPurpose::KeyIds, tracker.clone(), owner).unwrap(),
        }
    }
    fn input(
        values: &[&str],
        state: &TypedKeyState,
        owner: &dyn InputConstructionOwner,
        tracker: &Arc<MemTracker>,
    ) -> (KeyInput, crate::exec::hash_table::key_index::LocalHashBatch) {
        let array: ArrayRef = Arc::new(StringArray::from(values.to_vec()));
        let rows = (0..values.len()).collect::<Vec<_>>();
        let selected = ReachableRows::all(values.len())
            .materialize_observed(
                &mut |layout| owner.request(layout).map_err(ExecutionFailure::from),
                &mut |_| Ok(()),
            )
            .unwrap();
        let input = KeyInput::new_for_spec(
            state.index.spec(),
            &[array],
            &[FunctionValueType::new(DataType::Utf8, false)],
            &selected,
            &rows,
            &mut ValueConversionContext::default(),
            owner,
            &|| Ok(()),
        )
        .unwrap();
        let prepared = PreparedKeyBatch::from_captured(
            state.index.spec(),
            &input,
            AggregateAllocator::new(tracker.clone()),
            &|| Ok(()),
        )
        .unwrap();
        let hashes = hash_batch(
            &prepared,
            state.index.policy(),
            &mut HashScratch::new(),
            owner,
            &|| Ok(()),
        )
        .unwrap()
        .force_hash(0);
        drop(prepared);
        (input, hashes)
    }
    #[test]
    fn real_collisions_preserve_first_occurrence_ids_and_exact_duplicates_across_batches() {
        let (owner, tracker) = fixture();
        let mut state = state(owner.as_ref(), &tracker);
        for (values, expected, new_count) in [
            (vec!["prefix-A", "prefix-B", "prefix-A"], vec![0, 1, 0], 2),
            (vec!["prefix-B", "prefix-C"], vec![1, 2], 1),
        ] {
            let (input, hashes) = input(&values, &state, owner.as_ref(), &tracker);
            let prepared = PreparedKeyBatch::from_captured(
                state.index.spec(),
                &input,
                AggregateAllocator::new(tracker.clone()),
                &|| Ok(()),
            )
            .unwrap();
            let found = state
                .index
                .intern_batch(&prepared, &hashes, owner.as_ref(), &Control)
                .unwrap();
            assert_eq!(found.new_keys().len(), new_count);
            assert_eq!(
                found
                    .matches()
                    .iter()
                    .map(|v| match v {
                        KeyMatch::Found(id) => id.index(),
                        _ => panic!("participating key"),
                    })
                    .collect::<Vec<_>>(),
                expected
            );
        }
        assert_eq!(state.index.len(), 3);
        drop(state);
        drop(owner);
        assert_eq!(tracker.current(), 0);
    }
    struct Count {
        calls: AtomicUsize,
        stop: Option<usize>,
    }
    impl KernelEvaluationControl for Count {
        fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
            let n = self.calls.fetch_add(1, Ordering::Relaxed) + 1;
            if self.stop == Some(n) {
                Err(KernelFailure::DeadlineExceeded)
            } else {
                Ok(())
            }
        }
        fn wait(&self, _: std::time::Duration) -> Result<(), KernelFailure> {
            panic!("local key operation never waits")
        }
    }
    #[test]
    fn long_collision_comparison_observes_stop_and_readonly_failure_leaves_original_keys_readable()
    {
        let (owner, tracker) = fixture();
        let mut state = state(owner.as_ref(), &tracker);
        let mut a = "x".repeat(3 * 64 * 1024);
        a.push('a');
        let mut b = a.clone();
        b.pop();
        b.push('b');
        let (input, hashes) = input(&[&a, &b], &state, owner.as_ref(), &tracker);
        let prepared = PreparedKeyBatch::from_captured(
            state.index.spec(),
            &input,
            AggregateAllocator::new(tracker.clone()),
            &|| Ok(()),
        )
        .unwrap();
        state
            .index
            .intern_batch(&prepared, &hashes, owner.as_ref(), &Control)
            .unwrap();
        let baseline = Count {
            calls: AtomicUsize::new(0),
            stop: None,
        };
        let expected = state
            .index
            .probe_batch(&prepared, &hashes, owner.as_ref(), &baseline)
            .unwrap();
        assert!(
            baseline.calls.load(Ordering::Relaxed) > 4,
            "actual long comparisons carry control observations"
        );
        let stopped = Count {
            calls: AtomicUsize::new(0),
            stop: Some(baseline.calls.load(Ordering::Relaxed) / 2),
        };
        assert!(
            state
                .index
                .probe_batch(&prepared, &hashes, owner.as_ref(), &stopped)
                .is_err()
        );
        assert_eq!(state.index.len(), 2);
        assert_eq!(
            state
                .index
                .probe_batch(&prepared, &hashes, owner.as_ref(), &Control)
                .unwrap()
                .matches(),
            expected.matches()
        );
    }
}

#[cfg(test)]
mod applicability_long_key_tests {
    //! The existing K counter proves no local byte hashing for bypassed rows.
    //! The production empty-selection return also precedes key-column access
    //! and KeyInput preparation. Source metadata custody itself is not hidden.
    use super::*;
    use crate::exec::hash_table::key_index::BYTE_HASH_INITS;
    use crate::exec::pipeline::test_driver::TestDriver;
    use arrow::{
        array::{BooleanArray, StringArray},
        datatypes::DataType,
    };
    fn input(value: &str, masks: Vec<Option<bool>>) -> Chunk {
        let rows = masks.len();
        crate::exec::chunk::typed_fixture::columns(
            &["target", "applies"],
            &[SlotId::new(1), SlotId::new(2)],
            &[
                FunctionValueType::new(DataType::Utf8, true),
                FunctionValueType::new(DataType::Boolean, true),
            ],
            vec![
                Arc::new(StringArray::from(vec![value; rows])),
                Arc::new(BooleanArray::from(masks)),
            ],
            rows,
        )
    }
    #[test]
    fn false_and_null_applicability_skip_long_key_hashing_and_do_not_create_seen_state() {
        let rt = RuntimeState::default();
        let tracker = MemTracker::new_root("long bypass K state");
        let mode = AssertNumRowsMode::PerKeyAtMostOne {
            key_slots: vec![SlotId::new(1)],
            key_labels: vec!["target".into()],
            message_prefix: "duplicate target".into(),
        };
        let mut op = AssertNumRowsProcessorFactory::new_with_applicability(1, mode, SlotId::new(2))
            .unwrap()
            .with_key_types(vec![FunctionValueType::new(DataType::Utf8, true)])
            .unwrap()
            .create(1, 0);
        op.set_mem_tracker(tracker.clone());
        op.set_construction_factory(crate::exec::pipeline::operator::input_construction::try_runtime_input_construction_factory(
            crate::exec::pipeline::pipeline::representation_construction_profile().unwrap(),tracker.clone(),rt.error_state()).unwrap());
        op.prepare().expect("prepare keyed assertion fixture");
        op.bind_runtime_state(&rt)
            .expect("bind original keyed assertion runtime");
        let baseline = tracker.current();
        let large = "x".repeat(1024 * 1024 + 17);
        BYTE_HASH_INITS.with(|n| n.set(0));
        {
            let mut calls = TestDriver::new(op.as_mut(), &rt);
            assert_eq!(
                calls
                    .input_and_collect(input(&large, vec![Some(false), None]))
                    .unwrap()[0]
                    .len(),
                2
            );
        }
        assert_eq!(BYTE_HASH_INITS.with(|n| n.get()), 0);
        assert_eq!(
            tracker.current(),
            baseline,
            "bypassed rows create no retained equality state"
        );
        {
            let mut calls = TestDriver::new(op.as_mut(), &rt);
            assert_eq!(
                calls
                    .input_and_collect(input(&large, vec![Some(true)]))
                    .unwrap()[0]
                    .len(),
                1
            );
        }
        assert!(
            BYTE_HASH_INITS.with(|n| n.get()) > 0,
            "the same live byte path is actually observed for TRUE"
        );
        op.cancel();
        op.close().unwrap();
        drop(op);
        assert_eq!(tracker.current(), 0);
    }
}
