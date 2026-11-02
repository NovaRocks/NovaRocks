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

//! Construction responsibility and original compiler control are independent.

use crate::FragmentCompileError;
use novarocks_local_program::{LayoutCompileError, LayoutCreationFailure};
use novarocks_type_contract::owned_resources::layout::LayoutResourceError;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, ValueSchemaCreation, ValueSchemaCreationScope,
};
use std::{alloc::Layout, error::Error, fmt};

#[derive(Debug)]
pub enum FragmentCreationError<H> {
    Control(CompileControlError),
    Creator(H),
    Resource(LayoutResourceError),
    Compile(FragmentCompileError),
}
impl<H> From<FragmentCompileError> for FragmentCreationError<H> {
    fn from(error: FragmentCompileError) -> Self {
        match error {
            FragmentCompileError::Control(cause) => Self::Control(cause),
            other => Self::Compile(other),
        }
    }
}

/// Preserve a primary construction/control refusal without another callback.
/// Ordinary compiler errors still finish the caller's pending structural work.
pub(crate) fn finish_attempt<T, H>(
    result: Result<T, FragmentCreationError<H>>,
    work: CompileCheckpoints<'_>,
) -> Result<T, FragmentCreationError<H>> {
    // A nested compiler owner authors its own facts. Keeping a richer
    // cause must not make a later footer replace its already observed refusal.
    if let Err(FragmentCreationError::Compile(source)) = &result {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        if source.is_primary_construction()
            || matches!(
                source.failure_fact().class(),
                FailureClass::Resource | FailureClass::Control
            )
        {
            return result;
        }
    }
    if matches!(
        &result,
        Err(FragmentCreationError::Control(_)
            | FragmentCreationError::Creator(_)
            | FragmentCreationError::Resource(_)
            | FragmentCreationError::Compile(
                FragmentCompileError::ProjectMetadataRequest(_)
                    | FragmentCompileError::ProjectMetadataHost { .. }
            ))
    ) {
        return result;
    }
    work.finish()?;
    result
}
impl<H> From<CompileControlError> for FragmentCreationError<H> {
    fn from(error: CompileControlError) -> Self {
        Self::Control(error)
    }
}
impl<H> From<LayoutResourceError> for FragmentCreationError<H> {
    fn from(error: LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
impl<H> From<LayoutCompileError> for FragmentCreationError<H> {
    fn from(error: LayoutCompileError) -> Self {
        FragmentCompileError::from(error).into()
    }
}
impl<H> From<LayoutCreationFailure<H>> for FragmentCreationError<H> {
    fn from(error: LayoutCreationFailure<H>) -> Self {
        match error {
            LayoutCreationFailure::Creator(cause) => Self::Creator(cause),
            LayoutCreationFailure::Layout(cause) => cause.into(),
            LayoutCreationFailure::Resource(cause) => Self::Resource(cause),
        }
    }
}
impl<H> From<novarocks_local_program::ValuesCreationFailure<H>> for FragmentCreationError<H> {
    fn from(error: novarocks_local_program::ValuesCreationFailure<H>) -> Self {
        use novarocks_local_program::ValuesCreationFailure;
        match error {
            ValuesCreationFailure::Values(cause) => {
                novarocks_local_program::ValuesCompileError::Values(cause).into()
            }
            ValuesCreationFailure::Control(cause) => Self::Control(cause),
            ValuesCreationFailure::Creator(cause) => Self::Creator(cause),
            ValuesCreationFailure::Resource(cause) => Self::Resource(cause),
        }
    }
}
impl<H> From<novarocks_physical_plan::ConstantReferenceError> for FragmentCreationError<H> {
    fn from(error: novarocks_physical_plan::ConstantReferenceError) -> Self {
        FragmentCompileError::from(error).into()
    }
}
impl<H> From<novarocks_functions::ConstantError> for FragmentCreationError<H> {
    fn from(error: novarocks_functions::ConstantError) -> Self {
        FragmentCompileError::from(error).into()
    }
}
impl<H: fmt::Display> fmt::Display for FragmentCreationError<H> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Control(e) => e.fmt(f),
            Self::Creator(e) => e.fmt(f),
            Self::Resource(e) => write!(f, "compiler construction geometry: {e:?}"),
            Self::Compile(e) => e.fmt(f),
        }
    }
}
impl<H: Error + 'static> Error for FragmentCreationError<H> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Control(e) => Some(e),
            Self::Creator(e) => Some(e),
            Self::Resource(source) => Some(source),
            Self::Compile(e) => Some(e),
        }
    }
}

/// Observe control before the actual request. A creator's refusal returns
/// immediately, so later checkpoints cannot replace its original cause.
pub(crate) fn request<H>(
    layout: Layout,
    creation: &mut dyn ValueSchemaCreationScope<H>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    work.flush()?;
    creation
        .request_layout(layout)
        .map_err(FragmentCreationError::Creator)
}

macro_rules! compiler_cause {
    ($cause:ty) => {
        impl<H> From<$cause> for FragmentCreationError<H> {
            fn from(error: $cause) -> Self {
                FragmentCompileError::from(error).into()
            }
        }
    };
}
compiler_cause!(novarocks_functions::array_projection::ArrayProjectionFailure);
compiler_cause!(novarocks_type_contract::ValueTypeError);
compiler_cause!(crate::repeat::RepeatLoweringError);
compiler_cause!(crate::unpivot::UnpivotLoweringError);
compiler_cause!(crate::channels::ChannelLoweringError);
compiler_cause!(crate::expressions::ExpressionLoweringError);
compiler_cause!(novarocks_local_program::ProgramCompileError);
compiler_cause!(novarocks_local_program::BindingRequirementsCompileError);
compiler_cause!(novarocks_local_program::ProgramControlFlowError);
compiler_cause!(novarocks_local_program::ProgramRootBindingError);
compiler_cause!(novarocks_local_program::ProgramResolvedCallsError);
compiler_cause!(novarocks_local_program::ProgramExpressionTypeError);
compiler_cause!(novarocks_local_program::ValuesCompileError);
compiler_cause!(novarocks_local_program::ProgramChannelTypeError);
compiler_cause!(novarocks_local_program::ProgramLexicalBindingError);
compiler_cause!(novarocks_local_program::LocalProgramCompileError);

impl FragmentCreationError<std::convert::Infallible> {
    pub(crate) fn into_compile(self) -> FragmentCompileError {
        match self {
            Self::Control(cause) => FragmentCompileError::Control(cause),
            Self::Creator(never) => match never {},
            Self::Resource(cause) => FragmentCompileError::Owner {
                phase: "creation geometry",
                error: novarocks_failure_contract::SharedFailureCause::new(cause),
            },
            Self::Compile(cause) => cause,
        }
    }
}

/// Explicit FE preparation uses ordinary references and the same construction
/// body. This wrapper acquires no memory grant and installs no quota.
pub(crate) fn pure<T>(
    operation: impl FnOnce(
        &mut ValueSchemaCreation<'_, std::convert::Infallible>,
    ) -> Result<T, FragmentCreationError<std::convert::Infallible>>,
) -> Result<T, FragmentCompileError> {
    let mut request = |_: Layout| Ok::<(), std::convert::Infallible>(());
    let mut opaque = |_: usize| Ok::<(), std::convert::Infallible>(());
    let mut creation = ValueSchemaCreation::new(
        novarocks_type_contract::ValueTypeStorageLifetime::pure(),
        &mut request,
        &mut opaque,
    );
    operation(&mut creation).map_err(FragmentCreationError::into_compile)
}

impl<C, H> From<novarocks_local_program::ProgramCreationFailure<C, H>> for FragmentCreationError<H>
where
    FragmentCompileError: From<C>,
{
    fn from(e: novarocks_local_program::ProgramCreationFailure<C, H>) -> Self {
        use novarocks_local_program::ProgramCreationFailure as Failure;
        match e {
            Failure::Control(c) => Self::Control(c),
            Failure::Creator(c) => Self::Creator(c),
            Failure::Resource(c) => Self::Resource(c),
            Failure::Capacity(source) => program_publication_failure(source).into(),
            Failure::Owner(c) => FragmentCompileError::from(c).into(),
        }
    }
}
pub(crate) fn program_publication_failure(
    source: std::collections::TryReserveError,
) -> FragmentCompileError {
    FragmentCompileError::Owner {
        phase: "program publication",
        error: novarocks_failure_contract::SharedFailureCause::new(
            novarocks_local_program::ProgramCreationFailure::<
                std::convert::Infallible,
                std::convert::Infallible,
            >::Capacity(source),
        ),
    }
}

/// Same locked Vec operation quote, before the caller's unchanged push.
pub(crate) fn before_vec_push<T, H>(
    values: &Vec<T>,
    creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    let Some(creation) = creation else {
        return Ok(());
    };
    let facts = novarocks_type_contract::owned_resources::vec::push_growth(values).map_err(
        |e| match e {
            novarocks_type_contract::ControlResourceError::Control(c) => {
                FragmentCreationError::Control(c)
            }
            novarocks_type_contract::ControlResourceError::SourceModel(_) => {
                FragmentCreationError::Resource(LayoutResourceError::SourceModel)
            }
        },
    )?;
    if let Some(l) = facts.requested_backing {
        request(l, creation, work)?;
    }
    Ok(())
}
pub(crate) fn reserve_vec<T, H>(
    values: &mut Vec<T>,
    additional: usize,
    creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    if let Some(creation) = creation {
        let next = values
            .len()
            .checked_add(additional)
            .ok_or(LayoutResourceError::Arithmetic)?;
        if next > values.capacity() {
            request(
                Layout::array::<T>(next).map_err(|_| LayoutResourceError::Arithmetic)?,
                creation,
                work,
            )?;
        }
    }
    crate::assert_rows::reserve_vec(values, additional, work)?;
    Ok(())
}
pub(crate) fn before_tree_insert<K, V, H>(
    prior: usize,
    creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<(), FragmentCreationError<H>> {
    let Some(creation) = creation else {
        return Ok(());
    };
    use novarocks_type_contract::owned_resources::btree;
    let error = |e| match e {
        btree::BTreeResourceError::SourceModel(_) => LayoutResourceError::SourceModel,
        btree::BTreeResourceError::Arithmetic(_) => LayoutResourceError::Arithmetic,
    };
    let before = btree::insertion_only::<K, V>(prior).map_err(error)?;
    let after = btree::insertion_only::<K, V>(
        prior
            .checked_add(1)
            .ok_or(LayoutResourceError::Arithmetic)?,
    )
    .map_err(error)?;
    let delta = after
        .request_bytes_upper_bound
        .checked_sub(before.request_bytes_upper_bound)
        .ok_or(LayoutResourceError::Arithmetic)?;
    work.flush()?;
    creation
        .observe_opaque_upper_bound(delta)
        .map_err(FragmentCreationError::Creator)
}

/// Compile-time absence of a factory in the pure adapter. This uninhabited
/// type cannot create a scope, callback, authority or successful grant.
pub(crate) enum NoFactory {}
impl<H> novarocks_type_contract::ValueSchemaCreationScope<H> for NoFactory {
    fn storage_lifetime(&self) -> novarocks_type_contract::ValueTypeStorageLifetime {
        match *self {}
    }
    fn request_layout(&mut self, _: Layout) -> Result<(), H> {
        match *self {}
    }
    fn observe_opaque_upper_bound(&mut self, _: usize) -> Result<(), H> {
        match *self {}
    }
}
impl<H> novarocks_type_contract::ValueSchemaCreationFactory<H> for NoFactory {
    type Scope<'a> = NoFactory;
    fn begin_scope(&self) -> Result<Self::Scope<'_>, H> {
        match *self {}
    }
    fn begin_scope_observed<O>(
        &self,
        _: &mut dyn FnMut(Layout) -> Result<(), O>,
    ) -> Result<Self::Scope<'_>, novarocks_type_contract::ValueSchemaScopeBeginFailure<O, H>> {
        match *self {}
    }
}

pub(crate) fn publish_argument_types<H>(
    types: Vec<novarocks_type_contract::FunctionArgumentType>,
    mut creation: Option<&mut dyn ValueSchemaCreationScope<H>>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<
    novarocks_type_contract::FunctionArgumentTypes,
    novarocks_local_program::ProgramCreationFailure<std::convert::Infallible, H>,
> {
    use novarocks_local_program::ProgramCreationFailure as Failure;
    use novarocks_type_contract::{
        FunctionArgumentType, FunctionArgumentTypes, ValueTypeStorageLifetime,
    };
    if let Some(c) = crate::creation::borrow_scope(&mut creation) {
        if types.capacity() != types.len() && !types.is_empty() {
            let layout = Layout::array::<FunctionArgumentType>(types.len())
                .map_err(|_| Failure::Resource(LayoutResourceError::Arithmetic))?;
            work.flush()?;
            c.request_layout(layout).map_err(Failure::Creator)?;
        }
        if !types.is_empty() {
            let layout =
                FunctionArgumentTypes::storage_allocation_layout().map_err(Failure::Resource)?;
            work.flush()?;
            c.request_layout(layout).map_err(Failure::Creator)?;
        }
    }
    let storage = creation
        .as_ref()
        .map_or_else(ValueTypeStorageLifetime::pure, |c| c.storage_lifetime());
    Ok(FunctionArgumentTypes::from_boxed_in(
        types.into_boxed_slice(),
        storage,
    ))
}
// Closed publisher failures have no semantic Owner case to erase or guess.
pub(crate) fn argument_types_failure<H>(
    e: novarocks_local_program::ProgramCreationFailure<std::convert::Infallible, H>,
) -> FragmentCreationError<H> {
    use novarocks_local_program::ProgramCreationFailure as Failure;
    match e {
        Failure::Control(c) => FragmentCreationError::Control(c),
        Failure::Creator(c) => FragmentCreationError::Creator(c),
        Failure::Resource(c) => FragmentCreationError::Resource(c),
        Failure::Capacity(source) => program_publication_failure(source).into(),
        Failure::Owner(never) => match never {},
    }
}

/// Borrow the optional real scope for one synchronous operation. The caller's
/// longer scope lifetime does not extend this mutable borrow across a loop.
pub(crate) fn borrow_scope<'borrow, H>(
    scope: &'borrow mut Option<&mut dyn ValueSchemaCreationScope<H>>,
) -> Option<&'borrow mut dyn ValueSchemaCreationScope<H>> {
    match scope {
        Some(scope) => Some(&mut **scope),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use novarocks_type_contract::{
        CompilePhase, PureCompileControl, ValueTypeStorageHold, ValueTypeStorageLifetime,
    };
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    struct Control(AtomicBool);
    impl PureCompileControl for Control {
        fn checkpoint(&self, _: CompilePhase, _: u32) -> Result<(), CompileControlError> {
            if self.0.load(Ordering::SeqCst) {
                Err(CompileControlError::Cancelled)
            } else {
                Ok(())
            }
        }
    }
    #[derive(Debug, Eq, PartialEq)]
    struct Refusal(u32);
    #[test]
    fn typed_layout_creator_refusal_is_not_replaced_by_later_control() {
        let control = Control(AtomicBool::new(false));
        let mut request = |_: Layout| {
            control.0.store(true, Ordering::SeqCst);
            Err(Refusal(73))
        };
        let mut opaque = |_: usize| Ok::<_, Refusal>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        assert!(matches!(
            crate::typed_layout::TypedLayoutBuilder::new_with_creation(1, &mut work, &mut creation),
            Err(LayoutCreationFailure::Creator(Refusal(73)))
        ));
    }
    #[test]
    fn typed_array_resource_refusal_precedes_later_compilation_control() {
        use novarocks_failure_contract::{FailureClass, FailureProjection};
        use novarocks_functions::array_projection::{
            ArrayProjectionFailure, ArrayProjectionOperation,
        };
        for original in [
            ArrayProjectionFailure::ValueType(novarocks_type_contract::ValueTypeError::TooDeep),
            ArrayProjectionFailure::RequestArithmetic,
            ArrayProjectionFailure::Arrow {
                operation: ArrayProjectionOperation::RebuildArray,
                source: arrow_schema::ArrowError::MemoryError("original array request".into()),
            },
        ] {
            let expected = original.failure_fact();
            assert_eq!(expected.class(), FailureClass::Resource);
            let control = Control(AtomicBool::new(false));
            let mut work =
                CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
            work.step().unwrap();
            control.0.store(true, Ordering::SeqCst);
            let result: Result<(), FragmentCreationError<Refusal>> = finish_attempt(
                Err(FragmentCompileError::ArrayProjection(original).into()),
                work,
            );
            let Err(FragmentCreationError::Compile(FragmentCompileError::ArrayProjection(cause))) =
                result
            else {
                panic!("a later footer must not replace the originating array refusal: {result:?}");
            };
            assert_eq!(cause.failure_fact(), expected);
        }
    }
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    #[test]
    fn independently_retained_compiled_layout_keeps_its_real_new_creation_hold() {
        let drops = Arc::new(AtomicUsize::new(0));
        let control = Control(AtomicBool::new(false));
        let mut request = |_: Layout| Ok::<_, std::convert::Infallible>(());
        let mut opaque = |_: usize| Ok::<_, std::convert::Infallible>(());
        let mut creation = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::held(Arc::new(Hold(drops.clone()))),
            &mut request,
            &mut opaque,
        );
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let mut builder =
            crate::typed_layout::TypedLayoutBuilder::new_with_creation(1, &mut work, &mut creation)
                .unwrap();
        builder
            .push_with_creation(
                &novarocks_type_contract::FunctionValueType::new(
                    arrow_schema::DataType::Int64,
                    false,
                ),
                "v".to_owned(),
                &mut work,
                &mut creation,
            )
            .unwrap();
        let layout = builder
            .finish_with_creation(&[novarocks_types::SlotId::new(1)], &mut work, &mut creation)
            .unwrap();
        let cloned = layout.clone();
        drop(layout);
        drop(creation);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert_eq!(
            cloned.value_binding_at(0).unwrap().data_type(),
            &arrow_schema::DataType::Int64
        );
        drop(cloned);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn expression_type_backing_request_refusal_has_no_control_tail() {
        let control = Control(AtomicBool::new(false));
        let mut request = |_: Layout| {
            control.0.store(true, Ordering::SeqCst);
            Err(Refusal(79))
        };
        let mut opaque = |_: usize| Ok::<(), Refusal>(());
        let mut creation =
            ValueSchemaCreation::new(ValueTypeStorageLifetime::pure(), &mut request, &mut opaque);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let mut types = Vec::<novarocks_type_contract::FunctionArgumentType>::new();
        assert!(matches!(
            reserve_vec(&mut types, 1, Some(&mut creation), &mut work),
            Err(FragmentCreationError::Creator(Refusal(79)))
        ));
        assert!(types.is_empty());
        assert_eq!(types.capacity(), 0);
    }
    #[test]
    fn independently_cloned_argument_types_keep_only_their_arena_creator() {
        use novarocks_type_contract::{FunctionArgumentType, FunctionValueType};
        let arena_drops = Arc::new(AtomicUsize::new(0));
        let unrelated_drops = Arc::new(AtomicUsize::new(0));
        let unrelated = ValueTypeStorageLifetime::held(Arc::new(Hold(unrelated_drops.clone())));
        let control = Control(AtomicBool::new(false));
        let mut request = |_: Layout| Ok::<(), std::convert::Infallible>(());
        let mut opaque = |_: usize| Ok::<(), std::convert::Infallible>(());
        let mut creation = ValueSchemaCreation::new(
            ValueTypeStorageLifetime::held(Arc::new(Hold(arena_drops.clone()))),
            &mut request,
            &mut opaque,
        );
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::LowerProgram).unwrap();
        let mut types = Vec::new();
        reserve_vec(&mut types, 1, Some(&mut creation), &mut work).unwrap();
        types.push(FunctionArgumentType::Value(FunctionValueType::new(
            arrow_schema::DataType::Int64,
            false,
        )));
        let types = publish_argument_types(types, Some(&mut creation), &mut work).unwrap();
        let last = types.clone();
        drop(types);
        drop(creation);
        drop(unrelated);
        assert_eq!(unrelated_drops.load(Ordering::SeqCst), 1);
        assert_eq!(arena_drops.load(Ordering::SeqCst), 0);
        assert_eq!(last.len(), 1);
        drop(last);
        assert_eq!(arena_drops.load(Ordering::SeqCst), 1);
    }
}
