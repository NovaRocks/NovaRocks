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

use super::*;
use arrow_array::{ArrayRef, Int64Array};
use arrow_schema::DataType;
use novarocks_type_contract::{
    CallProofScope, CompileCheckpoints, CompileControlError, DecimalOverflowPolicy,
    EvaluationDemand, EvaluationDomainId, ExpressionEffectContext, ExpressionUseId,
    FunctionFailureBehavior, SemanticParameters,
};
use std::{
    sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const FUNCTION: &str = "fixture/pure-catalogue/function-v1";
const SCALAR: &str = "fixture/pure-catalogue/scalar-v1";
const HIGHER: &str = "fixture/pure-catalogue/higher-v1";
const CODE: &str = "fixture/pure-catalogue/cpu-v1";
fn id(value: &str) -> FunctionId {
    FunctionId::try_new(value).unwrap()
}
fn overload_id(value: &str) -> FunctionOverloadId {
    FunctionOverloadId::try_new(value).unwrap()
}
fn implementation(value: &str) -> PureImplementationId {
    PureImplementationId::try_new(value).unwrap()
}
fn value_type() -> FunctionValueType {
    FunctionValueType::new(DataType::Int64, false)
}
fn base(control: ArgumentControl) -> FunctionEffectDeclaration {
    FunctionEffectDeclaration {
        value_stability: FunctionVolatility::Immutable,
        own_row_error: FunctionIntrinsicRowError::NoRowError,
        failure_behavior: FunctionFailureBehavior::Propagate,
        null_behavior: FunctionNullBehavior::CalledOnNull,
        argument_control: control,
        instance_state: FunctionInstanceState::None,
        observable_effects: ObservableEffects::NONE,
        environment_dependencies: Box::new([]),
    }
}
fn overload(value: &str, effects: FunctionEffectDeclaration) -> FunctionOverloadDeclaration {
    FunctionOverloadDeclaration::from_effects(
        overload_id(value),
        if value == HIGHER {
            "(BIGINT, lambda(BIGINT)->BIGINT)"
        } else {
            "(BIGINT)"
        },
        "BIGINT",
        None,
        effects,
    )
}
#[derive(Debug, Default)]
struct Counts {
    resolve: AtomicUsize,
    refine: AtomicUsize,
    prepare: AtomicUsize,
    instances: AtomicUsize,
    addresses: Mutex<Vec<usize>>,
}
struct Owner {
    declaration: FunctionBindingDeclaration,
    implementations: Vec<PureImplementationDeclaration>,
    counts: Arc<Counts>,
    source_override: Option<FunctionEffectDeclaration>,
    binding_failure: Mutex<Option<CompileControlError>>,
}
impl Owner {
    fn new(higher: bool) -> Self {
        let mut overloads = vec![overload(SCALAR, base(ArgumentControl::Eager))];
        let mut implementations = vec![PureImplementationDeclaration {
            overload: overload_id(SCALAR),
            implementation: implementation(CODE),
            abi: PureKernelAbi::ScalarV1,
        }];
        if higher {
            overloads.push(overload(
                HIGHER,
                base(ArgumentControl::HigherOrder {
                    body_ordinal: 1,
                    body_demand: EvaluationDemand::Value,
                }),
            ));
            implementations.push(PureImplementationDeclaration {
                overload: overload_id(HIGHER),
                implementation: implementation("fixture/pure-catalogue/higher-cpu-v1"),
                abi: PureKernelAbi::HigherOrderV1,
            });
        }
        Self {
            declaration: FunctionBindingDeclaration::try_new_complete(
                id(FUNCTION),
                FunctionKind::Scalar,
                overloads,
            )
            .unwrap(),
            implementations,
            counts: Arc::new(Counts::default()),
            source_override: None,
            binding_failure: Mutex::new(None),
        }
    }
    fn mark(&self) {
        self.counts
            .addresses
            .lock()
            .unwrap()
            .push(self as *const Self as usize);
    }
    fn frozen(&self, selected: &FunctionBindingSelection) -> CallEffects {
        let base = self
            .declaration
            .effect_declaration(&selected.overload)
            .unwrap();
        CallEffects {
            value_stability: base.value_stability,
            own_row_error: base.own_row_error,
            failure_behavior: base.failure_behavior,
            null_behavior: base.null_behavior,
            argument_control: base.argument_control,
            instance_state: base.instance_state,
            observable_effects: base.observable_effects,
            environment: Box::new([]),
            proof_scope: CallProofScope::Domain(EvaluationDomainId::new(7)),
        }
    }
}
impl PureFunctionMetadataOwner for Owner {
    fn binding_declaration(&self) -> &FunctionBindingDeclaration {
        &self.declaration
    }
    fn implementation_declarations(&self) -> &[PureImplementationDeclaration] {
        &self.implementations
    }
}
impl FunctionBindingResolver for Owner {
    fn resolve(
        &self,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<FunctionBindingSelection, FunctionBindingError> {
        self.mark();
        self.counts.resolve.fetch_add(1, Ordering::Relaxed);
        let higher = request
            .arguments
            .iter()
            .any(|a| matches!(a, FunctionArgument::Lambda { .. }));
        let selection = FunctionBindingSelection {
            overload: overload_id(if higher { HIGHER } else { SCALAR }),
            argument_types: request
                .arguments
                .iter()
                .map(FunctionArgument::argument_type)
                .collect(),
            result_type: FunctionResultType::Scalar(value_type()),
            aggregate: None,
        };
        self.validate_selected(&selection, request, control)?;
        Ok(selection)
    }
    fn validate_selected(
        &self,
        selected: &FunctionBindingSelection,
        request: FunctionBindingRequest<'_>,
        control: &dyn novarocks_type_contract::PureCompileControl,
    ) -> Result<(), FunctionBindingError> {
        if let Some(error) = *self.binding_failure.lock().unwrap() {
            return Err(error.into());
        }
        self.declaration.effect_declaration(&selected.overload)?;
        let expected = if selected.overload == overload_id(SCALAR) {
            scalar_arguments()
        } else {
            higher_arguments()
        };
        let mut arguments_match = request.arguments.len() == expected.len();
        if arguments_match {
            for (actual, expected) in request.arguments.iter().zip(&expected) {
                if !actual.equals_observed(
                    expected,
                    CompilePhase::FunctionSpecialization,
                    control,
                )? {
                    arguments_match = false;
                    break;
                }
            }
        }
        if !arguments_match
            || request.logical_argument_count != expected.len()
            || selected.argument_types
                != expected
                    .iter()
                    .map(FunctionArgument::argument_type)
                    .collect::<Box<[_]>>()
            || selected.result_type != FunctionResultType::Scalar(value_type())
            || selected.aggregate.is_some()
        {
            Err(FunctionBindingError::InvalidBinding(
                "fixture exact signature mismatch".into(),
            ))
        } else {
            Ok(())
        }
    }
}
impl FunctionEffectOwner for Owner {
    type Error = FunctionBindingError;
    fn declaration(
        &self,
        function: &FunctionId,
        selected: &FunctionBindingSelection,
    ) -> Result<&FunctionEffectDeclaration, Self::Error> {
        if function != self.declaration.function_id() {
            return Err(FunctionBindingError::UnknownFunction);
        }
        if let Some(base) = &self.source_override {
            return Ok(base);
        }
        self.declaration.effect_declaration(&selected.overload)
    }
    fn validate_and_refine(
        &self,
        input: CallEffectInput<'_>,
        control: &dyn PureCompileControl,
    ) -> Result<CallEffects, FunctionEffectOwnerError<Self::Error>> {
        self.mark();
        self.counts.refine.fetch_add(1, Ordering::Relaxed);
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::FunctionSpecialization)
            .map_err(FunctionEffectOwnerError::Control)?;
        self.validate_selected(input.selected, input.request, control)?;
        if input.function_id != self.declaration.function_id()
            || input.kind != FunctionKind::Scalar
            || !input.environment.is_empty()
        {
            return Err(FunctionBindingError::UnknownFunction.into());
        }
        for _ in input.request.arguments {
            work.step().map_err(FunctionEffectOwnerError::Control)?;
        }
        work.finish().map_err(FunctionEffectOwnerError::Control)?;
        Ok(self.frozen(input.selected))
    }
}
impl PureScalarImplementation for Owner {
    fn prepare_scalar(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<ScalarCallContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedScalarKernel>, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(crate::kernel_control::compile_failure)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|_| crate::kernel_control::internal("fixture changed selected signature"))?;
        self.mark();
        self.counts.prepare.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(PreparedScalar {
            contract,
            counts: self.counts.clone(),
        }))
    }
}
#[derive(Debug)]
struct PreparedScalar {
    contract: Arc<ScalarCallContract>,
    counts: Arc<Counts>,
}
impl PreparedScalarKernel for PreparedScalar {
    fn contract(&self) -> &Arc<ScalarCallContract> {
        &self.contract
    }
    fn instance_retained_upper_bound(&self) -> usize {
        0
    }
    fn create_instance(&self) -> Result<Box<dyn ScalarKernelInstance>, KernelFailure> {
        self.counts.instances.fetch_add(1, Ordering::Relaxed);
        Ok(Box::new(ScalarInstance))
    }
}
struct ScalarInstance;
impl ScalarKernelInstance for ScalarInstance {
    fn retained_bytes(&self) -> usize {
        0
    }
    fn evaluate<'input>(
        &mut self,
        input: ScalarCallInput<'_, 'input>,
        control: &dyn KernelEvaluationControl,
    ) -> Result<SelectedValues<'input>, KernelFailure> {
        let argument = input.arguments()[0];
        let array = argument
            .array()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let mut values = Vec::new();
        let mut work = crate::kernel_input::EvaluationCheckpoints::new(control);
        for ordinal in 0..input.selection().len() {
            let row = input.selection().row(ordinal).unwrap();
            values.push(
                array
                    .value(argument.value_row(ordinal, row))
                    .wrapping_mul(3)
                    .wrapping_add(7),
            );
            work.step()?;
        }
        work.finish()?;
        let array: ArrayRef = Arc::new(Int64Array::from(values));
        SelectedValues::try_new(input.selection(), &DataType::Int64, array, Box::new([]))
            .map_err(|_| crate::kernel_control::internal("fixture invalid output"))
    }
}
impl PureHigherOrderImplementation for Owner {
    fn prepare_higher_order(
        &self,
        input: CallEffectInput<'_>,
        contract: Arc<HigherOrderCallContract>,
        body: Arc<LambdaBodyContract>,
        control: &dyn PureCompileControl,
    ) -> Result<Arc<dyn PreparedHigherOrderKernel>, KernelFailure> {
        control
            .checkpoint(CompilePhase::FunctionSpecialization, 0)
            .map_err(crate::kernel_control::compile_failure)?;
        self.validate_selected(input.selected, input.request, control)
            .map_err(|_| crate::kernel_control::internal("fixture changed higher signature"))?;
        self.mark();
        self.counts.prepare.fetch_add(1, Ordering::Relaxed);
        Ok(Arc::new(PreparedHigher { contract, body }))
    }
}
#[derive(Debug)]
struct PreparedHigher {
    contract: Arc<HigherOrderCallContract>,
    body: Arc<LambdaBodyContract>,
}
impl PreparedHigherOrderKernel for PreparedHigher {
    fn contract(&self) -> &Arc<HigherOrderCallContract> {
        &self.contract
    }
    fn body_contract(&self) -> &Arc<LambdaBodyContract> {
        &self.body
    }
    fn instance_retained_upper_bound(&self) -> usize {
        0
    }
    fn create_instance(
        &self,
        _: &dyn KernelEvaluationControl,
    ) -> Result<Box<dyn HigherOrderKernelInstance>, KernelFailure> {
        panic!("catalogue preparation must not instantiate higher-order state")
    }
}
fn scalar_arguments() -> Vec<FunctionArgument> {
    vec![FunctionArgument::Value {
        value_type: value_type(),
        constant: None,
    }]
}
fn higher_arguments() -> Vec<FunctionArgument> {
    vec![
        scalar_arguments().remove(0),
        FunctionArgument::Lambda {
            parameter_types: Box::from([value_type()]),
            result_type: value_type(),
        },
    ]
}
fn context() -> ExpressionEffectContext {
    ExpressionEffectContext {
        use_id: ExpressionUseId::new(0),
        domain: EvaluationDomainId::new(7),
        demand: EvaluationDemand::Value,
    }
}
fn call_input<'a>(
    function: &'a FunctionId,
    selected: &'a FunctionBindingSelection,
    arguments: &'a [FunctionArgument],
    uses: &'a [Option<ExpressionUseId>],
    parameters: &'a SemanticParameters,
) -> CallEffectInput<'a> {
    CallEffectInput {
        context: context(),
        argument_uses: crate::CallArgumentUses::SelectedChannels(uses),
        function_id: function,
        kind: FunctionKind::Scalar,
        selected,
        request: FunctionBindingRequest {
            expected_result_type: None,
            arguments,
            logical_argument_count: arguments.len(),
        },
        environment: &[],
        parameters,
        decimal_overflow_policy: DecimalOverflowPolicy::ReportError,
        proof_scope: CallProofScope::Domain(context().domain),
    }
}
fn scalar_options() -> PureCallPreparation {
    PureCallPreparation::Scalar {
        arguments: ScopedExpressionEffects::pure_value(context()),
    }
}
fn higher_options() -> PureCallPreparation {
    let body_context = ExpressionEffectContext {
        use_id: ExpressionUseId::new(10),
        domain: EvaluationDomainId::new(11),
        demand: EvaluationDemand::Value,
    };
    PureCallPreparation::HigherOrder(HigherOrderPreparationOptions {
        arguments: ScopedExpressionEffects::pure_value(context()),
        body_context,
        body_effects: ScopedExpressionEffects::pure_value(body_context),
        capture_types: Box::new([]),
    })
}
fn definition(owner: Arc<Owner>, mixed: bool) -> Result<FunctionDefinition, PureCatalogError> {
    if mixed {
        FunctionDefinition::try_new_pure_scalar_higher_order(
            "catalogue_fixture",
            FunctionVisibility::Public,
            owner,
        )
    } else {
        FunctionDefinition::try_new_pure_scalar(
            "catalogue_fixture",
            FunctionVisibility::Public,
            owner,
        )
    }
}
// Independent installation inventory for this test's real CPU owners. It is
// not derived from or zipped with the metadata declaration under test.
fn installed(higher: bool) -> Vec<InstalledPureKernel> {
    let mut rows = vec![InstalledPureKernel {
        function: id(FUNCTION),
        kind: FunctionKind::Scalar,
        implementation: PureImplementationDeclaration {
            overload: overload_id(SCALAR),
            implementation: implementation(CODE),
            abi: PureKernelAbi::ScalarV1,
        },
        aggregate_state_format: None,
    }];
    if higher {
        rows.push(InstalledPureKernel {
            function: id(FUNCTION),
            kind: FunctionKind::Scalar,
            implementation: PureImplementationDeclaration {
                overload: overload_id(HIGHER),
                implementation: implementation("fixture/pure-catalogue/higher-cpu-v1"),
                abi: PureKernelAbi::HigherOrderV1,
            },
            aggregate_state_format: None,
        });
    }
    rows
}
fn builder(definition: FunctionDefinition) -> EngineFunctionCatalogBuilder {
    let mut b = EngineFunctionCatalogBuilder::new();
    b.register(definition).unwrap();
    b
}
fn catalogue(owner: Arc<Owner>, higher: bool) -> PureEngineFunctionCatalog {
    builder(definition(owner, higher).unwrap())
        .seal_pure(installed(higher))
        .unwrap()
}
struct RuntimeControl;
impl KernelEvaluationControl for RuntimeControl {
    fn checkpoint(&self, _: u32) -> Result<(), KernelFailure> {
        Ok(())
    }
    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("no waits")
    }
}
#[derive(Default)]
struct CompileControl {
    failure: Option<CompileControlError>,
    positive: bool,
}
impl PureCompileControl for CompileControl {
    fn checkpoint(&self, phase: CompilePhase, work: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::FunctionSpecialization);
        assert!(work <= 256);
        if !self.positive || work > 0 {
            self.failure.map_or(Ok(()), Err)
        } else {
            Ok(())
        }
    }
}

pub(super) fn assert_borrowed_call(
    specialization: &PureCallSpecialization,
    input: CallEffectInput<'_>,
    expected: &CallEffects,
) {
    let direct: &FunctionCallContract = match specialization.prepared() {
        PreparedPureKernel::Scalar(kernel) => kernel.contract().call(),
        PreparedPureKernel::ScalarInvocation(kernel) => kernel.contract().call(),
        PreparedPureKernel::HigherOrder(kernel) => kernel.contract().call(),
        PreparedPureKernel::Aggregate(kernel) => kernel.contract().call(),
        PreparedPureKernel::Window(kernel) => kernel.contract().call(),
        PreparedPureKernel::Table(kernel) => kernel.contract().call(),
        PreparedPureKernel::ControlIntrinsic(call) => call.as_ref(),
    };
    let borrowed = specialization.call_contract();
    assert!(std::ptr::eq(borrowed, direct));
    assert!(std::ptr::eq(
        specialization.prepared().call_contract(),
        direct
    ));
    assert!(std::ptr::eq(borrowed.function_id(), direct.function_id()));
    assert!(std::ptr::eq(
        borrowed.selected_owner(),
        direct.selected_owner()
    ));
    assert!(std::ptr::eq(borrowed.effects(), direct.effects()));
    assert!(std::ptr::eq(borrowed.parameters(), direct.parameters()));
    assert!(std::ptr::eq(borrowed.selected(), input.selected));
    assert_eq!(borrowed.function_id(), input.function_id);
    assert_eq!(borrowed.kind(), input.kind);
    assert_eq!(borrowed.context(), input.context);
    assert_eq!(
        borrowed.decimal_overflow_policy(),
        input.decimal_overflow_policy
    );
    assert_eq!(
        borrowed.logical_argument_count(),
        input.request.logical_argument_count
    );
    assert_eq!(borrowed.effects(), expected);
    assert_eq!(
        borrowed.parameters(),
        &input
            .parameters
            .project(expected.environment.iter().copied())
            .unwrap()
    );
}

pub(super) fn assert_preparation_provenance(
    specialization: &PureCallSpecialization,
    expected: &PureImplementationDeclaration,
    source: PurePreparationSource,
) {
    assert_eq!(specialization.implementation(), expected);
    assert_eq!(specialization.source(), source);
    assert_eq!(
        specialization.implementation().overload,
        specialization.call_contract().selected().overload
    );
    let cloned = specialization.clone();
    assert!(std::ptr::eq(
        specialization.implementation(),
        cloned.implementation()
    ));
    assert!(std::ptr::eq(
        specialization.call_contract(),
        cloned.call_contract()
    ));
    assert_eq!(cloned.source(), source);
    assert_eq!(cloned.effects(), specialization.effects());
}

pub(super) fn checked_into_parts(
    specialization: PureCallSpecialization,
    input: CallEffectInput<'_>,
    expected: &CallEffects,
) -> PreparedPureKernel {
    assert_borrowed_call(&specialization, input, expected);
    let original = specialization.prepared().clone();
    let effects = specialization.effects();
    let (prepared, retained_effects) = specialization.into_parts();
    assert_eq!(retained_effects, effects);
    assert!(std::ptr::eq(
        prepared.call_contract(),
        original.call_contract()
    ));
    match (&original, &prepared) {
        (PreparedPureKernel::Scalar(a), PreparedPureKernel::Scalar(b)) => {
            assert!(Arc::ptr_eq(a, b))
        }
        (PreparedPureKernel::HigherOrder(a), PreparedPureKernel::HigherOrder(b)) => {
            assert!(Arc::ptr_eq(a, b))
        }
        (PreparedPureKernel::Aggregate(a), PreparedPureKernel::Aggregate(b)) => {
            assert!(Arc::ptr_eq(a.contract(), b.contract()));
            assert_eq!(a.state_layout(), b.state_layout());
        }
        (PreparedPureKernel::Window(a), PreparedPureKernel::Window(b)) => {
            assert!(Arc::ptr_eq(a, b))
        }
        (PreparedPureKernel::Table(a), PreparedPureKernel::Table(b)) => assert!(Arc::ptr_eq(a, b)),
        (PreparedPureKernel::ControlIntrinsic(a), PreparedPureKernel::ControlIntrinsic(b)) => {
            assert!(Arc::ptr_eq(a, b))
        }
        _ => panic!("consuming specialization changed the resolved lifecycle"),
    }
    prepared
}

#[test]
fn actual_builtin_abs_installs_eight_cpu_records_without_sealing_unmigrated_catalogue() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    let definition = builder.definition("abs", FunctionKind::Scalar).unwrap();
    let binding = definition.binding.as_ref().unwrap();
    let attachment = binding.pure.as_ref().expect("actual ABS CPU owner");
    assert_eq!(
        binding.declaration.function_id().as_str(),
        "builtin.scalar/abs/v1"
    );
    assert_eq!(binding.declaration.overloads().len(), 8);
    assert_eq!(attachment.implementations.len(), 8);
    for (overload, installed) in binding
        .declaration
        .overloads()
        .iter()
        .zip(attachment.implementations.iter())
    {
        assert_eq!(installed.overload, overload.identity);
        assert_eq!(
            installed.implementation.as_str(),
            "builtin.scalar/abs/selected-v1"
        );
        assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
        let effects = overload.effects.as_ref().unwrap();
        assert_eq!(effects.instance_state, FunctionInstanceState::None);
        assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
        assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
    }
    // A real migrated family is not coverage of the remaining whole catalogue.
    // Incomplete owners/effects must still fail before manifest reconciliation.
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn actual_builtin_rand_and_random_install_each_exact_cpu_record_and_observable_declaration() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    for name in ["rand", "random"] {
        let definition = builder.definition(name, FunctionKind::Scalar).unwrap();
        let binding = definition.binding.as_ref().unwrap();
        let attachment = binding.pure.as_ref().expect("actual instance-owned RNG");
        assert_eq!(
            binding.declaration.function_id().as_str(),
            format!("builtin.scalar/{name}/v1")
        );
        assert_eq!(binding.declaration.overloads().len(), 2);
        assert_eq!(attachment.implementations.len(), 2);
        for (overload, installed) in binding
            .declaration
            .overloads()
            .iter()
            .zip(attachment.implementations.iter())
        {
            assert_eq!(installed.overload, overload.identity);
            assert_eq!(
                installed.implementation.as_str(),
                format!("builtin.scalar/{name}/selected-v1")
            );
            assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
            let effects = overload.effects.as_ref().unwrap();
            assert_eq!(effects.value_stability, FunctionVolatility::Volatile);
            assert_eq!(
                effects.instance_state,
                FunctionInstanceState::ScalarInstance
            );
            assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
            assert_eq!(effects.null_behavior, FunctionNullBehavior::CalledOnNull);
            assert_eq!(effects.argument_control, ArgumentControl::Eager);
            assert!(effects.observable_effects.rng_sampling);
            assert!(effects.environment_dependencies.is_empty());
        }
    }
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn actual_builtin_numeric_unary_installs_all_182_exact_cpu_records() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    let names = [
        "acos", "asin", "atan", "cbrt", "ceil", "ceiling", "dceil", "cos", "cot", "degress",
        "dlog1", "exp", "dexp", "floor", "dfloor", "ln", "log10", "dlog10", "log2", "radians",
        "sin", "sqrt", "dsqrt", "square", "tan", "positive",
    ];
    let mut installed_count = 0;
    for name in names {
        let definition = builder.definition(name, FunctionKind::Scalar).unwrap();
        let binding = definition.binding.as_ref().unwrap();
        let attachment = binding
            .pure
            .as_ref()
            .expect("actual numeric unary CPU owner");
        assert_eq!(
            binding.declaration.function_id().as_str(),
            format!("builtin.scalar/{name}/v1")
        );
        assert_eq!(binding.declaration.overloads().len(), 7);
        assert_eq!(attachment.implementations.len(), 7);
        for (overload, installed) in binding
            .declaration
            .overloads()
            .iter()
            .zip(attachment.implementations.iter())
        {
            assert_eq!(installed.overload, overload.identity);
            assert_eq!(
                installed.implementation.as_str(),
                format!("builtin.scalar/{name}/selected-v1")
            );
            assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
            let effects = overload.effects.as_ref().unwrap();
            assert_eq!(effects.value_stability, FunctionVolatility::Immutable);
            assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
            assert_eq!(effects.failure_behavior, FunctionFailureBehavior::Propagate);
            assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
            assert_eq!(effects.argument_control, ArgumentControl::Eager);
            assert_eq!(effects.instance_state, FunctionInstanceState::None);
            assert_eq!(effects.observable_effects, ObservableEffects::NONE);
            assert!(effects.environment_dependencies.is_empty());
        }
        installed_count += attachment.implementations.len();
    }
    assert_eq!(installed_count, 182);
    // Declared but unavailable names must not become new product support.
    for name in ["degrees", "sinh", "cosh", "tanh"] {
        assert!(builder.definition(name, FunctionKind::Scalar).is_none());
    }
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn actual_builtin_numeric_binary_installs_all_216_exact_cpu_records() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    let mut installed_count = 0;
    for name in ["atan2", "fmod", "pow", "fpow", "dpow", "power"] {
        let definition = builder.definition(name, FunctionKind::Scalar).unwrap();
        let binding = definition.binding.as_ref().unwrap();
        let attachment = binding
            .pure
            .as_ref()
            .expect("actual binary numeric CPU owner");
        assert_eq!(
            binding.declaration.function_id().as_str(),
            format!("builtin.scalar/{name}/v1")
        );
        assert_eq!(binding.declaration.overloads().len(), 36);
        assert_eq!(attachment.implementations.len(), 36);
        for (overload, installed) in binding
            .declaration
            .overloads()
            .iter()
            .zip(attachment.implementations.iter())
        {
            assert_eq!(installed.overload, overload.identity);
            assert_eq!(
                installed.implementation.as_str(),
                format!("builtin.scalar/{name}/selected-v1")
            );
            assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
            let effects = overload.effects.as_ref().unwrap();
            assert_eq!(effects.value_stability, FunctionVolatility::Immutable);
            assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
            assert_eq!(effects.failure_behavior, FunctionFailureBehavior::Propagate);
            assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
            assert_eq!(effects.argument_control, ArgumentControl::Eager);
            assert_eq!(effects.instance_state, FunctionInstanceState::None);
            assert_eq!(effects.observable_effects, ObservableEffects::NONE);
            assert!(effects.environment_dependencies.is_empty());
        }
        installed_count += attachment.implementations.len();
    }
    assert_eq!(installed_count, 216);
    // Installing a complete module must not authorize a partial whole seal.
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn actual_builtin_mod_and_pmod_install_all_72_exact_cpu_records() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    let mut installed_count = 0;
    for name in ["mod", "pmod"] {
        let definition = builder.definition(name, FunctionKind::Scalar).unwrap();
        let binding = definition.binding.as_ref().unwrap();
        let attachment = binding
            .pure
            .as_ref()
            .expect("actual integer remainder CPU owner");
        assert_eq!(
            binding.declaration.function_id().as_str(),
            format!("builtin.scalar/{name}/v1")
        );
        assert_eq!(binding.declaration.overloads().len(), 36);
        assert_eq!(attachment.implementations.len(), 36);
        for (overload, installed) in binding
            .declaration
            .overloads()
            .iter()
            .zip(attachment.implementations.iter())
        {
            assert_eq!(installed.overload, overload.identity);
            assert_eq!(
                installed.implementation.as_str(),
                format!("builtin.scalar/{name}/selected-v1")
            );
            assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
            let effects = overload.effects.as_ref().unwrap();
            assert_eq!(effects.value_stability, FunctionVolatility::Immutable);
            assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
            assert_eq!(effects.failure_behavior, FunctionFailureBehavior::Propagate);
            assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
            assert_eq!(effects.argument_control, ArgumentControl::Eager);
            assert_eq!(effects.instance_state, FunctionInstanceState::None);
            assert_eq!(effects.observable_effects, ObservableEffects::NONE);
            assert!(effects.environment_dependencies.is_empty());
        }
        installed_count += attachment.implementations.len();
    }
    assert_eq!(installed_count, 72);
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn actual_builtin_log_sign_e_pi_install_all_52_exact_cpu_records() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    let mut installed_count = 0;
    for (name, count) in [("log", 43), ("sign", 7), ("e", 1), ("pi", 1)] {
        let definition = builder.definition(name, FunctionKind::Scalar).unwrap();
        let binding = definition.binding.as_ref().unwrap();
        let attachment = binding
            .pure
            .as_ref()
            .expect("actual elementary numeric CPU owner");
        assert_eq!(
            binding.declaration.function_id().as_str(),
            format!("builtin.scalar/{name}/v1")
        );
        assert_eq!(binding.declaration.overloads().len(), count);
        assert_eq!(attachment.implementations.len(), count);
        for (overload, installed) in binding
            .declaration
            .overloads()
            .iter()
            .zip(attachment.implementations.iter())
        {
            assert_eq!(installed.overload, overload.identity);
            assert_eq!(
                installed.implementation.as_str(),
                format!("builtin.scalar/{name}/selected-v1")
            );
            assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
            let effects = overload.effects.as_ref().unwrap();
            assert_eq!(effects.value_stability, FunctionVolatility::Immutable);
            assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
            assert_eq!(effects.failure_behavior, FunctionFailureBehavior::Propagate);
            assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
            assert_eq!(effects.argument_control, ArgumentControl::Eager);
            assert_eq!(effects.instance_state, FunctionInstanceState::None);
            assert_eq!(effects.observable_effects, ObservableEffects::NONE);
            assert!(effects.environment_dependencies.is_empty());
        }
        installed_count += attachment.implementations.len();
    }
    assert_eq!(installed_count, 52);
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn actual_builtin_dround_installs_all_eight_exact_cpu_records() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    let definition = builder.definition("dround", FunctionKind::Scalar).unwrap();
    let binding = definition.binding.as_ref().unwrap();
    let attachment = binding.pure.as_ref().expect("actual DROUND CPU owner");
    assert_eq!(
        binding.declaration.function_id().as_str(),
        "builtin.scalar/dround/v1"
    );
    assert_eq!(binding.declaration.overloads().len(), 8);
    assert_eq!(attachment.implementations.len(), 8);
    for (overload, installed) in binding
        .declaration
        .overloads()
        .iter()
        .zip(attachment.implementations.iter())
    {
        assert_eq!(installed.overload, overload.identity);
        assert_eq!(
            installed.implementation.as_str(),
            "builtin.scalar/dround/selected-v1"
        );
        assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
        let effects = overload.effects.as_ref().unwrap();
        assert_eq!(effects.value_stability, FunctionVolatility::Immutable);
        assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
        assert_eq!(effects.failure_behavior, FunctionFailureBehavior::Propagate);
        assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
        assert_eq!(effects.argument_control, ArgumentControl::Eager);
        assert_eq!(effects.instance_state, FunctionInstanceState::None);
        assert_eq!(effects.observable_effects, ObservableEffects::NONE);
        assert!(effects.environment_dependencies.is_empty());
    }
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn actual_builtin_truncate_installs_its_exact_dynamic_cpu_record_and_complete_effects() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    let definition = builder
        .definition("truncate", FunctionKind::Scalar)
        .unwrap();
    let binding = definition.binding.as_ref().unwrap();
    let attachment = binding.pure.as_ref().expect("actual TRUNCATE CPU owner");
    assert_eq!(
        binding.declaration.function_id().as_str(),
        "builtin.scalar/truncate/v1"
    );
    assert_eq!(binding.declaration.overloads().len(), 1);
    assert_eq!(attachment.implementations.len(), 1);
    let declared = &binding.declaration.overloads()[0];
    let installed = &attachment.implementations[0];
    assert_eq!(
        declared.identity.as_str(),
        "builtin.scalar/truncate/dynamic-v1"
    );
    assert_eq!(installed.overload, declared.identity);
    assert_eq!(
        installed.implementation.as_str(),
        "builtin.scalar/truncate/selected-v1"
    );
    assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
    let effects = declared.effects.as_ref().unwrap();
    assert_eq!(effects.value_stability, FunctionVolatility::Immutable);
    assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::NoRowError);
    assert_eq!(effects.failure_behavior, FunctionFailureBehavior::Propagate);
    assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
    assert_eq!(effects.argument_control, ArgumentControl::Eager);
    assert_eq!(effects.instance_state, FunctionInstanceState::None);
    assert_eq!(effects.observable_effects, ObservableEffects::NONE);
    assert!(effects.environment_dependencies.is_empty());
    // This complete owner contributes to the whole catalogue; unfinished
    // owners are still refused rather than creating a partial sealed service.
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn actual_builtin_round_installs_its_exact_dynamic_cpu_record_and_complete_effects() {
    let mut builder = EngineFunctionCatalogBuilder::new();
    crate::builtin::catalogue::contribute_builtin_functions(&mut builder).unwrap();
    let definition = builder.definition("round", FunctionKind::Scalar).unwrap();
    let binding = definition.binding.as_ref().unwrap();
    let attachment = binding.pure.as_ref().expect("actual ROUND CPU owner");
    assert_eq!(
        binding.declaration.function_id().as_str(),
        "builtin.scalar/round/v1"
    );
    assert_eq!(binding.declaration.overloads().len(), 1);
    assert_eq!(attachment.implementations.len(), 1);
    let declared = &binding.declaration.overloads()[0];
    let installed = &attachment.implementations[0];
    assert_eq!(
        declared.identity.as_str(),
        "builtin.scalar/round/dynamic-v1"
    );
    assert_eq!(installed.overload, declared.identity);
    assert_eq!(
        installed.implementation.as_str(),
        "builtin.scalar/round/selected-v1"
    );
    assert_eq!(installed.abi, PureKernelAbi::ScalarV1);
    let effects = declared.effects.as_ref().unwrap();
    assert_eq!(effects.value_stability, FunctionVolatility::Immutable);
    assert_eq!(effects.own_row_error, FunctionIntrinsicRowError::MayRaise);
    assert_eq!(effects.failure_behavior, FunctionFailureBehavior::Propagate);
    assert_eq!(effects.null_behavior, FunctionNullBehavior::Strict);
    assert_eq!(effects.argument_control, ArgumentControl::Eager);
    assert_eq!(effects.instance_state, FunctionInstanceState::None);
    assert_eq!(effects.observable_effects, ObservableEffects::NONE);
    assert!(effects.environment_dependencies.is_empty());
    // This complete owner contributes to the whole catalogue; unfinished
    // owners are still refused rather than creating a partial sealed service.
    assert!(matches!(
        builder.seal_pure(std::iter::empty()),
        Err(PureCatalogError::MissingOwner(_)
            | PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(_)))
    ));
}

#[test]
fn metadata_without_actual_owner_and_legacy_effects_cannot_be_pure_sealed() {
    let owner = Arc::new(Owner::new(false));
    let metadata = FunctionDefinition::try_new_bound(
        "catalogue_fixture",
        FunctionVisibility::Public,
        owner.declaration.clone(),
        owner.clone(),
    )
    .unwrap();
    assert!(
        matches!(builder(metadata).seal_pure(installed(false)),Err(PureCatalogError::MissingOwner(function)) if function==id(FUNCTION))
    );
    let mut owner = Owner::new(false);
    let mut legacy = owner.declaration.overloads()[0].clone();
    legacy.effects = None;
    owner.declaration =
        FunctionBindingDeclaration::try_new(id(FUNCTION), FunctionKind::Scalar, [legacy]).unwrap();
    assert!(
        matches!(definition(Arc::new(owner),false),Err(PureCatalogError::Binding(FunctionBindingError::MissingEffectDeclaration(overload))) if overload==overload_id(SCALAR))
    );
    let owner = Arc::new(Owner::new(false));
    let mut legacy = owner.declaration.overloads()[0].clone();
    legacy.effects = None;
    let declaration =
        FunctionBindingDeclaration::try_new(id(FUNCTION), FunctionKind::Scalar, [legacy]).unwrap();
    let metadata = FunctionDefinition::try_new_bound(
        "catalogue_fixture",
        FunctionVisibility::Public,
        declaration,
        owner,
    )
    .unwrap();
    assert!(matches!(
        builder(metadata).seal_pure(installed(false)),
        Err(PureCatalogError::Binding(
            FunctionBindingError::MissingEffectDeclaration(_)
        ))
    ));
}
#[test]
fn atomic_registration_requires_complete_unique_overload_coverage_and_compatible_kind_control_abi()
{
    for mutation in 0..4 {
        let mut owner = Owner::new(false);
        match mutation {
            0 => owner.implementations.clear(),
            1 => owner.implementations.push(PureImplementationDeclaration {
                overload: overload_id(HIGHER),
                implementation: implementation(CODE),
                abi: PureKernelAbi::ScalarV1,
            }),
            2 => owner.implementations.push(owner.implementations[0].clone()),
            _ => {
                owner.implementations[0].overload =
                    overload_id("fixture/pure-catalogue/unregistered")
            }
        }
        assert!(matches!(
            definition(Arc::new(owner), false),
            Err(PureCatalogError::ImplementationCoverage(_))
        ));
    }
    let mut owner = Owner::new(false);
    owner.implementations[0].abi = PureKernelAbi::HigherOrderV1;
    assert!(matches!(
        definition(Arc::new(owner), false),
        Err(PureCatalogError::InvalidAbi { .. })
    ));
    let mut owner = Owner::new(false);
    let mut control = base(ArgumentControl::If);
    control.null_behavior = FunctionNullBehavior::ControlDefined;
    owner.declaration = FunctionBindingDeclaration::try_new_complete(
        id(FUNCTION),
        FunctionKind::Scalar,
        [overload(SCALAR, control)],
    )
    .unwrap();
    assert!(matches!(
        definition(Arc::new(owner), false),
        Err(PureCatalogError::InvalidAbi { .. })
    ));
    let mut owner = Owner::new(false);
    let mut table = base(ArgumentControl::Table);
    table.instance_state = FunctionInstanceState::TableInstance;
    owner.declaration = FunctionBindingDeclaration::try_new_complete(
        id(FUNCTION),
        FunctionKind::Table,
        [overload(SCALAR, table)],
    )
    .unwrap();
    assert!(matches!(
        definition(Arc::new(owner), false),
        Err(PureCatalogError::InvalidAbi { .. })
    ));
    assert!(matches!(
        definition(Arc::new(Owner::new(true)), false),
        Err(PureCatalogError::InvalidAbi { .. })
    ));
}
#[test]
fn independent_installed_manifest_rejects_missing_extra_duplicate_and_every_exact_record_mismatch()
{
    for mutation in 0..8 {
        let owner = Arc::new(Owner::new(false));
        let definition = definition(owner, false).unwrap();
        let mut rows = installed(false);
        match mutation {
            0 => rows.clear(),
            1 => {
                let mut extra = rows[0].clone();
                extra.function = id("fixture/pure-catalogue/unexpected");
                rows.push(extra)
            }
            2 => rows.push(rows[0].clone()),
            3 => {
                rows[0].implementation.implementation =
                    implementation("fixture/pure-catalogue/wrong-cpu")
            }
            4 => rows[0].implementation.abi = PureKernelAbi::HigherOrderV1,
            5 => rows[0].kind = FunctionKind::Window,
            6 => {
                rows[0].aggregate_state_format = Some(
                    AggregateStateFormatIdentity::try_new("fixture/pure-catalogue/stray-state")
                        .unwrap(),
                )
            }
            _ => {
                rows[0].implementation.overload =
                    overload_id("fixture/pure-catalogue/wrong-overload")
            }
        }
        assert!(matches!(
            builder(definition).seal_pure(rows),
            Err(PureCatalogError::InstalledManifestMismatch)
        ));
    }
    let catalog = catalogue(Arc::new(Owner::new(false)), false);
    assert!(catalog.metadata().definition_by_id(&id(FUNCTION)).is_some());
}
#[test]
fn same_actual_owner_resolves_refines_and_prepares_fresh_and_frozen_and_scalar_cpu_computes_values()
{
    let owner = Arc::new(Owner::new(false));
    let catalog = catalogue(owner.clone(), false);
    let arguments = scalar_arguments();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: 1,
    };
    let bound = catalog
        .metadata()
        .resolve_bound_user(
            "CATALOGUE_FIXTURE",
            FunctionKind::Scalar,
            request,
            crate::binding_test_control(),
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let parameters = SemanticParameters::default();
    let uses = [Some(ExpressionUseId::new(1))];
    let input = call_input(
        &bound.function_id,
        &selected,
        &arguments,
        &uses,
        &parameters,
    );
    let fresh = catalog
        .prepare_fresh(
            input,
            selected.clone(),
            scalar_options(),
            &CompileControl::default(),
        )
        .unwrap();
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.instances.load(Ordering::Relaxed), 0);
    let frozen = owner.frozen(&selected);
    owner.counts.refine.store(0, Ordering::Relaxed);
    owner.counts.prepare.store(0, Ordering::Relaxed);
    let checked = catalog
        .prepare_frozen(
            input,
            selected.clone(),
            &frozen,
            scalar_options(),
            &CompileControl::default(),
        )
        .unwrap();
    assert_eq!(fresh.effects(), checked.effects());
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 1);
    assert!(
        owner
            .counts
            .addresses
            .lock()
            .unwrap()
            .iter()
            .all(|address| *address == Arc::as_ptr(&owner) as usize)
    );
    assert_preparation_provenance(
        &fresh,
        &owner.implementations[0],
        PurePreparationSource::Fresh,
    );
    assert_preparation_provenance(
        &checked,
        &owner.implementations[0],
        PurePreparationSource::Frozen,
    );
    checked_into_parts(fresh, input, &frozen);
    let PreparedPureKernel::Scalar(prepared) = checked_into_parts(checked, input, &frozen) else {
        panic!("wrong installed lifecycle")
    };
    assert!(Arc::ptr_eq(
        prepared.contract().call().selected_owner(),
        &selected
    ));
    let mut instance = ScalarEvaluationInstance::instantiate(prepared).unwrap();
    let array: ArrayRef = Arc::new(Int64Array::from(vec![100, 2, 200, 4]));
    let values = [EvaluatedArgument::Column(&array)];
    let rows = [1, 3];
    let selection = Selection::try_sparse(4, &rows).unwrap();
    let output = instance
        .evaluate(selection, &values, &RuntimeControl)
        .unwrap();
    assert_eq!(output.selection(), selection);
    assert_eq!(
        output
            .values()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values(),
        &[13, 19]
    );
    assert!(output.errors().is_empty());
    assert_eq!(owner.counts.instances.load(Ordering::Relaxed), 1);
}
#[test]
fn mixed_scalar_higher_order_owner_chooses_exact_overload_abi_without_cross_lifecycle_options() {
    let owner = Arc::new(Owner::new(true));
    let catalog = catalogue(owner.clone(), true);
    let arguments = higher_arguments();
    let request = FunctionBindingRequest {
        expected_result_type: None,
        arguments: &arguments,
        logical_argument_count: 2,
    };
    let bound = catalog
        .metadata()
        .resolve_bound_user(
            "catalogue_fixture",
            FunctionKind::Scalar,
            request,
            crate::binding_test_control(),
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    assert_eq!(selected.overload, overload_id(HIGHER));
    let uses = [Some(ExpressionUseId::new(1)), Some(ExpressionUseId::new(2))];
    let parameters = SemanticParameters::default();
    let input = call_input(
        &bound.function_id,
        &selected,
        &arguments,
        &uses,
        &parameters,
    );
    assert!(matches!(
        catalog.prepare_fresh(
            input,
            selected.clone(),
            scalar_options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 0);
    let fresh = catalog
        .prepare_fresh(
            input,
            selected.clone(),
            higher_options(),
            &CompileControl::default(),
        )
        .unwrap();
    let PreparedPureKernel::HigherOrder(prepared) = fresh.prepared() else {
        panic!("wrong exact ABI")
    };
    assert!(Arc::ptr_eq(
        prepared.body_contract().call(),
        prepared.contract()
    ));
    assert!(Arc::ptr_eq(
        prepared.contract().call().selected_owner(),
        &selected
    ));
    let frozen = owner.frozen(&selected);
    let checked = catalog
        .prepare_frozen(
            input,
            selected.clone(),
            &frozen,
            higher_options(),
            &CompileControl::default(),
        )
        .unwrap();
    assert_eq!(fresh.effects(), checked.effects());
    assert_preparation_provenance(
        &fresh,
        &owner.implementations[1],
        PurePreparationSource::Fresh,
    );
    assert_preparation_provenance(
        &checked,
        &owner.implementations[1],
        PurePreparationSource::Frozen,
    );
    checked_into_parts(fresh, input, &frozen);
    checked_into_parts(checked, input, &frozen);
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.instances.load(Ordering::Relaxed), 0);
}
#[test]
fn unknown_ids_overloads_and_inaccurate_frozen_selected_kind_options_fail_before_cpu_prepare() {
    let owner = Arc::new(Owner::new(false));
    let catalog = catalogue(owner.clone(), false);
    let arguments = scalar_arguments();
    let selected = Arc::new(
        owner
            .resolve(
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
                crate::binding_test_control(),
            )
            .unwrap(),
    );
    let parameters = SemanticParameters::default();
    let uses = [Some(ExpressionUseId::new(1))];
    let function = id(FUNCTION);
    let input = call_input(&function, &selected, &arguments, &uses, &parameters);
    let unknown = id("fixture/pure-catalogue/unknown-function");
    assert!(matches!(
        catalog.prepare_fresh(
            CallEffectInput {
                function_id: &unknown,
                ..input
            },
            selected.clone(),
            scalar_options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownFunction
        ))
    ));
    let mut unknown_selected = (*selected).clone();
    unknown_selected.overload = overload_id("fixture/pure-catalogue/unknown-overload");
    let unknown_selected = Arc::new(unknown_selected);
    assert!(matches!(
        catalog.prepare_fresh(
            CallEffectInput {
                selected: &unknown_selected,
                ..input
            },
            unknown_selected.clone(),
            scalar_options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::Binding(
            FunctionBindingError::UnknownOverload(_)
        ))
    ));
    assert!(matches!(
        catalog.prepare_fresh(
            input,
            Arc::new((*selected).clone()),
            scalar_options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    assert!(matches!(
        catalog.prepare_fresh(
            CallEffectInput {
                kind: FunctionKind::Window,
                ..input
            },
            selected.clone(),
            scalar_options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    assert!(matches!(
        catalog.prepare_fresh(
            input,
            selected.clone(),
            higher_options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    let mut frozen = owner.frozen(&selected);
    frozen.own_row_error = FunctionIntrinsicRowError::MayRaise;
    assert!(
        catalog
            .prepare_frozen(
                input,
                selected.clone(),
                &frozen,
                scalar_options(),
                &CompileControl::default()
            )
            .is_err()
    );
    let mut bad = (*selected).clone();
    bad.result_type = FunctionResultType::Scalar(FunctionValueType::new(DataType::Int32, false));
    let bad = Arc::new(bad);
    assert!(
        catalog
            .prepare_fresh(
                CallEffectInput {
                    selected: &bad,
                    ..input
                },
                bad.clone(),
                scalar_options(),
                &CompileControl::default()
            )
            .is_err()
    );
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 1);
}
#[test]
fn registered_metadata_cannot_hide_different_owner_effect_source_and_control_types_are_preserved() {
    let mut source = Owner::new(false);
    let mut changed = base(ArgumentControl::Eager);
    changed.own_row_error = FunctionIntrinsicRowError::MayRaise;
    source.source_override = Some(changed);
    let owner = Arc::new(source);
    let catalog = catalogue(owner.clone(), false);
    let arguments = scalar_arguments();
    let selected = Arc::new(
        owner
            .resolve(
                FunctionBindingRequest {
                    expected_result_type: None,
                    arguments: &arguments,
                    logical_argument_count: 1,
                },
                crate::binding_test_control(),
            )
            .unwrap(),
    );
    let parameters = SemanticParameters::default();
    let uses = [Some(ExpressionUseId::new(1))];
    let function = id(FUNCTION);
    let input = call_input(&function, &selected, &arguments, &uses, &parameters);
    assert!(
        catalog
            .prepare_fresh(
                input,
                selected.clone(),
                scalar_options(),
                &CompileControl::default()
            )
            .is_err()
    );
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    for failure in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        for positive in [false, true] {
            let owner = Arc::new(Owner::new(false));
            let catalog = catalogue(owner.clone(), false);
            let selected = Arc::new(
                owner
                    .resolve(
                        FunctionBindingRequest {
                            expected_result_type: None,
                            arguments: &arguments,
                            logical_argument_count: 1,
                        },
                        crate::binding_test_control(),
                    )
                    .unwrap(),
            );
            let input = call_input(&function, &selected, &arguments, &uses, &parameters);
            let result = catalog.prepare_frozen(
                input,
                selected.clone(),
                &owner.frozen(&selected),
                scalar_options(),
                &CompileControl {
                    failure: Some(failure),
                    positive,
                },
            );
            assert!(
                matches!(result,Err(FunctionSpecializationFailure::Control(error)) if error==failure)
            );
            assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
            if !positive {
                assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 0);
            }
        }
    }
}
#[test]
fn actual_catalogue_digest_records_pure_presence_implementation_identity_and_canonical_order() {
    let owner = Arc::new(Owner::new(false));
    let pure = catalogue(owner.clone(), false);
    let legacy = builder(
        FunctionDefinition::try_new_bound(
            "catalogue_fixture",
            FunctionVisibility::Public,
            owner.declaration.clone(),
            owner.clone(),
        )
        .unwrap(),
    )
    .seal_bound()
    .unwrap();
    assert_ne!(pure.digest(), legacy.digest());
    let mut changed = Owner::new(false);
    changed.implementations[0].implementation = implementation("fixture/pure-catalogue/cpu-v2");
    let mut manifest = installed(false);
    manifest[0].implementation.implementation = implementation("fixture/pure-catalogue/cpu-v2");
    let changed = builder(definition(Arc::new(changed), false).unwrap())
        .seal_pure(manifest)
        .unwrap();
    assert_ne!(pure.digest(), changed.digest());
    let canonical = catalogue(Arc::new(Owner::new(true)), true);
    let mut reverse = Owner::new(true);
    reverse.implementations.reverse();
    let mut declared = reverse.declaration.overloads().to_vec();
    declared.reverse();
    reverse.declaration =
        FunctionBindingDeclaration::try_new_complete(id(FUNCTION), FunctionKind::Scalar, declared)
            .unwrap();
    let mut manifest = installed(true);
    manifest.reverse();
    let reverse = builder(definition(Arc::new(reverse), true).unwrap())
        .seal_pure(manifest)
        .unwrap();
    assert_eq!(canonical.digest(), reverse.digest());
}

fn control_owner(effects: FunctionEffectDeclaration) -> Owner {
    let mut owner = Owner::new(false);
    owner.declaration = FunctionBindingDeclaration::try_new_complete(
        id(FUNCTION),
        FunctionKind::Scalar,
        [overload(SCALAR, effects)],
    )
    .unwrap();
    owner.implementations = vec![PureImplementationDeclaration {
        overload: overload_id(SCALAR),
        implementation: implementation("fixture/pure-catalogue/control-v1"),
        abi: PureKernelAbi::ControlIntrinsicV1,
    }];
    owner
}
fn coalesce_effects() -> FunctionEffectDeclaration {
    let mut effects = base(ArgumentControl::Coalesce);
    effects.null_behavior = FunctionNullBehavior::ControlDefined;
    effects
}

#[test]
fn exact_control_intrinsic_coalesce_fresh_frozen_keeps_descriptor_and_skips_ordinary_cpu_prepare() {
    let owner = Arc::new(control_owner(coalesce_effects()));
    let definition = FunctionDefinition::try_new_pure_control(
        "catalogue_fixture",
        FunctionVisibility::Public,
        owner.clone(),
    )
    .unwrap();
    let manifest = [InstalledPureKernel {
        function: id(FUNCTION),
        kind: FunctionKind::Scalar,
        implementation: PureImplementationDeclaration {
            overload: overload_id(SCALAR),
            implementation: implementation("fixture/pure-catalogue/control-v1"),
            abi: PureKernelAbi::ControlIntrinsicV1,
        },
        aggregate_state_format: None,
    }];
    let catalog = builder(definition).seal_pure(manifest).unwrap();
    let arguments = scalar_arguments();
    let bound = catalog
        .metadata()
        .resolve_bound_user(
            "catalogue_fixture",
            FunctionKind::Scalar,
            FunctionBindingRequest {
                expected_result_type: None,
                arguments: &arguments,
                logical_argument_count: 1,
            },
            crate::binding_test_control(),
        )
        .unwrap();
    let selected = Arc::new(bound.selected);
    let uses = [Some(ExpressionUseId::new(1))];
    let parameters = SemanticParameters::default();
    let input = call_input(
        &bound.function_id,
        &selected,
        &arguments,
        &uses,
        &parameters,
    );
    let options = || PureCallPreparation::ControlIntrinsic {
        arguments: ScopedExpressionEffects::pure_value(context()),
    };
    let fresh = catalog
        .prepare_fresh(
            input,
            selected.clone(),
            options(),
            &CompileControl::default(),
        )
        .unwrap();
    let frozen_facts = owner.frozen(&selected);
    let frozen = catalog
        .prepare_frozen(
            input,
            selected.clone(),
            &frozen_facts,
            options(),
            &CompileControl::default(),
        )
        .unwrap();
    assert_eq!(fresh.effects(), frozen.effects());
    for (specialization, source) in [
        (&fresh, PurePreparationSource::Fresh),
        (&frozen, PurePreparationSource::Frozen),
    ] {
        assert_preparation_provenance(specialization, &owner.implementations[0], source);
        assert_borrowed_call(specialization, input, &frozen_facts);
        let PreparedPureKernel::ControlIntrinsic(contract) = specialization.prepared() else {
            panic!("control ABI must return its exact intrinsic descriptor")
        };
        assert!(Arc::ptr_eq(contract.selected_owner(), &selected));
        assert_eq!(contract.effects(), &frozen_facts);
        assert_eq!(contract.context(), context());
        assert_eq!(contract.function_id(), &id(FUNCTION));
        assert_eq!(
            contract.effects().argument_control,
            ArgumentControl::Coalesce
        );
        assert_eq!(
            contract.effects().null_behavior,
            FunctionNullBehavior::ControlDefined
        );
        assert_eq!(
            contract.effects().own_row_error,
            FunctionIntrinsicRowError::NoRowError
        );
        assert_eq!(
            contract.effects().instance_state,
            FunctionInstanceState::None
        );
    }
    assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 1);
    assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 2);
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    assert_eq!(owner.counts.instances.load(Ordering::Relaxed), 0);
    assert!(
        owner
            .counts
            .addresses
            .lock()
            .unwrap()
            .iter()
            .all(|address| *address == Arc::as_ptr(&owner) as usize)
    );
    checked_into_parts(fresh, input, &frozen_facts);
    checked_into_parts(frozen, input, &frozen_facts);
    // An ordinary scalar option cannot route a control descriptor to its CPU.
    assert!(matches!(
        catalog.prepare_fresh(
            input,
            selected.clone(),
            scalar_options(),
            &CompileControl::default()
        ),
        Err(FunctionSpecializationFailure::InvalidInput(_))
    ));
    assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
}

#[test]
fn control_intrinsic_registration_rejects_own_errors_state_observables_environment_and_stability() {
    let mut invalid = vec![];
    let mut errors = coalesce_effects();
    errors.own_row_error = FunctionIntrinsicRowError::MayRaise;
    invalid.push(errors);
    let mut state = coalesce_effects();
    state.instance_state = FunctionInstanceState::ScalarInstance;
    invalid.push(state);
    let mut observable = coalesce_effects();
    observable.observable_effects.warnings = true;
    invalid.push(observable);
    let mut environment = coalesce_effects();
    environment.environment_dependencies =
        Box::from([novarocks_type_contract::SemanticParameterKey::TimeZone]);
    invalid.push(environment);
    let mut stability = coalesce_effects();
    stability.value_stability = FunctionVolatility::Stable;
    invalid.push(stability);
    for effects in invalid {
        let owner = Arc::new(control_owner(effects));
        assert!(matches!(
            FunctionDefinition::try_new_pure_control(
                "catalogue_fixture",
                FunctionVisibility::Public,
                owner.clone()
            ),
            Err(PureCatalogError::InvalidAbi { .. })
        ));
        assert_eq!(owner.counts.resolve.load(Ordering::Relaxed), 0);
        assert_eq!(owner.counts.refine.load(Ordering::Relaxed), 0);
        assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    }
}

#[test]
fn selected_owner_control_remains_a_top_level_specialization_failure() {
    for error in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let owner = Arc::new(Owner::new(false));
        let catalog = catalogue(owner.clone(), false);
        let arguments = scalar_arguments();
        let selected = Arc::new(
            owner
                .resolve(
                    FunctionBindingRequest {
                        expected_result_type: None,
                        arguments: &arguments,
                        logical_argument_count: 1,
                    },
                    crate::binding_test_control(),
                )
                .unwrap(),
        );
        *owner.binding_failure.lock().unwrap() = Some(error);
        let parameters = SemanticParameters::default();
        let uses = [Some(ExpressionUseId::new(1))];
        let function = id(FUNCTION);
        let input = call_input(&function, &selected, &arguments, &uses, &parameters);
        assert!(matches!(crate::specialize_scalar(
            owner.as_ref(), input, selected.clone(), ScopedExpressionEffects::pure_value(context()),
            &CompileControl::default(),
        ), Err(FunctionSpecializationFailure::Control(actual)) if actual == error));
        assert!(
            matches!(catalog.prepare_fresh(input, selected.clone(), scalar_options(), &CompileControl::default()), Err(FunctionSpecializationFailure::Control(actual)) if actual == error)
        );
        assert_eq!(owner.counts.prepare.load(Ordering::Relaxed), 0);
    }
}
