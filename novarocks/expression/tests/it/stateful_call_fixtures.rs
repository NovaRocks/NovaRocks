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

//! Selected-state and byte-source fixture inputs authored as checked units.

use arrow::{
    array::{ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use novarocks_constant_contract::{
    ConstantPolicy, ConstantPoolId, ConstantPools, ConstantReference,
};
use novarocks_expression::{
    CompiledExprSet, EvaluationEnvironment, ScopeLayouts,
    testkit::{
        ClaimAuthor, CountingAllocator, HarnessControl, add_call, checked_constants,
        compile_fixture_unit, declare_call_controls, finish_unit, fixture_scope,
        register_call_requests,
    },
};
use novarocks_expression_contract::{
    BoundExprSetBuilder, ExprId, ExprKind, ExprPort, RootOrdinal, RootScope, UnitLimits, ValueId,
};
use novarocks_failure_contract::{FailureClass, FailureProjection, FailureReason, domain};
use novarocks_functions::{
    AggregateStateAllocator, EngineFunctionCatalog, EngineFunctionCatalogBuilder, FunctionArgument,
    FunctionKind, InstalledPureKernel, KernelEvaluationControl, KernelFailure,
    PureEngineFunctionCatalog,
    builtin::catalogue::{
        build_builtin_engine_function_catalog, ds_hll_scalar_private_test_catalog,
        percentile_raw_private_test_catalog,
    },
    opaque_memory::OpaqueAllocationHost,
};
use novarocks_type_contract::{
    CompileCheckpoints, CompilePhase, ControlShape, EvaluationDemand, FunctionValueType,
    SemanticParameters,
};
use std::{
    alloc::Layout,
    collections::BTreeMap,
    num::NonZeroU16,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

pub(super) struct StatefulFixture {
    pub(super) unit: Arc<CompiledExprSet>,
    pub(super) root: RootOrdinal,
}
impl StatefulFixture {
    pub(super) fn schema(&self) -> &SchemaRef {
        self.unit.scope(RootScope::Port(0)).unwrap().schema()
    }

    pub(super) fn batch(&self, arrays: Vec<ArrayRef>) -> RecordBatch {
        RecordBatch::try_new(self.schema().clone(), arrays).unwrap()
    }
}

// The original FB+LC fixture compiled under this small constant policy.
const POLICY: ConstantPolicy = ConstantPolicy {
    max_rows: 16,
    max_array_nodes: 128,
    max_logical_elements: 1024,
    max_retained_buffer_bytes: 1 << 20,
    max_type_depth: 64,
    max_type_nodes: 4096,
    max_dictionary_depth: 64,
    max_metadata_bytes: 1 << 20,
    max_library_validation_work: 1 << 20,
    max_library_validation_bytes: 1 << 20,
};

fn functions(
    actual: &EngineFunctionCatalog,
    declarations: &[(&str, Option<usize>)],
) -> PureEngineFunctionCatalog {
    let mut builder = EngineFunctionCatalogBuilder::new();
    let mut installed = Vec::new();
    for &(name, count) in declarations {
        let definition = actual.definition(name, FunctionKind::Scalar).unwrap();
        let declaration = definition.binding_declaration().unwrap();
        if let Some(count) = count {
            assert_eq!(declaration.overloads().len(), count);
        }
        for overload in declaration.overloads() {
            let entry = actual
                .pure_overload_declaration_observed(
                    declaration.function_id(),
                    declaration.kind(),
                    &overload.identity,
                    &HarnessControl,
                )
                .unwrap();
            installed.push(InstalledPureKernel {
                function: declaration.function_id().clone(),
                kind: declaration.kind(),
                implementation: entry.implementation().clone(),
                aggregate_state_format: overload
                    .aggregate
                    .as_ref()
                    .map(|record| record.state_format.clone()),
            });
        }
        builder.register(definition.clone()).unwrap();
    }
    builder.seal_pure(installed).unwrap()
}

fn input_unit(types: &[FunctionValueType]) -> (BoundExprSetBuilder, ExprPort, Vec<ExprId>) {
    let mut draft = BoundExprSetBuilder::new(UnitLimits {
        expression_semantic_depth: 256,
    });
    let values = types
        .iter()
        .enumerate()
        .map(|(i, ty)| (ValueId::new(91 + u32::try_from(i).unwrap()), ty.clone()))
        .collect::<Box<[_]>>();
    let children = values
        .iter()
        .map(|(value, ty)| {
            draft
                .add(None, ty.clone(), ExprKind::Value(*value))
                .unwrap()
        })
        .collect();
    (draft, ExprPort { values }, children)
}

fn call_author(
    catalogue: &PureEngineFunctionCatalog,
    name: &str,
    args: &[FunctionValueType],
    shape: ControlShape,
) -> ClaimAuthor {
    ClaimAuthor::with_legacy_binding(
        catalogue,
        name,
        args.iter()
            .map(|ty| FunctionArgument::Value {
                value_type: ty.clone(),
                constant: None,
            })
            .collect(),
        shape,
        None,
        POLICY,
        &HarnessControl,
    )
}

fn checked_fixture(
    catalogue: &PureEngineFunctionCatalog,
    mut draft: BoundExprSetBuilder,
    port: ExprPort,
    authors: &BTreeMap<ExprId, ClaimAuthor>,
    mut constants: ConstantPools,
    result: ExprId,
) -> StatefulFixture {
    let root = draft
        .root(result, EvaluationDemand::Value, RootScope::Port(0))
        .unwrap();
    register_call_requests(&mut draft, authors, &mut constants);
    let mut work = CompileCheckpoints::try_new(&HarnessControl, CompilePhase::Validate).unwrap();
    declare_call_controls(&mut draft, authors, &mut work);

    // The original physical input was the first Project after zero-column
    // Values. Those non-result columns were named local_1_{ordinal}.
    let fields = port
        .values
        .iter()
        .enumerate()
        .map(|(i, (_, ty))| {
            Field::new(
                format!("local_1_{i}"),
                ty.data_type().clone(),
                ty.nullable(),
            )
        })
        .collect::<Vec<_>>();
    let scope = fixture_scope(&port, Arc::new(Schema::new(fields)));
    let parameters = SemanticParameters::try_new([]).unwrap();
    let sealed = draft.seal(vec![port], &mut work).unwrap();
    let unit = finish_unit(
        catalogue,
        sealed,
        authors,
        &parameters,
        &mut work,
        |_, _, effects| effects,
    );
    work.finish().unwrap();
    let compiled = compile_fixture_unit(
        &unit,
        EvaluationEnvironment {
            constants: &constants,
            parameters: &parameters,
        },
        ScopeLayouts {
            ports: std::slice::from_ref(&scope),
            all_ports: Some(&scope),
        },
        POLICY,
        catalogue,
    );
    StatefulFixture {
        unit: compiled,
        root,
    }
}

fn direct_call(
    catalogue: &PureEngineFunctionCatalog,
    name: &str,
    inputs: &[FunctionValueType],
    result_type: DataType,
) -> StatefulFixture {
    let (mut draft, port, children) = input_unit(inputs);
    let owner = call_author(catalogue, name, inputs, ControlShape::Eager);
    assert_eq!(owner.result().data_type(), &result_type);
    let mut authors = BTreeMap::new();
    let result = add_call(&mut draft, &mut authors, owner, children);
    checked_fixture(
        catalogue,
        draft,
        port,
        &authors,
        ConstantPools::empty(),
        result,
    )
}

#[derive(Clone, Copy)]
pub(super) enum DsCatalogue {
    BuiltIn,
    Private,
}
impl DsCatalogue {
    fn engine(self) -> EngineFunctionCatalog {
        match self {
            Self::BuiltIn => build_builtin_engine_function_catalog().unwrap(),
            Self::Private => ds_hll_scalar_private_test_catalog(),
        }
    }
}
pub(super) fn ds_program(catalogue: DsCatalogue, count: usize) -> StatefulFixture {
    let catalogue = functions(
        &catalogue.engine(),
        &[("ds_hll_count_distinct_state", Some(3))],
    );
    let types = (0..count)
        .map(|i| {
            FunctionValueType::new(
                if i == 2 {
                    DataType::Utf8
                } else {
                    DataType::Int64
                },
                true,
            )
        })
        .collect::<Vec<_>>();
    direct_call(
        &catalogue,
        "ds_hll_count_distinct_state",
        &types,
        DataType::Binary,
    )
}

pub(super) fn ds_if_program(catalogue: DsCatalogue, target: DataType) -> StatefulFixture {
    let catalogue = functions(
        &catalogue.engine(),
        &[("ds_hll_count_distinct_state", None), ("if", None)],
    );
    let types = [
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(DataType::Int64, true),
        FunctionValueType::new(target, true),
        FunctionValueType::new(DataType::Boolean, true),
    ];
    let (mut draft, port, children) = input_unit(&types);
    let mut authors = BTreeMap::new();
    let owner = call_author(
        &catalogue,
        "ds_hll_count_distinct_state",
        &types[..3],
        ControlShape::Eager,
    );
    let result_type = owner.result();
    assert_eq!(result_type.data_type(), &DataType::Binary);
    let yes = add_call(&mut draft, &mut authors, owner, children[..3].to_vec());
    let fallback = checked_constants::null(&result_type, POLICY);
    // The original constant table registered four predecessor placeholders
    // before this used Binary NULL. Keep its original reference and ordinal.
    let pool = ConstantPoolId::new(4);
    let mut constants = ConstantPools::empty();
    constants.insert(pool, fallback.pool().clone()).unwrap();
    let no = draft
        .add(
            None,
            result_type.clone(),
            ExprKind::Constant(ConstantReference {
                pool,
                ordinal: fallback.ordinal(),
            }),
        )
        .unwrap();
    let conditional = call_author(
        &catalogue,
        "if",
        &[types[3].clone(), result_type.clone(), result_type],
        ControlShape::If,
    );
    let result = add_call(
        &mut draft,
        &mut authors,
        conditional,
        vec![children[3], yes, no],
    );
    checked_fixture(&catalogue, draft, port, &authors, constants, result)
}

pub(super) fn ds_if_type() -> DataType {
    DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
}

pub(super) fn ds_batch(program: &StatefulFixture, count: usize) -> RecordBatch {
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(-7)]))];
    if count >= 2 {
        arrays.push(Arc::new(Int64Array::from(vec![Some(10), Some(999), None])));
    }
    if count >= 3 {
        arrays.push(Arc::new(StringArray::from(vec![
            Some("HLL_6"),
            None,
            Some("HLL_6"),
        ])));
    }
    program.batch(arrays)
}

pub(super) fn ds_if_batch(program: &StatefulFixture) -> RecordBatch {
    use arrow::array::builder::{ListBuilder, StringBuilder};
    let mut builder = ListBuilder::new(StringBuilder::new());
    for _ in 0..3 {
        for _ in 0..90 {
            builder.values().append_value("full-original-诊断");
        }
        builder.append(true);
    }
    program.batch(vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Int64Array::from(vec![17, 17, 17])),
        Arc::new(builder.finish()),
        Arc::new(BooleanArray::from(vec![false, true, false])),
    ])
}

pub(super) fn ds_if_row_data_batch(program: &StatefulFixture) -> RecordBatch {
    program.batch(vec![
        Arc::new(Int64Array::from(vec![1, 2, 3])),
        Arc::new(Int64Array::from(vec![17, 999, 17])),
        Arc::new(StringArray::from(vec!["HLL_6", "HLL_6", "HLL_6"])),
        Arc::new(BooleanArray::from(vec![false, true, false])),
    ])
}

pub(super) fn percentile_program() -> StatefulFixture {
    let catalogue = functions(
        &percentile_raw_private_test_catalog(),
        &[("percentile_approx_raw", Some(1))],
    );
    direct_call(
        &catalogue,
        "percentile_approx_raw",
        &[
            FunctionValueType::new(DataType::Binary, true),
            FunctionValueType::new(DataType::Float64, true),
        ],
        DataType::Float64,
    )
}
pub(super) fn percentile_arrays() -> Vec<ArrayRef> {
    use novarocks_functions::approx_percentile_core::{PercentileState, add_value, encode_state};
    let mut state = PercentileState::default();
    for value in [1.0, 2.0, 3.0, 4.0] {
        add_value(&mut state, value).unwrap();
    }
    let bytes = encode_state(&state).expect("percentile frame fixture");
    vec![
        Arc::new(BinaryArray::from(vec![
            Some(b"bad".as_slice()),
            None,
            Some(bytes.as_slice()),
        ])),
        Arc::new(Float64Array::from(vec![0.5, 0.5, 0.5])),
    ]
}
pub(super) fn percentile_batch(program: &StatefulFixture) -> RecordBatch {
    program.batch(percentile_arrays())
}

pub(super) fn binary_program(count: usize) -> StatefulFixture {
    let actual = build_builtin_engine_function_catalog().unwrap();
    direct_call(
        &functions(&actual, &[("to_binary", Some(3))]),
        "to_binary",
        &vec![FunctionValueType::new(DataType::Utf8, true); count],
        DataType::Binary,
    )
}
pub(super) fn binary_batch(program: &StatefulFixture, count: usize) -> RecordBatch {
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(vec![
        Some("ff"),
        Some(""),
        Some("YWJj"),
        None,
        Some("00"),
        Some("!"),
    ]))];
    if count == 2 {
        arrays.push(Arc::new(StringArray::from(vec![
            None,
            Some("utf8"),
            Some("encode64"),
            Some("utf8"),
            Some("unknown"),
            Some("encode64"),
        ])));
    }
    program.batch(arrays)
}
pub(super) fn binary_values(array: &ArrayRef) -> Vec<Option<Vec<u8>>> {
    array
        .as_any()
        .downcast_ref::<BinaryArray>()
        .unwrap()
        .iter()
        .map(|v| v.map(<[u8]>::to_vec))
        .collect()
}

const FIXTURE_CAPACITY: FailureReason = FailureReason::new(domain::EXPRESSION, NonZeroU16::MAX);

pub(super) fn capacity_refusing_allocator(limit: usize) -> Arc<CountingAllocator> {
    Arc::new(CountingAllocator::with_capacity_reason(
        limit,
        FIXTURE_CAPACITY,
    ))
}

pub(super) fn assert_fixture_capacity(
    failure: &(impl std::error::Error + FailureProjection + 'static),
    limit: u64,
) {
    let fact = failure.failure_fact();
    assert_eq!(fact.class(), FailureClass::Resource);
    assert_eq!(fact.reason(), FIXTURE_CAPACITY);
    assert!(fact.representation().is_none());
    let resource = fact.resource().unwrap();
    assert_eq!(resource.limit, Some(limit));
    assert!(resource.requested.unwrap() > 0);
    assert!(resource.committed.unwrap() > limit);
    assert_eq!(resource.request_scope, None);
    assert_eq!(resource.constraint_scope, None);
    let mut source: &dyn std::error::Error = failure;
    loop {
        if let Some(original) = source.downcast_ref::<novarocks_functions::KernelCapacityRefusal>()
        {
            assert_eq!(original.failure_fact(), fact);
            break;
        }
        source = source
            .source()
            .expect("the original allocator capacity receipt");
    }
}

pub(super) struct RefusingHost {
    pub(super) actual: Arc<CountingAllocator>,
    pub(super) cause: KernelFailure,
    pub(super) stop: usize,
    pub(super) fail_metadata: bool,
    pub(super) requests: AtomicUsize,
    pub(super) refused: AtomicBool,
}
impl AggregateStateAllocator for RefusingHost {
    fn opaque_allocation_host(&self) -> Option<&dyn OpaqueAllocationHost> {
        Some(self)
    }

    fn allocate(&self, layout: Layout) -> Result<NonNull<u8>, KernelFailure> {
        if self.fail_metadata {
            self.refused.store(true, Ordering::Relaxed);
            return Err(self.cause.clone());
        }
        self.actual.allocate(layout)
    }

    unsafe fn release(&self, block: NonNull<u8>, layout: Layout) {
        // SAFETY: the test returns the exact block and layout granted by the same allocator.
        unsafe { self.actual.release(block, layout) }
    }
}
impl OpaqueAllocationHost for RefusingHost {
    fn reserve_opaque(&self, bytes: usize) -> Result<(), KernelFailure> {
        let at = self.requests.fetch_add(1, Ordering::Relaxed);
        assert!(
            at <= self.stop,
            "opaque request after actual originating refusal"
        );
        if at == self.stop {
            self.refused.store(true, Ordering::Relaxed);
            return Err(self.cause.clone());
        }
        self.actual.reserve_opaque(bytes)
    }

    fn release_opaque(&self, bytes: usize) {
        self.actual.release_opaque(bytes);
    }
}

pub(super) struct NoTail<'a>(pub(super) &'a RefusingHost, pub(super) &'static str);
impl KernelEvaluationControl for NoTail<'_> {
    fn checkpoint(&self, units: u32) -> Result<(), KernelFailure> {
        assert!(units <= 256);
        assert!(
            !self.0.refused.load(Ordering::Relaxed),
            "control callback after actual host first failure"
        );
        Ok(())
    }

    fn wait(&self, _: Duration) -> Result<(), KernelFailure> {
        panic!("{}", self.1)
    }
}

pub(super) const GUARDED_DIAGNOSTICS: [&str; 3] = [
    "guarded-origin-invalid",
    "guarded-origin-internal",
    "guarded-origin-operational",
];
