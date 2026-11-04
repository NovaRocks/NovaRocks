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
use novarocks_type_contract::{
    CompileCheckpoints, ControlOwnedResourceFacts, ControlResourceCounter, SemanticParameterError,
};
use std::{alloc::Layout, sync::Mutex};

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
struct BorrowedControl {
    trace: Mutex<Vec<(CompilePhase, u32)>>,
    refusal: Option<(usize, CompileControlError)>,
}
impl BorrowedControl {
    fn new(refusal: Option<(usize, CompileControlError)>) -> Self {
        Self {
            trace: Mutex::new(Vec::new()),
            refusal,
        }
    }
    fn trace(&self) -> Vec<(CompilePhase, u32)> {
        self.trace.lock().unwrap().clone()
    }
}
impl PureCompileControl for BorrowedControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(phase, CompilePhase::Decode, "private Validate scope");
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        if let Some((at, _)) = self.refusal {
            assert!(trace.len() <= at, "callback after refusal");
        }
        let at = trace.len();
        trace.push((phase, units));
        if let Some((refusal, cause)) = self.refusal.clone()
            && at == refusal
        {
            Err(cause)
        } else {
            Ok(())
        }
    }
}
fn finished<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, FragmentPackageError>,
) -> Result<T, FragmentPackageError> {
    if matches!(&result, Err(FragmentPackageError::Control(_))) {
        return result;
    }
    work.finish().map_err(FragmentPackageError::Control)?;
    result
}
fn construct(
    input: &FragmentPackageInput,
    control: &BorrowedControl,
) -> Result<FragmentPackage, FragmentPackageError> {
    // Fixture copies precede the caller scope; they are not measured production clone claims.
    let input = input.clone();
    let mut work = CompileCheckpoints::try_new(control, CompilePhase::Decode)
        .map_err(FragmentPackageError::Control)?;
    let result =
        FragmentPackage::try_new_in(input, package_admission(), &mut |_| Ok(()), &mut work);
    finished(work, result)
}
fn every_prefix(input: &FragmentPackageInput, expected: Result<(), FragmentPackageError>) {
    let baseline = BorrowedControl::new(None);
    assert_eq!(construct(input, &baseline).map(|_| ()), expected);
    let trace = baseline.trace();
    assert_eq!(trace.first(), Some(&(CompilePhase::Decode, 0)));
    for cause in CAUSES {
        for at in 0..trace.len() {
            let c = BorrowedControl::new(Some((at, cause.clone())));
            assert_eq!(
                construct(input, &c).map(|_| ()),
                Err(FragmentPackageError::Control(cause.clone()))
            );
            assert_eq!(c.trace(), trace[..=at]);
        }
    }
}
fn parameter_input(occurrences: u32) -> FragmentPackageInput {
    let reference = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let mut input = package_input(parameter_occurrences_fragment(reference, occurrences));
    input.parameters = SemanticParameters::try_new([(
        reference.id,
        SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
    )])
    .unwrap();
    input
}
type SnapshotTrace = (
    Vec<(ControlOwnedResourceFacts, usize)>,
    Vec<(CompilePhase, u32)>,
);

fn snapshots(input: &FragmentPackageInput) -> SnapshotTrace {
    let c = BorrowedControl::new(None);
    let mut snapshots = Vec::new();
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let result = FragmentPackage::try_new_in(
        input.clone(),
        package_admission(),
        &mut |facts| {
            snapshots.push((*facts, c.trace().len()));
            Ok(())
        },
        &mut w,
    );
    finished(w, result).unwrap();
    (snapshots, c.trace())
}

#[test]
fn borrowed_complete_package_keeps_source_and_leaves_success_tail_to_decode_caller() {
    let input = parameter_input(1);
    let c = BorrowedControl::new(None);
    let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let package =
        FragmentPackage::try_new_in(input.clone(), package_admission(), &mut |_| Ok(()), &mut w)
            .unwrap();
    assert_no_constant_package_equal(
        &package,
        &FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap(),
    );
    assert_eq!(
        package.fragment().call_requests(),
        input.fragment.call_requests()
    );
    let before = c.trace();
    w.finish().unwrap();
    assert_eq!(&c.trace()[..before.len()], before);
    assert_eq!(
        c.trace().len(),
        before.len() + 1,
        "only explicit caller finish adds the tail"
    );
    every_prefix(&input, Ok(()));
}

#[test]
fn borrowed_complete_package_parameters_keep_missing_wrong_key_unused_and_sparse_intrinsics() {
    let input = parameter_input(1);
    let r = SemanticParameterRef {
        id: SemanticParameterId::new(u32::MAX),
        expected_key: SemanticParameterKey::TimeZone,
    };
    let mut missing = input.clone();
    missing.parameters = SemanticParameters::try_new([]).unwrap();
    every_prefix(
        &missing,
        Err(FragmentPackageError::Parameter(
            SemanticParameterError::MissingId(r.id),
        )),
    );
    let mut wrong = input.clone();
    wrong.parameters =
        SemanticParameters::try_new([(r.id, SemanticParameterValue::AllowThrowException(false))])
            .unwrap();
    every_prefix(
        &wrong,
        Err(FragmentPackageError::Parameter(
            SemanticParameterError::KeyMismatch(r),
        )),
    );
    let mut unused = input.clone();
    unused.parameters = SemanticParameters::try_new([
        (
            r.id,
            SemanticParameterValue::TimeZone("Asia/Shanghai".into()),
        ),
        (
            SemanticParameterId::new(0),
            SemanticParameterValue::TimeZone("UTC".into()),
        ),
    ])
    .unwrap();
    every_prefix(&unused, Err(FragmentPackageError::UnusedParameters));
    let intrinsic = intrinsic_package_fixture();
    every_prefix(&intrinsic, Ok(()));
    let p = construct(&intrinsic, &BorrowedControl::new(None)).unwrap();
    assert_eq!(
        p.parameters().require(intrinsic_reference(0)).unwrap(),
        &SemanticParameterValue::AllowThrowException(false)
    );
    assert_eq!(
        p.parameters()
            .require(intrinsic_reference(u32::MAX))
            .unwrap(),
        &SemanticParameterValue::AllowThrowException(true)
    );
}

#[test]
fn borrowed_package_keeps_original_requests_constants_and_pruning_error_order() {
    let input = parameter_input(1);
    let definition = *input
        .fragment
        .call_requests()
        .entries()
        .keys()
        .next()
        .unwrap();
    let mut parts = input.fragment.clone().into_parts();
    // A deliberate malformed source, never a substitute for mandatory facts.
    parts.call_requests = crate::FragmentCallRequests::unpublished_empty(parts.id);
    let mut missing = package_input(Fragment::from(parts));
    missing.parameters = input.parameters.clone();
    every_prefix(
        &missing,
        Err(FragmentPackageError::Requests(
            crate::CallRequestError::MissingDefinition(definition),
        )),
    );
    let mut pruning = input.clone();
    pruning.pruning =
        FrozenFragmentPruning::try_new(FragmentId::new(82), Vec::new(), &Control).unwrap();
    every_prefix(
        &pruning,
        Err(FragmentPackageError::Pruning(
            crate::FrozenPruningError::WrongFragment,
        )),
    );
    let mut unused = input.clone();
    let original_policy = input
        .fragment
        .call_requests()
        .entries()
        .values()
        .next()
        .unwrap()
        .constant_policy;
    let array = arrow_array::Int64Array::from(vec![7, 9]);
    let value_type = ty(DataType::Int64, false);
    let pool = crate::ConstantPool::try_new(
        Arc::new(value_type.try_to_field("original").unwrap()),
        value_type,
        arrow_array::Array::to_data(&array),
        original_policy,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap();
    unused
        .constants
        .insert(crate::ConstantPoolId::new(u32::MAX), pool)
        .unwrap();
    every_prefix(
        &unused,
        Err(FragmentPackageError::Constant(
            crate::ConstantReferenceError::UnusedPools,
        )),
    );
    // Requests precede Constants even when the original input has both faults.
    missing.constants = unused.constants;
    every_prefix(
        &missing,
        Err(FragmentPackageError::Requests(
            crate::CallRequestError::MissingDefinition(definition),
        )),
    );
}

#[test]
fn borrowed_package_parameter_projection_has_independent_vec_tree_and_unique_box_oracle() {
    for repetitions in [1_u32, 3] {
        let input = parameter_input(repetitions);
        let (facts, _) = snapshots(&input);
        let count = repetitions as usize;
        // Original count walk, collection, then two real projection hooks per reference.
        let tail = &facts[facts.len() - (2 + 2 * count)..];
        let count_snapshot = tail[0].0;
        let collection = tail[1].0;
        let layout = Layout::array::<SemanticParameterRef>(count).unwrap();
        assert_eq!(
            collection.allocation_requests_upper_bound
                - count_snapshot.allocation_requests_upper_bound,
            1
        );
        assert_eq!(
            collection.allocation_request_bytes_upper_bound
                - count_snapshot.allocation_request_bytes_upper_bound,
            layout.size()
        );
        assert_eq!(
            collection.cumulative_work_upper_bound - count_snapshot.cumulative_work_upper_bound,
            2 * count + 2 + layout.size() + 32
        );
        let mut tree = ControlResourceCounter::default();
        tree.tree::<SemanticParameterId, SemanticParameterValue>(1)
            .unwrap();
        let lookup = tail[2].0;
        let unique = tail[3].0;
        assert_eq!(
            unique.allocation_requests_upper_bound - lookup.allocation_requests_upper_bound,
            tree.facts().allocation_requests_upper_bound + 1
        );
        assert_eq!(
            unique.allocation_request_bytes_upper_bound
                - lookup.allocation_request_bytes_upper_bound,
            tree.facts().allocation_request_bytes_upper_bound + "Asia/Shanghai".len()
        );
        let mut previous = unique;
        for pair in tail[4..].as_chunks::<2>().0 {
            assert_eq!(
                pair[0].0.allocation_requests_upper_bound,
                unique.allocation_requests_upper_bound
            );
            assert_eq!(
                pair[1].0.allocation_request_bytes_upper_bound,
                unique.allocation_request_bytes_upper_bound
            );
            assert_eq!(
                pair[1].0, pair[0].0,
                "duplicate captured value adds no clone or insertion"
            );
            assert_eq!(
                pair[0].0.cumulative_work_upper_bound - previous.cumulative_work_upper_bound,
                2 * ControlResourceCounter::lookup_work(1).unwrap()
            );
            previous = pair[1].0;
        }
        every_prefix(&input, Ok(()));
    }
}

#[test]
fn borrowed_package_known_parameter_allocations_refuse_before_actual_late_control_callback() {
    let input = parameter_input(3);
    let (baseline, trace) = snapshots(&input);
    let tail = baseline.len() - 8;
    // Actual collection Vec gate and first unique value clone/tree gate.
    for target in [tail + 1, tail + 3] {
        for axis in 0..2 {
            let facts = baseline[target].0;
            let bound = if axis == 0 {
                facts.allocation_requests_upper_bound - 1
            } else {
                facts.allocation_request_bytes_upper_bound - 1
            };
            for cause in CAUSES {
                let c = BorrowedControl::new(Some((baseline[target].1, cause)));
                let mut w = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
                let mut index = 0;
                let result = FragmentPackage::try_new_in(
                    input.clone(),
                    package_admission(),
                    &mut |f| {
                        let current = index;
                        index += 1;
                        let amount = if axis == 0 {
                            f.allocation_requests_upper_bound
                        } else {
                            f.allocation_request_bytes_upper_bound
                        };
                        if amount > bound {
                            assert_eq!(
                                current, target,
                                "first real numerical refusal belongs to this gate"
                            );
                            return Err(CompileControlError::ResourceExhausted);
                        }
                        Ok(())
                    },
                    &mut w,
                );
                assert_eq!(
                    finished(w, result).unwrap_err(),
                    FragmentPackageError::Control(CompileControlError::ResourceExhausted)
                );
                assert_eq!(c.trace(), trace[..baseline[target].1]);
            }
        }
    }
}

use crate::{PlanLimits, UnpivotConstant};
use arrow_array::ArrayRef;
use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
use arrow_schema::Field;
fn original_pool(array: ArrayRef) -> crate::ConstantPool {
    let value_type = ty(array.data_type().clone(), false);
    let original_policy = parameter_input(1)
        .fragment
        .call_requests()
        .entries()
        .values()
        .next()
        .unwrap()
        .constant_policy;
    crate::ConstantPool::try_new(
        Arc::new(value_type.try_to_field("selected").unwrap()),
        value_type,
        array.to_data(),
        original_policy,
        CompilePhase::Validate,
        &Control,
    )
    .unwrap()
}
fn collection_fragment(
    constants: &[UnpivotConstant],
    ty: &crate::FunctionValueType,
) -> (Fragment, ValueId) {
    let mut builder = crate::FragmentBuilder::new(crate::FragmentId::new(91));
    let empty = crate::NodeId::new(u32::MAX);
    let project = crate::NodeId::new(0);
    let unpivot = crate::NodeId::new(901);
    builder
        .add_values(empty, Box::from([Box::default()]), Box::default())
        .unwrap();
    let integer = crate::FunctionValueType::new(DataType::Int64, false);
    let expression = builder
        .add_expression(
            project,
            integer.clone(),
            crate::ExprKind::Literal(crate::LiteralValue::Int64(7)),
        )
        .unwrap();
    let input = builder
        .add_value(
            integer.clone(),
            crate::ValueOrigin::Expr {
                node: project,
                expr: expression,
            },
        )
        .unwrap();
    builder
        .add_project(
            project,
            empty,
            Box::from([(expression, input)]),
            Box::from([input]),
        )
        .unwrap();
    let value = builder
        .add_value(
            integer,
            crate::ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: 0,
            },
        )
        .unwrap();
    let literal = builder
        .add_value(
            ty.clone(),
            crate::ValueOrigin::NodeOutput {
                node: unpivot,
                output_ordinal: 1,
            },
        )
        .unwrap();
    builder
        .add_row_rewriting(
            unpivot,
            project,
            Some(&BTreeMap::new()),
            Box::from([value, literal]),
            crate::NodeKind::Unpivot {
                spec: crate::UnpivotSpec {
                    passthrough: Box::default(),
                    value_output: value,
                    literal_outputs: Box::from([literal]),
                    mappings: constants
                        .iter()
                        .cloned()
                        .map(|constant| crate::UnpivotValueMapping {
                            input,
                            constants: Box::from([constant]),
                        })
                        .collect(),
                    max_output_rows: 1024,
                    max_output_bytes: 1 << 20,
                },
            },
        )
        .unwrap();
    let fragment = builder
        .finish_structure(
            unpivot,
            crate::FragmentSink::Noop,
            crate::PipelineDopDomain {
                min: 1,
                max: 1,
                requires_power_of_two: false,
            },
            PlanLimits::FROZEN,
            &Control,
            crate::StagePlacementPolicy::Singleton,
        )
        .unwrap();
    (fragment, literal)
}

#[test]
fn borrowed_complete_list_and_map_packages_keep_resource_stage_on_original_decode_scope() {
    let mut list = ListBuilder::new(Int32Builder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Int32,
        false,
    )));
    for row in [[8, 9], [2, 3]] {
        for item in row {
            list.values().append_value(item);
        }
        list.append(true);
    }
    let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new())
        .with_keys_field(Arc::new(Field::new("key", DataType::Utf8, false)))
        .with_values_field(Arc::new(Field::new("value", DataType::Utf8, false)));
    for row in [
        [("a", "unused"), ("b", "unused")],
        [("a", "λ\0"), ("z", "tail")],
    ] {
        for (key, value) in row {
            map.keys().append_value(key);
            map.values().append_value(value);
        }
        map.append(true).unwrap();
    }
    for (pool_id, backing, is_map) in [
        (0, original_pool(Arc::new(list.finish())), false),
        (u32::MAX, original_pool(Arc::new(map.finish())), true),
    ] {
        let address = crate::ConstantReference {
            pool: crate::ConstantPoolId::new(pool_id),
            ordinal: 1,
        };
        let constant = if is_map {
            UnpivotConstant::Utf8Map(address)
        } else {
            UnpivotConstant::Int32List(address)
        };
        let (fragment, _) =
            collection_fragment(std::slice::from_ref(&constant), backing.value_type());
        let mut input = package_input(fragment);
        input
            .constants
            .insert(address.pool, backing.clone())
            .unwrap();
        let plain = FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap();
        let actual = construct(&input, &BorrowedControl::new(None)).unwrap();
        assert_eq!(actual.fragment().nodes(), plain.fragment().nodes());
        assert_eq!(
            actual.constants().entries()[&address.pool].backing_identity(),
            backing.backing_identity()
        );
        every_prefix(&input, Ok(()));
    }
}

#[test]
fn borrowed_result_package_counts_both_original_type_walks_in_existing_decode_scope() {
    let id = FragmentId::new(981);
    let (fragment, value) = literal_fragment(id, FragmentSink::Result, false);
    let mut input = package_input(fragment.clone());
    input.result = Some(ResultPort {
        scalar_schema: None,
        fragment: id,
        output: fragment.nodes()[&fragment.root()].output.clone(),
        fields: Box::from([ResultField {
            domain: crate::ResultValueDomain::Plain,
            name: "actual_result".into(),
            alias: None,
            value,
            ty: fragment.values()[&value].ty.clone(),
        }]),
    });
    assert_no_constant_package_equal(
        &construct(&input, &BorrowedControl::new(None)).unwrap(),
        &FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap(),
    );
    let measure = |input: &FragmentPackageInput| {
        let control = BorrowedControl::new(None);
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut last = ControlOwnedResourceFacts::default();
        let result = FragmentPackage::try_new_in(
            input.clone(),
            package_admission(),
            &mut |facts| {
                last = *facts;
                Ok(())
            },
            &mut work,
        );
        (last, finished(work, result))
    };
    let (with_type, accepted) = measure(&input);
    assert!(accepted.is_ok());
    let mut no_port = input.clone();
    no_port.result = None;
    let (without_type, missing) = measure(&no_port);
    assert!(matches!(missing, Err(FragmentPackageError::Structure(_))));
    // Same actual fragment/call/graph walks; only the admitted primitive result
    // type adds one real carrier Vec request. The second logical law uses its
    // genuine fixed scratch and must also fund that complete initialization.
    assert_eq!(
        with_type.allocation_requests_upper_bound - without_type.allocation_requests_upper_bound,
        1
    );
    assert_eq!(
        with_type.allocation_request_bytes_upper_bound
            - without_type.allocation_request_bytes_upper_bound,
        Layout::new::<(
            &arrow_schema::DataType,
            novarocks_type_contract::ArrowTypeDepth
        )>()
        .size()
    );
    assert!(with_type.cumulative_work_upper_bound - without_type.cumulative_work_upper_bound >= 1);
    every_prefix(&input, Ok(()));
}

#[test]
fn borrowed_package_resource_stage_visits_original_fragment_types_on_decode_scope() {
    let (fragment, _) = literal_fragment(FragmentId::new(982), FragmentSink::Noop, false);
    let input = package_input(fragment);
    let control = BorrowedControl::new(None);
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut resources = ControlResourceCounter::default();
    let mut snapshots = Vec::new();
    let result = crate::validation::validate_package_in(
        &input,
        PlanLimits::FROZEN,
        0,
        package_admission().source_retained_bytes,
        &mut resources,
        &mut |facts| {
            snapshots.push(*facts);
            Ok(())
        },
        &mut work,
    );
    finished(work, result).unwrap();
    // This actual Values fragment owns one ValueDef and one expression with
    // the same primitive type. Both original occurrences must be visited;
    // sharing that type cannot eliminate either carrier or logical law.
    let tuple_bytes = Layout::new::<(
        &arrow_schema::DataType,
        novarocks_type_contract::ArrowTypeDepth,
    )>()
    .size();
    assert_eq!(snapshots[0].allocation_requests_upper_bound, 1);
    assert_eq!(
        snapshots[0].allocation_request_bytes_upper_bound,
        tuple_bytes
    );
    let second = snapshots
        .iter()
        .find(|facts| facts.allocation_requests_upper_bound == 2)
        .unwrap();
    assert_eq!(second.allocation_request_bytes_upper_bound, 2 * tuple_bytes);
    assert!(second.cumulative_work_upper_bound >= 1);
    assert_no_constant_package_equal(
        &construct(&input, &BorrowedControl::new(None)).unwrap(),
        &FragmentPackage::try_new(input.clone(), package_admission(), &Control).unwrap(),
    );
    every_prefix(&input, Ok(()));
}

#[test]
fn borrowed_package_fragment_type_known_request_precedes_pending_control_refusal() {
    let (fragment, _) = literal_fragment(FragmentId::new(983), FragmentSink::Noop, false);
    let input = package_input(fragment);
    let spelling = "q".repeat(255);
    let baseline = BorrowedControl::new(None);
    let mut before = CompileCheckpoints::try_new(&baseline, CompilePhase::Decode).unwrap();
    novarocks_type_contract::owned_resources::copy::copy_string::<CompileControlError>(
        &spelling,
        &mut before,
    )
    .unwrap();
    let original_prefix = baseline.trace();
    // Real character copies left exactly 255 completed units pending. No
    // synthetic step loop or scope reset manufactures this control window.
    for cause in CAUSES {
        let control = BorrowedControl::new(Some((original_prefix.len(), cause)));
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
        let mut resources = ControlResourceCounter::default();
        resources
            .layout(Layout::array::<u8>(spelling.len()).unwrap(), 1)
            .unwrap();
        resources.work(spelling.len()).unwrap();
        let copied = novarocks_type_contract::owned_resources::copy::copy_string::<
            CompileControlError,
        >(&spelling, &mut work)
        .unwrap();
        assert_eq!(copied, spelling);
        assert_eq!(control.trace(), original_prefix);
        let result = crate::validation::validate_package_in(
            &input,
            PlanLimits::FROZEN,
            0,
            package_admission().source_retained_bytes,
            &mut resources,
            &mut |facts| {
                if facts.allocation_requests_upper_bound > 1 {
                    Err(CompileControlError::ResourceExhausted)
                } else {
                    Ok(())
                }
            },
            &mut work,
        );
        assert_eq!(
            result,
            Err(FragmentPackageError::Control(
                CompileControlError::ResourceExhausted
            ))
        );
        assert_eq!(control.trace(), original_prefix);
    }
}
