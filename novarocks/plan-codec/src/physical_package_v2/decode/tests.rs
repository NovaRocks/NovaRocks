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
use crate::physical_package_v2::definition_sources::tests::{
    cv_package, rich_package, values_package, writer_constant_package, writer_finish_package,
};
use crate::physical_package_v2::encode::{PackageEncodeError, encode_fragment_package};
use crate::physical_package_v2::provider_sources::tests::checked_read;
use crate::physical_package_v2::test_support::{decode_limits, encode_limits};
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, CompilePhase, PureCompileControl,
};
use prost::Message;
use std::sync::Mutex;

#[derive(Default)]
struct Control {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for Control {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        trace.push((phase, units));
        match self.stop {
            Some((stop, cause)) if stop == at => Err(cause),
            _ => Ok(()),
        }
    }
}

fn model() -> FragmentDecodeResourceModel {
    FragmentDecodeResourceModel::try_new(&Control::default()).unwrap()
}
fn encode(package: &p::FragmentPackage) -> Result<Vec<u8>, PackageEncodeError> {
    Ok(encode_fragment_package(package, &encode_limits(), &Control::default())?.encode_to_vec())
}
fn fixtures() -> Vec<(&'static str, p::FragmentPackage)> {
    vec![
        ("rich", rich_package()),
        ("cv", cv_package()),
        ("writer", writer_constant_package()),
        ("finish", writer_finish_package()),
        ("data-read", checked_read(false)),
        ("metadata-read", checked_read(true)),
    ]
}

// Lossless and canonical: sender bytes decode through the generated preflight
// and the original constructors into a checked package whose own encoding is
// byte-identical. No component is defaulted, re-derived or dropped.
#[test]
fn whole_package_roundtrip_is_byte_identical_through_original_constructors() {
    let model = model();
    for (name, package) in fixtures() {
        let bytes = encode(&package).unwrap_or_else(|e| panic!("{name}: {e}"));
        let decoded =
            decode_fragment_package(&bytes, &model, &decode_limits(), &Control::default())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(decoded.fragment().id(), package.fragment().id(), "{name}");
        assert_eq!(
            decoded.fragment().nodes().len(),
            package.fragment().nodes().len(),
            "{name}"
        );
        assert_eq!(decoded.scans().len(), package.scans().len(), "{name}");
        assert_eq!(decoded.writes().len(), package.writes().len(), "{name}");
        assert_eq!(
            decoded.result().is_some(),
            package.result().is_some(),
            "{name}"
        );
        let again = encode(&decoded).unwrap_or_else(|e| panic!("{name} re-encode: {e}"));
        assert_eq!(
            again, bytes,
            "{name}: decode∘encode is the identity on sender bytes"
        );
    }
}

// Many-row VALUES lists and wide projections carry hundreds of strict scalar
// type roots. The sender charges each root's validator by its actual type
// size, so such a package fits the sender's allocation-request ceiling and
// its bytes survive the receiver and a re-encode unchanged.
#[test]
fn many_scalar_type_roots_roundtrip_byte_identically() {
    let model = model();
    for (rows, columns) in [(60, 1), (400, 1), (1000, 1), (1, 400)] {
        let name = format!("{rows}x{columns}");
        let package = values_package(rows, columns);
        let dto = encode_fragment_package(&package, &encode_limits(), &Control::default())
            .unwrap_or_else(|e| panic!("{name}: {e}"));
        let roots = dto.types.as_ref().unwrap().value_types.len();
        assert_eq!(roots, rows * columns + 2 * columns + 1, "{name}");
        let bytes = dto.encode_to_vec();
        let decoded =
            decode_fragment_package(&bytes, &model, &decode_limits(), &Control::default())
                .unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(decoded.fragment(), package.fragment(), "{name}");
        assert_eq!(encode(&decoded).unwrap(), bytes, "{name}");
    }
}

/// Nested Arrow fields (List item, Map entries and their Struct children)
/// carried by one data type, counted on the original value type.
fn nested_fields(ty: &arrow::datatypes::DataType) -> usize {
    use arrow::datatypes::DataType;
    match ty {
        DataType::List(field) | DataType::LargeList(field) | DataType::Map(field, _) => {
            1 + nested_fields(field.data_type())
        }
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| 1 + nested_fields(field.data_type()))
            .sum(),
        _ => 0,
    }
}

// The finisher fragment of a checked writer plan: the writer's multiplexed
// result relation arrives over an ExchangeSource and feeds a TableFinish
// whose root write-result schema carries List and Map fields. The receiver
// reproduces its TableFinish exactly and the sender its bytes.
#[test]
fn finish_package_roundtrips_byte_identically_with_its_exact_table_finish() {
    let package = writer_finish_package();
    let finish = package
        .fragment()
        .nodes()
        .values()
        .find(|node| matches!(node.kind, p::NodeKind::TableFinish(_)))
        .expect("finish fixture has a TableFinish")
        .clone();
    assert!(package.writes().is_empty() && package.scans().is_empty());
    let bytes = encode(&package).unwrap();
    let decoded = decode_fragment_package(&bytes, &model(), &decode_limits(), &Control::default())
        .unwrap_or_else(|error| panic!("finish package receives: {error:?}"));
    assert_eq!(decoded.fragment().nodes()[&finish.id], finish);
    assert_eq!(
        decoded.fragment().values(),
        package.fragment().values(),
        "imported and derived writer values are reproduced"
    );
    assert_eq!(encode(&decoded).unwrap(), bytes);
}

// The receiver publishes through the caller-scope Package constructor with
// its host admission. Each nested field's logical metadata lookup is charged
// against that retained-source invoice; the charge must stay linear in it.
// With a source-sized key length, two nested fields under the 2 GiB receiver
// invoice overflowed the work meter and the finish package was refused.
#[test]
fn caller_scope_package_admission_charges_nested_field_lookups_linearly() {
    let package = writer_finish_package();
    let nested: usize = package
        .fragment()
        .values()
        .values()
        .map(|value| nested_fields(&value.ty.data_type))
        .sum();
    assert!(nested >= 2, "the fixture carries {nested} nested fields");
    let admission = decode_limits().admission;
    assert!(admission.source_retained_bytes >= 2 * 1024 * 1024 * 1024);
    let control = Control::default();
    let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Decode).unwrap();
    let mut peak = 0usize;
    let readmitted = p::FragmentPackage::try_new_in(
        package.clone().into_input(),
        admission,
        &mut |facts| {
            peak = peak.max(facts.cumulative_work_upper_bound);
            Ok(())
        },
        &mut work,
    )
    .unwrap_or_else(|error| panic!("caller-scope admission: {error:?}"));
    work.finish().unwrap();
    assert_eq!(
        readmitted.fragment().nodes(),
        package.fragment().nodes(),
        "the original constructor publishes the same fragment"
    );
    // Linear: far below the about 2 * source^2 a source-sized key charged
    // for each nested field.
    let source = admission.source_retained_bytes;
    let per_field_quadratic = source.saturating_mul(2).saturating_mul(source);
    assert!(peak < per_field_quadratic / 1024, "peak work {peak}");
}

fn mutated(
    package: &p::FragmentPackage,
    change: impl FnOnce(&mut wire::FragmentPackage),
) -> Vec<u8> {
    let mut dto = encode_fragment_package(package, &encode_limits(), &Control::default()).unwrap();
    change(&mut dto);
    dto.encode_to_vec()
}
fn refused(bytes: &[u8]) -> PackageDecodeError {
    decode_fragment_package(bytes, &model(), &decode_limits(), &Control::default())
        .expect_err("receiver must refuse")
}

#[test]
fn absent_required_components_are_refused_without_defaults() {
    let package = rich_package();
    let cases: [(&str, fn(&mut wire::FragmentPackage)); 7] = [
        ("fragment", |d| d.fragment = None),
        ("expression_control", |d| d.expression_control = None),
        ("calls", |d| d.calls = None),
        ("pruning", |d| d.pruning = None),
        ("parameters", |d| d.parameters = None),
        ("cuts", |d| d.cuts = None),
        ("call_requests", |d| {
            d.fragment.as_mut().unwrap().call_requests = None;
        }),
    ];
    for (name, change) in cases {
        let error = refused(&mutated(&package, change));
        assert!(
            !matches!(error, PackageDecodeError::Control(_)),
            "{name}: absence is an ordinary refusal, got {error}"
        );
    }
}

#[test]
fn duplicate_node_ids_and_malformed_bytes_are_refused() {
    let package = rich_package();
    let duplicate = mutated(&package, |d| {
        let fragment = d.fragment.as_mut().unwrap();
        let first = fragment.nodes[0].clone();
        fragment.nodes.push(first);
    });
    assert!(matches!(
        refused(&duplicate),
        PackageDecodeError::Invalid(_)
    ));
    let mut truncated = encode(&package).unwrap();
    truncated.truncate(truncated.len() / 2);
    assert!(!matches!(
        refused(&truncated),
        PackageDecodeError::Control(_)
    ));
    // An unused constant pool is not silently accepted by the original
    // closed-pool publication law.
    let cv = cv_package();
    let extra = mutated(&cv, |d| {
        let mut pool = d.constants[0].clone();
        pool.id = pool.id.wrapping_sub(1);
        d.constants.push(pool);
    });
    assert!(!matches!(refused(&extra), PackageDecodeError::Control(_)));
}

#[test]
fn oversized_input_is_refused_by_byte_admission_before_any_dto() {
    let bytes = encode(&rich_package()).unwrap();
    let mut limits = decode_limits();
    limits.wire.max_input_bytes = bytes.len() - 1;
    let error = decode_fragment_package(&bytes, &model(), &limits, &Control::default())
        .expect_err("over byte limit");
    assert!(matches!(
        error,
        PackageDecodeError::Control(CompileControlError::ResourceExhausted)
            | PackageDecodeError::Wire(_)
    ));
}

// Representative positions on the one Decode scope: the caller's cause is
// primary at entry, in the middle and at the footer.
#[test]
fn caller_control_cause_stays_primary_across_the_receiver() {
    let package = checked_read(true);
    let bytes = encode(&package).unwrap();
    let control = Control::default();
    decode_fragment_package(&bytes, &model(), &decode_limits(), &control).unwrap();
    let callbacks = control.trace.lock().unwrap().len();
    assert!(callbacks > 2);
    assert!(
        control
            .trace
            .lock()
            .unwrap()
            .iter()
            .all(|(phase, _)| *phase == CompilePhase::Decode)
    );
    for at in [0, callbacks / 3, callbacks / 2, callbacks - 1] {
        for cause in [
            CompileControlError::Cancelled,
            CompileControlError::DeadlineExceeded,
            CompileControlError::ResourceExhausted,
        ] {
            let stop = Control {
                stop: Some((at, cause)),
                ..Default::default()
            };
            assert!(
                matches!(
                    decode_fragment_package(&bytes, &model(), &decode_limits(), &stop),
                    Err(PackageDecodeError::Control(actual)) if actual == cause
                ),
                "position {at}"
            );
            assert_eq!(stop.trace.lock().unwrap().len(), at + 1);
        }
    }
}

/// Every checked package of one plan that reads and writes nothing: eager
/// root uses in one domain, no calls and an explicit empty pruning table.
fn plan_packages(plan: &p::PhysicalPlan) -> BTreeMap<p::FragmentId, p::FragmentPackage> {
    use novarocks_type_contract::{
        ControlShape, EvaluationDomainId, ExpressionControlFlow, ExpressionEffectContext,
        ExpressionEvaluationDomain, ExpressionInvocation, ExpressionUseId,
    };
    let setup = Control::default();
    let mut uses = BTreeMap::new();
    let mut calls = BTreeMap::new();
    let mut pruning = BTreeMap::new();
    let mut admissions = BTreeMap::new();
    for (id, fragment) in plan.fragments() {
        let roots = p::PhysicalExpressionRoots::try_new(fragment, &setup).unwrap();
        let bindings = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(ordinal, (site, _))| (*site, ExpressionUseId::new(ordinal as u32)))
            .collect::<Vec<_>>();
        let invocations = roots
            .sites()
            .iter()
            .enumerate()
            .map(|(ordinal, (_, root))| ExpressionInvocation {
                context: ExpressionEffectContext {
                    use_id: ExpressionUseId::new(ordinal as u32),
                    domain: EvaluationDomainId::new(0),
                    demand: root.demand,
                },
                definition: root.expr,
                control: ControlShape::Eager,
                arguments: Box::default(),
            })
            .collect::<Vec<_>>();
        let domains = if invocations.is_empty() {
            vec![]
        } else {
            vec![ExpressionEvaluationDomain {
                id: EvaluationDomainId::new(0),
                parent: None,
                guard: None,
            }]
        };
        let flow = ExpressionControlFlow::try_new(
            domains,
            invocations,
            fragment.expressions(),
            CompilePhase::Validate,
            &setup,
        )
        .unwrap();
        let actual = p::PhysicalRootUses::try_new(fragment, flow, bindings, &setup).unwrap();
        calls.insert(
            *id,
            p::FrozenFragmentCalls::try_new(fragment, &actual, vec![], &setup).unwrap(),
        );
        uses.insert(*id, actual);
        pruning.insert(
            *id,
            p::FrozenFragmentPruning::try_new(*id, vec![], &setup).unwrap(),
        );
        admissions.insert(
            *id,
            p::FragmentPackageAdmission {
                plan_limits: p::PlanLimits::FROZEN,
                source_retained_bytes: 128 * 1024 * 1024,
                property_projection_limits: p::PropertyProofProjectionLimits {
                    max_request_bytes: 64 * 1024 * 1024,
                    max_coexisting_bytes: 512 * 1024 * 1024,
                    max_projection_work: 128 * 1024 * 1024,
                },
            },
        );
    }
    p::extract_fragment_packages(
        plan,
        &BTreeMap::new(),
        &BTreeMap::new(),
        &uses,
        &calls,
        &pruning,
        &admissions,
        &setup,
    )
    .unwrap()
}

// Each package carries its own slice of the plan-global runtime-filter
// binding numbering. The whole-package wire preserves it exactly, and the
// receiver's package law refuses identities the plan never gave the slice.
#[test]
fn runtime_filter_package_roundtrips_its_plan_binding_slice_byte_identically() {
    use p::RuntimeFilterBindingRole::{Consumer, Producer};
    let plan = crate::physical_encode::tests::finish_ordered_join_build_filter_plan();
    let numbered = p::runtime_filter_bindings(&plan).unwrap();
    let join_fragment = p::FragmentId::new(42);
    let filter = p::RuntimeFilterId::new(40);
    assert_eq!(
        numbered
            .iter()
            .map(|binding| (binding.binding_id, binding.fragment, binding.role))
            .collect::<Vec<_>>(),
        [
            (1, join_fragment, Producer(0)),
            (2, join_fragment, Consumer(0))
        ]
    );
    let packages = plan_packages(&plan);
    assert_eq!(packages.len(), 3);
    for (id, package) in &packages {
        let slice = numbered
            .iter()
            .filter(|binding| binding.fragment == *id)
            .map(p::RuntimeFilterBinding::cut)
            .collect::<Vec<_>>();
        assert_eq!(
            &*package.cuts().runtime_filter_bindings,
            slice,
            "{}",
            id.get()
        );
        let bytes = encode(package).unwrap_or_else(|e| panic!("{}: {e}", id.get()));
        let decoded =
            decode_fragment_package(&bytes, &model(), &decode_limits(), &Control::default())
                .unwrap_or_else(|e| panic!("{}: {e}", id.get()));
        assert_eq!(decoded.cuts(), package.cuts(), "{}", id.get());
        assert_eq!(encode(&decoded).unwrap(), bytes, "{}", id.get());
    }

    let package = &packages[&join_fragment];
    let dto = encode_fragment_package(package, &encode_limits(), &Control::default()).unwrap();
    let wire_bindings = &dto.cuts.as_ref().unwrap().runtime_filter_bindings;
    use wire::runtime_filter_binding_cut::Role;
    assert_eq!(
        wire_bindings
            .iter()
            .map(|binding| (binding.binding_id, binding.runtime_filter_id, binding.role))
            .collect::<Vec<_>>(),
        [
            (Some(1), Some(filter.get()), Some(Role::ProducerIndex(0))),
            (Some(2), Some(filter.get()), Some(Role::ConsumerIndex(0))),
        ]
    );
    let structure = |error: PackageDecodeError| match error {
        PackageDecodeError::Package(p::FragmentPackageError::Structure(errors)) => errors,
        other => panic!("binding table was not refused by the package law: {other}"),
    };
    let swapped = refused(&mutated(package, |d| {
        let bindings = &mut d.cuts.as_mut().unwrap().runtime_filter_bindings;
        bindings[0].binding_id = Some(2);
        bindings[1].binding_id = Some(1);
    }));
    let errors = structure(swapped);
    assert!(
        errors.errors().iter().any(|error| error.path()
            == "fragments[42].cuts.runtime_filter_bindings[1]"
            && error.message()
                == "runtime-filter binding identities of one fragment are not consecutive"),
        "{errors}"
    );
    let dropped = refused(&mutated(package, |d| {
        d.cuts.as_mut().unwrap().runtime_filter_bindings.pop();
    }));
    let errors = structure(dropped);
    assert!(
        errors.errors().iter().any(|error| error.path()
            == "fragments[42].cuts.runtime_filter_bindings"
            && error.message() == "local consumer 0 of runtime filter 40 has no binding"),
        "{errors}"
    );
    // A shapeless entry is refused by the cut codec before any package law.
    let shapeless = refused(&mutated(package, |d| {
        d.cuts.as_mut().unwrap().runtime_filter_bindings[0].role = None;
    }));
    assert!(
        matches!(shapeless, PackageDecodeError::Node(_)),
        "{shapeless}"
    );
}

struct TypeScope {
    calls: usize,
    ran: usize,
    refuse: bool,
}
impl PackageTypeMaterializationScope for TypeScope {
    type HostError = u32;
    fn materialize<B>(
        &mut self,
        _: &PackageTypeProjectionFacts,
        body: B,
    ) -> Result<crate::physical_type_v2::DecodedTypeTable, ProjectionFailure<TypeCodecError, u32>>
    where
        B: FnOnce() -> Result<crate::physical_type_v2::DecodedTypeTable, TypeCodecError>,
    {
        self.calls += 1;
        if self.refuse {
            return Err(ProjectionFailure::Host(79));
        }
        self.ran += 1;
        body().map_err(ProjectionFailure::Codec)
    }
}
#[test]
fn whole_package_type_host_keeps_original_bytes_and_every_checkpoint() {
    let model = model();
    for (name, package) in fixtures() {
        let bytes = encode(&package).unwrap();
        let direct = Control::default();
        let expected = decode_fragment_package(&bytes, &model, &decode_limits(), &direct).unwrap();
        let hosted = Control::default();
        let mut host = TypeScope {
            calls: 0,
            ran: 0,
            refuse: false,
        };
        let actual = decode_fragment_package_with_type_host(
            &bytes,
            &model,
            &decode_limits(),
            &hosted,
            &mut host,
        )
        .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        assert_eq!(
            encode(&actual).unwrap(),
            encode(&expected).unwrap(),
            "{name}"
        );
        assert_eq!(
            *hosted.trace.lock().unwrap(),
            *direct.trace.lock().unwrap(),
            "{name}"
        );
        assert_eq!((host.calls, host.ran), (1, 1), "{name}");
    }
}
#[test]
fn whole_package_type_host_refusal_is_nominal_without_completion_callback() {
    let bytes = encode(&rich_package()).unwrap();
    let model = model();
    let mut host = TypeScope {
        calls: 0,
        ran: 0,
        refuse: true,
    };
    let original = Control::default();
    assert!(matches!(
        decode_fragment_package_with_type_host(
            &bytes,
            &model,
            &decode_limits(),
            &original,
            &mut host
        ),
        Err(ProjectionFailure::Host(79))
    ));
    let length = original.trace.lock().unwrap().len();
    // This callback would refuse the ordinary footer. It must never be called
    // after the same host refusal, or replace that original nominal cause.
    let stop = Control {
        stop: Some((length, CompileControlError::Cancelled)),
        trace: Mutex::new(Vec::new()),
    };
    let mut host = TypeScope {
        calls: 0,
        ran: 0,
        refuse: true,
    };
    assert!(matches!(
        decode_fragment_package_with_type_host(&bytes, &model, &decode_limits(), &stop, &mut host),
        Err(ProjectionFailure::Host(79))
    ));
    assert_eq!(stop.trace.lock().unwrap().len(), length);
    assert_eq!((host.calls, host.ran), (1, 0));
    let mut host = TypeScope {
        calls: 0,
        ran: 0,
        refuse: false,
    };
    assert!(matches!(
        decode_fragment_package_with_type_host(
            &[0x80],
            &model,
            &decode_limits(),
            &Control::default(),
            &mut host
        ),
        Err(ProjectionFailure::Codec(_))
    ));
    assert_eq!((host.calls, host.ran), (0, 0));
}
