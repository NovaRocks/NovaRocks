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

//! Admit borrowed exact binding comparisons before entering their original
//! owner. The trusted invoice includes BOTH pool and type-table sources,
//! strings and deleted HashMap buckets. This is not an allocation grant.

use super::PhysicalConstantCodecError;
use crate::physical_type_v2::TypeCodecError;
#[cfg(test)]
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use novarocks_constant_contract::ConstantPool;
use novarocks_type_contract::{
    CompileCheckpoints, CompileControlError, MAX_VALUE_TYPE_NODES, NR_LOGICAL_TYPE_KEY,
    ValueTypeVisit, validate_value_type_structure_with_scratch_observed,
};
use std::{mem, sync::Arc};

#[derive(Clone, Copy, Debug)]
pub(super) struct ConstantBindingResourceFacts {
    work_upper_bound: usize,
    /// The caller must follow this SAME immutable-Arc branch. Pointer equality
    /// proves the whole Field identical, but never skips the FVT comparison.
    pub(super) compare_full_field: bool,
}
impl ConstantBindingResourceFacts {
    pub(super) const fn work_upper_bound(&self) -> usize {
        self.work_upper_bound
    }
}

fn shape(message: &'static str) -> TypeCodecError {
    TypeCodecError::InvalidShape(message)
}
fn add(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_add(b)
        .ok_or_else(|| shape("constant binding resource sum overflow"))
}
fn mul(a: usize, b: usize) -> Result<usize, TypeCodecError> {
    a.checked_mul(b)
        .ok_or_else(|| shape("constant binding resource product overflow"))
}
fn cap(bound: usize, limit: usize) -> Result<(), TypeCodecError> {
    if bound > limit {
        return Err(shape("constant binding work envelope exceeded"));
    }
    Ok(())
}

// One numerical contribution on the original caller meter. Plain retains its
// completed-operation error order; the caller path gates known work first.
type Admit<'a> = dyn FnMut(usize) -> Result<(), CompileControlError> + 'a;
struct Admission<'borrow, 'callback> {
    parent: Option<&'borrow mut Admit<'callback>>,
}
impl Admission<'_, '_> {
    fn numeric<T>(&self, result: Result<T, TypeCodecError>) -> Result<T, TypeCodecError> {
        if self.parent.is_some() {
            result.map_err(|_| CompileControlError::ResourceExhausted.into())
        } else {
            result
        }
    }
    fn known(&mut self, bound: usize, limit: usize) -> Result<(), TypeCodecError> {
        if let Some(parent) = &mut self.parent {
            if bound > limit {
                return Err(CompileControlError::ResourceExhausted.into());
            }
            parent(bound)?;
        }
        Ok(())
    }
}

#[derive(Default)]
struct Metrics {
    types: usize,
    fields: usize,
    edges: usize,
    entries: usize,
    squared_entries: usize,
    /// Field events precede the sole walker's logical-domain lookup. The root
    /// Field is compared directly and therefore has no walker lookup.
    logical_probes: usize,
    model_visits: usize,
}
impl Metrics {
    fn field(&mut self, field: &Field, logical_probe: bool) -> Result<(), TypeCodecError> {
        let entries = field.metadata().len();
        self.fields = add(self.fields, 1)?;
        self.entries = add(self.entries, entries)?;
        self.squared_entries = add(self.squared_entries, mul(entries, entries)?)?;
        self.logical_probes = add(self.logical_probes, usize::from(logical_probe))?;
        Ok(())
    }

    fn bound(&self, source: usize, prefix: usize) -> Result<usize, TypeCodecError> {
        // In each exact metadata comparison equal counts K permit one left
        // scan and <=K right scans; unequal counts terminate before scanning.
        // Their opaque bucket scans and repeated key bytes are covered by
        // 3K source visits. Values and names add <=3 source visits per Field;
        // timestamp zones add <=one per type node. This also covers aliases:
        // the same retained backing may be visited at every left occurrence.
        let source_visits = add(
            add(mul(3, self.entries)?, mul(3, self.fields)?)?,
            self.types,
        )?;
        // Candidate visits and byte-length checks: <=2K²+2K. Separate node,
        // Field and edge headers cover closed tags, nullability, dictionary
        // attributes, child counts and Union IDs without relying on B>0.
        let candidates = add(mul(2, self.squared_entries)?, mul(2, self.entries)?)?;
        let headers = add(
            add(mul(4, self.types)?, mul(8, self.fields)?)?,
            mul(2, self.edges)?,
        )?;
        // A fixed-key logical lookup hashes NR_LOGICAL_TYPE_KEY once and
        // inspects at most the original retained bucket/string backing. Public
        // HashMap capacity is deliberately absent, including after deletion.
        let probes = mul(self.logical_probes, add(source, NR_LOGICAL_TYPE_KEY.len())?)?;
        add(
            add(prefix, self.model_visits)?,
            add(
                add(mul(source, source_visits)?, candidates)?,
                add(headers, probes)?,
            )?,
        )
    }

    fn observe(
        &mut self,
        event: ValueTypeVisit<'_>,
        source: usize,
        prefix: usize,
        limit: usize,
        admission: &mut Admission<'_, '_>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), TypeCodecError> {
        let opaque_boundary = matches!(
            event,
            ValueTypeVisit::Field(_) | ValueTypeVisit::ChildEdge(_)
        );
        match event {
            ValueTypeVisit::TypeNode(_) => self.types = admission.numeric(add(self.types, 1))?,
            ValueTypeVisit::ChildEdge(_) => self.edges = admission.numeric(add(self.edges, 1))?,
            ValueTypeVisit::Field(field) => admission.numeric(self.field(field, true))?,
        }
        self.model_visits = admission.numeric(add(self.model_visits, 1))?;
        let bound = admission.numeric(self.bound(source, prefix))?;
        admission.known(bound, limit)?;
        let admitted = cap(bound, limit);
        // One completed constant-size numerical/event operation. This is not
        // a synthetic loop representing predicted bytes or future library work.
        work.step()?;
        admitted?;
        if opaque_boundary {
            // The shared walk performs field_logical_type immediately after
            // this callback. Admit that opaque probe, then observe its actual
            // exit at the following ChildEdge event or ordinary tail.
            work.flush()?;
        }
        Ok(())
    }
}

/// Both shapes are original checked owners. Only the sole shared scratch walk
/// supplies topology and Field events; no cloned types, shadow grammar, map
/// bucket table or heap-backed pending traversal is introduced. The caller
/// owns ordinary/success finish and the ensuing exact comparison operations.
pub(super) fn preflight_constant_binding_resources(
    pool: &ConstantPool,
    expected_field: &Arc<Field>,
    source_retained_bytes: usize,
    max_work: usize,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantBindingResourceFacts, PhysicalConstantCodecError> {
    preflight_core(
        pool,
        expected_field,
        source_retained_bytes,
        max_work,
        &mut Admission { parent: None },
        work,
    )
}

pub(super) fn preflight_constant_binding_resources_in(
    pool: &ConstantPool,
    expected_field: &Arc<Field>,
    source_retained_bytes: usize,
    max_work: usize,
    admit: &mut Admit<'_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantBindingResourceFacts, PhysicalConstantCodecError> {
    preflight_core(
        pool,
        expected_field,
        source_retained_bytes,
        max_work,
        &mut Admission {
            parent: Some(admit),
        },
        work,
    )
}

fn preflight_core(
    pool: &ConstantPool,
    expected_field: &Arc<Field>,
    source_retained_bytes: usize,
    max_work: usize,
    admission: &mut Admission<'_, '_>,
    work: &mut CompileCheckpoints<'_>,
) -> Result<ConstantBindingResourceFacts, PhysicalConstantCodecError> {
    // The fixed scratch request is known without inspecting source metadata.
    // Its numerical refusal precedes a pending original observation.
    if admission.parent.is_some() {
        let scratch_bytes = mem::size_of::<
            novarocks_type_contract::owned_resources::type_validation::TypeValidationScratch<'_>,
        >();
        let prefix = admission.numeric(add(source_retained_bytes, scratch_bytes))?;
        admission.known(prefix, max_work)?;
    }
    work.flush()?;
    let retained = usize::try_from(pool.resource_facts().retained_buffer_capacity_bytes)
        .map_err(|_| shape("constant binding source retention is not representable"))?;
    let covers_pool = source_retained_bytes >= retained;
    work.step()?;
    if !covers_pool {
        return Err(shape("constant binding source invoice omits checked pool backing").into());
    }
    let scratch_bytes = mem::size_of::<
        novarocks_type_contract::owned_resources::type_validation::TypeValidationScratch<'_>,
    >();
    let prefix = admission.numeric(add(source_retained_bytes, scratch_bytes))?;
    admission.known(prefix, max_work)?;
    let admitted = cap(prefix, max_work);
    work.step()?;
    admitted?;
    work.flush()?;
    let mut scratch = [None; MAX_VALUE_TYPE_NODES];
    work.flush()?;
    let mut metrics = Metrics::default();
    validate_value_type_structure_with_scratch_observed::<TypeCodecError>(
        pool.value_type().data_type(),
        &mut scratch,
        |event| {
            metrics.observe(
                event,
                source_retained_bytes,
                prefix,
                max_work,
                admission,
                work,
            )
        },
    )?;
    let compare_full_field = !Arc::ptr_eq(pool.field_ref(), expected_field);
    if admission.parent.is_none() {
        work.step()?;
    }
    if compare_full_field {
        admission.numeric(metrics.field(pool.field(), false))?;
        metrics.model_visits = admission.numeric(add(metrics.model_visits, 1))?;
        let bound = admission.numeric(metrics.bound(source_retained_bytes, prefix))?;
        admission.known(bound, max_work)?;
        let admitted = cap(bound, max_work);
        if admission.parent.is_some() {
            work.step()?;
        }
        work.step()?;
        admitted?;
        validate_value_type_structure_with_scratch_observed::<TypeCodecError>(
            pool.field().data_type(),
            &mut scratch,
            |event| {
                metrics.observe(
                    event,
                    source_retained_bytes,
                    prefix,
                    max_work,
                    admission,
                    work,
                )
            },
        )?;
    }
    if admission.parent.is_some() && !compare_full_field {
        work.step()?;
    }
    let work_upper_bound = admission.numeric(metrics.bound(source_retained_bytes, prefix))?;
    admission.known(work_upper_bound, max_work)?;
    work.flush()?;
    Ok(ConstantBindingResourceFacts {
        work_upper_bound,
        compare_full_field,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, Int64Array};
    use novarocks_constant_contract::ConstantPolicy;
    use novarocks_type_contract::{
        CompileControlError, CompilePhase, FunctionValueType, PureCompileControl,
        arrow_fields_exact_borrowed_observed,
    };
    use std::{collections::HashMap, sync::Mutex};

    const SOURCE: usize = 1024 * 1024;
    const WORK: usize = 128 * 1024 * 1024;
    const CAUSES: [CompileControlError; 3] = [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ];

    #[derive(Default)]
    struct Control {
        trace: Mutex<Vec<u32>>,
        refusal: Option<(usize, CompileControlError)>,
    }
    impl PureCompileControl for Control {
        fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
            assert_eq!(phase, CompilePhase::Encode);
            let mut trace = self.trace.lock().unwrap();
            let at = trace.len();
            if let Some((stop, _)) = self.refusal {
                assert!(at <= stop, "callback after primary refusal");
            }
            trace.push(units);
            match self.refusal.clone() {
                Some((stop, cause)) if stop == at => Err(cause),
                _ => Ok(()),
            }
        }
    }

    fn pool() -> ConstantPool {
        // Preserve a map that really had many entries before deletion. Neither
        // its current len nor public capacity is an invoice for bucket backing.
        let mut metadata = HashMap::with_capacity(256);
        for index in 0..256 {
            metadata.insert(format!("deleted-{index}"), "value".into());
        }
        metadata.retain(|key, _| key == "deleted-0");
        metadata.insert("k".repeat(2051), "v".repeat(3073));
        let field = Arc::new(Field::new("original", DataType::Int64, true).with_metadata(metadata));
        let policy = ConstantPolicy {
            max_rows: 16,
            max_array_nodes: 16,
            max_logical_elements: 256,
            max_retained_buffer_bytes: 1024 * 1024,
            max_type_depth: 64,
            max_type_nodes: 4096,
            max_dictionary_depth: 8,
            max_metadata_bytes: 65536,
            max_library_validation_work: 32 * 1024 * 1024,
            max_library_validation_bytes: 32 * 1024 * 1024,
        };
        ConstantPool::try_new(
            field,
            FunctionValueType::new(DataType::Int64, true),
            Int64Array::from(vec![Some(7), None]).to_data(),
            policy,
            CompilePhase::Encode,
            &Control::default(),
        )
        .unwrap()
    }

    fn run(
        pool: &ConstantPool,
        field: &Arc<Field>,
        source: usize,
        maximum: usize,
        control: &Control,
    ) -> Result<ConstantBindingResourceFacts, PhysicalConstantCodecError> {
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::Encode)?;
        let result = preflight_constant_binding_resources(pool, field, source, maximum, &mut work);
        // The surrounding namespace owns this finish, including ordinary
        // numerical refusals. The borrowed helper creates no second scope.
        super::super::finish(work, result)
    }

    #[test]
    fn binding_resources_original_arc_skips_only_full_field_comparison() {
        let pool = pool();
        let copied = Arc::new(pool.field().clone());
        let same = run(&pool, pool.field_ref(), SOURCE, WORK, &Control::default()).unwrap();
        let distinct = run(&pool, &copied, SOURCE, WORK, &Control::default()).unwrap();
        assert!(!same.compare_full_field);
        assert!(distinct.compare_full_field);
        assert!(same.work_upper_bound() < distinct.work_upper_bound());
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        assert!(
            arrow_fields_exact_borrowed_observed::<TypeCodecError>(pool.field(), &copied, || work
                .step()
                .map_err(TypeCodecError::from),)
            .unwrap()
        );
        work.finish().unwrap();
    }

    #[test]
    fn binding_resources_deleted_map_uses_trusted_source_invoice_and_exact_work_cap() {
        let pool = pool();
        let copied = Arc::new(pool.field().clone());
        let facts = run(&pool, &copied, SOURCE, WORK, &Control::default()).unwrap();
        assert!(
            run(
                &pool,
                &copied,
                SOURCE,
                facts.work_upper_bound(),
                &Control::default()
            )
            .is_ok()
        );
        assert!(matches!(
            run(
                &pool,
                &copied,
                SOURCE,
                facts.work_upper_bound() - 1,
                &Control::default()
            ),
            Err(PhysicalConstantCodecError::Type(
                TypeCodecError::InvalidShape(_)
            ))
        ));
        let larger = run(&pool, &copied, SOURCE + 4096, WORK, &Control::default()).unwrap();
        assert!(larger.work_upper_bound() > facts.work_upper_bound());
        assert!(matches!(
            run(&pool, &copied, 0, WORK, &Control::default()),
            Err(PhysicalConstantCodecError::Type(
                TypeCodecError::InvalidShape(_)
            ))
        ));
    }

    #[test]
    fn binding_resources_success_and_ordinary_tail_keep_every_original_control_prefix() {
        let pool = pool();
        let copied = Arc::new(pool.field().clone());
        for maximum in [WORK, 0] {
            let recording = Control::default();
            let result = run(&pool, &copied, SOURCE, maximum, &recording);
            assert_eq!(result.is_ok(), maximum != 0);
            let expected = recording.trace.lock().unwrap().clone();
            for stop in 0..expected.len() {
                for cause in CAUSES {
                    let refusing = Control {
                        refusal: Some((stop, cause.clone())),
                        ..Control::default()
                    };
                    assert!(matches!(
                        run(&pool, &copied, SOURCE, maximum, &refusing),
                        Err(PhysicalConstantCodecError::Control(actual)) if actual == cause
                    ));
                    assert_eq!(*refusing.trace.lock().unwrap(), expected[..=stop]);
                }
            }
        }
    }

    #[test]
    fn caller_binding_known_scratch_work_refuses_before_pending_late_control() {
        let pool = pool();
        for pending in [0, 254, 255] {
            for cause in CAUSES {
                let control = Control {
                    refusal: Some((1, cause)),
                    ..Control::default()
                };
                let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
                for _ in 0..pending {
                    work.step().unwrap();
                }
                let result = preflight_constant_binding_resources_in(
                    &pool,
                    pool.field_ref(),
                    SOURCE,
                    0,
                    &mut |_| panic!("parent called after known local scratch refusal"),
                    &mut work,
                );
                assert!(matches!(
                    result,
                    Err(PhysicalConstantCodecError::Control(
                        CompileControlError::ResourceExhausted
                    ))
                ));
                assert_eq!(*control.trace.lock().unwrap(), vec![0]);
            }
        }
    }

    #[test]
    fn caller_binding_snapshot_replays_exact_work_and_source_floor_stays_ordinary() {
        let pool = pool();
        let copied = Arc::new(pool.field().clone());
        let plain = run(&pool, &copied, SOURCE, WORK, &Control::default()).unwrap();
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let mut last = 0;
        let actual = preflight_constant_binding_resources_in(
            &pool,
            &copied,
            SOURCE,
            plain.work_upper_bound(),
            &mut |prefix| {
                assert!(prefix >= last);
                last = prefix;
                Ok(())
            },
            &mut work,
        )
        .unwrap();
        work.finish().unwrap();
        assert_eq!(last, plain.work_upper_bound());
        assert_eq!(actual.work_upper_bound(), plain.work_upper_bound());
        assert!(actual.compare_full_field);
        let control = Control::default();
        let mut work = CompileCheckpoints::try_new(&control, CompilePhase::Encode).unwrap();
        let result = preflight_constant_binding_resources_in(
            &pool,
            &copied,
            0,
            WORK,
            &mut |_| Ok(()),
            &mut work,
        );
        assert!(matches!(
            result,
            Err(PhysicalConstantCodecError::Type(
                TypeCodecError::InvalidShape(_)
            ))
        ));
        work.finish().unwrap();
    }
}
