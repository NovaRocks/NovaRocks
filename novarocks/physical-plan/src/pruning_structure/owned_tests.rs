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

const CAUSES: [CompileControlError; 3] = [
    CompileControlError::Cancelled,
    CompileControlError::DeadlineExceeded,
    CompileControlError::ResourceExhausted,
];
#[derive(Default)]
struct OwnerControl {
    stop: Option<(usize, CompileControlError)>,
    trace: Mutex<Vec<(CompilePhase, u32)>>,
}
impl PureCompileControl for OwnerControl {
    fn checkpoint(&self, phase: CompilePhase, units: u32) -> Result<(), CompileControlError> {
        assert_eq!(
            phase,
            CompilePhase::Decode,
            "borrowed path created a Validate scope"
        );
        assert!(units <= 256);
        let mut trace = self.trace.lock().unwrap();
        let at = trace.len();
        if let Some((stop, _)) = self.stop {
            assert!(at <= stop, "callback after refusal");
        }
        trace.push((phase, units));
        match self.stop.clone() {
            Some((stop, cause)) if at == stop => Err(cause),
            _ => Ok(()),
        }
    }
}
fn finish<T>(
    work: CompileCheckpoints<'_>,
    result: Result<T, FrozenPruningError>,
) -> Result<T, FrozenPruningError> {
    if matches!(result, Err(FrozenPruningError::Control(_))) {
        return result;
    }
    work.finish()?;
    result
}
fn run_count(table: &FrozenFragmentPruning, c: &OwnerControl) -> Result<usize, FrozenPruningError> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    let result = table.dynamic_items_in(&mut work);
    finish(work, result)
}
fn run_validate(
    table: &FrozenFragmentPruning,
    p: &FragmentPackage,
    c: &OwnerControl,
) -> Result<(), FrozenPruningError> {
    let mut work = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
    let result = table.validate_package_in(p, &mut work);
    finish(work, result)
}
fn assert_causes(run: impl Fn(&OwnerControl) -> Result<(), FrozenPruningError>) {
    let c = OwnerControl::default();
    let result = run(&c);
    assert!(!matches!(result, Err(FrozenPruningError::Control(_))));
    let baseline = c.trace.lock().unwrap().clone();
    assert!(!baseline.is_empty());
    for at in 0..baseline.len() {
        for cause in CAUSES {
            let c = OwnerControl {
                stop: Some((at, cause.clone())),
                ..OwnerControl::default()
            };
            assert_eq!(run(&c), Err(FrozenPruningError::Control(cause)));
            assert_eq!(*c.trace.lock().unwrap(), baseline[..=at]);
        }
    }
}

#[test]
fn pruning_borrowed_dynamic_count_retains_original_combined_limit_and_caller_scope() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (package, witness) = fixture(false, 1, &mut fixture_constants);
    let table = pruning_table(&package, vec![witness.clone()]);
    let old = Control::default();
    let expected = table.dynamic_items_observed(&old).unwrap();
    let c = OwnerControl::default();
    assert_eq!(run_count(&table, &c).unwrap(), expected);
    assert_eq!(expected, 6); // target + source + edge + trace + two values.
    assert_eq!(
        c.trace
            .lock()
            .unwrap()
            .iter()
            .map(|(_, n)| *n)
            .collect::<Vec<_>>(),
        *old.work.lock().unwrap()
    );
    assert_causes(|c| run_count(&table, c).map(|_| ()));
    let empty = pruning_table(&package, vec![]);
    let c = OwnerControl::default();
    assert_eq!(run_count(&empty, &c).unwrap(), 0);
    assert_eq!(
        *c.trace.lock().unwrap(),
        vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
    );
    let mut enforced = witness.clone();
    let mut unenforced = witness;
    unenforced.target.field = PruningDomainField::Unenforced;
    let per = (MAX_CONTROL_USE_REFERENCES - 8) / 2;
    enforced.sources[0].columns[0].values = vec![ValueId::new(0); per].into();
    unenforced.sources[0].columns[0].values = vec![ValueId::new(0); per].into();
    let exact = pruning_table(&package, vec![enforced.clone(), unenforced.clone()]);
    assert_eq!(
        run_count(&exact, &OwnerControl::default()).unwrap(),
        MAX_CONTROL_USE_REFERENCES
    );
    // Constructor declaration counting does not certify trace transport.
    unenforced.sources[0].columns[0].values = vec![ValueId::new(0); per + 1].into();
    assert_eq!(
        FrozenFragmentPruning::try_new(
            package.fragment().id(),
            vec![enforced, unenforced],
            &Control::default()
        ),
        Err(FrozenPruningError::TooLarge)
    );
}

#[test]
fn pruning_borrowed_structure_consumes_original_predicate_and_one_index_without_private_scopes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (package, enforced) = fixture(false, 2, &mut fixture_constants);
    let mut unenforced = enforced.clone();
    unenforced.target.field = PruningDomainField::Unenforced;
    let tables = [
        pruning_table(&package, vec![enforced.clone()]),
        pruning_table(&package, vec![unenforced.clone()]),
        pruning_table(&package, vec![enforced.clone(), unenforced]),
    ];
    let mut sums = Vec::new();
    for table in &tables {
        let c = OwnerControl::default();
        run_validate(table, &package, &c).unwrap();
        sums.push(c.trace.lock().unwrap().iter().map(|(_, n)| *n).sum::<u32>());
        assert_causes(|c| run_validate(table, &package, c));
    }
    let index_units: u32 = package
        .fragment()
        .nodes()
        .values()
        .map(|n| 1 + n.inputs.len() as u32)
        .sum();
    assert_eq!(sums[2], sums[0] + sums[1] - index_units);
    let c = OwnerControl::default();
    let mut work = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    let checked = PruningDomainStructure::try_new_in(&package, &enforced, &mut work).unwrap();
    assert!(std::ptr::eq(checked.package(), &package));
    assert!(std::ptr::eq(checked.witness(), &enforced));
    assert_eq!(checked.sources().len(), 2);
    for (checked, original) in checked.sources().iter().zip(enforced.sources.iter()) {
        assert_eq!(checked.responsibility().anchor(), original.responsibility);
        assert_eq!(checked.context(), original.context);
    }
    work.finish().unwrap();
}

#[test]
fn pruning_borrowed_original_witness_failures_keep_categories_and_all_control_prefixes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (package, witness) = fixture(false, 2, &mut fixture_constants);
    for case in 0..6 {
        let mut bad = witness.clone();
        let expected = match case {
            0 => {
                bad.target.scan = NodeId::new(u32::MAX);
                PruningStructureError::InvalidScan
            }
            1 => {
                bad.target.occurrence = ProviderReadOccurrenceId::new(18);
                PruningStructureError::InvalidOccurrence
            }
            2 => {
                bad.sources[0].input_path = Box::default();
                PruningStructureError::InvalidPath
            }
            3 => {
                bad.sources[0].context = witness.sources[1].context;
                PruningStructureError::WrongContext
            }
            4 => {
                bad.sources[0].responsibility.use_id = witness.sources[1].responsibility.use_id;
                PruningStructureError::WrongResponsibility
            }
            _ => {
                bad.sources[0].columns[0].column = ScanColumnId::new(1);
                PruningStructureError::InvalidColumn
            }
        };
        let table = pruning_table(&package, vec![bad.clone()]);
        assert_eq!(
            table.validate_package(&package, &Control::default()),
            Err(FrozenPruningError::Structure(expected.clone()))
        );
        assert_eq!(
            run_validate(&table, &package, &OwnerControl::default()),
            Err(FrozenPruningError::Structure(expected))
        );
        assert_causes(|c| run_validate(&table, &package, c));
    }
    let empty = pruning_table(&package, vec![]);
    let c = OwnerControl::default();
    run_validate(&empty, &package, &c).unwrap();
    assert_eq!(
        *c.trace.lock().unwrap(),
        vec![(CompilePhase::Decode, 0), (CompilePhase::Decode, 0)]
    );
    let foreign =
        FrozenFragmentPruning::try_new(FragmentId::new(999), vec![], &Control::default()).unwrap();
    assert_eq!(
        run_validate(&foreign, &package, &OwnerControl::default()),
        Err(FrozenPruningError::WrongFragment)
    );
    assert_causes(|c| run_validate(&foreign, &package, c));
}

#[test]
fn pruning_borrowed_consumer_index_still_refuses_equal_foreign_snapshot() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (package, witness) = fixture(false, 1, &mut fixture_constants);
    let foreign = package.clone();
    let c = OwnerControl::default();
    let mut observed = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    {
        let mut work = PruningWork::borrowed(&mut observed);
        let index = PruningConsumerIndex::try_new(&package, &mut work).unwrap();
        assert_eq!(
            PruningDomainStructure::try_new_indexed(&foreign, &witness, &index, &c, &mut work)
                .unwrap_err(),
            PruningStructureError::WrongSnapshot
        );
    }
    observed.finish().unwrap();
}

#[test]
fn predicate_borrowed_ports_preserve_actual_source_guards_and_all_callback_causes() {
    let mut fixture_constants = crate::tests::constant_fixture::ConstantFixture::new();
    let (package, witness) = fixture(false, 1, &mut fixture_constants);
    let site = witness.sources[0].responsibility.site;
    for bad in [false, true] {
        let run = |c: &OwnerControl| -> Result<(), FrozenPruningError> {
            let mut observed = CompileCheckpoints::try_new(c, CompilePhase::Decode)?;
            let result = (|| {
                let exact = ExactPredicateResponsibility::try_new_in(
                    package.fragment(),
                    package.expression_uses(),
                    site,
                    &mut observed,
                )
                .map_err(PruningStructureError::from)?;
                assert_eq!(exact.anchor(), witness.sources[0].responsibility);
                let selected = PredicateConjunctSource::try_new_in(
                    package.fragment(),
                    package.expression_uses(),
                    site,
                    if bad { vec![0] } else { vec![] },
                    &mut observed,
                )
                .map_err(PruningStructureError::from)?;
                assert_eq!(selected.context(), witness.sources[0].context);
                Ok(())
            })();
            finish(observed, result)
        };
        if bad {
            assert_eq!(
                run(&OwnerControl::default()),
                Err(FrozenPruningError::Structure(
                    PruningStructureError::Source(PredicateSourceError::NotPositiveConjunction)
                ))
            );
        } else {
            run(&OwnerControl::default()).unwrap();
        }
        assert_causes(run);
    }
}

#[test]
fn pruning_borrowed_original_structural_fuel_exact_boundary_has_no_extra_observation() {
    // This is the original structural contains-value law on an actual borrowed
    // list. It is not a valid Fragment output or a Package admission fixture.
    let columns: Vec<_> = (0..MAX_PRUNING_STRUCTURE_WORK)
        .map(|n| {
            ValueId::new(if n + 1 == MAX_PRUNING_STRUCTURE_WORK {
                1
            } else {
                0
            })
        })
        .collect();
    let c = OwnerControl::default();
    let mut observed = CompileCheckpoints::try_new(&c, CompilePhase::Decode).unwrap();
    {
        let mut work = PruningWork::borrowed(&mut observed);
        contains_value(&columns, ValueId::new(1), &mut work).unwrap();
        assert_eq!(work.units, MAX_PRUNING_STRUCTURE_WORK);
        let before = c.trace.lock().unwrap().clone();
        assert_eq!(
            contains_value(&[ValueId::new(1)], ValueId::new(1), &mut work),
            Err(PruningStructureError::TooLarge)
        );
        assert_eq!(work.units, MAX_PRUNING_STRUCTURE_WORK);
        assert_eq!(*c.trace.lock().unwrap(), before);
        work.finish().unwrap(); // Borrowed finish never observes a footer.
        assert_eq!(*c.trace.lock().unwrap(), before);
    }
    observed.finish().unwrap();
}
