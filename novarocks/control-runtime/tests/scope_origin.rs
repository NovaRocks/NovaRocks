// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not //! Origin predicates do not grant new work or revive a failed scope.
use novarocks_control_runtime::transaction::RuntimeLedger;
use novarocks_control_runtime_core::{
    capacity::ResourceVector,
    identity::Generation,
    scope::{WorkAdmission, WorkKind},
};

#[test]
fn original_scope_identity_rejects_another_ledgers_same_number_and_survives_gate_changes() {
    let first = RuntimeLedger::new(ResourceVector::default(), 4, 2);
    let second = RuntimeLedger::new(ResourceVector::default(), 4, 1);
    let generation = Generation::new(1).unwrap();
    let original = first.open_scope_next(None, generation, 1, 0).unwrap();
    let sibling = first.open_scope_next(None, generation, 1, 0).unwrap();
    let foreign = second.open_scope_next(None, generation, 1, 0).unwrap();
    let same = original.clone();
    assert_eq!(original.id(), foreign.id());
    assert!(original.same_scope(&same) && same.same_scope(&original));
    assert!(!original.same_scope(&sibling) && !original.same_scope(&foreign));
    let failed = original.fail().unwrap();
    assert!(failed > generation && original.same_scope(&same));
    assert!(
        original
            .reserve_next(
                failed,
                ResourceVector::default(),
                WorkKind::Business,
                WorkAdmission::External
            )
            .is_err()
    );
    for scope in [&original, &sibling] {
        scope.close_external().unwrap();
        first.remove_scope(scope.id()).unwrap();
    }
    foreign.close_external().unwrap();
    second.remove_scope(foreign.id()).unwrap();
    assert!(first.is_empty() && second.is_empty());
}
