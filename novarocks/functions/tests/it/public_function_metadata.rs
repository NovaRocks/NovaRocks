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

use std::collections::BTreeSet;

use arrow_schema::DataType;
use novarocks_functions::{
    FunctionArgument, FunctionBindingRequest, FunctionKind, FunctionResultType, FunctionValueType,
    FunctionVisibility, testkit::shared_installed_builtin_owner_catalogue,
};

use super::scalar_rows::Control;

#[test]
fn installed_public_function_metadata_borrows_each_original_overload() {
    let catalog = shared_installed_builtin_owner_catalogue();
    let public = catalog.public_function_overloads().collect::<Vec<_>>();
    assert!(!public.is_empty());
    let observed = public
        .iter()
        .map(|row| (row.name, row.function_id, row.overload_id))
        .collect::<BTreeSet<_>>();
    let forms = public
        .iter()
        .map(|row| {
            (
                row.name,
                row.function_id,
                row.overload_id,
                row.argument_types,
            )
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        forms.len(),
        public.len(),
        "duplicate published call signatures"
    );

    for definition in catalog.metadata().definitions() {
        let declaration = definition.binding_declaration().unwrap();
        for overload in declaration.overloads() {
            let found = observed.contains(&(
                definition.canonical_name(),
                declaration.function_id(),
                &overload.identity,
            ));
            assert_eq!(
                found,
                definition.visibility() == FunctionVisibility::Public
                    && !definition.is_declared_only_pure_overload(&overload.identity),
                "{definition:?}"
            );
        }
    }
    assert!(public.iter().all(|row| {
        !row.return_type.is_empty()
            && !row.return_type.contains("derived")
            && !row.argument_types.contains("derived")
    }));
    assert!(
        public
            .iter()
            .all(|row| row.name != "__like_with_escape" && row.name != "version")
    );
}

#[test]
fn public_aliases_resolve_to_the_overload_that_supplied_the_metadata() {
    let catalog = shared_installed_builtin_owner_catalogue();
    for alias in ["rand", "random"] {
        let binding = catalog
            .metadata()
            .resolve_bound_user(
                alias,
                FunctionKind::Scalar,
                FunctionBindingRequest {
                    arguments: &[],
                    logical_argument_count: 0,
                    expected_result_type: None,
                },
                &Control,
            )
            .unwrap();
        let row = catalog
            .public_function_overloads()
            .find(|row| row.name == alias && row.overload_id == &binding.selected.overload)
            .unwrap();
        assert_eq!(row.function_id, &binding.function_id);
        assert_eq!((row.return_type, row.argument_types), ("double", ""));
        assert!(!row.deterministic);
    }

    assert!(catalog.public_function_overloads().any(|row| {
        row.name == "concat"
            && row.return_type == "array(T)"
            && row.argument_types == "array(T), T"
            && row.deterministic
    }));
}

#[test]
fn public_overloads_follow_the_trino_collection_string_and_aggregate_owners() {
    let catalog = shared_installed_builtin_owner_catalogue();
    for (name, arguments, result) in [
        ("map", "", "map(unknown,unknown)"),
        ("map", "array(K), array(V)", "map(K,V)"),
        ("length", "char(x)", "bigint"),
        ("length", "varchar(x)", "bigint"),
        ("min", "T", "T"),
        ("min", "T, bigint", "array(T)"),
        (
            "listagg",
            "varchar, varchar, boolean, varchar, boolean",
            "varchar",
        ),
        ("date_add", "varchar(x), bigint, date", "date"),
        ("approx_distinct", "T", "bigint"),
    ] {
        assert!(
            catalog
                .public_function_overloads()
                .any(|row| row.name == name
                    && row.argument_types == arguments
                    && row.return_type == result),
            "missing public signature {name}({arguments}) -> {result}",
        );
    }
    assert!(
        !catalog
            .public_function_overloads()
            .any(|row| row.name == "approx_distinct" && row.argument_types.ends_with(", double"))
    );
}

#[test]
fn derived_owner_publishes_distinct_real_call_shapes() {
    use DataType::{Decimal128, Float64, Int64};
    let catalog = shared_installed_builtin_owner_catalogue();
    for (name, kind, inputs, parameters, result, output) in [
        (
            "count",
            FunctionKind::Aggregate,
            vec![],
            "",
            "bigint",
            Int64,
        ),
        (
            "count",
            FunctionKind::Aggregate,
            vec![Int64],
            "T",
            "bigint",
            Int64,
        ),
        (
            "sum",
            FunctionKind::Aggregate,
            vec![Int64],
            "bigint",
            "bigint",
            Int64,
        ),
        (
            "sum",
            FunctionKind::Aggregate,
            vec![Decimal128(8, 4)],
            "decimal(p,s)",
            "decimal(38,s)",
            Decimal128(38, 4),
        ),
        (
            "avg",
            FunctionKind::Aggregate,
            vec![Float64],
            "double",
            "double",
            Float64,
        ),
        (
            "round",
            FunctionKind::Scalar,
            vec![Float64],
            "double",
            "bigint",
            Int64,
        ),
        (
            "round",
            FunctionKind::Scalar,
            vec![Float64, Int64],
            "double, bigint",
            "double",
            Float64,
        ),
        (
            "truncate",
            FunctionKind::Scalar,
            vec![Decimal128(8, 4)],
            "decimal(p,s)",
            "decimal(38,s)",
            Decimal128(38, 4),
        ),
    ] {
        let arguments = inputs
            .into_iter()
            .map(|value| FunctionArgument::Value {
                value_type: FunctionValueType::new(value, false),
                constant: None,
            })
            .collect::<Vec<_>>();
        let binding = catalog
            .metadata()
            .resolve_bound_user(
                name,
                kind,
                FunctionBindingRequest {
                    arguments: &arguments,
                    logical_argument_count: arguments.len(),
                    expected_result_type: None,
                },
                &Control,
            )
            .unwrap();
        let FunctionResultType::Scalar(bound_result) = binding.selected.result_type else {
            panic!("expected scalar result for {name}");
        };
        assert_eq!(bound_result.data_type(), &output);
        assert!(catalog.public_function_overloads().any(|row| {
            row.name == name
                && row.function_id == &binding.function_id
                && row.overload_id == &binding.selected.overload
                && row.argument_types == parameters
                && row.return_type == result
        }));
    }
}
