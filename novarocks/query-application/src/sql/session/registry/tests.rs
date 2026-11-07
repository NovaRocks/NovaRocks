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
use crate::session_error::QueryServiceErrorKind;
use novarocks_spi::connector::{ConnectorInstanceId, ConnectorProviderId};
use std::sync::atomic::{AtomicUsize, Ordering};

fn registry() -> SessionPropertyRegistry {
    SessionPropertyRegistry::new().unwrap()
}
struct NoCatalog {
    calls: AtomicUsize,
}
impl SessionCatalogPropertyPort for NoCatalog {
    fn session_property_catalog(
        &self,
        _: &str,
    ) -> Result<SessionCatalogPropertyAdmission, QueryServiceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(SessionCatalogPropertyAdmission::Absent)
    }
}

#[test]
fn header_preserves_original_text_and_auto_settings_remain_absent() {
    let registry = registry();
    let raw = Vec::from([
        ("enable_global_runtime_filter".into(), "TRUE".into()),
        ("pipeline_dop".into(), "-2".into()),
    ]);
    let inputs = registry
        .prepare_header_inputs(
            &raw,
            &NoCatalog {
                calls: AtomicUsize::new(0),
            },
        )
        .unwrap();
    assert_eq!(
        inputs.raw_system_value("enable_global_runtime_filter"),
        Some("TRUE")
    );
    assert_eq!(
        inputs.read_override("enable_global_runtime_filter"),
        Some(&SessionPropertyValue::Boolean(true))
    );
    assert_eq!(
        inputs.read_override("pipeline_dop"),
        Some(&SessionPropertyValue::Integer(-2))
    );
    let mut state = SessionSqlState::default();
    inputs.project_into(&mut state).unwrap();
    assert_eq!(state.execution_settings.pipeline_dop(), None);
    assert_eq!(
        state.optimizer_settings.enable_global_runtime_filter,
        Some(true)
    );
    assert_eq!(state.optimizer_settings.cbo_broadcast_backend_count, None);
    assert_eq!(
        state.optimizer_settings.cbo_broadcast_node_mem_budget_bytes,
        None
    );
    let visible = registry.visible_metadata(&inputs, &[]).unwrap();
    let row = visible
        .iter()
        .find(|p| p.name == "enable_global_runtime_filter")
        .unwrap();
    assert_eq!(row.value.as_deref(), Some("TRUE"));
    assert_eq!(row.default.as_deref(), Some("true"));
    let auto = visible.iter().find(|p| p.name == "pipeline_dop").unwrap();
    assert_eq!(auto.value.as_deref(), Some("-2"));
    assert_eq!(auto.default, None);
}

#[test]
fn metadata_never_materializes_fixed_or_dynamic_defaults_into_inputs() {
    let registry = registry();
    let inputs = SessionPropertyInputs::default();
    let metadata = registry.visible_metadata(&inputs, &[]).unwrap();
    assert!(inputs.raw_values().is_empty());
    for name in [
        "query_mem_limit",
        "runtime_filter_scan_wait_time",
        "global_runtime_filter_wait_timeout",
        "pipeline_dop",
        "cbo_broadcast_backend_count",
        "cbo_broadcast_node_mem_budget_bytes",
    ] {
        let entry = metadata.iter().find(|row| row.name == name).unwrap();
        assert_eq!(entry.default, None, "{name}");
        assert_eq!(entry.value, None, "{name}");
        assert_eq!(inputs.read_override(name), None);
    }
    let mut state = SessionSqlState::default();
    inputs.project_into(&mut state).unwrap();
    assert_eq!(state.execution_settings.query_mem_limit_bytes(), None);
    assert_eq!(state.execution_settings.pipeline_dop(), None);
    assert_eq!(state.optimizer_settings.effective_backend_count, None);
}

#[test]
fn strict_header_values_reject_incomplete_numbers_and_overflow() {
    let registry = registry();
    for value in ["yes", "1", "on", " true", "false "] {
        assert!(matches!(
            registry.prepare_system_header("enable_global_runtime_filter", value),
            Err(SessionPropertyError::InvalidValue { .. })
        ));
    }
    for value in ["2147483648", "1.0", "2x", " 2", ""] {
        assert!(
            registry
                .prepare_system_header("pipeline_dop", value)
                .is_err(),
            "{value}"
        );
    }
    for value in ["9223372036854775808", "1_000", "2ms", " 2"] {
        assert!(
            registry
                .prepare_system_header("query_timeout", value)
                .is_err(),
            "{value}"
        );
    }
    for value in ["inf", "infinity", "1.0x", " 2.0"] {
        assert!(
            registry
                .prepare_system_header("cbo_broadcast_backend_count", value)
                .is_err(),
            "{value}"
        );
    }
    assert_eq!(
        registry
            .prepare_system_header("query_timeout", "9007199254740993")
            .unwrap()
            .value(),
        &SessionPropertyValue::Bigint(9_007_199_254_740_993)
    );
}

#[test]
fn sql_effect_rendering_roundtrips_without_normalizing_varchar() {
    let registry = registry();
    for (number, text) in [
        (2.0, "2.0"),
        (2.5, "2.5"),
        (1e10, "1.0E10"),
        (-0.0, "-0.0"),
        (f64::INFINITY, "Infinity"),
        (f64::NEG_INFINITY, "-Infinity"),
    ] {
        let effect = registry
            .prepare_sql(
                "cbo_broadcast_backend_count",
                SessionPropertyValue::Double(number),
            )
            .unwrap();
        assert_eq!(effect.raw_value(), text);
        let replay = registry
            .prepare_system_header(effect.name(), effect.raw_value())
            .unwrap();
        let SessionPropertyValue::Double(actual) = replay.value() else {
            panic!("double declaration");
        };
        assert_eq!(actual.to_bits(), number.to_bits());
    }
    let nan = registry
        .prepare_sql(
            "cbo_broadcast_backend_count",
            SessionPropertyValue::Double(f64::NAN),
        )
        .unwrap();
    assert_eq!(nan.raw_value(), "NaN");
    assert!(
        matches!(registry.prepare_system_header(nan.name(), nan.raw_value()).unwrap().value(), SessionPropertyValue::Double(value) if value.is_nan())
    );
    let text = registry
        .prepare_sql(
            "sql_mode",
            SessionPropertyValue::Varchar("group_concat_legacy".into()),
        )
        .unwrap();
    assert_eq!(text.raw_value(), "group_concat_legacy");
    let mut state = SessionSqlState::default();
    text.project_into(&mut state).unwrap();
    assert!(state.sql_semantics.sql_mode().group_concat_legacy());
    assert_eq!(text.raw_value(), "group_concat_legacy");
}

#[test]
fn names_share_one_declaration_without_retired_aliases() {
    let registry = registry();
    let lower = registry
        .lookup_system("pipeline_dop", SessionPropertyOrigin::Header)
        .unwrap();
    let sql = registry
        .lookup_system("PIPELINE_DOP", SessionPropertyOrigin::Sql)
        .unwrap();
    assert!(Arc::ptr_eq(&lower, &sql));
    assert!(
        registry
            .lookup_system("PIPELINE_DOP", SessionPropertyOrigin::Header)
            .is_err()
    );
    for name in [
        "autocommit",
        "catalog",
        "cbo_disabled_rules",
        "trc_unknown_property",
    ] {
        assert!(matches!(
            registry.lookup_system(name, SessionPropertyOrigin::Sql),
            Err(SessionPropertyError::UnknownProperty { .. })
        ));
    }
    assert!(
        registry
            .prepare_sql("pipeline_dop", SessionPropertyValue::Bigint(1))
            .is_err()
    );
}

#[test]
fn whole_header_checks_system_before_catalog_without_mutating_session() {
    let registry = registry();
    let port = NoCatalog {
        calls: AtomicUsize::new(0),
    };
    let raw = Vec::from([
        ("a_missing.x".into(), "1".into()),
        ("trc_unknown_property".into(), "1".into()),
    ]);
    assert!(
        matches!(registry.prepare_header_inputs(&raw, &port), Err(SessionPropertyError::UnknownProperty { name }) if name == "trc_unknown_property")
    );
    assert_eq!(port.calls.load(Ordering::SeqCst), 0);
    let raw = Vec::from([("a_missing.x".into(), "1".into())]);
    assert!(
        matches!(registry.prepare_header_inputs(&raw, &port), Err(SessionPropertyError::CatalogNotFound { catalog }) if catalog == "a_missing")
    );
}

#[test]
fn declaration_defaults_use_original_consumers_and_projection_keeps_other_settings() {
    let registry = registry();
    let original = novarocks_sql::compiler::SessionOptimizerSettings::declaration_defaults();
    let default = |name: &str| {
        registry
            .lookup_system(name, SessionPropertyOrigin::Header)
            .unwrap()
            .default_value()
            .cloned()
    };
    assert_eq!(
        default("global_runtime_filter_build_max_size"),
        original
            .rf_build_max_bytes
            .map(|v| SessionPropertyValue::Bigint(i64::try_from(v).unwrap()))
    );
    assert_eq!(
        default("global_runtime_filter_probe_min_selectivity"),
        original
            .rf_probe_min_selectivity
            .map(SessionPropertyValue::Double)
    );
    assert_eq!(
        default("cbo_max_reorder_node_use_dp"),
        original
            .max_reorder_node_use_dp
            .map(|v| SessionPropertyValue::Bigint(i64::try_from(v).unwrap()))
    );
    assert_eq!(
        default("recursive_cte_max_depth"),
        Some(SessionPropertyValue::Integer(
            novarocks_sql::sql_mode::DEFAULT_RECURSIVE_CTE_MAX_DEPTH
        ))
    );
    let mut state = SessionSqlState::default();
    state.execution_settings.set_pipeline_dop(7);
    let before_mode = state.sql_semantics.clone();
    registry
        .prepare_sql("query_mem_limit", SessionPropertyValue::Bigint(1024))
        .unwrap()
        .project_into(&mut state)
        .unwrap();
    assert_eq!(state.execution_settings.query_mem_limit_bytes(), Some(1024));
    assert_eq!(state.execution_settings.pipeline_dop(), Some(7));
    assert_eq!(state.sql_semantics, before_mode);
    registry
        .prepare_sql("recursive_cte_max_depth", SessionPropertyValue::Integer(8))
        .unwrap()
        .project_into(&mut state)
        .unwrap();
    assert_eq!(state.sql_semantics.recursive_cte_max_depth(), Some(8));
    for value in [0, -1] {
        assert!(
            registry
                .prepare_sql("query_mem_limit", SessionPropertyValue::Bigint(value))
                .is_err()
        );
        assert!(
            registry
                .prepare_sql(
                    "recursive_cte_max_depth",
                    SessionPropertyValue::Integer(value as i32)
                )
                .is_err()
        );
        assert!(
            registry
                .prepare_sql(
                    "global_runtime_filter_build_max_size",
                    SessionPropertyValue::Bigint(-1)
                )
                .is_err()
        );
    }
}

fn identity(catalog: &str, generation: u64) -> CatalogPropertyIdentity {
    CatalogPropertyIdentity {
        attachment_id: uuid::Uuid::from_u128(u128::from(generation)),
        instance_id: ConnectorInstanceId::parse(catalog).unwrap(),
        provider_id: ConnectorProviderId::parse("iceberg").unwrap(),
        generation,
    }
}
struct Factory {
    provider: ConnectorProviderId,
}
impl CatalogPropertyDeclarationFactory for Factory {
    fn provider_id(&self) -> &ConnectorProviderId {
        &self.provider
    }
    fn declarations(
        &self,
        _: &CatalogPropertyIdentity,
        configuration: &[(String, String)],
    ) -> Result<Vec<PropertyDeclaration>, SessionPropertyError> {
        let raw = configuration
            .iter()
            .find(|(key, _)| key == "default")
            .ok_or(SessionPropertyError::InvalidDeclaration {
                detail: "Test instance default missing",
            })?;
        let default: i32 = raw
            .1
            .parse()
            .map_err(|_| SessionPropertyError::InvalidDeclaration {
                detail: "Test instance default invalid",
            })?;
        ["limit", "threshold"]
            .into_iter()
            .map(|name| {
                PropertyDeclaration::catalog(
                    name,
                    SessionPropertyType::Integer,
                    Some(SessionPropertyValue::Integer(default)),
                    "Instance-configured limit",
                    false,
                    |value| {
                        if matches!(value, SessionPropertyValue::Integer(v) if *v > 0) {
                            Ok(())
                        } else {
                            Err("Value must be positive".into())
                        }
                    },
                )
            })
            .collect()
    }
}
fn catalog_registry() -> SessionPropertyRegistry {
    SessionPropertyRegistry::compose(
        declarations::system_declarations().unwrap(),
        vec![Arc::new(Factory {
            provider: ConnectorProviderId::parse("iceberg").unwrap(),
        })],
    )
    .unwrap()
}

#[test]
fn same_provider_catalog_defaults_and_overrides_are_instance_bound() {
    let registry = catalog_registry();
    let first = registry
        .bind_catalog(
            "first",
            identity("first", 1),
            &[("default".into(), "4".into())],
        )
        .unwrap();
    let second = registry
        .bind_catalog(
            "second",
            identity("second", 2),
            &[("default".into(), "9".into())],
        )
        .unwrap();
    let mut inputs = SessionPropertyInputs::default();
    inputs.insert(first.prepare_header("first", "limit", "7").unwrap());
    inputs.insert(second.prepare_header("second", "limit", "8").unwrap());
    assert_eq!(
        inputs.read_catalog_override(&first, "limit"),
        Some(&SessionPropertyValue::Integer(7))
    );
    assert_eq!(
        inputs.read_catalog_override(&second, "limit"),
        Some(&SessionPropertyValue::Integer(8))
    );
    assert_eq!(
        first.lookup("limit").unwrap().default_value(),
        Some(&SessionPropertyValue::Integer(4))
    );
    assert_eq!(
        second.lookup("limit").unwrap().default_value(),
        Some(&SessionPropertyValue::Integer(9))
    );
    let recreated = registry
        .bind_catalog(
            "first",
            identity("first", 3),
            &[("default".into(), "11".into())],
        )
        .unwrap();
    assert_eq!(inputs.read_catalog_override(&recreated, "limit"), None);
    assert_eq!(inputs.raw_catalog_value("first", "limit"), Some("7"));
    let metadata = registry
        .visible_metadata(&inputs, &[Arc::new(recreated)])
        .unwrap();
    let first = metadata
        .iter()
        .find(|row| row.name == "first.limit")
        .unwrap();
    assert_eq!(first.value.as_deref(), Some("11"));
    assert_eq!(first.default.as_deref(), Some("11"));
}

#[test]
fn catalog_failures_keep_absent_unavailable_and_ready_distinct() {
    struct Unavailable;
    impl SessionCatalogPropertyPort for Unavailable {
        fn session_property_catalog(
            &self,
            _: &str,
        ) -> Result<SessionCatalogPropertyAdmission, QueryServiceError> {
            Ok(SessionCatalogPropertyAdmission::Unavailable(
                QueryServiceError::new(
                    QueryServiceErrorKind::Unavailable,
                    "Original catalog unavailable",
                ),
            ))
        }
    }
    let registry = registry();
    let raw = Vec::from([("lake.limit".into(), "1".into())]);
    let error = registry
        .prepare_header_inputs(&raw, &Unavailable)
        .unwrap_err();
    assert!(
        matches!(error, SessionPropertyError::Source(ref source) if source.kind() == QueryServiceErrorKind::Unavailable && source.message() == "Original catalog unavailable")
    );
    let local = CatalogPropertyMetadata::local("memory");
    assert!(
        matches!(local.prepare_header("memory", "limit", "1"), Err(SessionPropertyError::UnknownProperty { name }) if name == "memory.limit")
    );
    assert!(matches!(
        local.prepare_header("another", "limit", "1"),
        Err(SessionPropertyError::CatalogIdentityMismatch)
    ));
}

#[test]
fn duplicate_declarations_are_scoped_and_hidden_metadata_stays_hidden() {
    let property = || {
        PropertyDeclaration::catalog(
            "private",
            SessionPropertyType::Boolean,
            Some(SessionPropertyValue::Boolean(false)),
            "Hidden property",
            true,
            |_| Ok(()),
        )
        .unwrap()
    };
    assert!(
        matches!(SessionPropertyRegistry::compose(vec![property(), property()], Vec::new()), Err(SessionPropertyError::DuplicateProperty { name }) if name == "private")
    );
    let registry = SessionPropertyRegistry::compose(vec![property()], Vec::new()).unwrap();
    assert!(
        registry
            .visible_metadata(&SessionPropertyInputs::default(), &[])
            .unwrap()
            .is_empty()
    );
    assert!(
        PropertyDeclaration::catalog(
            "UPPER",
            SessionPropertyType::Boolean,
            None,
            "Invalid name",
            false,
            |_| Ok(())
        )
        .is_err()
    );
    assert!(
        PropertyDeclaration::catalog(
            "wrong_type",
            SessionPropertyType::Integer,
            Some(SessionPropertyValue::Bigint(1)),
            "Invalid default",
            false,
            |_| Ok(())
        )
        .is_err()
    );
    let factory = || {
        Arc::new(Factory {
            provider: ConnectorProviderId::parse("iceberg").unwrap(),
        }) as Arc<dyn CatalogPropertyDeclarationFactory>
    };
    assert!(matches!(
        SessionPropertyRegistry::compose(Vec::new(), vec![factory(), factory()]),
        Err(SessionPropertyError::DuplicateProvider { .. })
    ));
}

struct ConstantControl(Option<novarocks_type_contract::CompileControlError>);
impl novarocks_type_contract::PureCompileControl for ConstantControl {
    fn checkpoint(
        &self,
        _: novarocks_type_contract::CompilePhase,
        _: u32,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        self.0.clone().map_or(Ok(()), Err)
    }
}
fn constant_pool(
    array: arrow::array::ArrayRef,
    ty: novarocks_type_contract::FunctionValueType,
) -> novarocks_constant_contract::ConstantPool {
    let field = Arc::new(
        ty.try_to_field("original_source_field")
            .unwrap()
            .with_metadata(std::collections::HashMap::from([(
                "source.annotation".to_owned(),
                "retained".to_owned(),
            )])),
    );
    // The test owns three tiny rows. These are test construction bounds, not
    // production session limits or an alternative constant factory.
    let policy = novarocks_constant_contract::ConstantPolicy {
        max_rows: 3,
        max_array_nodes: 4,
        max_logical_elements: 3,
        max_retained_buffer_bytes: 65536,
        max_type_depth: 4,
        max_type_nodes: 4,
        max_dictionary_depth: 2,
        max_metadata_bytes: 65536,
        max_library_validation_work: 65536,
        max_library_validation_bytes: 65536,
    };
    novarocks_constant_contract::ConstantPool::try_new(
        field,
        ty,
        array.to_data(),
        policy,
        novarocks_type_contract::CompilePhase::Validate,
        &ConstantControl(None),
    )
    .unwrap()
}

#[test]
fn checked_sql_constants_read_selected_ordinal_and_retain_complete_source() {
    use arrow::{
        array::{ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, StringArray},
        datatypes::DataType,
    };
    use novarocks_type_contract::FunctionValueType;
    let registry = registry();
    let cases: Vec<(&str, ArrayRef, SessionPropertyValue)> = vec![
        (
            "enable_global_runtime_filter",
            Arc::new(BooleanArray::from(vec![false, true, false])),
            SessionPropertyValue::Boolean(true),
        ),
        (
            "pipeline_dop",
            Arc::new(Int32Array::from(vec![99, 4, 88])),
            SessionPropertyValue::Integer(4),
        ),
        (
            "query_timeout",
            Arc::new(Int64Array::from(vec![99, 9_007_199_254_740_993, 88])),
            SessionPropertyValue::Bigint(9_007_199_254_740_993),
        ),
        (
            "cbo_broadcast_backend_count",
            Arc::new(Float64Array::from(vec![99.0, 2.5, 88.0])),
            SessionPropertyValue::Double(2.5),
        ),
        (
            "sql_mode",
            Arc::new(StringArray::from(vec![
                "unused",
                "GROUP_CONCAT_LEGACY",
                "different",
            ])),
            SessionPropertyValue::Varchar("GROUP_CONCAT_LEGACY".into()),
        ),
    ];
    for (name, array, expected) in cases {
        let ty = FunctionValueType::new(array.data_type().clone(), false);
        let pool = constant_pool(array, ty.clone());
        let source = pool.value(1).unwrap();
        let admitted = registry
            .prepare_sql_constant(name, &source, &ConstantControl(None))
            .unwrap();
        assert_eq!(admitted.value(), &expected);
        let retained = admitted.constant_source().unwrap();
        assert_eq!(retained.ordinal(), 1);
        assert_eq!(retained.value_type(), &ty);
        assert_eq!(retained.field().name(), "original_source_field");
        assert_eq!(
            retained
                .field()
                .metadata()
                .get("source.annotation")
                .map(String::as_str),
            Some("retained")
        );
        assert!(Arc::ptr_eq(retained.pool().array(), source.pool().array()));
        let mut state = SessionSqlState::default();
        let effects = crate::session_effects::SessionEffects::property(admitted.clone());
        state.apply_effects(&effects).unwrap();
        assert_eq!(state.property_inputs().read_override(name), Some(&expected));
        assert!(
            state
                .property_inputs()
                .system_overrides()
                .all(|value| value.constant_source().is_none())
        );
        assert!(
            admitted.constant_source().is_some(),
            "the prepared consumer still owns its original source"
        );
    }
    // SQL must perform widening before this checked target read. A smaller
    // carrier is not retagged merely because its selected value fits.
    let pool = constant_pool(
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        FunctionValueType::new(DataType::Int32, false),
    );
    assert!(matches!(
        registry.prepare_sql_constant(
            "query_timeout",
            &pool.value(1).unwrap(),
            &ConstantControl(None)
        ),
        Err(SessionPropertyError::InvalidValue { .. })
    ));
}

#[test]
fn checked_sql_constants_reject_actual_null_and_nominal_lookalikes() {
    use arrow::{
        array::{Int32Array, StringArray},
        datatypes::DataType,
    };
    use novarocks_type_contract::{FunctionValueType, ValueLogicalType};
    let registry = registry();
    let pool = constant_pool(
        Arc::new(Int32Array::from(vec![Some(4), None, Some(7)])),
        FunctionValueType::new(DataType::Int32, true),
    );
    assert!(
        matches!(registry.prepare_sql_constant("pipeline_dop", &pool.value(1).unwrap(), &ConstantControl(None)), Err(SessionPropertyError::NullValue { name }) if name == "pipeline_dop")
    );
    // A nullable declaration does not make a selected non-NULL value NULL.
    assert_eq!(
        registry
            .prepare_sql_constant(
                "pipeline_dop",
                &pool.value(2).unwrap(),
                &ConstantControl(None)
            )
            .unwrap()
            .value(),
        &SessionPropertyValue::Integer(7)
    );
    let ty =
        FunctionValueType::try_with_logical_type(DataType::Utf8, false, ValueLogicalType::Json)
            .unwrap();
    let nominal = constant_pool(Arc::new(StringArray::from(vec!["1", "2", "3"])), ty);
    assert!(matches!(
        registry.prepare_sql_constant(
            "sql_mode",
            &nominal.value(1).unwrap(),
            &ConstantControl(None)
        ),
        Err(SessionPropertyError::InvalidValue { .. })
    ));
}

#[test]
fn checked_sql_constant_control_refusals_remain_typed() {
    use arrow::{array::Int32Array, datatypes::DataType};
    use novarocks_type_contract::{CompileControlError, FunctionValueType};
    let registry = registry();
    let pool = constant_pool(
        Arc::new(Int32Array::from(vec![1, 2, 3])),
        FunctionValueType::new(DataType::Int32, false),
    );
    for original in [
        CompileControlError::Cancelled,
        CompileControlError::DeadlineExceeded,
        CompileControlError::ResourceExhausted,
    ] {
        let error = registry
            .prepare_sql_constant(
                "pipeline_dop",
                &pool.value(1).unwrap(),
                &ConstantControl(Some(original.clone())),
            )
            .unwrap_err();
        assert!(
            matches!(error, SessionPropertyError::Constant(novarocks_constant_contract::ConstantError::Control(actual)) if actual == original)
        );
    }
}

#[test]
fn one_statement_reuses_one_original_catalog_instance_snapshot() {
    struct Port {
        calls: AtomicUsize,
        metadata: Arc<CatalogPropertyMetadata>,
    }
    impl SessionCatalogPropertyPort for Port {
        fn session_property_catalog(
            &self,
            _: &str,
        ) -> Result<SessionCatalogPropertyAdmission, QueryServiceError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(SessionCatalogPropertyAdmission::Ready(
                self.metadata.clone(),
            ))
        }
    }
    let registry = catalog_registry();
    let metadata = Arc::new(
        registry
            .bind_catalog(
                "first",
                identity("first", 1),
                &[("default".into(), "4".into())],
            )
            .unwrap(),
    );
    let port = Port {
        calls: AtomicUsize::new(0),
        metadata: metadata.clone(),
    };
    let raw = Vec::from([
        ("first.limit".into(), "7".into()),
        ("first.threshold".into(), "8".into()),
    ]);
    let inputs = registry.prepare_header_inputs(&raw, &port).unwrap();
    assert_eq!(port.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        inputs.read_catalog_override(&metadata, "limit"),
        Some(&SessionPropertyValue::Integer(7))
    );
    assert_eq!(
        inputs.read_catalog_override(&metadata, "threshold"),
        Some(&SessionPropertyValue::Integer(8))
    );
}

#[test]
fn catalog_composition_rejects_instance_duplicates_and_requires_provider_factory() {
    struct DuplicateFactory {
        provider: ConnectorProviderId,
    }
    impl CatalogPropertyDeclarationFactory for DuplicateFactory {
        fn provider_id(&self) -> &ConnectorProviderId {
            &self.provider
        }
        fn declarations(
            &self,
            _: &CatalogPropertyIdentity,
            _: &[(String, String)],
        ) -> Result<Vec<PropertyDeclaration>, SessionPropertyError> {
            let declaration = || {
                PropertyDeclaration::catalog(
                    "limit",
                    SessionPropertyType::Integer,
                    None,
                    "Duplicate instance declaration",
                    false,
                    |_| Ok(()),
                )
                .unwrap()
            };
            Ok(vec![declaration(), declaration()])
        }
    }
    let provider = ConnectorProviderId::parse("iceberg").unwrap();
    let duplicate =
        SessionPropertyRegistry::with_catalog_factories(vec![Arc::new(DuplicateFactory {
            provider: provider.clone(),
        })])
        .unwrap();
    assert!(
        matches!(duplicate.bind_catalog("lake", identity("lake", 1), &[]), Err(SessionPropertyError::DuplicateProperty { name }) if name == "limit")
    );
    assert!(matches!(
        registry().bind_catalog("lake", identity("lake", 1), &[]),
        Err(SessionPropertyError::InvalidDeclaration { .. })
    ));
    let empty = SessionPropertyRegistry::with_catalog_factories(vec![Arc::new(
        EmptyCatalogPropertyDeclarationFactory::new(provider),
    )])
    .unwrap();
    let metadata = empty
        .bind_catalog("lake", identity("lake", 1), &[])
        .unwrap();
    assert_eq!(metadata.declarations().count(), 0);
    assert!(
        matches!(metadata.prepare_header("lake", "limit", "1"), Err(SessionPropertyError::UnknownProperty { name }) if name == "lake.limit")
    );
}
