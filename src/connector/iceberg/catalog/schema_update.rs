#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::parser::ast::{DefaultLiteral, SqlType};
    use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
    use std::collections::HashMap;

    fn schema() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Float)).into(),
            ])
            .build()
            .expect("schema")
    }

    fn schema_with_identifier() -> Schema {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "v", Type::Primitive(PrimitiveType::Float)).into(),
            ])
            .with_identifier_field_ids(vec![1])
            .build()
            .expect("schema")
    }

    fn props(entries: &[(&str, &str)]) -> HashMap<String, String> {
        entries
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect()
    }

    fn sorted(mut values: Vec<String>) -> Vec<String> {
        values.sort();
        values
    }

    #[test]
    fn add_column_assigns_fresh_field_id() {
        let updated = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                parent: crate::engine::statement::ColumnPath::root(),
                name: "new_col".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Null),
                position: crate::engine::statement::AddPosition::Default,
            },
        )
        .expect("updated");
        let field = updated.field_by_name("new_col").expect("new field");
        assert_eq!(field.id, 3);
    }

    #[test]
    fn rename_and_modify_preserve_field_id() {
        let renamed = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::RenameColumn {
                path: crate::engine::statement::ColumnPath::parse("id").unwrap(),
                new_name: "order_id".to_string(),
            },
        )
        .expect("renamed");
        assert_eq!(renamed.field_by_name("order_id").expect("renamed").id, 1);

        let modified = apply_change_to_schema_for_test(
            &renamed,
            2,
            &IcebergSchemaChange::ModifyColumn {
                path: crate::engine::statement::ColumnPath::parse("order_id").unwrap(),
                new_type: SqlType::BigInt,
            },
        )
        .expect("modified");
        let field = modified.field_by_name("order_id").expect("modified");
        assert_eq!(field.id, 1);
        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
    }

    #[test]
    fn drop_removes_field_without_reusing_id() {
        let dropped = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::DropColumn {
                path: crate::engine::statement::ColumnPath::parse("v").unwrap(),
            },
        )
        .expect("dropped");
        assert!(dropped.field_by_name("v").is_none());

        let added = apply_change_to_schema_for_test(
            &dropped,
            2,
            &IcebergSchemaChange::AddColumn {
                parent: crate::engine::statement::ColumnPath::root(),
                name: "later".to_string(),
                data_type: SqlType::Int,
                default: None,
                position: crate::engine::statement::AddPosition::Default,
            },
        )
        .expect("added");
        assert_eq!(added.field_by_name("later").expect("later").id, 3);
    }

    #[test]
    fn modify_rejects_unsafe_type_changes() {
        let err = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::ModifyColumn {
                path: crate::engine::statement::ColumnPath::parse("id").unwrap(),
                new_type: SqlType::Double,
            },
        )
        .expect_err("unsafe change");
        assert!(err.contains("unsupported Iceberg type evolution"));
    }

    #[test]
    fn identifier_field_ids_survive_add_rename_modify() {
        let added = apply_change_to_schema_for_test(
            &schema_with_identifier(),
            2,
            &IcebergSchemaChange::AddColumn {
                parent: crate::engine::statement::ColumnPath::root(),
                name: "new_col".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Null),
                position: crate::engine::statement::AddPosition::Default,
            },
        )
        .expect("added");
        assert_eq!(added.identifier_field_ids().collect::<Vec<_>>(), vec![1]);

        let renamed = apply_change_to_schema_for_test(
            &added,
            3,
            &IcebergSchemaChange::RenameColumn {
                path: crate::engine::statement::ColumnPath::parse("id").unwrap(),
                new_name: "order_id".to_string(),
            },
        )
        .expect("renamed");
        assert_eq!(renamed.identifier_field_ids().collect::<Vec<_>>(), vec![1]);
        assert_eq!(renamed.field_by_name("order_id").expect("renamed").id, 1);

        let modified = apply_change_to_schema_for_test(
            &renamed,
            3,
            &IcebergSchemaChange::ModifyColumn {
                path: crate::engine::statement::ColumnPath::parse("order_id").unwrap(),
                new_type: SqlType::BigInt,
            },
        )
        .expect("modified");
        assert_eq!(modified.identifier_field_ids().collect::<Vec<_>>(), vec![1]);
        let field = modified.field_by_name("order_id").expect("modified");
        assert_eq!(
            field.field_type.as_ref(),
            &Type::Primitive(PrimitiveType::Long)
        );
    }

    #[test]
    fn drop_identifier_field_is_rejected() {
        let err = apply_change_to_schema_for_test(
            &schema_with_identifier(),
            2,
            &IcebergSchemaChange::DropColumn {
                path: crate::engine::statement::ColumnPath::parse("id").unwrap(),
            },
        )
        .expect_err("identifier drop");
        assert!(err.contains("identifier"));
    }

    #[test]
    fn reserved_column_changes_are_rejected() {
        let err = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::DropColumn {
                path: crate::engine::statement::ColumnPath::parse("_row_id").unwrap(),
            },
        )
        .expect_err("reserved");
        assert!(err.contains("reserved column"));
    }

    #[test]
    fn drop_rejects_equality_delete_dependency_by_name() {
        let deps = vec!["id".to_string()];
        let err = reject_drop_dependencies_for_test("id", &deps, &[]).expect_err("drop dependency");
        assert!(err.contains("equality-delete"));
    }

    #[test]
    fn drop_rejects_managed_mv_explicit_column_dependency() {
        let mv_sqls = vec!["SELECT id FROM ice.ns.orders".to_string()];
        let err =
            reject_drop_dependencies_for_test("id", &[], &mv_sqls).expect_err("drop dependency");
        assert!(err.contains("materialized view"));
    }

    #[test]
    fn drop_allows_managed_mv_unrelated_column_dependency() {
        let mv_sqls = vec!["SELECT v FROM ice.ns.orders".to_string()];
        reject_drop_dependencies_for_test("id", &[], &mv_sqls).expect("unrelated column");
    }

    #[test]
    fn drop_rejects_managed_mv_select_wildcard_dependency() {
        let mv_sqls = vec!["SELECT * FROM ice.ns.orders".to_string()];
        let err =
            reject_drop_dependencies_for_test("id", &[], &mv_sqls).expect_err("drop dependency");
        assert!(err.contains("materialized view"));
    }

    #[test]
    fn drop_rejects_managed_mv_qualified_wildcard_dependency() {
        for sql in [
            "SELECT o.* FROM ice.ns.orders o",
            "SELECT orders.* FROM ice.ns.orders",
        ] {
            let mv_sqls = vec![sql.to_string()];
            let err = reject_drop_dependencies_for_test("id", &[], &mv_sqls)
                .expect_err("drop dependency");
            assert!(err.contains("materialized view"));
        }
    }

    #[test]
    fn drop_allows_managed_mv_count_star_without_column_token() {
        let mv_sqls = vec!["SELECT COUNT(*) FROM ice.ns.orders".to_string()];
        reject_drop_dependencies_for_test("id", &[], &mv_sqls).expect("count star");
    }

    #[test]
    fn drop_nested_column_blocked_by_equality_delete() {
        let res = reject_drop_dependencies_for_test(
            "address.street",
            &["address.street".to_string()],
            &[],
        );
        assert!(res.is_err());
    }

    #[test]
    fn drop_top_level_struct_blocked_when_equality_delete_targets_inner() {
        let res =
            reject_drop_dependencies_for_test("address", &["address.street".to_string()], &[]);
        assert!(res.is_err());
    }

    #[test]
    fn drop_nested_blocked_when_equality_delete_targets_ancestor() {
        let res =
            reject_drop_dependencies_for_test("address.street", &["address".to_string()], &[]);
        assert!(res.is_err());
    }

    #[test]
    fn drop_unrelated_top_level_not_blocked_when_equality_delete_targets_other() {
        let res = reject_drop_dependencies_for_test("name", &["address.street".to_string()], &[]);
        assert!(res.is_ok());
    }

    #[test]
    fn drop_unrelated_nested_not_blocked_when_equality_delete_targets_sibling() {
        let res =
            reject_drop_dependencies_for_test("address.city", &["address.street".to_string()], &[]);
        assert!(res.is_ok());
    }

    #[test]
    fn drop_nested_blocked_by_managed_mv_referencing_leaf() {
        let res = reject_drop_dependencies_for_test(
            "address.street",
            &[],
            &["SELECT street FROM ice.ns.orders".to_string()],
        );
        assert!(res.is_err());
    }

    #[test]
    fn add_column_sets_logical_type_property_only_when_needed() {
        let tinyint = build_property_updates_for_test(
            &HashMap::new(),
            &IcebergSchemaChange::AddColumn {
                parent: crate::engine::statement::ColumnPath::root(),
                name: "New_Col".to_string(),
                data_type: SqlType::TinyInt,
                default: Some(DefaultLiteral::Null),
                position: crate::engine::statement::AddPosition::Default,
            },
        )
        .expect("updates");
        assert_eq!(
            tinyint.sets.get("novarocks.logical_type.new_col"),
            Some(&"tinyint".to_string())
        );
        assert!(tinyint.removals.is_empty());

        let int = build_property_updates_for_test(
            &HashMap::new(),
            &IcebergSchemaChange::AddColumn {
                parent: crate::engine::statement::ColumnPath::root(),
                name: "new_col".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Null),
                position: crate::engine::statement::AddPosition::Default,
            },
        )
        .expect("updates");
        assert!(int.is_empty());
    }

    #[test]
    fn drop_column_removes_logical_and_aggregation_properties() {
        let changes = build_property_updates_for_test(
            &props(&[
                ("novarocks.logical_type.v", "smallint"),
                ("novarocks.column_agg.v", "sum"),
                ("novarocks.table.key_columns", "id"),
            ]),
            &IcebergSchemaChange::DropColumn {
                path: crate::engine::statement::ColumnPath::parse("V").unwrap(),
            },
        )
        .expect("updates");

        assert!(changes.sets.is_empty());
        assert_eq!(
            sorted(changes.removals),
            vec![
                "novarocks.column_agg.v".to_string(),
                "novarocks.logical_type.v".to_string()
            ]
        );
    }

    #[test]
    fn drop_column_rejects_key_column() {
        let err = build_property_updates_for_test(
            &props(&[("novarocks.table.key_columns", "id,v")]),
            &IcebergSchemaChange::DropColumn {
                path: crate::engine::statement::ColumnPath::parse("V").unwrap(),
            },
        )
        .expect_err("key column drop");
        assert!(err.contains("key column"));
    }

    #[test]
    fn rename_column_moves_properties_and_updates_key_columns() {
        let changes = build_property_updates_for_test(
            &props(&[
                ("novarocks.logical_type.old_col", "smallint"),
                ("novarocks.column_agg.old_col", "max"),
                ("novarocks.table.key_columns", "id,old_col"),
            ]),
            &IcebergSchemaChange::RenameColumn {
                path: crate::engine::statement::ColumnPath::parse("old_col").unwrap(),
                new_name: "New_Col".to_string(),
            },
        )
        .expect("updates");

        assert_eq!(
            changes.sets.get("novarocks.logical_type.new_col"),
            Some(&"smallint".to_string())
        );
        assert_eq!(
            changes.sets.get("novarocks.column_agg.new_col"),
            Some(&"max".to_string())
        );
        assert_eq!(
            changes.sets.get("novarocks.table.key_columns"),
            Some(&"id,new_col".to_string())
        );
        assert_eq!(
            sorted(changes.removals),
            vec![
                "novarocks.column_agg.old_col".to_string(),
                "novarocks.logical_type.old_col".to_string()
            ]
        );
    }

    #[test]
    fn add_column_with_int_default_v3_sets_initial_and_write_default() {
        let updated = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                parent: crate::engine::statement::ColumnPath::root(),
                name: "c".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Int(5)),
                position: crate::engine::statement::AddPosition::Default,
            },
        )
        .expect("v3 add column");
        let field = updated.field_by_name("c").expect("new field");
        let expected = iceberg::spec::Literal::Primitive(iceberg::spec::PrimitiveLiteral::Int(5));
        assert_eq!(field.initial_default.as_ref(), Some(&expected));
        assert_eq!(field.write_default.as_ref(), Some(&expected));
    }

    #[test]
    fn add_column_with_default_null_does_not_persist_metadata() {
        let updated = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                parent: crate::engine::statement::ColumnPath::root(),
                name: "c".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Null),
                position: crate::engine::statement::AddPosition::Default,
            },
        )
        .expect("default null");
        let field = updated.field_by_name("c").expect("new field");
        assert!(field.initial_default.is_none());
        assert!(field.write_default.is_none());
    }

    #[test]
    fn add_column_default_metadata_construction_independent_of_v2_v3() {
        // build_updated_schema does not see format-version; the gate lives in
        // alter_table_schema. Document this by asserting build_updated_schema
        // succeeds even without v3 — the gate must be applied at the
        // alter_table_schema call site, not here.
        let _ = apply_change_to_schema_for_test(
            &schema(),
            2,
            &IcebergSchemaChange::AddColumn {
                parent: crate::engine::statement::ColumnPath::root(),
                name: "c".to_string(),
                data_type: SqlType::Int,
                default: Some(DefaultLiteral::Int(5)),
                position: crate::engine::statement::AddPosition::Default,
            },
        )
        .expect("schema build succeeds; gate enforced upstream");
    }

    #[test]
    fn modify_column_updates_or_removes_logical_type_property() {
        let bigint = build_property_updates_for_test(
            &props(&[("novarocks.logical_type.id", "tinyint")]),
            &IcebergSchemaChange::ModifyColumn {
                path: crate::engine::statement::ColumnPath::parse("ID").unwrap(),
                new_type: SqlType::BigInt,
            },
        )
        .expect("updates");
        assert!(bigint.sets.is_empty());
        assert_eq!(
            bigint.removals,
            vec!["novarocks.logical_type.id".to_string()]
        );

        let decimal = build_property_updates_for_test(
            &HashMap::new(),
            &IcebergSchemaChange::ModifyColumn {
                path: crate::engine::statement::ColumnPath::parse("amount").unwrap(),
                new_type: SqlType::Decimal {
                    precision: 12,
                    scale: 2,
                },
            },
        )
        .expect("updates");
        assert_eq!(
            decimal.sets.get("novarocks.logical_type.amount"),
            Some(&"decimal(12,2)".to_string())
        );
        assert!(decimal.removals.is_empty());
    }

    #[test]
    fn build_property_updates_attests_set_not_null() {
        let change = IcebergSchemaChange::SetNullable {
            path: ColumnPath::parse("address.street").unwrap(),
            nullable: false,
        };
        let updates = build_property_updates_for_test(&HashMap::new(), &change).unwrap();
        let key = "novarocks.nullability.attested.address.street";
        assert!(
            updates.sets.contains_key(key),
            "expected attestation key, got sets={:?}",
            updates.sets
        );
        assert!(
            !updates.removals.contains(&key.to_string()),
            "attestation key must not also be in removals"
        );
    }

    #[test]
    fn build_property_updates_attests_set_not_null_top_level() {
        let change = IcebergSchemaChange::SetNullable {
            path: ColumnPath::parse("c").unwrap(),
            nullable: false,
        };
        let updates = build_property_updates_for_test(&HashMap::new(), &change).unwrap();
        assert!(
            updates
                .sets
                .contains_key("novarocks.nullability.attested.c")
        );
    }

    #[test]
    fn build_property_updates_removes_attestation_on_drop_not_null_when_present() {
        let mut existing = HashMap::new();
        existing.insert(
            "novarocks.nullability.attested.c".to_string(),
            "2026-05-06T00:00:00Z".to_string(),
        );
        let change = IcebergSchemaChange::SetNullable {
            path: ColumnPath::parse("c").unwrap(),
            nullable: true,
        };
        let updates = build_property_updates_for_test(&existing, &change).unwrap();
        assert!(
            updates
                .removals
                .contains(&"novarocks.nullability.attested.c".to_string())
        );
        assert!(updates.sets.is_empty());
    }

    #[test]
    fn build_property_updates_drop_not_null_no_op_when_attestation_absent() {
        let change = IcebergSchemaChange::SetNullable {
            path: ColumnPath::parse("c").unwrap(),
            nullable: true,
        };
        let updates = build_property_updates_for_test(&HashMap::new(), &change).unwrap();
        assert!(updates.is_empty());
    }

    #[test]
    fn build_property_updates_reorder_remains_no_op() {
        let change = IcebergSchemaChange::Reorder {
            path: ColumnPath::parse("c").unwrap(),
            position: AddPosition::First,
        };
        let updates = build_property_updates_for_test(&HashMap::new(), &change).unwrap();
        assert!(updates.is_empty());
    }

    // ----- find_field_by_path tests -----

    #[test]
    fn find_field_by_path_top_level() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("a").unwrap();
        let (field_id, _ty) = find_field_by_path(&schema, &path).unwrap();
        assert_eq!(field_id, 1);
    }

    #[test]
    fn find_field_by_path_nested_struct() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::optional(
                11,
                "street",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                12,
                "city",
                Type::Primitive(PrimitiveType::String),
            )),
        ]));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("address.street").unwrap();
        let (field_id, ty) = find_field_by_path(&schema, &path).unwrap();
        assert_eq!(field_id, 11);
        assert_eq!(ty, Type::Primitive(PrimitiveType::String));
    }

    #[test]
    fn find_field_by_path_unknown_returns_err() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("nonexistent").unwrap();
        assert!(find_field_by_path(&schema, &path).is_err());
    }

    #[test]
    fn find_field_by_path_array_element() {
        use iceberg::spec::ListType;
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "tags",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    11,
                    Type::Primitive(PrimitiveType::Int),
                    false,
                )))),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("tags.element").unwrap();
        let (field_id, ty) = find_field_by_path(&schema, &path).unwrap();
        assert_eq!(field_id, 11);
        assert_eq!(ty, Type::Primitive(PrimitiveType::Int));
    }

    #[test]
    fn find_field_by_path_map_value() {
        use iceberg::spec::MapType;
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "m",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        11,
                        "key",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        12,
                        "value",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                )),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("m.value").unwrap();
        let (field_id, _ty) = find_field_by_path(&schema, &path).unwrap();
        assert_eq!(field_id, 12);
    }

    #[test]
    fn find_field_by_path_map_key() {
        use iceberg::spec::MapType;
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "m",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        11,
                        "key",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        12,
                        "value",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                )),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("m.key").unwrap();
        let (field_id, ty) = find_field_by_path(&schema, &path).unwrap();
        assert_eq!(field_id, 11);
        assert_eq!(ty, Type::Primitive(PrimitiveType::String));
    }

    #[test]
    fn find_field_by_path_list_invalid_descent() {
        use iceberg::spec::ListType;
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "tags",
                Type::List(ListType::new(Arc::new(NestedField::list_element(
                    11,
                    Type::Primitive(PrimitiveType::Int),
                    true,
                )))),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("tags.foo").unwrap();
        let res = find_field_by_path(&schema, &path);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("element"));
    }

    #[test]
    fn find_field_by_path_map_invalid_descent() {
        use iceberg::spec::MapType;
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "m",
                Type::Map(MapType::new(
                    Arc::new(NestedField::required(
                        11,
                        "key",
                        Type::Primitive(PrimitiveType::String),
                    )),
                    Arc::new(NestedField::optional(
                        12,
                        "value",
                        Type::Primitive(PrimitiveType::Int),
                    )),
                )),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("m.invalid").unwrap();
        let res = find_field_by_path(&schema, &path);
        assert!(res.is_err());
    }

    #[test]
    fn find_field_by_path_descent_into_primitive() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "id",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("id.foo").unwrap();
        let res = find_field_by_path(&schema, &path);
        assert!(res.is_err());
        assert!(res.unwrap_err().contains("non-composite"));
    }

    #[test]
    fn apply_drop_at_nested_struct() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::optional(
                11,
                "street",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                12,
                "city",
                Type::Primitive(PrimitiveType::String),
            )),
        ]));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("address.city").unwrap();
        let new = apply_drop_at(&schema, &path).unwrap();
        let address = new.as_struct().fields()[0].clone();
        let Type::Struct(s) = &*address.field_type else {
            panic!()
        };
        assert_eq!(s.fields().len(), 1);
        assert_eq!(s.fields()[0].name, "street");
    }

    #[test]
    fn apply_drop_at_top_level_works() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("a").unwrap();
        let new = apply_drop_at(&schema, &path).unwrap();
        assert_eq!(new.as_struct().fields().len(), 1);
        assert_eq!(new.as_struct().fields()[0].name, "b");
    }

    #[test]
    fn apply_drop_at_unknown_path_errors() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("nonexistent").unwrap();
        assert!(apply_drop_at(&schema, &path).is_err());
    }

    #[test]
    fn apply_drop_at_into_list_or_map_rejected() {
        use iceberg::spec::ListType;
        let element = Arc::new(NestedField::list_element(
            11,
            Type::Primitive(PrimitiveType::Int),
            true,
        ));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "tags",
                Type::List(ListType::new(element)),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("tags.element").unwrap();
        let res = apply_drop_at(&schema, &path);
        assert!(res.is_err());
        // Drop on list element / map key/value is not allowed; only struct fields can be dropped.
    }

    // ----- apply_rename_at tests -----

    #[test]
    fn apply_rename_at_nested() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
            11,
            "street",
            Type::Primitive(PrimitiveType::String),
        ))]));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("address.street").unwrap();
        let new = apply_rename_at(&schema, &path, "road").unwrap();
        let address = new.as_struct().fields()[0].clone();
        let Type::Struct(s) = &*address.field_type else {
            panic!()
        };
        assert_eq!(s.fields()[0].name, "road");
        assert_eq!(s.fields()[0].id, 11);
    }

    #[test]
    fn apply_rename_at_top_level() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "old",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("old").unwrap();
        let new = apply_rename_at(&schema, &path, "fresh").unwrap();
        assert_eq!(new.as_struct().fields()[0].name, "fresh");
        assert_eq!(new.as_struct().fields()[0].id, 1);
    }

    #[test]
    fn apply_rename_at_conflict_with_sibling() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::optional(
                11,
                "street",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                12,
                "city",
                Type::Primitive(PrimitiveType::String),
            )),
        ]));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("address.street").unwrap();
        assert!(apply_rename_at(&schema, &path, "city").is_err());
    }

    #[test]
    fn apply_rename_at_into_list_or_map_rejected() {
        use iceberg::spec::ListType;
        let element = Arc::new(NestedField::list_element(
            11,
            Type::Primitive(PrimitiveType::Int),
            true,
        ));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "tags",
                Type::List(ListType::new(element)),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("tags.element").unwrap();
        assert!(apply_rename_at(&schema, &path, "item").is_err());
    }

    #[test]
    fn apply_rename_at_unknown_path_errors() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("nonexistent").unwrap();
        assert!(apply_rename_at(&schema, &path, "x").is_err());
    }

    // ----- apply_modify_at tests -----

    #[test]
    fn apply_modify_at_top_level_int_to_long() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "n",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("n").unwrap();
        let new =
            apply_modify_at(&schema, &path, &crate::sql::parser::ast::SqlType::BigInt).unwrap();
        assert!(matches!(
            *new.as_struct().fields()[0].field_type,
            Type::Primitive(PrimitiveType::Long)
        ));
        assert_eq!(new.as_struct().fields()[0].id, 1);
    }

    #[test]
    fn apply_modify_at_nested_struct_int_to_long() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
            11,
            "n",
            Type::Primitive(PrimitiveType::Int),
        ))]));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "wrap", inner))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("wrap.n").unwrap();
        let new =
            apply_modify_at(&schema, &path, &crate::sql::parser::ast::SqlType::BigInt).unwrap();
        let Type::Struct(s) = &*new.as_struct().fields()[0].field_type else {
            panic!()
        };
        assert!(matches!(
            *s.fields()[0].field_type,
            Type::Primitive(PrimitiveType::Long)
        ));
        assert_eq!(s.fields()[0].id, 11);
    }

    #[test]
    fn apply_modify_at_array_element() {
        use iceberg::spec::ListType;
        let element = Arc::new(NestedField::list_element(
            11,
            Type::Primitive(PrimitiveType::Int),
            true,
        ));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "tags",
                Type::List(ListType::new(element)),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("tags.element").unwrap();
        let new =
            apply_modify_at(&schema, &path, &crate::sql::parser::ast::SqlType::BigInt).unwrap();
        let Type::List(l) = &*new.as_struct().fields()[0].field_type else {
            panic!()
        };
        assert!(matches!(
            *l.element_field.field_type,
            Type::Primitive(PrimitiveType::Long)
        ));
        assert_eq!(l.element_field.id, 11);
    }

    #[test]
    fn apply_modify_at_map_value() {
        use iceberg::spec::MapType;
        let key = Arc::new(NestedField::map_key_element(
            11,
            Type::Primitive(PrimitiveType::String),
        ));
        let value = Arc::new(NestedField::map_value_element(
            12,
            Type::Primitive(PrimitiveType::Int),
            true,
        ));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "m",
                Type::Map(MapType::new(key, value)),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("m.value").unwrap();
        let new =
            apply_modify_at(&schema, &path, &crate::sql::parser::ast::SqlType::BigInt).unwrap();
        let Type::Map(m) = &*new.as_struct().fields()[0].field_type else {
            panic!()
        };
        assert!(matches!(
            *m.value_field.field_type,
            Type::Primitive(PrimitiveType::Long)
        ));
        assert_eq!(m.value_field.id, 12);
    }

    #[test]
    fn apply_modify_at_unsupported_widen_rejected() {
        // String -> BigInt is not in the widen_type matrix, so this must fail.
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "s",
                Type::Primitive(PrimitiveType::String),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("s").unwrap();
        let res = apply_modify_at(&schema, &path, &crate::sql::parser::ast::SqlType::BigInt);
        assert!(res.is_err());
    }

    #[test]
    fn apply_modify_at_unknown_path_errors() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let path = crate::engine::statement::ColumnPath::parse("nonexistent").unwrap();
        assert!(
            apply_modify_at(&schema, &path, &crate::sql::parser::ast::SqlType::BigInt).is_err()
        );
    }

    // ----- apply_add_at tests -----

    #[test]
    fn apply_add_at_top_level_default_position() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let mut last_id = 1;
        let new = apply_add_at(
            &schema,
            &crate::engine::statement::ColumnPath::root(),
            "b",
            &SqlType::Int,
            None,
            crate::engine::statement::AddPosition::Default,
            &mut last_id,
        )
        .unwrap();
        assert_eq!(new.as_struct().fields().len(), 2);
        assert_eq!(new.as_struct().fields()[1].name, "b");
        assert_eq!(new.as_struct().fields()[1].id, 2);
    }

    #[test]
    fn apply_add_at_top_level_first_position() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let mut last_id = 1;
        let new = apply_add_at(
            &schema,
            &crate::engine::statement::ColumnPath::root(),
            "b",
            &SqlType::Int,
            None,
            crate::engine::statement::AddPosition::First,
            &mut last_id,
        )
        .unwrap();
        assert_eq!(new.as_struct().fields()[0].name, "b");
        assert_eq!(new.as_struct().fields()[1].name, "a");
    }

    #[test]
    fn apply_add_at_top_level_after_position() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "c",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap();
        let mut last_id = 2;
        let new = apply_add_at(
            &schema,
            &crate::engine::statement::ColumnPath::root(),
            "b",
            &SqlType::Int,
            None,
            crate::engine::statement::AddPosition::After("a".to_string()),
            &mut last_id,
        )
        .unwrap();
        let names: Vec<_> = new
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn apply_add_at_top_level_before_position() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "c",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap();
        let mut last_id = 2;
        let new = apply_add_at(
            &schema,
            &crate::engine::statement::ColumnPath::root(),
            "b",
            &SqlType::Int,
            None,
            crate::engine::statement::AddPosition::Before("c".to_string()),
            &mut last_id,
        )
        .unwrap();
        let names: Vec<_> = new
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        assert_eq!(names, vec!["a", "b", "c"]);
    }

    #[test]
    fn apply_add_at_nested_struct() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
            11,
            "street",
            Type::Primitive(PrimitiveType::String),
        ))]));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
            .build()
            .unwrap();
        let parent = crate::engine::statement::ColumnPath::parse("address").unwrap();
        let mut last_id = 11;
        let new = apply_add_at(
            &schema,
            &parent,
            "zip",
            &SqlType::Int,
            None,
            crate::engine::statement::AddPosition::Default,
            &mut last_id,
        )
        .unwrap();
        let Type::Struct(s) = &*new.as_struct().fields()[0].field_type else {
            panic!()
        };
        assert_eq!(s.fields().len(), 2);
        assert_eq!(s.fields()[1].name, "zip");
        assert_eq!(s.fields()[1].id, 12);
    }

    #[test]
    fn apply_add_at_name_conflict_top_level() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let mut last_id = 1;
        let res = apply_add_at(
            &schema,
            &crate::engine::statement::ColumnPath::root(),
            "a",
            &SqlType::Int,
            None,
            crate::engine::statement::AddPosition::Default,
            &mut last_id,
        );
        assert!(res.is_err());
    }

    #[test]
    fn apply_add_at_after_target_not_found() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let mut last_id = 1;
        let res = apply_add_at(
            &schema,
            &crate::engine::statement::ColumnPath::root(),
            "b",
            &SqlType::Int,
            None,
            crate::engine::statement::AddPosition::After("nonexistent".to_string()),
            &mut last_id,
        );
        assert!(res.is_err());
    }

    #[test]
    fn apply_add_at_into_non_struct_parent_rejected() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "n",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let parent = crate::engine::statement::ColumnPath::parse("n").unwrap();
        let mut last_id = 1;
        let res = apply_add_at(
            &schema,
            &parent,
            "x",
            &SqlType::Int,
            None,
            crate::engine::statement::AddPosition::Default,
            &mut last_id,
        );
        assert!(res.is_err());
    }

    #[test]
    fn apply_set_nullable_at_top_level() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let path = ColumnPath::parse("a").unwrap();
        let new = apply_set_nullable_at(&schema, &path, false).unwrap();
        assert!(new.as_struct().fields()[0].required);
    }

    #[test]
    fn apply_set_nullable_at_nested() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
            11,
            "street",
            Type::Primitive(PrimitiveType::String),
        ))]));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
            .build()
            .unwrap();
        let path = ColumnPath::parse("address.street").unwrap();
        let new = apply_set_nullable_at(&schema, &path, false).unwrap();
        let Type::Struct(s) = &*new.as_struct().fields()[0].field_type else {
            panic!()
        };
        assert!(s.fields()[0].required);
    }

    #[test]
    fn apply_set_nullable_at_identifier_field_rejects_drop_not_null() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::required(
                1,
                "id",
                Type::Primitive(PrimitiveType::Long),
            ))])
            .with_identifier_field_ids(vec![1])
            .build()
            .unwrap();
        let path = ColumnPath::parse("id").unwrap();
        assert!(apply_set_nullable_at(&schema, &path, true).is_err());
    }

    #[test]
    fn apply_reorder_at_top_level_first() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "c",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap();
        let path = ColumnPath::parse("c").unwrap();
        let new = apply_reorder_at(&schema, &path, &AddPosition::First).unwrap();
        let names: Vec<_> = new
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        assert_eq!(names, vec!["c", "a", "b"]);
    }

    #[test]
    fn apply_reorder_at_after_target() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    3,
                    "c",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap();
        let path = ColumnPath::parse("a").unwrap();
        let new = apply_reorder_at(&schema, &path, &AddPosition::After("b".to_string())).unwrap();
        let names: Vec<_> = new
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        assert_eq!(names, vec!["b", "a", "c"]);
    }

    #[test]
    fn apply_reorder_at_nested_struct() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![
            Arc::new(NestedField::optional(
                11,
                "street",
                Type::Primitive(PrimitiveType::String),
            )),
            Arc::new(NestedField::optional(
                12,
                "city",
                Type::Primitive(PrimitiveType::String),
            )),
        ]));
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(1, "address", inner))])
            .build()
            .unwrap();
        let path = ColumnPath::parse("address.city").unwrap();
        let new =
            apply_reorder_at(&schema, &path, &AddPosition::Before("street".to_string())).unwrap();
        let Type::Struct(s) = &*new.as_struct().fields()[0].field_type else {
            panic!()
        };
        let names: Vec<_> = s.fields().iter().map(|f| f.name.clone()).collect();
        assert_eq!(names, vec!["city", "street"]);
    }

    #[test]
    fn apply_reorder_at_after_target_in_different_parent_rejected() {
        use iceberg::spec::StructType;
        let inner = Type::Struct(StructType::new(vec![Arc::new(NestedField::optional(
            11,
            "street",
            Type::Primitive(PrimitiveType::String),
        ))]));
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(1, "address", inner)),
                Arc::new(NestedField::optional(
                    2,
                    "name",
                    Type::Primitive(PrimitiveType::String),
                )),
            ])
            .build()
            .unwrap();
        let path = ColumnPath::parse("address.street").unwrap();
        assert!(apply_reorder_at(&schema, &path, &AddPosition::After("name".to_string())).is_err());
    }

    #[test]
    fn build_updated_schema_dispatches_set_nullable() {
        let schema = Schema::builder()
            .with_fields(vec![Arc::new(NestedField::optional(
                1,
                "a",
                Type::Primitive(PrimitiveType::Int),
            ))])
            .build()
            .unwrap();
        let change = IcebergSchemaChange::SetNullable {
            path: ColumnPath::parse("a").unwrap(),
            nullable: false,
        };
        let new = build_updated_schema(&schema, 1, &change).unwrap();
        assert!(new.as_struct().fields()[0].required);
    }

    #[test]
    fn build_updated_schema_dispatches_reorder() {
        let schema = Schema::builder()
            .with_fields(vec![
                Arc::new(NestedField::optional(
                    1,
                    "a",
                    Type::Primitive(PrimitiveType::Int),
                )),
                Arc::new(NestedField::optional(
                    2,
                    "b",
                    Type::Primitive(PrimitiveType::Int),
                )),
            ])
            .build()
            .unwrap();
        let change = IcebergSchemaChange::Reorder {
            path: ColumnPath::parse("b").unwrap(),
            position: AddPosition::First,
        };
        let new = build_updated_schema(&schema, 2, &change).unwrap();
        assert_eq!(new.as_struct().fields()[0].name, "b");
    }

    #[test]
    fn widen_decimal_precision_increase_same_scale() {
        let curr = Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        });
        let new = SqlType::Decimal {
            precision: 20,
            scale: 2,
        };
        let widened = widen_type(&curr, &new).unwrap();
        let Type::Primitive(PrimitiveType::Decimal { precision, scale }) = widened else {
            panic!()
        };
        assert_eq!(precision, 20);
        assert_eq!(scale, 2);
    }

    #[test]
    fn widen_decimal_scale_change_rejected() {
        let curr = Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        });
        let new = SqlType::Decimal {
            precision: 10,
            scale: 3,
        };
        assert!(widen_type(&curr, &new).is_err());
    }

    #[test]
    fn widen_decimal_precision_decrease_rejected() {
        let curr = Type::Primitive(PrimitiveType::Decimal {
            precision: 20,
            scale: 2,
        });
        let new = SqlType::Decimal {
            precision: 10,
            scale: 2,
        };
        assert!(widen_type(&curr, &new).is_err());
    }

    #[test]
    fn widen_decimal_same_precision_same_scale_rejected() {
        let curr = Type::Primitive(PrimitiveType::Decimal {
            precision: 10,
            scale: 2,
        });
        let new = SqlType::Decimal {
            precision: 10,
            scale: 2,
        };
        assert!(widen_type(&curr, &new).is_err());
    }

    #[test]
    fn widen_date_to_timestamp() {
        let curr = Type::Primitive(PrimitiveType::Date);
        let new = SqlType::DateTime;
        let widened = widen_type(&curr, &new).unwrap();
        assert!(matches!(widened, Type::Primitive(PrimitiveType::Timestamp)));
    }

    #[test]
    fn widen_string_to_binary_rejected() {
        let curr = Type::Primitive(PrimitiveType::String);
        let new = SqlType::Binary;
        assert!(widen_type(&curr, &new).is_err());
    }

    #[test]
    fn widen_long_to_int_rejected() {
        let curr = Type::Primitive(PrimitiveType::Long);
        let new = SqlType::Int;
        assert!(widen_type(&curr, &new).is_err());
    }

    #[test]
    fn widen_double_to_float_rejected() {
        let curr = Type::Primitive(PrimitiveType::Double);
        let new = SqlType::Float;
        assert!(widen_type(&curr, &new).is_err());
    }

    #[test]
    fn widen_timestamp_to_date_rejected() {
        let curr = Type::Primitive(PrimitiveType::Timestamp);
        let new = SqlType::Date;
        assert!(widen_type(&curr, &new).is_err());
    }

    #[test]
    fn reserved_key_format_version() {
        let reason = is_reserved_property_key("format-version").expect("denied");
        let lower = reason.to_lowercase();
        assert!(lower.contains("upgrade table") || lower.contains("format-version"));
    }

    #[test]
    fn reserved_key_identifier_field_ids() {
        assert!(is_reserved_property_key("identifier-field-ids").is_some());
    }

    #[test]
    fn reserved_key_internal_schema_id() {
        assert!(is_reserved_property_key("current-schema-id").is_some());
        assert!(is_reserved_property_key("default-spec-id").is_some());
        assert!(is_reserved_property_key("default-sort-order-id").is_some());
    }

    #[test]
    fn reserved_key_internal_counters() {
        assert!(is_reserved_property_key("last-column-id").is_some());
        assert!(is_reserved_property_key("last-partition-id").is_some());
        assert!(is_reserved_property_key("last-sequence-number").is_some());
    }

    #[test]
    fn reserved_key_novarocks_logical_type_prefix() {
        assert!(is_reserved_property_key("novarocks.logical_type.foo").is_some());
    }

    #[test]
    fn reserved_key_novarocks_column_agg_prefix() {
        assert!(is_reserved_property_key("novarocks.column_agg.bar").is_some());
    }

    #[test]
    fn reserved_key_novarocks_table_key_columns() {
        assert!(is_reserved_property_key("novarocks.table.key_columns").is_some());
    }

    #[test]
    fn reserved_key_novarocks_nullability_attested_prefix() {
        assert!(
            is_reserved_property_key("novarocks.nullability.attested.address.street").is_some()
        );
    }

    #[test]
    fn reserved_key_novarocks_unknown_prefix_blocked() {
        // Forward-compat: any unknown novarocks.* key is reserved.
        assert!(is_reserved_property_key("novarocks.future.feature").is_some());
        assert!(is_reserved_property_key("novarocks.x").is_some());
    }

    #[test]
    fn reserved_key_allows_iceberg_write_props() {
        assert!(is_reserved_property_key("write.parquet.compression-codec").is_none());
        assert!(is_reserved_property_key("write.format.default").is_none());
        assert!(is_reserved_property_key("write.target-file-size-bytes").is_none());
        assert!(is_reserved_property_key("history.expire.max-snapshot-age-ms").is_none());
        assert!(is_reserved_property_key("commit.retry.num-retries").is_none());
        assert!(is_reserved_property_key("gc.enabled").is_none());
    }

    #[test]
    fn reserved_key_allows_user_custom_keys() {
        assert!(is_reserved_property_key("my.custom.key").is_none());
        assert!(is_reserved_property_key("foo").is_none());
        assert!(is_reserved_property_key("comment").is_none());
    }

    #[test]
    fn properties_op_collect_denylist_hits_on_set() {
        use crate::engine::statement::PropertiesOp;
        let op = PropertiesOp::Set {
            entries: vec![
                ("comment".to_string(), "ok".to_string()),
                ("format-version".to_string(), "2".to_string()),
                ("write.format.default".to_string(), "parquet".to_string()),
                ("novarocks.internal".to_string(), "x".to_string()),
            ],
        };
        let hits = collect_property_denylist_hits(&op);
        assert_eq!(hits.len(), 2, "expected 2 denied keys, got: {hits:?}");
        let denied_keys: Vec<&str> = hits.iter().map(|(k, _)| k.as_str()).collect();
        assert!(denied_keys.contains(&"format-version"));
        assert!(denied_keys.contains(&"novarocks.internal"));
    }

    #[test]
    fn properties_op_collect_denylist_hits_on_unset() {
        use crate::engine::statement::PropertiesOp;
        let op = PropertiesOp::Unset {
            keys: vec![
                "comment".to_string(),
                "last-column-id".to_string(),
                "write.parquet.compression-codec".to_string(),
                "novarocks.nullability.attested.id".to_string(),
            ],
            if_exists: false,
        };
        let hits = collect_property_denylist_hits(&op);
        assert_eq!(hits.len(), 2, "expected 2 denied keys, got: {hits:?}");
        let denied_keys: Vec<&str> = hits.iter().map(|(k, _)| k.as_str()).collect();
        assert!(denied_keys.contains(&"last-column-id"));
        assert!(denied_keys.contains(&"novarocks.nullability.attested.id"));
    }

    #[test]
    fn properties_op_validate_unset_strict_missing_key() {
        use crate::engine::statement::PropertiesOp;
        let op = PropertiesOp::Unset {
            keys: vec!["present".to_string(), "missing-key".to_string()],
            if_exists: false,
        };
        let existing = props(&[("present", "v")]);
        let result = validate_unset_keys_present(&op, &existing);
        assert!(result.is_err(), "expected Err for missing key");
        let msg = result.unwrap_err();
        assert!(
            msg.contains("'missing-key'"),
            "error must quote the missing key; got: {msg}"
        );
    }

    #[test]
    fn properties_op_validate_unset_if_exists_skips_missing() {
        use crate::engine::statement::PropertiesOp;
        let op = PropertiesOp::Unset {
            keys: vec!["not-there".to_string(), "also-absent".to_string()],
            if_exists: true,
        };
        let existing = props(&[]);
        assert!(
            validate_unset_keys_present(&op, &existing).is_ok(),
            "IF EXISTS must not error on missing keys"
        );
    }

    #[test]
    fn properties_op_compute_remove_keys_filters_missing_when_if_exists() {
        use crate::engine::statement::PropertiesOp;
        let op = PropertiesOp::Unset {
            keys: vec![
                "present".to_string(),
                "absent".to_string(),
                "also-present".to_string(),
            ],
            if_exists: true,
        };
        let existing = props(&[("present", "v1"), ("also-present", "v2")]);
        let mut result = compute_remove_keys(&op, &existing);
        result.sort();
        assert_eq!(
            result,
            vec!["also-present".to_string(), "present".to_string()]
        );
    }

    #[test]
    fn properties_op_compute_remove_keys_all_missing_with_if_exists_returns_empty() {
        use crate::engine::statement::PropertiesOp;
        let existing = props(&[]);
        let op = PropertiesOp::Unset {
            keys: vec!["a".to_string(), "b".to_string()],
            if_exists: true,
        };
        let computed = compute_remove_keys(&op, &existing);
        assert!(computed.is_empty());
    }
}

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use iceberg::spec::{NestedField, NestedFieldRef, PrimitiveType, Schema, StructType, Type};
use iceberg::transaction::{ActionCommit, ApplyTransactionAction, Transaction, TransactionAction};
use iceberg::{TableRequirement, TableUpdate};

use crate::connector::iceberg::catalog::registry::{
    TABLE_KEY_COLUMNS_PROPERTY, column_aggregation_property_key, logical_type_property_key,
    logical_type_property_value,
};
use crate::connector::iceberg::commit::retry::commit_with_retry;
use crate::engine::StandaloneState;
use crate::engine::backend_resolver::resolve_existing_table_target;
use crate::engine::catalog::normalize_identifier;
use crate::engine::statement::{
    AlterIcebergPropertiesStmt, AlterIcebergSchemaStmt, ColumnPath, IcebergSchemaChange,
    PropertiesOp,
};
use crate::sql::parser::ast::SqlType;

#[cfg(test)]
pub(crate) fn apply_change_to_schema_for_test(
    current: &Schema,
    last_column_id: i32,
    change: &IcebergSchemaChange,
) -> Result<Schema, String> {
    build_updated_schema(current, last_column_id, change)
}

/// Walk `path` through `schema`, descending into nested STRUCT, LIST (via "element"),
/// and MAP (via "key" / "value") types.  Returns the leaf field-id and its `Type`.
// This function is called by the B2-B7 walker subagents in subsequent schema evolution tasks.
#[allow(dead_code)]
pub(crate) fn find_field_by_path(
    schema: &Schema,
    path: &ColumnPath,
) -> Result<(i32, Type), String> {
    if path.is_empty() {
        return Err("column path is empty".to_string());
    }
    let mut current_fields: Vec<NestedFieldRef> = schema.as_struct().fields().to_vec();
    let mut field_id: Option<i32> = None;
    let mut field_type: Option<Type> = None;
    let segments = path.segments();
    let mut idx = 0;
    while idx < segments.len() {
        let seg = &segments[idx];
        let is_last = idx + 1 == segments.len();
        let normalized = normalize_identifier(seg)?;
        let found: Option<NestedFieldRef> = current_fields
            .iter()
            .find(|f| normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str()))
            .cloned();
        let Some(f) = found else {
            return Err(format!("column path '{}' not found", path.dotted()));
        };
        field_id = Some(f.id);
        field_type = Some((*f.field_type).clone());
        if is_last {
            break;
        }
        // Descend one level into the composite child; skip the synthetic segment name on
        // the next iteration because the field itself already carries the right name.
        match &*f.field_type {
            Type::Struct(s) => {
                current_fields = s.fields().to_vec();
            }
            Type::List(l) => {
                let next = &segments[idx + 1];
                let next_norm = normalize_identifier(next)?;
                if next_norm != "element" {
                    return Err(format!(
                        "list field '{}' can only descend into 'element'",
                        path.dotted()
                    ));
                }
                // The element field's own name is "element", so the next loop
                // iteration will match it by name.
                current_fields = vec![l.element_field.clone()];
            }
            Type::Map(m) => {
                let next = &segments[idx + 1];
                let next_norm = normalize_identifier(next)?;
                match next_norm.as_str() {
                    "key" => {
                        current_fields = vec![m.key_field.clone()];
                    }
                    "value" => {
                        current_fields = vec![m.value_field.clone()];
                    }
                    _ => {
                        return Err(format!(
                            "map field '{}' can only descend into 'key' or 'value'",
                            path.dotted()
                        ));
                    }
                }
            }
            _ => {
                return Err(format!(
                    "column path '{}' descends into non-composite type",
                    path.dotted()
                ));
            }
        }
        idx += 1;
    }
    Ok((field_id.unwrap(), field_type.unwrap()))
}

/// Rebuild `schema` with the column at `path` removed.
///
/// Only struct fields can be dropped.  Descending into a list element or map key/value
/// is rejected, because those are not named columns and cannot be individually dropped.
#[allow(dead_code)]
pub(crate) fn apply_drop_at(schema: &Schema, path: &ColumnPath) -> Result<Schema, String> {
    let identifier_field_ids: Vec<i32> = schema.identifier_field_ids().collect();
    let new_fields = drop_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
    )?;
    let arc_fields: Vec<NestedFieldRef> = new_fields.into_iter().map(Arc::new).collect();
    Schema::builder()
        .with_fields(arc_fields)
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after drop: {e}"))
}

fn drop_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("drop path is empty".to_string());
    }
    let head = normalize_identifier(&segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            if segments.len() == 1 {
                // skip = drop this field at the top of the remaining path
                continue;
            }
            let new_inner_type = drop_in_type(&f.field_type, &segments[1..])?;
            let mut updated = (*f).clone();
            updated.field_type = Box::new(new_inner_type);
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{}' not found for drop", head));
    }
    Ok(out)
}

fn drop_in_type(ty: &Type, segments: &[String]) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = drop_in_fields(s.fields().iter().cloned().collect(), segments)?;
            let arc_fields: Vec<NestedFieldRef> = new.into_iter().map(Arc::new).collect();
            Ok(Type::Struct(StructType::new(arc_fields)))
        }
        Type::List(_) | Type::Map(_) => {
            Err("drop path cannot descend into list element or map key/value".to_string())
        }
        _ => Err("drop path descends into non-composite type".to_string()),
    }
}

/// Rebuild `schema` with the column at `path` renamed to `new_name`.
///
/// Only struct fields can be renamed.  Descending into a list element or map key/value
/// is rejected.  Renaming to a name already used by a sibling field is rejected.
#[allow(dead_code)]
pub(crate) fn apply_rename_at(
    schema: &Schema,
    path: &ColumnPath,
    new_name: &str,
) -> Result<Schema, String> {
    let identifier_field_ids: Vec<i32> = schema.identifier_field_ids().collect();
    let new_fields = rename_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
        new_name,
    )?;
    let arc_fields: Vec<NestedFieldRef> = new_fields.into_iter().map(Arc::new).collect();
    Schema::builder()
        .with_fields(arc_fields)
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after rename: {e}"))
}

fn rename_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
    new_name: &str,
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("rename path is empty".to_string());
    }
    let head = normalize_identifier(&segments[0])?;
    let new_norm = normalize_identifier(new_name)?;
    let is_leaf = segments.len() == 1;
    // For leaf rename, validate no sibling already has the new name (case-insensitive).
    if is_leaf {
        for f in &fields {
            let f_norm = normalize_identifier(&f.name).ok();
            if f_norm.as_deref() != Some(head.as_str())
                && f_norm.as_deref() == Some(new_norm.as_str())
            {
                return Err(format!(
                    "rename target '{new_name}' conflicts with existing sibling"
                ));
            }
        }
    }
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let mut updated = (*f).clone();
            if is_leaf {
                updated.name = new_name.to_string();
            } else {
                let new_inner = rename_in_type(&f.field_type, &segments[1..], new_name)?;
                updated.field_type = Box::new(new_inner);
            }
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{head}' not found for rename"));
    }
    Ok(out)
}

fn rename_in_type(ty: &Type, segments: &[String], new_name: &str) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = rename_in_fields(s.fields().iter().cloned().collect(), segments, new_name)?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        Type::List(_) | Type::Map(_) => {
            Err("rename path cannot descend into list element or map key/value".to_string())
        }
        _ => Err("rename path descends into non-composite type".to_string()),
    }
}

/// Rebuild `schema` with the column at `path` widened to `new_type`.
///
/// Descends into STRUCT fields recursively.  Also handles LIST `element` and MAP `key`/`value`
/// element widening at any depth.  The actual type-compatibility check is delegated to
/// [`widen_type`], which enforces the narrow safe-widening matrix (Int → Long, Float → Double).
#[allow(dead_code)]
pub(crate) fn apply_modify_at(
    schema: &Schema,
    path: &ColumnPath,
    new_type: &SqlType,
) -> Result<Schema, String> {
    let identifier_field_ids: Vec<i32> = schema.identifier_field_ids().collect();
    let new_fields = modify_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
        new_type,
    )?;
    let arc_fields: Vec<NestedFieldRef> = new_fields.into_iter().map(Arc::new).collect();
    Schema::builder()
        .with_fields(arc_fields)
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after modify: {e}"))
}

fn modify_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
    new_type: &SqlType,
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("modify path is empty".to_string());
    }
    let head = normalize_identifier(&segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let mut updated = (*f).clone();
            if segments.len() == 1 {
                // Leaf: apply the type widening directly.
                let widened = widen_type(&f.field_type, new_type)?;
                updated.field_type = Box::new(widened);
            } else {
                // Non-leaf: descend into the composite child type.
                let new_inner = modify_in_type(&f.field_type, &segments[1..], new_type)?;
                updated.field_type = Box::new(new_inner);
            }
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{head}' not found for modify"));
    }
    Ok(out)
}

fn modify_in_type(ty: &Type, segments: &[String], new_type: &SqlType) -> Result<Type, String> {
    let head = normalize_identifier(&segments[0])?;
    match ty {
        Type::Struct(s) => {
            // Re-enter modify_in_fields with the same segments: modify_in_fields will consume
            // `head` by matching it against the struct's child fields.
            let new = modify_in_fields(s.fields().iter().cloned().collect(), segments, new_type)?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        Type::List(l) => {
            if head != "element" || segments.len() != 1 {
                return Err("list modify must target '<list>.element'".to_string());
            }
            let widened = widen_type(&l.element_field.field_type, new_type)?;
            let mut new_elem = (*l.element_field).clone();
            new_elem.field_type = Box::new(widened);
            Ok(Type::List(iceberg::spec::ListType::new(Arc::new(new_elem))))
        }
        Type::Map(m) => match (head.as_str(), segments.len()) {
            ("value", 1) => {
                let widened = widen_type(&m.value_field.field_type, new_type)?;
                let mut new_v = (*m.value_field).clone();
                new_v.field_type = Box::new(widened);
                Ok(Type::Map(iceberg::spec::MapType::new(
                    m.key_field.clone(),
                    Arc::new(new_v),
                )))
            }
            ("key", 1) => {
                let widened = widen_type(&m.key_field.field_type, new_type)?;
                let mut new_k = (*m.key_field).clone();
                new_k.field_type = Box::new(widened);
                Ok(Type::Map(iceberg::spec::MapType::new(
                    Arc::new(new_k),
                    m.value_field.clone(),
                )))
            }
            _ => Err("map modify must target '<map>.key' or '<map>.value'".to_string()),
        },
        _ => Err("modify path descends into non-composite type".to_string()),
    }
}

/// Rebuild `schema` with the column at `path` having its nullability flipped.
///
/// `nullable = false` => SET NOT NULL (`required = true`).
/// `nullable = true` => DROP NOT NULL (`required = false`).
/// DROP NOT NULL is rejected on identifier fields, since identifier columns must remain
/// required by Iceberg spec.  Only top-level or STRUCT-nested fields are supported; LIST
/// element / MAP key/value nullability cannot be toggled this way.
#[allow(dead_code)]
pub(crate) fn apply_set_nullable_at(
    schema: &Schema,
    path: &ColumnPath,
    nullable: bool,
) -> Result<Schema, String> {
    let identifier_field_ids: Vec<i32> = schema.identifier_field_ids().collect();
    if nullable {
        let (target_id, _) = find_field_by_path(schema, path)?;
        if identifier_field_ids.contains(&target_id) {
            return Err(format!(
                "cannot DROP NOT NULL on identifier field '{}'",
                path.dotted()
            ));
        }
    }
    let new_fields = set_nullable_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
        nullable,
    )?;
    let arc_fields: Vec<NestedFieldRef> = new_fields.into_iter().map(Arc::new).collect();
    Schema::builder()
        .with_fields(arc_fields)
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after set nullable: {e}"))
}

fn set_nullable_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
    nullable: bool,
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("set nullable path is empty".to_string());
    }
    let head = normalize_identifier(&segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let mut updated = (*f).clone();
            if segments.len() == 1 {
                updated.required = !nullable;
            } else {
                let new_inner = set_nullable_in_type(&f.field_type, &segments[1..], nullable)?;
                updated.field_type = Box::new(new_inner);
            }
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{}' not found for set nullable", head));
    }
    Ok(out)
}

fn set_nullable_in_type(ty: &Type, segments: &[String], nullable: bool) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new =
                set_nullable_in_fields(s.fields().iter().cloned().collect(), segments, nullable)?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        _ => {
            Err("SET/DROP NOT NULL only supported on top-level or STRUCT-nested fields".to_string())
        }
    }
}

fn build_updated_schema(
    current: &Schema,
    last_column_id: i32,
    change: &IcebergSchemaChange,
) -> Result<Schema, String> {
    reject_reserved_change(change)?;
    match change {
        IcebergSchemaChange::AddColumn {
            parent,
            name,
            data_type,
            default,
            position,
        } => {
            let mut next_id = last_column_id;
            apply_add_at(
                current,
                parent,
                name,
                data_type,
                default.as_ref(),
                position.clone(),
                &mut next_id,
            )
        }
        IcebergSchemaChange::DropColumn { path } => {
            let identifier_field_ids: Vec<i32> = current.identifier_field_ids().collect();
            let (id, _) = find_field_by_path(current, path)?;
            if identifier_field_ids.contains(&id) {
                return Err(format!(
                    "Iceberg schema evolution cannot drop identifier column `{}`",
                    path.dotted()
                ));
            }
            apply_drop_at(current, path)
        }
        IcebergSchemaChange::RenameColumn { path, new_name } => {
            apply_rename_at(current, path, new_name)
        }
        IcebergSchemaChange::ModifyColumn { path, new_type } => {
            apply_modify_at(current, path, new_type)
        }
        IcebergSchemaChange::SetNullable { path, nullable } => {
            apply_set_nullable_at(current, path, *nullable)
        }
        IcebergSchemaChange::Reorder { path, position } => {
            apply_reorder_at(current, path, position)
        }
    }
}

fn reject_reserved_change(change: &IcebergSchemaChange) -> Result<(), String> {
    let names: Vec<&str> = match change {
        IcebergSchemaChange::AddColumn { name, .. } => vec![name.as_str()],
        IcebergSchemaChange::DropColumn { path } => {
            debug_assert!(!path.is_empty(), "DropColumn path must be non-empty");
            path.last().map(|n| vec![n]).unwrap_or_default()
        }
        IcebergSchemaChange::RenameColumn { path, new_name } => {
            debug_assert!(!path.is_empty(), "RenameColumn path must be non-empty");
            let mut names = Vec::new();
            if let Some(old_name) = path.last() {
                names.push(old_name);
            }
            names.push(new_name.as_str());
            names
        }
        IcebergSchemaChange::ModifyColumn { path, .. } => {
            debug_assert!(!path.is_empty(), "ModifyColumn path must be non-empty");
            path.last().map(|n| vec![n]).unwrap_or_default()
        }
        IcebergSchemaChange::SetNullable { path, .. } => {
            debug_assert!(!path.is_empty(), "SetNullable path must be non-empty");
            path.last().map(|n| vec![n]).unwrap_or_default()
        }
        IcebergSchemaChange::Reorder { path, .. } => {
            debug_assert!(!path.is_empty(), "Reorder path must be non-empty");
            path.last().map(|n| vec![n]).unwrap_or_default()
        }
    };
    for name in names {
        if crate::exec::row_position::is_iceberg_row_id(name)
            || crate::exec::row_position::is_iceberg_last_updated_sequence_number(name)
        {
            return Err(format!(
                "Iceberg schema evolution cannot modify reserved column `{name}`"
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
fn reject_drop_dependencies_for_test(
    column: &str,
    equality_delete_columns: &[String],
    mv_sqls: &[String],
) -> Result<(), String> {
    let target = ManagedMvTarget::new("ice", "ns", "orders")?;
    let mv_dependencies = mv_sqls
        .iter()
        .map(|sql| ManagedMvDependency {
            select_sql: sql.clone(),
            target: target.clone(),
        })
        .collect::<Vec<_>>();
    reject_drop_dependencies(column, equality_delete_columns, &mv_dependencies)
}

fn reject_drop_dependencies(
    column: &str,
    equality_delete_columns: &[String],
    mv_dependencies: &[ManagedMvDependency],
) -> Result<(), String> {
    let target_segments = normalize_dotted_path(column)?;
    if target_segments.is_empty() {
        return Err(format!("DROP COLUMN `{column}` has empty column path"));
    }
    for ed in equality_delete_columns {
        let ed_segments = normalize_dotted_path(ed)?;
        // Block when the dropped path is an ancestor, descendant, or equal
        // to an equality-delete column reference.
        if path_overlaps(&target_segments, &ed_segments) {
            return Err(format!(
                "DROP COLUMN `{column}` is blocked because an Iceberg equality-delete file references `{ed}`"
            ));
        }
    }
    let leaf = target_segments
        .last()
        .expect("checked non-empty above")
        .as_str();
    for dependency in mv_dependencies {
        if managed_mv_depends_on_column(dependency, leaf) {
            return Err(format!(
                "DROP COLUMN `{column}` is blocked because a managed materialized view references it"
            ));
        }
    }
    Ok(())
}

fn normalize_dotted_path(input: &str) -> Result<Vec<String>, String> {
    input
        .split('.')
        .map(|segment| {
            if segment.is_empty() {
                return Err(format!("invalid column path `{input}`: empty segment"));
            }
            normalize_identifier(segment)
        })
        .collect()
}

fn path_overlaps(a: &[String], b: &[String]) -> bool {
    let common = a.len().min(b.len());
    a[..common] == b[..common]
}

#[derive(Clone, Debug)]
struct ManagedMvDependency {
    select_sql: String,
    target: ManagedMvTarget,
}

#[derive(Clone, Debug)]
struct ManagedMvTarget {
    catalog: String,
    namespace: String,
    table: String,
}

impl ManagedMvTarget {
    fn new(catalog: &str, namespace: &str, table: &str) -> Result<Self, String> {
        Ok(Self {
            catalog: normalize_identifier(catalog)?,
            namespace: normalize_identifier(namespace)?,
            table: normalize_identifier(table)?,
        })
    }

    fn from_backend(
        target: &crate::engine::backend_resolver::TargetBackend,
    ) -> Result<Self, String> {
        Self::new(&target.catalog, &target.namespace, &target.table)
    }
}

fn managed_mv_depends_on_column(
    dependency: &ManagedMvDependency,
    normalized_identifier: &str,
) -> bool {
    sql_mentions_identifier(&dependency.select_sql, normalized_identifier)
        || sql_projects_target_wildcard(&dependency.select_sql, &dependency.target)
}

fn sql_mentions_identifier(sql: &str, normalized_identifier: &str) -> bool {
    sql.split(|ch: char| !(ch == '_' || ch.is_ascii_alphanumeric()))
        .filter(|token| !token.is_empty())
        .any(|token| token.eq_ignore_ascii_case(normalized_identifier))
}

fn sql_projects_target_wildcard(sql: &str, target: &ManagedMvTarget) -> bool {
    let Ok(normalized) = crate::sql::parser::dialect::normalize_for_raw_parse(sql) else {
        return false;
    };
    let Ok(statement) = crate::sql::parser::parse_normalized_sql_raw(&normalized) else {
        return false;
    };
    let sqlparser::ast::Statement::Query(query) = statement else {
        return false;
    };
    query_projects_target_wildcard(&query, target)
}

fn query_projects_target_wildcard(query: &sqlparser::ast::Query, target: &ManagedMvTarget) -> bool {
    if let Some(with) = &query.with {
        for cte in &with.cte_tables {
            if query_projects_target_wildcard(&cte.query, target) {
                return true;
            }
        }
    }
    set_expr_projects_target_wildcard(query.body.as_ref(), target)
}

fn set_expr_projects_target_wildcard(
    set_expr: &sqlparser::ast::SetExpr,
    target: &ManagedMvTarget,
) -> bool {
    match set_expr {
        sqlparser::ast::SetExpr::Select(select) => select_projects_target_wildcard(select, target),
        sqlparser::ast::SetExpr::Query(query) => query_projects_target_wildcard(query, target),
        sqlparser::ast::SetExpr::SetOperation { left, right, .. } => {
            set_expr_projects_target_wildcard(left, target)
                || set_expr_projects_target_wildcard(right, target)
        }
        _ => false,
    }
}

fn select_projects_target_wildcard(
    select: &sqlparser::ast::Select,
    target: &ManagedMvTarget,
) -> bool {
    let mut target_qualifiers = HashSet::new();
    for table_with_joins in &select.from {
        if collect_target_qualifiers_from_table_with_joins(
            table_with_joins,
            target,
            &mut target_qualifiers,
        ) {
            return true;
        }
    }

    select.projection.iter().any(|item| match item {
        sqlparser::ast::SelectItem::Wildcard(_) => !target_qualifiers.is_empty(),
        sqlparser::ast::SelectItem::QualifiedWildcard(kind, _) => {
            qualified_wildcard_matches_target(kind, &target_qualifiers)
        }
        _ => false,
    })
}

fn collect_target_qualifiers_from_table_with_joins(
    table_with_joins: &sqlparser::ast::TableWithJoins,
    target: &ManagedMvTarget,
    qualifiers: &mut HashSet<String>,
) -> bool {
    if collect_target_qualifiers_from_factor(&table_with_joins.relation, target, qualifiers) {
        return true;
    }
    for join in &table_with_joins.joins {
        if collect_target_qualifiers_from_factor(&join.relation, target, qualifiers) {
            return true;
        }
    }
    false
}

fn collect_target_qualifiers_from_factor(
    factor: &sqlparser::ast::TableFactor,
    target: &ManagedMvTarget,
    qualifiers: &mut HashSet<String>,
) -> bool {
    match factor {
        sqlparser::ast::TableFactor::Table { name, alias, .. } => {
            if object_name_matches_target(name, target) {
                qualifiers.extend(object_name_qualifier_keys(name));
                if let Some(alias) = alias
                    && let Ok(normalized) = normalize_identifier(&alias.name.value)
                {
                    qualifiers.insert(normalized);
                }
            }
            false
        }
        sqlparser::ast::TableFactor::Derived { subquery, .. } => {
            query_projects_target_wildcard(subquery, target)
        }
        sqlparser::ast::TableFactor::NestedJoin {
            table_with_joins, ..
        } => collect_target_qualifiers_from_table_with_joins(table_with_joins, target, qualifiers),
        sqlparser::ast::TableFactor::Pivot { table, .. }
        | sqlparser::ast::TableFactor::Unpivot { table, .. }
        | sqlparser::ast::TableFactor::MatchRecognize { table, .. } => {
            collect_target_qualifiers_from_factor(table, target, qualifiers)
        }
        _ => false,
    }
}

fn qualified_wildcard_matches_target(
    kind: &sqlparser::ast::SelectItemQualifiedWildcardKind,
    target_qualifiers: &HashSet<String>,
) -> bool {
    match kind {
        sqlparser::ast::SelectItemQualifiedWildcardKind::ObjectName(name) => {
            object_name_qualifier_keys(name)
                .into_iter()
                .any(|key| target_qualifiers.contains(&key))
        }
        sqlparser::ast::SelectItemQualifiedWildcardKind::Expr(expr) => expr_qualifier_keys(expr)
            .into_iter()
            .any(|key| target_qualifiers.contains(&key)),
    }
}

fn expr_qualifier_keys(expr: &sqlparser::ast::Expr) -> Vec<String> {
    match expr {
        sqlparser::ast::Expr::Identifier(ident) => normalize_identifier(&ident.value)
            .map(|name| vec![name])
            .unwrap_or_default(),
        sqlparser::ast::Expr::CompoundIdentifier(idents) => {
            let parts = idents
                .iter()
                .map(|ident| normalize_identifier(&ident.value))
                .collect::<Result<Vec<_>, _>>();
            parts
                .map(|parts| qualifier_keys_from_parts(&parts))
                .unwrap_or_default()
        }
        _ => Vec::new(),
    }
}

fn object_name_matches_target(name: &sqlparser::ast::ObjectName, target: &ManagedMvTarget) -> bool {
    let parts = normalized_object_name_parts(name);
    match parts.as_deref() {
        Some([catalog, namespace, table]) => {
            catalog == &target.catalog && namespace == &target.namespace && table == &target.table
        }
        Some([namespace, table]) => namespace == &target.namespace && table == &target.table,
        Some([table]) => table == &target.table,
        _ => false,
    }
}

fn object_name_qualifier_keys(name: &sqlparser::ast::ObjectName) -> Vec<String> {
    normalized_object_name_parts(name)
        .map(|parts| qualifier_keys_from_parts(&parts))
        .unwrap_or_default()
}

fn qualifier_keys_from_parts(parts: &[String]) -> Vec<String> {
    let mut keys = Vec::new();
    if !parts.is_empty() {
        keys.push(parts.join("."));
        if parts.len() >= 2 {
            keys.push(parts[parts.len() - 2..].join("."));
        }
        keys.push(parts[parts.len() - 1].clone());
    }
    keys
}

fn normalized_object_name_parts(name: &sqlparser::ast::ObjectName) -> Option<Vec<String>> {
    name.0
        .iter()
        .map(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => normalize_identifier(&ident.value),
            _ => Err("unsupported object name part".to_string()),
        })
        .collect::<Result<Vec<_>, _>>()
        .ok()
}

use crate::engine::statement::AddPosition;

/// Rebuild `schema` with a new column added under `parent` (or at top-level if `parent` is root).
///
/// The new column is inserted at `position` (Default = append, First, After, Before).
/// Only STRUCT parents are supported; adding into a LIST or MAP element is rejected.
/// A name-conflict check (case-insensitive) is performed against the target sibling list.
#[allow(dead_code)]
pub(crate) fn apply_add_at(
    schema: &Schema,
    parent: &ColumnPath,
    name: &str,
    data_type: &SqlType,
    default: Option<&crate::sql::parser::ast::DefaultLiteral>,
    position: AddPosition,
    last_column_id: &mut i32,
) -> Result<Schema, String> {
    let identifier_field_ids: Vec<i32> = schema.identifier_field_ids().collect();

    // Allocate new field id; reserve a window above for any nested complex type ids.
    let new_id = last_column_id
        .checked_add(1)
        .ok_or_else(|| "too many iceberg columns".to_string())?;
    let mut next_nested_id = new_id
        .checked_add(1)
        .ok_or_else(|| "too many iceberg columns".to_string())?;
    let new_ty = crate::connector::iceberg::catalog::registry::iceberg_type_for_sql_type(
        data_type,
        &mut next_nested_id,
    )?;
    let mut new_field = NestedField::optional(new_id, name, new_ty);
    if let Some(lit) = default {
        if let Some(iceberg_lit) =
            crate::connector::iceberg::default_value::default_literal_to_iceberg(lit, data_type)?
        {
            new_field = new_field
                .with_initial_default(iceberg_lit.clone())
                .with_write_default(iceberg_lit);
        }
    }
    *last_column_id = next_nested_id - 1;

    let new_fields = add_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        parent.segments(),
        new_field,
        &position,
    )?;
    let arc_fields: Vec<NestedFieldRef> = new_fields.into_iter().map(Arc::new).collect();
    Schema::builder()
        .with_fields(arc_fields)
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after add: {e}"))
}

fn add_in_fields(
    fields: Vec<Arc<NestedField>>,
    parent_segments: &[String],
    new_field: NestedField,
    position: &AddPosition,
) -> Result<Vec<NestedField>, String> {
    if parent_segments.is_empty() {
        // Top-level add: name conflict check + position insertion.
        let normalized = normalize_identifier(&new_field.name)?;
        for f in &fields {
            if normalize_identifier(&f.name).ok().as_deref() == Some(normalized.as_str()) {
                return Err(format!(
                    "Iceberg column `{}` already exists",
                    new_field.name
                ));
            }
        }
        let mut existing: Vec<NestedField> = fields.iter().map(|f| (**f).clone()).collect();
        insert_at_position(&mut existing, new_field, position)?;
        return Ok(existing);
    }
    let head = normalize_identifier(&parent_segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let new_inner = add_in_type(
                &f.field_type,
                &parent_segments[1..],
                new_field.clone(),
                position,
            )?;
            let mut updated = (*f).clone();
            updated.field_type = Box::new(new_inner);
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!(
            "parent column '{}' not found for add",
            &parent_segments[0]
        ));
    }
    Ok(out)
}

fn add_in_type(
    ty: &Type,
    parent_segments: &[String],
    new_field: NestedField,
    position: &AddPosition,
) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = add_in_fields(
                s.fields().iter().cloned().collect(),
                parent_segments,
                new_field,
                position,
            )?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        _ => Err("ADD COLUMN parent path must point to a STRUCT".to_string()),
    }
}

/// Insert `new_field` into `fields` at the requested `position`.
///
/// Reused by B7 reorder.
#[allow(dead_code)]
pub(crate) fn insert_at_position(
    fields: &mut Vec<NestedField>,
    new_field: NestedField,
    position: &AddPosition,
) -> Result<(), String> {
    match position {
        AddPosition::Default => {
            fields.push(new_field);
            Ok(())
        }
        AddPosition::First => {
            fields.insert(0, new_field);
            Ok(())
        }
        AddPosition::After(target) => {
            let target_norm = normalize_identifier(target)?;
            let idx = fields
                .iter()
                .position(|f| {
                    normalize_identifier(&f.name).ok().as_deref() == Some(target_norm.as_str())
                })
                .ok_or_else(|| format!("AFTER target '{target}' not found in same parent"))?;
            fields.insert(idx + 1, new_field);
            Ok(())
        }
        AddPosition::Before(target) => {
            let target_norm = normalize_identifier(target)?;
            let idx = fields
                .iter()
                .position(|f| {
                    normalize_identifier(&f.name).ok().as_deref() == Some(target_norm.as_str())
                })
                .ok_or_else(|| format!("BEFORE target '{target}' not found in same parent"))?;
            fields.insert(idx, new_field);
            Ok(())
        }
    }
}

/// Rebuild `schema` with the column at `path` moved to `position` within its parent.
///
/// Only fields within a STRUCT scope can be reordered.  `AddPosition::After`/`Before` look up
/// the target name in the same parent's child list (with the moved field already removed),
/// so attempting to reference a name in a different parent produces an error.
#[allow(dead_code)]
pub(crate) fn apply_reorder_at(
    schema: &Schema,
    path: &ColumnPath,
    position: &AddPosition,
) -> Result<Schema, String> {
    let identifier_field_ids: Vec<i32> = schema.identifier_field_ids().collect();
    let new_fields = reorder_in_fields(
        schema.as_struct().fields().iter().cloned().collect(),
        path.segments(),
        position,
    )?;
    let arc_fields: Vec<NestedFieldRef> = new_fields.into_iter().map(Arc::new).collect();
    Schema::builder()
        .with_fields(arc_fields)
        .with_identifier_field_ids(identifier_field_ids)
        .build()
        .map_err(|e| format!("rebuild schema after reorder: {e}"))
}

fn reorder_in_fields(
    fields: Vec<Arc<NestedField>>,
    segments: &[String],
    position: &AddPosition,
) -> Result<Vec<NestedField>, String> {
    if segments.is_empty() {
        return Err("reorder path is empty".to_string());
    }
    if segments.len() == 1 {
        let head = normalize_identifier(&segments[0])?;
        let mut existing: Vec<NestedField> = fields.iter().map(|f| (**f).clone()).collect();
        let idx = existing
            .iter()
            .position(|f| normalize_identifier(&f.name).ok().as_deref() == Some(head.as_str()))
            .ok_or_else(|| format!("column '{}' not found for reorder", head))?;
        let target = existing.remove(idx);
        insert_at_position(&mut existing, target, position)?;
        return Ok(existing);
    }
    let head = normalize_identifier(&segments[0])?;
    let mut out = Vec::new();
    let mut matched = false;
    for f in fields {
        let f_norm = normalize_identifier(&f.name).ok();
        if f_norm.as_deref() == Some(head.as_str()) {
            matched = true;
            let new_inner = reorder_in_type(&f.field_type, &segments[1..], position)?;
            let mut updated = (*f).clone();
            updated.field_type = Box::new(new_inner);
            out.push(updated);
        } else {
            out.push((*f).clone());
        }
    }
    if !matched {
        return Err(format!("column '{}' not found for reorder", head));
    }
    Ok(out)
}

fn reorder_in_type(ty: &Type, segments: &[String], position: &AddPosition) -> Result<Type, String> {
    match ty {
        Type::Struct(s) => {
            let new = reorder_in_fields(s.fields().iter().cloned().collect(), segments, position)?;
            Ok(Type::Struct(StructType::new(
                new.into_iter().map(Arc::new).collect(),
            )))
        }
        _ => Err("reorder path descends into non-struct type".to_string()),
    }
}

fn widen_type(current: &Type, new_type: &SqlType) -> Result<Type, String> {
    match (current, new_type) {
        (Type::Primitive(PrimitiveType::Int), SqlType::BigInt) => {
            Ok(Type::Primitive(PrimitiveType::Long))
        }
        (Type::Primitive(PrimitiveType::Float), SqlType::Double) => {
            Ok(Type::Primitive(PrimitiveType::Double))
        }
        (
            Type::Primitive(PrimitiveType::Decimal {
                precision: cp,
                scale: cs,
            }),
            SqlType::Decimal {
                precision: np,
                scale: ns,
            },
        ) => {
            // Iceberg spec: decimal precision can only increase, scale must remain unchanged.
            if (*cs as i64) != (*ns as i64) {
                return Err(format!(
                    "decimal scale change is not allowed (current decimal({cp},{cs}), new decimal({np},{ns}))"
                ));
            }
            if (*np as u32) <= *cp {
                return Err(format!(
                    "decimal precision must strictly increase (current decimal({cp},{cs}), new decimal({np},{ns}))"
                ));
            }
            Ok(Type::Primitive(PrimitiveType::Decimal {
                precision: *np as u32,
                scale: *ns as u32,
            }))
        }
        (Type::Primitive(PrimitiveType::Date), SqlType::DateTime) => {
            Ok(Type::Primitive(PrimitiveType::Timestamp))
        }
        _ => Err(format!(
            "unsupported Iceberg type evolution: {current:?} -> {new_type:?}"
        )),
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SchemaPropertyUpdates {
    sets: HashMap<String, String>,
    removals: Vec<String>,
}

impl SchemaPropertyUpdates {
    #[cfg(test)]
    fn is_empty(&self) -> bool {
        self.sets.is_empty() && self.removals.is_empty()
    }

    fn push_removal(&mut self, key: String) {
        if !self.removals.contains(&key) {
            self.removals.push(key);
        }
    }

    fn into_table_updates(self) -> Vec<TableUpdate> {
        let mut updates = Vec::new();
        if !self.sets.is_empty() {
            updates.push(TableUpdate::SetProperties { updates: self.sets });
        }
        if !self.removals.is_empty() {
            updates.push(TableUpdate::RemoveProperties {
                removals: self.removals,
            });
        }
        updates
    }
}

#[cfg(test)]
fn build_property_updates_for_test(
    properties: &HashMap<String, String>,
    change: &IcebergSchemaChange,
) -> Result<SchemaPropertyUpdates, String> {
    build_property_updates(properties, change)
}

fn build_property_updates(
    properties: &HashMap<String, String>,
    change: &IcebergSchemaChange,
) -> Result<SchemaPropertyUpdates, String> {
    let mut updates = SchemaPropertyUpdates::default();
    match change {
        IcebergSchemaChange::AddColumn {
            name, data_type, ..
        } => {
            if let Some(value) = logical_type_property_value(data_type) {
                updates.sets.insert(logical_type_property_key(name)?, value);
            }
        }
        IcebergSchemaChange::DropColumn { path } => {
            // Phase B note: when nested DROP COLUMN is supported, this site must
            // be extended to handle non-top-level property keys explicitly.
            // For now, only top-level columns carry novarocks properties.
            if path.segments().len() != 1 {
                return Ok(Default::default());
            }
            let name = path.last().unwrap();
            reject_key_column_drop(properties, name)?;
            let logical_key = logical_type_property_key(name)?;
            if properties.contains_key(&logical_key) {
                updates.push_removal(logical_key);
            }
            let aggregation_key = column_aggregation_property_key(name)?;
            if properties.contains_key(&aggregation_key) {
                updates.push_removal(aggregation_key);
            }
        }
        IcebergSchemaChange::RenameColumn { path, new_name } => {
            // Phase B note: when nested RENAME COLUMN is supported, this site must
            // be extended to handle non-top-level property keys explicitly.
            if path.segments().len() != 1 {
                return Ok(Default::default());
            }
            let old_name = path.last().unwrap();
            let old_logical_key = logical_type_property_key(old_name)?;
            if let Some(value) = properties.get(&old_logical_key) {
                updates
                    .sets
                    .insert(logical_type_property_key(new_name)?, value.clone());
                updates.push_removal(old_logical_key);
            }

            let old_aggregation_key = column_aggregation_property_key(old_name)?;
            if let Some(value) = properties.get(&old_aggregation_key) {
                updates
                    .sets
                    .insert(column_aggregation_property_key(new_name)?, value.clone());
                updates.push_removal(old_aggregation_key);
            }

            if let Some(key_columns) = rename_key_columns(properties, old_name, new_name)? {
                updates
                    .sets
                    .insert(TABLE_KEY_COLUMNS_PROPERTY.to_string(), key_columns);
            }
        }
        IcebergSchemaChange::ModifyColumn { path, new_type } => {
            // Phase B note: when nested MODIFY COLUMN is supported, this site must
            // be extended to handle non-top-level property keys explicitly.
            if path.segments().len() != 1 {
                return Ok(Default::default());
            }
            let name = path.last().unwrap();
            let logical_key = logical_type_property_key(name)?;
            if let Some(value) = logical_type_property_value(new_type) {
                updates.sets.insert(logical_key, value);
            } else if properties.contains_key(&logical_key) {
                updates.push_removal(logical_key);
            }
        }
        IcebergSchemaChange::SetNullable { path, nullable } => {
            let key = nullability_attestation_property_key(path);
            if !*nullable {
                // SET NOT NULL: leave a metadata trail (not an existence proof).
                // The schema-id update and this property write commit together.
                let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ").to_string();
                updates.sets.insert(key, now);
            } else if properties.contains_key(&key) {
                updates.push_removal(key);
            }
        }
        IcebergSchemaChange::Reorder { .. } => {}
    }
    Ok(updates)
}

fn nullability_attestation_property_key(path: &ColumnPath) -> String {
    format!("novarocks.nullability.attested.{}", path.dotted())
}

fn reject_key_column_drop(properties: &HashMap<String, String>, name: &str) -> Result<(), String> {
    let normalized = normalize_identifier(name)?;
    let Some(key_columns) = properties.get(TABLE_KEY_COLUMNS_PROPERTY) else {
        return Ok(());
    };
    for column in normalized_key_columns(key_columns)? {
        if column == normalized {
            return Err(format!(
                "Iceberg schema evolution cannot drop key column `{name}`"
            ));
        }
    }
    Ok(())
}

fn rename_key_columns(
    properties: &HashMap<String, String>,
    old_name: &str,
    new_name: &str,
) -> Result<Option<String>, String> {
    let Some(key_columns) = properties.get(TABLE_KEY_COLUMNS_PROPERTY) else {
        return Ok(None);
    };
    let old_normalized = normalize_identifier(old_name)?;
    let new_normalized = normalize_identifier(new_name)?;
    let mut renamed = false;
    let columns = normalized_key_columns(key_columns)?
        .into_iter()
        .map(|column| {
            if column == old_normalized {
                renamed = true;
                new_normalized.clone()
            } else {
                column
            }
        })
        .collect::<Vec<_>>();

    Ok(renamed.then(|| columns.join(",")))
}

fn normalized_key_columns(value: &str) -> Result<Vec<String>, String> {
    value
        .split(',')
        .filter(|column| !column.trim().is_empty())
        .map(|column| normalize_identifier(column.trim()))
        .collect()
}

struct SchemaUpdateTxnAction {
    change: IcebergSchemaChange,
}

#[async_trait]
impl TransactionAction for SchemaUpdateTxnAction {
    async fn commit(
        self: Arc<Self>,
        table: &iceberg::table::Table,
    ) -> iceberg::Result<ActionCommit> {
        let metadata = table.metadata();
        let current_schema = metadata.current_schema();
        let new_schema =
            build_updated_schema(current_schema, metadata.last_column_id(), &self.change)
                .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;
        let property_updates = build_property_updates(metadata.properties(), &self.change)
            .map_err(|e| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e))?;
        // Iceberg REST `add-schema` carries `last-column-id` so the server
        // can keep the monotonically-increasing high-watermark even when the
        // new schema dropped the previously-highest field. Compute as
        // max(table.last_column_id, schema.highest_field_id) — this matches
        // table_metadata_builder.rs::add_schema.
        let next_last_column_id =
            std::cmp::max(metadata.last_column_id(), new_schema.highest_field_id());
        let mut updates = vec![
            TableUpdate::AddSchema {
                schema: new_schema,
                last_column_id: Some(next_last_column_id),
            },
            TableUpdate::SetCurrentSchema { schema_id: -1 },
        ];
        updates.extend(property_updates.into_table_updates());

        Ok(ActionCommit::new(
            updates,
            vec![
                TableRequirement::CurrentSchemaIdMatch {
                    current_schema_id: metadata.current_schema_id(),
                },
                TableRequirement::LastAssignedFieldIdMatch {
                    last_assigned_field_id: metadata.last_column_id(),
                },
            ],
        ))
    }
}

pub(crate) fn alter_table_schema(
    state: &Arc<StandaloneState>,
    stmt: &AlterIcebergSchemaStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), String> {
    let target =
        resolve_existing_table_target(state, &stmt.table, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(
            "Iceberg schema evolution only supports standalone iceberg catalogs".to_string(),
        );
    }

    protect_schema_change(state, &target, &stmt.change)?;

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let loaded = crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &target.namespace,
        &target.table,
    )?;

    let format_version = loaded.table.metadata().format_version();
    if let IcebergSchemaChange::AddColumn {
        default: Some(literal),
        data_type,
        ..
    } = &stmt.change
    {
        let iceberg_lit = crate::connector::iceberg::default_value::default_literal_to_iceberg(
            literal, data_type,
        )?;
        crate::connector::iceberg::default_value::require_v3_for_default(
            format_version,
            &iceberg_lit,
        )?;
    }

    let change_for_retry = stmt.change.clone();
    let entry_for_retry = entry.clone();
    let namespace_for_retry = target.namespace.clone();
    let table_for_retry = target.table.clone();
    let commit_result =
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
            commit_with_retry(|_attempt| {
                let entry_inner = entry_for_retry.clone();
                let namespace_inner = namespace_for_retry.clone();
                let table_inner = table_for_retry.clone();
                let change_inner = change_for_retry.clone();
                async move {
                    // Each retry must start with a fresh metadata read; otherwise load_table()
                    // would serve the stale cached state that just produced the conflict.
                    entry_inner.invalidate_table_cache(&namespace_inner, &table_inner);
                    // Catalog handles are not Clone (Hadoop holds a tokio::sync::Mutex,
                    // REST holds an HTTP client builder); rebuild per attempt rather
                    // than share a stale instance.
                    let catalog =
                        crate::connector::iceberg::catalog::registry::build_iceberg_catalog(
                            &entry_inner,
                        )
                        .map_err(|e| {
                            iceberg::Error::new(
                                iceberg::ErrorKind::Unexpected,
                                format!("build catalog for retry: {e}"),
                            )
                        })?;
                    let loaded_inner = crate::connector::iceberg::catalog::registry::load_table(
                        &entry_inner,
                        &namespace_inner,
                        &table_inner,
                    )
                    .map_err(|e| {
                        iceberg::Error::new(
                            iceberg::ErrorKind::Unexpected,
                            format!("reload table for retry: {e}"),
                        )
                    })?;
                    let tx = Transaction::new(&loaded_inner.table);
                    let tx = SchemaUpdateTxnAction {
                        change: change_inner,
                    }
                    .apply(tx)
                    .map_err(|e| {
                        iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                    })?;
                    tx.commit(catalog.as_ref()).await.map(|_committed| ())
                }
            })
            .await
        })
        .map_err(|e| format!("alter iceberg schema runtime failed: {e}"))?;

    entry.invalidate_table_cache(&target.namespace, &target.table);
    commit_result?;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;
    Ok(())
}

fn protect_schema_change(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
    change: &IcebergSchemaChange,
) -> Result<(), String> {
    reject_reserved_change(change)?;
    let IcebergSchemaChange::DropColumn { path } = change else {
        return Ok(());
    };

    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };
    entry.invalidate_table_cache(&target.namespace, &target.table);
    let loaded = crate::connector::iceberg::catalog::registry::load_table(
        &entry,
        &target.namespace,
        &target.table,
    )?;
    let metadata = loaded.table.metadata();
    build_updated_schema(metadata.current_schema(), metadata.last_column_id(), change)?;
    build_property_updates(metadata.properties(), change)?;

    let equality_delete_columns =
        crate::connector::iceberg::catalog::registry::current_equality_delete_column_names(
            &loaded.table,
        )?;

    let mv_dependencies = managed_mv_dependencies_for_target(state, target)?;
    reject_drop_dependencies(&path.dotted(), &equality_delete_columns, &mv_dependencies)
}

fn managed_mv_dependencies_for_target(
    state: &Arc<StandaloneState>,
    target: &crate::engine::backend_resolver::TargetBackend,
) -> Result<Vec<ManagedMvDependency>, String> {
    let Some(provider) = state.metadata_provider.as_ref() else {
        return Ok(Vec::new());
    };
    let read = provider
        .begin_read()
        .map_err(|e| format!("open metadata read transaction failed: {e}"))?;
    let definitions = state
        .mv_repo
        .list_definitions(read.as_ref())
        .map_err(|e| format!("load materialized view metadata failed: {e}"))?;
    let target_key = format!("{}.{}.{}", target.catalog, target.namespace, target.table);
    let target_key_lower = target_key.to_ascii_lowercase();
    let target = ManagedMvTarget::from_backend(target)?;
    Ok(definitions
        .into_iter()
        .filter(|mv| {
            mv.base_table_refs
                .iter()
                .any(|base| base.eq_ignore_ascii_case(&target_key))
                || mv
                    .select_sql
                    .to_ascii_lowercase()
                    .contains(&target_key_lower)
        })
        .map(|mv| ManagedMvDependency {
            select_sql: mv.select_sql,
            target: target.clone(),
        })
        .collect())
}

/// Whether a property key is reserved (cannot be set/unset by SET TBLPROPERTIES).
/// Returns `None` if the key is user-modifiable, or `Some(reason)` containing a
/// human-readable category to include in the error message.
fn is_reserved_property_key(key: &str) -> Option<&'static str> {
    if key == "format-version" {
        return Some(
            "format-version is reserved; use UPGRADE TABLE syntax (not yet implemented in NovaRocks)",
        );
    }
    if matches!(
        key,
        "identifier-field-ids"
            | "current-schema-id"
            | "default-spec-id"
            | "default-sort-order-id"
            | "last-column-id"
            | "last-partition-id"
            | "last-sequence-number"
    ) {
        return Some("Iceberg internal metadata key, not user-settable");
    }
    if key.starts_with("novarocks.") {
        return Some("novarocks.* namespace is reserved for engine-managed properties");
    }
    None
}

/// Collect any property keys in `op` that are blocked by the denylist.
/// Returns a list of `(key, reason)` pairs for each denied key.
fn collect_property_denylist_hits(op: &PropertiesOp) -> Vec<(String, &'static str)> {
    let mut hits = Vec::new();
    match op {
        PropertiesOp::Set { entries } => {
            for (k, _) in entries {
                if let Some(reason) = is_reserved_property_key(k) {
                    hits.push((k.clone(), reason));
                }
            }
        }
        PropertiesOp::Unset { keys, .. } => {
            for k in keys {
                if let Some(reason) = is_reserved_property_key(k) {
                    hits.push((k.clone(), reason));
                }
            }
        }
    }
    hits
}

/// For a strict (non-IF-EXISTS) UNSET, verify every requested key is present in
/// the current table properties. Returns an error naming the first missing key.
fn validate_unset_keys_present(
    op: &PropertiesOp,
    existing: &std::collections::HashMap<String, String>,
) -> Result<(), String> {
    if let PropertiesOp::Unset { keys, if_exists } = op {
        if !*if_exists {
            for k in keys {
                if !existing.contains_key(k) {
                    return Err(format!(
                        "UNSET TBLPROPERTIES key '{k}' does not exist; use IF EXISTS to silently skip"
                    ));
                }
            }
        }
    }
    Ok(())
}

/// Compute the final list of keys to remove for an UNSET operation.
/// For IF EXISTS, filters out keys that are not present in `existing`.
/// For strict UNSET, returns all keys as-is (caller must have validated them).
/// Returns an empty list for SET operations.
fn compute_remove_keys(
    op: &PropertiesOp,
    existing: &std::collections::HashMap<String, String>,
) -> Vec<String> {
    if let PropertiesOp::Unset { keys, if_exists } = op {
        if *if_exists {
            return keys
                .iter()
                .filter(|k| existing.contains_key(*k))
                .cloned()
                .collect();
        }
        return keys.clone();
    }
    Vec::new()
}

/// Execute SET TBLPROPERTIES or UNSET TBLPROPERTIES on an Iceberg table.
///
/// Mirrors `alter_table_schema`: resolves the catalog entry, invalidates the
/// table cache, then calls `commit_with_retry` with a closure that re-invalidates,
/// re-loads, and re-builds the action on each attempt to avoid stale-cache conflicts.
pub(crate) fn alter_table_properties(
    state: &Arc<StandaloneState>,
    stmt: &AlterIcebergPropertiesStmt,
    current_catalog: Option<&str>,
    current_database: &str,
) -> Result<(), String> {
    // 1. Resolve target — same helper as alter_table_schema.
    let target =
        resolve_existing_table_target(state, &stmt.table, current_catalog, current_database)?;
    if target.backend_name != "iceberg" {
        return Err(
            "ALTER TABLE TBLPROPERTIES only supports standalone iceberg catalogs".to_string(),
        );
    }

    // 2. Denylist check — fail fast before any IO.
    let denied = collect_property_denylist_hits(&stmt.op);
    if !denied.is_empty() {
        let mut msgs: Vec<String> = denied
            .iter()
            .map(|(k, reason)| format!("`{k}`: {reason}"))
            .collect();
        msgs.sort();
        return Err(format!(
            "ALTER TABLE TBLPROPERTIES rejected reserved key(s): {}",
            msgs.join("; ")
        ));
    }

    // 3. Acquire catalog entry — same pattern as alter_table_schema.
    let entry = {
        let registry = state
            .iceberg_catalogs
            .read()
            .map_err(|e| format!("iceberg catalog registry read lock: {e}"))?;
        registry.get(&target.catalog)?
    };

    // 4. Pre-commit cache invalidate.
    entry.invalidate_table_cache(&target.namespace, &target.table);

    let entry_for_retry = entry.clone();
    let namespace_for_retry = target.namespace.clone();
    let table_for_retry = target.table.clone();
    let op_for_retry = stmt.op.clone();

    let commit_result =
        crate::connector::iceberg::catalog::registry::block_on_iceberg(async move {
            commit_with_retry(|_attempt| {
                let entry_inner = entry_for_retry.clone();
                let namespace_inner = namespace_for_retry.clone();
                let table_inner = table_for_retry.clone();
                let op_inner = op_for_retry.clone();
                async move {
                    // Each retry must start with a fresh metadata read to avoid stale-cache
                    // conflicts (mirrors the same pattern in alter_table_schema).
                    entry_inner.invalidate_table_cache(&namespace_inner, &table_inner);
                    // Catalog handles are not Clone; rebuild per attempt.
                    let catalog =
                        crate::connector::iceberg::catalog::registry::build_iceberg_catalog(
                            &entry_inner,
                        )
                        .map_err(|e| {
                            iceberg::Error::new(
                                iceberg::ErrorKind::Unexpected,
                                format!("build catalog for retry: {e}"),
                            )
                        })?;
                    let loaded_inner = crate::connector::iceberg::catalog::registry::load_table(
                        &entry_inner,
                        &namespace_inner,
                        &table_inner,
                    )
                    .map_err(|e| {
                        iceberg::Error::new(
                            iceberg::ErrorKind::Unexpected,
                            format!("reload table for retry: {e}"),
                        )
                    })?;

                    // Strict UNSET: validate every requested key against the LATEST metadata.
                    let existing = loaded_inner.table.metadata().properties().clone();
                    validate_unset_keys_present(&op_inner, &existing)
                        .map_err(|msg| iceberg::Error::new(iceberg::ErrorKind::DataInvalid, msg))?;

                    // Early return for the IF EXISTS no-op case: all requested keys are
                    // already absent from the latest metadata. Avoids an empty metadata
                    // rewrite that would otherwise bump the version hint.
                    if let PropertiesOp::Unset {
                        if_exists: true, ..
                    } = &op_inner
                    {
                        let removes = compute_remove_keys(&op_inner, &existing);
                        if removes.is_empty() {
                            return Ok(());
                        }
                    }

                    let tx = Transaction::new(&loaded_inner.table);
                    let mut action = tx.update_table_properties();
                    match &op_inner {
                        PropertiesOp::Set { entries } => {
                            for (k, v) in entries {
                                action = action.set(k.clone(), v.clone());
                            }
                        }
                        PropertiesOp::Unset { .. } => {
                            for k in compute_remove_keys(&op_inner, &existing) {
                                action = action.remove(k);
                            }
                        }
                    }
                    let tx = action.apply(tx).map_err(|e| {
                        iceberg::Error::new(iceberg::ErrorKind::DataInvalid, e.to_string())
                    })?;
                    tx.commit(catalog.as_ref()).await.map(|_| ())
                }
            })
            .await
        })
        .map_err(|e| format!("alter table properties runtime failed: {e}"))?;

    // 5. Post-commit cache invalidate (mirror alter_table_schema).
    entry.invalidate_table_cache(&target.namespace, &target.table);
    commit_result?;

    crate::engine::iceberg_writer::invalidate_iceberg_caches(state, &target)?;
    Ok(())
}
