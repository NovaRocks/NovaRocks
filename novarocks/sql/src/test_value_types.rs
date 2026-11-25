// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

//! Explicit semantic declarations for SQL type correspondence fixtures.

use arrow::datatypes::{DataType, Field};
use novarocks_type_contract::{
    FunctionValueType, ValueField, ValueKind, ValueLayout, ValueLogicalType, ValueTypeRef,
    ValueTypeStorageLifetime,
};

pub(crate) fn string_structure(fields: &[(&str, bool, bool)], nullable: bool) -> FunctionValueType {
    let layout = DataType::Struct(
        fields
            .iter()
            .map(|(name, nullable, json)| {
                let carrier = if *json {
                    DataType::LargeBinary
                } else {
                    DataType::Utf8
                };
                Field::new(*name, carrier, *nullable)
            })
            .collect::<Vec<_>>()
            .into(),
    );
    string_structure_from_source(fields, nullable, &layout)
}
pub(crate) fn string_structure_from_source(
    fields: &[(&str, bool, bool)],
    nullable: bool,
    source_layout: &DataType,
) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let semantic = fields
        .iter()
        .map(|(name, nullable, json)| {
            let kind = if *json {
                ValueKind::Nominal(ValueLogicalType::Json)
            } else {
                ValueKind::String
            };
            ValueField::new(
                *name,
                ValueTypeRef::try_new(kind, *nullable, storage.clone()).unwrap(),
                storage.clone(),
            )
        })
        .collect::<Vec<_>>();
    FunctionValueType::try_from_value(
        ValueTypeRef::try_new(
            ValueKind::Struct(semantic.into()),
            nullable,
            storage.clone(),
        )
        .unwrap(),
        ValueLayout::try_new(source_layout, storage).unwrap(),
    )
    .unwrap()
}

/// A test author declares the item domain independently of Arrow attributes.
pub(crate) fn string_list(item_nullable: bool, json: bool, nullable: bool) -> FunctionValueType {
    let storage = ValueTypeStorageLifetime::pure();
    let item = ValueTypeRef::try_new(
        if json {
            ValueKind::Nominal(ValueLogicalType::Json)
        } else {
            ValueKind::String
        },
        item_nullable,
        storage.clone(),
    )
    .unwrap();
    let value = ValueTypeRef::try_new(ValueKind::List(item), nullable, storage.clone()).unwrap();
    let carrier = if json {
        DataType::LargeBinary
    } else {
        DataType::Utf8
    };
    let layout = ValueLayout::try_new(
        &DataType::List(std::sync::Arc::new(Field::new(
            "item",
            carrier,
            item_nullable,
        ))),
        storage,
    )
    .unwrap();
    FunctionValueType::try_from_value(value, layout).unwrap()
}
