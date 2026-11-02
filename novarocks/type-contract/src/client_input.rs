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

//! A render-purpose proof over an independently retained immutable input owner.

use std::sync::Arc;

use arrow_schema::{DataType as D, TimeUnit};
use novarocks_result_contract::{
    FrozenRootOutput, NativeRenderType as N, OpaqueRenderType as O, RenderField,
    RenderTimeUnit as U, RootContractError, RootOutputContract, RootProfileV1,
};

use crate::{TypedValueBinding, ValueKind, ValueLogicalType as V, ValueTypeRef};

/// An immutable typed occurrence layout. Implementations retain only their
/// own backing and its creator responsibility, never an entire input/query.
pub trait ClientInputTypeBindings: Send + Sync {
    fn binding_at(&self, ordinal: usize) -> Option<&TypedValueBinding>;
    fn slot_at(&self, ordinal: usize) -> Option<u32>;
    /// This exact schema was already checked together with these bindings by
    /// the typed input constructor. Comparing identity reuses that proof.
    fn matches_schema(&self, schema: &Arc<arrow_schema::Schema>) -> bool;
}

/// The original complete types have been checked against this exact purpose.
/// Cloning shares existing owners and makes no allocation or capacity request.
#[derive(Clone)]
pub struct ValidatedClientInputTypes {
    contract: Arc<RootOutputContract>,
    input: Arc<dyn ClientInputTypeBindings>,
}

impl ValidatedClientInputTypes {
    pub fn try_new(
        contract: Arc<RootOutputContract>,
        input: Arc<dyn ClientInputTypeBindings>,
    ) -> Result<Self, RootContractError> {
        let FrozenRootOutput::ClientRows(schema) = contract.output() else {
            return Err(RootContractError::InvalidSchema);
        };
        let mut nodes = RootProfileV1::SCHEMA_TYPE_NODES;
        for column in schema.columns() {
            let ordinal = column.source_ordinal as usize;
            let binding = input
                .binding_at(ordinal)
                .ok_or(RootContractError::InvalidSchema)?;
            if column.source_slot != input.slot_at(ordinal)
                || !matches_field(&column.field, binding.value_type(), 1, &mut nodes)
            {
                return Err(RootContractError::InvalidSchema);
            }
        }
        Ok(Self { contract, input })
    }

    pub fn contract(&self) -> &Arc<RootOutputContract> {
        &self.contract
    }

    pub fn binding_at(&self, ordinal: usize) -> Option<&TypedValueBinding> {
        self.input.binding_at(ordinal)
    }

    pub fn matches_schema(&self, schema: &Arc<arrow_schema::Schema>) -> bool {
        self.input.matches_schema(schema)
    }

    /// A batch may have new private encoding scaffolds while sharing all
    /// previously validated semantic/layout nodes. Check only these retained
    /// handles and exact occurrences; fresh nodes require ordinary validation.
    pub fn rebind_shared(&self, input: Arc<dyn ClientInputTypeBindings>) -> Option<Self> {
        let FrozenRootOutput::ClientRows(schema) = self.contract.output() else {
            return None;
        };
        for column in schema.columns() {
            let ordinal = column.source_ordinal as usize;
            let old = self.input.binding_at(ordinal)?;
            let new = input.binding_at(ordinal)?;
            if column.source_slot != input.slot_at(ordinal)
                || !old.value_type().shares_storage(new.value_type())
                || !old.layout().shares_storage(new.layout())
            {
                return None;
            }
        }
        Some(Self {
            contract: self.contract.clone(),
            input,
        })
    }

    /// Pointer equality only reuses a proof already established for this
    /// immutable owner. Different owners always require their own validation.
    pub fn is_for(
        &self,
        contract: &Arc<RootOutputContract>,
        input: &Arc<dyn ClientInputTypeBindings>,
    ) -> bool {
        Arc::ptr_eq(&self.contract, contract) && Arc::ptr_eq(&self.input, input)
    }
}

fn unit(unit: TimeUnit) -> U {
    match unit {
        TimeUnit::Second => U::Second,
        TimeUnit::Millisecond => U::Millisecond,
        TimeUnit::Microsecond => U::Microsecond,
        TimeUnit::Nanosecond => U::Nanosecond,
    }
}

fn matches_field(
    field: &RenderField,
    value: &ValueTypeRef,
    depth: usize,
    nodes: &mut usize,
) -> bool {
    if depth > RootProfileV1::MAX_DEPTH || *nodes == 0 || field.nullable != value.nullable() {
        return false;
    }
    *nodes -= 1;
    match (&field.native_type, value.kind()) {
        (N::String, ValueKind::String) | (N::Binary, ValueKind::Binary) => true,
        (N::Json, ValueKind::Nominal(V::Json))
        | (N::Variant, ValueKind::Nominal(V::Variant))
        | (N::LargeInt, ValueKind::Nominal(V::LargeInt))
        | (N::Opaque(O::Hll), ValueKind::Nominal(V::Hll))
        | (N::Opaque(O::Bitmap), ValueKind::Nominal(V::Bitmap))
        | (N::Opaque(O::Object), ValueKind::Nominal(V::Object))
        | (N::Opaque(O::Percentile), ValueKind::Nominal(V::Percentile)) => true,
        (N::Null, ValueKind::Scalar(D::Null))
        | (N::Boolean, ValueKind::Scalar(D::Boolean))
        | (N::Float32, ValueKind::Scalar(D::Float32))
        | (N::Float64, ValueKind::Scalar(D::Float64))
        | (N::Date, ValueKind::Scalar(D::Date32)) => true,
        (N::SignedInteger(bits), ValueKind::Scalar(data_type)) => match data_type {
            D::Int8 => *bits == 8,
            D::Int16 => *bits == 16,
            D::Int32 => *bits == 32,
            D::Int64 => *bits == 64,
            _ => false,
        },
        (N::UnsignedInteger(bits), ValueKind::Scalar(data_type)) => match data_type {
            D::UInt8 => *bits == 8,
            D::UInt16 => *bits == 16,
            D::UInt32 => *bits == 32,
            D::UInt64 => *bits == 64,
            _ => false,
        },
        (
            N::Decimal {
                bits,
                precision,
                scale,
            },
            ValueKind::Scalar(data_type),
        ) => match data_type {
            D::Decimal128(p, s) => *bits == 128 && precision == p && scale == s,
            D::Decimal256(p, s) => *bits == 256 && precision == p && scale == s,
            _ => false,
        },
        (N::Time { unit: expected }, ValueKind::Scalar(D::Time32(actual) | D::Time64(actual))) => {
            *expected == unit(*actual)
        }
        (
            N::Timestamp {
                unit: expected,
                timezone,
            },
            ValueKind::Scalar(D::Timestamp(actual, zone)),
        ) => *expected == unit(*actual) && timezone.as_deref() == zone.as_deref(),
        (N::List(child), ValueKind::List(actual)) => matches_field(child, actual, depth + 1, nodes),
        (
            N::Map { key, value },
            ValueKind::Map {
                key: actual_key,
                value: actual_value,
            },
        ) => {
            matches_field(key, actual_key, depth + 1, nodes)
                && matches_field(value, actual_value, depth + 1, nodes)
        }
        (N::Struct(fields), ValueKind::Struct(actual)) if fields.len() == actual.len() => {
            fields.iter().zip(actual).all(|(field, actual)| {
                field.name == actual.name()
                    && matches_field(&field.field, actual.value_type(), depth + 1, nodes)
            })
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ValueField, ValueLayout, ValueTypeStorageHold, ValueTypeStorageLifetime};
    use novarocks_result_contract::{
        ClientRenderSchema, NamedRenderField, RenderColumn, RenderPresentation, RootProfileId,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Input {
        schema: Arc<arrow_schema::Schema>,
        binding: TypedValueBinding,
        slot: u32,
    }
    impl ClientInputTypeBindings for Input {
        fn binding_at(&self, ordinal: usize) -> Option<&TypedValueBinding> {
            (ordinal == 0).then_some(&self.binding)
        }
        fn slot_at(&self, ordinal: usize) -> Option<u32> {
            (ordinal == 0).then_some(self.slot)
        }
        fn matches_schema(&self, schema: &Arc<arrow_schema::Schema>) -> bool {
            Arc::ptr_eq(&self.schema, schema)
        }
    }
    fn owner(binding: TypedValueBinding) -> Arc<dyn ClientInputTypeBindings> {
        Arc::new(Input {
            schema: Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "input",
                binding.data_type().clone(),
                binding.nullable(),
            )])),
            binding,
            slot: 17,
        })
    }
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn field(native_type: N, nullable: bool) -> RenderField {
        RenderField {
            presentation: match native_type {
                N::Json => RenderPresentation::JsonText,
                N::Struct(_) => RenderPresentation::MysqlContainer,
                _ => RenderPresentation::ScalarText,
            },
            nullable,
            native_type,
        }
    }
    fn contract(field: RenderField) -> Arc<RootOutputContract> {
        Arc::new(RootOutputContract::new(
            RootProfileId::V1,
            FrozenRootOutput::ClientRows(
                ClientRenderSchema::try_new(
                    novarocks_result_contract::ClientPresentationFamily::MysqlText,
                    vec![RenderColumn {
                        source_ordinal: 0,
                        source_slot: Some(17),
                        name: "output".into(),
                        field,
                    }],
                    1,
                )
                .unwrap(),
            ),
        ))
    }
    fn input(kind: ValueKind, data_type: D, nullable: bool) -> Arc<dyn ClientInputTypeBindings> {
        let storage = ValueTypeStorageLifetime::pure();
        owner(
            TypedValueBinding::try_new(
                ValueTypeRef::try_new(kind, nullable, storage.clone()).unwrap(),
                ValueLayout::try_new(&data_type, storage).unwrap(),
            )
            .unwrap(),
        )
    }

    #[test]
    fn same_carrier_never_proves_a_nominal_render_domain() {
        let json = contract(field(N::Json, true));
        assert!(
            ValidatedClientInputTypes::try_new(
                json.clone(),
                input(ValueKind::Nominal(V::Json), D::Utf8, true)
            )
            .is_ok()
        );
        assert!(
            ValidatedClientInputTypes::try_new(json, input(ValueKind::String, D::Utf8, true))
                .is_err()
        );
        let integer = contract(field(N::LargeInt, false));
        assert!(
            ValidatedClientInputTypes::try_new(
                integer.clone(),
                input(
                    ValueKind::Nominal(V::LargeInt),
                    D::FixedSizeBinary(16),
                    false
                )
            )
            .is_ok()
        );
        assert!(
            ValidatedClientInputTypes::try_new(
                integer,
                input(ValueKind::Nominal(V::Uuid), D::FixedSizeBinary(16), false)
            )
            .is_err()
        );
    }

    #[test]
    fn recursive_domain_name_and_null_laws_remain_exact() {
        let expected = contract(field(
            N::Struct(vec![NamedRenderField {
                name: "payload".into(),
                field: field(N::Json, false),
            }]),
            true,
        ));
        for (name, kind, nullable, valid) in [
            ("payload", ValueKind::Nominal(V::Json), false, true),
            ("payload", ValueKind::String, false, false),
            ("renamed", ValueKind::Nominal(V::Json), false, false),
            ("payload", ValueKind::Nominal(V::Json), true, false),
        ] {
            let storage = ValueTypeStorageLifetime::pure();
            let child = ValueTypeRef::try_new(kind, nullable, storage.clone()).unwrap();
            let kind = ValueKind::Struct(vec![ValueField::new(name, child, storage)].into());
            let layout =
                D::Struct(vec![Arc::new(arrow_schema::Field::new(name, D::Utf8, nullable))].into());
            assert_eq!(
                ValidatedClientInputTypes::try_new(expected.clone(), input(kind, layout, true))
                    .is_ok(),
                valid
            );
        }
    }

    #[test]
    fn proof_shares_only_its_exact_owner_and_retains_the_last_type_reference() {
        let released = Arc::new(AtomicUsize::new(0));
        let storage = ValueTypeStorageLifetime::held(Arc::new(Hold(released.clone())));
        let binding = TypedValueBinding::try_new(
            ValueTypeRef::try_new(ValueKind::Nominal(V::Json), true, storage.clone()).unwrap(),
            ValueLayout::try_new(&D::Utf8, storage.clone()).unwrap(),
        )
        .unwrap();
        let source = owner(binding.clone());
        let expected = contract(field(N::Json, true));
        let proof = ValidatedClientInputTypes::try_new(expected.clone(), source.clone()).unwrap();
        assert!(proof.is_for(&expected, &source));
        assert!(!proof.is_for(
            &expected,
            &input(ValueKind::Nominal(V::Json), D::Utf8, true)
        ));
        assert!(!proof.is_for(&contract(field(N::Json, true)), &source));
        let shared = owner(binding);
        let rebound = proof.rebind_shared(shared.clone()).unwrap();
        assert!(rebound.is_for(&expected, &shared));
        assert!(
            proof
                .rebind_shared(input(ValueKind::Nominal(V::Json), D::Utf8, true))
                .is_none()
        );
        drop(rebound);
        drop(shared);
        let independent = proof.clone();
        drop(storage);
        drop(source);
        drop(proof);
        assert_eq!(released.load(Ordering::SeqCst), 0);
        drop(independent);
        assert_eq!(released.load(Ordering::SeqCst), 1);
    }
}
