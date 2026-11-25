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

//! The provider-authored public schema of one frozen read.

use std::sync::Arc;

use arrow::datatypes::Schema;
use novarocks_connector_contract::{ConnectorReadExecutionSchema, ConnectorSourceSchemaFacts};
use novarocks_type_contract::{CompileControlError, ValueLogicalType, ValueTypeStorageLifetime};

use crate::connector::{ConnectorError, ConnectorErrorKind};

type SchemaCreationObservers<'a> = (
    &'a mut (dyn FnMut(std::alloc::Layout) -> Result<(), CompileControlError> + 'a),
    &'a mut (dyn FnMut(usize) -> Result<(), CompileControlError> + 'a),
);

/// Borrowed caller-owned creation observations. The result keeps only the
/// actual storage lifetime; it never retains these callbacks or their owner.
pub struct ConnectorReadSchemaCreation<'a> {
    storage: ValueTypeStorageLifetime,
    request: &'a mut dyn FnMut(std::alloc::Layout) -> Result<(), CompileControlError>,
    work: &'a mut dyn FnMut(usize) -> Result<(), CompileControlError>,
}
impl<'a> ConnectorReadSchemaCreation<'a> {
    pub fn new(
        storage: ValueTypeStorageLifetime,
        request: &'a mut dyn FnMut(std::alloc::Layout) -> Result<(), CompileControlError>,
        work: &'a mut dyn FnMut(usize) -> Result<(), CompileControlError>,
    ) -> Self {
        Self {
            storage,
            request,
            work,
        }
    }
    pub fn storage(&self) -> ValueTypeStorageLifetime {
        self.storage.clone()
    }
    pub fn request(
        &mut self,
        layout: std::alloc::Layout,
    ) -> Result<(), novarocks_connector_contract::PureProviderCompileError<ConnectorError>> {
        (self.request)(layout)
            .map_err(novarocks_connector_contract::PureProviderCompileError::Control)
    }
    pub fn work(
        &mut self,
        units: usize,
    ) -> Result<(), novarocks_connector_contract::PureProviderCompileError<ConnectorError>> {
        (self.work)(units).map_err(novarocks_connector_contract::PureProviderCompileError::Control)
    }
    /// Split the independent observers for constructors accepting two borrowed
    /// callbacks. Both remain borrowed from this creation call.
    pub fn observers(&mut self) -> SchemaCreationObservers<'_> {
        (&mut *self.request, &mut *self.work)
    }
    pub fn reserve<T>(
        &mut self,
        count: usize,
    ) -> Result<Vec<T>, novarocks_connector_contract::PureProviderCompileError<ConnectorError>>
    {
        let layout = std::alloc::Layout::array::<T>(count).map_err(|_| creation_extent())?;
        if layout.size() != 0 {
            self.request(layout)?;
        }
        self.work(1)?;
        let mut values = Vec::new();
        values
            .try_reserve_exact(count)
            .map_err(|_| creation_extent())?;
        Ok(values)
    }
    pub fn arc<T>(
        &mut self,
        value: T,
    ) -> Result<Arc<T>, novarocks_connector_contract::PureProviderCompileError<ConnectorError>>
    {
        let layout = novarocks_type_contract::owned_resources::layout::arc_layout(
            std::alloc::Layout::new::<T>(),
        )
        .map_err(|_| creation_extent())?;
        self.request(layout)?;
        self.work(1)?;
        Ok(Arc::new(value))
    }
    pub fn arc_slice<T>(
        &mut self,
        values: Vec<T>,
    ) -> Result<Arc<[T]>, novarocks_connector_contract::PureProviderCompileError<ConnectorError>>
    {
        // reserve() creates exact capacity; refuse unexpected excess backing
        // instead of hiding a Vec-to-Box shrink allocation from this owner.
        if values.len() != values.capacity() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "schema creation slice has unobserved spare capacity",
            )
            .into());
        }
        let payload =
            std::alloc::Layout::array::<T>(values.len()).map_err(|_| creation_extent())?;
        let layout = novarocks_type_contract::owned_resources::layout::arc_layout(payload)
            .map_err(|_| creation_extent())?;
        self.request(layout)?;
        self.work(payload.size())?;
        Ok(values.into_boxed_slice().into())
    }
    pub fn text(
        &mut self,
        source: &str,
    ) -> Result<String, novarocks_connector_contract::PureProviderCompileError<ConnectorError>>
    {
        let mut bytes = self.reserve::<u8>(source.len())?;
        self.work(source.len())?;
        bytes.extend_from_slice(source.as_bytes());
        // The source is UTF-8; conversion reuses the exact existing backing.
        String::from_utf8(bytes).map_err(|_| {
            ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "schema source text is not UTF-8",
            )
            .into()
        })
    }
    pub fn arrow_fields(
        &mut self,
        fields: Vec<Arc<arrow::datatypes::Field>>,
    ) -> Result<
        arrow::datatypes::Fields,
        novarocks_connector_contract::PureProviderCompileError<ConnectorError>,
    > {
        if fields.len() != fields.capacity() {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "schema field collection has unobserved spare capacity",
            )
            .into());
        }
        let payload = std::alloc::Layout::array::<Arc<arrow::datatypes::Field>>(fields.len())
            .map_err(|_| creation_extent())?;
        let layout = novarocks_type_contract::owned_resources::layout::arc_layout(payload)
            .map_err(|_| creation_extent())?;
        self.request(layout)?;
        self.work(payload.size())?;
        Ok(fields.into())
    }
}
fn creation_extent() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::ResourceExhausted,
        "public schema creation allocation or extent was refused",
    )
}

/// The complete execution bindings and independent provider source facts of
/// one frozen read, in assignment order.
///
/// Only the provider that owns the read authors this. A field's name, NULL
/// contract, nested domain and source metadata (field IDs, initial defaults)
/// are that provider's external facts, and its pure read compiler checks a
/// frozen read's public schema against the same field author. No role derives
/// one from SQL names or engine types, and nothing downstream may re-author
/// it: the value is carried unchanged into the frozen read's public facts.
///
/// A read projecting no column has the empty schema. Whether such a read can
/// be frozen into a program is decided by the program's own laws, not here.
#[derive(Clone, Debug)]
pub struct ConnectorReadPublicSchema {
    logical_types: Arc<[ValueLogicalType]>,
    typed: TypedPublicSchema,
}
#[derive(Clone, Debug)]
struct TypedPublicSchema {
    execution: ConnectorReadExecutionSchema,
    source: ConnectorSourceSchemaFacts,
    _storage: ValueTypeStorageLifetime,
}

impl ConnectorReadPublicSchema {
    /// Publish explicitly authored recursive values and separate source facts.
    /// No root-only label list or generic annotation supplies missing semantics.
    pub fn try_new_typed(
        execution: ConnectorReadExecutionSchema,
        source: ConnectorSourceSchemaFacts,
        storage: ValueTypeStorageLifetime,
    ) -> Result<Self, ConnectorError> {
        Self::try_new_typed_observed(
            &execution,
            &source,
            storage,
            |_| Ok::<_, ConnectorError>(()),
            |_| Ok(()),
        )
    }
    pub fn try_new_typed_observed<E: From<ConnectorError>>(
        execution: &ConnectorReadExecutionSchema,
        source: &ConnectorSourceSchemaFacts,
        storage: ValueTypeStorageLifetime,
        mut request: impl FnMut(std::alloc::Layout) -> Result<(), E>,
        mut work: impl FnMut(usize) -> Result<(), E>,
    ) -> Result<Self, E> {
        work(1)?;
        if execution.bindings().len() != source.shape().schema().fields().len() {
            return Err(E::from(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "public read execution/source occurrence counts differ",
            )));
        }
        let count = execution.bindings().len();
        let layout = std::alloc::Layout::array::<ValueLogicalType>(count).map_err(|_| {
            E::from(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "public read projection extent exceeds address space",
            ))
        })?;
        if layout.size() != 0 {
            request(layout)?;
        }
        let arc_layout = novarocks_type_contract::owned_resources::layout::arc_layout(layout)
            .map_err(|_| {
                E::from(ConnectorError::new(
                    ConnectorErrorKind::ResourceExhausted,
                    "public read projection extent exceeds address space",
                ))
            })?;
        request(arc_layout)?;
        let mut logical_types = Vec::new();
        logical_types.try_reserve_exact(count).map_err(|_| {
            E::from(ConnectorError::new(
                ConnectorErrorKind::ResourceExhausted,
                "public read projection allocation was refused",
            ))
        })?;
        for binding in execution.bindings() {
            work(1)?;
            logical_types.push(binding.logical_type());
        }
        // Box-to-Arc copies the actual compatibility slice payload. Admit
        // that copy before conversion while both allocations remain live.
        work(layout.size())?;
        Ok(Self {
            logical_types: logical_types.into_boxed_slice().into(),
            typed: TypedPublicSchema {
                execution: execution.clone(),
                source: source.clone(),
                _storage: storage,
            },
        })
    }

    pub fn execution_schema(&self) -> Option<&ConnectorReadExecutionSchema> {
        Some(&self.typed.execution)
    }
    pub fn source_schema_facts(&self) -> Option<&ConnectorSourceSchemaFacts> {
        Some(&self.typed.source)
    }
    /// The complete guarded parts, retaining the exact original creators.
    pub fn into_typed_parts(
        self,
    ) -> Result<(ConnectorReadExecutionSchema, ConnectorSourceSchemaFacts), ConnectorError> {
        let Self {
            logical_types,
            typed,
        } = self;
        let TypedPublicSchema {
            execution,
            source,
            _storage,
        } = typed;
        // Dispose the compatibility projection before its creator hold.
        drop(logical_types);
        drop(_storage);
        Ok((execution, source))
    }

    pub fn schema(&self) -> &Schema {
        self.typed.execution.schema()
    }

    pub fn logical_types(&self) -> &[ValueLogicalType] {
        &self.logical_types
    }
}

/// Provider source facts remain part of read identity: two declarations that
/// differ only in a source field ID are different reads, even though their
/// metadata-free execution bindings are equal.
impl PartialEq for ConnectorReadPublicSchema {
    fn eq(&self, other: &Self) -> bool {
        self.typed.execution == other.typed.execution && self.typed.source == other.typed.source
    }
}

impl Eq for ConnectorReadPublicSchema {}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use arrow::datatypes::{DataType, Field};

    use super::*;

    use novarocks_connector_contract::{ConnectorSourceFieldFacts, ConnectorSourceSchemaShape};
    use novarocks_type_contract::FunctionValueType;

    fn execution(count: usize) -> ConnectorReadExecutionSchema {
        let fields = (0..count)
            .map(|index| Field::new(format!("id_{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        ConnectorReadExecutionSchema::try_new(
            Arc::new(Schema::new(fields)),
            vec![FunctionValueType::new(DataType::Int64, false); count].into(),
            ValueTypeStorageLifetime::pure(),
        )
        .expect("complete execution declaration")
    }

    fn source(count: usize, id: i32) -> ConnectorSourceSchemaFacts {
        let fields = (0..count)
            .map(|index| Field::new(format!("id_{index}"), DataType::Int64, false))
            .collect::<Vec<_>>();
        let shape = ConnectorSourceSchemaShape::try_new(
            Arc::new(Schema::new(fields)),
            ValueTypeStorageLifetime::pure(),
        )
        .expect("source shape");
        let facts = (0..count)
            .map(|index| {
                ConnectorSourceFieldFacts::try_new(
                    vec![u32::try_from(index).unwrap()].into(),
                    Some(id),
                    None,
                    HashMap::new(),
                    &[],
                    ValueTypeStorageLifetime::pure(),
                )
                .expect("original source field ID")
            })
            .collect::<Vec<_>>();
        ConnectorSourceSchemaFacts::try_new(
            shape,
            HashMap::new(),
            facts.into(),
            ValueTypeStorageLifetime::pure(),
        )
        .expect("source facts")
    }

    #[test]
    fn every_field_has_exactly_one_logical_type() {
        let error = ConnectorReadPublicSchema::try_new_typed(
            execution(1),
            source(0, 1),
            ValueTypeStorageLifetime::pure(),
        )
        .expect_err("a complete execution field without its source occurrence is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let error = ConnectorReadPublicSchema::try_new_typed(
            execution(2),
            source(1, 1),
            ValueTypeStorageLifetime::pure(),
        )
        .expect_err("a complete type without a source field is refused");
        assert_eq!(error.kind(), ConnectorErrorKind::InvalidRequest);
        let empty = ConnectorReadPublicSchema::try_new_typed(
            execution(0),
            source(0, 1),
            ValueTypeStorageLifetime::pure(),
        )
        .expect("a read projecting no column has the empty schema");
        assert!(empty.schema().fields().is_empty());
    }

    #[test]
    fn field_metadata_is_part_of_the_published_fact() {
        let make = |id| {
            ConnectorReadPublicSchema::try_new_typed(
                execution(1),
                source(1, id),
                ValueTypeStorageLifetime::pure(),
            )
            .expect("schema")
        };
        let one = make(1);
        let same = make(1);
        let other_id = make(2);
        assert_eq!(one, same);
        assert_ne!(one, other_id);
        let (execution, source) = one.into_typed_parts().unwrap();
        assert_eq!(source.field_facts()[0].field_id(), Some(1));
        assert_eq!(
            execution.bindings()[0].logical_type(),
            ValueLogicalType::Physical
        );
    }
}

#[cfg(test)]
mod typed_tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};
    use novarocks_connector_contract::{ConnectorSourceFieldFacts, ConnectorSourceSchemaShape};
    use novarocks_type_contract::{FunctionValueType, ValueTypeStorageHold};
    use std::{
        collections::HashMap,
        sync::atomic::{AtomicUsize, Ordering},
    };
    struct Hold(Arc<AtomicUsize>);
    impl ValueTypeStorageHold for Hold {}
    impl Drop for Hold {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    fn held(exits: &Arc<AtomicUsize>) -> ValueTypeStorageLifetime {
        ValueTypeStorageLifetime::held(Arc::new(Hold(exits.clone())))
    }
    #[test]
    fn typed_public_schema_shares_complete_parts_and_refuses_legacy_guard_escape() {
        let exec_exits = Arc::new(AtomicUsize::new(0));
        let shape_exits = Arc::new(AtomicUsize::new(0));
        let public_exits = Arc::new(AtomicUsize::new(0));
        let binding = FunctionValueType::try_with_logical_type(
            DataType::LargeBinary,
            false,
            ValueLogicalType::Json,
        )
        .unwrap();
        let execution = ConnectorReadExecutionSchema::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "scan",
                DataType::LargeBinary,
                false,
            )])),
            vec![binding.clone()].into(),
            held(&exec_exits),
        )
        .unwrap();
        let shape = ConnectorSourceSchemaShape::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "source",
                DataType::LargeBinary,
                false,
            )])),
            held(&shape_exits),
        )
        .unwrap();
        let fact = ConnectorSourceFieldFacts::try_new(
            vec![0].into(),
            Some(17),
            None,
            HashMap::new(),
            &[],
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        let source = ConnectorSourceSchemaFacts::try_new(
            shape,
            HashMap::new(),
            vec![fact].into(),
            ValueTypeStorageLifetime::pure(),
        )
        .unwrap();
        #[derive(Debug, PartialEq)]
        enum Failure {
            Caller(usize),
            Source(ConnectorErrorKind),
        }
        impl From<ConnectorError> for Failure {
            fn from(error: ConnectorError) -> Self {
                Self::Source(error.kind())
            }
        }
        let trace = std::cell::RefCell::new(Vec::new());
        let refuse = std::cell::Cell::new(None);
        let observe = |event: (u8, usize, usize)| -> Result<(), Failure> {
            let mut trace = trace.borrow_mut();
            trace.push(event);
            let index = trace.len() - 1;
            if refuse.get() == Some(index) {
                Err(Failure::Caller(index))
            } else {
                Ok(())
            }
        };
        let run = || {
            ConnectorReadPublicSchema::try_new_typed_observed(
                &execution,
                &source,
                ValueTypeStorageLifetime::pure(),
                |layout| observe((0, layout.size(), layout.align())),
                |units| observe((1, units, 0)),
            )
        };
        drop(run().unwrap());
        let complete = trace.borrow().clone();
        assert!(complete.contains(&(1, std::mem::size_of::<ValueLogicalType>(), 0)));
        for index in 0..complete.len() {
            trace.borrow_mut().clear();
            refuse.set(Some(index));
            assert_eq!(run().unwrap_err(), Failure::Caller(index));
            assert_eq!(*trace.borrow(), complete[..=index]);
        }
        let mut requests = Vec::new();
        let public = ConnectorReadPublicSchema::try_new_typed_observed(
            &execution,
            &source,
            held(&public_exits),
            |layout| {
                requests.push(layout);
                Ok::<_, ConnectorError>(())
            },
            |_| Ok(()),
        )
        .unwrap();
        drop(execution);
        drop(source);
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0],
            std::alloc::Layout::array::<ValueLogicalType>(1).unwrap()
        );
        assert_eq!(
            requests[1],
            novarocks_type_contract::owned_resources::layout::arc_layout(requests[0]).unwrap()
        );
        assert_eq!(
            public.execution_schema().unwrap().binding(0).unwrap(),
            &binding
        );
        assert_eq!(public.schema().field(0).name(), "scan");
        assert_eq!(
            public
                .source_schema_facts()
                .unwrap()
                .shape()
                .schema()
                .field(0)
                .name(),
            "source"
        );
        let cloned = public.clone();
        assert_eq!(
            public.logical_types().as_ptr(),
            cloned.logical_types().as_ptr()
        );
        // There is no consuming source-only escape: both full parts transfer together.
        drop(public);
        assert_eq!(public_exits.load(Ordering::SeqCst), 0);
        let (execution, source) = cloned.into_typed_parts().unwrap();
        assert_eq!(public_exits.load(Ordering::SeqCst), 1);
        assert_eq!(exec_exits.load(Ordering::SeqCst), 0);
        assert_eq!(shape_exits.load(Ordering::SeqCst), 0);
        drop(execution);
        assert_eq!(exec_exits.load(Ordering::SeqCst), 1);
        drop(source);
        assert_eq!(shape_exits.load(Ordering::SeqCst), 1);
    }
}
