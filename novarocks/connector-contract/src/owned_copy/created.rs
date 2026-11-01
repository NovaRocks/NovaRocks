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

//! Actual finite responsibility on the original owned-copy constructor.
use super::*;
use novarocks_type_contract::owned_resources::{layout::LayoutResourceError, vec as owned_vec};
use novarocks_type_contract::{
    ValueSchemaCreationFactory, ValueSchemaCreationScope, ValueSchemaScopeBeginFailure,
    ValueTypeStorageLifetime,
};

/// The actual creator failure is returned unchanged; it is not a provider or
/// numeric-profile refusal and invokes no subsequent copy/control callback.
#[derive(Debug)]
pub enum WriterOwnedCreationFailure<H> {
    Contract(PureProviderCompileError<ConnectorError>),
    Creator(H),
    Resource(LayoutResourceError),
}
impl<H> From<ConnectorError> for WriterOwnedCreationFailure<H> {
    fn from(error: ConnectorError) -> Self {
        Self::Contract(PureProviderCompileError::Provider(error))
    }
}
impl<H> From<CompileControlError> for WriterOwnedCreationFailure<H> {
    fn from(error: CompileControlError) -> Self {
        Self::Contract(PureProviderCompileError::Control(error))
    }
}
impl<H> From<LayoutResourceError> for WriterOwnedCreationFailure<H> {
    fn from(error: LayoutResourceError) -> Self {
        Self::Resource(error)
    }
}
impl<H: std::fmt::Display> std::fmt::Display for WriterOwnedCreationFailure<H> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Contract(e) => e.fmt(f),
            Self::Creator(e) => e.fmt(f),
            Self::Resource(e) => write!(f, "writer construction layout: {e:?}"),
        }
    }
}
impl<H: std::error::Error + 'static> std::error::Error for WriterOwnedCreationFailure<H> {}
type Failure<H> = WriterOwnedCreationFailure<H>;
fn control_resource<H>(error: novarocks_type_contract::ControlResourceError) -> Failure<H> {
    match error {
        novarocks_type_contract::ControlResourceError::Control(e) => e.into(),
        novarocks_type_contract::ControlResourceError::SourceModel(_) => {
            Failure::Resource(LayoutResourceError::SourceModel)
        }
    }
}

struct FieldFrame<S> {
    scope: S,
    children: Vec<ValueTypeStorageLifetime>,
}

pub(crate) struct CreatedWriteCopy<'a, 'work, 'control, H, F: ValueSchemaCreationFactory<H> + 'a, A>
{
    original: ObservedCopy<'work, 'control, A>,
    factory: &'a F,
    temporary: Option<F::Scope<'a>>,
    recipe: Option<F::Scope<'a>>,
    receipts: Option<F::Scope<'a>>,
    fields: Vec<FieldFrame<F::Scope<'a>>>,
    marker: std::marker::PhantomData<fn() -> H>,
}
fn begin<'a, H, F, A>(
    factory: &'a F,
    original: &mut ObservedCopy<'_, '_, A>,
) -> Result<F::Scope<'a>, Failure<H>>
where
    F: ValueSchemaCreationFactory<H>,
    A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
{
    original.flush().map_err(Failure::Contract)?;
    factory
        .begin_scope_observed(&mut |layout| original.extra_request(layout))
        .map_err(|error| match error {
            ValueSchemaScopeBeginFailure::Observer(error) => Failure::Contract(error),
            ValueSchemaScopeBeginFailure::Creator(error) => Failure::Creator(error),
        })
}
fn actual<H, A>(
    scope: &mut impl ValueSchemaCreationScope<H>,
    layout: Layout,
    original: &mut ObservedCopy<'_, '_, A>,
) -> Result<(), Failure<H>>
where
    A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
{
    if layout.size() != 0 {
        original.flush().map_err(Failure::Contract)?;
        scope.request_layout(layout).map_err(Failure::Creator)?;
    }
    Ok(())
}
impl<'a, 'work, 'control, H, F, A> CreatedWriteCopy<'a, 'work, 'control, H, F, A>
where
    F: ValueSchemaCreationFactory<H>,
    A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
{
    pub(crate) fn new(
        source: usize,
        factory: &'a F,
        admit: &'work mut A,
        work: &'work mut CompileCheckpoints<'control>,
    ) -> Result<Self, Failure<H>> {
        let original = ObservedCopy::new(source, admit, work).map_err(Failure::Contract)?;
        Ok(Self {
            original,
            factory,
            temporary: None,
            recipe: None,
            receipts: None,
            fields: Vec::new(),
            marker: std::marker::PhantomData,
        })
    }
    fn temporary(&mut self, layout: Layout, extra: bool) -> Result<(), Failure<H>> {
        if layout.size() == 0 {
            return Ok(());
        }
        if self.temporary.is_none() {
            self.temporary = Some(begin(self.factory, &mut self.original)?);
        }
        if extra {
            self.original
                .extra_request(layout)
                .map_err(Failure::Contract)?;
        }
        actual(
            self.temporary.as_mut().expect("temporary scope opened"),
            layout,
            &mut self.original,
        )
    }
    fn retained(&mut self, layout: Layout) -> Result<(), Failure<H>> {
        if layout.size() == 0 {
            return Ok(());
        }
        if let Some(frame) = self.fields.last_mut() {
            actual(&mut frame.scope, layout, &mut self.original)
        } else {
            if self.recipe.is_none() {
                self.recipe = Some(begin(self.factory, &mut self.original)?);
            }
            actual(
                self.recipe.as_mut().expect("recipe scope opened"),
                layout,
                &mut self.original,
            )
        }
    }
}
impl<H, F, A> OwnedCopy for CreatedWriteCopy<'_, '_, '_, H, F, A>
where
    F: ValueSchemaCreationFactory<H>,
    A: FnMut(&WriterOwnedResourceFacts) -> Result<(), CompileControlError>,
{
    type Error = Failure<H>;
    fn materializes(&self) -> bool {
        self.original.materializes()
    }
    fn created_fields(&self) -> bool {
        self.materializes()
    }
    fn source_invoice(&self) -> Option<usize> {
        self.original.source_invoice()
    }
    fn arithmetic(&self) -> Self::Error {
        Failure::Contract(self.original.arithmetic())
    }
    fn source_floor(&mut self, minimum: usize) -> Result<(), Self::Error> {
        self.original
            .source_floor(minimum)
            .map_err(Failure::Contract)
    }
    fn temporary_model_request(
        &mut self,
        layout: Layout,
        copies: usize,
    ) -> Result<(), Self::Error> {
        self.original
            .request(layout, copies)
            .map_err(Failure::Contract)?;
        if self.materializes() {
            for _ in 0..copies {
                self.temporary(layout, false)?;
            }
        }
        Ok(())
    }
    fn request(&mut self, layout: Layout, copies: usize) -> Result<(), Self::Error> {
        self.original
            .request(layout, copies)
            .map_err(Failure::Contract)?;
        if self.materializes() {
            for _ in 0..copies {
                self.retained(layout)?;
            }
        }
        Ok(())
    }
    fn opaque_model_request(&mut self, bytes: usize, copies: usize) -> Result<(), Self::Error> {
        let model = Layout::array::<u8>(bytes).map_err(|_| self.arithmetic())?;
        self.original
            .request(model, copies)
            .map_err(Failure::Contract)?;
        if self.materializes() && bytes != 0 {
            for _ in 0..copies {
                self.original.flush().map_err(Failure::Contract)?;
                if let Some(frame) = self.fields.last_mut() {
                    frame
                        .scope
                        .observe_opaque_upper_bound(bytes)
                        .map_err(Failure::Creator)?;
                } else {
                    if self.recipe.is_none() {
                        self.recipe = Some(begin(self.factory, &mut self.original)?);
                    }
                    self.recipe
                        .as_mut()
                        .expect("recipe scope opened")
                        .observe_opaque_upper_bound(bytes)
                        .map_err(Failure::Creator)?;
                }
            }
        }
        Ok(())
    }
    fn temporary_request(&mut self, layout: Layout) -> Result<(), Self::Error> {
        // These validation sets/name copies are genuinely allocated during
        // Count. Their original numerical quote already precedes this event.
        self.temporary(layout, false)
    }
    fn prefunded_request(&mut self, layout: Layout, copies: usize) -> Result<(), Self::Error> {
        for _ in 0..copies {
            self.retained(layout)?;
        }
        Ok(())
    }
    fn receipt_request(&mut self, layout: Layout, copies: usize) -> Result<(), Self::Error> {
        if layout.size() == 0 {
            return Ok(());
        }
        if self.receipts.is_none() {
            self.receipts = Some(begin(self.factory, &mut self.original)?);
        }
        for _ in 0..copies {
            actual(
                self.receipts.as_mut().expect("receipt scope opened"),
                layout,
                &mut self.original,
            )?;
        }
        Ok(())
    }
    fn created_wrapper(&mut self, layout: Layout) -> Result<(), Self::Error> {
        self.original
            .extra_request(layout)
            .map_err(Failure::Contract)?;
        self.retained(layout)
    }
    fn begin_field(&mut self) -> Result<(), Self::Error> {
        if !self.materializes() {
            return Ok(());
        }
        let growth = owned_vec::push_growth(&self.fields).map_err(control_resource::<H>)?;
        if let Some(layout) = growth.requested_backing {
            self.temporary(layout, true)?;
            let result = self.fields.try_reserve(1);
            self.reserve_exit(result)?;
        }
        let scope = begin(self.factory, &mut self.original)?;
        self.fields.push(FieldFrame {
            scope,
            children: Vec::new(),
        });
        Ok(())
    }
    fn finish_field(&mut self) -> Result<ValueTypeStorageLifetime, Self::Error> {
        if !self.materializes() {
            return Ok(ValueTypeStorageLifetime::pure());
        }
        let mut frame = self.fields.pop().ok_or_else(|| self.arithmetic())?;
        let own = frame.scope.storage_lifetime();
        // Only this Field's own backing and exactly reachable child Fields.
        // No namespace, recipe, validation scratch or complete source owner.
        let ObservedCopy {
            facts,
            admit,
            checkpoints,
            ..
        } = &mut self.original;
        own.retaining_flat_observed(
            frame.children.iter().map(Ok),
            |layout| {
                extra_request(facts, *admit, layout).map_err(Failure::Contract)?;
                frame.scope.request_layout(layout).map_err(Failure::Creator)
            },
            checkpoints,
        )
    }
    fn retain_shared_field(
        &mut self,
        storage: ValueTypeStorageLifetime,
    ) -> Result<(), Self::Error> {
        let frame = self.fields.last().ok_or_else(|| self.arithmetic())?;
        let growth = owned_vec::push_growth(&frame.children).map_err(control_resource::<H>)?;
        if let Some(layout) = growth.requested_backing {
            self.temporary(layout, true)?;
            let result = self
                .fields
                .last_mut()
                .expect("parent Field frame")
                .children
                .try_reserve(1);
            self.reserve_exit(result)?;
        }
        self.fields
            .last_mut()
            .expect("parent Field frame")
            .children
            .push(storage);
        Ok(())
    }
    fn storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.recipe
            .as_ref()
            .map_or_else(ValueTypeStorageLifetime::pure, |s| s.storage_lifetime())
    }
    fn receipt_storage_lifetime(&self) -> ValueTypeStorageLifetime {
        self.receipts
            .as_ref()
            .map_or_else(ValueTypeStorageLifetime::pure, |s| s.storage_lifetime())
    }
    fn work(&mut self, units: usize) -> Result<(), Self::Error> {
        self.original.work(units).map_err(Failure::Contract)
    }
    fn step(&mut self) -> Result<(), Self::Error> {
        self.original.step().map_err(Failure::Contract)
    }
    fn flush(&mut self) -> Result<(), Self::Error> {
        self.original.flush().map_err(Failure::Contract)
    }
    fn begin_copy(&mut self) -> Result<(), Self::Error> {
        self.original.begin_copy().map_err(Failure::Contract)
    }
    fn reserve_exit(&mut self, r: Result<(), TryReserveError>) -> Result<(), Self::Error> {
        self.original.reserve_exit(r).map_err(Failure::Contract)
    }
    fn spelling(&mut self, s: &str) -> Result<String, Self::Error> {
        self.original.spelling(s).map_err(Failure::Contract)
    }
}
