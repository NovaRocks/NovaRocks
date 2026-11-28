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

//! Pure provider-owned partition facts used before the write exchange.

use crate::{
    ConnectorCodecCategory, ConnectorEncodedPayload, ConnectorError, ConnectorErrorKind,
    ConnectorWriteBinding, ConnectorWriteFieldToken, PureProviderCompileError,
};
use novarocks_type_contract::{CompileCheckpoints, CompilePhase, PureCompileControl};
use std::{error::Error, sync::Arc};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ConnectorWriteLayoutRecipeDraft {
    binding: ConnectorWriteBinding,
    payload: ConnectorEncodedPayload,
    inputs: Arc<[ConnectorWriteFieldToken]>,
}

impl ConnectorWriteLayoutRecipeDraft {
    pub fn try_new(
        binding: ConnectorWriteBinding,
        payload: ConnectorEncodedPayload,
        inputs: impl Into<Arc<[ConnectorWriteFieldToken]>>,
    ) -> Result<Self, ConnectorError> {
        let inputs = inputs.into();
        let header = payload.header();
        if header.provider_id() != &binding.descriptor().provider_id
            || header.catalog() != binding.catalog_handle()
            || binding.catalog_handle().catalog_name() != &binding.descriptor().instance_id
            || header.category() != ConnectorCodecCategory::WriteLayout
            || payload.payload().is_empty()
            || inputs.is_empty()
            || inputs
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != inputs.len()
        {
            return Err(ConnectorError::new(
                ConnectorErrorKind::InvalidRequest,
                "write layout recipe must bind one exact provider, payload and nonempty input sequence",
            ));
        }
        Ok(Self {
            binding,
            payload,
            inputs,
        })
    }

    pub fn with_canonical_payload(
        &self,
        payload: ConnectorEncodedPayload,
    ) -> Result<Self, ConnectorError> {
        if payload.header() != self.payload.header() || payload.payload().is_empty() {
            return Err(public_facts_changed());
        }
        Ok(Self {
            binding: self.binding.clone(),
            payload,
            inputs: Arc::clone(&self.inputs),
        })
    }

    pub const fn binding(&self) -> &ConnectorWriteBinding {
        &self.binding
    }

    pub const fn payload(&self) -> &ConnectorEncodedPayload {
        &self.payload
    }

    pub fn inputs(&self) -> &[ConnectorWriteFieldToken] {
        &self.inputs
    }
}

pub trait ConnectorWriteLayoutRecipeCompiler: Send + Sync {
    type Error: Error + 'static;

    fn compile_private(
        &self,
        draft: &ConnectorWriteLayoutRecipeDraft,
        control: &dyn PureCompileControl,
    ) -> Result<ConnectorWriteLayoutRecipeDraft, PureProviderCompileError<Self::Error>>;
}

#[derive(Clone, Debug)]
pub struct ConnectorWriteLayoutRecipe(ConnectorWriteLayoutRecipeDraft);

impl ConnectorWriteLayoutRecipe {
    pub const fn draft(&self) -> &ConnectorWriteLayoutRecipeDraft {
        &self.0
    }

    pub(crate) fn compile<E: Error + 'static>(
        draft: &ConnectorWriteLayoutRecipeDraft,
        compiler: &dyn ConnectorWriteLayoutRecipeCompiler<Error = E>,
        control: &dyn PureCompileControl,
    ) -> Result<Self, crate::PureProviderProgramError<E>> {
        use crate::PureProviderProgramError as Failure;
        let mut work = CompileCheckpoints::try_new(control, CompilePhase::ProviderValidation)?;
        let result = (|| {
            work.flush()?;
            let canonical = compiler
                .compile_private(draft, work.control())
                .map_err(|failure| match failure {
                    PureProviderCompileError::Provider(error) => Failure::Provider(error),
                    PureProviderCompileError::Control(error) => Failure::Control(error),
                })?;
            if canonical.binding != draft.binding
                || canonical.payload.header() != draft.payload.header()
            {
                return Err(Failure::Contract(public_facts_changed()));
            }
            if !Arc::ptr_eq(&canonical.inputs, &draft.inputs) {
                if canonical.inputs.len() != draft.inputs.len() {
                    return Err(Failure::Contract(public_facts_changed()));
                }
                for (input, expected) in canonical.inputs.iter().zip(draft.inputs.iter()) {
                    work.step()?;
                    if input != expected {
                        return Err(Failure::Contract(public_facts_changed()));
                    }
                }
            }
            Ok(Self(canonical))
        })();
        work.finish_result(result, Failure::Control)
    }
}

fn public_facts_changed() -> ConnectorError {
    ConnectorError::new(
        ConnectorErrorKind::InvalidRequest,
        "write layout recipe public facts changed during provider canonicalization",
    )
}
