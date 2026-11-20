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

//! Borrowed dependency facts at scalar evaluation and plan publication.
//! Observation owns no binding, provenance, constant backing, or capacity grant.

use novarocks_functions::ConstantValue;
use novarocks_type_contract::PureCompileControl;

/// Observation is owned by the original compile request.
pub trait SqlFoldDependencyObserver: Send + Sync {
    /// Borrow the original standalone unit and its optional admitted result.
    /// A kept node has no result. The receipt may refuse through the same control.
    fn observe_fold_unit_observed(
        &self,
        _source: &super::SqlAuthoredScalarUnit,
        _result: Option<&ConstantValue>,
        _control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }

    /// Borrow the complete source only after its original publication succeeds.
    /// This loans no kernel coverage, resource grant, or statement-success proof.
    /// The host admits its receipt work; only original compile control may refuse.
    /// Existing observers add no work or checkpoints by default.
    fn observe_published_source_observed(
        &self,
        _source: &super::SqlAuthoredPhysicalPlan,
        _control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
    /// Borrow the original completed required scalar source. This is not an
    /// optional fold, a fragment publication, or statement-success evidence.
    /// Default observers add no work or checkpoints.
    fn observe_required_scalar_source_observed(
        &self,
        _source: &super::SqlAuthoredScalarUnit,
        _control: &dyn PureCompileControl,
    ) -> Result<(), novarocks_type_contract::CompileControlError> {
        Ok(())
    }
}
