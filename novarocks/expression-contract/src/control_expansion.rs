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

//! Checked physical wrappers that occupy a single source control position.

use crate::unit_creation::{UnitValidationCreation, ValidationFailure};
use crate::{ExprId, ExprKind, ExprNode, UnitConstructionFailure, UnitError};
use novarocks_type_contract::{
    CompileCheckpoints, FunctionKind, OperatorFunction, ValueSchemaCreationScope,
    ValueTypeStorageLifetime,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ControlExpansion {
    pub root: ExprId,
    pub positive: ExprId,
    pub conjunction: ExprId,
    pub branches: [ExprId; 2],
}

#[derive(Debug)]
pub struct ControlExpansions {
    records: Box<[ControlExpansion]>,
    roots: Box<[Option<ExprId>]>,
    _storage: ValueTypeStorageLifetime,
}

impl Default for ControlExpansions {
    fn default() -> Self {
        Self {
            records: Box::default(),
            roots: Box::default(),
            _storage: ValueTypeStorageLifetime::pure(),
        }
    }
}

impl ControlExpansions {
    pub fn records(&self) -> &[ControlExpansion] {
        &self.records
    }

    pub fn source_position(&self, definition: ExprId) -> Option<ExprId> {
        self.roots.get(definition.get() as usize).copied().flatten()
    }

    pub fn shares_source_position(&self, parent: ExprId, child: ExprId) -> bool {
        self.source_position(parent)
            .is_some_and(|source| self.source_position(child) == Some(source))
    }

    pub fn try_new_observed(
        records: Box<[ControlExpansion]>,
        definitions: &[ExprNode],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, UnitError> {
        let mut creation = UnitValidationCreation::<std::convert::Infallible>::pure();
        Self::try_new_created(records, definitions, &mut creation, work)
            .map_err(|error| crate::unit_creation::pure_error(creation.finish_error(error, work)))
    }

    pub fn try_new_with_creator_in<H>(
        records: Box<[ControlExpansion]>,
        definitions: &[ExprNode],
        creation: &mut dyn ValueSchemaCreationScope<H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, UnitConstructionFailure<H>> {
        let mut creation = UnitValidationCreation::single(creation);
        Self::try_new_created(records, definitions, &mut creation, work)
            .map_err(|error| creation.finish_error(error, work))
    }

    pub(crate) fn try_new_created<'a, H>(
        records: Box<[ControlExpansion]>,
        definitions: &[ExprNode],
        creation: &mut UnitValidationCreation<'_, H>,
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<Self, ValidationFailure<'a, H>> {
        if records.is_empty() {
            return Ok(Self::default());
        }
        if records.len() > definitions.len() / 3 {
            return Err(invalid(records[0].root));
        }
        let mut roots = Vec::new();
        creation.reserve_retained(&mut roots, definitions.len(), work)?;
        for _ in definitions {
            roots.push(None);
            work.step()?;
        }
        for record in &records {
            let members = [
                record.root,
                record.positive,
                record.conjunction,
                record.branches[0],
                record.branches[1],
            ];
            for (position, &id) in members.iter().enumerate() {
                if !members[..position].contains(&id) {
                    let slot = roots
                        .get_mut(id.get() as usize)
                        .ok_or_else(|| invalid(record.root))?;
                    if slot.replace(record.root).is_some() {
                        return Err(invalid(record.root));
                    }
                }
                work.step()?;
            }
        }
        let checked = Self {
            records,
            roots: creation.boxed_retained(roots, work)?,
            _storage: creation.retained_storage(),
        };
        checked.validate_definitions(definitions, work)?;
        Ok(checked)
    }

    pub(crate) fn validate_definitions<'a, H>(
        &self,
        definitions: &[ExprNode],
        work: &mut CompileCheckpoints<'_>,
    ) -> Result<(), ValidationFailure<'a, H>> {
        if self.records.is_empty() {
            return Ok(());
        }
        let definition = |id: ExprId| {
            definitions
                .get(id.get() as usize)
                .filter(|node| node.id == id)
        };
        for record in &self.records {
            let root = definition(record.root).ok_or_else(|| invalid(record.root))?;
            let positive = definition(record.positive).ok_or_else(|| invalid(record.root))?;
            let conjunction = definition(record.conjunction).ok_or_else(|| invalid(record.root))?;
            if record.root != record.positive
                && !matches!(
                    &root.kind,
                    ExprKind::FunctionCall { function, args }
                        if function.kind == FunctionKind::Scalar
                            && function.function_id.as_str() == OperatorFunction::BooleanNot.function_id_text()
                            && args.as_ref() == [record.positive]
                )
            {
                return Err(invalid(record.root));
            }
            if record.positive != record.conjunction
                && !matches!(
                    positive.kind,
                    ExprKind::Let { value, body }
                        if body == record.conjunction && self.source_position(value) != Some(record.root)
                )
            {
                return Err(invalid(record.root));
            }
            if !matches!(&conjunction.kind, ExprKind::Conjunction { args } if args.as_ref() == record.branches)
                || record.branches[0] == record.branches[1]
                || root.lambda_scope != positive.lambda_scope
                || root.lambda_scope != conjunction.lambda_scope
            {
                return Err(invalid(record.root));
            }
            for id in record.branches {
                let branch = definition(id).ok_or_else(|| invalid(record.root))?;
                let valid = matches!(&branch.kind, ExprKind::FunctionCall { function, args }
                    if function.kind == FunctionKind::Scalar
                        && OperatorFunction::from_function_id(&function.function_id)
                            .and_then(OperatorFunction::comparison_overload_text) == Some(function.overload.as_str())
                        && args.len() == 2
                        && args.iter().all(|arg| self.source_position(*arg) != Some(record.root)));
                if !valid || branch.lambda_scope != root.lambda_scope {
                    return Err(invalid(record.root));
                }
                work.step()?;
            }
            work.step()?;
        }
        for parent in definitions {
            parent.kind.expression_references_observed(|child| {
                if let Some(source) = self.source_position(child)
                    && source != child
                    && self.source_position(parent.id) != Some(source)
                {
                    return Err(invalid(source));
                }
                work.step()?;
                Ok::<_, ValidationFailure<'a, H>>(())
            })?;
            work.step()?;
        }
        Ok(())
    }
}

fn invalid<'a, H>(id: ExprId) -> ValidationFailure<'a, H> {
    ValidationFailure::invalid(Some(id), None, "invalid source control expansion")
}
