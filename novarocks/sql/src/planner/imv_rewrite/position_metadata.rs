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

use crate::optimizer::rewrite::context::RewriteContext;
use novarocks_types::schema::ColumnDef;

use super::annotation::ImvExtension;

pub(super) fn target_file_metadata_columns<'ctx>(
    ctx: &'ctx RewriteContext<'_>,
) -> &'ctx [ColumnDef] {
    ctx.extension::<ImvExtension>()
        .and_then(|ext| ext.snapshot.target_file_metadata.as_deref())
        .map_or(&[], |fields| fields.as_slice())
}

pub(super) fn is_target_file_metadata(name: &str) -> bool {
    name.eq_ignore_ascii_case("_spec_id") || name.eq_ignore_ascii_case("_partition")
}

pub(crate) fn nullable_file_metadata_value(
    field: &ColumnDef,
) -> Result<novarocks_type_contract::FunctionValueType, String> {
    field
        .declared_value_type()
        .map_err(|error| error.to_string())?
        .with_nullable(true)
        .map_err(|error| error.to_string())
}
