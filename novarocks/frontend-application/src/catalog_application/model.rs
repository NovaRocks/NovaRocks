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

//! Frontend catalog-application requests lowered from typed parser AST.
//!
//! These requests carry admitted catalog facts only. They are deliberately
//! separate from source syntax and from connector-owned mutation requests.

use novarocks_sql::semantic::{
    IcebergPartitionFieldExpr, ObjectName, TableColumnDef, TableKeyDesc,
};

#[derive(Clone, Debug, PartialEq)]
pub struct CatalogCreateTableRequest {
    pub name: ObjectName,
    pub kind: CatalogCreateTableKind,
    /// Set when the admitted statement used `CREATE TABLE IF NOT EXISTS`.
    pub if_not_exists: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CatalogCreateTableKind {
    Iceberg {
        columns: Vec<TableColumnDef>,
        key_desc: Option<TableKeyDesc>,
        bucket_count: Option<u32>,
        distribution_columns: Vec<String>,
        partition_fields: Vec<IcebergPartitionFieldExpr>,
        properties: Vec<(String, String)>,
    },
}
