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

use std::fmt::Debug;

use crate::connector::read_stack::{ColumnHandle, ConnectorSplit};

/// The concrete values that form one provider's read family.
///
/// A marker implementation is shared by the provider's FE and BE adapters, so
/// the two roles cannot independently choose merely similar handle types.
pub trait ProviderReadTypes: Send + Sync + 'static {
    type Table: Debug + Send + Sync + 'static;
    type Column: ColumnHandle;
    type ReadView: Clone + Debug + Send + Sync + 'static;
    type Split: ConnectorSplit;
}

/// The concrete values that form one provider's write family.
pub trait ProviderWriteTypes: Send + Sync + 'static {
    type WriterHandle: Clone + Debug + Send + Sync + 'static;
    type CommitFragment: Debug + Send + Sync + 'static;
}
