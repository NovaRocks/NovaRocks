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
use crate::lower::node::Lowered;

/// Lower a SELECT_NODE plan node to a `Lowered` ExecNode.
pub(crate) fn lower_select_node(children: Vec<Lowered>) -> Result<Lowered, String> {
    if children.len() != 1 {
        return Err(format!(
            "SELECT_NODE expected 1 child, got {}",
            children.len()
        ));
    }
    Ok(children.into_iter().next().expect("child"))
}
