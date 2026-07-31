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
pub mod aggregate;
pub mod analytic;
pub mod assert;
pub mod change_event_expand;
pub mod exchange_source;
pub mod fetch;
pub mod filter;
pub mod join;
pub mod limit;
pub mod lookup;
pub mod nljoin;
pub mod project;
pub mod repeat;
pub mod runtime_filter;
pub mod scan;
pub mod set_op;
pub mod sort;
pub mod table_function;
pub mod union_all;
pub mod values;

use crate::exec::chunk::Chunk;
use crate::exec::expr::ExprArena;
use crate::exec::node::aggregate::AggregateNode;
use crate::exec::node::analytic::AnalyticNode;
use crate::exec::node::assert::AssertNumRowsNode;
use crate::exec::node::change_event_expand::ChangeEventExpandNode;
use crate::exec::node::exchange_source::ExchangeSourceNode;
use crate::exec::node::fetch::FetchNode;
use crate::exec::node::filter::FilterNode;
use crate::exec::node::join::JoinNode;
use crate::exec::node::limit::LimitNode;
use crate::exec::node::lookup::LookUpNode;
use crate::exec::node::nljoin::NestedLoopJoinNode;
use crate::exec::node::project::ProjectNode;
use crate::exec::node::repeat::RepeatNode;
use crate::exec::node::scan::ScanNode;
use crate::exec::node::set_op::SetOpNode;
use crate::exec::node::sort::SortNode;
use crate::exec::node::table_function::TableFunctionNode;
use crate::exec::node::union_all::UnionAllNode;
use crate::exec::node::values::ValuesNode;

pub type ExecResult = Result<Chunk, String>;
pub type BoxedExecIter = Box<dyn Iterator<Item = ExecResult> + Send>;

#[derive(Clone, Debug)]
pub enum ExecNodeKind {
    AssertNumRows(AssertNumRowsNode),
    Values(ValuesNode),
    Project(ProjectNode),
    Filter(FilterNode),
    Repeat(RepeatNode),
    ChangeEventExpand(ChangeEventExpandNode),
    UnionAll(UnionAllNode),
    Limit(LimitNode),
    ExchangeSource(ExchangeSourceNode),
    Scan(ScanNode),
    Fetch(FetchNode),
    LookUp(LookUpNode),
    Aggregate(AggregateNode),
    Join(JoinNode),
    NestedLoopJoin(NestedLoopJoinNode),
    Sort(SortNode),
    TableFunction(TableFunctionNode),
    Analytic(AnalyticNode),
    SetOp(SetOpNode),
    RuntimeFilterConsumer(runtime_filter::RuntimeFilterConsumerNode),
}

#[derive(Clone, Debug)]
pub struct ExecNode {
    pub kind: ExecNodeKind,
}

#[derive(Clone, Debug)]
pub struct ExecPlan {
    pub arena: ExprArena,
    pub root: ExecNode,
}
