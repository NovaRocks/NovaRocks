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

//! Answering a compiler's needs from the owners that hold the answers.
//!
//! Pure compilation asks for exactly the outside facts it is missing, and
//! something has to go and get them. That something is this: one place that
//! knows which owner answers which kind of question, and nothing else. It does
//! not decide what to ask, because the compiler decides that; it does not
//! decide what the answer means, because the owner decides that.
//!
//! Each kind of question goes to exactly one owner, named by a port. Keeping
//! them separate is what stops the catalog from being asked for statistics, or
//! a provider from being consulted about a materialized view - and it means an
//! owner can be exercised on its own, without standing up the others.

use std::sync::Arc;

use async_trait::async_trait;
use novarocks_sql::compiler::{
    CatalogRelationFact, CatalogRelationNeed, MaterializedViewFact, MaterializedViewNeed,
    ProviderReadFact, ProviderReadNeed, SqlCompileError, SqlFactBatch, SqlNeedBatch,
    StatisticsFact, StatisticsNeed, SystemTableSnapshotFact, SystemTableSnapshotNeed,
};

use super::{final_plan::SqlCompletionFactSource, runtime_access::ReadAccessSink};

/// Resolves table and metadata-relation lookups.
#[async_trait]
pub trait CatalogFactPort: Send + Sync {
    async fn resolve_relations(
        &self,
        needs: &[CatalogRelationNeed],
    ) -> Result<Vec<CatalogRelationFact>, SqlCompileError>;
}

/// Reads the statistics the optimizer costs with.
#[async_trait]
pub trait StatisticsFactPort: Send + Sync {
    async fn resolve_statistics(
        &self,
        needs: &[StatisticsNeed],
    ) -> Result<Vec<StatisticsFact>, String>;
}

/// Discovers materialized views a query might be rewritten onto.
///
/// This one is an optimization, so an owner that cannot answer says the view is
/// unavailable rather than failing the query. That is a fact about the view,
/// not an error.
#[async_trait]
pub trait MaterializedViewFactPort: Send + Sync {
    async fn resolve_materialized_views(
        &self,
        needs: &[MaterializedViewNeed],
    ) -> Result<Vec<MaterializedViewFact>, String>;
}

/// Negotiates and freezes each scan's read with its provider.
///
/// Unlike the others, answering here is not a lookup: the owner negotiates what
/// the provider will take on and then commits it, and what it commits is what
/// the plan is built against. A freeze also yields the capability to perform
/// that read, which never travels with the facts - it is deposited in the sink
/// as each read is frozen, so a freeze that fails on its third read of four
/// still leaves the first two accounted for.
#[async_trait]
pub trait ProviderReadFactPort: Send + Sync {
    /// What performing one frozen read requires at runtime.
    type Access: Send;

    async fn resolve_provider_reads(
        &self,
        needs: &[ProviderReadNeed],
        taken: &ReadAccessSink<Self::Access>,
    ) -> Result<Vec<ProviderReadFact>, String>;
}

/// Materializes optimized system-table occurrences in the original statement scope.
#[async_trait]
pub trait SystemTableSnapshotFactPort: Send + Sync {
    async fn resolve_system_tables(
        &self,
        needs: &[SystemTableSnapshotNeed],
    ) -> Result<Vec<SystemTableSnapshotFact>, SqlCompileError>;
}

/// The one place that knows which owner answers which kind of question.
pub struct QueryCompletionFactSource<A> {
    catalog: Arc<dyn CatalogFactPort>,
    statistics: Arc<dyn StatisticsFactPort>,
    materialized_views: Arc<dyn MaterializedViewFactPort>,
    provider_reads: Arc<dyn ProviderReadFactPort<Access = A>>,
    system_tables: Arc<dyn SystemTableSnapshotFactPort>,
}

impl<A> QueryCompletionFactSource<A> {
    pub const fn new(
        catalog: Arc<dyn CatalogFactPort>,
        statistics: Arc<dyn StatisticsFactPort>,
        materialized_views: Arc<dyn MaterializedViewFactPort>,
        provider_reads: Arc<dyn ProviderReadFactPort<Access = A>>,
        system_tables: Arc<dyn SystemTableSnapshotFactPort>,
    ) -> Self {
        Self {
            catalog,
            statistics,
            materialized_views,
            provider_reads,
            system_tables,
        }
    }
}

/// Every answer is checked against the question before it is handed back.
///
/// A compiler that asked about three relations and is handed two facts, or
/// facts about relations it did not ask about, cannot tell which of its needs
/// went unanswered - it would resume with a gap it has no way to name. The
/// count is the cheapest statement of that, and the owners answer in order.
fn expect_one_answer_each<T>(kind: &str, asked: usize, answers: Vec<T>) -> Result<Vec<T>, String> {
    if answers.len() == asked {
        return Ok(answers);
    }
    Err(format!(
        "{kind} owner answered {} of {asked} needs",
        answers.len()
    ))
}

#[async_trait]
impl<A: Send + Sync> SqlCompletionFactSource for QueryCompletionFactSource<A> {
    type Access = A;

    async fn resolve(
        &self,
        needs: &SqlNeedBatch,
        taken: &ReadAccessSink<A>,
    ) -> Result<SqlFactBatch, SqlCompileError> {
        match needs {
            SqlNeedBatch::CatalogRelations(needs) => self
                .catalog
                .resolve_relations(needs)
                .await
                .and_then(|facts| {
                    expect_one_answer_each("catalog", needs.len(), facts).map_err(Into::into)
                })
                .map(|facts| SqlFactBatch::CatalogRelations(facts.into_boxed_slice())),
            SqlNeedBatch::Statistics(needs) => self
                .statistics
                .resolve_statistics(needs)
                .await
                .and_then(|facts| expect_one_answer_each("statistics", needs.len(), facts))
                .map_err(Into::into)
                .map(|facts| SqlFactBatch::Statistics(facts.into_boxed_slice())),
            SqlNeedBatch::MaterializedViews(needs) => self
                .materialized_views
                .resolve_materialized_views(needs)
                .await
                .and_then(|facts| expect_one_answer_each("materialized view", needs.len(), facts))
                .map_err(Into::into)
                .map(|facts| SqlFactBatch::MaterializedViews(facts.into_boxed_slice())),
            SqlNeedBatch::ProviderInputs {
                reads,
                system_tables,
            } => {
                let reads = if reads.is_empty() {
                    Vec::new()
                } else {
                    self.provider_reads
                        .resolve_provider_reads(reads, taken)
                        .await
                        .and_then(|facts| {
                            expect_one_answer_each("provider read", reads.len(), facts)
                        })
                        .map_err(SqlCompileError::from)?
                };
                let system_tables = if system_tables.is_empty() {
                    Vec::new()
                } else {
                    let facts = self
                        .system_tables
                        .resolve_system_tables(system_tables)
                        .await?;
                    expect_one_answer_each("system table", system_tables.len(), facts)
                        .map_err(SqlCompileError::from)?
                };
                Ok(SqlFactBatch::ProviderInputs {
                    reads: reads.into_boxed_slice(),
                    system_tables: system_tables.into_boxed_slice(),
                })
            }
            SqlNeedBatch::ProviderReads(needs) => self
                .provider_reads
                .resolve_provider_reads(needs, taken)
                .await
                .and_then(|facts| expect_one_answer_each("provider read", needs.len(), facts))
                .map_err(Into::into)
                .map(|facts| SqlFactBatch::ProviderReads(facts.into_boxed_slice())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RefusingCatalog(SqlCompileError);

    #[async_trait]
    impl CatalogFactPort for RefusingCatalog {
        async fn resolve_relations(
            &self,
            _: &[CatalogRelationNeed],
        ) -> Result<Vec<CatalogRelationFact>, SqlCompileError> {
            Err(self.0.clone())
        }
    }

    #[tokio::test]
    async fn catalog_refusals_cross_the_aggregate_port_without_losing_the_original_fact() {
        use novarocks_sql::analyze_error::AnalyzeError;
        use novarocks_type_contract::{
            CompileControlError, owned_resources::layout::LayoutResourceError,
        };
        for original in [
            SqlCompileError::Cancelled,
            SqlCompileError::DeadlineExceeded,
            SqlCompileError::ResourceExhausted,
            SqlCompileError::from(CompileControlError::from_domain(
                LayoutResourceError::SourceModel,
            )),
            SqlCompileError::Analyze(AnalyzeError::view_is_recursive("recursive", None)),
        ] {
            let source = QueryCompletionFactSource::new(
                Arc::new(RefusingCatalog(original.clone())),
                Arc::new(Silent),
                Arc::new(Silent),
                Arc::new(Silent),
                Arc::new(Silent),
            );
            let taken = ReadAccessSink::new();
            let actual = source
                .resolve(&SqlNeedBatch::CatalogRelations(Box::default()), &taken)
                .await
                .unwrap_err();
            assert_eq!(actual, original);
            assert!(taken.into_taken().is_empty());
        }
    }

    struct Silent;

    #[async_trait]
    impl CatalogFactPort for Silent {
        async fn resolve_relations(
            &self,
            _: &[CatalogRelationNeed],
        ) -> Result<Vec<CatalogRelationFact>, SqlCompileError> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl StatisticsFactPort for Silent {
        async fn resolve_statistics(
            &self,
            _: &[StatisticsNeed],
        ) -> Result<Vec<StatisticsFact>, String> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl MaterializedViewFactPort for Silent {
        async fn resolve_materialized_views(
            &self,
            _: &[MaterializedViewNeed],
        ) -> Result<Vec<MaterializedViewFact>, String> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl ProviderReadFactPort for Silent {
        type Access = ();

        async fn resolve_provider_reads(
            &self,
            _: &[ProviderReadNeed],
            _: &ReadAccessSink<()>,
        ) -> Result<Vec<ProviderReadFact>, String> {
            Ok(Vec::new())
        }
    }

    #[async_trait]
    impl SystemTableSnapshotFactPort for Silent {
        async fn resolve_system_tables(
            &self,
            _: &[SystemTableSnapshotNeed],
        ) -> Result<Vec<SystemTableSnapshotFact>, SqlCompileError> {
            Ok(Vec::new())
        }
    }

    /// An owner that answers fewer needs than it was asked leaves the compiler
    /// with a gap it cannot name, so the shortfall is refused here instead.
    #[test]
    fn an_owner_that_skips_a_need_is_refused() {
        assert!(expect_one_answer_each("catalog", 2, vec![()]).is_err());
        assert!(expect_one_answer_each("catalog", 1, vec![(), ()]).is_err());
        assert!(expect_one_answer_each("catalog", 2, vec![(), ()]).is_ok());
    }

    /// Each kind of question reaches exactly one owner. Silence from the one
    /// that was asked is still a shortfall, which is what proves the routing
    /// rather than some other owner having answered.
    #[test]
    fn every_need_kind_is_routed_to_one_owner() {
        let source = QueryCompletionFactSource::new(
            Arc::new(Silent),
            Arc::new(Silent),
            Arc::new(Silent),
            Arc::new(Silent),
            Arc::new(Silent),
        );
        let batches = [
            SqlNeedBatch::CatalogRelations(Box::default()),
            SqlNeedBatch::Statistics(Box::default()),
            SqlNeedBatch::MaterializedViews(Box::default()),
            SqlNeedBatch::ProviderReads(Box::default()),
            SqlNeedBatch::ProviderInputs {
                reads: Box::default(),
                system_tables: Box::default(),
            },
        ];
        let taken = ReadAccessSink::new();
        for needs in batches {
            let kind = needs.kind();
            let answered = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("runtime")
                .block_on(source.resolve(&needs, &taken))
                .unwrap_or_else(|error| panic!("{kind:?}: {error}"));
            assert_eq!(answered.kind(), kind);
        }
        // A lookup is not a freeze: routing the declared question kinds took no
        // capability at all.
        assert!(taken.into_taken().is_empty());
    }
}
