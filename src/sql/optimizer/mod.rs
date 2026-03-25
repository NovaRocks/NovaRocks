use crate::sql::analyzer::{AnalyzedQuery, AnalyzedStatement, QuerySource};

#[derive(Clone, Debug)]
pub(crate) enum OptimizedStatement {
    Query(RelQueryPlan),
    Passthrough(AnalyzedStatement),
}

#[derive(Clone, Debug)]
pub(crate) struct RelQueryPlan {
    pub(crate) source: QuerySource,
    pub(crate) bound: crate::sql::analyzer::BoundQuery,
    pub(crate) order_by: Vec<crate::sql::ast::OrderByExpr>,
}

pub(crate) fn optimize_statement(stmt: AnalyzedStatement) -> Result<OptimizedStatement, String> {
    match stmt {
        AnalyzedStatement::Query(query) => Ok(OptimizedStatement::Query(optimize_query(query))),
        other => Ok(OptimizedStatement::Passthrough(other)),
    }
}

fn optimize_query(query: AnalyzedQuery) -> RelQueryPlan {
    RelQueryPlan {
        source: query.source,
        bound: query.bound,
        order_by: query.order_by,
    }
}
