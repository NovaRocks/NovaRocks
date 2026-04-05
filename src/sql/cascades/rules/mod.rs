//! Rule registration for the Cascades optimizer.

pub(crate) mod implement;
pub(crate) mod join_associativity;
pub(crate) mod join_commutativity;

use super::rule::Rule;

/// Returns all implementation rules (logical -> physical).
pub(crate) fn all_implementation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(implement::ScanToPhysical),
        Box::new(implement::FilterToPhysical),
        Box::new(implement::ProjectToPhysical),
        Box::new(implement::JoinToHashJoin),
        Box::new(implement::JoinToNestLoop),
        Box::new(implement::AggToHashAgg),
        Box::new(implement::SortToPhysical),
        Box::new(implement::LimitToPhysical),
        Box::new(implement::WindowToPhysical),
        Box::new(implement::CTEAnchorToPhysical),
        Box::new(implement::CTEProduceToPhysical),
        Box::new(implement::CTEConsumeToPhysical),
        Box::new(implement::RepeatToPhysical),
        Box::new(implement::UnionToPhysical),
        Box::new(implement::IntersectToPhysical),
        Box::new(implement::ExceptToPhysical),
        Box::new(implement::ValuesToPhysical),
        Box::new(implement::GenerateSeriesToPhysical),
        Box::new(implement::SubqueryAliasToPhysical),
    ]
}

/// Returns all transformation rules (logical -> logical).
pub(crate) fn all_transformation_rules() -> Vec<Box<dyn Rule>> {
    vec![
        Box::new(join_commutativity::JoinCommutativity),
        Box::new(join_associativity::JoinAssociativity),
    ]
}
