use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use crate::thrift::{metrics, runtime_profile};
use novarocks::runtime::mem_tracker::MemTracker;
use novarocks::runtime::profile::{
    CounterAggregateType, CounterMergeType, CounterMinMaxType, CounterStrategy, ProfileCounter,
    ProfileNode, ProfileUnit, Profiler, RuntimeProfileTree, merge_pipeline_profiles,
};
use novarocks::runtime::runtime_filter_observability::{QueryKey, RuntimeFilterLifecycleRegistry};

use super::FragmentReportRegistration;

pub(crate) fn build_profile_tree(
    registration: &FragmentReportRegistration,
    include_runtime_filters: bool,
) -> Option<runtime_profile::TRuntimeProfileTree> {
    let merged = build_merged_profile(registration, include_runtime_filters)?;
    let mut tree = profile_to_thrift_tree(&merged);
    normalize_profile_tree(&mut tree);
    Some(tree)
}

fn build_merged_profile(
    registration: &FragmentReportRegistration,
    include_runtime_filters: bool,
) -> Option<Profiler> {
    if !registration.enable_profile() {
        return None;
    }
    let profiler = registration.profiler()?;
    let merged = merge_pipeline_profiles(profiler);
    if include_runtime_filters {
        RuntimeFilterLifecycleRegistry::global().export_to_profile(
            QueryKey::from_hi_lo(
                registration.query_id().high(),
                registration.query_id().low(),
            ),
            &merged,
        );
    }
    add_memory_counters(
        &merged,
        registration.fragment_mem_tracker(),
        registration.query_mem_tracker(),
    );
    Some(merged)
}

fn add_memory_counters(
    profile: &Profiler,
    fragment: Option<&Arc<MemTracker>>,
    query: Option<&Arc<MemTracker>>,
) {
    if let Some(tracker) = fragment {
        profile.counter_set(
            "InstancePeakMemoryUsage",
            ProfileUnit::Bytes,
            tracker.peak(),
        );
        profile.counter_set(
            "InstanceAllocatedMemoryUsage",
            ProfileUnit::Bytes,
            tracker.allocated(),
        );
        profile.counter_set(
            "InstanceDeallocatedMemoryUsage",
            ProfileUnit::Bytes,
            tracker.deallocated(),
        );
    }
    if let Some(tracker) = query {
        profile.counter_set("QueryPeakMemoryUsage", ProfileUnit::Bytes, tracker.peak());
    }
}

pub(crate) fn profile_to_thrift_tree(profiler: &Profiler) -> runtime_profile::TRuntimeProfileTree {
    runtime_profile_tree_to_thrift(&profiler.to_native_tree())
        .expect("RuntimeProfile should always produce thrift-compatible profile units")
}

fn runtime_profile_tree_to_thrift(
    tree: &RuntimeProfileTree,
) -> Result<runtime_profile::TRuntimeProfileTree, String> {
    let mut nodes = Vec::new();
    native_profile_node_to_thrift(&tree.root, &mut nodes)?;
    Ok(runtime_profile::TRuntimeProfileTree::new(nodes))
}

fn native_profile_node_to_thrift(
    node: &ProfileNode,
    out: &mut Vec<runtime_profile::TRuntimeProfileNode>,
) -> Result<(), String> {
    let mut counters = node
        .counters
        .iter()
        .map(native_counter_to_thrift)
        .collect::<Result<Vec<_>, _>>()?;
    counters.sort_by(|left, right| left.name.cmp(&right.name));
    let mut child_counters_map = BTreeMap::<String, BTreeSet<String>>::new();
    for counter in &node.counters {
        child_counters_map
            .entry(counter.parent_name.clone())
            .or_default()
            .insert(counter.name.clone());
    }
    let info_strings = node.info_strings.clone();
    let info_strings_display_order = info_strings.keys().cloned().collect();
    out.push(runtime_profile::TRuntimeProfileNode::new(
        node.name.clone(),
        node.children.len() as i32,
        counters,
        i64::from(node.node_id),
        false,
        info_strings,
        info_strings_display_order,
        child_counters_map,
        None,
    ));
    for child in &node.children {
        native_profile_node_to_thrift(child, out)?;
    }
    Ok(())
}

fn native_counter_to_thrift(counter: &ProfileCounter) -> Result<runtime_profile::TCounter, String> {
    Ok(runtime_profile::TCounter::new(
        counter.name.clone(),
        profile_unit_to_thrift(counter.unit),
        counter.value,
        Some(counter_strategy_to_thrift(counter.strategy)),
        counter.min_value,
        counter.max_value,
    ))
}

fn profile_unit_to_thrift(unit: ProfileUnit) -> metrics::TUnit {
    match unit {
        ProfileUnit::Unit => metrics::TUnit::UNIT,
        ProfileUnit::CpuTicks => metrics::TUnit::CPU_TICKS,
        ProfileUnit::Bytes => metrics::TUnit::BYTES,
        ProfileUnit::TimeNs => metrics::TUnit::TIME_NS,
        ProfileUnit::TimeMs => metrics::TUnit::TIME_MS,
        ProfileUnit::TimeS => metrics::TUnit::TIME_S,
        ProfileUnit::None => metrics::TUnit::NONE,
    }
}

fn counter_strategy_to_thrift(strategy: CounterStrategy) -> runtime_profile::TCounterStrategy {
    let aggregate_type = match strategy.aggregate_type() {
        CounterAggregateType::Sum => runtime_profile::TCounterAggregateType::SUM,
        CounterAggregateType::Avg => runtime_profile::TCounterAggregateType::AVG,
        CounterAggregateType::SumAvg => runtime_profile::TCounterAggregateType::SUM_AVG,
        CounterAggregateType::AvgSum => runtime_profile::TCounterAggregateType::AVG_SUM,
    };
    let merge_type = match strategy.merge_type() {
        CounterMergeType::MergeAll => runtime_profile::TCounterMergeType::MERGE_ALL,
        CounterMergeType::SkipAll => runtime_profile::TCounterMergeType::SKIP_ALL,
        CounterMergeType::SkipFirstMerge => runtime_profile::TCounterMergeType::SKIP_FIRST_MERGE,
        CounterMergeType::SkipSecondMerge => runtime_profile::TCounterMergeType::SKIP_SECOND_MERGE,
    };
    let min_max_type = strategy.min_max_type().map(|value| match value {
        CounterMinMaxType::MinMaxAll => runtime_profile::TCounterMinMaxType::MIN_MAX_ALL,
        CounterMinMaxType::SkipAll => runtime_profile::TCounterMinMaxType::SKIP_ALL,
    });
    runtime_profile::TCounterStrategy::new(
        aggregate_type,
        merge_type,
        strategy.display_threshold(),
        min_max_type,
    )
}

fn normalize_profile_tree(tree: &mut runtime_profile::TRuntimeProfileTree) {
    let mut stack: Vec<(String, i32, bool)> = Vec::new();
    for node in &mut tree.nodes {
        while stack
            .last()
            .is_some_and(|(_, remaining, _)| *remaining <= 0)
        {
            stack.pop();
        }
        if let Some((_, remaining, _)) = stack.last_mut() {
            *remaining -= 1;
        }
        let name = node.name.as_str();
        let mut skip_warn = stack.last().is_some_and(|(_, _, skip)| *skip);
        skip_warn |= name.starts_with("MemTracker") || name == "RuntimeFilters";
        if name.starts_with("Pipeline (id=") || name.starts_with("PipelineDriver (id=") {
            if node.num_children > 0 {
                stack.push((node.name.clone(), node.num_children, skip_warn));
            }
            continue;
        }
        if name == "RESULT_SINK" {
            node.name = "RESULT_SINK (plan_node_id=-1)".to_string();
        }
        if node.name.contains("(id=") && !node.name.contains("plan_node_id=") {
            node.name = node.name.replace("(id=", "(plan_node_id=");
        }
        if node.num_children > 0 {
            stack.push((node.name.clone(), node.num_children, skip_warn));
        }
    }
}
