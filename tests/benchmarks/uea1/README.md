# UEA-1 performance protocol

This directory contains the frozen workload manifest and comparison tools for
UEA-1. The system-test runner must use the `performance` launch profile and an
explicit manifest path. The three scenarios are intentionally excluded from
the default fault-scenario run.

`workloads.json` describes the formal 1FE+3BE run. Its mixed-product table and
materialized view are private fixtures that the formal runner creates before
measurement. Do not interpret the short controller smoke as performance
evidence. `workloads-smoke.json` exercises controller plumbing with bounded
synthetic SQL and does not represent the product workload.

`compare.py` derives the allowed relative noise from two baseline A/A sample
sets using pooled median and MAD. It rejects zero-valued positive metrics and
noise above five percent. `build_feedback.py` records command duration and
peak process-tree RSS for reproducible development-feedback samples.
