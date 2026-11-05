-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @order_sensitive=true
-- @tags=runtime_filter,cross_process,distributed
-- Default pre-aggregation TopN pruning preserves every accepted SUM contribution.
-- A separate controlled plan retains the complete-group ordered-filter lifecycle.

CREATE TABLE ${case_db}.rf_topn_source (
    k INT,
    v INT
)
TBLPROPERTIES ("format-version" = "3");

INSERT INTO ${case_db}.rf_topn_source VALUES
    (5, 10), (5, 20), (20, 1), (20, 2),
    (40, 3), (60, 4), (80, 5), (100, 6);

SET enable_global_runtime_filter = false;
SELECT k, SUM(v) AS total
FROM ${case_db}.rf_topn_source
GROUP BY k
ORDER BY k ASC NULLS LAST
LIMIT 2;

SET enable_global_runtime_filter = true;
SET disable_optimizer_rules = '';
-- Default numeric Partial pruning has no admitted direct-partial ordered filter.
-- @explain_contains=LOCAL TOP-N (limit=2, offset=0) [k ASC NULLS LAST]
-- @explain_contains=HASH AGGREGATE (LOCAL, group by: [k])
-- @explain_contains=HASH AGGREGATE (GLOBAL, group by: [k])
-- @explain_not_contains=domain = OrderedBound
-- @explain_not_contains=activation = NonBlockingLive(Batch)
-- @expect_runtime_filter_available=available
SELECT k, SUM(v) AS total
FROM ${case_db}.rf_topn_source
GROUP BY k
ORDER BY k ASC NULLS LAST
LIMIT 2;

-- Keep SplitTopN enabled and disable only the earlier local pruning rewrite.
-- The original ordered-filter producer/consumer and runtime assertions remain.
SET disable_optimizer_rules = 'PushDownTopNToPreAgg';
-- @explain_contains=domain = OrderedBound(key=Int32 ASC NULLS LAST, inclusive=true)
-- @explain_contains=producer binding 1
-- @explain_contains=activation = NonBlockingLive(Batch)
-- @expect_runtime_filter_available=available
-- @expect_runtime_filter_detail=completed-channel
-- @expect_runtime_filter_detail=accepted-producer
-- @expect_runtime_filter_detail=delivered-consumer
-- @expect_runtime_filter_total_at_least=channel_completed_count,1
-- @expect_runtime_filter_total_at_least=producer_accepted_count,1
SELECT k, SUM(v) AS total
FROM ${case_db}.rf_topn_source
GROUP BY k
ORDER BY k ASC NULLS LAST
LIMIT 2;

-- @skip_result_check=true
-- @normalize_explain_timing=true
-- @result_contains=Profile: tasks=
-- @result_contains=AggregateTopNKey
EXPLAIN ANALYZE
SELECT k, SUM(v) AS total
FROM ${case_db}.rf_topn_source
GROUP BY k
ORDER BY k ASC NULLS LAST
LIMIT 2;

SET disable_optimizer_rules = 'SplitTopN';
-- @explain_not_contains=domain = OrderedBound
SELECT k, SUM(v) AS total
FROM ${case_db}.rf_topn_source
GROUP BY k
ORDER BY k ASC NULLS LAST
LIMIT 2;

SET disable_optimizer_rules = '';
