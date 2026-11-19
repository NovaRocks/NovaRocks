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

-- @tags=optimizer,topn,aggregate,session_rule_disable
-- Test Objective:
-- Default pre-aggregate pruning keeps Partial grouped states distinct from
-- complete Global groups. Preserve that shape and the admitted ordered-bound
-- publication when only the pruning rule is disabled, without unstable IDs.

DROP TABLE IF EXISTS ${case_db}.t_pushdown_topn_preagg;
CREATE TABLE ${case_db}.t_pushdown_topn_preagg (city INT, sales INT);
INSERT INTO ${case_db}.t_pushdown_topn_preagg
    SELECT generate_series % 1000, generate_series
    FROM TABLE(generate_series(1, 100000));
ANALYZE ${case_db}.t_pushdown_topn_preagg;

SET SESSION disable_optimizer_rules = '';

-- Default pruning adds a lower budget over Partial grouped states. Its
-- Int32/UNION input has no admitted Partial scan filter, and the Global probe
-- trace does not cross that lower TopN. Final ranking still uses Global groups.
-- @skip_result_check=true
-- @result_contains=LOCAL TOP-N (limit=10, offset=0) [city ASC NULLS FIRST]
-- @result_contains=HASH AGGREGATE (GLOBAL, group by: [city])
-- @result_contains=HASH AGGREGATE (LOCAL, group by: [city])
-- @result_not_contains=domain = OrderedBound
-- @result_not_contains=activation = NonBlockingLive(Batch)
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT city, SUM(sales)
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY city
LIMIT 10;

SET SESSION disable_optimizer_rules = 'PushDownTopNToPreAgg';

-- Disable only pre-aggregate pruning. SplitTopN still ranks complete Global
-- groups and retains the original Int32 ordered-bound publication contract.
-- @skip_result_check=true
-- @result_contains=LOCAL TOP-N
-- @result_contains=HASH AGGREGATE (GLOBAL, group by: [city])
-- @result_contains=domain = OrderedBound(key=Int32 ASC NULLS FIRST, inclusive=true)
-- @result_contains=activation = NonBlockingLive(Batch)
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT city, SUM(sales)
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY city
LIMIT 10;

SET SESSION disable_optimizer_rules = 'SplitTopN';

-- A single Rows budget still completes the answer, without a split producer.
-- @skip_result_check=true
-- @result_contains=TOP-N
-- @result_contains=HASH AGGREGATE (
-- @result_not_contains=LOCAL TOP-N
-- @result_not_contains=domain = OrderedBound
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT city, SUM(sales)
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY city
LIMIT 10;

SET SESSION disable_optimizer_rules = '';

-- Ranking an aggregate result cannot publish a bound on its input group key.
-- @skip_result_check=true
-- @result_contains=sum(sales) DESC NULLS LAST
-- @result_not_contains=domain = OrderedBound
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT city, SUM(sales) AS s
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city
ORDER BY s DESC
LIMIT 10;

-- A proper subset of grouping keys cannot prove a distinct-key bound.
-- @skip_result_check=true
-- @result_contains=group by: [city, sales]
-- @result_not_contains=domain = OrderedBound
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT city, sales, SUM(sales) AS s
FROM (
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
    UNION ALL
    SELECT city, sales FROM ${case_db}.t_pushdown_topn_preagg
) o
GROUP BY city, sales
ORDER BY city
LIMIT 10;

SET SESSION disable_optimizer_rules = '';
