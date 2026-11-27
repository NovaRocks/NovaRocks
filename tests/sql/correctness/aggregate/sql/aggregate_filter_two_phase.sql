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

-- query 1
-- @skip_result_check=true
-- Each partition has one selected and one rejected row for the same group.
CREATE TABLE ${case_db}.filter_two_phase (
    part_id INTEGER, g INTEGER, x INTEGER, keep_row BOOLEAN
) WITH (format_version = 3, partitioning = ARRAY['part_id']);
INSERT INTO ${case_db}.filter_two_phase VALUES
    ROW(1, 7, 1, true), ROW(1, 7, 101, false),
    ROW(2, 7, 2, true), ROW(2, 7, 102, false),
    ROW(3, 7, 3, true), ROW(3, 7, 103, false),
    ROW(4, 7, 4, true), ROW(4, 7, 104, false),
    ROW(5, 7, 5, true), ROW(5, 7, 105, false),
    ROW(6, 7, 6, true), ROW(6, 7, 106, false);

-- query 2
-- @order_sensitive=false
SELECT "partition", record_count
FROM ${case_db}."filter_two_phase$files"
WHERE content = 0;

-- query 3
-- @result_contains=HASH AGGREGATE (LOCAL,
-- @result_contains=HASH AGGREGATE (GLOBAL,
-- @result_contains=HASH EXCHANGE
-- @result_contains=SCAN ${case_db}.filter_two_phase
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT g, count(*) AS n, sum(x) FILTER (WHERE keep_row) AS kept,
       sum(x) FILTER (WHERE NOT keep_row) AS discarded
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 4
-- @be_log_be_count_at_least=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK,2
SELECT g, count(*) AS n, sum(x) FILTER (WHERE keep_row) AS kept,
       sum(x) FILTER (WHERE NOT keep_row) AS discarded
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 5
-- @explain_contains=HASH AGGREGATE (SINGLE,
-- @explain_not_contains=HASH AGGREGATE (LOCAL,
WITH SESSION disable_optimizer_rules = 'SplitAggregateRule'
SELECT g, count(*) AS n, sum(x) FILTER (WHERE keep_row) AS kept,
       sum(x) FILTER (WHERE NOT keep_row) AS discarded
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 6
-- @explain_contains=HASH AGGREGATE (SINGLE,
-- @explain_contains=GATHER
-- @explain_not_contains=HASH AGGREGATE (DISTINCT_GLOBAL,
SELECT g, count(DISTINCT x) FILTER (WHERE keep_row) AS dc, sum(x) AS total
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 7
-- @result_contains=HASH AGGREGATE (LOCAL,
-- @result_contains=HASH AGGREGATE (GLOBAL,
-- @result_contains=HASH EXCHANGE
-- @result_contains=SCAN ${case_db}.filter_two_phase
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT g, approx_distinct(x) FILTER (WHERE keep_row) AS kept,
       approx_distinct(x, 2.3E-2) FILTER (WHERE keep_row) AS explicit_kept,
       approx_distinct(x % 2 = 0) FILTER (WHERE keep_row) AS kept_boolean,
       approx_distinct(x) FILTER (WHERE false) AS empty_count
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 8
-- @be_log_be_count_at_least=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK,2
SELECT g, approx_distinct(x) FILTER (WHERE keep_row) AS kept,
       approx_distinct(x, 2.3E-2) FILTER (WHERE keep_row) AS explicit_kept,
       approx_distinct(x % 2 = 0) FILTER (WHERE keep_row) AS kept_boolean,
       approx_distinct(x) FILTER (WHERE false) AS empty_count
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 9
-- @explain_contains=HASH AGGREGATE (SINGLE,
-- @explain_not_contains=HASH AGGREGATE (LOCAL,
WITH SESSION disable_optimizer_rules = 'SplitAggregateRule', pipeline_dop = 1
SELECT g, approx_distinct(x) FILTER (WHERE keep_row) AS kept,
       approx_distinct(x, 2.3E-2) FILTER (WHERE keep_row) AS explicit_kept,
       approx_distinct(x % 2 = 0) FILTER (WHERE keep_row) AS kept_boolean,
       approx_distinct(x) FILTER (WHERE false) AS empty_count
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 10
SELECT g, count(DISTINCT x) FILTER (WHERE keep_row) AS kept,
       count(DISTINCT (x % 2 = 0)) FILTER (WHERE keep_row) AS kept_boolean,
       count(DISTINCT x) FILTER (WHERE false) AS empty_count
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 11
-- @explain_contains=HASH AGGREGATE (SINGLE,
-- @explain_not_contains=HASH AGGREGATE (LOCAL,
SELECT g, approx_distinct(x, IF(keep_row, DOUBLE '0.023', DOUBLE '-1'))
           FILTER (WHERE keep_row) AS dynamic_kept
FROM ${case_db}.filter_two_phase GROUP BY g;

-- query 12
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.filter_two_phase;

-- query 13
-- @skip_result_check=true
CREATE TABLE ${case_db}.approx_distinct_vectors (
    part_id INTEGER, g INTEGER, x BIGINT
) WITH (format_version = 3, partitioning = ARRAY['part_id']);
INSERT INTO ${case_db}.approx_distinct_vectors
SELECT CAST((x - 1) % 6 AS INTEGER), 7, x
FROM TABLE(sequence(1, 8192)) AS vector(x);

-- query 14
-- @explain_contains=HASH AGGREGATE (LOCAL,
-- @explain_contains=HASH AGGREGATE (GLOBAL,
-- @be_log_be_count_at_least=NOVAROCKS_CONNECTOR_SCAN_FIRST_CHUNK,2
SELECT g, approx_distinct(IF(x <= 4, x), 2.6E-1) AS crossing_low,
       approx_distinct(IF(x <= 251, x)) AS sparse_default,
       approx_distinct(IF(x <= 252, x)) AS crossing_default,
       approx_distinct(IF(x <= 252, x), 2.3E-2) AS crossing_explicit,
       approx_distinct(IF(x <= 8191, x), 4.0625E-3) AS sparse_high,
       approx_distinct(x, 2.6E-1) AS dense_low,
       approx_distinct(x) AS dense_default,
       approx_distinct(x, 4.0625E-3) AS crossing_high
FROM ${case_db}.approx_distinct_vectors GROUP BY g;

-- query 15
-- @explain_contains=HASH AGGREGATE (SINGLE,
-- @explain_not_contains=HASH AGGREGATE (LOCAL,
WITH SESSION disable_optimizer_rules = 'SplitAggregateRule', pipeline_dop = 1
SELECT g, approx_distinct(IF(x <= 4, x), 2.6E-1) AS crossing_low,
       approx_distinct(IF(x <= 251, x)) AS sparse_default,
       approx_distinct(IF(x <= 252, x)) AS crossing_default,
       approx_distinct(IF(x <= 252, x), 2.3E-2) AS crossing_explicit,
       approx_distinct(IF(x <= 8191, x), 4.0625E-3) AS sparse_high,
       approx_distinct(x, 2.6E-1) AS dense_low,
       approx_distinct(x) AS dense_default,
       approx_distinct(x, 4.0625E-3) AS crossing_high
FROM ${case_db}.approx_distinct_vectors GROUP BY g;

-- query 16
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.approx_distinct_vectors;
