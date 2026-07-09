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

-- @tags=optimizer,topn,compactness
-- Test Objective:
-- Lock in TopN pushdown behavior for UNION ALL while preserving fail-closed
-- guards for Aggregate and Join.
-- The SetOp positive case uses enough branch rows for the extractor to choose
-- the branch-pruning candidate, so the golden shows pushed branch TopN nodes.
DROP TABLE IF EXISTS ${case_db}.topn_compactness_left_src;
DROP TABLE IF EXISTS ${case_db}.topn_compactness_right_src;
CREATE TABLE ${case_db}.topn_compactness_left_src (id INT, score INT);
CREATE TABLE ${case_db}.topn_compactness_right_src (id INT, score INT);
INSERT INTO ${case_db}.topn_compactness_left_src
    SELECT generate_series, generate_series
    FROM TABLE(generate_series(1, 100000));
INSERT INTO ${case_db}.topn_compactness_right_src
    SELECT generate_series + 100000, 200000 - generate_series
    FROM TABLE(generate_series(1, 100000));

-- @skip_result_check=true
-- @result_contains=UNION ALL
-- @result_contains=MERGING-EXCHANGE
-- @result_contains=LOCAL TOP-N (limit=2, offset=0)
-- @result_contains=score DESC
-- @result_contains=id ASC
-- @result_contains=topn_compactness_left_src
-- @result_contains=topn_compactness_right_src
EXPLAIN VERBOSE
SELECT id, score
FROM (
    SELECT id, score
    FROM ${case_db}.topn_compactness_left_src
    UNION ALL
    SELECT id, score
    FROM ${case_db}.topn_compactness_right_src
) u
ORDER BY score DESC, id ASC
LIMIT 2;

-- @skip_result_check=true
-- @result_contains=UNION ALL
-- @result_contains=HASH AGGREGATE (GLOBAL
-- @result_contains=HASH AGGREGATE (LOCAL
-- @result_contains=LOCAL TOP-N (limit=1, offset=0)
-- @result_contains=sum(score) DESC
-- @result_not_contains=LOCAL TOP-N (limit=2, offset=0)
EXPLAIN VERBOSE
SELECT id, SUM(score) AS total_score
FROM (
    SELECT id, score
    FROM ${case_db}.topn_compactness_left_src
    UNION ALL
    SELECT id, score
    FROM ${case_db}.topn_compactness_right_src
) u
GROUP BY id
ORDER BY total_score DESC
LIMIT 1;

-- @skip_result_check=true
-- @result_contains=MERGING-EXCHANGE
-- @result_contains=LOCAL TOP-N (limit=1, offset=0)
-- @result_contains=HASH JOIN (
-- @result_contains=INNER, eq:
EXPLAIN VERBOSE
SELECT l.id, l.score, r.score AS rhs_score
FROM ${case_db}.topn_compactness_left_src l
INNER JOIN ${case_db}.topn_compactness_right_src r ON l.id = r.id
ORDER BY l.score DESC
LIMIT 1;
