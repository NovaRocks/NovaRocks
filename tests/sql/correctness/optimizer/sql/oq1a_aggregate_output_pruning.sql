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

-- name: oq1a_aggregate_output_pruning
DROP TABLE IF EXISTS ${case_db}.oq1a_t;
CREATE TABLE ${case_db}.oq1a_t (
    k INT,
    a BIGINT,
    b BIGINT
);
INSERT INTO ${case_db}.oq1a_t VALUES
    (1, 10, 100),
    (1, 20, 200),
    (2, 30, 300);

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE
-- @explain_contains=sum
-- @explain_not_contains=count(b)
EXPLAIN VERBOSE SELECT sum(s) AS s
FROM (
    SELECT k, sum(a) AS s, count(b) AS unused_count
    FROM ${case_db}.oq1a_t
    GROUP BY k
) q;

SELECT sum(s) AS s
FROM (
    SELECT k, sum(a) AS s, count(b) AS unused_count
    FROM ${case_db}.oq1a_t
    GROUP BY k
) q;

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE
-- @explain_contains=count
EXPLAIN VERBOSE SELECT count() AS c
FROM ${case_db}.oq1a_t;

SELECT count() AS c
FROM ${case_db}.oq1a_t;

-- @skip_result_check=true
-- @explain_contains=HASH AGGREGATE
-- @explain_contains=sum
EXPLAIN VERBOSE SELECT sum_a
FROM (
    SELECT k, sum(a) AS sum_a
    FROM ${case_db}.oq1a_t
    GROUP BY k
    HAVING sum(a) > 0
) q
ORDER BY sum_a;

SELECT sum_a
FROM (
    SELECT k, sum(a) AS sum_a
    FROM ${case_db}.oq1a_t
    GROUP BY k
    HAVING sum(a) > 0
) q
ORDER BY sum_a;
