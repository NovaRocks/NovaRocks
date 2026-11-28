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
-- @tags=optimizer,join,residual,runtime_filter
-- With equality first, only matching non-NULL pairs need the checked CAST.
-- The independent oracles are k=2 when the bad source row cannot match,
-- a typed error when it matches k=1, and an empty result with an empty side.
-- With CAST first, even pairs that fail the later equality need the CAST.
-- The probe's TopN first picks k=1. Moving the runtime filter before TopN
-- would promote k=2 into its result and incorrectly produce a match.

CREATE TABLE ${case_db}.i1_join_left (k BIGINT, s VARCHAR) WITH (format_version = 3);

CREATE TABLE ${case_db}.i1_join_right (k BIGINT, v BIGINT) WITH (format_version = 3);

CREATE TABLE ${case_db}.i1_join_match_bad (k BIGINT, v BIGINT) WITH (format_version = 3);

CREATE TABLE ${case_db}.i1_join_double (d DOUBLE) WITH (format_version = 3);

INSERT INTO ${case_db}.i1_join_left VALUES ROW(1, 'bad'), ROW(2, '7'), ROW(NULL, 'bad');

INSERT INTO ${case_db}.i1_join_right VALUES ROW(2, 7), ROW(NULL, 0);

INSERT INTO ${case_db}.i1_join_match_bad VALUES ROW(1, 7);

INSERT INTO ${case_db}.i1_join_double VALUES ROW(2e0), ROW(NULL);

-- @skip_result_check=true
ANALYZE ${case_db}.i1_join_left;

-- @skip_result_check=true
ANALYZE ${case_db}.i1_join_right;

SET SESSION disable_optimizer_rules = 'JoinCommutativity';

SET SESSION global_runtime_filter_probe_min_selectivity = 0.0;

SET SESSION enable_global_runtime_filter = true;

-- @explain_contains=HASH JOIN
-- @explain_contains=other:
-- @explain_contains=consumer binding
SELECT l.k
FROM ${case_db}.i1_join_left l
JOIN ${case_db}.i1_join_right r
  ON l.k = r.k AND CAST(l.s AS BIGINT) = r.v
ORDER BY l.k;

-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT l.k
FROM ${case_db}.i1_join_left l
JOIN ${case_db}.i1_join_match_bad r
  ON l.k = r.k AND CAST(l.s AS BIGINT) = r.v
ORDER BY l.k;

SELECT count(*) AS cnt
FROM ${case_db}.i1_join_left l
JOIN (SELECT k, v FROM ${case_db}.i1_join_right WHERE k < 0) r
  ON l.k = r.k AND CAST(l.s AS BIGINT) = r.v;

SELECT p.k
FROM ${case_db}.i1_join_right p
WHERE EXISTS (
    SELECT 1 FROM ${case_db}.i1_join_left l
    WHERE p.k = l.k AND CAST(l.s AS BIGINT) = p.v
)
ORDER BY p.k;

SELECT p.k
FROM ${case_db}.i1_join_right p
WHERE NOT EXISTS (
    SELECT 1 FROM ${case_db}.i1_join_left l
    WHERE p.k = l.k AND CAST(l.s AS BIGINT) = p.v
)
ORDER BY p.k;

-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT p.k
FROM ${case_db}.i1_join_match_bad p
WHERE EXISTS (
    SELECT 1 FROM ${case_db}.i1_join_left l
    WHERE p.k = l.k AND CAST(l.s AS BIGINT) = p.v
)
ORDER BY p.k;

-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT p.k
FROM ${case_db}.i1_join_match_bad p
WHERE NOT EXISTS (
    SELECT 1 FROM ${case_db}.i1_join_left l
    WHERE p.k = l.k AND CAST(l.s AS BIGINT) = p.v
)
ORDER BY p.k;

-- @explain_contains=HASH JOIN
SELECT l.k, r.d
FROM ${case_db}.i1_join_left l
JOIN ${case_db}.i1_join_double r ON CAST(l.k AS DOUBLE) = r.d
ORDER BY l.k;

-- @explain_contains=HASH JOIN
-- @explain_contains=other:
-- @explain_contains=TOP-N
-- @explain_contains=consumer binding
SELECT count(*) AS cnt
FROM (
    SELECT k, s
    FROM ${case_db}.i1_join_left
    ORDER BY k NULLS LAST
    LIMIT 1
) p
JOIN ${case_db}.i1_join_right r
  ON p.k = r.k AND CAST(p.s AS BIGINT) = r.v;

SET SESSION enable_global_runtime_filter = false;

-- @explain_contains=HASH JOIN
-- @explain_not_contains=RUNTIME FILTER GRAPH
SELECT l.k
FROM ${case_db}.i1_join_left l
JOIN ${case_db}.i1_join_right r
  ON l.k = r.k AND CAST(l.s AS BIGINT) = r.v
ORDER BY l.k;

-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT l.k
FROM ${case_db}.i1_join_left l
JOIN ${case_db}.i1_join_match_bad r
  ON l.k = r.k AND CAST(l.s AS BIGINT) = r.v
ORDER BY l.k;

-- @explain_contains=HASH JOIN
-- @explain_contains=other:
-- @explain_not_contains=RUNTIME FILTER GRAPH
SELECT count(*) AS cnt
FROM (
    SELECT k, s
    FROM ${case_db}.i1_join_left
    ORDER BY k NULLS LAST
    LIMIT 1
) p
JOIN ${case_db}.i1_join_right r
  ON p.k = r.k AND CAST(p.s AS BIGINT) = r.v;

SET SESSION disable_optimizer_rules = 'JoinCommutativity,JoinToHashJoin';

-- @explain_contains=NEST LOOP JOIN
-- @explain_not_contains=HASH JOIN
SELECT l.k
FROM ${case_db}.i1_join_left l
JOIN ${case_db}.i1_join_right r
  ON l.k = r.k AND CAST(l.s AS BIGINT) = r.v
ORDER BY l.k;

SET SESSION enable_global_runtime_filter = true;

SET SESSION disable_optimizer_rules = '';

-- @result_contains=NEST LOOP JOIN
-- @result_not_contains=HASH JOIN
-- @result_not_contains=RUNTIME FILTER GRAPH
EXPLAIN WITH SESSION explain_detail = 'VERBOSE'
SELECT l.k
FROM ${case_db}.i1_join_left l
JOIN ${case_db}.i1_join_right r
  ON CAST(l.s AS BIGINT) = r.v AND l.k = r.k
ORDER BY l.k;

-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT l.k
FROM ${case_db}.i1_join_left l
JOIN ${case_db}.i1_join_right r
  ON CAST(l.s AS BIGINT) = r.v AND l.k = r.k
ORDER BY l.k;

-- The only candidate has a NULL key comparison after the checked CAST.
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT l.k
FROM (SELECT k, s FROM ${case_db}.i1_join_left WHERE k IS NULL) l
JOIN (SELECT k, v FROM ${case_db}.i1_join_right WHERE k = 2) r
  ON CAST(l.s AS BIGINT) = r.v AND l.k = r.k;

-- The producer can still use the equality. Its RF must not discard the
-- unmatched probe row before the input relation's own checked filter.
-- @result_contains=HASH JOIN
-- @result_contains=consumer binding
EXPLAIN WITH SESSION explain_detail = 'VERBOSE', disable_optimizer_rules = 'JoinCommutativity'
SELECT l.k
FROM (SELECT k FROM ${case_db}.i1_join_left WHERE CAST(s AS BIGINT) > 0) l
JOIN ${case_db}.i1_join_right r ON l.k = r.k;

-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT l.k
FROM (SELECT k FROM ${case_db}.i1_join_left WHERE CAST(s AS BIGINT) > 0) l
JOIN ${case_db}.i1_join_right r ON l.k = r.k;
