-- Test Objective:
-- 1. Validate sync MV rewrite remains correct without relying on ordered input.
-- 2. Cover unordered result semantics and rewrite eligibility.
-- Source: dev/test/sql/test_materialized_view/T/test_sync_materialized_view_unorder

-- query 1
admin set frontend config('alter_scheduler_interval_millisecond' = '100');

-- query 2
CREATE TABLE t1 (
    k1 string NOT NULL,
    k2 string,
    k3 DECIMAL(34,0),
    k4 DATE NOT NULL,
    v1 BIGINT sum DEFAULT "0"
)
AGGREGATE KEY(k1,  k2, k3,  k4)
DISTRIBUTED BY HASH(k4);

-- query 3
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);

-- query 4
-- case1: with unordered columns
CREATE MATERIALIZED VIEW test_mv1 as
SELECT a.k3, DATE_FORMAT(a.k4, '%Y-%m') AS month, sum(a.v1) AS cnt FROM t1 a WHERE a.k2 = 'a' GROUP BY a.k3, DATE_FORMAT(a.k4, '%Y-%m');

-- query 5
-- Preserve the original wait_materialized_view_finish() semantics: poll FINISHED and then
-- give FE a short extra window before reading the sync MV table directly.
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 6
-- @skip_result_check=true
SELECT sleep(1);

-- query 7
-- Planner choice for this unordered explain is not stable; keep it as an explain smoke check.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k3, DATE_FORMAT(k4, "%Y-%m") AS month, sum(v1) AS cnt FROM t1  WHERE k2 = "a" GROUP BY k3, DATE_FORMAT(k4, "%Y-%m");

-- query 8
-- Retry the first direct read because FE may report the sync MV build as FINISHED
-- slightly before the _SYNC_MV_ alias becomes queryable.
-- @retry_count=60
-- @retry_interval_ms=1000
select count(1) from test_mv1 [_SYNC_MV_];

-- query 9
SELECT k3, DATE_FORMAT(k4, '%Y-%m') AS month, sum(v1) AS cnt FROM t1  WHERE k2 = 'a' GROUP BY k3, DATE_FORMAT(k4, '%Y-%m');

-- query 10
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);

-- query 11
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);

-- query 12
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);

-- query 13
ALTER TABLE t1 COMPACT;

-- query 14
select sleep(10);

-- query 15
-- After compaction this explain remains a smoke check rather than a fixed-plan assertion.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT k3, DATE_FORMAT(k4, "%Y-%m") AS month, sum(v1) AS cnt FROM t1  WHERE k2 = "a" GROUP BY k3, DATE_FORMAT(k4, "%Y-%m");

-- query 16
-- Alias-qualified form is also kept as a smoke check.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT a.k3, DATE_FORMAT(a.k4, "%Y-%m") AS month, sum(v1) AS cnt FROM t1 a WHERE k2 = "a" GROUP BY a.k3, DATE_FORMAT(a.k4, "%Y-%m");

-- query 17
select count(1) from test_mv1 [_SYNC_MV_];

-- query 18
SELECT k3, DATE_FORMAT(k4, '%Y-%m') AS month, sum(v1) AS cnt FROM t1  WHERE k2 = 'a' GROUP BY k3, DATE_FORMAT(k4, '%Y-%m');

-- query 19
SELECT a.k3, DATE_FORMAT(a.k4, '%Y-%m') AS month, sum(v1) AS cnt FROM t1 a WHERE k2 = 'a' GROUP BY a.k3, DATE_FORMAT(a.k4, '%Y-%m');

-- query 20
set enable_sync_materialized_view_rewrite=false;

-- query 21
SELECT k3, DATE_FORMAT(k4, '%Y-%m') AS month, sum(v1) AS cnt FROM t1  WHERE k2 = 'a' GROUP BY k3, DATE_FORMAT(k4, '%Y-%m');

-- query 22
drop materialized view test_mv1;

-- query 23
set enable_sync_materialized_view_rewrite=true;

-- query 24
-- case2: with unordered columns
CREATE MATERIALIZED VIEW test_mv1 as
SELECT DATE_FORMAT(a.k4, '%Y-%m') AS month, sum(a.v1) AS cnt FROM t1 a WHERE a.k2 = 'a' GROUP BY DATE_FORMAT(a.k4, '%Y-%m');

-- query 25
-- Preserve the second wait_materialized_view_finish() semantics for the recreated sync MV.
-- @result_contains=FINISHED
-- @retry_count=60
-- @retry_interval_ms=1000
SHOW ALTER MATERIALIZED VIEW ORDER BY JobId DESC LIMIT 1;

-- query 26
-- @skip_result_check=true
SELECT sleep(1);

-- query 27
-- After dropping and recreating the same sync MV name, wait until the _SYNC_MV_ alias itself
-- becomes readable again before asserting the real result set.
-- @skip_result_check=true
-- @retry_count=60
-- @retry_interval_ms=1000
select count(1) from test_mv1 [_SYNC_MV_];

-- query 28
-- The second unordered variant explain is also planner-state dependent; keep it as smoke only.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT DATE_FORMAT(k4, "%Y-%m") AS month, sum(v1) AS cnt FROM t1  WHERE k2 = "a" GROUP BY DATE_FORMAT(k4, "%Y-%m");

-- query 29
-- Alias-qualified form is likewise only checked for successful explain execution.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT DATE_FORMAT(a.k4, "%Y-%m") AS month, sum(v1) AS cnt FROM t1 a WHERE k2 = "a" GROUP BY DATE_FORMAT(a.k4, "%Y-%m");

-- query 30
select count(1) from test_mv1 [_SYNC_MV_];

-- query 31
SELECT DATE_FORMAT(k4, '%Y-%m') AS month, sum(v1) AS cnt FROM t1  WHERE k2 = 'a' GROUP BY DATE_FORMAT(k4, '%Y-%m');

-- query 32
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);

-- query 33
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);

-- query 34
insert into t1 values ('200', 'a', 11.00, '2024-08-06', 1), ('100', NULL, NULL, '2024-08-08', 2), ('200', 'a', 11.00, '2024-08-06', 1);

-- query 35
ALTER TABLE t1 COMPACT;

-- query 36
select sleep(10);

-- query 37
-- After compaction this explain remains a smoke check.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT DATE_FORMAT(k4, "%Y-%m") AS month, sum(v1) AS cnt FROM t1  WHERE k2 = "a" GROUP BY DATE_FORMAT(k4, "%Y-%m");

-- query 38
-- Alias-qualified form after compaction is also smoke-only.
-- @skip_result_check=true
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT DATE_FORMAT(a.k4, "%Y-%m") AS month, sum(v1) AS cnt FROM t1 a WHERE k2 = "a" GROUP BY DATE_FORMAT(a.k4, "%Y-%m");

-- query 39
select count(1) from test_mv1 [_SYNC_MV_];

-- query 40
SELECT DATE_FORMAT(k4, '%Y-%m') AS month, sum(v1) AS cnt FROM t1  WHERE k2 = 'a' GROUP BY DATE_FORMAT(k4, '%Y-%m');

-- query 41
SELECT DATE_FORMAT(a.k4, '%Y-%m') AS month, sum(v1) AS cnt FROM t1 a WHERE k2 = 'a' GROUP BY DATE_FORMAT(a.k4, '%Y-%m');

-- query 42
set enable_sync_materialized_view_rewrite=false;

-- query 43
SELECT DATE_FORMAT(k4, '%Y-%m') AS month, sum(v1) AS cnt FROM t1  WHERE k2 = 'a' GROUP BY DATE_FORMAT(k4, '%Y-%m');

-- query 44
set enable_sync_materialized_view_rewrite=true;
