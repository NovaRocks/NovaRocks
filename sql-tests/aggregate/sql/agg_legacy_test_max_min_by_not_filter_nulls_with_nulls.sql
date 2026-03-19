-- Migrated from: dev/test/sql/test_max_min_by_not_filter_nulls_with_nulls/T/test_max_min_by_not_filter_nulls_with_nulls
-- Test Objective:
-- 1. Preserve legacy max_by/min_by aggregate fingerprints on nullable inputs.
-- 2. Cover grouped aggregates, DISTINCT mix, and window execution across agg stages and streaming modes.

-- query 1
-- @skip_result_check=true
USE ${case_db};
DROP TABLE IF EXISTS t0;
CREATE TABLE IF NOT EXISTS t0
(
  c0 INT NULL,
  c1 INT NULL,
  c2 DECIMAL128(7, 2) NULL,
  c3 VARCHAR(10) NULL
) ENGINE=OLAP
DUPLICATE KEY(`c0`, `c1`, `c2`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c0`, `c1`) BUCKETS 32
PROPERTIES(
  "replication_num" = "1",
  "in_memory" = "false",
  "storage_format" = "default"
);
INSERT INTO t0
(c0, c1, c2, c3)
VALUES
('4', '8', NULL, NULL),
(NULL, NULL, '55157.78', NULL),
('9', '6', NULL, 'foo'),
('8', NULL, '8.20', 'foobar'),
('7', NULL, NULL, NULL),
('2', NULL, '55157.78', NULL),
('6', NULL, '29660.05', 'foo'),
('4', NULL, NULL, NULL),
(NULL, NULL, NULL, 'foobar'),
(NULL, '6', NULL, NULL);

-- query 2
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 3
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 4
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 5
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 6
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, (count(DISTINCT c3)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 7
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 8
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c2)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 9
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 10
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) a, min_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) b from t0) as t;

-- query 11
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='1') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) b from t0) as t;

-- query 12
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 13
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 14
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 15
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 16
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, (count(DISTINCT c3)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 17
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 18
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c2)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 19
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 20
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) a, min_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) b from t0) as t;

-- query 21
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) b from t0) as t;

-- query 22
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 23
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 24
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 25
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 26
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, (count(DISTINCT c3)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 27
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 28
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c2)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 29
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 30
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) a, min_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) b from t0) as t;

-- query 31
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='2', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) b from t0) as t;

-- query 32
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 33
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 34
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 35
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 36
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, (count(DISTINCT c3)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 37
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 38
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c2)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 39
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 40
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) a, min_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) b from t0) as t;

-- query 41
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) b from t0) as t;

-- query 42
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 43
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 44
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 45
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 46
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, (count(DISTINCT c3)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 47
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 48
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c2)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 49
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 50
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) a, min_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) b from t0) as t;

-- query 51
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='3', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) b from t0) as t;

-- query 52
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 53
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 54
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 55
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 56
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, (count(DISTINCT c3)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 57
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 58
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c2)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 59
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 60
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) a, min_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) b from t0) as t;

-- query 61
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_preaggregation') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) b from t0) as t;

-- query 62
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 63
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 64
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 65
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 66
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, (count(DISTINCT c3)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0 group by c2) as t;

-- query 67
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0 group by c0) as t;

-- query 68
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c2)) as __c_0, max_by(c0, coalesce(c0,0) * 1000 + c1) a, min_by(c0, coalesce(c0,0) * 1000 + c1) b from t0) as t;

-- query 69
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(__c_0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select (count(DISTINCT c1)) as __c_0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) b from t0) as t;

-- query 70
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c2,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c2, max_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) a, min_by(c0, coalesce(c0,0) * 1000 + c1) over(partition by c2) b from t0) as t;

-- query 71
USE ${case_db};
select /*+ SET_VAR(new_planner_agg_stage='4', streaming_preaggregation_mode='force_streaming') */
  (sum(murmur_hash3_32(ifnull(c0,0)) + murmur_hash3_32(ifnull(a,0)) + murmur_hash3_32(ifnull(b,0)))) as fingerprint
from (select c0, max_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) a, min_by(c2, concat(coalesce(c2,'NULL'), c3)) over(partition by c1) b from t0) as t;
