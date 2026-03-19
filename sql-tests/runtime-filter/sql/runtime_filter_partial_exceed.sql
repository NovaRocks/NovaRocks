-- Test Objective:
-- 1. Validate correctness of left semi join when GRF partially exceeds the size limit.
-- 2. With global_runtime_filter_build_max_size=320001, some filters exceed the limit
--    and fall back, while others don't. The query result must remain stable across
--    repeated executions.
-- 3. Run the same query 17 times to detect any non-determinism.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.row_util_base (
  k1 BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(k1)
DISTRIBUTED BY HASH(k1) BUCKETS 32
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.row_util_base SELECT generate_series FROM TABLE(generate_series(0, 10000 - 1));
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base;
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base;
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base;
INSERT INTO ${case_db}.row_util_base SELECT * FROM ${case_db}.row_util_base;

CREATE TABLE ${case_db}.row_util (
  idx BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(idx)
DISTRIBUTED BY HASH(idx) BUCKETS 32
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.row_util SELECT row_number() over() AS idx FROM ${case_db}.row_util_base;

CREATE TABLE ${case_db}.t1 (
  idx BIGINT NULL,
  c1  BIGINT NULL,
  c2  BIGINT NULL
) ENGINE=OLAP
DUPLICATE KEY(idx)
DISTRIBUTED BY HASH(idx) BUCKETS 32
PROPERTIES ("replication_num" = "1");

INSERT INTO ${case_db}.t1 SELECT idx, 1, 1 FROM ${case_db}.row_util;
INSERT INTO ${case_db}.t1 SELECT idx, 2, 2 FROM ${case_db}.row_util WHERE idx < 160000 - 10;
INSERT INTO ${case_db}.t1 SELECT idx, 3, 3 FROM ${case_db}.row_util WHERE idx < 160000 - 10;
INSERT INTO ${case_db}.t1 SELECT idx, 4, 4 FROM ${case_db}.row_util WHERE idx < 160000 - 10;
INSERT INTO ${case_db}.t1 SELECT idx, 5, 5 FROM ${case_db}.row_util WHERE idx < 160000 - 10;
INSERT INTO ${case_db}.t1 SELECT idx, 6, 6 FROM ${case_db}.row_util WHERE idx < 160000 - 10;
INSERT INTO ${case_db}.t1 SELECT idx, idx, idx FROM ${case_db}.row_util WHERE idx > 4;

-- @skip_result_check=true
ANALYZE TABLE ${case_db}.t1;

set pipeline_dop = 100;
set enable_join_runtime_bitset_filter = false;
set global_runtime_filter_wait_timeout = 10000;
set runtime_filter_scan_wait_time = 10000;
set global_runtime_filter_build_max_size = 320001;

-- query 2
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 3
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 4
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 5
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 6
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 7
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 8
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 9
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 10
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 11
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 12
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 13
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 14
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 15
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 16
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 17
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
-- query 18
select count(1) from ${case_db}.t1 left semi join [shuffle] ${case_db}.t1 t2 on t1.c1 = t2.c1 and t2.c2 < 10;
