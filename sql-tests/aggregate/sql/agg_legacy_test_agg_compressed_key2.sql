-- Migrated from dev/test/sql/test_agg/R/test_agg_compressed_key2
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_agg_compressed_key2 @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t3 (
    c_2_0 LARGEINT NOT NULL,
    c_2_1 LARGEINT NOT NULL,
    c_2_2 LARGEINT NOT NULL,
    c_2_3 LARGEINT NOT NULL,
    c_2_4 LARGEINT NOT NULL,
    c_2_5 LARGEINT NOT NULL,
    c_2_6 LARGEINT NOT NULL,
    c_2_7 LARGEINT NOT NULL,
    c_2_8 LARGEINT NOT NULL,
    c_2_9 LARGEINT NOT NULL,
    c_2_10 LARGEINT NOT NULL,
    c_2_11 LARGEINT NOT NULL,
    c_2_12 LARGEINT NOT NULL,
    c_2_13 LARGEINT NOT NULL,
    c_2_14 LARGEINT NOT NULL,
    c_2_15 LARGEINT NOT NULL
) DUPLICATE KEY (c_2_0) DISTRIBUTED BY HASH (c_2_0) properties("replication_num" = "1");

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t3 values (0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

-- query 4
-- @skip_result_check=true
USE ${case_db};
insert into t3 values (128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128, 128);

-- query 5
-- @skip_result_check=true
USE ${case_db};
insert into t3 values (1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1);

-- query 6
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_0 FROM t3;

-- query 7
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_1 FROM t3;

-- query 8
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_2 FROM t3;

-- query 9
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_3 FROM t3;

-- query 10
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_4 FROM t3;

-- query 11
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_5 FROM t3;

-- query 12
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_6 FROM t3;

-- query 13
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_7 FROM t3;

-- query 14
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_8 FROM t3;

-- query 15
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_9 FROM t3;

-- query 16
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_10 FROM t3;

-- query 17
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_11 FROM t3;

-- query 18
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_12 FROM t3;

-- query 19
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_13 FROM t3;

-- query 20
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_14 FROM t3;

-- query 21
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_15 FROM t3;

-- query 22
USE ${case_db};
select distinct c_2_0, c_2_1, c_2_2, c_2_3, c_2_4, c_2_5, c_2_6, c_2_7, c_2_8, c_2_9, c_2_10, c_2_11, c_2_12, c_2_13, c_2_14, c_2_15 from t3 order by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16;

-- query 23
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t4 (
    c_2_0 LARGEINT NOT NULL
) DUPLICATE KEY (c_2_0)
DISTRIBUTED BY HASH (c_2_0)
properties("replication_num" = "1");

-- query 24
-- @skip_result_check=true
USE ${case_db};
insert into t4 values (1024), (-2139922094);

-- query 25
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=min-max stats
-- @skip_result_check=true
USE ${case_db};
EXPLAIN VERBOSE SELECT DISTINCT c_2_0 FROM t4;

-- query 26
-- @expect_error=Column 't4' cannot be resolved.
USE ${case_db};
select distinct c_2_0 from t4 order by t4;
