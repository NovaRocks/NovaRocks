-- Migrated from dev/test/sql/test_agg_function/R/test_min_max_n
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_min_max_n
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t_without_null` (
  `c_id` INT(11) NOT NULL,
  `c_int` INT(11) NOT NULL,
  `c_tinyint` TINYINT NOT NULL,
  `c_smallint` SMALLINT NOT NULL,
  `c_bigint` BIGINT NOT NULL,
  `c_largeint` LARGEINT NOT NULL,
  `c_float` FLOAT NOT NULL,
  `c_double` DOUBLE NOT NULL,
  `c_char` CHAR(10) NOT NULL,
  `c_varchar` VARCHAR(100) NOT NULL,
  `c_date` DATE NOT NULL,
  `c_datetime` DATETIME NOT NULL,
  `c_decimal` DECIMAL64(9,3) NOT NULL
) ENGINE=OLAP
DUPLICATE KEY(`c_id`)
DISTRIBUTED BY HASH(`c_id`) BUCKETS 10
PROPERTIES (
 "replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE ${case_db};
INSERT INTO `t_without_null` (
  `c_id`, `c_tinyint`, `c_smallint`, `c_int`, `c_bigint`, `c_largeint`, `c_float`, `c_double`, `c_char`, `c_varchar`, `c_date`, `c_datetime`, `c_decimal`)
VALUES
  (1, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (2, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (3, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (4, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),
  (5, 1, 11, 111, 1111, 11111, 1.1, 11.11, 'char1', 'varchar1', '2021-01-01', '2021-01-01 00:00:00', 111.11),

  (11, 2, 22, 222, 2222, 22222, 2.2, 22.22, 'char2', 'varchar2', '2021-01-02', '2021-01-02 00:00:00', 222.22),
  (12, 2, 22, 222, 2222, 22222, 2.2, 22.22, 'char2', 'varchar2', '2021-01-02', '2021-01-02 00:00:00', 222.22),
  (13, 2, 22, 222, 2222, 22222, 2.2, 22.22, 'char2', 'varchar2', '2021-01-02', '2021-01-02 00:00:00', 222.22),

  (21, 3, 33, 333, 3333, 33333, 3.3, 33.33, 'char3', 'varchar3', '2021-01-03', '2021-01-03 00:00:00', 333.33),
  (22, 3, 33, 333, 3333, 33333, 3.3, 33.33, 'char3', 'varchar3', '2021-01-03', '2021-01-03 00:00:00', 333.33),

  (31, 4, 44, 444, 4444, 44444, 4.4, 44.44, 'char4', 'varchar4', '2021-01-04', '2021-01-04 00:00:00', 444.44),

  (91, 10, 100, 1000, 10000, 100000, 1.01, 100.01, 'char10', 'varchar10', '2021-01-10', '2021-01-10 00:00:00', 1000.01);

-- query 4
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='auto')*/ MIN_N(c_tinyint, 3) FROM t_without_null;

-- query 5
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation')*/ MIN_N(c_smallint, 3) FROM t_without_null;

-- query 6
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming')*/ MIN_N(c_int, 3) FROM t_without_null;

-- query 7
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='auto')*/ MIN_N(c_bigint, 3) FROM t_without_null;

-- query 8
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation')*/ MIN_N(c_largeint, 3) FROM t_without_null;

-- query 9
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming')*/ MIN_N(c_float, 3) FROM t_without_null;

-- query 10
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='auto')*/ MIN_N(c_double, 3) FROM t_without_null;

-- query 11
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation')*/ MIN_N(c_char, 3) FROM t_without_null;

-- query 12
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming')*/ MIN_N(c_varchar, 3) FROM t_without_null;

-- query 13
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='auto')*/ MIN_N(c_date, 3) FROM t_without_null;

-- query 14
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation')*/ MIN_N(c_datetime, 3) FROM t_without_null;

-- query 15
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming')*/ MIN_N(c_decimal, 3) FROM t_without_null;

-- query 16
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation')*/ MIN_N(c_tinyint, 1) FROM t_without_null;

-- query 17
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming')*/ MIN_N(c_smallint, 1) FROM t_without_null;

-- query 18
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='auto')*/ MIN_N(c_int, 1) FROM t_without_null;

-- query 19
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming')*/ MIN_N(c_tinyint, 5) FROM t_without_null;

-- query 20
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='auto')*/ MIN_N(c_smallint, 5) FROM t_without_null;

-- query 21
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation')*/ MIN_N(c_int, 5) FROM t_without_null;

-- query 22
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='auto')*/ c_id % 2 AS g, MIN_N(c_int, 2) FROM t_without_null GROUP BY g ORDER BY g;

-- query 23
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_preaggregation')*/ c_id % 3 AS g, MIN_N(c_varchar, 2) FROM t_without_null GROUP BY g ORDER BY g;

-- query 24
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming')*/ c_id % 3 AS g, MIN_N(c_varchar, 2) FROM t_without_null GROUP BY g ORDER BY g;

-- query 25
USE ${case_db};
SELECT /*+ SET_VAR(streaming_preaggregation_mode='force_streaming')*/ c_id % 4 AS g, MIN_N(c_date, 2) FROM t_without_null GROUP BY g ORDER BY g;

-- query 26
USE ${case_db};
SELECT min_n(col1, 3) FROM (VALUES (1)) AS tmp(col1);

-- query 27
USE ${case_db};
SELECT min_n(col1, 3) FROM (VALUES (1),(2),(3),(4),(5),(6)) AS tmp(col1);

-- query 28
USE ${case_db};
SELECT min_n(1, 3);
