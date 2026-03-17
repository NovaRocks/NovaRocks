-- Test Objective:
-- 1. Validate histogram statistics collection and cardinality estimation.
-- 2. Verify histogram-based join estimation vs default NDV-based estimation.
-- 3. Test different histogram modes: default, HLL, and sample bucket NDV collection.
-- 4. Test histogram with various MCV sizes and bucket counts.
-- 5. Test histogram on escaped string values and ALL COLUMNS syntax.

-- query 1
-- @skip_result_check=true
CREATE TABLE t1 (
    `k1`  date,
    `k2`  int,
    `k3`  int
)
PROPERTIES ('replication_num' = '1');
CREATE TABLE `t2` LIKE `t1`;
CREATE TABLE `t3` LIKE `t1`;

-- skew data on k1 column
INSERT INTO t1
WITH series AS (
    SELECT g1 FROM TABLE(generate_series(1, 300)) AS t(g1)
)
SELECT date_add('2020-01-01', s1.g1) as k1 , s1.g1, s2.g1
FROM series s1, series s2
WHERE s1.g1 <= s2.g1
ORDER BY s1.g1;

-- t2 is asymmetric with t1
INSERT INTO t2
WITH series AS (
    SELECT g1 FROM TABLE(generate_series(1, 300)) AS t(g1)
)
SELECT date_add('2020-01-01', 300-s1.g1) as k1 , s1.g1, s2.g1
FROM series s1, series s2
WHERE s1.g1 <= s2.g1
ORDER BY s1.g1;

-- t3 is distributed uniformly
INSERT INTO t3
WITH
series AS (
    SELECT g1 FROM TABLE(generate_series(1, 300)) AS t(g1)
)
SELECT date_add('2020-01-01', s1.g1), s1.g1, s2.g1
FROM series s1, series s2;

-- query 2
SELECT k1, count(*)
FROM t1
GROUP BY k1
ORDER BY k1
LIMIT 10;

-- query 3
SELECT k2, count(*)
FROM t1
GROUP BY k2
ORDER BY k2
LIMIT 10;

-- query 4
SELECT k1, count(*)
FROM t2
GROUP BY k1
ORDER BY k1
LIMIT 10;

-- query 5
SELECT k1, count(*)
FROM t3
GROUP BY k1
ORDER BY k1
LIMIT 10;

-- query 6
-- @skip_result_check=true
ANALYZE FULL TABLE t1;
ANALYZE FULL TABLE t2;
ANALYZE FULL TABLE t3;

-- query 7
-- @skip_result_check=true
-- @result_contains=2020-01-02
-- @result_contains=2020-10-27
-- @result_contains=45150
SELECT min,max,row_count,hll_cardinality(ndv) FROM _statistics_.column_statistics WHERE table_name = '${case_db}.t1' and column_name = 'k1';

-- query 8
-- @skip_result_check=true
-- @result_contains=2020-01-01
-- @result_contains=2020-10-26
-- @result_contains=45150
SELECT min,max,row_count,hll_cardinality(ndv) FROM _statistics_.column_statistics WHERE table_name = '${case_db}.t2' and column_name = 'k1';

-- query 9
-- @skip_result_check=true
-- @result_contains=2020-01-02
-- @result_contains=2020-10-27
-- @result_contains=90000
SELECT min,max,row_count,hll_cardinality(ndv) FROM _statistics_.column_statistics WHERE table_name = '${case_db}.t3' and column_name = 'k1';

-- Without histogram, cardinality is not accurate (uniform assumption)
-- query 10
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-01-02";

-- query 11
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-01-10";

-- query 12
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-01-30";

-- query 13
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-01-02";

-- query 14
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-01-10";

-- query 15
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-01-30";

-- With histogram
-- query 16
-- @skip_result_check=true
ANALYZE TABLE t1 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0');
ANALYZE TABLE t2 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0');
ANALYZE TABLE t3 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0');

-- query 17
-- @skip_result_check=true
set enable_stats_to_optimize_skew_join = false;

-- query 18
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k2=120;

-- query 19
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-05-11";

-- query 20
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-06-11";

-- query 21
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-07-11";

-- query 22
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-05-11";

-- query 23
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-06-11";

-- query 24
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-07-11";

-- Expected join count results
-- query 25
SELECT COUNT(*) FROM t1 JOIN t2 USING (k1);

-- query 26
SELECT COUNT(*) FROM t1 JOIN t2 USING (k1) WHERE t1.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 27
SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k1 = n3.k1;

-- query 28
SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k3 = n3.k3;

-- query 29
-- @skip_result_check=true
set cbo_enable_histogram_join_estimation = false;

-- Without histogram join estimation - NDV-based
-- query 30
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1);

-- query 31
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1) WHERE t1.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 32
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k1 = n3.k1;

-- query 33
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k3 = n3.k3;

-- query 34
-- @skip_result_check=true
set cbo_enable_histogram_join_estimation = true;

-- With histogram join estimation
-- query 35
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1);

-- query 36
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1) WHERE t1.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 37
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k1 = n3.k1;

-- query 38
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k3 = n3.k3;

-- More MCVs lead to better estimations
-- query 39
-- @skip_result_check=true
ANALYZE TABLE t1 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '400');
ANALYZE TABLE t2 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '400');
ANALYZE TABLE t3 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '400');

-- query 40
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1);

-- query 41
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1) WHERE t1.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 42
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k1 = n3.k1;

-- query 43
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k3 = n3.k3;

-- More buckets also lead to better estimations
-- query 44
-- @skip_result_check=true
ANALYZE TABLE t1 UPDATE HISTOGRAM ON k1,k2,k3 WITH 256 BUCKETS PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '100');
ANALYZE TABLE t2 UPDATE HISTOGRAM ON k1,k2,k3 WITH 256 BUCKETS PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '100');
ANALYZE TABLE t3 UPDATE HISTOGRAM ON k1,k2,k3 WITH 256 BUCKETS PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0');

-- query 45
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1);

-- query 46
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1) WHERE t1.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 47
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k1 = n3.k1;

-- query 48
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k3 = n3.k3;

-- Escaped string test
-- query 49
-- @skip_result_check=true
create table test_escaped_string (k1 string) properties("replication_num"="1");
insert into test_escaped_string select "aaaaa's";
insert into test_escaped_string select "bbbbbbb";
analyze table test_escaped_string update histogram on k1;

-- query 50
-- @skip_result_check=true
EXPLAIN COSTS select k1 from test_escaped_string;

-- HLL bucket NDV mode
-- query 51
-- @skip_result_check=true
ANALYZE TABLE t1 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_collect_bucket_ndv_mode' = 'hll');
ANALYZE TABLE t2 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_collect_bucket_ndv_mode' = 'hll');
ANALYZE TABLE t3 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0', 'histogram_collect_bucket_ndv_mode' = 'hll');

-- query 52
-- @skip_result_check=true
SELECT buckets,mcv FROM _statistics_.histogram_statistics WHERE table_name = '${case_db}.t1' and column_name = 'k1';

-- query 53
-- @skip_result_check=true
SELECT buckets,mcv FROM _statistics_.histogram_statistics WHERE table_name = '${case_db}.t2' and column_name = 'k1';

-- query 54
-- @skip_result_check=true
SELECT buckets,mcv FROM _statistics_.histogram_statistics WHERE table_name = '${case_db}.t3' and column_name = 'k1';

-- Sample bucket NDV mode
-- query 55
-- @skip_result_check=true
ANALYZE TABLE t1 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_collect_bucket_ndv_mode' = 'sample');
ANALYZE TABLE t2 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', 'histogram_collect_bucket_ndv_mode' = 'sample');
ANALYZE TABLE t3 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0', 'histogram_collect_bucket_ndv_mode' = 'sample');

-- query 56
-- @skip_result_check=true
SELECT buckets,mcv FROM _statistics_.histogram_statistics WHERE table_name = '${case_db}.t1' and column_name = 'k1';

-- query 57
-- @skip_result_check=true
SELECT buckets,mcv FROM _statistics_.histogram_statistics WHERE table_name = '${case_db}.t2' and column_name = 'k1';

-- query 58
-- @skip_result_check=true
SELECT buckets,mcv FROM _statistics_.histogram_statistics WHERE table_name = '${case_db}.t3' and column_name = 'k1';

-- Sample mode EXPLAIN VERBOSE checks
-- query 59
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k2=120;

-- query 60
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-05-11";

-- query 61
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-06-11";

-- query 62
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t1 WHERE k1="2020-07-11";

-- query 63
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-05-11";

-- query 64
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-06-11";

-- query 65
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t2 WHERE k1="2020-07-11";

-- Sample mode EXPLAIN COSTS checks
-- query 66
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1);

-- query 67
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t1 JOIN t2 USING (k1) WHERE t1.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 68
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k1 = n3.k1;

-- query 69
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t1 n1 JOIN t2 n2 ON n1.k1 = n2.k1) JOIN t3 n3 ON n1.k3 = n3.k3;

-- Distinct count estimations with non-dense values
-- query 70
-- @skip_result_check=true
CREATE TABLE `t4` (
    `k1`  date,
    `k2`  int,
    `k3`  int
)
PROPERTIES ('replication_num' = '1');
CREATE TABLE `t5` LIKE `t1`;
CREATE TABLE `t6` LIKE `t1`;

INSERT INTO t4
SELECT * FROM t1
WHERE k2 % 3 != 2;

INSERT INTO t5
SELECT * FROM t2
WHERE k2 % 3 != 2;

INSERT INTO t6
SELECT * FROM t3
WHERE k2 % 3 != 2;

-- query 71
SELECT k1, count(*)
FROM t4
GROUP BY k1
ORDER BY k1
LIMIT 10;

-- query 72
SELECT k2, count(*)
FROM t4
GROUP BY k2
ORDER BY k2
LIMIT 10;

-- query 73
SELECT k1, count(*)
FROM t5
GROUP BY k1
ORDER BY k1
LIMIT 10;

-- query 74
SELECT k1, count(*)
FROM t6
GROUP BY k1
ORDER BY k1
LIMIT 10;

-- Expected results for t4-t6
-- query 75
SELECT COUNT(*) FROM t4 WHERE k1="2020-02-01";

-- query 76
SELECT COUNT(*) FROM t5 WHERE k1="2020-05-12";

-- query 77
SELECT COUNT(*) FROM t5 WHERE k1="2020-07-11";

-- query 78
SELECT COUNT(*) FROM t4 JOIN t5 USING (k1);

-- query 79
SELECT COUNT(*) FROM t4 JOIN t5 USING (k1) WHERE t4.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 80
SELECT COUNT(*) FROM (t4 n1 JOIN t5 n2 ON n1.k1 = n2.k1) JOIN t6 n3 ON n1.k1 = n3.k1;

-- query 81
SELECT COUNT(*) FROM (t4 n1 JOIN t5 n2 ON n1.k1 = n2.k1) JOIN t6 n3 ON n1.k3 = n3.k3;

-- query 82
-- @skip_result_check=true
ANALYZE TABLE t4 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0');
ANALYZE TABLE t5 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0');
ANALYZE TABLE t6 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0');

-- query 83
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t4 WHERE k1="2020-02-01";

-- query 84
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t5 WHERE k1="2020-05-12";

-- query 85
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t5 WHERE k1="2020-07-11";

-- query 86
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t4 JOIN t5 USING (k1);

-- query 87
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t4 JOIN t5 USING (k1) WHERE t4.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 88
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t4 n1 JOIN t5 n2 ON n1.k1 = n2.k1) JOIN t6 n3 ON n1.k1 = n3.k1;

-- query 89
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t4 n1 JOIN t5 n2 ON n1.k1 = n2.k1) JOIN t6 n3 ON n1.k3 = n3.k3;

-- HLL mode for t4-t6
-- query 90
-- @skip_result_check=true
ANALYZE TABLE t4 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0', 'histogram_collect_bucket_ndv_mode' = 'hll');
ANALYZE TABLE t5 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0', 'histogram_collect_bucket_ndv_mode' = 'hll');
ANALYZE TABLE t6 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0', 'histogram_collect_bucket_ndv_mode' = 'hll');

-- query 91
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t4 WHERE k1="2020-02-01";

-- query 92
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t5 WHERE k1="2020-05-12";

-- query 93
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t5 WHERE k1="2020-07-11";

-- query 94
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t4 JOIN t5 USING (k1);

-- query 95
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t4 JOIN t5 USING (k1) WHERE t4.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 96
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t4 n1 JOIN t5 n2 ON n1.k1 = n2.k1) JOIN t6 n3 ON n1.k1 = n3.k1;

-- query 97
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t4 n1 JOIN t5 n2 ON n1.k1 = n2.k1) JOIN t6 n3 ON n1.k3 = n3.k3;

-- Sample mode for t4-t6
-- query 98
-- @skip_result_check=true
ANALYZE TABLE t4 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0', 'histogram_collect_bucket_ndv_mode' = 'sample');
ANALYZE TABLE t5 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0', 'histogram_collect_bucket_ndv_mode' = 'sample');
ANALYZE TABLE t6 UPDATE HISTOGRAM ON k1,k2,k3 PROPERTIES('histogram_sample_ratio' = '1.0', "histogram_mcv_size" = '0', 'histogram_collect_bucket_ndv_mode' = 'sample');

-- query 99
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t4 WHERE k1="2020-02-01";

-- query 100
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t5 WHERE k1="2020-05-12";

-- query 101
-- @skip_result_check=true
EXPLAIN VERBOSE SELECT COUNT(*) FROM t5 WHERE k1="2020-07-11";

-- query 102
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t4 JOIN t5 USING (k1);

-- query 103
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM t4 JOIN t5 USING (k1) WHERE t4.k1 BETWEEN "2020-01-21" AND "2020-01-30";

-- query 104
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t4 n1 JOIN t5 n2 ON n1.k1 = n2.k1) JOIN t6 n3 ON n1.k1 = n3.k1;

-- query 105
-- @skip_result_check=true
EXPLAIN COSTS SELECT COUNT(*) FROM (t4 n1 JOIN t5 n2 ON n1.k1 = n2.k1) JOIN t6 n3 ON n1.k3 = n3.k3;

-- Histogram ALL COLUMNS
-- query 106
-- @skip_result_check=true
CREATE TABLE `t_histogram_all_columns` (
    `k1` int,
    `k2` varchar(20),
    `k3` date
)
PROPERTIES ('replication_num' = '1');

INSERT INTO t_histogram_all_columns VALUES
    (1, 'a', '2020-01-01'),
    (2, 'b', '2020-01-02'),
    (3, 'b', '2020-01-03'),
    (NULL, NULL, NULL);

ANALYZE TABLE t_histogram_all_columns UPDATE HISTOGRAM ON ALL COLUMNS;

-- query 107
-- @skip_result_check=true
EXPLAIN COSTS select * from t_histogram_all_columns;
