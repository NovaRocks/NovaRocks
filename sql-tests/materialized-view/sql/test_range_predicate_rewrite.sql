-- Test Objective:
-- 1. Validate MV rewrite for range predicates over partition-like dimensions.
-- 2. Cover predicate folding and rewrite eligibility on bounded filters.
-- Source: dev/test/sql/test_materialized_view/T/test_range_predicate_rewrite

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
CREATE TABLE `t0` (
  `date_col` date NOT NULL COMMENT "",
  `id` int(11) NOT NULL COMMENT "",
  `int_col` int(11) NOT NULL COMMENT "",
  `float_col_1` float NOT NULL COMMENT "",
  `float_col_2` float NOT NULL COMMENT "",
  `varchar_col` varchar(255) NOT NULL COMMENT "",
  `tinyint_col` tinyint(4) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`date_col`, `id`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id`)
PROPERTIES (
"replication_num" = "1"
);

-- query 4
CREATE TABLE `t1` (
  `id_1` int(11) NOT NULL COMMENT "",
  `varchar_col_1` varchar(255) NOT NULL COMMENT "",
  `varchar_col_2` varchar(255) NOT NULL COMMENT "",
  `int_col_1` int(11) NOT NULL COMMENT "",
  `tinyint_col_1` tinyint(4) NOT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`id_1`, `varchar_col_1`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`id_1`)
PROPERTIES (
"replication_num" = "1"
);

-- query 5
INSERT INTO `t0` VALUES
('2024-02-01', 1, 100, 10.5, 20.5, 'varchar_value_1', 1),
('2024-03-01', 2, 150, 15.2, 25.7, 'varchar_value_2', 0),
('2024-03-02', 3, 200, 20.1, 30.3, 'varchar_value_3', 1),
('2024-04-02', 4, 250, 25.3, 35.8, 'varchar_value_4', 0),
('2024-04-03', 5, 300, 30.2, 40.2, 'varchar_value_5', 1),
('2024-05-05', 6, 350, 35.5, 45.6, 'varchar_value_6', 0),
('2024-06-01', 7, 400, 40.7, 50.1, 'varchar_value_7', 1),
('2024-06-15', 8, 450, 45.9, 55.4, 'varchar_value_8', 0),
('2025-02-05', 9, 500, 50.4, 60.9, 'varchar_value_9', 1),
('2026-02-05', 10, 550, 55.6, 65.3, 'varchar_value_10', 0),
('2027-02-06', 11, 600, 60.8, 70.2, 'varchar_value_11', 1),
('2028-02-06', 12, 650, 65.2, 75.7, 'varchar_value_12', 0),
('2029-01-07', 13, 700, 70.6, 80.4, 'varchar_value_13', 1),
('2029-01-08', 14, 750, 75.1, 85.6, 'varchar_value_14', 0),
('2029-01-09', 15, 800, 80.3, 90.1, 'varchar_value_15', 1),
('2029-02-08', 16, 850, 85.7, 95.3, 'varchar_value_16', 0),
('2029-02-09', 17, 900, 90.2, 100.7, 'varchar_value_17', 1),
('2029-03-09', 18, 950, 95.5, 105.2, 'varchar_value_18', 0),
('2029-03-10', 19, 1000, 100.1, 110.6, 'varchar_value_19', 1),
('2029-03-11', 20, 1050, 105.4, 115.9, 'varchar_value_20', 0);

-- query 6
INSERT INTO `t1` VALUES
(1, 'varchar_value_1', 'varchar_value_21', 100, 1),
(2, 'varchar_value_2', 'varchar_value_22', 150, 0),
(3, 'varchar_value_3', 'varchar_value_23', 200, 1),
(4, 'varchar_value_4', 'varchar_value_24', 250, 0),
(5, 'varchar_value_5', 'varchar_value_25', 300, 1),
(6, 'varchar_value_6', 'varchar_value_26', 350, 0),
(7, 'varchar_value_7', 'varchar_value_27', 400, 1),
(8, 'varchar_value_8', 'varchar_value_28', 450, 0),
(9, 'varchar_value_9', 'varchar_value_29', 500, 1),
(10, 'varchar_value_10', 'varchar_value_30', 550, 0),
(11, 'varchar_value_11', 'varchar_value_31', 600, 1),
(12, 'varchar_value_12', 'varchar_value_32', 650, 0),
(13, 'varchar_value_13', 'varchar_value_33', 700, 1),
(14, 'varchar_value_14', 'varchar_value_34', 750, 0),
(15, 'varchar_value_15', 'varchar_value_35', 800, 1),
(16, 'varchar_value_16', 'varchar_value_36', 850, 0),
(17, 'varchar_value_17', 'varchar_value_37', 900, 1),
(18, 'varchar_value_18', 'varchar_value_38', 950, 0),
(19, 'varchar_value_19', 'varchar_value_39', 1000, 1),
(20, 'varchar_value_20', 'varchar_value_40', 1050, 0);

-- query 7
set new_planner_optimize_timeout=10000;

-- query 8
set materialized_view_rewrite_mode="force";

-- query 9
create MATERIALIZED VIEW flat_mv
REFRESH DEFERRED MANUAL
PROPERTIES (
"replication_num" = "1"
) as select t0.id, t0.date_col, t0.float_col_1, t0.float_col_2, t0.varchar_col, t0.tinyint_col, t1.varchar_col_1, t1.varchar_col_2, t1.int_col_1, t1.tinyint_col_1 from t0 join t1 on t0.tinyint_col = t1.tinyint_col_1;

-- query 10
refresh materialized view flat_mv with sync mode;

-- query 11
create MATERIALIZED VIEW join_filter_mv
REFRESH DEFERRED MANUAL
PROPERTIES (
"replication_num" = "1"
) as select id, date_col, float_col_1, int_col_1, tinyint_col, tinyint_col_1 from flat_mv where id in (1, 2, 3, 4, 5, 6, 6, 7, 9, 10);

-- query 12
refresh materialized view join_filter_mv with sync mode;

-- query 13
create MATERIALIZED VIEW date_mv
REFRESH DEFERRED MANUAL
PROPERTIES (
"replication_num" = "1"
)   as select tinyint_col, date_col , sum(float_col_1 * int_col_1) as sum_value from join_filter_mv group by tinyint_col, date_col;

-- query 14
refresh materialized view date_mv with sync mode;

-- query 15
create MATERIALIZED VIEW month_mv
REFRESH DEFERRED MANUAL
PROPERTIES (
"replication_num" = "1"
)  as select tinyint_col, date_trunc('month', date_col) as date_col, sum(sum_value) as sum_value from date_mv group by tinyint_col, date_trunc('month', date_col);

-- query 16
refresh materialized view month_mv with sync mode;

-- query 17
create MATERIALIZED VIEW year_mv
REFRESH DEFERRED MANUAL
PROPERTIES (
"replication_num" = "1"
)   as select  tinyint_col, date_trunc('year', date_col) as date_col, sum(sum_value) as sum_value from date_mv group by tinyint_col, date_trunc('year', date_col);

-- query 18
refresh materialized view year_mv with sync mode;

-- query 19
-- @skip_result_check=true
analyze table join_filter_mv with sync mode;

-- query 20
-- @skip_result_check=true
analyze table date_mv with sync mode;

-- query 21
-- @skip_result_check=true
analyze table month_mv with sync mode;

-- query 22
-- @skip_result_check=true
analyze table year_mv with sync mode;

-- query 23
select /*+ set_Var(enable_fine_grained_range_predicate = true, enable_force_rule_based_mv_rewrite = true, nested_mv_rewrite_max_level = 10) */
sum(t0.float_col_1 * t1.int_col_1), t0.tinyint_col from  t0 join t1 on t0.tinyint_col = t1.tinyint_col_1 where t0.id in (1, 2, 3, 4, 5, 6, 6, 7, 9, 10)
and date_col > '2024-02-11' and date_col < '2028-05-14' group by tinyint_col order by 1;

-- query 24
select sum(t0.float_col_1 * t1.int_col_1), t0.tinyint_col from  t0 join t1 on t0.tinyint_col = t1.tinyint_col_1 where t0.id in (1, 2, 3, 4, 5, 6, 6, 7, 9, 10)
and date_col > '2024-02-11' and date_col < '2028-05-14' group by tinyint_col order by 1;

-- query 25
-- @result_contains_any=date_mv
-- @result_contains_any=month_mv
-- @result_contains_any=year_mv
SET enable_materialized_view_rewrite = true;
EXPLAIN select /*+ set_Var(enable_fine_grained_range_predicate = true, enable_force_rule_based_mv_rewrite = true, nested_mv_rewrite_max_level = 10) */ sum(t0.float_col_1 * t1.int_col_1), t0.tinyint_col from  t0 join t1 on t0.tinyint_col = t1.tinyint_col_1 where t0.id in (1, 2, 3, 4, 5, 6, 6, 7, 9, 10) and date_col > '2024-02-11' and date_col < '2028-05-14' group by tinyint_col;

-- query 26
drop database db_${uuid0} force;
