-- @order_sensitive=true
-- Test Objective:
-- 1. Validate predicate column tracking (predicate, group_by, join usage).
-- 2. Verify analyze predicate columns only collects stats for columns used in predicates.
-- 3. Verify cumulative column usage tracking across different query patterns.
-- Note: This case modifies global FE config; run with -j 1.

-- query 1
-- @skip_result_check=true
admin set frontend config ('enable_statistic_collect_on_first_load'='false');
admin set frontend config ('enable_statistic_collect'='false');
CREATE TABLE ${case_db}.t1(c1 int, c2 bigint, c3 string, c4 string)
PROPERTIES('replication_num'='1');
INSERT INTO t1 VALUES (1, 1, 's1', 's1');
INSERT INTO t1 VALUES (2, 2, 's2', 's2');
INSERT INTO t1 VALUES (3, 3, 's3', 's3');

-- query 2
select table_name, column_name, usage from information_schema.column_stats_usage where table_database = '${case_db}' and table_name = 't1' order by column_name;

-- query 3
-- @skip_result_check=true
analyze table t1 predicate columns;

-- query 4
-- @skip_result_check=true
select count(*) from t1 where c1 = 1;
select count(*) from t1 where c1 = 2;
select count(*) from t1 where c1 > 2;

-- query 5
-- @skip_result_check=true
admin execute on frontend 'import com.starrocks.statistic.columns.PredicateColumnsMgr; PredicateColumnsMgr.getInstance().persist();';

-- query 6
select table_name, column_name, usage from information_schema.column_stats_usage where table_database = '${case_db}' and table_name = 't1' order by column_name;

-- query 7
-- @skip_result_check=true
analyze table t1 predicate columns with sync mode;

-- query 8
select `table`, array_join(array_sort(split(`columns`, ',')), ',') from information_schema.analyze_status where `database`='${case_db}' order by Id;

-- query 9
-- @skip_result_check=true
select c1, count(c1), sum(c2), max(c3), min(c4) from t1 group by c1 order by c1;
select c2, count(c1), max(c3), min(c4) from t1 where c1 > 2 group by c2 order by c2;

-- query 10
-- @skip_result_check=true
admin execute on frontend 'import com.starrocks.statistic.columns.PredicateColumnsMgr; PredicateColumnsMgr.getInstance().persist();';

-- query 11
select table_name, column_name, usage from information_schema.column_stats_usage where table_database = '${case_db}' and table_name = 't1' order by column_name;

-- query 12
-- @skip_result_check=true
analyze table t1 predicate columns;

-- query 13
select `table`, array_join(array_sort(split(`columns`, ',')), ',') from information_schema.analyze_status where `database`='${case_db}' order by Id;

-- query 14
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2(c5 int, c6 bigint, c7 string, c8 string)
PROPERTIES('replication_num'='1');
INSERT INTO t2 SELECT * FROM t1;

-- query 15
-- @skip_result_check=true
select * from t1 join t2 on t1.c1 = t2.c5 order by t1.c1, t2.c5;
select * from t1 join t2 on t1.c1 = t2.c5 where t1.c1 > 2 order by t1.c1, t2.c5;
select * from t1 join t2 on t1.c1 = t2.c5 where t1.c2 > 2 and t1.c3 = 's2' order by t1.c1, t2.c5;

-- query 16
-- @skip_result_check=true
admin execute on frontend 'import com.starrocks.statistic.columns.PredicateColumnsMgr; PredicateColumnsMgr.getInstance().persist();';

-- query 17
select table_name, column_name, usage from information_schema.column_stats_usage where table_database = '${case_db}' and table_name = 't1' order by column_name;

-- query 18
select table_name, column_name, usage from information_schema.column_stats_usage where table_database = '${case_db}' and table_name = 't2' order by column_name;

-- query 19
-- @skip_result_check=true
analyze table t1 predicate columns;

-- query 20
select `table`, array_join(array_sort(split(`columns`, ',')), ',') from information_schema.analyze_status where `database`='${case_db}' order by Id;

-- query 21
-- @skip_result_check=true
admin set frontend config ('enable_statistic_collect_on_first_load'='true');
admin set frontend config ('enable_statistic_collect'='false');
