-- @order_sensitive=true
-- Test Objective:
-- 1. Validate that first-load INSERT triggers statistics collection.
-- 2. Verify small UPDATE does not trigger new analyze.
-- 3. Verify large UPDATE (>20% rows) triggers full re-analyze.
-- 4. Verify very large UPDATE triggers sample re-analyze.
-- Note: Uses PRIMARY KEY table. Requires shared-nothing mode.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.test_update_stats (
    k2 int,
    k3 varchar(20)
)
PRIMARY KEY (k2)
DISTRIBUTED BY HASH(k2) BUCKETS 1
PROPERTIES ("replication_num" = "1");
CREATE VIEW ${case_db}.statistic_verify AS
select column_name, partition_name, row_count, max, min
from _statistics_.column_statistics
where table_name = '${case_db}.test_update_stats';
CREATE VIEW ${case_db}.analyze_status_verify AS
select `Table`, Columns, Type,
    IF(cast(Id as bigint) > @last_analyze_id, 'new analyze', 'no analyze') as is_new
from information_schema.analyze_status
where `Database` = '${case_db}' and `Table` = 'test_update_stats'
order by cast(Id as bigint) desc limit 1;
CREATE VIEW ${case_db}.last_analyze_id_view AS
select ifnull(max(cast(Id as bigint)), 0) as last_id
from information_schema.analyze_status
where `Database` = '${case_db}' and `Table` = 'test_update_stats';

-- query 2
-- @skip_result_check=true
INSERT INTO test_update_stats SELECT generate_series, 'data' FROM TABLE(generate_series(1, 1000000));

-- query 3
select * from statistic_verify order by column_name;

-- query 4
-- @skip_result_check=true
set @last_analyze_id=(select last_id from last_analyze_id_view);
update test_update_stats set k3 = '1updated' where k2 = 1;

-- query 5
select * from statistic_verify order by column_name;

-- query 6
select * from analyze_status_verify;

-- query 7
-- @skip_result_check=true
update test_update_stats set k3 = '2updated2' where k2 < 1000;

-- query 8
select * from statistic_verify order by column_name;

-- query 9
select * from analyze_status_verify;

-- query 10
-- @skip_result_check=true
set @last_analyze_id=(select last_id from last_analyze_id_view);
update test_update_stats set k3 = '3updated3' where k2 < 200*1000;

-- query 11
select * from statistic_verify order by column_name;

-- query 12
select * from analyze_status_verify;

-- query 13
-- @skip_result_check=true
set @last_analyze_id=(select last_id from last_analyze_id_view);
update test_update_stats set k3 = '4updated4' where k2 < 1000000000;

-- query 14
select * from analyze_status_verify;

-- query 15
-- @skip_result_check=true
drop table test_update_stats;
drop view statistic_verify;
drop view analyze_status_verify;
drop view last_analyze_id_view;
