-- Test Objective:
-- 1. Validate SHOW CREATE/SHOW MATERIALIZED VIEWS and information_schema metadata output.
-- 2. Cover refresh-state visibility and current NovaRocks DDL serialization.
-- Source: dev/test/sql/test_materialized_view/T/test_show_materialized_view

-- query 1
set enable_rewrite_bitmap_union_to_bitamp_agg = false;

-- query 2
set new_planner_optimize_timeout=10000;

-- query 3
set enable_evaluate_schema_scan_rule=false;

-- query 4
create database test_show_materialized_view;

-- query 5
use test_show_materialized_view;

-- query 6
create table user_tags (time date, user_id int, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');

-- query 7
create materialized view user_tags_mv1
distributed by hash(user_id)
REFRESH DEFERRED MANUAL
as select user_id, bitmap_union(to_bitmap(tag_id)) from user_tags group by user_id;

-- query 8
-- @skip_result_check=true
-- @result_contains=CREATE MATERIALIZED VIEW `user_tags_mv1`
-- @result_contains=COMMENT "MATERIALIZED_VIEW"
-- @result_contains=REFRESH DEFERRED MANUAL
-- @result_contains="replication_num" = "1"
-- @result_contains="warehouse" = "default_warehouse"
-- @result_not_contains="session.insert_timeout" = "3600"
-- @result_not_contains="mv_rewrite_staleness_second" = "3600"
show create materialized view user_tags_mv1;

-- query 9
-- @skip_result_check=true
-- @result_contains=CREATE MATERIALIZED VIEW `user_tags_mv1`
-- @result_contains=COMMENT "MATERIALIZED_VIEW"
-- @result_contains=REFRESH DEFERRED MANUAL
-- @result_contains="replication_num" = "1"
-- @result_contains="warehouse" = "default_warehouse"
-- @result_not_contains="session.insert_timeout" = "3600"
-- @result_not_contains="mv_rewrite_staleness_second" = "3600"
show create table user_tags_mv1;

-- query 10
alter materialized view user_tags_mv1 set ("session.insert_timeout" = "3600");

-- query 11
alter materialized view user_tags_mv1 set ("mv_rewrite_staleness_second" = "3600");

-- query 12
-- @skip_result_check=true
-- @result_contains=CREATE MATERIALIZED VIEW `user_tags_mv1`
-- @result_contains=COMMENT "MATERIALIZED_VIEW"
-- @result_contains=REFRESH DEFERRED MANUAL
-- @result_contains="session.insert_timeout" = "3600"
-- @result_contains="mv_rewrite_staleness_second" = "3600"
-- @result_contains="warehouse" = "default_warehouse"
show create materialized view user_tags_mv1;

-- query 13
-- @skip_result_check=true
-- @result_contains=CREATE MATERIALIZED VIEW `user_tags_mv1`
-- @result_contains=COMMENT "MATERIALIZED_VIEW"
-- @result_contains=REFRESH DEFERRED MANUAL
-- @result_contains="session.insert_timeout" = "3600"
-- @result_contains="mv_rewrite_staleness_second" = "3600"
-- @result_contains="warehouse" = "default_warehouse"
show create table user_tags_mv1;

-- query 14
-- information_schema.materialized_views
refresh materialized view user_tags_mv1 with sync mode;

-- query 15
-- @result_contains=done
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF(
    LAST_REFRESH_STATE IN ('SUCCESS', 'MERGED', 'SKIPPED', 'FAILED'),
    'done',
    'pending'
) AS mv_wait_state
FROM information_schema.materialized_views
WHERE TABLE_NAME = 'user_tags_mv1'
  AND TABLE_SCHEMA = 'test_show_materialized_view'
ORDER BY LAST_REFRESH_START_TIME DESC
LIMIT 1;

-- query 16
select
    TABLE_NAME,
    LAST_REFRESH_STATE,
    LAST_REFRESH_ERROR_CODE,
    IS_ACTIVE,
    INACTIVE_REASON
from information_schema.materialized_views where table_name = 'user_tags_mv1' and TABLE_SCHEMA = 'test_show_materialized_view';

-- query 17
set @last_refresh_time = (
    select max(last_refresh_start_time)
    from information_schema.materialized_views where table_name = 'user_tags_mv1' and TABLE_SCHEMA = 'test_show_materialized_view'
);

-- query 18
-- multiple refresh tasks
refresh materialized view user_tags_mv1 force with sync mode;

-- query 19
-- @result_contains=done
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF(
    LAST_REFRESH_STATE IN ('SUCCESS', 'MERGED', 'SKIPPED', 'FAILED'),
    'done',
    'pending'
) AS mv_wait_state
FROM information_schema.materialized_views
WHERE TABLE_NAME = 'user_tags_mv1'
  AND TABLE_SCHEMA = 'test_show_materialized_view'
ORDER BY LAST_REFRESH_START_TIME DESC
LIMIT 1;

-- query 20
select
    TABLE_NAME,
    LAST_REFRESH_STATE,
    LAST_REFRESH_ERROR_CODE,
    IS_ACTIVE,
    INACTIVE_REASON
from information_schema.materialized_views where table_name = 'user_tags_mv1' and TABLE_SCHEMA = 'test_show_materialized_view';

-- query 21
set @this_refresh_time = (
    select max(last_refresh_start_time)
    from information_schema.materialized_views where table_name = 'user_tags_mv1' and TABLE_SCHEMA = 'test_show_materialized_view'
);

-- query 22
select if(@last_refresh_time != @this_refresh_time,
    'refreshed', concat('no refresh after ', @last_refresh_time));

-- query 23
select
    TABLE_NAME,
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
from information_schema.materialized_views
where TABLE_SCHEMA = 'test_show_materialized_view' and table_name = 'user_tags_mv1'
ORDER BY LAST_REFRESH_START_TIME DESC;

-- query 24
select
    TABLE_NAME,
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
from information_schema.materialized_views
where TABLE_SCHEMA = 'test_show_materialized_view' and table_name like 'user_tags_mv1'
ORDER BY LAST_REFRESH_START_TIME DESC;

-- query 25
select
    TABLE_NAME,
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
from information_schema.materialized_views
where TABLE_SCHEMA = 'test_show_materialized_view' and table_name like '%user_tags_mv1%'
ORDER BY LAST_REFRESH_START_TIME DESC;

-- query 26
select
    TABLE_NAME,
    IS_ACTIVE,
    IFNULL(NULLIF(INACTIVE_REASON, ''), '<empty>') AS INACTIVE_REASON
from information_schema.materialized_views
where TABLE_SCHEMA = 'test_show_materialized_view' and table_name like '%%'
ORDER BY LAST_REFRESH_START_TIME DESC;

-- query 27
select TABLE_NAME, LAST_REFRESH_ERROR_CODE, IS_ACTIVE, INACTIVE_REASON from information_schema.materialized_views where TABLE_SCHEMA = 'test_show_materialized_view' and table_name like '%bad_name%' ORDER BY LAST_REFRESH_START_TIME DESC;

-- query 28
-- @skip_result_check=true
SELECT * FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'test_show_materialized_view' and table_name = 'user_tags_mv1';

-- query 29
-- @skip_result_check=true
SELECT * FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'test_show_materialized_view' and table_name like 'user_tags_mv1';

-- query 30
-- @skip_result_check=true
SELECT * FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'test_show_materialized_view' and table_name like '%user_tags_mv1%';

-- query 31
-- @skip_result_check=true
SELECT * FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'test_show_materialized_view' and table_name like '%%';

-- query 32
-- @skip_result_check=true
SELECT * FROM information_schema.materialized_views WHERE TABLE_SCHEMA = 'test_show_materialized_view' and table_name like '%bad_name%';

-- query 33
-- @skip_result_check=true
show materialized views from test_show_materialized_view where name like 'user_tags_mv1';

-- query 34
-- @skip_result_check=true
show materialized views from test_show_materialized_view where name like '%user_tags_mv1%';

-- query 35
-- @skip_result_check=true
show materialized views from test_show_materialized_view where name = 'user_tags_mv1';

-- query 36
-- @skip_result_check=true
show materialized views where name like 'user_tags_mv1';

-- query 37
-- @skip_result_check=true
show materialized views where name like '%user_tags_mv1%';

-- query 38
-- @skip_result_check=true
show materialized views where name = 'user_tags_mv1';

-- query 39
drop database test_show_materialized_view;
