-- Test Objective:
-- 1. Validate force refresh strategy scheduling and final refresh status.
-- 2. Cover async refresh orchestration with explicit force semantics.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_refresh_strategy_with_force

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
create table user_tags (time date, user_id int, user_name varchar(20), tag_id int)
partition by date_trunc('day', time)
distributed by hash(time) buckets 3
properties('replication_num' = '1');

-- query 4
insert into user_tags values('2023-04-13', 1, 'a', 1);

-- query 5
insert into user_tags values('2023-04-13', 1, 'b', 2);

-- query 6
insert into user_tags values('2023-04-14', 2, 'e', 5);

-- query 7
insert into user_tags values('2023-04-14', 3, 'e', 6);

-- query 8
create materialized view user_tags_mv1  distributed by hash(user_id)
partition by date_trunc('day', time)
properties(
    'partition_refresh_number' = '-1',
    'partition_refresh_strategy' = 'force'
)
refresh deferred manual
as select user_id, time, count(tag_id) from user_tags group by user_id, time;

-- query 9
refresh materialized view user_tags_mv1 with sync mode;

-- query 10
select * from user_tags_mv1 order by user_id;

-- query 11
-- @result_contains=ready
-- @retry_count=180
-- @retry_interval_ms=1000
SELECT IF(
    COUNT(*) > 0
    AND SUM(CASE WHEN a.STATE IN ('SUCCESS', 'MERGED', 'SKIPPED') THEN 0 ELSE 1 END) = 0,
    'ready',
    'pending'
) AS status
FROM information_schema.task_runs a
JOIN information_schema.materialized_views b
  ON a.task_name = b.task_name
WHERE b.table_name = 'user_tags_mv1'
  AND a.`database` = 'db_${uuid0}';

-- query 12
-- @skip_result_check=true
set @task_name = (
    SELECT TASK_NAME
    FROM information_schema.materialized_views
    WHERE TABLE_SCHEMA = 'db_${uuid0}'
      AND TABLE_NAME = 'user_tags_mv1'
);

-- query 13
-- @skip_result_check=true
set @refresh_count1 = (
    SELECT count(1)
    FROM information_schema.task_runs
    WHERE TASK_NAME = @task_name
);

-- query 14
refresh materialized view user_tags_mv1 with sync mode;

-- query 15
select * from user_tags_mv1 order by user_id;

-- query 16
-- @result_contains=ready
-- @retry_count=180
-- @retry_interval_ms=1000
SELECT IF(
    COUNT(*) > 0
    AND SUM(CASE WHEN a.STATE IN ('SUCCESS', 'MERGED', 'SKIPPED') THEN 0 ELSE 1 END) = 0,
    'ready',
    'pending'
) AS status
FROM information_schema.task_runs a
JOIN information_schema.materialized_views b
  ON a.task_name = b.task_name
WHERE b.table_name = 'user_tags_mv1'
  AND a.`database` = 'db_${uuid0}';

-- query 17
-- @skip_result_check=true
set @refresh_count2 = (
    SELECT count(1)
    FROM information_schema.task_runs
    WHERE TASK_NAME = @task_name
);

-- query 18
SELECT @refresh_count1 = @refresh_count2;

-- query 19
drop database db_${uuid0} force;
