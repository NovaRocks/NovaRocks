-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

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
TBLPROPERTIES ("format-version" = "3");

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
CREATE TABLE trc5c_snapshot_task AS
SELECT (
    SELECT TASK_NAME
    FROM information_schema.materialized_views
    WHERE TABLE_SCHEMA = 'db_${uuid0}'
      AND TABLE_NAME = 'user_tags_mv1'
) AS task_name;

-- query 13
-- @skip_result_check=true
CREATE TABLE trc5c_snapshot_before_count AS
SELECT count(1) AS value
FROM information_schema.task_runs
WHERE TASK_NAME = (SELECT task_name FROM trc5c_snapshot_task);

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
CREATE TABLE trc5c_snapshot_after_count AS
SELECT count(1) AS value
FROM information_schema.task_runs
WHERE TASK_NAME = (SELECT task_name FROM trc5c_snapshot_task);

-- query 18
SELECT a.value = b.value AS `(@refresh_count1) = (@refresh_count2)`
FROM trc5c_snapshot_before_count a
CROSS JOIN trc5c_snapshot_after_count b;

-- query 19
DROP TABLE trc5c_snapshot_after_count;
DROP TABLE trc5c_snapshot_before_count;
DROP TABLE trc5c_snapshot_task;
drop database db_${uuid0} force;
