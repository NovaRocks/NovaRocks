-- Test Objective:
-- 1. Validate automatic refresh triggers multiple task runs after base-table changes.
-- 2. Cover refresh-count visibility through information_schema without manual refresh.
-- Source: dev/test/sql/test_materialized_view/T/test_auto_refresh

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
CREATE TABLE t1 (
    __time datetime,
    c1 int
)
PARTITION BY date_trunc('DAY', __time);

-- query 4
INSERT INTO t1 VALUES
    ('2024-04-01', 1),
    ('2024-04-02', 1),
    ('2024-04-03', 1),
    ('2024-04-04', 1),
    ('2024-04-05', 1);

-- query 5
CREATE MATERIALIZED VIEW `mv1`
PARTITION BY (`__time`)
REFRESH ASYNC
PROPERTIES (
    "auto_refresh_partitions_limit" = "3",
    "partition_refresh_number" = "-1"
)
AS select * from t1;

-- query 6
-- case 0: default refresh
-- Mirror the old dev/test helper: wait for the refresh task to be counted, then give FE one more
-- second to publish the refreshed MV snapshot before validating row visibility.
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF(
    SUM(CASE WHEN state IN ('SUCCESS', 'MERGED', 'SKIPPED') THEN 1 ELSE 0 END) >= 1,
    'ready',
    'pending'
) AS status
FROM information_schema.materialized_views
JOIN information_schema.task_runs USING(task_name)
WHERE table_schema = 'db_${uuid0}'
  AND table_name = 'mv1';

-- query 7
-- The task run being marked as finished is not sufficient under full-suite load.
-- Wait until the refreshed MV row count becomes visible before snapshotting it.
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF((SELECT count(*) FROM mv1) = 3, 'ready', 'pending') AS status;

-- query 8
SELECT count(*) FROM mv1;

-- query 9
-- case 1: auto refresh
INSERT INTO t1 SELECT * FROM t1;

-- query 10
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF(
    SUM(CASE WHEN state IN ('SUCCESS', 'MERGED', 'SKIPPED') THEN 1 ELSE 0 END) >= 2,
    'ready',
    'pending'
) AS status
FROM information_schema.materialized_views
JOIN information_schema.task_runs USING(task_name)
WHERE table_schema = 'db_${uuid0}'
  AND table_name = 'mv1';

-- query 11
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF((SELECT count(*) FROM mv1) = 6, 'ready', 'pending') AS status;

-- query 12
SELECT count(*) FROM mv1;

-- query 13
-- case 2: manual complete refresh
REFRESH MATERIALIZED VIEW mv1 WITH SYNC MODE;

-- query 14
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF(
    SUM(CASE WHEN state IN ('SUCCESS', 'MERGED', 'SKIPPED') THEN 1 ELSE 0 END) >= 3,
    'ready',
    'pending'
) AS status
FROM information_schema.materialized_views
JOIN information_schema.task_runs USING(task_name)
WHERE table_schema = 'db_${uuid0}'
  AND table_name = 'mv1';

-- query 15
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF((SELECT count(*) FROM mv1) = 10, 'ready', 'pending') AS status;

-- query 16
SELECT count(*) FROM mv1;

-- query 17
-- case 3: manual partial refresh
REFRESH MATERIALIZED VIEW mv1 PARTITION start('2024-04-02') end('2024-04-04') WITH SYNC MODE;

-- query 18
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF(
    SUM(CASE WHEN state IN ('SUCCESS', 'MERGED', 'SKIPPED') THEN 1 ELSE 0 END) >= 4,
    'ready',
    'pending'
) AS status
FROM information_schema.materialized_views
JOIN information_schema.task_runs USING(task_name)
WHERE table_schema = 'db_${uuid0}'
  AND table_name = 'mv1';

-- query 19
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF((SELECT count(*) FROM mv1) = 10, 'ready', 'pending') AS status;

-- query 20
SELECT count(*) FROM mv1;

-- query 21
-- case 4: truncate table
TRUNCATE TABLE t1;

-- query 22
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF(
    SUM(CASE WHEN state IN ('SUCCESS', 'MERGED', 'SKIPPED') THEN 1 ELSE 0 END) >= 5,
    'ready',
    'pending'
) AS status
FROM information_schema.materialized_views
JOIN information_schema.task_runs USING(task_name)
WHERE table_schema = 'db_${uuid0}'
  AND table_name = 'mv1';

-- query 23
-- @result_contains=ready
-- @retry_count=60
-- @retry_interval_ms=1000
SELECT IF((SELECT count(*) FROM mv1) = 4, 'ready', 'pending') AS status;

-- query 24
SELECT count(*) FROM mv1;

-- query 25
-- cleanup
drop database db_${uuid0};
