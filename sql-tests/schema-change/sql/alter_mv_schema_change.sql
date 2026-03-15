-- Test Objective:
-- 1. Validate MV behavior when base table schema changes (modify decimal column).
-- 2. Validate MV behavior when base table schema changes (modify char column).
-- 3. Verify ALTER MATERIALIZED VIEW ... ACTIVE re-activates MV after base table schema change.
-- 4. Verify data correctness after refresh following schema change.
-- Migrated from: dev/test/sql/test_alter_mv/T/test_alter_mv (second part)

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t1 (
    k1 date not null,
    k2 datetime not null,
    k3 char(20),
    k4 varchar(20),
    k5 boolean,
    k6 tinyint,
    k7 smallint,
    k8 int,
    k9 bigint,
    k10 largeint,
    k11 float,
    k12 double,
    k13 decimal(27,9))
DUPLICATE KEY(k1)
PARTITION BY date_trunc('day', k1)
DISTRIBUTED BY RANDOM BUCKETS 3;
INSERT INTO t1 VALUES
    ('2020-10-11','2020-10-24 12:12:12','k3','k4',0,0,2,3,4,5,1.1,1.12,2.889),
    ('2020-10-12','2020-10-25 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889),
    ('2020-10-21','2020-10-24 12:12:12','k3','k4',0,0,2,3,4,5,1.1,1.12,2.889);
CREATE MATERIALIZED VIEW test_mv1
REFRESH DEFERRED MANUAL
AS SELECT k1, k2, k3, k4, k5, k6, k7, k8, k9, k10, k11, k12, k13 FROM t1;
ADMIN SET FRONTEND CONFIG("enable_active_materialized_view_schema_strict_check"="false");

-- query 2
-- Alter decimal column k13 from decimal(27,9) to decimal(32,10).
-- @skip_result_check=true
USE ${case_db};
ALTER TABLE t1 MODIFY COLUMN k13 decimal(32, 10);

-- query 3
-- Wait for schema change to finish.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE ${case_db};
SHOW ALTER TABLE COLUMN FROM ${case_db} ORDER BY CreateTime DESC LIMIT 1;

-- query 4
-- Re-activate MV after schema change.
-- @skip_result_check=true
USE ${case_db};
ALTER MATERIALIZED VIEW test_mv1 ACTIVE;

-- query 5
-- Insert data and refresh MV. Retry because table state may still be transitioning.
-- @retry_count=30
-- @retry_interval_ms=2000
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
    ('2020-10-23','2020-10-24 12:12:12','k3','k4',0,0,2,3,4,5,1.1,1.12,2.889);
REFRESH MATERIALIZED VIEW test_mv1 FORCE WITH SYNC MODE;

-- query 6
-- @order_sensitive=true
USE ${case_db};
SELECT * FROM test_mv1 ORDER BY k1;

-- query 7
-- Alter char column k3 from char(20) to char(32).
-- @skip_result_check=true
USE ${case_db};
ALTER TABLE t1 MODIFY COLUMN k3 char(32);

-- query 8
-- Wait for schema change to finish.
-- @retry_count=60
-- @retry_interval_ms=1000
-- @result_contains=FINISHED
-- @skip_result_check=true
USE ${case_db};
SHOW ALTER TABLE COLUMN FROM ${case_db} WHERE TableName = 't1' ORDER BY CreateTime DESC LIMIT 1;

-- query 9
-- Re-activate MV after schema change.
-- @skip_result_check=true
USE ${case_db};
ALTER MATERIALIZED VIEW test_mv1 ACTIVE;

-- query 10
-- Insert data and refresh MV. Retry because table state may still be transitioning.
-- @retry_count=30
-- @retry_interval_ms=2000
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t1 VALUES
    ('2020-10-24','2020-10-25 12:12:12','k3','k4',0,1,2,3,4,5,1.1,1.12,2.889);
REFRESH MATERIALIZED VIEW test_mv1 FORCE WITH SYNC MODE;

-- query 11
-- @order_sensitive=true
USE ${case_db};
SELECT * FROM test_mv1 ORDER BY k1;

-- query 12
-- @skip_result_check=true
USE ${case_db};
DROP TABLE t1;
ADMIN SET FRONTEND CONFIG("enable_active_materialized_view_schema_strict_check"="true");
