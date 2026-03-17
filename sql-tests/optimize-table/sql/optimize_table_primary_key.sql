-- Migrated from dev/test/sql/test_optimize_table/T/test_optimize_table (test_alter_key_buckets)
-- Test Objective:
-- 1. Verify optimize table works on PRIMARY KEY table.
-- @sequential=true

-- query 1
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE t_pk (
    user_name VARCHAR(32) DEFAULT '',
    city_code VARCHAR(100),
    `from` VARCHAR(32) DEFAULT '',
    pv BIGINT DEFAULT '0'
) PRIMARY KEY(user_name)
DISTRIBUTED BY HASH(user_name) BUCKETS 5
PROPERTIES("replication_num" = "1");

-- query 2
-- @skip_result_check=true
USE ${case_db};
INSERT INTO t_pk VALUES('alice','BJ','web',10),('bob','SH','app',20);

-- query 3
-- @skip_result_check=true
-- @wait_alter_optimize=t_pk
USE ${case_db};
ALTER TABLE t_pk DISTRIBUTED BY HASH(user_name) BUCKETS 10;

-- query 4
-- @result_contains=BUCKETS 10
USE ${case_db};
SHOW CREATE TABLE t_pk;

-- query 5
USE ${case_db};
SELECT * FROM t_pk ORDER BY user_name;
