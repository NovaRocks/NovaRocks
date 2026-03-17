-- Migrated from dev/test/sql/test_function/T/test_substr
-- Test Objective:
-- 1. Validate SUBSTR/SUBSTRING with out-of-range BIGINT literal arguments produce cast errors.
-- 2. Validate SUBSTR/SUBSTRING with BIGINT column arguments handle overflow gracefully (return NULL).
-- 3. Cover both one-arg and two-arg offset/length forms.

-- query 1
-- @expect_error=Cast argument 9223372036854775807 to int type failed
select SUBSTR('', 9223372036854775807) ;

-- query 2
-- @expect_error=Cast argument 9223372036854775807 to int type failed
select SUBSTR('', 9223372036854775807, 465254298) ;

-- query 3
-- @expect_error=Cast argument -9223372036854775807 to int type failed
select SUBSTR('', -9223372036854775807, 465254298) ;

-- query 4
-- @expect_error=Cast argument 9223372036854775806 to int type failed
select SUBSTR('', 9223372036854775806, 465254298) ;

-- query 5
-- @expect_error=Cast argument 9223372036854775807 to int type failed
select SUBSTRING('', 9223372036854775807) ;

-- query 6
-- @expect_error=Cast argument 9223372036854775807 to int type failed
select SUBSTRING('', 9223372036854775807, 465254298) ;

-- query 7
-- @expect_error=Cast argument -9223372036854775807 to int type failed
select SUBSTRING('', -9223372036854775807, 465254298) ;

-- query 8
-- @expect_error=Cast argument 9223372036854775806 to int type failed
select SUBSTRING('', 9223372036854775806, 465254298) ;

-- query 9
-- @skip_result_check=true
CREATE TABLE ${case_db}.t1 (id int, v bigint)
DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 10
-- @skip_result_check=true
USE ${case_db};
insert into t1 values(1, 9223372036854775807), (2, -9223372036854775807), (3, 9223372036854775806);

-- query 11
USE ${case_db};
select SUBSTR('', v) from t1;

-- query 12
USE ${case_db};
select SUBSTR('', v, id) from t1;

-- query 13
USE ${case_db};
select SUBSTR('STARROCKS', v, id) from t1;

-- query 14
USE ${case_db};
select SUBSTRING('', v) from t1;

-- query 15
USE ${case_db};
select SUBSTRING('', v, id) from t1;

-- query 16
USE ${case_db};
select SUBSTRING('STARROCKS', v, id) from t1;
