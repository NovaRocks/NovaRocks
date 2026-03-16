-- Migrated from dev/test/sql/test_bitmap_functions/T/test_bitmap_replace_if_not_null
-- Test Objective:
-- 1. Validate REPLACE_IF_NOT_NULL aggregate: initial insert sets bitmap.
-- 2. Validate that inserting NULL does NOT replace the existing bitmap.
-- 3. Validate that inserting a new non-NULL bitmap DOES replace the existing bitmap.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_bm_rin;
CREATE TABLE ${case_db}.t_bm_rin (
  `c1` int(11) NULL COMMENT "",
  `c2` bitmap REPLACE_IF_NOT_NULL NULL COMMENT ""
) ENGINE=OLAP
AGGREGATE KEY(`c1`)
DISTRIBUTED BY HASH(`c1`) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- query 2
-- @skip_result_check=true
insert into ${case_db}.t_bm_rin values (1, bitmap_from_string("1,2,3")), (2, bitmap_from_string("4,5,6"));

-- query 3
-- @order_sensitive=true
-- After initial insert: c1=1 has {1,2,3}, c1=2 has {4,5,6}
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;

-- query 4
-- @skip_result_check=true
-- Insert NULL for c1=1: REPLACE_IF_NOT_NULL means NULL does NOT overwrite
insert into ${case_db}.t_bm_rin values (1, null);

-- query 5
-- @order_sensitive=true
-- c1=1 should still have {1,2,3} because NULL was not applied
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;

-- query 6
-- @skip_result_check=true
-- Insert a new non-NULL bitmap for c1=1: this DOES replace
insert into ${case_db}.t_bm_rin values (1, bitmap_from_string("7,8,9"));

-- query 7
-- @order_sensitive=true
-- c1=1 is now {7,8,9}, c1=2 is still {4,5,6}
select c1, bitmap_to_string(c2) from ${case_db}.t_bm_rin order by c1;
