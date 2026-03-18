-- @tags=join,null_safe_equal
-- Test Objective:
-- 1. Validate null-safe equality (<=>) join semantics across shuffle, broadcast, and bucket hints.
-- 2. Verify inner, left, right joins with <=> on nullable and non-nullable columns.
-- 3. Self-join with <=> on a fully-nullable table to test NULL-matches-NULL behavior.
-- Test Flow:
-- 1. Create nullable_t1 (all nullable) and t2 (all NOT NULL with defaults).
-- 2. Insert rows including NULLs on both sides.
-- 3. Execute join/left/right with shuffle/broadcast/bucket hints using <=>.
-- 4. Self-join nullable_t1 with itself using <=> on varchar column.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.nullable_t1 (
  `t1_c1` int,
  `t1_c2` int,
  `t1_c3` int,
  `t1_c4` varchar(10)
) DUPLICATE KEY(`t1_c1`)
DISTRIBUTED BY HASH(`t1_c1`)
PROPERTIES ("replication_num" = "1");

-- query 2
-- @skip_result_check=true
CREATE TABLE ${case_db}.t2 (
  `t2_c1` int NOT NULL default "0",
  `t2_c2` int NOT NULL default "0",
  `t2_c3` int NOT NULL default "0",
  `t2_c4` varchar(10) NOT NULL default ""
) DUPLICATE KEY(`t2_c1`)
DISTRIBUTED BY HASH(`t2_c1`)
PROPERTIES ("replication_num" = "1");

-- query 3
-- @skip_result_check=true
insert into ${case_db}.nullable_t1 (t1_c1, t1_c2, t1_c3, t1_c4) values
(1, 11, 111, '1111'), (2, 22, 222, '2222'), (3, null, 333, '3333'), (4, null, null, '4444'), (null, 55, 555, null), (6, 66, null, '6666'), (null, null, null, null);

-- query 4
-- @skip_result_check=true
insert into ${case_db}.t2 (t2_c1, t2_c2, t2_c3, t2_c4) values
(1, 11, 111, '1111'), (2, 22, 222, '2222'), (3, 33, 333, '3333'), (4, 44, 444, '4444'), (5, 55, 555, '5555'), (6, 66, 666, '6666'), (7, 77, 777, '7777');

-- query 5
-- @skip_result_check=true
set pipeline_dop = 1;

-- inner join [shuffle] with <=> on varchar
-- query 6
-- @order_sensitive=true
select * from ${case_db}.t2 join [shuffle] ${case_db}.nullable_t1 on t1_c4 <=> t2_c4;

-- inner join [broadcast] with <=> on varchar
-- query 7
-- @order_sensitive=true
select * from ${case_db}.t2 join [broadcast] ${case_db}.nullable_t1 on t1_c4 <=> t2_c4;

-- inner join [bucket] with <=> on two columns
-- query 8
-- @order_sensitive=true
select * from ${case_db}.t2 join [bucket] ${case_db}.nullable_t1 on t1_c1 <=> t2_c1 and t1_c4 <=> t2_c4;

-- left join [shuffle] with <=> on varchar
-- query 9
-- @order_sensitive=true
select * from ${case_db}.t2 left join [shuffle] ${case_db}.nullable_t1 on t1_c4 <=> t2_c4;

-- left join [broadcast] with <=> on varchar
-- query 10
-- @order_sensitive=true
select * from ${case_db}.t2 left join [broadcast] ${case_db}.nullable_t1 on t1_c4 <=> t2_c4;

-- left join [bucket] with <=> on two columns
-- query 11
-- @order_sensitive=true
select * from ${case_db}.t2 left join [bucket] ${case_db}.nullable_t1 on t1_c1 <=> t2_c1 and t1_c4 <=> t2_c4;

-- right join [shuffle] with <=> on varchar
-- query 12
-- @order_sensitive=true
select * from ${case_db}.t2 right join [shuffle] ${case_db}.nullable_t1 on t1_c4 <=> t2_c4;

-- right join [bucket] with <=> on two columns
-- query 13
-- @order_sensitive=true
select * from ${case_db}.t2 right join [bucket] ${case_db}.nullable_t1 on t1_c1 <=> t2_c1 and t1_c4 <=> t2_c4;

-- self-join [shuffle] with <=> on varchar
-- query 14
-- @order_sensitive=true
select * from ${case_db}.nullable_t1 t1 join [shuffle] ${case_db}.nullable_t1 t2 on t1.t1_c4 <=> t2.t1_c4;

-- self-join [broadcast] with <=> on varchar
-- query 15
-- @order_sensitive=true
select * from ${case_db}.nullable_t1 t1 join [broadcast] ${case_db}.nullable_t1 t2 on t1.t1_c4 <=> t2.t1_c4;

-- self-join [bucket] with <=> on varchar
-- query 16
-- @order_sensitive=true
select * from ${case_db}.nullable_t1 t1 join [bucket] ${case_db}.nullable_t1 t2 on t1.t1_c4 <=> t2.t1_c4;

-- self left join [shuffle] with <=> on varchar
-- query 17
-- @order_sensitive=true
select * from ${case_db}.nullable_t1 t1 left join [shuffle] ${case_db}.nullable_t1 t2 on t1.t1_c4 <=> t2.t1_c4;

-- self left join [broadcast] with <=> on varchar
-- query 18
-- @order_sensitive=true
select * from ${case_db}.nullable_t1 t1 left join [broadcast] ${case_db}.nullable_t1 t2 on t1.t1_c4 <=> t2.t1_c4;

-- self left join [bucket] with <=> on varchar
-- query 19
-- @order_sensitive=true
select * from ${case_db}.nullable_t1 t1 left join [bucket] ${case_db}.nullable_t1 t2 on t1.t1_c4 <=> t2.t1_c4;

-- self right join [shuffle] with <=> on varchar
-- query 20
-- @order_sensitive=true
select * from ${case_db}.nullable_t1 t1 right join [shuffle] ${case_db}.nullable_t1 t2 on t1.t1_c4 <=> t2.t1_c4;

-- self right join [bucket] with <=> on varchar
-- query 21
-- @order_sensitive=true
select * from ${case_db}.nullable_t1 t1 right join [bucket] ${case_db}.nullable_t1 t2 on t1.t1_c4 <=> t2.t1_c4;
