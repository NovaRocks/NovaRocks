-- Test Objective:
-- 1. Validate refresh and rewrite remain correct when MV partitions map many-to-many or one-to-many to base-table partitions.
-- 2. Cover partitioned MV refresh after incremental inserts and filtered rewrite on partition ranges.

-- query 1
create database db_${uuid0};

-- query 2
use db_${uuid0};

-- query 3
-- Scenario 1: month-partitioned MV over finer-grained base partitions exercises many-to-many partition mapping.
CREATE TABLE mock_tbl_many (
  k1 date,
  k2 int,
  v1 int
)
ENGINE=OLAP
PARTITION BY RANGE(k1)
(
  PARTITION p0 VALUES [('2021-07-23'),('2021-07-26')),
  PARTITION p1 VALUES [('2021-07-26'),('2021-07-29')),
  PARTITION p2 VALUES [('2021-07-29'),('2021-08-02')),
  PARTITION p3 VALUES [('2021-08-02'),('2021-08-04'))
)
DISTRIBUTED BY HASH(k2) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 4
insert into mock_tbl_many values
  ('2021-07-23',2,10),
  ('2021-07-27',2,10),
  ('2021-07-29',2,10),
  ('2021-08-02',2,10);

-- query 5
create materialized view mv_many_to_many
partition by date_trunc('month', k1)
distributed by hash(k2) buckets 3
refresh deferred manual
properties('replication_num' = '1', 'partition_refresh_number' = '1')
as select k1, k2, v1 from mock_tbl_many;

-- query 6
refresh materialized view mv_many_to_many with sync mode;

-- query 7
-- @result_contains=mv_many_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, v1 from mock_tbl_many order by k1, k2;

-- query 8
-- @result_contains=mv_many_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN
select k1, k2, v1 from mock_tbl_many
where k1 >= '2021-07-23' and k1 < '2021-07-26'
order by k1, k2;

-- query 9
select * from mv_many_to_many order by k1, k2;

-- query 10
insert into mock_tbl_many values ('2021-07-29',3,10), ('2021-08-02',3,10);

-- query 11
refresh materialized view mv_many_to_many with sync mode;

-- query 12
-- @result_contains=mv_many_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, v1 from mock_tbl_many order by k1, k2;

-- query 13
select * from mv_many_to_many order by k1, k2;

-- query 14
drop materialized view mv_many_to_many;

-- query 15
drop table mock_tbl_many;

-- query 16
-- Scenario 2: day-partitioned MV over month-partitioned base data exercises one-to-many partition mapping.
CREATE TABLE mock_tbl_one (
  k1 date,
  k2 int,
  v1 int
)
ENGINE=OLAP
PARTITION BY RANGE(k1)
(
  PARTITION p0 VALUES [('2021-07-01'),('2021-08-01')),
  PARTITION p1 VALUES [('2021-08-01'),('2021-09-01')),
  PARTITION p2 VALUES [('2021-09-01'),('2021-10-01'))
)
DISTRIBUTED BY HASH(k2) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- query 17
insert into mock_tbl_one values
  ('2021-07-01',2,10),
  ('2021-08-01',2,10),
  ('2021-08-02',2,10),
  ('2021-09-03',2,10);

-- query 18
create materialized view mv_one_to_many
partition by date_trunc('day', k1)
distributed by hash(k2) buckets 3
refresh deferred manual
properties('replication_num' = '1', 'partition_refresh_number' = '1')
as select k1, k2, v1 from mock_tbl_one;

-- query 19
refresh materialized view mv_one_to_many with sync mode;

-- query 20
-- @result_contains=mv_one_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, v1 from mock_tbl_one order by k1, k2;

-- query 21
-- @result_contains=mv_one_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN
select k1, k2, v1 from mock_tbl_one
where k1 >= '2021-08-01' and k1 < '2021-09-01'
order by k1, k2;

-- query 22
select * from mv_one_to_many order by k1, k2;

-- query 23
insert into mock_tbl_one values ('2021-08-02',3,10), ('2021-09-03',3,10);

-- query 24
refresh materialized view mv_one_to_many with sync mode;

-- query 25
-- @result_contains=mv_one_to_many
SET enable_materialized_view_rewrite = true;
EXPLAIN select k1, k2, v1 from mock_tbl_one order by k1, k2;

-- query 26
select * from mv_one_to_many order by k1, k2;

-- query 27
drop database db_${uuid0} force;
