-- Test Objective:
-- 1. Validate count-rollup rewrite stays correct when the base table is empty and after late inserts.
-- 2. Cover empty-result aggregation semantics, async partitioned MV refresh, and PK-table rollup behavior.
-- Source: dev/test/sql/test_materialized_view/T/test_materialized_view_rewrite

-- query 1
create table empty_tbl(time date, user_id int not null, user_name varchar(20), tag_id int) partition by range (time)  (partition p1 values less than MAXVALUE) distributed by hash(time) buckets 3 properties('replication_num' = '1');

-- query 2
create materialized view empty_tbl_with_mv distributed by hash(user_id)
as select user_id, time, count(tag_id) from empty_tbl group by user_id, time;

-- query 3
select count() from empty_tbl;

-- query 4
select user_id, count(tag_id) from empty_tbl group by user_id, time;

-- query 5
select user_id, count(tag_id) from empty_tbl group by user_id;

-- query 6
insert into empty_tbl values('2023-04-13', 1, 'a', 1);

-- query 7
refresh materialized view empty_tbl_with_mv with sync mode;

-- query 8
select count() from empty_tbl where user_id > 2;

-- query 9
select user_id, count(tag_id) from empty_tbl where user_id = 2 group by user_id, time;

-- query 10
select user_id, count(tag_id) from empty_tbl where user_id > 2 group by user_id;

-- query 11
select user_id, count(tag_id) from empty_tbl group by user_id;

-- query 12
select count(user_id) from empty_tbl where user_id > 2 group by user_id;

-- query 13
select count(user_id) from empty_tbl where user_id > 2;

-- query 14
drop table empty_tbl;

-- query 15
drop materialized view empty_tbl_with_mv;

-- query 16
CREATE TABLE orders (
    dt date NOT NULL,
    order_id bigint NOT NULL,
    user_id int NOT NULL,
    merchant_id int NOT NULL,
    good_id int NOT NULL,
    good_name string NOT NULL,
    price int NOT NULL,
    cnt int NOT NULL,
    revenue int NOT NULL,
    state tinyint NOT NULL
)
PRIMARY KEY (dt, order_id)
PARTITION BY RANGE(dt) (
    PARTITION p20210820 VALUES [('2021-08-20'), ('2021-08-21')),
    PARTITION p20210821 VALUES [('2021-08-21'), ('2021-08-22'))
)
DISTRIBUTED BY HASH(order_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_persistent_index" = "true"
);

-- query 17
CREATE MATERIALIZED VIEW order_mv2
PARTITION BY date_trunc('MONTH', dt)
DISTRIBUTED BY HASH(order_id) BUCKETS 10
REFRESH ASYNC START('2023-07-01 10:00:00') EVERY (interval 1 day)
AS
select
    dt,
    order_id,
    user_id,
    sum(cnt) as total_cnt,
    sum(revenue) as total_revenue,
    count(state) as state_count
from orders group by dt, order_id, user_id;

-- query 18
select count() from orders;

-- query 19
drop materialized view order_mv2;
