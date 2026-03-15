-- @order_sensitive=true
-- @tags=aggregate,legacy-migration
-- Migrated from dev/test/sql/test_agg/R/test_meta_scan_agg
-- query 1
USE ${case_db};
create table t2 (
    c0 INT
) DUPLICATE key (c0) DISTRIBUTED BY HASH(c0) BUCKETS 3 PROPERTIES('replication_num' = '1');

-- query 2
USE ${case_db};
insert into t2 values (1), (2), (3);

-- query 3
USE ${case_db};
select count(*) from t2[_META_];

-- query 4
USE ${case_db};
select count(*) from t2;

-- query 5
USE ${case_db};
alter table t2 rename column c0 to c1;

-- query 6
USE ${case_db};
select count(*) from t2[_META_];

-- query 7
USE ${case_db};
select count(*) from t2;

-- query 8
USE ${case_db};
alter table t2 rename column c1 to c2;

-- query 9
USE ${case_db};
select count(*) from t2[_META_];

-- query 10
USE ${case_db};
select count(*) from t2;
