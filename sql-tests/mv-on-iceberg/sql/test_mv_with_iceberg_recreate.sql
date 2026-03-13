-- Test Objective:
-- 1. Validate an MV over a local Iceberg catalog is invalidated when the external base table is dropped and recreated.
-- 2. Cover the current NovaRocks behavior after recreate: refresh failure, inactive status, and rewrite fallback.
-- Source: dev/test/sql/test_materialized_view/T/test_mv_with_iceberg_recreate

-- query 1
-- This case uses the local Hadoop-style Iceberg catalog from sql-tests config.
create external catalog mv_iceberg_${uuid0}
properties
(
    "type" = "iceberg",
    "iceberg.catalog.type" = "${iceberg_catalog_type}",
    "iceberg.catalog.warehouse" = "${iceberg_catalog_warehouse}",
    "aws.s3.access_key" = "${oss_ak}",
    "aws.s3.secret_key" = "${oss_sk}",
    "aws.s3.endpoint" = "${oss_endpoint}",
    "aws.s3.enable_path_style_access" = "true"
);

-- query 2
set catalog mv_iceberg_${uuid0};

-- query 3
create database mv_ice_db_${uuid0};

-- query 4
use mv_ice_db_${uuid0};

-- query 5
create table mv_ice_tbl_${uuid0} (
  col_str string,
  col_int int,
  dt date
) partition by(dt);

-- query 6
insert into mv_ice_tbl_${uuid0} values
  ('1d8cf2a2c0e14fa89d8117792be6eb6f', 2000, '2023-12-01'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2000, '2023-12-01'),
  ('abc', 2000, '2023-12-02'),
  (NULL, 2000, '2023-12-02'),
  ('ab1d8cf2a2c0e14fa89d8117792be6eb6f', 2001, '2023-12-03'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2001, '2023-12-03'),
  ('abc', 2001, '2023-12-04'),
  (NULL, 2001, '2023-12-04');

-- query 7
set catalog default_catalog;

-- query 8
create database db_${uuid0};

-- query 9
use db_${uuid0};

-- query 10
CREATE MATERIALIZED VIEW test_mv1 PARTITION BY dt
REFRESH DEFERRED MANUAL AS SELECT dt,sum(col_int)
FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0}  GROUP BY dt;

-- query 11
-- The sync refresh returns a generated task/query id that is not stable across runs.
-- @skip_result_check=true
REFRESH MATERIALIZED VIEW test_mv1 WITH SYNC MODE;

-- query 12
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} WHERE dt='2023-12-01' GROUP BY dt;

-- query 13
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} WHERE dt='2023-12-02' GROUP BY dt;

-- query 14
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} WHERE dt='2023-12-03' GROUP BY dt;

-- query 15
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0}  GROUP BY dt;

-- query 16
-- @result_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} WHERE dt='2023-12-03' GROUP BY dt;

-- query 17
SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} WHERE dt>='2023-12-03' GROUP BY dt order by dt;

-- query 18
SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} GROUP BY dt order by dt;

-- query 19
admin set frontend config('enable_mv_automatic_active_check'='false');

-- query 20
-- drop base table
use mv_iceberg_${uuid0}.mv_ice_db_${uuid0};

-- query 21
DROP TABLE mv_ice_tbl_${uuid0};

-- query 22
set catalog default_catalog;

-- query 23
use db_${uuid0};

-- query 24
-- recreate it
use mv_iceberg_${uuid0}.mv_ice_db_${uuid0};

-- query 25
create table mv_ice_tbl_${uuid0} (
  col_str string,
  col_int int,
  dt date
) partition by(dt);

-- query 26
insert into mv_ice_tbl_${uuid0} values
  ('1d8cf2a2c0e14fa89d8117792be6eb6f', 2000, '2023-12-01'),
  ('3e82e36e56718dc4abc1168d21ec91ab', 2000, '2023-12-01');

-- query 27
set catalog default_catalog;

-- query 28
-- @expect_error=not supported by MVPCTMetaRepairer
REFRESH MATERIALIZED VIEW default_catalog.db_${uuid0}.test_mv1 WITH SYNC MODE;

-- query 29
-- The inactive reason exposed by information_schema reflects the base-table change,
-- while the refresh command above still reports the deeper repair limitation.
-- @result_contains=base-table changed:
select is_active, inactive_reason from information_schema.materialized_views where TABLE_NAME = 'test_mv1' and table_schema = 'db_${uuid0}';

-- query 30
-- @result_not_contains=test_mv1
SET enable_materialized_view_rewrite = true;
EXPLAIN SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} WHERE dt='2023-12-01' GROUP BY dt;

-- query 31
SELECT dt,sum(col_int) FROM mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} WHERE dt>='2023-12-03' GROUP BY dt order by dt;

-- query 32
select * from default_catalog.db_${uuid0}.test_mv1 order by 1, 2;

-- query 33
admin set frontend config('enable_mv_automatic_active_check'='true');

-- query 34
drop table mv_iceberg_${uuid0}.mv_ice_db_${uuid0}.mv_ice_tbl_${uuid0} force;

-- query 35
drop materialized view default_catalog.db_${uuid0}.test_mv1;

-- query 36
drop database mv_iceberg_${uuid0}.mv_ice_db_${uuid0} force;

-- query 37
drop catalog mv_iceberg_${uuid0};
