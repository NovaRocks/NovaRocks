-- Test Objective:
-- 1. Validate CREATE MATERIALIZED VIEW DDL variants and basic metadata correctness.
-- 2. Cover common create/drop flows on local OLAP tables.
-- Source: dev/test/sql/test_materialized_view/T/test_create

-- query 1
create database test_create_mv;

-- query 2
use test_create_mv;

-- query 3
CREATE TABLE `t_hash` (
  `k1` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
DUPLICATE KEY(`k1`)
DISTRIBUTED BY hash(k1) BUCKETS 64
PROPERTIES ( "replication_num" = "1");

-- query 4
CREATE TABLE `t_random` (
  `k1` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
)
DUPLICATE KEY(`k1`)
DISTRIBUTED BY RANDOM BUCKETS 64
PROPERTIES ( "replication_num" = "1");

-- query 5
CREATE TABLE `t1` (
  `k1` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
COMMENT "OLAP"
PARTITION BY RANGE(`k1`) (
    PARTITION p2 VALUES [("202301"), ("202302")),
    PARTITION p3 VALUES [("202302"), ("202303")),
    PARTITION p4 VALUES [("202303"), ("202304")),
    PARTITION p5 VALUES [("202304"), ("202305")),
    PARTITION p6 VALUES [("202305"), ("202306"))
)
DISTRIBUTED BY HASH(`k1`) BUCKETS 2
PROPERTIES ( "replication_num" = "1");

-- query 6
CREATE TABLE `t2` (
  `k1` int(11) NULL COMMENT "",
  `v1` int(11) NULL COMMENT "",
  `v2` int(11) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`k1`)
COMMENT "OLAP"
PARTITION BY RANGE(`k1`) (
    PARTITION p2 VALUES [("202301"), ("202302")),
    PARTITION p3 VALUES [("202302"), ("202303")),
    PARTITION p4 VALUES [("202303"), ("202304")),
    PARTITION p5 VALUES [("202304"), ("202305")),
    PARTITION p6 VALUES [("202305"), ("202306"))
)
DISTRIBUTED BY HASH(`k1`) BUCKETS 2
PROPERTIES ( "replication_num" = "1");

-- query 7
-- verify the CREATE & REFRESH clause
CREATE VIEW verify_result AS
SELECT
  table_schema, table_name,
  array_filter(split(MATERIALIZED_VIEW_DEFINITION, '\n'),
    line -> line like 'CREATE%' or line like 'REFRESH%' or line like 'AS %')
    AS mv_query
FROM information_schema.materialized_views
WHERE table_schema = 'test_create_mv';

-- query 8
-- simplified syntax case 1
CREATE MATERIALIZED VIEW `mv1`
REFRESH ASYNC
AS
select t0.k1, t1.v1
from t_hash t0 join t_random t1 on t0.k1 = t1.k1;

-- query 9
SELECT * FROM verify_result WHERE table_name = 'mv1';

-- query 10
-- simplified syntax case 2
CREATE MATERIALIZED VIEW `mv2`
REFRESH ASYNC
AS
select k1, count(v1)  from t_random group by k1;

-- query 11
SELECT * FROM verify_result WHERE table_name = 'mv2';

-- query 12
-- simplified syntax for partition table
CREATE MATERIALIZED VIEW `mv3`
REFRESH ASYNC
AS
select t0.k1, t1.v1
from t_hash t0 join t1 on t0.k1 = t1.k1;

-- query 13
SELECT * FROM verify_result WHERE table_name = 'mv3';

-- query 14
-- simplified syntax for partition table
CREATE MATERIALIZED VIEW `mv4`
REFRESH ASYNC
AS
select t0.k1, t1.v1
from t_random t0 join t1 on t0.k1 = t1.k1;

-- query 15
SELECT * FROM verify_result WHERE table_name = 'mv4';

-- query 16
-- simplified syntax for partition table
CREATE MATERIALIZED VIEW `mv_part1`
REFRESH ASYNC
AS
select t1.k1, t2.v1
from t1 join t2 on t1.k1 = t2.k1;

-- query 17
SELECT * FROM verify_result WHERE table_name = 'mv_part1';

-- query 18
-- complex syntax case 1
CREATE MATERIALIZED VIEW `mv5`
PARTITION BY (`k1`)
DISTRIBUTED BY HASH(`k1`) BUCKETS 2
REFRESH ASYNC
PROPERTIES (
"replication_num" = "1",
"storage_medium" = "HDD"
)
AS select k1, v1, v2 from t1
union all
select k1, v1, v2 from t2;

-- query 19
SELECT * FROM verify_result WHERE table_name = 'mv5';

-- query 20
-- complex syntax case 2
CREATE MATERIALIZED VIEW `mv6`
PARTITION BY (`k1`)
DISTRIBUTED BY RANDOM
REFRESH ASYNC
PROPERTIES (
"replication_num" = "1",
"storage_medium" = "HDD"
)
AS select k1, v1, v2 from t1
union all
select k1, v1, v2 from t2;

-- query 21
SELECT * FROM verify_result WHERE table_name = 'mv6';

-- query 22
drop database test_create_mv force;
