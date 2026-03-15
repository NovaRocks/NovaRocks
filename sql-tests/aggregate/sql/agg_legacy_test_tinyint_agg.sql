-- Migrated from dev/test/sql/test_agg/R/test_tinyint_agg
-- Test Objective:
-- Preserve legacy aggregate coverage in a self-contained sql-tests case.
-- query 1
-- @skip_result_check=true
USE ${case_db};

-- name: test_tinyint_agg @mac
-- query 2
-- @skip_result_check=true
USE ${case_db};
CREATE TABLE `t1` (
  `tinyint_col_1` tinyint NOT NULL,
  `tinyint_col_2` tinyint
) ENGINE=OLAP
PROPERTIES (
"replication_num" = "1"
);

-- query 3
-- @skip_result_check=true
USE ${case_db};
insert into t1 values (1, 1), (1, 2), (1,3), (1,4), (2, null), (3, null), (4, null);

-- query 4
USE ${case_db};
select count(distinct tinyint_col_1) from t1;

-- query 5
USE ${case_db};
select count(distinct tinyint_col_2) from t1;
