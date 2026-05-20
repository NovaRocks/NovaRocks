-- @order_sensitive=true
-- @tags=iceberg
-- Verify that an Iceberg table can be written and queried end-to-end with
-- Puffin NDV statistics enabled. The test does not directly inspect the
-- statistics_files entry (no `$statistics` metadata table exists yet) — it
-- relies on the writer/reader round-trip and verifies the optimizer can
-- still produce a plan with the stats path in place.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_db_${uuid0}.stats_basic_${uuid0} (
  id INT,
  name STRING,
  amount DOUBLE
);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_stats_db_${uuid0}.stats_basic_${uuid0} VALUES
  (1, 'alice', 10.0),
  (2, 'bob', 20.0),
  (3, 'carol', 30.0),
  (4, 'dave', 40.0),
  (5, 'eve', 50.0);

-- query 4
-- Reading the table back must reflect every row. The Puffin write side runs
-- as a post-commit follow-up — failure to register stats must not affect
-- the data commit, but the SELECT path must still return all rows.
SELECT count(*) AS n
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_db_${uuid0}.stats_basic_${uuid0};

-- query 5
-- @explain_contains=stats={rows=
-- Optimizer must emit row-count stats for the scan after stats registration.
SELECT id, amount
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_db_${uuid0}.stats_basic_${uuid0}
  WHERE id > 2
  ORDER BY id;

-- query 6
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_db_${uuid0}.stats_basic_${uuid0};

-- query 7
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_db_${uuid0};
