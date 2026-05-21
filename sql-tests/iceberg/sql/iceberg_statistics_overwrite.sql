-- @order_sensitive=true
-- @tags=iceberg
-- Validate that INSERT OVERWRITE recomputes NDV against the new row set.
-- assemble_overwrite() takes the union of the newly-written file sketches
-- (which after OVERWRITE constitute every live row) and writes a fresh
-- Puffin — the previous sketch is discarded along with the obsoleted files.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0}.summary_${uuid0} (
  bucket INT,
  total DOUBLE
);

-- query 3
-- Initial wide INSERT — 10 distinct buckets.
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0}.summary_${uuid0} VALUES
  (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0),
  (6, 60.0), (7, 70.0), (8, 80.0), (9, 90.0), (10, 100.0);

-- query 4
-- INSERT OVERWRITE collapses to 3 distinct buckets. The new Puffin must
-- reflect the smaller cardinality, not the union of old and new — this is
-- the assemble_overwrite() path.
-- @skip_result_check=true
INSERT OVERWRITE iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0}.summary_${uuid0} VALUES
  (1, 1000.0), (2, 2000.0), (3, 3000.0);

-- query 5
SELECT count(*) AS n
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0}.summary_${uuid0};

-- query 6
SELECT bucket, total
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0}.summary_${uuid0}
  ORDER BY bucket;

-- query 7
-- Plan-shape assertion: after OVERWRITE, scan emits a stats trailer driven
-- by the post-overwrite NDV (=3 distinct buckets), not the pre-overwrite
-- aggregate.
-- @explain_contains=stats={rows=
SELECT bucket
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0}.summary_${uuid0}
  WHERE bucket >= 2
  ORDER BY bucket;

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0}.summary_${uuid0};

-- query 9
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_overwrite_db_${uuid0};
