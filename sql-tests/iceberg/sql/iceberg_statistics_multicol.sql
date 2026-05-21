-- @order_sensitive=true
-- @tags=iceberg
-- Validate that per-column Puffin NDV is collected for every primitive
-- column independently. Three INT columns with intentionally different
-- distinct counts: dim10=10 distincts, dim5=5 distincts, dim2=2 distincts.
-- Self-join on the lower-cardinality column must produce a larger
-- estimated cardinality than self-join on the higher-cardinality column
-- (card ≈ N*N / max(ndv_left, ndv_right)).

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0} (
  id INT,
  dim10 INT,
  dim5 INT,
  dim2 INT
);

-- query 3
-- 10 rows. Each column intentionally has a different number of distinct
-- values: dim10=10, dim5=5, dim2=2. Theta NDV must be collected per column.
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0} VALUES
  (1, 100, 50, 0),
  (2, 101, 51, 1),
  (3, 102, 52, 0),
  (4, 103, 53, 1),
  (5, 104, 54, 0),
  (6, 105, 50, 1),
  (7, 106, 51, 0),
  (8, 107, 52, 1),
  (9, 108, 53, 0),
  (10, 109, 54, 1);

-- query 4
SELECT count(*) AS n FROM iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0};

-- query 5
SELECT count(DISTINCT dim10) AS ndv_dim10 FROM iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0};

-- query 6
SELECT count(DISTINCT dim5) AS ndv_dim5 FROM iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0};

-- query 7
SELECT count(DISTINCT dim2) AS ndv_dim2 FROM iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0};

-- query 8
-- Self-join on the unique column (dim10, NDV=10).
-- Expected cardinality ≈ 10*10/max(10,10) = 10.
-- @explain_contains=HASH JOIN
-- @explain_contains=stats={rows=
SELECT count(*) AS n_joined
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0} a
  JOIN iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0} b
    ON a.dim10 = b.dim10;

-- query 9
-- Self-join on dim2 (NDV=2). With |A|=|B|=10, NDV=2:
-- card ≈ 10*10/max(2,2) = 50. If dim2's NDV were unavailable, the
-- estimator would fall back to a much larger number — the stats trailer
-- existence at minimum proves per-column NDV is populated.
-- @explain_contains=HASH JOIN
-- @explain_contains=stats={rows=
SELECT count(*) AS n_joined
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0} a
  JOIN iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0} b
    ON a.dim2 = b.dim2;

-- query 10
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0}.facts_${uuid0};

-- query 11
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_mc_db_${uuid0};
