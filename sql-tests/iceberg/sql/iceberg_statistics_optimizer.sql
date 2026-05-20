-- @order_sensitive=true
-- @tags=iceberg
-- End-to-end check that the optimizer picks up Iceberg Puffin NDV after
-- INSERT and produces a JOIN plan whose row-count stats are emitted on every
-- physical operator. We do not pin a specific cardinality because Theta
-- sketch estimates carry ~1.5% noise; we instead assert the plan shape and
-- that stats are present.

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0}.users_${uuid0} (
  user_id INT,
  city STRING
);

-- query 3
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0}.orders_${uuid0} (
  order_id INT,
  user_id INT,
  total DOUBLE
);

-- query 4
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0}.users_${uuid0} VALUES
  (1, 'SF'), (2, 'NY'), (3, 'LA'), (4, 'SEA'), (5, 'PDX'),
  (6, 'SF'), (7, 'NY'), (8, 'LA'), (9, 'SEA'), (10, 'PDX');

-- query 5
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0}.orders_${uuid0} VALUES
  (101, 1, 50.0), (102, 2, 80.0), (103, 3, 30.0), (104, 4, 20.0),
  (105, 1, 75.0), (106, 2, 60.0), (107, 5, 90.0), (108, 6, 40.0);

-- query 6
-- @explain_contains=HASH_JOIN
-- @explain_contains=stats={rows=
-- Join cardinality is computed from NDV; we only check that the join shape
-- and stats trailer are present. With NDV(users.user_id)=10 and
-- |users|=10, |orders|=8, the expected cardinality is roughly
-- 10*8/max(10, ndv(orders.user_id)) ≈ 8 rows.
SELECT count(*) AS n_joined
  FROM iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0}.orders_${uuid0} o
  JOIN iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0}.users_${uuid0} u
    ON o.user_id = u.user_id;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0}.orders_${uuid0};

-- query 8
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0}.users_${uuid0};

-- query 9
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_opt_stats_db_${uuid0};
