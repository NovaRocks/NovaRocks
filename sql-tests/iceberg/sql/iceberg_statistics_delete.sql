-- @order_sensitive=true
-- @tags=iceberg
-- Validate that DELETE preserves the previous Puffin via the rebind path
-- in stats_assembler::assemble (CommitType::Delete returns None; the commit
-- collector carries forward the prior statistics entry against the new
-- snapshot id so the optimizer still finds NDV after row removal).

-- query 1
-- @skip_result_check=true
CREATE DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_delete_db_${uuid0};

-- query 2
-- @skip_result_check=true
CREATE TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_delete_db_${uuid0}.orders_${uuid0} (
  id INT,
  amount DOUBLE
);

-- query 3
-- @skip_result_check=true
INSERT INTO iceberg_cat_${suite_uuid0}.iceberg_stats_delete_db_${uuid0}.orders_${uuid0} VALUES
  (1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0), (5, 50.0),
  (6, 60.0), (7, 70.0), (8, 80.0), (9, 90.0), (10, 100.0);

-- query 4
-- DELETE some rows. Puffin must remain discoverable on the new snapshot via
-- rebind — no new Puffin is written.
-- @skip_result_check=true
DELETE FROM iceberg_cat_${suite_uuid0}.iceberg_stats_delete_db_${uuid0}.orders_${uuid0}
  WHERE id > 7;

-- query 5
SELECT count(*) AS n
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_delete_db_${uuid0}.orders_${uuid0};

-- query 6
-- Plan-shape assertion: after DELETE, scan still emits a stats trailer.
-- @explain_contains=stats={rows=
SELECT id, amount
  FROM iceberg_cat_${suite_uuid0}.iceberg_stats_delete_db_${uuid0}.orders_${uuid0}
  ORDER BY id;

-- query 7
-- @skip_result_check=true
DROP TABLE iceberg_cat_${suite_uuid0}.iceberg_stats_delete_db_${uuid0}.orders_${uuid0};

-- query 8
-- @skip_result_check=true
DROP DATABASE iceberg_cat_${suite_uuid0}.iceberg_stats_delete_db_${uuid0};
