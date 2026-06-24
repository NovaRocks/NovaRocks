-- @order_sensitive=true
-- Test Point: variant-bearing Iceberg v3 tables support row-level mutations
-- once variant read/write paths are available. Variant columns themselves are
-- still rejected as equality-delete keys.

-- query 1
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant_mut FORCE;
DROP TABLE IF EXISTS ${case_db}.s_v3_variant_mut FORCE;
CREATE TABLE ${case_db}.t_v3_variant_mut (
  id BIGINT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_v3_variant_mut VALUES
  (1, parse_json('{"a":1}')),
  (2, parse_json('{"a":2}')),
  (3, parse_json('{"a":3}'));
CREATE TABLE ${case_db}.s_v3_variant_mut (
  id BIGINT,
  new_id BIGINT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.s_v3_variant_mut VALUES (3, 30);

-- query 2
SELECT id, variant_get(v, '$.a', 'bigint') AS a
FROM ${case_db}.t_v3_variant_mut
ORDER BY id;

-- query 3
-- @skip_result_check=true
DELETE FROM ${case_db}.t_v3_variant_mut WHERE id = 2;

-- query 4
SELECT id, variant_get(v, '$.a', 'bigint') AS a
FROM ${case_db}.t_v3_variant_mut
ORDER BY id;

-- query 5
-- @skip_result_check=true
UPDATE ${case_db}.t_v3_variant_mut AS t SET id = 10 WHERE t.id = 1;

-- query 6
SELECT id, variant_get(v, '$.a', 'bigint') AS a
FROM ${case_db}.t_v3_variant_mut
ORDER BY id;

-- query 7
-- @skip_result_check=true
MERGE INTO ${case_db}.t_v3_variant_mut AS t
USING ${case_db}.s_v3_variant_mut AS s
ON t.id = s.id
WHEN MATCHED THEN UPDATE SET id = s.new_id;

-- query 8
SELECT id, variant_get(v, '$.a', 'bigint') AS a
FROM ${case_db}.t_v3_variant_mut
ORDER BY id;

-- query 9
-- @skip_result_check=true
INSERT OVERWRITE ${case_db}.t_v3_variant_mut VALUES
  (20, parse_json('{"a":20}'));

-- query 10
SELECT id, variant_get(v, '$.a', 'bigint') AS a
FROM ${case_db}.t_v3_variant_mut
ORDER BY id;

-- query 11
-- @skip_result_check=true
ALTER TABLE ${case_db}.t_v3_variant_mut ADD EQUALITY DELETE (id) VALUES (20);

-- query 12
SELECT COUNT(*) AS remaining_rows
FROM ${case_db}.t_v3_variant_mut;

-- query 13
-- @expect_error=equality-delete keys
ALTER TABLE ${case_db}.t_v3_variant_mut ADD EQUALITY DELETE (v) VALUES (NULL);

-- query 14
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_variant_mut FORCE;
DROP TABLE ${case_db}.s_v3_variant_mut FORCE;

-- query 15
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant_part_mut FORCE;
CREATE TABLE ${case_db}.t_v3_variant_part_mut (
  id BIGINT,
  region VARCHAR,
  v VARIANT
)
PARTITION BY identity(region)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);
INSERT INTO ${case_db}.t_v3_variant_part_mut VALUES
  (1, 'us', parse_json('{"a":1}')),
  (2, 'eu', parse_json('{"a":2}')),
  (3, 'us', parse_json('{"a":3}'));

-- query 16
SELECT id, region, variant_get(v, '$.a', 'bigint') AS a
FROM ${case_db}.t_v3_variant_part_mut
ORDER BY id;

-- query 17
-- @skip_result_check=true
DELETE FROM ${case_db}.t_v3_variant_part_mut
WHERE try_variant_get(v, '$.a', 'bigint') = 2;
UPDATE ${case_db}.t_v3_variant_part_mut
SET id = 30
WHERE try_variant_get(v, '$.a', 'bigint') = 3;
INSERT OVERWRITE PARTITIONS ${case_db}.t_v3_variant_part_mut VALUES
  (4, 'us', parse_json('{"a":4}'));

-- query 18
SELECT id, region, variant_get(v, '$.a', 'bigint') AS a
FROM ${case_db}.t_v3_variant_part_mut
ORDER BY id;

-- query 19
-- @expect_error=variant columns cannot appear in the partition spec
CREATE TABLE ${case_db}.t_v3_variant_partition_bad (
  id BIGINT,
  v VARIANT
)
PARTITION BY identity(v)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);

-- query 20
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.t_v3_variant_partition_alter_bad FORCE;
CREATE TABLE ${case_db}.t_v3_variant_partition_alter_bad (
  id BIGINT,
  v VARIANT
)
TBLPROPERTIES (
  "format-version" = "3",
  "write.row-lineage" = "true"
);

-- query 21
-- @expect_error=variant columns cannot appear in the partition spec
ALTER TABLE ${case_db}.t_v3_variant_partition_alter_bad ADD PARTITION COLUMN v;

-- query 22
-- @skip_result_check=true
DROP TABLE ${case_db}.t_v3_variant_part_mut FORCE;
DROP TABLE ${case_db}.t_v3_variant_partition_alter_bad FORCE;
