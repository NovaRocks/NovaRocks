-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- @sequential=true
-- @order_sensitive=true
-- @tags=write_path,iceberg,row_lineage,uniqueness
-- Test Point:
--   Iceberg V3 row-lineage uniqueness invariants under realistic mutation
--   sequences, including multiple OPTIMIZE rewrites:
--     I1 (intra-snapshot): count(*) = count(DISTINCT _row_id)
--     I2 (cross-snapshot): for each logical row identity (id), the set of
--                          _row_id values across history has size <= 1.
-- Method:
--   Mutate the table through INSERT / DELETE / UPDATE / OPTIMIZE / INSERT /
--   OPTIMIZE while tagging each "interesting" snapshot, then assert both
--   invariants by joining over the tags.
-- Scope:
--   Phase C cross-snapshot _row_id uniqueness — the invariant the IVM
--   row-lineage pairing relies on.

-- query 1
-- @skip_result_check=true
CREATE CATALOG uniq_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'hadoop', "iceberg.catalog.warehouse" = '${iceberg_test_warehouse}/uniq_${uuid0}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA uniq_${uuid0}.ns_${uuid0};
CREATE TABLE uniq_${uuid0}.ns_${uuid0}.uniq (id INT, v INT) WITH (format_version = 3);

-- query 2
-- @skip_result_check=true
INSERT INTO uniq_${uuid0}.ns_${uuid0}.uniq VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30), ROW(4, 40), ROW(5, 50);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE create_tag(name => 't1', replace => false, if_not_exists => false);

-- query 3
-- @skip_result_check=true
DELETE FROM uniq_${uuid0}.ns_${uuid0}.uniq WHERE id = 4;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE create_tag(name => 't2', replace => false, if_not_exists => false);

-- query 4
-- @skip_result_check=true
UPDATE uniq_${uuid0}.ns_${uuid0}.uniq SET v = v + 1 WHERE id = 1;
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE create_tag(name => 't3', replace => false, if_not_exists => false);

-- query 5
-- @db=uniq_${uuid0}.ns_${uuid0}
ALTER TABLE uniq EXECUTE optimize;

-- query 6
-- @skip_result_check=true
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE create_tag(name => 't4', replace => false, if_not_exists => false);
INSERT INTO uniq_${uuid0}.ns_${uuid0}.uniq VALUES ROW(10, 100), ROW(20, 200);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE create_tag(name => 't5', replace => false, if_not_exists => false);

-- query 7
-- @db=uniq_${uuid0}.ns_${uuid0}
ALTER TABLE uniq EXECUTE optimize;

-- query 8
-- @skip_result_check=true
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE create_tag(name => 't6', replace => false, if_not_exists => false);

-- query 9
-- I1 — current snapshot intra-snapshot uniqueness.
-- @db=uniq_${uuid0}.ns_${uuid0}
SELECT count() - count(DISTINCT _row_id) AS row_id_collisions FROM uniq;

-- query 10
-- I1 — same invariant on each historical tag.
-- @db=uniq_${uuid0}.ns_${uuid0}
SELECT
  (SELECT count() - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't1') AS t1_collisions,
  (SELECT count() - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't2') AS t2_collisions,
  (SELECT count() - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't3') AS t3_collisions,
  (SELECT count() - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't4') AS t4_collisions,
  (SELECT count() - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't5') AS t5_collisions,
  (SELECT count() - count(DISTINCT _row_id) FROM uniq FOR VERSION AS OF 't6') AS t6_collisions;

-- query 11
-- I2 — cross-snapshot row-id uniqueness per logical id.
-- For every id observed across all tags, the set of _row_id values must
-- have size 1 (id keeps the same row-lineage identity through its life).
-- @db=uniq_${uuid0}.ns_${uuid0}
SELECT max(distinct_row_ids) AS max_row_ids_per_id
  FROM (
    SELECT id, count(DISTINCT _row_id) AS distinct_row_ids
      FROM (
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't1'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't2'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't3'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't4'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't5'
        UNION ALL
        SELECT id, _row_id FROM uniq FOR VERSION AS OF 't6'
      ) hist
      GROUP BY id
  ) per_id;

-- query 12
-- @skip_result_check=true
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE drop_tag(name => 't1', if_exists => false);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE drop_tag(name => 't2', if_exists => false);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE drop_tag(name => 't3', if_exists => false);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE drop_tag(name => 't4', if_exists => false);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE drop_tag(name => 't5', if_exists => false);
ALTER TABLE uniq_${uuid0}.ns_${uuid0}.uniq EXECUTE drop_tag(name => 't6', if_exists => false);
DROP TABLE uniq_${uuid0}.ns_${uuid0}.uniq ;
DROP SCHEMA uniq_${uuid0}.ns_${uuid0};
DROP CATALOG uniq_${uuid0};
