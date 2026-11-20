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
-- @tags=write_path,iceberg,branch,row_lineage
-- Test Point:
--   Branch writes do not perturb main's row-lineage state, and a tag
--   captures a stable (id, _row_id) snapshot accessible via time travel.
-- Method:
--   Create a v3 row-lineage table, INSERT to main, tag, branch off, mutate
--   the branch (INSERT/UPDATE/DELETE), and verify that
--     1. main's row-lineage triples are unchanged after branch mutation;
--     2. the tag snapshot returns the original row-lineage triples;
--     3. the branch sees the mutated set.
-- Scope:
--   Phase C (row-lineage isolation across branch/tag) end-to-end.

-- query 1
-- @skip_result_check=true
CREATE CATALOG br_rl_${uuid0} USING iceberg WITH ("iceberg.catalog.type" = 'hadoop', "iceberg.catalog.warehouse" = '${iceberg_test_warehouse}/br_rl_${uuid0}', "aws.s3.endpoint" = '${oss_endpoint}', "credential.object-store-metadata.consumer-role" = 'frontend', "credential.object-store-metadata.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-metadata.name" = '${iceberg_object_store_metadata_credential_name}', "credential.object-store-metadata.generation" = '${iceberg_object_store_credential_generation}', "credential.object-store-data.consumer-role" = 'backend', "credential.object-store-data.mode" = '${iceberg_object_store_credential_mode}', "credential.object-store-data.name" = '${iceberg_object_store_data_credential_name}', "credential.object-store-data.generation" = '${iceberg_object_store_credential_generation}', "aws.s3.enable_path_style_access" = '${oss_path_style}');
CREATE SCHEMA br_rl_${uuid0}.ns_${uuid0};
CREATE TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage (id INT, v INT) WITH (format_version = 3);
INSERT INTO br_rl_${uuid0}.ns_${uuid0}.brlineage VALUES ROW(1, 10), ROW(2, 20), ROW(3, 30);

-- query 2
-- @skip_result_check=true
-- Tag and branch off the initial state.
ALTER TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage EXECUTE create_tag(name => 'snap0', replace => false, if_not_exists => false);
CREATE BRANCH feat IN TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage;

-- query 3
-- @skip_result_check=true
-- Mutate the branch only.
INSERT INTO br_rl_${uuid0}.ns_${uuid0}.brlineage @feat VALUES (4, 40);
UPDATE br_rl_${uuid0}.ns_${uuid0}.brlineage @feat SET v = 99 WHERE id = 2;
DELETE FROM br_rl_${uuid0}.ns_${uuid0}.brlineage @feat WHERE id = 3;

-- query 4
-- main is unchanged: still 3 rows with their original row-lineage triples.
-- @db=br_rl_${uuid0}.ns_${uuid0}
SELECT count() AS n_main_rows, count(DISTINCT _row_id) AS n_main_unique
  FROM brlineage;

-- query 5
-- main is identical to the snap0 tag (no diff in row-lineage triples).
-- @db=br_rl_${uuid0}.ns_${uuid0}
SELECT count(*) AS n_main_diverged
  FROM (
    SELECT id, _row_id, _last_updated_sequence_number FROM brlineage
    EXCEPT
    SELECT id, _row_id, _last_updated_sequence_number
      FROM brlineage FOR VERSION AS OF 'snap0'
  ) diff;

-- query 6
-- branch_feat sees: 1 (unchanged), 2 (UPDATEd, _row_id preserved), 4 (new).
-- @db=br_rl_${uuid0}.ns_${uuid0}
SELECT count() AS n_branch_rows, count(DISTINCT _row_id) AS n_branch_unique
  FROM brlineage FOR VERSION AS OF 'feat';

-- query 7
-- The UPDATEd row on branch retains its main-side _row_id (V3 row-lineage
-- preservation under UPDATE). Compare id=2's _row_id between snap0 and
-- branch_feat — they must be identical.
-- @db=br_rl_${uuid0}.ns_${uuid0}
SELECT
  (SELECT _row_id FROM brlineage FOR VERSION AS OF 'snap0' WHERE id = 2)
  =
  (SELECT _row_id FROM brlineage FOR VERSION AS OF 'feat' WHERE id = 2)
  AS row_id_preserved_under_branch_update;

-- query 8
-- @skip_result_check=true
ALTER TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage EXECUTE drop_tag(name => 'snap0', if_exists => false);
DROP BRANCH feat IN TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage;
DROP TABLE br_rl_${uuid0}.ns_${uuid0}.brlineage ;
DROP SCHEMA br_rl_${uuid0}.ns_${uuid0};
DROP CATALOG br_rl_${uuid0};
