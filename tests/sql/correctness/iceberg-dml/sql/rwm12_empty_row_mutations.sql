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

-- @order_sensitive=true
-- A13. Iceberg v3 with an existing file. Empty row-level operations keep its snapshot.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm12_empty_rows (id BIGINT, value BIGINT) WITH (format_version = 3);
INSERT INTO ${case_db}.rwm12_empty_rows VALUES ROW(1, 10);
CREATE TABLE ${case_db}.rwm12_empty_rows_source (id BIGINT) WITH (format_version = 3);
INSERT INTO ${case_db}.rwm12_empty_rows_source VALUES ROW(3);

-- query 2
SELECT count(*) AS snapshots FROM ${case_db}."rwm12_empty_rows$snapshots";

-- query 3
DELETE FROM ${case_db}.rwm12_empty_rows WHERE id = 99;

-- query 4
UPDATE ${case_db}.rwm12_empty_rows SET value = 100 WHERE FALSE;

-- query 5
MERGE INTO ${case_db}.rwm12_empty_rows AS t
USING ${case_db}.rwm12_empty_rows_source AS s ON t.id = s.id
WHEN MATCHED THEN DELETE;

-- query 6
SELECT count(*) AS snapshots FROM ${case_db}."rwm12_empty_rows$snapshots";

-- query 7
-- @skip_result_check=true
DROP TABLE ${case_db}.rwm12_empty_rows;
DROP TABLE ${case_db}.rwm12_empty_rows_source;
