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
-- @tags=iceberg_ddl,default,numeric
-- Test Objective:
-- 1. Validate ALTER ADD COLUMN ... DEFAULT for numeric types at boundary values
--    (negative, zero, very large), which v3_default_primitive_types.sql does not.
-- 2. Cover SMALLINT/INT/BIGINT/FLOAT/DOUBLE with explicit large/negative/zero
--    defaults distinct from the small literals in v3_default_primitive_types.

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name VARCHAR) WITH (format_version = 3);
INSERT INTO ${case_db}.t VALUES ROW(1, 'alice'), ROW(2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN score SMALLINT DEFAULT 100;
ALTER TABLE ${case_db}.t ADD COLUMN salary INT DEFAULT 50000;
ALTER TABLE ${case_db}.t ADD COLUMN revenue BIGINT DEFAULT 1000000;
ALTER TABLE ${case_db}.t ADD COLUMN rating REAL DEFAULT 4.5;
ALTER TABLE ${case_db}.t ADD COLUMN percentage DOUBLE DEFAULT 95.5;
ALTER TABLE ${case_db}.t ADD COLUMN zero_v INT DEFAULT 0;
ALTER TABLE ${case_db}.t ADD COLUMN neg_v INT DEFAULT -100;

SELECT id, name, score, salary, revenue, rating, percentage, zero_v, neg_v
FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES ROW(3, 'charlie');

SELECT id, name, score, salary, revenue, rating, percentage, zero_v, neg_v
FROM ${case_db}.t ORDER BY id;
