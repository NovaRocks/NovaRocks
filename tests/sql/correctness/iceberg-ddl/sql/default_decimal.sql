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
-- @tags=iceberg_ddl,default,decimal
-- Test Objective:
-- 1. Validate ALTER ADD COLUMN ... DEFAULT for DECIMAL across multiple
--    precision/scale combos including DECIMAL(20, 6) (exceeds the single
--    DECIMAL(10, 2) example in v3_default_primitive_types.sql).

DROP TABLE IF EXISTS ${case_db}.t;
CREATE TABLE ${case_db}.t (id INT, name VARCHAR) WITH (format_version = 3);
INSERT INTO ${case_db}.t VALUES ROW(1, 'alice'), ROW(2, 'bob');

ALTER TABLE ${case_db}.t ADD COLUMN price DECIMAL(10, 2) DEFAULT 9.99;
ALTER TABLE ${case_db}.t ADD COLUMN rate DECIMAL(5, 4) DEFAULT 0.1234;
ALTER TABLE ${case_db}.t ADD COLUMN big DECIMAL(20, 6) DEFAULT 123456789.000001;

SELECT id, name, price, rate, big FROM ${case_db}.t ORDER BY id;

INSERT INTO ${case_db}.t (id, name) VALUES ROW(3, 'charlie');
SELECT id, name, price, rate, big FROM ${case_db}.t ORDER BY id;
