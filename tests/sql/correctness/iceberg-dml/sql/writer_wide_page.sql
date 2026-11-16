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

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.rwm13_wide (id BIGINT, payload VARCHAR);
INSERT INTO ${case_db}.rwm13_wide VALUES (1, repeat('x', 16777217));

-- query 2
SELECT id, length(payload) AS payload_chars FROM ${case_db}.rwm13_wide;

-- query 3
-- @cleanup=true
-- @skip_result_check=true
DROP TABLE IF EXISTS ${case_db}.rwm13_wide FORCE;
