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
-- @tags=iceberg_dml,delete
-- Test Objective:
-- DELETE matching zero rows on an Iceberg v3 table is a no-op: no error, all
-- original rows remain visible.
DROP TABLE IF EXISTS ${case_db}.t_delete_no_match;
CREATE TABLE ${case_db}.t_delete_no_match (
  id INT,
  v INT
) WITH (format_version = 3);
INSERT INTO ${case_db}.t_delete_no_match VALUES ROW(1, 100);
DELETE FROM ${case_db}.t_delete_no_match WHERE id = 999;
SELECT id, v FROM ${case_db}.t_delete_no_match ORDER BY id;
