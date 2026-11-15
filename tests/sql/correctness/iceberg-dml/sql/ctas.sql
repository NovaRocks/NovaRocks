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
-- Ordinary iceberg-dml runs without the test-only fenced REST proxy. CTAS
-- therefore has to fail closed before source execution. The positive CTAS
-- publication fault matrix lives in the explicit-only lake-publication
-- suite, where the runner-owned fenced catalog capability is enabled.

-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.src (id INT, name VARCHAR(16))
TBLPROPERTIES ("format-version" = "3");

-- query 2
-- @skip_result_check=true
INSERT INTO ${case_db}.src VALUES (1, 'alice'), (2, 'bob');

-- query 3
-- @expect_error=staged
CREATE TABLE ${case_db}.hadoop_dst AS
  SELECT id, UPPER(name) AS uname FROM ${case_db}.src;

-- query 4
-- @expect_error=staged
CREATE TABLE IF NOT EXISTS ${case_db}.hadoop_dst_if_missing AS
  SELECT id FROM ${case_db}.src;

-- query 5
-- @skip_result_check=true
DROP TABLE ${case_db}.src FORCE;
