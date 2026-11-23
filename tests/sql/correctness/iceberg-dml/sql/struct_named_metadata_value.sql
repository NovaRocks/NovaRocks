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
-- Ordinary binary struct fields must never be recognized as logical Variant.
-- query 1
-- @skip_result_check=true
CREATE TABLE ${case_db}.struct_named_metadata_value (id INT,s ROW(metadata VARBINARY,value VARBINARY));
INSERT INTO ${case_db}.struct_named_metadata_value VALUES
  ROW(1,ROW(X'0100',X'FF00')),
  ROW(2,ROW(NULL,X'')),ROW(3,NULL);

-- query 2
SELECT id,to_hex(s.metadata) AS metadata_hex,to_hex(s.value) AS value_hex,s IS NULL AS whole_null
FROM ${case_db}.struct_named_metadata_value ORDER BY id;

-- query 3
-- @skip_result_check=true
DROP TABLE ${case_db}.struct_named_metadata_value;
