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

-- query 1
SELECT UUID 'F24F9B64-81FA-49D1-B74E-8C09A6E31C56' AS literal_uuid,
       CAST('f24f9b64-81fa-49d1-b74e-8c09a6e31c56' AS UUID) AS text_uuid,
       CAST(X'F24F9B6481FA49D1B74E8C09A6E31C56' AS UUID) AS bytes_uuid,
       CAST(UUID 'f24f9b64-81fa-49d1-b74e-8c09a6e31c56' AS VARBINARY) AS bytes,
       CAST(UUID 'f24f9b64-81fa-49d1-b74e-8c09a6e31c56' AS VARCHAR(36)) AS text;

-- query 2
SELECT ARRAY[UUID 'f24f9b64-81fa-49d1-b74e-8c09a6e31c56', NULL] AS uuid_list,
       UUID '7fffffff-0000-0000-0000-000000000000'
           < UUID '80000000-0000-0000-0000-000000000000' AS byte_order;

-- query 3
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(UUID 'f24f9b64-81fa-49d1-b74e-8c09a6e31c56' AS VARCHAR(35));

-- query 4
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST(X'0001' AS UUID);

-- query 5
-- @expect_trino_error=INVALID_CAST_ARGUMENT,USER_ERROR
SELECT CAST('f24f9b6481fa49d1b74e8c09a6e31c56' AS UUID);
